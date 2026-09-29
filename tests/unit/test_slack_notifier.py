"""Built-in Slack notifier and the run-level hook it is built on.

The notifier posts one digest per command invocation rather than one message
per pipeline, because a systemic failure (an expired credential, say) fails
every pipeline in the selection at once and fifty messages bury the signal.
These tests pin that grouping, the failure-only default, and that a broken
Slack never breaks the run it is reporting on.
"""

import logging
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional
from unittest.mock import patch

import pytest

from dlt_saga.hooks.notifiers.slack import SlackNotifier, register_slack_notifier
from dlt_saga.hooks.registry import (
    ON_PIPELINE_ERROR,
    ON_RUN_COMPLETE,
    HookContext,
    HookRegistry,
    RunContext,
)
from dlt_saga.project_config import SlackNotificationConfig


@dataclass
class _Result:
    """Stand-in for session.PipelineResult."""

    pipeline_name: str
    success: bool
    error: Optional[str] = None
    config: Optional[Any] = None


@dataclass
class _Config:
    """Stand-in for PipelineConfig, carrying only what the notifier reads."""

    config_dict: Dict[str, Any] = field(default_factory=dict)


class _Recorder:
    """Captures sends instead of hitting Slack."""

    def __init__(self):
        self.sent: List[Dict[str, Any]] = []

    def __call__(self, url, payload, timeout):
        self.sent.append({"url": url, "payload": payload, "timeout": timeout})

    @property
    def text(self) -> str:
        return self.sent[-1]["payload"]["text"]


def _notifier(recorder, **overrides):
    config = SlackNotificationConfig(webhook_url="env_secret::SLACK_HOOK", **overrides)
    return SlackNotifier(config, sender=recorder)


def _run_context(results, **overrides):
    defaults = {
        "command": "ingest",
        "select": ["tag:daily"],
        "target": "prod",
        "environment": "prod",
        "results": results,
    }
    defaults.update(overrides)
    return RunContext(**defaults)


@pytest.fixture(autouse=True)
def _resolve_secret_passthrough():
    """Keep secret resolution out of these tests — it is not what they cover."""
    with patch(
        "dlt_saga.utility.secrets.resolve_secret", side_effect=lambda v: f"https://{v}"
    ):
        yield


@pytest.mark.unit
class TestDigestGrouping:
    def test_one_message_for_many_failures(self):
        """The whole point: fifty broken pipelines make one message."""
        recorder = _Recorder()
        results = [_Result(f"p{i}", success=False, error="boom") for i in range(50)]

        _notifier(recorder).on_run_complete(_run_context(results))

        assert len(recorder.sent) == 1
        assert "50 of 50" in recorder.text

    def test_long_failure_list_is_capped_with_a_remainder(self):
        recorder = _Recorder()
        results = [_Result(f"p{i}", success=False, error="boom") for i in range(13)]

        _notifier(recorder).on_run_complete(_run_context(results))

        assert "…and 3 more" in recorder.text
        assert recorder.text.count("• `p") == 10

    def test_failure_lines_name_the_pipeline_and_error(self):
        recorder = _Recorder()
        results = [
            _Result("shop__orders", success=False, error="min_rows=1 not met"),
            _Result("shop__customers", success=True),
        ]

        _notifier(recorder).on_run_complete(_run_context(results))

        assert "1 of 2" in recorder.text
        assert "• `shop__orders` — min_rows=1 not met" in recorder.text
        assert "shop__customers" not in recorder.text

    def test_verbose_error_is_truncated(self):
        recorder = _Recorder()
        results = [_Result("p", success=False, error="x" * 5000)]

        _notifier(recorder).on_run_complete(_run_context(results))

        assert "…" in recorder.text
        assert len(recorder.text) < 1000

    def test_context_line_carries_target_selection_and_duration(self):
        from datetime import datetime, timedelta, timezone

        recorder = _Recorder()
        start = datetime(2026, 1, 1, tzinfo=timezone.utc)
        ctx = _run_context(
            [_Result("p", success=False, error="boom")],
            started_at=start,
            finished_at=start + timedelta(seconds=42.5),
        )

        _notifier(recorder).on_run_complete(ctx)

        assert "target `prod/prod`" in recorder.text
        assert "select `tag:daily`" in recorder.text
        assert "42.5s" in recorder.text


@pytest.mark.unit
class TestNotifyOn:
    def test_failure_default_stays_silent_on_success(self):
        recorder = _Recorder()
        results = [_Result("p", success=True)]

        _notifier(recorder).on_run_complete(_run_context(results))

        assert recorder.sent == []

    def test_always_posts_on_success(self):
        recorder = _Recorder()
        results = [_Result("p", success=True)]

        _notifier(recorder, notify_on="always").on_run_complete(_run_context(results))

        assert len(recorder.sent) == 1
        assert ":white_check_mark:" in recorder.text

    def test_always_flags_an_empty_selection(self):
        """A scheduled run that silently stopped matching anything."""
        recorder = _Recorder()

        _notifier(recorder, notify_on="always").on_run_complete(_run_context([]))

        assert "selected no pipelines" in recorder.text


@pytest.mark.unit
class TestMentions:
    def test_mentions_appended_on_failure(self):
        recorder = _Recorder()
        results = [_Result("p", success=False, error="boom")]

        _notifier(recorder, mentions=["<@U1>", "<!subteam^S2>"]).on_run_complete(
            _run_context(results)
        )

        assert "<@U1> <!subteam^S2>" in recorder.text

    def test_mentions_absent_from_a_success_digest(self):
        recorder = _Recorder()
        results = [_Result("p", success=True)]

        _notifier(recorder, notify_on="always", mentions=["<@U1>"]).on_run_complete(
            _run_context(results)
        )

        assert "<@U1>" not in recorder.text


@pytest.mark.unit
class TestPerPipelineMentions:
    """One key at two levels: the project block pings for any failure, a
    pipeline's own `notifications.slack.mentions` pings for its own.
    """

    def _failing(self, config_dict):
        return _run_context(
            [
                _Result(
                    "shop__orders",
                    success=False,
                    error="boom",
                    config=_Config(config_dict),
                )
            ]
        )

    def test_pipeline_mentions_appended_to_its_line(self):
        recorder = _Recorder()

        _notifier(recorder).on_run_complete(
            self._failing({"notifications": {"slack": {"mentions": ["<@U9>"]}}})
        )

        assert "• `shop__orders` — boom <@U9>" in recorder.text

    def test_several_pipeline_mentions(self):
        recorder = _Recorder()

        _notifier(recorder).on_run_complete(
            self._failing(
                {"notifications": {"slack": {"mentions": ["<@U9>", "<!subteam^S1>"]}}}
            )
        )

        assert "boom <@U9> <!subteam^S1>" in recorder.text

    def test_bare_string_accepted(self):
        recorder = _Recorder()

        _notifier(recorder).on_run_complete(
            self._failing({"notifications": {"slack": {"mentions": "<@U9>"}}})
        )

        assert "boom <@U9>" in recorder.text

    def test_project_and_pipeline_mentions_compose(self):
        """Ops list at the bottom, the pipeline's own on its line."""
        recorder = _Recorder()

        _notifier(recorder, mentions=["<@UOPS>"]).on_run_complete(
            self._failing({"notifications": {"slack": {"mentions": ["<@U9>"]}}})
        )

        assert "• `shop__orders` — boom <@U9>" in recorder.text
        assert recorder.text.rstrip().endswith("<@UOPS>")

    def test_meta_is_never_read(self):
        """`meta` is documented as uninterpreted by the runtime; keep it so."""
        recorder = _Recorder()

        _notifier(recorder).on_run_complete(
            self._failing({"meta": {"data_owner": "data@example.com"}})
        )

        assert "• `shop__orders` — boom" in recorder.text
        assert "data@example.com" not in recorder.text

    @pytest.mark.parametrize(
        "config_dict",
        [
            {},
            {"notifications": None},
            {"notifications": "not-a-dict"},
            {"notifications": {}},
            {"notifications": {"slack": None}},
            {"notifications": {"slack": {}}},
            {"notifications": {"slack": {"mentions": None}}},
            {"notifications": {"slack": {"mentions": []}}},
            {"notifications": {"slack": {"mentions": [42]}}},
            {"notifications": {"slack": {"mentions": {"a": 1}}}},
        ],
    )
    def test_unusable_values_are_ignored_not_raised(self, config_dict):
        """A notifier must never fail the run it is reporting on."""
        recorder = _Recorder()

        _notifier(recorder).on_run_complete(self._failing(config_dict))

        assert "• `shop__orders` — boom" in recorder.text


@pytest.mark.unit
class TestPerPipeline:
    def test_off_by_default(self):
        recorder = _Recorder()
        ctx = HookContext(
            pipeline_name="p", config=_Config(), command="ingest", error=Exception("x")
        )

        _notifier(recorder).on_pipeline_error(ctx)

        assert recorder.sent == []

    def test_opt_in_posts_per_failed_pipeline(self):
        recorder = _Recorder()
        ctx = HookContext(
            pipeline_name="shop__orders",
            config=_Config({"notifications": {"slack": {"mentions": ["<@U9>"]}}}),
            command="ingest",
            error=Exception("boom"),
        )

        _notifier(recorder, per_pipeline=True).on_pipeline_error(ctx)

        assert "`shop__orders` (ingest) — boom <@U9>" in recorder.text


@pytest.mark.unit
class TestDeliveryIsBestEffort:
    def test_send_failure_is_logged_not_raised(self, caplog):
        """A broken alerting channel must never break the load it reports on."""

        def explode(url, payload, timeout):
            raise RuntimeError("slack is down")

        notifier = _notifier(explode)
        with caplog.at_level(logging.WARNING):
            notifier.on_run_complete(
                _run_context([_Result("p", success=False, error="boom")])
            )

        assert "Slack notification failed" in caplog.text

    def test_empty_resolved_webhook_warns_and_skips(self, caplog):
        recorder = _Recorder()
        notifier = _notifier(recorder)
        with patch("dlt_saga.utility.secrets.resolve_secret", return_value=""):
            with caplog.at_level(logging.WARNING):
                notifier.on_run_complete(
                    _run_context([_Result("p", success=False, error="boom")])
                )

        assert recorder.sent == []
        assert "resolved to an empty value" in caplog.text


@pytest.mark.unit
class TestRegistration:
    def test_no_webhook_registers_nothing(self):
        registry = HookRegistry()

        result = register_slack_notifier(SlackNotificationConfig(), registry)

        assert result is None
        assert registry.is_empty()

    def test_digest_only_by_default(self):
        registry = HookRegistry()

        register_slack_notifier(
            SlackNotificationConfig(webhook_url="env_secret::X"), registry
        )

        assert registry.has_handlers(ON_RUN_COMPLETE)
        assert not registry.has_handlers(ON_PIPELINE_ERROR)

    def test_per_pipeline_also_registers_the_error_event(self):
        registry = HookRegistry()

        register_slack_notifier(
            SlackNotificationConfig(webhook_url="env_secret::X", per_pipeline=True),
            registry,
        )

        assert registry.has_handlers(ON_RUN_COMPLETE)
        assert registry.has_handlers(ON_PIPELINE_ERROR)


@pytest.mark.unit
class TestRunContext:
    def test_counts_and_failures(self):
        ctx = _run_context(
            [
                _Result("a", success=True),
                _Result("b", success=False, error="x"),
                _Result("c", success=False, error="y"),
            ]
        )

        assert ctx.succeeded == 1
        assert ctx.failed == 2
        assert ctx.has_failures
        assert [r.pipeline_name for r in ctx.failures] == ["b", "c"]

    def test_duration_is_none_without_both_stamps(self):
        assert _run_context([]).duration_seconds is None


@pytest.mark.unit
class TestRegistryFireWithRunContext:
    def test_failing_run_handler_is_caught_not_raised(self, caplog):
        """RunContext has no pipeline_name; the warning path must not assume one."""
        registry = HookRegistry()

        def boom(ctx):
            raise RuntimeError("handler exploded")

        registry.register(ON_RUN_COMPLETE, boom)
        with caplog.at_level(logging.WARNING):
            registry.fire(ON_RUN_COMPLETE, _run_context([]))

        assert "raised an exception" in caplog.text
        assert "subject=ingest" in caplog.text


@pytest.mark.unit
class TestWebhookTransport:
    """Retry policy for the actual HTTP call."""

    def _response(self, status, text="ok"):
        from unittest.mock import MagicMock

        response = MagicMock()
        response.status_code = status
        response.text = text
        return response

    def _post(self, responses):
        from dlt_saga.hooks.notifiers.slack import _post_to_slack

        with patch("time.sleep"), patch("requests.post", side_effect=responses) as post:
            try:
                _post_to_slack("https://hooks.example/x", {"text": "hi"}, 10.0)
                error = None
            except RuntimeError as exc:
                error = str(exc)
        return post, error

    def test_success_posts_once(self):
        post, error = self._post([self._response(200)])
        assert error is None
        assert post.call_count == 1

    def test_transient_status_is_retried(self):
        post, error = self._post([self._response(503), self._response(200)])
        assert error is None
        assert post.call_count == 2

    def test_rate_limit_is_retried(self):
        post, error = self._post([self._response(429), self._response(200)])
        assert error is None
        assert post.call_count == 2

    def test_client_error_is_not_retried(self):
        """A 400 means the payload or webhook is wrong; repeating won't fix it."""
        post, error = self._post([self._response(400, "invalid_payload")])
        assert post.call_count == 1
        assert "HTTP 400" in error

    def test_connection_errors_are_retried_then_give_up(self):
        import requests

        post, error = self._post([requests.ConnectionError("no route")] * 3)
        assert post.call_count == 3
        assert "ConnectionError" in error

    def test_timeout_is_passed_through(self):
        post, _ = self._post([self._response(200)])
        assert post.call_args.kwargs["timeout"] == 10.0


@pytest.mark.unit
class TestNotificationsProjectConfig:
    def _parse(self, data):
        from dlt_saga.project_config import SagaProjectConfig

        return SagaProjectConfig.from_dict(data)

    def test_absent_section_is_none(self):
        assert self._parse({}).notifications is None

    def test_slack_block_is_parsed(self):
        config = self._parse(
            {
                "notifications": {
                    "slack": {
                        "webhook_url": "googlesecretmanager::projects/p/secrets/s/versions/latest",
                        "notify_on": "always",
                        "mentions": ["<@U1>"],
                        "per_pipeline": True,
                        "timeout_seconds": 5,
                    }
                }
            }
        ).notifications.slack

        assert config.notify_on == "always"
        assert config.mentions == ["<@U1>"]
        assert config.per_pipeline is True
        assert config.timeout_seconds == 5.0

    def test_single_mention_string_is_wrapped(self):
        config = self._parse(
            {"notifications": {"slack": {"webhook_url": "x", "mentions": "<@U1>"}}}
        ).notifications.slack
        assert config.mentions == ["<@U1>"]

    def test_defaults(self):
        config = self._parse(
            {"notifications": {"slack": {"webhook_url": "x"}}}
        ).notifications.slack
        assert config.notify_on == "failure"
        assert config.per_pipeline is False
        assert config.mentions is None

    def test_invalid_notify_on_rejected(self):
        with pytest.raises(ValueError, match="notify_on must be"):
            self._parse(
                {"notifications": {"slack": {"webhook_url": "x", "notify_on": "daily"}}}
            )

    def test_invalid_mentions_rejected(self):
        with pytest.raises(ValueError, match="mentions must be"):
            self._parse(
                {"notifications": {"slack": {"webhook_url": "x", "mentions": {"a": 1}}}}
            )
