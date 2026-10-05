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


def _outcome(name, attempts, failures, failing, error="boom", config=None):
    from dlt_saga.notify import PipelineOutcome

    return PipelineOutcome(
        pipeline_name=name,
        table_name=name.split("__")[-1],
        attempts=attempts,
        failures=failures,
        currently_failing=failing,
        latest_error=error if failures else None,
        config_dict=config or {},
    )


def _sweep(outcomes, **overrides):
    from dlt_saga.notify import SweepContext

    defaults = {
        "environment": "prod",
        "since_days": 7,
        "execution_ids": ["e1", "e2", "e3"],
        "outcomes": outcomes,
        # Totals span every pipeline in the sweep, not just the failing ones —
        # default to exactly the failing attempts so a test that cares about
        # scale has to say so explicitly.
        "total_attempts": sum(o.attempts for o in outcomes),
        "failed_attempts": sum(o.failures for o in outcomes),
    }
    defaults.update(overrides)
    return SweepContext(**defaults)


def _rendered(recorder):
    """All text in a Block Kit payload, flattened for substring assertions."""
    payload = recorder.sent[-1]["payload"]
    parts = [payload.get("text", "")]

    def walk(blocks):
        for block in blocks or []:
            if "text" in block and isinstance(block["text"], dict):
                parts.append(block["text"]["text"])
            for element in block.get("elements", []):
                parts.append(element.get("text", ""))

    walk(payload.get("blocks"))
    for attachment in payload.get("attachments", []):
        walk(attachment.get("blocks"))
    return "\n".join(parts)


def _payload(recorder):
    return recorder.sent[-1]["payload"]


@pytest.mark.unit
class TestSweepDigest:
    """The digest is Block Kit, not a wall of text.

    Posted through an app installed for something else, a plain-text message
    reads as that app's alert — so attribution and structure are part of the
    message's job, not decoration.
    """

    def test_attributed_to_saga(self):
        """Observed in production: a digest posted via a borrowed app looked
        like that app's own alert.
        """
        recorder = _Recorder()

        _notifier(recorder).on_sweep_complete(
            _sweep([_outcome("shop__orders", 1, 1, True)])
        )

        assert "*dlt-saga*" in _rendered(recorder)
        assert _payload(recorder)["text"].startswith("dlt-saga:")

    def test_fallback_text_is_set(self):
        """Notifications, sidebars and screen readers get `text`, not blocks."""
        recorder = _Recorder()

        _notifier(recorder).on_sweep_complete(
            _sweep([_outcome("shop__orders", 1, 1, True)])
        )

        text = _payload(recorder)["text"]
        assert "1 pipeline failing" in text
        assert "prod" in text

    def test_failing_is_red_recovered_is_amber(self):
        """The colour bar is read before any text in a busy channel."""
        recorder = _Recorder()
        notifier = _notifier(recorder)

        notifier.on_sweep_complete(_sweep([_outcome("shop__orders", 1, 1, True)]))
        assert _payload(recorder)["attachments"][0]["color"] == "#d7263d"

        notifier.on_sweep_complete(_sweep([_outcome("crm__accounts", 5, 1, False)]))
        assert _payload(recorder)["attachments"][0]["color"] == "#e0a800"

    def test_header_states_what_is_wrong(self):
        recorder = _Recorder()

        _notifier(recorder).on_sweep_complete(
            _sweep([_outcome("shop__orders", 3, 3, True)])
        )

        header = _payload(recorder)["blocks"][0]
        assert header["type"] == "header"
        assert header["text"]["text"] == "1 pipeline failing"

    def test_singular_and_plural_agree(self):
        """ "1 pipelines failing" reads as a bug in the tool."""
        recorder = _Recorder()
        notifier = _notifier(recorder)

        notifier.on_sweep_complete(_sweep([_outcome("a", 1, 1, True)]))
        assert "1 pipeline failing" in _rendered(recorder)

        notifier.on_sweep_complete(
            _sweep([_outcome("a", 1, 1, True), _outcome("b", 1, 1, True)])
        )
        assert "2 pipelines failing" in _rendered(recorder)

    def test_errors_render_as_code_blocks(self):
        """A BigQuery or HTTP error reflowed as prose is unreadable, and it is
        the part you actually need.
        """
        recorder = _Recorder()

        _notifier(recorder).on_sweep_complete(
            _sweep([_outcome("shop__orders", 1, 1, True, "Database Error in model x")])
        )

        assert "```Database Error in model x```" in _rendered(recorder)

    def test_a_fenced_error_cannot_break_out_of_its_block(self):
        """An error containing a fence would otherwise corrupt the layout."""
        recorder = _Recorder()

        _notifier(recorder).on_sweep_complete(
            _sweep([_outcome("shop__orders", 1, 1, True, "boom ``` tail")])
        )

        body = _rendered(recorder)
        assert "boom" in body
        assert body.count("```") == 2

    def test_persistence_distinguishes_outage_from_blip(self):
        recorder = _Recorder()
        ctx = _sweep(
            [
                _outcome("shop__orders", 3, 3, True),
                _outcome("crm__accounts", 3, 1, True),
            ]
        )

        _notifier(recorder).on_sweep_complete(ctx)

        body = _rendered(recorder)
        assert "failed 3 of 3 attempts" in body
        assert "failed 1 of 3 attempts" in body

    def test_single_attempt_omits_the_ratio(self):
        recorder = _Recorder()

        _notifier(recorder).on_sweep_complete(
            _sweep([_outcome("shop__orders", 1, 1, True)], execution_ids=["e1"])
        )

        body = _rendered(recorder)
        assert "— failed" in body
        assert "1 of 1" not in body

    def test_recovered_only_still_posts(self):
        """A pipeline that fails one run in five and always recovers before the
        next sweep would otherwise never be reported at all.
        """
        recorder = _Recorder()

        _notifier(recorder).on_sweep_complete(
            _sweep([_outcome("crm__accounts", 5, 1, False)])
        )

        body = _rendered(recorder)
        assert "failed and recovered" in body
        assert "`crm__accounts` — 1 of 5 attempts failed, latest OK" in body
        assert "*Failing*" not in body

    def test_failing_and_recovered_are_separated(self):
        recorder = _Recorder()
        ctx = _sweep(
            [
                _outcome("shop__orders", 3, 3, True),
                _outcome("crm__accounts", 3, 1, False),
            ]
        )

        _notifier(recorder).on_sweep_complete(ctx)

        body = _rendered(recorder)
        assert body.index("*Failing*") < body.index("*Recovered*")
        assert body.index("shop__orders") < body.index("*Recovered*")

    def test_clean_sweep_posts_nothing(self):
        recorder = _Recorder()

        _notifier(recorder).on_sweep_complete(_sweep([]))

        assert recorder.sent == []

    def test_attempt_totals_give_scale(self):
        """The denominator covers every pipeline, so a lone failure among many
        healthy ones reads as isolated rather than systemic.
        """
        recorder = _Recorder()
        ctx = _sweep(
            [_outcome("shop__orders", 3, 2, True)],
            total_attempts=46,
            failed_attempts=2,
        )

        _notifier(recorder).on_sweep_complete(ctx)

        assert "44 of 46 pipeline attempts succeeded" in _rendered(recorder)

    def test_environment_is_named(self):
        recorder = _Recorder()

        _notifier(recorder).on_sweep_complete(
            _sweep([_outcome("shop__orders", 1, 1, True)])
        )

        assert "prod" in _rendered(recorder)

    def test_no_backlog_line(self):
        """The notifier reports recent runs that may need handling, not history."""
        recorder = _Recorder()

        _notifier(recorder).on_sweep_complete(
            _sweep([_outcome("shop__orders", 1, 1, True)])
        )

        body = _rendered(recorder)
        assert "not reported" not in body
        assert "outside" not in body

    def test_failure_list_is_capped(self):
        recorder = _Recorder()
        ctx = _sweep([_outcome(f"p{i}", 1, 1, True) for i in range(13)])

        _notifier(recorder).on_sweep_complete(ctx)

        body = _rendered(recorder)
        assert body.count("*`p") == 10
        assert "…and 3 more" in body

    def test_block_count_stays_within_slack_limits(self):
        """Slack rejects a message over 50 blocks outright."""
        recorder = _Recorder()
        ctx = _sweep([_outcome(f"p{i}", 1, 1, True) for i in range(40)])

        _notifier(recorder).on_sweep_complete(ctx)

        payload = _payload(recorder)
        total = len(payload["blocks"]) + sum(
            len(a["blocks"]) for a in payload["attachments"]
        )
        assert total <= 50

    def test_pipeline_mentions_come_from_the_stored_config(self):
        """No single process ran every pipeline, but the plan row kept its config."""
        recorder = _Recorder()
        ctx = _sweep(
            [
                _outcome(
                    "shop__orders",
                    1,
                    1,
                    True,
                    config={"notifications": {"slack": {"mentions": ["<@U9>"]}}},
                )
            ]
        )

        _notifier(recorder).on_sweep_complete(ctx)

        assert "<@U9>" in _rendered(recorder)

    def test_project_mentions_only_on_failures(self):
        recorder = _Recorder()

        _notifier(recorder, mentions=["<@UOPS>"]).on_sweep_complete(
            _sweep([_outcome("crm__accounts", 5, 1, False)])
        )

        assert "<@UOPS>" not in _rendered(recorder)

    def test_project_mentions_appended_when_something_is_failing(self):
        recorder = _Recorder()

        _notifier(recorder, mentions=["<@UOPS>"]).on_sweep_complete(
            _sweep([_outcome("shop__orders", 1, 1, True)])
        )

        assert "<@UOPS>" in _rendered(recorder)

    def test_verbose_error_is_truncated(self):
        recorder = _Recorder()
        ctx = _sweep([_outcome("shop__orders", 1, 1, True, "x" * 5000)])

        _notifier(recorder).on_sweep_complete(ctx)

        assert len(_rendered(recorder)) < 1500


@pytest.mark.unit
class TestTokenTransport:
    """Bot-token delivery via chat.postMessage, mirroring Elementary.

    The Web API answers HTTP 200 even on failure, carrying the outcome in
    `ok`/`error` — unlike a webhook, where the status code is the answer. A
    sender that checked only the status would swallow every API error.
    """

    def _config(self, **overrides):
        from dlt_saga.project_config import SlackNotificationConfig

        defaults = {"token": "env_secret::SLACK_TOKEN", "channel": "#data-alerts"}
        defaults.update(overrides)
        return SlackNotificationConfig(**defaults)

    def _api(self, *responses):
        """Patch the Slack API call with a scripted sequence of bodies."""
        from unittest.mock import patch

        return patch(
            "dlt_saga.hooks.notifiers.slack._slack_api_call",
            side_effect=list(responses),
        )

    def _notify(self, config, api):
        from dlt_saga.hooks.notifiers.slack import SlackNotifier

        notifier = SlackNotifier(config)
        with patch(
            "dlt_saga.utility.secrets.resolve_secret", side_effect=lambda v: f"xoxb-{v}"
        ):
            with api as mocked:
                notifier.on_run_complete(
                    _run_context([_Result("p", success=False, error="boom")])
                )
        return mocked

    def test_posts_to_chat_post_message_with_the_channel(self):
        mocked = self._notify(self._config(), self._api({"ok": True}))

        method, _token, payload, _timeout = mocked.call_args[0]
        assert method == "chat.postMessage"
        assert payload["channel"] == "#data-alerts"
        assert "saga ingest failed" in payload["text"]

    def test_token_is_sent_resolved_not_as_a_secret_uri(self):
        mocked = self._notify(self._config(), self._api({"ok": True}))

        _method, token, _payload, _timeout = mocked.call_args[0]
        assert token == "xoxb-env_secret::SLACK_TOKEN"

    def test_not_in_channel_joins_then_retries(self, caplog):
        """Elementary's recovery: the app simply hasn't joined yet."""
        mocked = self._notify(
            self._config(),
            self._api(
                {"ok": False, "error": "not_in_channel"},
                {"ok": True},  # conversations.join
                {"ok": True},  # the retried post
            ),
        )

        methods = [c[0][0] for c in mocked.call_args_list]
        assert methods == ["chat.postMessage", "conversations.join", "chat.postMessage"]

    def test_unjoinable_channel_reports_what_to_do(self, caplog):
        with caplog.at_level(logging.WARNING):
            self._notify(
                self._config(),
                self._api(
                    {"ok": False, "error": "not_in_channel"},
                    {"ok": False, "error": "channel_not_found"},
                ),
            )

        assert "Invite the app to the channel" in caplog.text

    def test_channel_not_found_is_explained(self, caplog):
        with caplog.at_level(logging.WARNING):
            self._notify(
                self._config(), self._api({"ok": False, "error": "channel_not_found"})
            )

        assert "was not found" in caplog.text

    def test_bad_token_is_not_retried(self, caplog):
        """Retrying an invalid token just repeats it."""
        mocked = self._notify(
            self._config(), self._api({"ok": False, "error": "invalid_auth"})
        )

        assert mocked.call_count == 1
        assert "rejected the token" in caplog.text

    def test_transient_error_is_retried(self):
        mocked = self._notify(
            self._config(),
            self._api({"ok": False, "error": "ratelimited"}, {"ok": True}),
        )

        assert mocked.call_count == 2

    def test_a_failed_send_never_raises(self, caplog):
        """An alerting channel that breaks must not break the caller."""
        with caplog.at_level(logging.WARNING):
            self._notify(
                self._config(),
                self._api(*[{"ok": False, "error": "ratelimited"}] * 3),
            )

        assert "Slack notification failed" in caplog.text


@pytest.mark.unit
class TestTransportSelection:
    def _config(self, **kw):
        from dlt_saga.project_config import SlackNotificationConfig

        return SlackNotificationConfig(**kw)

    def test_token_wins_over_webhook(self):
        """Matches Elementary, which checks slack_token before slack_webhook."""
        from unittest.mock import patch

        from dlt_saga.hooks.notifiers.slack import SlackNotifier

        webhook_calls = []
        config = self._config(
            token="env_secret::T", channel="#c", webhook_url="env_secret::W"
        )
        notifier = SlackNotifier(config, sender=lambda *a: webhook_calls.append(a))
        with patch("dlt_saga.utility.secrets.resolve_secret", side_effect=lambda v: v):
            with patch(
                "dlt_saga.hooks.notifiers.slack._slack_api_call",
                return_value={"ok": True},
            ) as api:
                notifier.on_run_complete(
                    _run_context([_Result("p", success=False, error="x")])
                )

        assert api.called
        assert webhook_calls == []

    def test_webhook_still_works_alone(self):
        recorder = _Recorder()

        _notifier(recorder).on_run_complete(
            _run_context([_Result("p", success=False, error="x")])
        )

        assert len(recorder.sent) == 1

    def test_registration_accepts_a_token(self):
        from dlt_saga.hooks.notifiers.slack import register_slack_notifier
        from dlt_saga.hooks.registry import ON_RUN_COMPLETE, HookRegistry

        registry = HookRegistry()

        register_slack_notifier(
            self._config(token="env_secret::T", channel="#c"), registry
        )

        assert registry.has_handlers(ON_RUN_COMPLETE)

    def test_registration_still_needs_one_of_them(self):
        from dlt_saga.hooks.notifiers.slack import register_slack_notifier
        from dlt_saga.hooks.registry import HookRegistry

        registry = HookRegistry()

        assert register_slack_notifier(self._config(), registry) is None
        assert registry.is_empty()

    def test_token_without_a_channel_is_rejected_at_config_time(self):
        from dlt_saga.project_config import SagaProjectConfig

        with pytest.raises(ValueError, match="channel is required"):
            SagaProjectConfig.from_dict(
                {"notifications": {"slack": {"token": "env_secret::T"}}}
            )


@pytest.mark.unit
class TestSectionLayout:
    """A section's line breaks *are* its layout.

    The error-summary truncator collapses all whitespace onto one line, which is
    right inside a bullet and destructive in a section — it put the code fence
    inline with the pipeline name.
    """

    def test_error_sits_on_its_own_line(self):
        recorder = _Recorder()

        _notifier(recorder).on_sweep_complete(
            _sweep([_outcome("shop__orders", 1, 1, True, "boom")])
        )

        section = _payload(recorder)["attachments"][0]["blocks"][1]["text"]["text"]
        assert section.splitlines()[0].endswith("failed")
        assert section.splitlines()[1] == "```boom```"

    def test_recovered_bullets_stay_on_separate_lines(self):
        recorder = _Recorder()

        _notifier(recorder).on_sweep_complete(
            _sweep(
                [
                    _outcome("a", 4, 1, False),
                    _outcome("b", 4, 2, False),
                    _outcome("c", 4, 3, False),
                ]
            )
        )

        blocks = _payload(recorder)["attachments"][0]["blocks"]
        bullets = next(
            b["text"]["text"]
            for b in blocks
            if b.get("text", {}).get("text", "").startswith("•")
        )
        assert len(bullets.splitlines()) == 3


@pytest.mark.unit
class TestReportLink:
    """saga publishes its own report but only ever sees a `gs://` output URI,
    which is not browsable — so the public URL has to be configured.
    """

    URL = "https://docs.example.com/saga_report.html"

    def test_report_is_linked_in_the_footer(self):
        recorder = _Recorder()

        _notifier(recorder, report_url=self.URL).on_sweep_complete(
            _sweep([_outcome("shop__orders", 1, 1, True)])
        )

        assert f"<{self.URL}|saga report>" in _rendered(recorder)

    def test_label_comes_from_saga_not_config(self):
        """saga knows what this URL is, so every project says the same thing."""
        recorder = _Recorder()

        _notifier(recorder, report_url=self.URL).on_sweep_complete(
            _sweep([_outcome("shop__orders", 1, 1, True)])
        )

        assert "|saga report>" in _rendered(recorder)

    def test_link_sits_alongside_the_scale_line(self):
        recorder = _Recorder()

        _notifier(recorder, report_url=self.URL).on_sweep_complete(
            _sweep(
                [_outcome("shop__orders", 1, 1, True)],
                total_attempts=46,
                failed_attempts=1,
            )
        )

        footer = _payload(recorder)["attachments"][0]["blocks"][-1]["elements"][0]
        assert "45 of 46" in footer["text"]
        assert self.URL in footer["text"]

    def test_unset_adds_nothing(self):
        recorder = _Recorder()

        _notifier(recorder).on_sweep_complete(
            _sweep([_outcome("shop__orders", 1, 1, True)])
        )

        assert "<http" not in _rendered(recorder)

    def test_linked_on_a_recovered_only_digest(self):
        """The report is where you go to look, failing or not."""
        recorder = _Recorder()

        _notifier(recorder, report_url=self.URL).on_sweep_complete(
            _sweep([_outcome("crm__accounts", 5, 1, False)])
        )

        assert self.URL in _rendered(recorder)

    def test_non_string_is_rejected_at_config_time(self):
        from dlt_saga.project_config import SagaProjectConfig

        with pytest.raises(ValueError, match="report_url must be a URL string"):
            SagaProjectConfig.from_dict(
                {
                    "notifications": {
                        "slack": {"webhook_url": "x", "report_url": ["https://a"]}
                    }
                }
            )


@pytest.mark.unit
class TestIdentityOverrideFeedback:
    """An app without `chat:write.customize` gets no error from Slack — the
    message posts, silently under the app's own name, and the call reports
    success. Observed in production: a digest kept arriving as another app's
    alert with nothing in the log to explain why.
    """

    def _post(self, responses, username="dlt-saga"):
        from dlt_saga.hooks.notifiers.slack import SlackNotifier
        from dlt_saga.project_config import SlackNotificationConfig

        notifier = SlackNotifier(
            SlackNotificationConfig(
                token="env_secret::T", channel="#c", username=username
            )
        )
        with (
            patch("dlt_saga.utility.secrets.resolve_secret", return_value="xoxb-x"),
            patch(
                "dlt_saga.hooks.notifiers.slack._slack_api_call",
                side_effect=list(responses),
            ),
        ):
            for _ in responses:
                notifier.on_run_complete(
                    _run_context([_Result("p", success=False, error="x")])
                )
                break
        return notifier

    def test_dropped_override_is_reported(self, caplog):
        with caplog.at_level(logging.WARNING):
            self._post([{"ok": True, "message": {"username": "Elementary"}}])

        assert "posted under the app's own name" in caplog.text
        assert "chat:write.customize" in caplog.text

    def test_applied_override_is_silent(self, caplog):
        with caplog.at_level(logging.WARNING):
            self._post([{"ok": True, "message": {"username": "dlt-saga"}}])

        assert "posted under the app" not in caplog.text

    def test_warns_once_not_per_message(self, caplog):
        """A scheduled digest would otherwise repeat this warning forever."""
        from dlt_saga.hooks.notifiers.slack import SlackNotifier
        from dlt_saga.project_config import SlackNotificationConfig

        notifier = SlackNotifier(
            SlackNotificationConfig(token="env_secret::T", channel="#c")
        )
        with (
            patch("dlt_saga.utility.secrets.resolve_secret", return_value="xoxb-x"),
            patch(
                "dlt_saga.hooks.notifiers.slack._slack_api_call",
                return_value={"ok": True, "message": {"username": "Elementary"}},
            ),
        ):
            with caplog.at_level(logging.WARNING):
                for _ in range(3):
                    notifier.on_run_complete(
                        _run_context([_Result("p", success=False, error="x")])
                    )

        assert caplog.text.count("posted under the app's own name") == 1

    def test_no_username_asked_for_means_nothing_to_report(self, caplog):
        with caplog.at_level(logging.WARNING):
            self._post(
                [{"ok": True, "message": {"username": "Elementary"}}], username=None
            )

        assert "posted under the app" not in caplog.text

    def test_unparseable_response_stays_quiet(self, caplog):
        """Without the echoed message there is nothing to compare — guessing
        would produce a warning on every send.
        """
        with caplog.at_level(logging.WARNING):
            self._post([{"ok": True}])

        assert "posted under the app" not in caplog.text
