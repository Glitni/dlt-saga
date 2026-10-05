"""Slack notifier for run outcomes.

Posts one **digest per command invocation** rather than one message per
pipeline: a credential that broke overnight fails every pipeline in the
selection, and fifty separate messages bury the signal they were meant to
carry. Per-pipeline messages are available via ``per_pipeline: true`` for the
cases where a single pipeline warrants its own alert.

Two transports, mirroring Elementary's. A **bot token** posts through
``chat.postMessage`` and needs a ``channel``; it joins the channel on
``not_in_channel`` rather than failing, so alerting works from the first run.
An **incoming webhook** posts to a URL whose channel is fixed at creation, so
routing to several channels means several webhooks. ``token`` wins when both
are set.

Neither can resolve display names, so mentions must be raw Slack IDs
(``<@U012ABCDEF>``, ``<!subteam^S012ABCDEF>``).

Mentions use one key at two levels: ``notifications.slack.mentions`` in
*saga_project.yml* is appended once to a failure digest, and the same key on a
pipeline's own config is appended to that pipeline's line. Nothing is derived
from ``meta``, which stays free-form and uninterpreted as documented, and
nothing needs a lookup table — the tokens are written where they apply.

Failures to notify are logged and swallowed. An alerting channel that breaks a
data load would be worse than the missing alert, and the hook registry already
treats handler exceptions as non-fatal — this module is explicit about it so
the warning names Slack rather than surfacing as a generic hook failure.
"""

import logging
from typing import TYPE_CHECKING, Any, Callable, Dict, List, Optional

if TYPE_CHECKING:
    from dlt_saga.hooks.registry import HookContext, HookRegistry, RunContext
    from dlt_saga.notify import SweepContext
    from dlt_saga.project_config import SlackNotificationConfig

logger = logging.getLogger(__name__)

# Slack renders long messages poorly and a digest is meant to be scannable, so
# the failure list is capped and the remainder summarised.
_MAX_LISTED_FAILURES = 10
# Slack's own per-block text limit is 3000 characters; individual error
# messages are truncated well below it so one verbose traceback can't crowd out
# the other failures in the digest.
_MAX_ERROR_CHARS = 300

_RETRY_STATUS = frozenset({429, 500, 502, 503, 504})
_MAX_ATTEMPTS = 3


def _truncate(text: str, limit: int) -> str:
    """Shorten *text* to *limit* characters with an ellipsis."""
    text = " ".join(str(text).split())
    if len(text) <= limit:
        return text
    return text[: limit - 1] + "…"


def _post_to_slack(webhook_url: str, payload: Dict[str, Any], timeout: float) -> None:
    """POST *payload* to a Slack incoming webhook, retrying transient failures.

    Retries connection errors and the status codes Slack uses for rate limiting
    and backend trouble; a 4xx other than 429 means the payload or webhook is
    wrong and retrying would only repeat it.

    Args:
        webhook_url: Resolved Slack incoming-webhook URL.
        payload: JSON body to post.
        timeout: Per-request timeout in seconds.

    Raises:
        RuntimeError: If every attempt failed.
    """
    import time

    import requests

    last_error: Optional[str] = None
    for attempt in range(1, _MAX_ATTEMPTS + 1):
        try:
            response = requests.post(webhook_url, json=payload, timeout=timeout)
            if response.status_code == 200:
                return
            last_error = f"HTTP {response.status_code}: {_truncate(response.text, 200)}"
            if response.status_code not in _RETRY_STATUS:
                break
        except requests.RequestException as exc:
            last_error = f"{type(exc).__name__}: {exc}"

        if attempt < _MAX_ATTEMPTS:
            time.sleep(2 ** (attempt - 1))

    raise RuntimeError(f"Slack webhook post failed: {last_error}")


_SLACK_API = "https://slack.com/api"
# Slack Web API errors that another attempt will not fix.
_FATAL_API_ERRORS = frozenset(
    {"invalid_auth", "account_inactive", "token_revoked", "not_authed"}
)


def _slack_api_call(
    method: str, token: str, payload: Dict[str, Any], timeout: float
) -> Dict[str, Any]:
    """Call one Slack Web API method and return its parsed body.

    The Web API answers **HTTP 200 even on failure**, carrying the outcome in
    ``ok``/``error`` instead — unlike an incoming webhook, where the status code
    is the answer. Checking only the status here would swallow every API error
    silently, which is the opposite of what a notifier is for.
    """
    import requests

    response = requests.post(
        f"{_SLACK_API}/{method}",
        json=payload,
        headers={"Authorization": f"Bearer {token}"},
        timeout=timeout,
    )
    try:
        return dict(response.json())
    except ValueError:
        return {"ok": False, "error": f"HTTP {response.status_code}: non-JSON reply"}


# Returned when the app may not override its display name or icon. Degrading to
# the app's own identity is far better than not posting at all.
_CUSTOMIZE_ERRORS = frozenset({"missing_scope", "not_allowed_token_type"})


def _post_via_token(
    token: str,
    channel: str,
    payload: Dict[str, Any],
    timeout: float,
    on_identity_ignored: Optional[Callable[[], None]] = None,
) -> None:
    """Post through ``chat.postMessage``, handling the errors Elementary does.

    ``not_in_channel`` means the app simply has not joined yet, so it joins and
    retries once — the same recovery Elementary performs, and the difference
    between "alerting silently never worked" and "it worked from the first run".

    Args:
        token: Slack bot token.
        channel: Channel name (``#alerts``) or ID.
        payload: Message body; ``channel`` is filled in here.
        timeout: Per-request timeout in seconds.

    Raises:
        RuntimeError: If the message could not be posted.
    """
    import time

    body = dict(payload, channel=channel)
    last_error: Optional[str] = None

    for attempt in range(1, _MAX_ATTEMPTS + 1):
        result = _slack_api_call("chat.postMessage", token, body, timeout)
        if result.get("ok"):
            _check_identity_applied(body, result, on_identity_ignored)
            return

        last_error = str(result.get("error") or "unknown_error")
        if last_error in _CUSTOMIZE_ERRORS and (
            "username" in body or "icon_emoji" in body
        ):
            # The app lacks chat:write.customize. Post under its own name
            # rather than staying silent — the message still says it is saga's.
            logger.warning(
                "Slack app may not set a custom username/icon (%s); posting "
                "under the app's own identity. Grant chat:write.customize to "
                "label these as dlt-saga.",
                last_error,
            )
            body.pop("username", None)
            body.pop("icon_emoji", None)
            continue
        if last_error == "not_in_channel":
            if _join_channel(token, channel, timeout):
                continue
            raise RuntimeError(
                f"Slack app is not in channel {channel!r} and could not join it. "
                "Invite the app to the channel, or grant chat:write.public."
            )
        if last_error == "channel_not_found":
            raise RuntimeError(
                f"Slack channel {channel!r} was not found. Check the name, and "
                "that the app is installed in this workspace."
            )
        if last_error in _FATAL_API_ERRORS:
            raise RuntimeError(f"Slack rejected the token: {last_error}")
        if attempt < _MAX_ATTEMPTS:
            time.sleep(2 ** (attempt - 1))

    raise RuntimeError(f"Slack chat.postMessage failed: {last_error}")


def _check_identity_applied(
    body: Dict[str, Any],
    result: Dict[str, Any],
    on_ignored: Optional[Callable[[], None]],
) -> None:
    """Notice when Slack accepted the post but dropped the identity override.

    An app without ``chat:write.customize`` does not get an error — Slack posts
    the message, silently under the app's own name, and reports success. Without
    this check a project would configure ``username`` and never learn it had no
    effect, which is precisely the kind of quiet nothing this notifier exists to
    remove.

    The posted message is echoed back, so the asked-for name can be compared
    against the one actually used.
    """
    wanted = body.get("username")
    if not wanted or on_ignored is None:
        return
    message = result.get("message")
    if not isinstance(message, dict):
        return  # Nothing to compare against; stay quiet rather than guess.
    if message.get("username") != wanted:
        on_ignored()


def _join_channel(token: str, channel: str, timeout: float) -> bool:
    """Join *channel* so a subsequent post can succeed. Public channels only."""
    result = _slack_api_call(
        "conversations.join", token, {"channel": channel.lstrip("#")}, timeout
    )
    if result.get("ok"):
        logger.info("Joined Slack channel %s", channel)
        return True
    logger.debug("Could not join %s: %s", channel, result.get("error"))
    return False


# Attachment colours. The left bar is the fastest signal in a busy channel —
# it reads before any text does.
_COLOUR_FAILING = "#d7263d"
_COLOUR_RECOVERED = "#e0a800"
_COLOUR_OK = "#2eb67d"

# Slack truncates a text object at 3000 characters and rejects the message at
# 50 blocks. Errors are already capped at _MAX_ERROR_CHARS; this guards the
# combined section.
_MAX_SECTION_CHARS = 2900


def _s(items: Any) -> str:
    """'s' unless there is exactly one — '1 pipelines' reads as a bug."""
    return "" if len(items) == 1 else "s"


def _header(text: str) -> Dict[str, Any]:
    """A header block. Slack renders plain_text only — no mrkdwn, 150 char cap."""
    return {
        "type": "header",
        "text": {"type": "plain_text", "text": _truncate(text, 150), "emoji": True},
    }


def _context(text: str) -> Dict[str, Any]:
    """A small-print line under the header: who sent this, and over what."""
    return {"type": "context", "elements": [{"type": "mrkdwn", "text": text}]}


def _clip(text: str, limit: int) -> str:
    """Shorten *text* without touching its line breaks.

    Distinct from :func:`_truncate`, which collapses all whitespace to one line
    — right for an error summary inside a bullet, destructive for a section
    whose layout *is* its line breaks.
    """
    return text if len(text) <= limit else text[: limit - 1] + "…"


def _section(text: str) -> Dict[str, Any]:
    return {
        "type": "section",
        "text": {"type": "mrkdwn", "text": _clip(text, _MAX_SECTION_CHARS)},
    }


def _code(text: str) -> str:
    """Fence *text* so an error renders as a block rather than reflowed prose."""
    return "```" + text.replace("```", "`​``") + "```"


class SlackNotifier:
    """Posts run outcomes to a Slack incoming webhook.

    Args:
        config: The parsed ``notifications.slack`` block.
        sender: Injection point for the HTTP call, taking
            ``(webhook_url, payload, timeout)``. Defaults to
            :func:`_post_to_slack`; tests substitute a recorder.
    """

    def __init__(
        self,
        config: "SlackNotificationConfig",
        sender: Optional[Callable[[str, Dict[str, Any], float], None]] = None,
    ) -> None:
        self.config = config
        # `sender` injects the webhook transport only; the token transport is
        # chosen inside _send, which tests substitute at the API-call level.
        self._sender = sender or _post_to_slack
        self._identity_warned = False

    # ------------------------------------------------------------------
    # Hook entry points
    # ------------------------------------------------------------------

    def on_run_complete(self, ctx: "RunContext") -> None:
        """Post the run digest, if this run's outcome warrants one."""
        if self.config.notify_on == "failure" and not ctx.has_failures:
            logger.debug("Slack digest skipped: run had no failures")
            return
        self._send(self._build_run_payload(ctx))

    def on_pipeline_error(self, ctx: "HookContext") -> None:
        """Post a message for one failed pipeline (``per_pipeline: true``)."""
        if not self.config.per_pipeline:
            return
        self._send(self._build_pipeline_payload(ctx))

    # ------------------------------------------------------------------
    # Message construction
    # ------------------------------------------------------------------

    def _build_run_payload(self, ctx: "RunContext") -> Dict[str, Any]:
        """Build the Slack payload for a completed run."""
        total = len(ctx.results)
        if ctx.has_failures:
            headline = (
                f":x: saga {ctx.command} failed — {ctx.failed} of {total} pipeline(s)"
            )
        elif total == 0:
            # Worth saying out loud: a scheduled run whose selection silently
            # stopped matching looks identical to a healthy no-op.
            headline = f":warning: saga {ctx.command} selected no pipelines"
        else:
            headline = (
                f":white_check_mark: saga {ctx.command} complete — "
                f"{ctx.succeeded} pipeline(s)"
            )

        lines = [headline, self._context_line(ctx)]

        if ctx.has_failures:
            lines.append("")
            for result in ctx.failures[:_MAX_LISTED_FAILURES]:
                lines.append(self._failure_line(result))
            remaining = ctx.failed - _MAX_LISTED_FAILURES
            if remaining > 0:
                lines.append(f"…and {remaining} more")

            mentions = " ".join(self.config.mentions or [])
            if mentions:
                lines.append("")
                lines.append(mentions)

        return {"text": "\n".join(line for line in lines if line is not None)}

    def on_sweep_complete(self, ctx: "SweepContext") -> bool:
        """Post the digest for a ``saga notify`` sweep.

        Unlike the per-run digest this is driven by recorded state rather than a
        finished run, so it can describe several executions at once — including
        ones that ran in containers this process never saw.
        """
        if not ctx.has_anything_to_report:
            logger.debug("Sweep digest skipped: nothing failed")
            return True
        return self._send(self.build_sweep_payload(ctx))

    def build_sweep_payload(self, ctx: "SweepContext") -> Dict[str, Any]:
        """Build the Slack message for a sweep across several executions.

        Public so a dry run can show exactly what would be posted.

        Structured rather than a block of text: a header states the state, a
        context line attributes the message to saga and names the scope, and a
        coloured attachment carries the detail — so the left bar and the first
        line answer "is this mine, and is it bad" before anything is read.

        Pipelines lead rather than executions: the reader's first question is
        "what is broken", not "which run broke". Persistence is kept, because
        *failed every attempt* and *failed once* are the difference between an
        outage and a blip.
        """
        failing, recovered = ctx.failing, ctx.recovered
        runs = len(ctx.execution_ids)
        window = f"{runs} {'run' if runs == 1 else 'runs'} · last {ctx.since_days}d"

        if failing:
            headline = f"{len(failing)} pipeline{_s(failing)} failing"
            colour = _COLOUR_FAILING
        else:
            headline = f"{len(recovered)} pipeline{_s(recovered)} failed and recovered"
            colour = _COLOUR_RECOVERED

        scope = " · ".join(
            part for part in (ctx.environment, ctx.command, window) if part
        )
        blocks: List[Dict[str, Any]] = [
            _header(headline),
            _context(f"*dlt-saga* · {scope}"),
        ]

        detail: List[Dict[str, Any]] = []
        if failing:
            detail.append(_section("*Failing*"))
            detail.extend(self._outcome_sections(failing))
        if recovered:
            detail.append(_section("*Recovered*"))
            detail.append(_section(self._recovered_text(recovered)))

        footer = []
        if ctx.total_attempts:
            succeeded = ctx.total_attempts - ctx.failed_attempts
            footer.append(
                f"{succeeded} of {ctx.total_attempts} pipeline attempts succeeded"
            )
        report = self._report_link()
        if report:
            footer.append(report)
        if footer:
            detail.append(_context("  ·  ".join(footer)))

        mentions = " ".join(self.config.mentions or []) if failing else ""
        if mentions:
            detail.append(_section(mentions))

        return {
            # Fallback text: what Slack shows in notifications and sidebars,
            # where blocks are not rendered at all.
            "text": f"dlt-saga: {headline} ({scope})",
            "blocks": blocks,
            "attachments": [{"color": colour, "blocks": detail}],
        }

    def _outcome_sections(self, failing: list) -> List[Dict[str, Any]]:
        """One section per failing pipeline: what it is, then why.

        The error goes in a fenced block rather than inline — a BigQuery or HTTP
        error reflowed as prose is the part that becomes unreadable first, and
        it is the part you actually need.
        """
        sections: List[Dict[str, Any]] = []
        for outcome in failing[:_MAX_LISTED_FAILURES]:
            mentions = self._pipeline_mentions_from_config(outcome.config_dict)
            suffix = f"  {mentions}" if mentions else ""
            error = _truncate(outcome.latest_error or "failed", _MAX_ERROR_CHARS)
            sections.append(
                _section(
                    f"*`{outcome.pipeline_name}`* — {self._persistence(outcome)}"
                    f"{suffix}\n{_code(error)}"
                )
            )
        remaining = len(failing) - _MAX_LISTED_FAILURES
        if remaining > 0:
            sections.append(_context(f"…and {remaining} more"))
        return sections

    @staticmethod
    def _persistence(outcome: Any) -> str:
        """How persistent a failure is — an outage reads differently to a blip."""
        if outcome.attempts > 1:
            return f"failed {outcome.failures} of {outcome.attempts} attempts"
        return "failed"

    def _recovered_text(self, recovered: list) -> str:
        """Pipelines that failed in the window but have since passed.

        One section with a bullet per pipeline: the attempt counts are what say
        whether something is flapping, and they are the first thing lost when
        this is collapsed into a comma-joined line.
        """
        lines = [
            f"• `{o.pipeline_name}` — {o.failures} of {o.attempts} attempts failed, "
            "latest OK"
            for o in recovered[:_MAX_LISTED_FAILURES]
        ]
        remaining = len(recovered) - _MAX_LISTED_FAILURES
        if remaining > 0:
            lines.append(f"…and {remaining} more")
        return "\n".join(lines)

    def _build_pipeline_payload(self, ctx: "HookContext") -> Dict[str, Any]:
        """Build the Slack payload for a single failed pipeline."""
        error = _truncate(str(ctx.error), _MAX_ERROR_CHARS) if ctx.error else "failed"
        mentions = self._pipeline_mentions(getattr(ctx, "config", None))
        line = f":x: `{ctx.pipeline_name}` ({ctx.command}) — {error}"
        if mentions:
            line = f"{line} {mentions}"
        return {"text": line}

    def _context_line(self, ctx: "RunContext") -> str:
        """Build the target / selection / duration line under the headline."""
        parts = []
        if ctx.target:
            env = f"/{ctx.environment}" if ctx.environment else ""
            parts.append(f"target `{ctx.target}{env}`")
        elif ctx.environment:
            parts.append(f"environment `{ctx.environment}`")
        if ctx.select:
            parts.append(f"select `{' '.join(ctx.select)}`")
        duration = ctx.duration_seconds
        if duration is not None:
            parts.append(f"{duration:.1f}s")
        return " · ".join(parts)

    def _failure_line(self, result: Any) -> str:
        """Build one bullet describing a failed pipeline."""
        error = _truncate(result.error or "failed", _MAX_ERROR_CHARS)
        line = f"• `{result.pipeline_name}` — {error}"
        mentions = self._pipeline_mentions(getattr(result, "config", None))
        if mentions:
            line = f"{line} {mentions}"
        return line

    @staticmethod
    def _pipeline_mentions(config: Any) -> str:
        """Return a pipeline's own mention tokens, joined, or an empty string.

        Reads ``notifications.slack.mentions`` from the pipeline config — the
        same key the project-level block uses — so ownership of an alert is
        declared where the pipeline is declared. Malformed values are ignored
        rather than raised on: a notifier must not fail the run it reports on,
        and ``saga validate`` is where a bad config should surface.
        """
        if config is None:
            return ""
        return SlackNotifier._pipeline_mentions_from_config(
            getattr(config, "config_dict", {})
        )

    @staticmethod
    def _pipeline_mentions_from_config(config_dict: Any) -> str:
        """Resolve ``notifications.slack.mentions`` out of a raw config dict.

        Shared by the hook path (which holds a PipelineConfig) and the sweep
        (which holds the config stored on the execution-plan row), so both reach
        the same key by the same rules.
        """
        if not isinstance(config_dict, dict):
            return ""
        notifications = config_dict.get("notifications")
        if not isinstance(notifications, dict):
            return ""
        slack = notifications.get("slack")
        if not isinstance(slack, dict):
            return ""
        mentions = slack.get("mentions")
        if isinstance(mentions, str):
            mentions = [mentions]
        if not isinstance(mentions, list):
            return ""
        return " ".join(m for m in mentions if isinstance(m, str) and m)

    # ------------------------------------------------------------------
    # Delivery
    # ------------------------------------------------------------------

    def _send(self, payload: Dict[str, Any]) -> bool:
        """Resolve the credential and post.

        Returns ``True`` only when the message actually went out. Callers that
        claim state on the strength of a send — ``saga notify`` marks executions
        as reported — must not do so on a failure, or an outage turns one
        undelivered digest into one permanently lost.

        Logs rather than raises: an alerting channel that breaks must not break
        the caller.

        The webhook is resolved per send rather than at registration: it is
        typically a secret URI, and resolving it eagerly would put a Secret
        Manager round trip (and its failure modes) into startup for every run,
        including the ones that never notify.
        """
        try:
            from dlt_saga.utility.secrets import resolve_secret

            # Token wins over webhook when both are set, as in Elementary.
            if self.config.token:
                token = resolve_secret(self.config.token)
                if not self._usable(token, "token"):
                    return False
                _post_via_token(
                    str(token),
                    str(self.config.channel),
                    self._with_identity(payload),
                    self.config.timeout_seconds,
                    on_identity_ignored=self._warn_identity_ignored,
                )
            elif self.config.webhook_url:
                webhook = resolve_secret(self.config.webhook_url)
                if not self._usable(webhook, "webhook_url"):
                    return False
                self._sender(
                    str(webhook),
                    self._with_identity(payload),
                    self.config.timeout_seconds,
                )
            else:
                return False
            logger.debug("Slack notification sent")
            return True
        except Exception as exc:
            logger.warning("Slack notification failed: %s", exc, exc_info=True)
            return False

    def _warn_identity_ignored(self) -> None:
        """Say once that the display name was dropped.

        Once, not per message: a digest is already a scheduled, repeating thing,
        and a warning on every send would be the noise people learn to skip.
        """
        if self._identity_warned:
            return
        self._identity_warned = True
        logger.warning(
            "Slack posted under the app's own name, not %r — the app lacks the "
            "chat:write.customize scope. Grant it to label these as %s, or set "
            "notifications.slack.username to null to stop asking.",
            self.config.username,
            self.config.username,
        )

    def _report_link(self) -> str:
        """Render the configured report URL as Slack mrkdwn.

        Labelled here rather than in config: saga knows what this URL *is*, so
        every project's digest can say the same thing about it.

        The URL has to be configured — saga only ever sees the ``gs://`` URI
        passed to ``saga report --output``, which is not browsable, and how that
        bucket maps to a public URL belongs to the deployment, not to saga.
        """
        url = self.config.report_url
        return f"<{url}|saga report>" if url else ""

    def _with_identity(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        """Label the message as saga's.

        Matters most when posting through an app installed for something else:
        without this a saga digest arrives under that app's name and icon, and
        reads as its alert rather than ours.
        """
        identity = {
            k: v
            for k, v in (
                ("username", self.config.username),
                ("icon_emoji", self.config.icon_emoji),
            )
            if v
        }
        return {**payload, **identity} if identity else payload

    @staticmethod
    def _usable(value: Any, key: str) -> bool:
        """Warn and refuse when a credential resolved to nothing.

        A secret URI that resolves empty would otherwise fail deep inside the
        HTTP call with a confusing message, or — for a webhook — post nowhere.
        """
        if value:
            return True
        logger.warning(
            "Slack notification skipped: notifications.slack.%s resolved to an "
            "empty value",
            key,
        )
        return False


def register_slack_notifier(
    config: "SlackNotificationConfig",
    registry: "HookRegistry",
    sender: Optional[Callable[[str, Dict[str, Any], float], None]] = None,
) -> Optional[SlackNotifier]:
    """Register a :class:`SlackNotifier` for the events its config enables.

    Args:
        config: Parsed ``notifications.slack`` block.
        registry: Registry to register into.
        sender: Optional HTTP sender override (tests).

    Returns:
        The registered notifier, or ``None`` when no ``webhook_url`` is set.
    """
    if not (config.token or config.webhook_url):
        logger.debug(
            "Slack notifier not registered: neither token nor webhook_url configured"
        )
        return None

    from dlt_saga.hooks.registry import ON_PIPELINE_ERROR, ON_RUN_COMPLETE

    notifier = SlackNotifier(config, sender=sender)
    registry.register(ON_RUN_COMPLETE, notifier.on_run_complete)
    events: List[str] = ["on_run_complete"]
    if config.per_pipeline:
        registry.register(ON_PIPELINE_ERROR, notifier.on_pipeline_error)
        events.append("on_pipeline_error")
    logger.debug("Registered Slack notifier for %s", ", ".join(events))
    return notifier
