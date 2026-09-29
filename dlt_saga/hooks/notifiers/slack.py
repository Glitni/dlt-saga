"""Slack notifier for run outcomes.

Posts one **digest per command invocation** rather than one message per
pipeline: a credential that broke overnight fails every pipeline in the
selection, and fifty separate messages bury the signal they were meant to
carry. Per-pipeline messages are available via ``per_pipeline: true`` for the
cases where a single pipeline warrants its own alert.

Delivery is via a Slack incoming webhook, which fixes the channel and cannot
resolve display names — mentions must therefore be raw Slack IDs
(``<@U012ABCDEF>``, ``<!subteam^S012ABCDEF>``). Routing to several channels
means several webhooks.

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
        self._sender = sender or _post_to_slack

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
        notifications = getattr(config, "config_dict", {}).get("notifications")
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

    def _send(self, payload: Dict[str, Any]) -> None:
        """Resolve the webhook and post, logging rather than raising on failure.

        The webhook is resolved per send rather than at registration: it is
        typically a secret URI, and resolving it eagerly would put a Secret
        Manager round trip (and its failure modes) into startup for every run,
        including the ones that never notify.
        """
        if not self.config.webhook_url:
            return
        try:
            from dlt_saga.utility.secrets import resolve_secret

            webhook = resolve_secret(self.config.webhook_url)
            if not webhook:
                logger.warning(
                    "Slack notification skipped: notifications.slack.webhook_url "
                    "resolved to an empty value"
                )
                return
            self._sender(str(webhook), payload, self.config.timeout_seconds)
            logger.debug("Slack notification sent")
        except Exception as exc:
            logger.warning("Slack notification failed: %s", exc, exc_info=True)


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
    if not config.webhook_url:
        logger.debug("Slack notifier not registered: no webhook_url configured")
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
