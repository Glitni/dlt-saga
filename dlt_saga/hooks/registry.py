"""Lifecycle hook registry for dlt-saga pipeline execution.

Hooks are callables registered for a lifecycle event.  The per-pipeline events
pass a :class:`HookContext`; ``on_run_complete`` passes a :class:`RunContext`
covering the whole command invocation.  Per-pipeline handlers are called
synchronously in the worker thread that executes the pipeline, so they must be
thread-safe and should not perform long-running blocking work.
"""

import logging
from dataclasses import dataclass, field
from datetime import datetime
from typing import TYPE_CHECKING, Any, Callable, Dict, List, Optional

if TYPE_CHECKING:
    from dlt_saga.pipeline_config import PipelineConfig

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Event names
# ---------------------------------------------------------------------------

ON_PIPELINE_START = "on_pipeline_start"
ON_PIPELINE_COMPLETE = "on_pipeline_complete"
ON_PIPELINE_ERROR = "on_pipeline_error"
ON_RUN_COMPLETE = "on_run_complete"

HOOK_EVENTS: List[str] = [
    ON_PIPELINE_START,
    ON_PIPELINE_COMPLETE,
    ON_PIPELINE_ERROR,
    ON_RUN_COMPLETE,
]

# ---------------------------------------------------------------------------
# Context type
# ---------------------------------------------------------------------------


@dataclass
class HookContext:
    """Context passed to all hook callables.

    Attributes:
        pipeline_name: Fully-qualified pipeline name (e.g.
            ``google_sheets__budget``).
        config: The :class:`~dlt_saga.pipeline_config.PipelineConfig` for the
            pipeline being executed.
        command: Which command is running — ``"ingest"`` or ``"historize"``.
        result: Execution result on success.  For ``ingest`` this is the
            ``load_info`` value returned by
            :func:`~dlt_saga.pipelines.executor.execute_pipeline`.
            For ``historize`` this is the ``run_result`` dict from
            :class:`~dlt_saga.historize.runner.HistorizeRunner`.
            ``None`` for ``on_pipeline_start`` events and on error.
        error: The exception raised on failure.  ``None`` for
            ``on_pipeline_start`` and ``on_pipeline_complete`` events.
    """

    pipeline_name: str
    config: "PipelineConfig"
    command: str
    result: Optional[Any] = None
    error: Optional[Exception] = None


@dataclass
class RunContext:
    """Context passed to ``on_run_complete`` handlers.

    Fired once per command invocation — ``saga run`` fires one event covering
    both phases, not one per phase — which is what lets a notifier post a single
    digest rather than one message per pipeline.  A selection that matched
    nothing still fires, so "the scheduled run did nothing" is observable.

    Attributes:
        command: Which command ran — ``"ingest"``, ``"historize"`` or
            ``"run"``.
        select: Selector expressions the run was invoked with, or ``None`` for
            an unfiltered run.
        target: Profile target name the run executed against (e.g. ``"prod"``).
        environment: Environment of that target (e.g. ``"prod"``, ``"dev"``).
        results: Per-pipeline outcomes.  For a combined ``run`` a pipeline may
            appear twice, once per phase.
        started_at: When the command started (UTC).
        finished_at: When the command finished (UTC).
    """

    command: str
    select: Optional[List[str]] = None
    target: Optional[str] = None
    environment: Optional[str] = None
    results: List[Any] = field(default_factory=list)
    started_at: Optional[datetime] = None
    finished_at: Optional[datetime] = None

    @property
    def succeeded(self) -> int:
        """Number of pipeline results that succeeded."""
        return sum(1 for r in self.results if r.success)

    @property
    def failed(self) -> int:
        """Number of pipeline results that failed."""
        return sum(1 for r in self.results if not r.success)

    @property
    def failures(self) -> List[Any]:
        """Only the failed pipeline results."""
        return [r for r in self.results if not r.success]

    @property
    def has_failures(self) -> bool:
        """Whether any pipeline failed."""
        return any(not r.success for r in self.results)

    @property
    def duration_seconds(self) -> Optional[float]:
        """Wall-clock duration of the command, when both stamps are present."""
        if self.started_at is None or self.finished_at is None:
            return None
        return (self.finished_at - self.started_at).total_seconds()


# Handlers receive whichever context their event carries: HookContext for the
# per-pipeline events, RunContext for on_run_complete.
HookCallable = Callable[[Any], None]

# ---------------------------------------------------------------------------
# Registry
# ---------------------------------------------------------------------------


class HookRegistry:
    """Simple registry that maps lifecycle event names to lists of callables.

    The registry is not thread-safe for *registration* — hooks should be
    registered before parallel execution starts (e.g. during
    :class:`~dlt_saga.session.Session` initialisation).  The :meth:`fire`
    method is safe to call from multiple threads simultaneously because it
    only reads from the registry.
    """

    def __init__(self) -> None:
        self._hooks: Dict[str, List[HookCallable]] = {e: [] for e in HOOK_EVENTS}

    def register(self, event: str, handler: HookCallable) -> None:
        """Register *handler* for *event*.

        Args:
            event: One of :data:`HOOK_EVENTS`.
            handler: Callable that accepts a single context argument —
                :class:`HookContext` for the per-pipeline events,
                :class:`RunContext` for ``on_run_complete``.  Must be
                thread-safe.

        Raises:
            ValueError: If *event* is not a recognised lifecycle event.
        """
        if event not in self._hooks:
            raise ValueError(
                f"Unknown hook event {event!r}. Valid events: {HOOK_EVENTS}"
            )
        self._hooks[event].append(handler)
        name = getattr(handler, "__qualname__", repr(handler))
        logger.debug("Registered hook %s → %s", event, name)

    def fire(self, event: str, context: Any) -> None:
        """Call all handlers registered for *event*.

        Exceptions raised by individual handlers are caught and logged as
        warnings so that a failing hook never aborts pipeline execution.
        """
        for handler in self._hooks.get(event, []):
            try:
                handler(context)
            except Exception:
                name = getattr(handler, "__qualname__", repr(handler))
                # RunContext has no pipeline_name; fall back to the command so
                # the warning can never itself raise inside the handler.
                subject = getattr(context, "pipeline_name", None) or getattr(
                    context, "command", "?"
                )
                logger.warning(
                    "Hook %s raised an exception (event=%s, subject=%s)",
                    name,
                    event,
                    subject,
                    exc_info=True,
                )

    def has_handlers(self, event: str) -> bool:
        """Return ``True`` if anything is registered for *event*.

        Lets callers skip building an expensive context (and the work of
        gathering what goes in it) when nothing would receive it.
        """
        return bool(self._hooks.get(event))

    def clear(self) -> None:
        """Remove all registered hooks.  Intended for use in tests."""
        self._hooks = {e: [] for e in HOOK_EVENTS}

    def is_empty(self) -> bool:
        """Return ``True`` if no hooks are registered."""
        return all(len(v) == 0 for v in self._hooks.values())


# ---------------------------------------------------------------------------
# Global instance
# ---------------------------------------------------------------------------

_registry = HookRegistry()


def get_hook_registry() -> HookRegistry:
    """Return the process-wide hook registry."""
    return _registry
