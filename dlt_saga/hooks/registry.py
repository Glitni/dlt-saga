"""Lifecycle hook registry for dlt-saga pipeline execution.

Hooks are callables that receive a :class:`HookContext` and can be registered
for specific lifecycle events.  They are called synchronously in the worker
thread that executes the pipeline, so they must be thread-safe and should
not perform long-running blocking work.
"""

import logging
from dataclasses import dataclass
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

HOOK_EVENTS: List[str] = [ON_PIPELINE_START, ON_PIPELINE_COMPLETE, ON_PIPELINE_ERROR]

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


HookCallable = Callable[[HookContext], None]

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
            handler: Callable that accepts a single :class:`HookContext`
                argument.  Must be thread-safe.

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

    def fire(self, event: str, context: HookContext) -> None:
        """Call all handlers registered for *event*.

        Exceptions raised by individual handlers are caught and logged as
        warnings so that a failing hook never aborts pipeline execution.
        """
        for handler in self._hooks.get(event, []):
            try:
                handler(context)
            except Exception:
                name = getattr(handler, "__qualname__", repr(handler))
                logger.warning(
                    "Hook %s raised an exception (event=%s, pipeline=%s)",
                    name,
                    event,
                    context.pipeline_name,
                    exc_info=True,
                )

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
