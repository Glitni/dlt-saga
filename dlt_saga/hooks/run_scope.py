"""Coalesce several session calls into one ``on_run_complete`` event.

``saga run`` is one invocation to the user but two calls to the session layer —
``Session.ingest`` followed by ``Session.historize`` (see the ``run`` command in
``cli.py``, which needs the phases separate to confirm ``--full-refresh`` per
phase). Firing per call would post two digests for one command, which is the
flooding the digest exists to avoid.

A scope makes the boundary explicit: contributions inside it accumulate, and a
single event fires when it closes, carrying every phase's results. Outside a
scope — the programmatic API, where one ``Session`` call *is* the invocation —
events fire immediately and nothing changes.

The scope is a :class:`~contextvars.ContextVar`, so it follows the calling task
rather than leaking across threads; worker threads never open one.
"""

import logging
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Iterator, List, Optional

logger = logging.getLogger(__name__)


@dataclass
class _RunScope:
    """Accumulator for one user-facing command invocation."""

    command: str
    select: Optional[List[str]] = None
    target: Optional[str] = None
    environment: Optional[str] = None
    started_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    results: List[Any] = field(default_factory=list)


_active_scope: ContextVar[Optional[_RunScope]] = ContextVar(
    "saga_run_notification_scope", default=None
)


def get_active_scope() -> Optional[_RunScope]:
    """Return the scope in effect, or ``None`` when events fire immediately."""
    return _active_scope.get()


def contribute(
    results: List[Any],
    *,
    target: Optional[str] = None,
    environment: Optional[str] = None,
) -> bool:
    """Add one phase's results to the active scope.

    Args:
        results: Per-pipeline results from the phase that just finished.
        target: Profile target name, recorded if the scope lacks one.
        environment: Environment name, recorded if the scope lacks one.

    Returns:
        ``True`` if a scope absorbed the results (so the caller must not fire
        its own event), ``False`` when there is no scope.
    """
    scope = _active_scope.get()
    if scope is None:
        return False
    scope.results.extend(results)
    if scope.target is None:
        scope.target = target
    if scope.environment is None:
        scope.environment = environment
    return True


@contextmanager
def run_notification_scope(
    command: str,
    select: Optional[List[str]] = None,
    target: Optional[str] = None,
    environment: Optional[str] = None,
) -> Iterator[None]:
    """Coalesce every phase inside the block into one ``on_run_complete`` event.

    The event fires once on exit, including when the block raises — a command
    that died halfway through is exactly when a notification matters — and
    including when no phase contributed anything, so a selection that stopped
    matching is still reported.

    Args:
        command: Command name reported to handlers (e.g. ``"run"``).
        select: Selector expressions the command was invoked with.
        target: Profile target name.
        environment: Environment of that target.
    """
    scope = _RunScope(
        command=command, select=select, target=target, environment=environment
    )
    token = _active_scope.set(scope)
    try:
        yield
    finally:
        _active_scope.reset(token)
        _fire(scope)


def _fire(scope: _RunScope) -> None:
    """Fire ``on_run_complete`` for a closed scope.  Never raises."""
    try:
        from dlt_saga.hooks.registry import (
            ON_RUN_COMPLETE,
            RunContext,
            get_hook_registry,
        )

        registry = get_hook_registry()
        if not registry.has_handlers(ON_RUN_COMPLETE):
            return
        registry.fire(
            ON_RUN_COMPLETE,
            RunContext(
                command=scope.command,
                select=scope.select,
                target=scope.target,
                environment=scope.environment,
                results=scope.results,
                started_at=scope.started_at,
                finished_at=datetime.now(timezone.utc),
            ),
        )
    except Exception:
        logger.warning("Failed to fire on_run_complete hooks", exc_info=True)
