"""Load lifecycle hooks from saga_project.yml and installed entry points."""

import importlib
import logging
from typing import Dict, List, Optional

from .registry import HOOK_EVENTS, HookCallable, HookRegistry, get_hook_registry

logger = logging.getLogger(__name__)

_loaded = False


def _resolve_callable(ref: str) -> HookCallable:
    """Resolve a ``'module:attribute'`` string to a Python callable."""
    if ":" not in ref:
        raise ValueError(
            f"Hook reference {ref!r} must be in 'module:callable' format "
            "(e.g. 'mypackage.hooks:notify_failure')"
        )
    module_path, attr = ref.rsplit(":", 1)
    try:
        module = importlib.import_module(module_path)
    except ImportError as exc:
        raise ImportError(
            f"Could not import hook module '{module_path}': {exc}"
        ) from exc
    obj = getattr(module, attr, None)
    if obj is None:
        raise AttributeError(f"Module '{module_path}' has no attribute '{attr}'")
    if not callable(obj):
        raise TypeError(
            f"Hook reference {ref!r} resolved to a non-callable: {type(obj)}"
        )
    return obj  # type: ignore[return-value]


def load_hooks_from_config(
    hooks_config: Dict[str, List[str]],
    registry: Optional[HookRegistry] = None,
) -> None:
    """Load hooks from the ``hooks:`` section of *saga_project.yml*.

    Expected shape::

        hooks:
          on_pipeline_start:
            - mypackage.hooks:notify_start
          on_pipeline_complete:
            - mypackage.hooks:notify_complete
          on_pipeline_error:
            - mypackage.hooks:notify_error

    Args:
        hooks_config: Parsed ``hooks:`` dict from saga_project.yml.
        registry: Registry to register into.  Defaults to the global registry.
    """
    if registry is None:
        registry = get_hook_registry()
    for event, refs in hooks_config.items():
        if event not in HOOK_EVENTS:
            logger.warning(
                "Unknown hook event '%s' in saga_project.yml; valid events: %s",
                event,
                HOOK_EVENTS,
            )
            continue
        for ref in refs:
            try:
                fn = _resolve_callable(ref)
                registry.register(event, fn)
            except Exception:
                logger.error(
                    "Failed to load hook '%s' for event '%s'",
                    ref,
                    event,
                    exc_info=True,
                )


def load_hooks_from_entry_points(
    registry: Optional[HookRegistry] = None,
) -> None:
    """Load hook callables registered via the ``dlt_saga.hooks`` entry point group.

    Each entry point should expose a callable that accepts a
    :class:`~dlt_saga.hooks.registry.HookContext`.  The callable's optional
    ``saga_hook_events`` attribute controls which events it is registered for.
    If absent, the callable is registered for **all** events::

        # mypackage/hooks.py
        from dlt_saga.hooks import HookContext

        def on_failure(ctx: HookContext) -> None:
            send_slack_alert(ctx.pipeline_name, ctx.error)

        on_failure.saga_hook_events = ["on_pipeline_error"]

    In *pyproject.toml*::

        [project.entry-points."dlt_saga.hooks"]
        slack_notifier = "mypackage.hooks:on_failure"

    Args:
        registry: Registry to register into.  Defaults to the global registry.
    """
    if registry is None:
        registry = get_hook_registry()
    try:
        from importlib.metadata import entry_points

        eps = entry_points(group="dlt_saga.hooks")
    except Exception:
        logger.debug("Could not load entry points for dlt_saga.hooks", exc_info=True)
        return
    for ep in eps:
        try:
            fn = ep.load()
        except Exception:
            logger.error("Failed to load hook entry point '%s'", ep.name, exc_info=True)
            continue
        if not callable(fn):
            logger.warning(
                "Entry point '%s' in dlt_saga.hooks is not callable; skipping",
                ep.name,
            )
            continue
        events = getattr(fn, "saga_hook_events", HOOK_EVENTS)
        for event in events:
            try:
                registry.register(event, fn)
            except ValueError:
                logger.warning(
                    "Entry point '%s' declares unknown event '%s'; skipping",
                    ep.name,
                    event,
                )
        logger.debug("Loaded hook from entry point '%s'", ep.name)


def load_hooks(registry: Optional[HookRegistry] = None) -> None:
    """Load hooks from all sources: saga_project.yml and installed entry points.

    Idempotent — subsequent calls are no-ops unless :func:`_reset_loaded` is
    called first (e.g. in tests).

    Args:
        registry: Registry to register into.  Defaults to the global registry.
    """
    global _loaded
    if _loaded:
        return
    _loaded = True

    from dlt_saga.project_config import get_project_config

    if registry is None:
        registry = get_hook_registry()

    project_config = get_project_config()
    if project_config.hooks:
        load_hooks_from_config(project_config.hooks, registry)

    load_hooks_from_entry_points(registry)


def _reset_loaded() -> None:
    """Reset the loaded flag so :func:`load_hooks` runs again.  For tests only."""
    global _loaded
    _loaded = False
