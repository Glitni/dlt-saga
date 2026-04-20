"""Lifecycle hook system for dlt-saga pipeline execution.

Hooks allow reacting to pipeline lifecycle events without subclassing
:class:`~dlt_saga.pipelines.base_pipeline.BasePipeline`.

Quick start — define a hook function::

    # mypackage/hooks.py
    from dlt_saga.hooks import HookContext

    def on_failure(ctx: HookContext) -> None:
        send_alert(f"Pipeline {ctx.pipeline_name} failed: {ctx.error}")

    on_failure.saga_hook_events = ["on_pipeline_error"]

Register via *saga_project.yml*::

    hooks:
      on_pipeline_error:
        - mypackage.hooks:on_failure

Or via *pyproject.toml* entry points (for installed packages)::

    [project.entry-points."dlt_saga.hooks"]
    my_hook = "mypackage.hooks:on_failure"

Available events:

- ``on_pipeline_start`` — fired before each pipeline starts.
- ``on_pipeline_complete`` — fired after a pipeline finishes successfully.
- ``on_pipeline_error`` — fired when a pipeline raises an exception or
  the historize runner reports a non-completed status.

Hook functions receive a single :class:`HookContext` argument.  Exceptions
raised inside hooks are caught and logged as warnings so that a failing hook
never aborts pipeline execution.
"""

from .loader import load_hooks, load_hooks_from_config, load_hooks_from_entry_points
from .registry import (
    HOOK_EVENTS,
    ON_PIPELINE_COMPLETE,
    ON_PIPELINE_ERROR,
    ON_PIPELINE_START,
    HookCallable,
    HookContext,
    HookRegistry,
    get_hook_registry,
)

__all__ = [
    "HOOK_EVENTS",
    "ON_PIPELINE_START",
    "ON_PIPELINE_COMPLETE",
    "ON_PIPELINE_ERROR",
    "HookCallable",
    "HookContext",
    "HookRegistry",
    "get_hook_registry",
    "load_hooks",
    "load_hooks_from_config",
    "load_hooks_from_entry_points",
]
