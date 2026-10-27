"""Orchestrator and worker execution modes for the ``saga`` CLI.

Local runs go through :class:`dlt_saga.session.Session`. Distributed runs use
two additional process-lifecycle entrypoints that live here rather than in
``Session`` because they are coupled to CLI/container concerns (``typer.Exit``
exit codes, Cloud Run task-index env vars, execution-plan status writes):

- :func:`run_orchestrator_mode` — build an execution plan and trigger workers
  via the configured :class:`OrchestrationProvider`.
- :func:`run_worker_mode` — read a task assignment from the plan and execute it,
  recording task status back to the plan.

Extracted from ``cli.py`` (module-boundary cleanup); behavior unchanged.
"""

import logging
import os
import signal
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from functools import partial
from typing import TYPE_CHECKING, Any, Callable, Dict, List, Optional, Set, Tuple

import typer

from dlt_saga.pipeline_config import PipelineConfig
from dlt_saga.utility.cli.common import get_config_source
from dlt_saga.utility.cli.context import get_execution_context
from dlt_saga.utility.cli.reporting import summarize_load_info
from dlt_saga.utility.collisions import TargetCollisionError, check_target_collisions
from dlt_saga.utility.env import get_env
from dlt_saga.utility.naming import get_environment
from dlt_saga.utility.secrets.redaction import redact
from dlt_saga.utility.task_groups import (
    TaskConcurrencyError,
    check_task_concurrency,
    parallel_slots,
    submit_with_group_caps,
)

if TYPE_CHECKING:
    from dlt_saga.utility.orchestration.providers import OrchestrationProvider

logger = logging.getLogger(__name__)

# Default cap on per-worker in-task parallelism. Keeps memory predictable
# when a large ``task_group`` would otherwise fan out to as many threads as
# there are pipelines (see #82).
_DEFAULT_WORKER_CONCURRENCY = 4


def _resolve_worker_concurrency(cli_override: Optional[int] = None) -> int:
    """Resolve the worker concurrency cap.

    Precedence (highest first):
      1. ``cli_override`` (passed by the CLI when ``--workers`` is set).
      2. ``SAGA_WORKER_CONCURRENCY`` env var (set by the orchestrator on
         each Cloud Run task — this is how the orchestrator's ``--workers``
         reaches the worker).
      3. ``saga_project.yml: orchestration.worker_concurrency``.
      4. ``_DEFAULT_WORKER_CONCURRENCY``.
    """
    if cli_override is not None:
        return max(1, cli_override)

    env_value = get_env("SAGA_WORKER_CONCURRENCY")
    if env_value:
        try:
            parsed = int(env_value)
            if parsed >= 1:
                return parsed
            logger.warning(
                "Ignoring SAGA_WORKER_CONCURRENCY=%s (must be >= 1)", env_value
            )
        except ValueError:
            logger.warning(
                "Ignoring SAGA_WORKER_CONCURRENCY=%s (not an integer)", env_value
            )

    from dlt_saga.project_config import get_orchestration_config

    project_value = get_orchestration_config().worker_concurrency
    if project_value is not None:
        return project_value

    return _DEFAULT_WORKER_CONCURRENCY


# ---------------------------------------------------------------------------
# Destination preparation
# ---------------------------------------------------------------------------


def _prepare_destinations(configs: Dict[str, List[PipelineConfig]]) -> None:
    """Prepare destinations for running multiple pipelines."""
    from dlt_saga.defaults import apply_dlt_defaults
    from dlt_saga.destinations.factory import DestinationFactory

    apply_dlt_defaults()
    configs_by_destination: Dict[str, List[PipelineConfig]] = {}

    from dlt_saga.utility.cli.context import get_execution_context

    context_dest_type = get_execution_context().get_destination_type()

    for pipeline_configs in configs.values():
        for config in pipeline_configs:
            destination_type = config.config_dict.get(
                "destination_type", context_dest_type
            )
            if destination_type not in configs_by_destination:
                configs_by_destination[destination_type] = []
            configs_by_destination[destination_type].append(config)

    for destination_type, pipeline_configs in configs_by_destination.items():
        destination_class = DestinationFactory.get_destination_class(destination_type)
        destination_class.prepare_for_execution(pipeline_configs)


def _create_orchestration_destination():
    """Create a Destination instance for the orchestration schema.

    Used by orchestrator and worker modes to access execution plan tables.
    """
    from dlt_saga.destinations.factory import DestinationFactory

    context = get_execution_context()
    destination_type = context.get_destination_type()
    # Use an empty config dict — only database/schema matter for SQL execution
    return DestinationFactory.create_from_context(
        destination_type, context, {"schema_name": ""}
    )


# ---------------------------------------------------------------------------
# Safe per-pipeline execution wrappers (worker mode)
# ---------------------------------------------------------------------------

# Cap stored error messages so a pathological traceback-in-message can't bloat
# the execution-plan row; the report shows the full stored text (expandable).
_MAX_ERROR_MESSAGE_LEN = 2000


def _format_run_error(exc: Exception) -> str:
    """Build a concise, informative error string for storage and reporting.

    Includes the exception type so the cause is identifiable (e.g.
    ``NotFound: 404 Not found: Table ...``) rather than a bare message.
    """
    # Secrets masked before truncating: a secret cut in half no longer matches.
    message = redact(f"{type(exc).__name__}: {exc}".strip())
    if len(message) > _MAX_ERROR_MESSAGE_LEN:
        message = message[: _MAX_ERROR_MESSAGE_LEN - 1] + "…"
    return message


def _run_pipeline_safe(
    pipeline_config: PipelineConfig,
    log_prefix: str,
) -> Optional[str]:
    """Thread-safe wrapper for running a pipeline.

    Returns ``None`` on success, or the error message on failure (so callers
    can surface the real cause instead of a generic "failed").

    Fires the per-pipeline lifecycle events, as ``Session`` does for a local
    run. This is the only path a worker container takes, so without them a
    custom ``on_pipeline_error`` handler would never run on exactly the
    deployment it was written for.
    """
    from dlt_saga.hooks.registry import (
        fire_pipeline_complete,
        fire_pipeline_error,
        fire_pipeline_start,
    )
    from dlt_saga.pipelines.executor import execute_pipeline

    fire_pipeline_start(pipeline_config, "ingest")
    try:
        result = execute_pipeline(pipeline_config, log_prefix)
        if result:
            if isinstance(result, dict) and result.get("operation") == "update_access":
                table_name = result.get("table_name", "Unknown")
                tables_updated = result.get("tables_updated", [])
                message = (
                    f"Updated access for {table_name} ({len(tables_updated)} table(s))"
                )
                logger.info("%s %s", log_prefix, message)
            else:
                logger.info("%s %s", log_prefix, summarize_load_info(result))
        fire_pipeline_complete(pipeline_config, "ingest", result)
        return None
    except Exception as e:
        logger.error("%s %s", log_prefix, e)
        fire_pipeline_error(pipeline_config, "ingest", e)
        return _format_run_error(e)


def _build_historize_runner(
    pipeline_config: PipelineConfig,
    full_refresh: bool,
):
    """Build a HistorizeRunner for a pipeline config."""
    from dlt_saga.historize.factory import build_historize_runner

    return build_historize_runner(pipeline_config, full_refresh)


def _run_historize_safe(
    pipeline_config: PipelineConfig,
    full_refresh: bool,
    log_prefix: str,
) -> Optional[str]:
    """Thread-safe wrapper for historizing a single pipeline.

    Returns ``None`` on success, or the error message on failure. Fires the
    per-pipeline lifecycle events for the same reason :func:`_run_pipeline_safe`
    does — a worker is the only path this code takes.
    """
    from dlt_saga.hooks.registry import (
        fire_pipeline_complete,
        fire_pipeline_error,
        fire_pipeline_start,
    )

    fire_pipeline_start(pipeline_config, "historize")
    try:
        runner = _build_historize_runner(pipeline_config, full_refresh)
        result = runner.run()

        if result["status"] == "completed":
            mode = result["mode"]
            duration = result.get("duration", 0)
            timings = result.get("timings", {})
            timing_parts = ", ".join(f"{k}: {v:.1f}s" for k, v in timings.items())
            detail = (
                "all snapshots"
                if mode == "full_reprocess"
                else f"{result['snapshots_processed']} snapshot(s)"
            )
            stats_parts = []
            if result.get("new_or_changed_rows"):
                stats_parts.append(f"{result['new_or_changed_rows']} rows")
            if result.get("deleted_rows"):
                stats_parts.append(f"{result['deleted_rows']} deletions")
            stats_str = f", {', '.join(stats_parts)}" if stats_parts else ""
            msg = f"{pipeline_config.pipeline_name}: {mode} ({detail}{stats_str}, {duration:.1f}s [{timing_parts}])"
            logger.info("%s %s", log_prefix, msg)
            fire_pipeline_complete(pipeline_config, "historize", result)
            return None
        else:
            error = str(result.get("error", "Unknown error"))
            logger.error("%s %s: %s", log_prefix, pipeline_config.pipeline_name, error)
            # A runner that reports a failed status rather than raising is still
            # a failed pipeline, as it is in `Session`.
            fire_pipeline_error(pipeline_config, "historize", RuntimeError(error))
            return error

    except Exception as e:
        logger.error(
            "%s Historization failed for %s: %s",
            log_prefix,
            pipeline_config.pipeline_name,
            e,
            exc_info=True,
        )
        fire_pipeline_error(pipeline_config, "historize", e)
        return _format_run_error(e)


# ---------------------------------------------------------------------------
# Orchestrator mode
# ---------------------------------------------------------------------------


def _calculate_task_count(configs: List[PipelineConfig]) -> int:
    """Calculate the number of tasks needed for the given pipeline configs."""
    task_groups = set()
    ungrouped_count = 0
    for config in configs:
        task_group = config.config_dict.get("task_group")
        if task_group:
            task_groups.add(task_group)
        else:
            ungrouped_count += 1
    return len(task_groups) + ungrouped_count


def _build_task_assignments(configs: List[PipelineConfig]) -> List[Dict]:
    """Build task-index-to-pipeline mapping for preview or logging.

    Returns a list of dicts, one per task, with ``task_index``,
    optional ``task_group``, and ``pipelines`` (list of pipeline names).

    Mirrors ``ExecutionPlanManager.create_execution_plan`` so the dry-run
    preview matches the persisted plan exactly.
    """
    from dlt_saga.utility.orchestration.execution_plan import (
        ExecutionPlanManager,
        _group_into_task_units,
    )

    units = _group_into_task_units(configs)
    interleaved = ExecutionPlanManager._interleave_task_units(units)

    tasks = []
    for task_index, unit in enumerate(interleaved):
        task: Dict[str, Any] = {
            "task_index": task_index,
            "pipelines": [c.pipeline_name for c in unit],
        }
        # Surface the task_group name whenever the unit came from a group,
        # even a single-pipeline group — preserves the user's labeling so
        # the preview shows the same identity workers see.
        group_name = unit[0].config_dict.get("task_group")
        if group_name:
            task["task_group"] = group_name
        tasks.append(task)
    return tasks


def _resolve_orchestration_provider() -> "OrchestrationProvider":
    """Resolve orchestration provider from project config.

    Reads the ``orchestration:`` section from ``saga_project.yml``.

    Raises:
        typer.Exit: If no provider is configured or the environment
            is not prod (workers need access to the execution plan schema).
    """
    from dlt_saga.project_config import get_orchestration_config
    from dlt_saga.utility.orchestration.providers import resolve_provider

    config = get_orchestration_config()
    context = get_execution_context()
    project_id = context.get_database() or get_env("SAGA_DESTINATION_DATABASE")

    provider = resolve_provider(config=config, project_id=project_id)
    if provider is None:
        logger.error(
            "No orchestration provider configured. "
            "Add orchestration.provider to saga_project.yml"
        )
        raise typer.Exit(1)

    environment = context.get_environment() or get_environment()
    if environment != "prod":
        logger.error(
            "--orchestrate requires a prod target (current: %s). "
            "Workers need access to the execution plan schema, "
            "which is only available in the prod project.",
            environment,
        )
        raise typer.Exit(1)

    return provider


def run_orchestrator_mode(
    configs: Dict[str, List[PipelineConfig]],
    debug_logging: bool = False,
    command: str = "ingest",
    select_criteria: Optional[str] = None,
    profile: str = "default",
    target: Optional[str] = None,
    provider: Optional["OrchestrationProvider"] = None,
    start_value_override: Optional[str] = None,
    end_value_override: Optional[str] = None,
    force: bool = False,
    workers: Optional[int] = None,
):
    """Run in orchestrator mode: create execution plan and trigger workers.

    Args:
        provider: OrchestrationProvider to use for triggering workers.
        start_value_override: Backfill window start, baked into each plan
            row so workers receive it without seeing the orchestrator's CLI.
        end_value_override: Backfill window end (same propagation rationale).
        force: Forwarded to the trigger as ``SAGA_FORCE`` env var so workers
            bypass change detection. (``full_refresh`` deliberately is *not*
            propagated — it requires interactive confirmation and must run
            from a local orchestrator.)
        workers: Per-task concurrency cap. Forwarded to workers as
            ``SAGA_WORKER_CONCURRENCY`` so multi-pipeline ``task_group``s
            don't fan out to as many threads as they have pipelines.
            When ``None``, falls back to ``saga_project.yml`` config and
            then the framework default — same precedence the worker uses.
    """
    from dlt_saga.utility.naming import get_execution_plan_schema
    from dlt_saga.utility.orchestration.execution_plan import (
        ExecutionMetadata,
        ExecutionPlanManager,
    )

    all_configs = []
    for pipeline_configs in configs.values():
        all_configs.extend(pipeline_configs)

    if not all_configs:
        logger.error("No pipelines to execute")
        raise typer.Exit(1)

    # Fail the whole plan before any worker is triggered if two pipelines
    # resolve to the same target (both layers — the plan spans ingest+historize).
    try:
        config_source = get_config_source()
        check_target_collisions(
            all_configs,
            config_source,
            check_ingest=True,
            check_historize=True,
        )
        check_task_concurrency(all_configs, config_source)
    except (TargetCollisionError, TaskConcurrencyError) as e:
        logger.error(str(e))
        raise typer.Exit(1)

    logger.info(
        "Orchestrator mode: Planning execution for %d pipeline(s)",
        len(all_configs),
    )

    _prepare_destinations(configs)

    context = get_execution_context()
    plan_dataset = get_execution_plan_schema()
    destination = _create_orchestration_destination()

    plan_manager = ExecutionPlanManager(destination=destination, schema=plan_dataset)
    metadata = ExecutionMetadata(
        select_criteria=select_criteria,
        command=command,
        environment=context.get_environment() or get_environment(),
        profile=profile,
        target=target,
        start_value_override=start_value_override,
        end_value_override=end_value_override,
    )
    execution_id = plan_manager.create_execution_plan(all_configs, metadata=metadata)

    task_count = _calculate_task_count(all_configs)
    logger.info("Task allocation: %d total tasks", task_count)

    resolved_workers = _resolve_worker_concurrency(workers)

    try:
        result = provider.trigger(
            execution_id=execution_id,
            task_count=task_count,
            command=command,
            debug=debug_logging,
            force=force,
            worker_concurrency=resolved_workers,
        )
        logger.info(
            "Triggered execution: %s (worker concurrency: %d)",
            result.execution_reference,
            resolved_workers,
        )
    except Exception as e:
        logger.error("Failed to trigger workers: %s", e)
        raise typer.Exit(1)


# ---------------------------------------------------------------------------
# Worker mode
# ---------------------------------------------------------------------------


def _get_worker_environment(
    execution_id: Optional[str] = None,
    task_index: Optional[int] = None,
) -> tuple[str, int]:
    """Get and validate worker parameters from CLI args or environment variables.

    CLI args take precedence; env vars are used as fallback.
    """
    execution_id = execution_id or get_env("SAGA_EXECUTION_ID")
    if not execution_id:
        logger.error(
            "Execution ID required: pass --execution-id or set SAGA_EXECUTION_ID"
        )
        raise typer.Exit(1)

    if task_index is None:
        task_index_str = os.getenv("CLOUD_RUN_TASK_INDEX") or get_env("SAGA_TASK_INDEX")
        if task_index_str is None:
            logger.error(
                "Task index required: pass --task-index or set "
                "CLOUD_RUN_TASK_INDEX / SAGA_TASK_INDEX"
            )
            raise typer.Exit(1)
        try:
            task_index = int(task_index_str)
        except ValueError:
            logger.error("Invalid task index: %s", task_index_str)
            raise typer.Exit(1)

    return execution_id, task_index


def _log_assigned_pipelines(pipeline_configs: List[PipelineConfig]):
    """Log assigned pipelines for this worker."""
    if len(pipeline_configs) == 1:
        logger.info(
            "Assigned pipeline: %s (%s)",
            pipeline_configs[0].table_name,
            pipeline_configs[0].pipeline_group,
        )
    else:
        task_group = pipeline_configs[0].config_dict.get("task_group", "unknown")
        logger.info(
            "Assigned %d pipelines in task group '%s'",
            len(pipeline_configs),
            task_group,
        )
        for i, config in enumerate(pipeline_configs, 1):
            logger.info("  %d. %s (%s)", i, config.table_name, config.pipeline_group)


def _worker_log_prefix(task_index: int, i: int, total: int) -> str:
    """Build log prefix for worker tasks."""
    if total == 1:
        return f"[Task {task_index}]"
    return f"[Task {task_index}:{i}/{total}]"


def _execute_worker_parallel(
    configs: List[PipelineConfig],
    task_index: int,
    label: str,
    run_fn: Callable[[PipelineConfig, str], Optional[str]],
    max_workers: int = _DEFAULT_WORKER_CONCURRENCY,
    progress: Optional["_TaskProgress"] = None,
) -> List[tuple[str, str]]:
    """Execute pipelines in parallel within a worker task.

    Args:
        configs: Pre-filtered pipeline configs to run.
        task_index: Worker task index for log prefixes.
        label: Phase label for logging (e.g. "ingest", "historize").
        run_fn: Callable(config, log_prefix) -> Optional[str], returning ``None``
            on success or the error message on failure.
        max_workers: Cap on concurrent threads inside this worker. Capped
            again at the most pipelines that can run at once — one per
            pipeline, or the group's ``task_concurrency`` — since there's no
            point spawning idle threads. Defaults to
            ``_DEFAULT_WORKER_CONCURRENCY`` so a large ``task_group`` can't OOM
            the worker container by spawning one thread per pipeline.
        progress: Records each pipeline's outcome for this phase as it
            finishes, so a task stopped part-way still knows which did.

    Returns:
        List of (table_name, error_message) tuples for failed pipelines, carrying
        the real error so the report shows the cause rather than a generic "failed".
    """
    if not configs:
        return []

    effective_workers = max(1, min(parallel_slots(configs), max_workers))

    if len(configs) > 1:
        logger.info(
            "[Task %d] Running %d %s pipelines (concurrency=%d)",
            task_index,
            len(configs),
            label,
            effective_workers,
        )

    failed_pipelines: List[tuple[str, str]] = []
    with ThreadPoolExecutor(max_workers=effective_workers) as executor:
        future_to_config = submit_with_group_caps(
            executor,
            [
                (
                    config,
                    partial(
                        run_fn, config, _worker_log_prefix(task_index, i, len(configs))
                    ),
                )
                for i, config in enumerate(configs, 1)
            ],
        )

        for future in as_completed(future_to_config):
            config = future_to_config[future]
            try:
                error = future.result()
            except Exception as e:
                # Safety net: run_fn handles its own exceptions, but if one
                # escapes, still record the real cause rather than dropping it.
                logger.error(
                    "%s %s failed: %s", label.capitalize(), config.table_name, e
                )
                error = _format_run_error(e)
            if error:
                failed_pipelines.append((config.table_name, error))
            if progress is not None:
                progress.record(config, label, error)

    return failed_pipelines


def _execute_worker_ingest(
    pipeline_configs: List[PipelineConfig],
    task_index: int,
    max_workers: int = _DEFAULT_WORKER_CONCURRENCY,
    progress: Optional["_TaskProgress"] = None,
) -> List[tuple[str, str]]:
    """Execute ingest for assigned pipelines."""
    configs = [c for c in pipeline_configs if c.ingest_enabled]
    return _execute_worker_parallel(
        configs,
        task_index,
        "ingest",
        _run_pipeline_safe,
        max_workers=max_workers,
        progress=progress,
    )


def _execute_worker_historize(
    pipeline_configs: List[PipelineConfig],
    task_index: int,
    full_refresh: bool = False,
    max_workers: int = _DEFAULT_WORKER_CONCURRENCY,
    progress: Optional["_TaskProgress"] = None,
) -> List[tuple[str, str]]:
    """Execute historize for assigned pipelines."""
    configs = [c for c in pipeline_configs if c.historize_enabled]

    def _run(config: PipelineConfig, log_prefix: str) -> Optional[str]:
        return _run_historize_safe(config, full_refresh, log_prefix)

    return _execute_worker_parallel(
        configs,
        task_index,
        "historize",
        _run,
        max_workers=max_workers,
        progress=progress,
    )


def _execute_worker_run(
    pipeline_configs: List[PipelineConfig],
    task_index: int,
    full_refresh: bool = False,
    max_workers: int = _DEFAULT_WORKER_CONCURRENCY,
    progress: Optional["_TaskProgress"] = None,
) -> List[tuple[str, str]]:
    """Execute ingest then historize for assigned pipelines."""
    failed_pipelines = []

    # Run ingest first
    ingest_failed = _execute_worker_ingest(
        pipeline_configs, task_index, max_workers=max_workers, progress=progress
    )
    failed_pipelines.extend(ingest_failed)
    failed_names = {name for name, _ in ingest_failed}

    # Run historize, skipping pipelines whose ingest failed during full refresh
    historize_configs = pipeline_configs
    if full_refresh and failed_names:
        historize_configs = [
            c for c in pipeline_configs if c.table_name not in failed_names
        ]
        skipped = len(pipeline_configs) - len(historize_configs)
        if skipped:
            logger.warning(
                "[Task %d] Skipping historize for %d pipeline(s) due to failed ingest",
                task_index,
                skipped,
            )

    historize_failed = _execute_worker_historize(
        historize_configs,
        task_index,
        full_refresh,
        max_workers=max_workers,
        progress=progress,
    )
    failed_pipelines.extend(historize_failed)

    return failed_pipelines


_WORKER_PHASES: Dict[str, Tuple[str, ...]] = {
    "ingest": ("ingest",),
    "historize": ("historize",),
    "run": ("ingest", "historize"),
}

# Exit code for a worker stopped by SIGTERM (128 + signal number, by convention).
_SIGTERM_EXIT_CODE = 128 + int(signal.SIGTERM)

_SIGTERM_REASON = "Terminated before completing (SIGTERM: task timeout or cancellation)"

PipelineOutcomes = Dict[str, Tuple[str, Optional[str]]]


class _TaskProgress:
    """Each assigned pipeline's outcome within one worker task, as phases finish.

    Kept in memory so the task's final status can be written per pipeline from
    any exit path — normal completion, a crash, or termination by the
    platform — with pipelines that finished keeping their real outcome.
    """

    def __init__(self, pipeline_configs: List[PipelineConfig], worker_command: str):
        phases = _WORKER_PHASES.get(worker_command, ("ingest",))
        self._lock = threading.Lock()
        self._pending: Dict[str, Set[str]] = {
            config.identifier: {
                phase for phase in phases if getattr(config, f"{phase}_enabled")
            }
            for config in pipeline_configs
        }
        self._errors: Dict[str, List[Tuple[str, str]]] = {
            config.identifier: [] for config in pipeline_configs
        }

    def record(self, config: PipelineConfig, phase: str, error: Optional[str]) -> None:
        """Record that ``phase`` finished for ``config``, failed if ``error``."""
        with self._lock:
            self._pending.setdefault(config.identifier, set()).discard(phase)
            if error:
                self._errors.setdefault(config.identifier, []).append((phase, error))

    def outcomes(self, interrupted_by: Optional[str] = None) -> PipelineOutcomes:
        """``(status, error_message)`` per pipeline identifier.

        Args:
            interrupted_by: Why the task stopped early; a pipeline with a phase
                still unfinished is failed with it. None when the executor
                returned normally, i.e. every phase ran.
        """
        with self._lock:
            result: PipelineOutcomes = {}
            for identifier, errors in self._errors.items():
                if len(errors) == 1:
                    messages = [errors[0][1]]
                else:
                    messages = [f"{phase}: {error}" for phase, error in errors]
                if interrupted_by and self._pending.get(identifier):
                    messages.append(interrupted_by)
                result[identifier] = (
                    ("failed", "; ".join(messages)) if messages else ("completed", None)
                )
            return result


class _TaskStatusWriter:
    """Writes a worker task's final per-pipeline statuses exactly once.

    The normal exit, the crash path and the SIGTERM handler can race to record
    the outcome; whichever claims it first writes, the others do nothing.
    """

    def __init__(
        self, plan_manager, execution_id: str, task_index: int, progress: _TaskProgress
    ):
        self._plan_manager = plan_manager
        self._execution_id = execution_id
        self._task_index = task_index
        self._progress = progress
        # Only ever acquired without blocking: the signal handler runs on the
        # main thread, so it must not wait on a lock the main thread holds.
        self._claim = threading.Lock()

    def write(self, interrupted_by: Optional[str] = None) -> Optional[PipelineOutcomes]:
        """Write the final statuses; None if another exit path already claimed it.

        Best-effort: a failed write is logged, never raised, so it can't mask
        the error that ended the task.
        """
        if not self._claim.acquire(blocking=False):
            return None
        outcomes = self._progress.outcomes(interrupted_by)
        try:
            self._plan_manager.record_pipeline_outcomes(
                self._execution_id, self._task_index, outcomes
            )
        except Exception:
            logger.error(
                "[Task %d] Failed to record final pipeline statuses; "
                "the task may remain 'running' in the plan",
                self._task_index,
                exc_info=True,
            )
        return outcomes


def _install_sigterm_handler(
    status_writer: _TaskStatusWriter, task_index: int
) -> Callable[[], None]:
    """Record unfinished pipelines as failed when the platform stops the worker.

    A task timeout or cancellation arrives as SIGTERM, followed by SIGKILL after
    a short grace period. Python's default SIGTERM action ends the process
    without running any Python code, leaving every pipeline ``running`` until
    the stale reconciler relabels it — so the failure never reached the report
    or ``state:failed``. The handler exits with ``os._exit`` once the statuses
    are written: unwinding normally would wait on the worker pool, whose stuck
    thread is often why the task overran.

    Returns:
        A callable restoring the previous handler (a no-op when none was
        installed: signal handlers can only be set from the main thread).
    """

    def _handle(signum, frame):
        outcomes = status_writer.write(interrupted_by=_SIGTERM_REASON)
        if outcomes is None:
            # The normal exit is already writing the final statuses; let it finish.
            return
        failed = sum(1 for status, _ in outcomes.values() if status == "failed")
        logger.error(
            "[Task %d] Terminated by SIGTERM; recorded %d/%d pipeline(s) as failed",
            task_index,
            failed,
            len(outcomes),
        )
        logging.shutdown()
        os._exit(_SIGTERM_EXIT_CODE)

    try:
        previous = signal.signal(signal.SIGTERM, _handle)
    except ValueError:
        logger.debug("Not on the main thread; SIGTERM handler not installed")
        return lambda: None
    # None means the previous handler wasn't set from Python: the default.
    restored = signal.SIG_DFL if previous is None else previous

    def _restore() -> None:
        signal.signal(signal.SIGTERM, restored)

    return _restore


def _update_worker_status(
    status_writer: _TaskStatusWriter,
    task_index: int,
    pipeline_configs: List[PipelineConfig],
):
    """Record each pipeline's final status; exit non-zero if any failed."""
    outcomes = status_writer.write() or {}
    failed = sum(1 for status, _ in outcomes.values() if status == "failed")
    if failed:
        logger.error(
            "[Task %d] %d/%d pipeline(s) failed",
            task_index,
            failed,
            len(pipeline_configs),
        )
        raise typer.Exit(1)
    logger.info(
        "[Task %d] All %d pipeline(s) completed successfully",
        task_index,
        len(pipeline_configs),
    )


# Default for the CLI --workers option on ingest/historize/run. Kept here so the
# worker-mode warning can tell an explicit override from the untouched default.
_CLI_DEFAULT_WORKERS = 4


def _warn_workers_ignored_in_worker_mode(workers: int) -> None:
    """Warn when a non-default --workers is passed in worker mode.

    In worker mode per-task concurrency is fixed by the execution plan
    (``SAGA_WORKER_CONCURRENCY`` / ``saga_project.yml`` / the default), not the
    CLI flag, so an explicit --workers would otherwise be silently ignored.
    """
    if workers != _CLI_DEFAULT_WORKERS:
        logger.warning(
            "--workers=%d has no effect in worker mode; per-task concurrency is "
            "set by SAGA_WORKER_CONCURRENCY / saga_project.yml / the default.",
            workers,
        )


def run_worker_mode(
    execution_id: Optional[str] = None,
    task_index: Optional[int] = None,
    worker_command: Optional[str] = None,
    workers: Optional[int] = None,
):
    """Run in worker mode: read assignment from execution plan and execute.

    Args:
        execution_id: Explicit execution ID (falls back to SAGA_EXECUTION_ID).
        task_index: Explicit task index (falls back to CLOUD_RUN_TASK_INDEX / SAGA_TASK_INDEX).
        worker_command: Explicit command (falls back to SAGA_WORKER_COMMAND, default "ingest").
        workers: Explicit per-task concurrency cap (falls back to
            ``SAGA_WORKER_CONCURRENCY`` → ``saga_project.yml`` →
            ``_DEFAULT_WORKER_CONCURRENCY``).
    """
    from dlt_saga.utility.naming import get_execution_plan_schema
    from dlt_saga.utility.orchestration.execution_plan import ExecutionPlanManager

    execution_id, task_index = _get_worker_environment(execution_id, task_index)
    worker_command = worker_command or get_env("SAGA_WORKER_COMMAND", "ingest")
    full_refresh = get_execution_context().full_refresh
    max_workers = _resolve_worker_concurrency(workers)

    logger.info(
        "Worker mode: Task %d for execution %s (command: %s, concurrency: %d)",
        task_index,
        execution_id,
        worker_command,
        max_workers,
    )

    plan_dataset = get_execution_plan_schema()
    destination = _create_orchestration_destination()
    plan_manager = ExecutionPlanManager(destination=destination, schema=plan_dataset)

    try:
        pipeline_configs = plan_manager.get_task_assignment(execution_id, task_index)
    except Exception as e:
        logger.error("Failed to fetch task assignment: %s", e)
        raise typer.Exit(1)

    if not pipeline_configs:
        logger.error("No pipelines assigned to task %d", task_index)
        raise typer.Exit(1)

    _log_assigned_pipelines(pipeline_configs)
    plan_manager.update_task_status(execution_id, task_index, "running")

    progress = _TaskProgress(pipeline_configs, worker_command)
    status_writer = _TaskStatusWriter(plan_manager, execution_id, task_index, progress)
    restore_sigterm_handler = _install_sigterm_handler(status_writer, task_index)

    # Any crash between here and the final status write (connection loss, an
    # unexpected bug, Ctrl-C) would otherwise leave the task "running" until the
    # stale reconciler relabels it, and the orchestrator waits on task status.
    # Catch BaseException so SystemExit/KeyboardInterrupt are recorded too, write
    # the statuses best-effort, then re-raise to preserve the exit/traceback.
    # Termination by the platform is handled by the SIGTERM handler.
    try:
        try:
            if worker_command == "historize":
                _execute_worker_historize(
                    pipeline_configs,
                    task_index,
                    full_refresh,
                    max_workers=max_workers,
                    progress=progress,
                )
            elif worker_command == "run":
                _execute_worker_run(
                    pipeline_configs,
                    task_index,
                    full_refresh,
                    max_workers=max_workers,
                    progress=progress,
                )
            else:
                # Default: ingest only (backward compatible)
                _execute_worker_ingest(
                    pipeline_configs,
                    task_index,
                    max_workers=max_workers,
                    progress=progress,
                )
        except BaseException as exc:
            status_writer.write(interrupted_by=f"Worker crashed: {exc}")
            raise

        _update_worker_status(status_writer, task_index, pipeline_configs)
    finally:
        restore_sigterm_handler()
