# CLI for the saga data pipeline framework
#
# Single entry point: saga
# Subcommands: list, ingest, historize, run

import json
import logging
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path
from typing import TYPE_CHECKING, Callable, Dict, List, Optional

if TYPE_CHECKING:
    from dlt_saga.utility.cli.context import ExecutionContext
    from dlt_saga.utility.cli.profiles import ProfileTarget
    from dlt_saga.utility.orchestration.providers import OrchestrationProvider

import typer

from dlt_saga.pipeline_config import PipelineConfig
from dlt_saga.utility.auth.providers import AuthenticationError
from dlt_saga.utility.cli.common import (
    check_cloud_run_environment,
    discover_and_select_configs,
    execute_with_impersonation,
    flatten_configs,
    load_profile_config,
    setup_execution_context,
    setup_logging,
    validate_credentials,
)
from dlt_saga.utility.cli.context import get_execution_context
from dlt_saga.utility.cli.logging import configure_cli_logging
from dlt_saga.utility.cli.reporting import summarize_load_info
from dlt_saga.utility.cli.selectors import format_config_list
from dlt_saga.utility.env import get_env
from dlt_saga.utility.naming import get_environment

# Configure CLI logging (handlers, formatters, colors)
configure_cli_logging()

logger = logging.getLogger(__name__)


def _version_callback(value: bool):
    if value:
        from importlib.metadata import version as pkg_version

        try:
            ver = pkg_version("dlt-saga")
        except Exception:
            ver = "unknown"
        typer.echo(f"dlt-saga {ver}")
        raise typer.Exit()


app = typer.Typer(help="saga data pipeline framework")


@app.callback(invoke_without_command=True)
def _app_callback(
    version: bool = typer.Option(
        False,
        "--version",
        "-v",
        help="Show version and exit.",
        callback=_version_callback,
        is_eager=True,
    ),
):
    """saga data pipeline framework."""


# ---------------------------------------------------------------------------
# Selector/help strings shared across commands
# ---------------------------------------------------------------------------

_SELECT_HELP = (
    "Select pipelines using dbt-style selectors "
    "(space-separated=UNION, comma-separated=INTERSECTION). "
    "Supports: pipeline names, glob patterns (*), tag:name, group:name."
)


# ---------------------------------------------------------------------------
# Config filter helpers
# ---------------------------------------------------------------------------


def _is_ingest_enabled(config: PipelineConfig) -> bool:
    """Keep configs that have an ingest component in their write_disposition."""
    return config.ingest_enabled


def _is_historize_enabled(config: PipelineConfig) -> bool:
    """Keep configs that have a historize component in their write_disposition."""
    return config.historize_enabled


def _list_implementations():
    """Print available pipeline implementations."""
    from dlt_saga.pipelines.registry import discover_implementations

    impls = discover_implementations()
    if not impls:
        print("No pipeline implementations found.")
        return

    # Calculate column widths
    ns_width = max(len(i["namespace"]) for i in impls)
    path_width = max(len(i["path"]) for i in impls)
    class_width = max(len(i["class_name"]) for i in impls)

    # Header
    header = f"{'Namespace':<{ns_width}}  {'Path':<{path_width}}  {'Class':<{class_width}}  Source"
    print(f"\n{header}")
    print("-" * len(header))

    for impl in impls:
        print(
            f"{impl['namespace']:<{ns_width}}  "
            f"{impl['path']:<{path_width}}  "
            f"{impl['class_name']:<{class_width}}  "
            f"{impl['source']}"
        )
    print(f"\n{len(impls)} implementation(s) found.")


# ---------------------------------------------------------------------------
# Ingest pipeline execution helpers (worker mode)
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


def _run_pipeline_safe(
    pipeline_config: PipelineConfig,
    log_prefix: str,
) -> bool:
    """Thread-safe wrapper for running a pipeline."""
    from dlt_saga.pipelines.executor import execute_pipeline

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
        return True
    except Exception as e:
        logger.error("%s %s", log_prefix, e)
        return False


# ---------------------------------------------------------------------------
# Orchestration & Worker mode helpers
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
    """
    grouped: Dict[str, List[PipelineConfig]] = {}
    ungrouped: List[PipelineConfig] = []
    for config in configs:
        tg = config.config_dict.get("task_group")
        if tg:
            grouped.setdefault(tg, []).append(config)
        else:
            ungrouped.append(config)

    tasks = []
    task_index = 0
    for task_group, group_configs in grouped.items():
        tasks.append(
            {
                "task_index": task_index,
                "task_group": task_group,
                "pipelines": [c.pipeline_name for c in group_configs],
            }
        )
        task_index += 1
    # Interleave ungrouped pipelines by target dataset (schema_name) to
    # distribute concurrent writes across different datasets (matches
    # execution_plan.py).
    by_schema: Dict[str, List[PipelineConfig]] = {}
    for config in ungrouped:
        by_schema.setdefault(config.schema_name, []).append(config)

    group_iters = [iter(cfgs) for cfgs in by_schema.values()]
    while group_iters:
        remaining = []
        for it in group_iters:
            cfg = next(it, None)
            if cfg is not None:
                tasks.append(
                    {
                        "task_index": task_index,
                        "pipelines": [cfg.pipeline_name],
                    }
                )
                task_index += 1
                remaining.append(it)
        group_iters = remaining
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
):
    """Run in orchestrator mode: create execution plan and trigger workers.

    Args:
        provider: OrchestrationProvider to use for triggering workers.
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
    )
    execution_id = plan_manager.create_execution_plan(all_configs, metadata=metadata)

    task_count = _calculate_task_count(all_configs)
    logger.info("Task allocation: %d total tasks", task_count)

    try:
        result = provider.trigger(
            execution_id=execution_id,
            task_count=task_count,
            command=command,
            debug=debug_logging,
        )
        logger.info("Triggered execution: %s", result.execution_reference)
    except Exception as e:
        logger.error("Failed to trigger workers: %s", e)
        raise typer.Exit(1)


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
            "Assigned %d pipelines in task group '%s' to run in parallel",
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
    run_fn: Callable[[PipelineConfig, str], bool],
) -> List[tuple[str, str]]:
    """Execute pipelines in parallel within a worker task.

    Args:
        configs: Pre-filtered pipeline configs to run.
        task_index: Worker task index for log prefixes.
        label: Phase label for logging (e.g. "ingest", "historize").
        run_fn: Callable(config, log_prefix) -> bool (True=success).

    Returns:
        List of (table_name, error_message) tuples for failed pipelines.
    """
    if not configs:
        return []

    if len(configs) > 1:
        logger.info(
            "[Task %d] Running %d %s pipelines in parallel",
            task_index,
            len(configs),
            label,
        )

    failed_pipelines: List[tuple[str, str]] = []
    with ThreadPoolExecutor(max_workers=len(configs)) as executor:
        future_to_config = {}
        for i, config in enumerate(configs, 1):
            log_prefix = _worker_log_prefix(task_index, i, len(configs))
            future = executor.submit(run_fn, config, log_prefix)
            future_to_config[future] = config

        for future in as_completed(future_to_config):
            config = future_to_config[future]
            try:
                if not future.result():
                    failed_pipelines.append(
                        (config.table_name, f"{label.capitalize()} failed")
                    )
            except Exception as e:
                logger.error(
                    "%s %s failed: %s", label.capitalize(), config.table_name, e
                )
                failed_pipelines.append((config.table_name, str(e)))

    return failed_pipelines


def _execute_worker_ingest(
    pipeline_configs: List[PipelineConfig], task_index: int
) -> List[tuple[str, str]]:
    """Execute ingest for assigned pipelines."""
    configs = [c for c in pipeline_configs if c.ingest_enabled]
    return _execute_worker_parallel(configs, task_index, "ingest", _run_pipeline_safe)


def _execute_worker_historize(
    pipeline_configs: List[PipelineConfig],
    task_index: int,
    full_refresh: bool = False,
) -> List[tuple[str, str]]:
    """Execute historize for assigned pipelines."""
    configs = [c for c in pipeline_configs if c.historize_enabled]

    def _run(config: PipelineConfig, log_prefix: str) -> bool:
        return _run_historize_safe(config, full_refresh, log_prefix)

    return _execute_worker_parallel(configs, task_index, "historize", _run)


def _execute_worker_run(
    pipeline_configs: List[PipelineConfig],
    task_index: int,
    full_refresh: bool = False,
) -> List[tuple[str, str]]:
    """Execute ingest then historize for assigned pipelines."""
    failed_pipelines = []

    # Run ingest first
    ingest_failed = _execute_worker_ingest(pipeline_configs, task_index)
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
        historize_configs, task_index, full_refresh
    )
    failed_pipelines.extend(historize_failed)

    return failed_pipelines


def _update_worker_status(
    plan_manager,
    execution_id: str,
    task_index: int,
    pipeline_configs: List[PipelineConfig],
    failed_pipelines: List[tuple[str, str]],
):
    """Update final worker status in execution plan."""
    if failed_pipelines:
        error_summary = (
            f"{len(failed_pipelines)}/{len(pipeline_configs)} pipelines failed: "
            + "; ".join(f"{name}: {err}" for name, err in failed_pipelines)
        )
        plan_manager.update_task_status(
            execution_id, task_index, "failed", error_summary
        )
        raise typer.Exit(1)
    else:
        plan_manager.update_task_status(execution_id, task_index, "completed")
        logger.info(
            "[Task %d] All %d pipeline(s) completed successfully",
            task_index,
            len(pipeline_configs),
        )


def run_worker_mode(
    execution_id: Optional[str] = None,
    task_index: Optional[int] = None,
    worker_command: Optional[str] = None,
):
    """Run in worker mode: read assignment from execution plan and execute.

    Args:
        execution_id: Explicit execution ID (falls back to SAGA_EXECUTION_ID).
        task_index: Explicit task index (falls back to CLOUD_RUN_TASK_INDEX / SAGA_TASK_INDEX).
        worker_command: Explicit command (falls back to SAGA_WORKER_COMMAND, default "ingest").
    """
    from dlt_saga.utility.naming import get_execution_plan_schema
    from dlt_saga.utility.orchestration.execution_plan import ExecutionPlanManager

    execution_id, task_index = _get_worker_environment(execution_id, task_index)
    worker_command = worker_command or get_env("SAGA_WORKER_COMMAND", "ingest")
    full_refresh = get_execution_context().full_refresh

    logger.info(
        "Worker mode: Task %d for execution %s (command: %s)",
        task_index,
        execution_id,
        worker_command,
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

    if worker_command == "historize":
        failed_pipelines = _execute_worker_historize(
            pipeline_configs, task_index, full_refresh
        )
    elif worker_command == "run":
        failed_pipelines = _execute_worker_run(
            pipeline_configs, task_index, full_refresh
        )
    else:
        # Default: ingest only (backward compatible)
        failed_pipelines = _execute_worker_ingest(pipeline_configs, task_index)

    _update_worker_status(
        plan_manager, execution_id, task_index, pipeline_configs, failed_pipelines
    )


def _confirm_full_refresh(full_refresh: bool, in_cloud_run: bool):
    """Confirm full refresh operation if specified."""
    if not full_refresh:
        return

    _sep = "=" * 70
    logger.warning(
        """FULL REFRESH MODE
%s
This will permanently delete:
"  - All destination tables (main + staging)
"  - Pipeline state metadata
"  - Load tracking information
%s""",
        _sep,
        _sep,
    )

    if not in_cloud_run:
        confirmation = typer.confirm("Are you sure you want to proceed?", default=False)
        if not confirmation:
            logger.info("Operation cancelled by user")
            raise typer.Exit(0)


def _confirm_historize_full_refresh(full_refresh: bool, in_cloud_run: bool):
    """Confirm historize full refresh operation if specified."""
    if not full_refresh:
        return

    _sep = "=" * 70
    logger.warning(
        """HISTORIZE FULL REFRESH MODE
%s
This will permanently delete and rebuild:
  - All historized output tables
  - Historization log entries for selected pipelines
%s""",
        _sep,
        _sep,
    )

    if not in_cloud_run:
        confirmation = typer.confirm("Are you sure you want to proceed?", default=False)
        if not confirmation:
            logger.info("Operation cancelled by user")
            raise typer.Exit(0)


def _confirm_partial_refresh(
    partial_refresh: bool,
    historize_from: Optional[str],
    in_cloud_run: bool,
    yes: bool,
) -> None:
    """Confirm partial re-historization if --partial-refresh or --historize-from is set."""
    if not partial_refresh and historize_from is None:
        return
    if in_cloud_run or yes:
        return

    _sep = "=" * 70
    if partial_refresh:
        logger.warning(
            """PARTIAL RE-HISTORIZATION
%s
This will roll back and rebuild historization starting from the earliest
available raw snapshot. SCD2 records derived from older snapshots are preserved.
%s""",
            _sep,
            _sep,
        )
    else:
        logger.warning(
            """PARTIAL RE-HISTORIZATION FROM %s
%s
This will roll back and rebuild historization starting from %s.
SCD2 records derived from older snapshots are preserved.
%s""",
            historize_from,
            _sep,
            historize_from,
            _sep,
        )

    confirmation = typer.confirm("Are you sure you want to proceed?", default=False)
    if not confirmation:
        logger.info("Operation cancelled by user")
        raise typer.Exit(0)


def _validate_historize_flags(
    full_refresh: bool,
    partial_refresh: bool,
    historize_from: Optional[str],
) -> None:
    """Validate mutual exclusivity and format of historize flags."""
    flag_count = sum([full_refresh, partial_refresh, historize_from is not None])
    if flag_count > 1:
        logger.error(
            "--full-refresh, --partial-refresh, and --historize-from are mutually exclusive"
        )
        raise typer.Exit(1)

    if historize_from is not None:
        try:
            datetime.fromisoformat(historize_from)
        except ValueError:
            logger.error(
                "Invalid --historize-from value: %r. "
                "Use ISO format: YYYY-MM-DD or YYYY-MM-DD HH:MM:SS",
                historize_from,
            )
            raise typer.Exit(1)


# ---------------------------------------------------------------------------
# Historize pipeline execution helpers
# ---------------------------------------------------------------------------


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
) -> bool:
    """Thread-safe wrapper for historizing a single pipeline."""
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
            return True
        else:
            error = result.get("error", "Unknown error")
            logger.error("%s %s: %s", log_prefix, pipeline_config.pipeline_name, error)
            return False

    except Exception as e:
        logger.error(
            "%s Historization failed for %s: %s",
            log_prefix,
            pipeline_config.pipeline_name,
            e,
            exc_info=True,
        )
        return False


# ===========================================================================
# CLI COMMANDS
# ===========================================================================


@app.command("list")
def list_pipelines(
    select: Optional[List[str]] = typer.Option(
        None, "--select", "-s", help=_SELECT_HELP
    ),
    resource_type: str = typer.Option(
        "all",
        "--resource-type",
        help="Filter by resource type: ingest, historize, or all",
    ),
    pipelines: bool = typer.Option(
        False, "--pipelines", help="List available pipeline types and implementations"
    ),
    verbose: bool = typer.Option(False, "--verbose", help="Enable debug logging"),
    profile: Optional[str] = typer.Option(
        None, "--profile", help="Profile to use from profiles.yml"
    ),
    target: Optional[str] = typer.Option(
        None, "--target", help="Target within profile"
    ),
):
    """List pipelines matching selectors and resource-type filter.

    Examples:
        saga list                                           # All enabled pipelines
        saga list --resource-type ingest                    # Ingest-enabled only
        saga list --resource-type historize                  # Historize-enabled only
        saga list --select "tag:daily"                      # With selector
        saga list --select "tag:daily" --resource-type ingest
        saga list --pipelines                               # Available pipeline implementations
    """
    setup_logging(verbose)

    if pipelines:
        _list_implementations()
        return

    load_profile_config(profile, target)

    # Choose filter based on resource_type
    if resource_type == "ingest":
        filter_fn = _is_ingest_enabled
    elif resource_type == "historize":
        filter_fn = _is_historize_enabled
    elif resource_type == "all":
        filter_fn = None
    else:
        logger.error(
            "Invalid --resource-type: '%s'. Must be: ingest, historize, or all",
            resource_type,
        )
        raise typer.Exit(1)

    selected_configs, selected_disabled = discover_and_select_configs(
        select, filter_fn=filter_fn
    )
    output = format_config_list(selected_configs, selected_disabled)
    print(output)


@app.command("validate")
def validate_configs(
    select: Optional[List[str]] = typer.Option(
        None, "--select", "-s", help=_SELECT_HELP
    ),
    verbose: bool = typer.Option(False, "--verbose", help="Enable debug logging"),
    profile: Optional[str] = typer.Option(
        None, "--profile", help="Profile to use from profiles.yml"
    ),
    target: Optional[str] = typer.Option(
        None, "--target", help="Target within profile"
    ),
):
    """Validate pipeline configs without executing anything.

    Checks write_disposition, adapter resolution, source config
    fields, and historize config for each selected pipeline.

    Examples:
        saga validate                                       # Validate all configs
        saga validate --select "type:google_sheets"         # Validate specific type
        saga validate --select "tag:daily"                  # Validate by tag
    """
    setup_logging(verbose)
    load_profile_config(profile, target)

    selected_configs, _ = discover_and_select_configs(select)
    all_configs = flatten_configs(selected_configs)

    if not all_configs:
        logger.error("No pipeline configs found matching selectors")
        raise typer.Exit(1)

    from dlt_saga.validate import validate_pipeline_config

    has_errors = False
    valid_count = 0
    for config in all_configs:
        result = validate_pipeline_config(config)
        if result.is_valid and not result.warnings:
            valid_count += 1
            continue

        # Print pipeline header for configs with issues
        if result.errors:
            has_errors = True
            logger.error("%s:", result.pipeline_name)
            for error in result.errors:
                logger.error("  - %s", error)
        elif result.warnings:
            logger.warning("%s:", result.pipeline_name)

        for warning in result.warnings:
            logger.warning("  - %s", warning)

    total = len(all_configs)
    invalid = total - valid_count
    if has_errors:
        logger.info(
            "\nValidation complete: %d/%d valid, %d with issues",
            valid_count,
            total,
            invalid,
        )
        raise typer.Exit(1)

    logger.info("All %d pipeline config(s) valid", total)


@app.command()
def ingest(
    select: Optional[List[str]] = typer.Option(
        None, "--select", "-s", help=_SELECT_HELP
    ),
    verbose: bool = typer.Option(False, "--verbose", help="Enable debug logging"),
    workers: int = typer.Option(
        4, "--workers", "-w", help="Number of parallel workers"
    ),
    orchestrate: bool = typer.Option(
        False,
        "--orchestrate/--no-orchestrate",
        help="Distribute execution via the configured orchestration provider",
    ),
    profile: Optional[str] = typer.Option(
        None, "--profile", help="Profile to use from profiles.yml"
    ),
    target: Optional[str] = typer.Option(
        None, "--target", help="Target within profile"
    ),
    force: bool = typer.Option(
        False, "--force", "-f", help="Force execution even if data hasn't changed"
    ),
    full_refresh: bool = typer.Option(
        False, "--full-refresh", help="Drop state/tables and reload from scratch"
    ),
    start_value_override: Optional[str] = typer.Option(
        None,
        "--start-value-override",
        help="Override start value for incremental loading (backfill). Ignores incremental state.",
    ),
    end_value_override: Optional[str] = typer.Option(
        None,
        "--end-value-override",
        help="Override end value for incremental loading (backfill). Defaults to current period.",
    ),
):
    """Run data ingestion pipelines.

    Selects pipelines whose write_disposition has an ingest component
    (append, merge, replace — including append+historize).

    Examples:
        saga ingest                                         # All ingest-enabled
        saga ingest --select "tag:daily"                    # By tag
        saga ingest --select "type:google_sheets"           # By type
        saga ingest --workers 8 --select "tag:daily"        # Parallel
        saga ingest --orchestrate --target prod             # Distributed via Cloud Run
        saga ingest --full-refresh --select "pipeline_name" # Drop and reload
        saga ingest --select "type:api" --start-value-override "2026-01-01"  # Backfill
    """
    setup_logging(verbose)
    in_cloud_run = check_cloud_run_environment()

    # Worker mode: needs execution context set up before dispatching
    if (get_env("SAGA_WORKER_MODE") or "").lower() == "true":
        logger.info("Running in worker mode (SAGA_WORKER_MODE=true)")
        profile_target = load_profile_config(profile, target)
        setup_execution_context(
            profile_target,
            force=force,
            full_refresh=full_refresh,
            start_value_override=start_value_override,
            end_value_override=end_value_override,
        )
        run_worker_mode()
        return

    _confirm_full_refresh(full_refresh, in_cloud_run)

    # Load profile once — shared by orchestrator check and Session below
    profile_target = load_profile_config(profile, target)

    # Orchestrator mode
    if orchestrate:
        setup_execution_context(
            profile_target,
            force=force,
            full_refresh=full_refresh,
            start_value_override=start_value_override,
            end_value_override=end_value_override,
        )
        provider = _resolve_orchestration_provider()
        selected_configs, _ = discover_and_select_configs(
            select, filter_fn=_is_ingest_enabled
        )
        if not selected_configs:
            logger.error("No ingest-enabled pipelines matched the selection criteria")
            raise typer.Exit(1)
        validate_credentials(in_cloud_run)
        select_str = " ".join(select) if select else None
        execute_with_impersonation(
            profile_target,
            lambda: run_orchestrator_mode(
                selected_configs,
                debug_logging=verbose,
                command="ingest",
                select_criteria=select_str,
                profile=profile,
                target=target,
                provider=provider,
            ),
        )
        return

    # Normal (local) execution via Session
    from dlt_saga.session import Session

    try:
        result = Session(
            profile=profile, target=target, _profile_target=profile_target
        ).ingest(
            select=list(select) if select else None,
            workers=workers,
            force=force,
            full_refresh=full_refresh,
            start_value_override=start_value_override,
            end_value_override=end_value_override,
        )
    except AuthenticationError as e:
        logger.error(str(e))
        raise typer.Exit(1)
    if result.has_failures:
        raise typer.Exit(1)


@app.command()
def historize(
    select: Optional[List[str]] = typer.Option(
        None, "--select", "-s", help=_SELECT_HELP
    ),
    verbose: bool = typer.Option(False, "--verbose", help="Enable debug logging"),
    workers: int = typer.Option(
        4, "--workers", "-w", help="Number of parallel workers"
    ),
    orchestrate: bool = typer.Option(
        False,
        "--orchestrate/--no-orchestrate",
        help="Distribute execution via the configured orchestration provider",
    ),
    profile: Optional[str] = typer.Option(
        None, "--profile", help="Profile to use from profiles.yml"
    ),
    target: Optional[str] = typer.Option(
        None, "--target", help="Target within profile"
    ),
    full_refresh: bool = typer.Option(
        False, "--full-refresh", help="Rebuild historized tables from all raw data"
    ),
    partial_refresh: bool = typer.Option(
        False,
        "--partial-refresh",
        help=(
            "Rebuild from earliest available raw snapshot, "
            "preserving older SCD2 records (GDPR-safe alternative to --full-refresh)"
        ),
    ),
    historize_from: Optional[str] = typer.Option(
        None,
        "--historize-from",
        help=(
            "Reprocess historization from this date onwards "
            "(ISO format: YYYY-MM-DD or 'YYYY-MM-DD HH:MM:SS'). "
            "Older SCD2 records are preserved."
        ),
    ),
    yes: bool = typer.Option(False, "--yes", "-y", help="Skip confirmation prompts"),
    force: bool = typer.Option(
        False, "--force", "-f", help="Re-process even if no new snapshots detected"
    ),
):
    """Historize snapshot data into SCD2 tables.

    Selects pipelines whose write_disposition contains "historize"
    (append+historize, merge+historize, or historize).

    Examples:
        saga historize --select "tag:daily"                          # Daily pipelines
        saga historize --select "filesystem__proffdata__*"           # Specific pipeline
        saga historize --full-refresh --select "..."                 # Full rebuild
        saga historize --partial-refresh --select "..."              # GDPR-safe rebuild
        saga historize --historize-from 2025-07-01 --select "..."    # From a specific date
        saga historize --verbose --target dev                        # Debug in dev
        saga historize --orchestrate --target prod                   # Distributed via Cloud Run
    """
    setup_logging(verbose)
    in_cloud_run = check_cloud_run_environment()

    _validate_historize_flags(full_refresh, partial_refresh, historize_from)

    # Worker mode
    if (get_env("SAGA_WORKER_MODE") or "").lower() == "true":
        logger.info("Running in worker mode (SAGA_WORKER_MODE=true)")
        profile_target = load_profile_config(profile, target)
        setup_execution_context(profile_target, force=force, full_refresh=full_refresh)
        run_worker_mode()
        return

    _confirm_historize_full_refresh(full_refresh, in_cloud_run)
    _confirm_partial_refresh(partial_refresh, historize_from, in_cloud_run, yes)

    # Load profile once — shared by orchestrator check and Session below
    profile_target = load_profile_config(profile, target)

    # Orchestrator mode
    if orchestrate:
        setup_execution_context(profile_target, force=force, full_refresh=full_refresh)
        provider = _resolve_orchestration_provider()
        selected_configs, _ = discover_and_select_configs(
            select, filter_fn=_is_historize_enabled
        )
        if not selected_configs:
            logger.error(
                "No historize-enabled pipelines matched the selection criteria. "
                "Use write_disposition 'append+historize' or 'historize' in pipeline config."
            )
            raise typer.Exit(1)
        validate_credentials(in_cloud_run)
        select_str = " ".join(select) if select else None
        execute_with_impersonation(
            profile_target,
            lambda: run_orchestrator_mode(
                selected_configs,
                debug_logging=verbose,
                command="historize",
                select_criteria=select_str,
                profile=profile,
                target=target,
                provider=provider,
            ),
        )
        return

    # Normal (local) execution via Session
    from dlt_saga.session import Session

    try:
        result = Session(
            profile=profile, target=target, _profile_target=profile_target
        ).historize(
            select=list(select) if select else None,
            workers=workers,
            full_refresh=full_refresh,
            partial_refresh=partial_refresh,
            historize_from=historize_from,
        )
    except AuthenticationError as e:
        logger.error(str(e))
        raise typer.Exit(1)
    if result.has_failures:
        raise typer.Exit(1)


@app.command("update-access")
def update_access(
    select: Optional[List[str]] = typer.Option(
        None, "--select", "-s", help=_SELECT_HELP
    ),
    verbose: bool = typer.Option(False, "--verbose", help="Enable debug logging"),
    workers: int = typer.Option(
        4, "--workers", "-w", help="Number of parallel workers"
    ),
    profile: Optional[str] = typer.Option(
        None, "--profile", help="Profile to use from profiles.yml"
    ),
    target: Optional[str] = typer.Option(
        None, "--target", help="Target within profile"
    ),
):
    """Update BigQuery access controls (IAM policies) without running pipelines.

    Examples:
        saga update-access                                  # All tables
        saga update-access --select "type:google_sheets"    # Specific type
        saga update-access --target prod                    # In production
    """
    setup_logging(verbose)

    logger.info("=" * 60)
    logger.info("UPDATE ACCESS MODE")
    logger.info("Only updating BigQuery access controls (IAM policies)")
    logger.info("Pipelines will NOT be executed")
    logger.info("=" * 60)

    from dlt_saga.session import Session

    result = Session(profile=profile, target=target).update_access(
        select=list(select) if select else None,
        workers=workers,
    )
    if result.has_failures:
        raise typer.Exit(1)


def _confirm_run_full_refresh(
    full_refresh: bool,
    in_cloud_run: bool,
    has_ingest: bool,
    has_historize: bool,
) -> tuple:
    """Ask about ingest and historize full refresh independently.

    Returns (refresh_ingest, refresh_historize). When full_refresh is False
    both are False (phases run incrementally). When the user declines both
    prompts the command exits.
    """
    if not full_refresh:
        return False, False

    if in_cloud_run:
        return True, True

    _sep = "=" * 70
    refresh_ingest = False
    refresh_historize = False
    if has_ingest:
        logger.warning(
            """INGEST FULL REFRESH MODE
%s
This will permanently delete:
  - All destination tables (main + staging)
  - Pipeline state metadata
  - Load tracking information
%s""",
            _sep,
            _sep,
        )
        refresh_ingest = typer.confirm("Rebuild ingested (raw) tables?", default=False)
    if has_historize:
        logger.warning(
            """HISTORIZE FULL REFRESH MODE
%s
This will permanently delete and rebuild:
  - All historized output tables
  - Historization log entries for selected pipelines
%s""",
            _sep,
            _sep,
        )
        refresh_historize = typer.confirm("Rebuild historized tables?", default=False)
    if not refresh_ingest and not refresh_historize:
        logger.info("Operation cancelled by user")
        raise typer.Exit(0)
    return refresh_ingest, refresh_historize


def _run_orchestrate(
    *,
    profile_target,
    select: Optional[List[str]],
    select_str: Optional[str],
    profile: str,
    target: Optional[str],
    force: bool,
    full_refresh: bool,
    verbose: bool,
    in_cloud_run: bool,
    start_value_override: Optional[str],
    end_value_override: Optional[str],
) -> None:
    """Handle the orchestrator path for ``saga run``.

    Creates an execution plan and triggers distributed workers.
    Raises typer.Exit on any failure.
    """
    setup_execution_context(
        profile_target,
        force=force,
        full_refresh=False,
        start_value_override=start_value_override,
        end_value_override=end_value_override,
    )
    provider = _resolve_orchestration_provider()

    ingest_configs, _ = discover_and_select_configs(
        select, filter_fn=_is_ingest_enabled
    )
    historize_configs, _ = discover_and_select_configs(
        select, filter_fn=_is_historize_enabled
    )
    if not ingest_configs and not historize_configs:
        logger.error("No pipelines matched the selection criteria")
        raise typer.Exit(1)
    validate_credentials(in_cloud_run)
    refresh_ingest, _ = _confirm_run_full_refresh(
        full_refresh, in_cloud_run, bool(ingest_configs), bool(historize_configs)
    )
    get_execution_context().full_refresh = refresh_ingest
    # Merge ingest + historize configs, dedup by identifier
    all_configs: Dict[str, List[PipelineConfig]] = {}
    for source in [ingest_configs, historize_configs]:
        for k, v in source.items():
            existing = {c.identifier for c in all_configs.get(k, [])}
            all_configs.setdefault(k, []).extend(
                c for c in v if c.identifier not in existing
            )
    execute_with_impersonation(
        profile_target,
        lambda: run_orchestrator_mode(
            all_configs,
            debug_logging=verbose,
            command="run",
            select_criteria=select_str,
            profile=profile,
            target=target,
            provider=provider,
        ),
    )


@app.command()
def run(
    select: Optional[List[str]] = typer.Option(
        None, "--select", "-s", help=_SELECT_HELP
    ),
    verbose: bool = typer.Option(False, "--verbose", help="Enable debug logging"),
    workers: int = typer.Option(
        4, "--workers", "-w", help="Number of parallel workers"
    ),
    orchestrate: bool = typer.Option(
        False,
        "--orchestrate/--no-orchestrate",
        help="Distribute execution via the configured orchestration provider",
    ),
    profile: Optional[str] = typer.Option(
        None, "--profile", help="Profile to use from profiles.yml"
    ),
    target: Optional[str] = typer.Option(
        None, "--target", help="Target within profile"
    ),
    force: bool = typer.Option(
        False, "--force", "-f", help="Force execution even if data hasn't changed"
    ),
    full_refresh: bool = typer.Option(
        False, "--full-refresh", help="Full refresh for both ingest and historize"
    ),
    partial_refresh: bool = typer.Option(
        False,
        "--partial-refresh",
        help="Partial refresh for historize phase only (preserves older SCD2 records)",
    ),
    historize_from: Optional[str] = typer.Option(
        None,
        "--historize-from",
        help=(
            "Reprocess historize phase from this date onwards "
            "(ISO format: YYYY-MM-DD or 'YYYY-MM-DD HH:MM:SS')"
        ),
    ),
    yes: bool = typer.Option(False, "--yes", "-y", help="Skip confirmation prompts"),
    start_value_override: Optional[str] = typer.Option(
        None,
        "--start-value-override",
        help="Override start value for incremental loading (backfill). Ignores incremental state.",
    ),
    end_value_override: Optional[str] = typer.Option(
        None,
        "--end-value-override",
        help="Override end value for incremental loading (backfill). Defaults to current period.",
    ),
):
    """Run both ingest and historize sequentially.

    Discovers all matching pipelines, runs ingest first, then historize.
    With --orchestrate, execution is distributed via the configured provider.

    Examples:
        saga run --select "tag:daily"                                   # Ingest + historize
        saga run --workers 8 --select "tag:daily"                       # Parallel
        saga run --full-refresh --select "..."                          # Full refresh both
        saga run --partial-refresh --select "..."                       # Partial refresh historize
        saga run --historize-from 2025-07-01 --select "..."             # Historize from date
        saga run --orchestrate --target prod                            # Distributed via Cloud Run
        saga run --select "type:api" --start-value-override "2026-01-01"  # Backfill
    """
    setup_logging(verbose)
    in_cloud_run = check_cloud_run_environment()
    select_str = " ".join(select) if select else None

    _validate_historize_flags(full_refresh, partial_refresh, historize_from)

    # Worker mode
    if (get_env("SAGA_WORKER_MODE") or "").lower() == "true":
        logger.info("Running in worker mode (SAGA_WORKER_MODE=true)")
        profile_target = load_profile_config(profile, target)
        setup_execution_context(
            profile_target,
            force=force,
            full_refresh=False,
            start_value_override=start_value_override,
            end_value_override=end_value_override,
        )
        run_worker_mode()
        return

    # Load profile once — shared by orchestrator check and Session below
    profile_target = load_profile_config(profile, target)

    # Orchestrator mode
    if orchestrate:
        _run_orchestrate(
            profile_target=profile_target,
            select=select,
            select_str=select_str,
            profile=profile,
            target=target,
            force=force,
            full_refresh=full_refresh,
            verbose=verbose,
            in_cloud_run=in_cloud_run,
            start_value_override=start_value_override,
            end_value_override=end_value_override,
        )
        return

    # Normal (local) execution via Session
    from dlt_saga.session import Session, SessionResult

    session = Session(profile=profile, target=target, _profile_target=profile_target)
    select_list = list(select) if select else None

    ingest_cfgs = session.discover(select_list, resource_type="ingest")
    historize_cfgs = session.discover(select_list, resource_type="historize")

    if not ingest_cfgs and not historize_cfgs:
        logger.error("No pipelines matched the selection criteria")
        raise typer.Exit(1)

    logger.info(
        "Selected %d ingest + %d historize pipeline(s)",
        len(ingest_cfgs),
        len(historize_cfgs),
    )

    refresh_ingest, refresh_historize = _confirm_run_full_refresh(
        full_refresh, in_cloud_run, bool(ingest_cfgs), bool(historize_cfgs)
    )
    _confirm_partial_refresh(partial_refresh, historize_from, in_cloud_run, yes)

    # When full_refresh: only run a phase if the user confirmed refresh for it.
    # When not full_refresh: run both phases incrementally.
    run_ingest = (not full_refresh) or refresh_ingest
    run_historize = (not full_refresh) or refresh_historize

    all_results = []
    try:
        if run_ingest and ingest_cfgs:
            r = session.ingest(
                select=select_list,
                workers=workers,
                force=force,
                full_refresh=refresh_ingest,
                start_value_override=start_value_override,
                end_value_override=end_value_override,
            )
            all_results.extend(r.pipeline_results)
        if run_historize and historize_cfgs:
            r = session.historize(
                select=select_list,
                workers=workers,
                full_refresh=refresh_historize,
                partial_refresh=partial_refresh,
                historize_from=historize_from,
            )
            all_results.extend(r.pipeline_results)
    except AuthenticationError as e:
        logger.error(str(e))
        raise typer.Exit(1)

    if SessionResult(pipeline_results=all_results).has_failures:
        raise typer.Exit(1)


@app.command()
def report(
    select: Optional[List[str]] = typer.Option(
        None, "--select", "-s", help=_SELECT_HELP
    ),
    output: str = typer.Option(
        "saga_report.html",
        "--output",
        "-o",
        help=(
            "Output destination. A local file path (e.g. 'report.html') or a remote "
            "storage URI (e.g. 'gs://my-bucket/reports/report.html'). "
            "Supported URI schemes: gs://, s3://, az://"
        ),
    ),
    days: int = typer.Option(
        14, "--days", "-d", help="Number of days of run history to include"
    ),
    open_browser: bool = typer.Option(
        True,
        "--open/--no-open",
        help="Open the report in a browser after generating (local output only)",
    ),
    verbose: bool = typer.Option(False, "--verbose", help="Enable debug logging"),
    profile: Optional[str] = typer.Option(
        None, "--profile", help="Profile to use from profiles.yml"
    ),
    target: Optional[str] = typer.Option(
        None, "--target", help="Target within profile"
    ),
):
    """Generate a standalone HTML observability report.

    Produces a single HTML file with a dashboard, pipeline catalog,
    and run history -- no server required.

    By default the report is saved locally and opened in a browser.
    Pass a remote URI to upload it instead:

    Examples:
        saga report                                              # Local, open browser
        saga report --days 7 --output weekly.html               # Local, open browser
        saga report --no-open --output report.html              # Local, no browser
        saga report --output gs://my-bucket/reports/saga.html    # Upload to GCS
        saga report --output gs://my-bucket/reports/saga.html --target prod
    """
    import os
    import tempfile

    from dlt_saga.report.uploader import is_remote_uri, upload_report

    setup_logging(verbose)

    profile_target = load_profile_config(profile, target)
    setup_execution_context(profile_target)

    # Discover all configs (no resource-type filter — report shows everything)
    selected_configs, _ = discover_and_select_configs(select)

    if not selected_configs:
        logger.error("No pipeline configs found matching selectors")
        raise typer.Exit(1)

    context = get_execution_context()
    environment = context.get_environment() or get_environment()
    project = context.get_database() or ""
    destination_type = context.get_destination_type()

    # Create a destination instance for querying run data.
    # Use schema from the first pipeline config (collector queries all schemas).
    first_config = next(iter(next(iter(selected_configs.values()))))
    dest_config_dict = {
        **first_config.config_dict,
        "schema_name": first_config.schema_name,
    }
    from dlt_saga.destinations.factory import DestinationFactory

    destination = DestinationFactory.create_from_context(
        destination_type, context, dest_config_dict
    )

    remote = is_remote_uri(output)
    uri = None

    def _generate():
        nonlocal uri
        from dlt_saga.report.collector import collect_report_data
        from dlt_saga.report.generator import generate_report
        from dlt_saga.utility.naming import get_execution_plan_schema

        destination.connect()
        logger.info(
            "Collecting report data (%d days of history, %d pipeline(s))",
            days,
            sum(len(v) for v in selected_configs.values()),
        )
        report_data = collect_report_data(
            configs=selected_configs,
            destination=destination,
            schema=first_config.schema_name,
            days=days,
            environment=environment,
            project=project,
            orchestration_schema=get_execution_plan_schema(),
        )

        if remote:
            with tempfile.NamedTemporaryFile(suffix=".html", delete=False) as tmp:
                tmp_path = tmp.name
            try:
                generate_report(report_data, tmp_path)
                uri = upload_report(tmp_path, output)
            finally:
                os.unlink(tmp_path)
        else:
            abs_path = generate_report(report_data, output)
            logger.info("Report generated: %s", abs_path)
            if open_browser:
                import webbrowser

                webbrowser.open(f"file://{abs_path}")

        destination.close()

    execute_with_impersonation(profile_target, _generate)
    if remote:
        typer.echo(uri)


# ---------------------------------------------------------------------------
# Orchestration commands (plan / worker)
# ---------------------------------------------------------------------------

_PLAN_COMMAND_HELP = "Command that workers will execute: ingest, historize, or run."


@app.command()
def plan(
    select: Optional[List[str]] = typer.Option(
        None, "--select", "-s", help=_SELECT_HELP
    ),
    command: str = typer.Option("ingest", "--command", "-c", help=_PLAN_COMMAND_HELP),
    verbose: bool = typer.Option(False, "--verbose", help="Enable debug logging"),
    profile: Optional[str] = typer.Option(
        None, "--profile", help="Profile to use from profiles.yml"
    ),
    target: Optional[str] = typer.Option(
        None, "--target", help="Target within profile"
    ),
    dry_run: bool = typer.Option(
        False, "--dry-run", help="Preview task assignments without persisting"
    ),
):
    """Create an execution plan without triggering workers.

    Outputs JSON metadata to stdout for external orchestrators.
    Use --dry-run to preview the task assignments without writing to the
    execution plan store.

    Examples:
        saga plan --select "tag:daily" --command run --target prod
        saga plan --select "tag:daily" --dry-run            # Preview only
        saga plan --command historize --select "group:filesystem"
    """
    setup_logging(verbose)
    profile_target = load_profile_config(profile, target)
    setup_execution_context(profile_target)

    # Validate command
    if command not in ("ingest", "historize", "run"):
        logger.error(
            "Invalid --command: '%s'. Must be: ingest, historize, or run",
            command,
        )
        raise typer.Exit(1)

    # Determine filter based on command
    if command == "ingest":
        filter_fn = _is_ingest_enabled
    elif command == "historize":
        filter_fn = _is_historize_enabled
    else:
        filter_fn = None  # "run" includes both

    selected_configs, _ = discover_and_select_configs(select, filter_fn=filter_fn)
    if not selected_configs:
        logger.error("No pipelines matched the selection criteria")
        raise typer.Exit(1)

    all_configs = flatten_configs(selected_configs)
    tasks = _build_task_assignments(all_configs)
    task_count = len(tasks)

    if dry_run:
        output = {
            "dry_run": True,
            "task_count": task_count,
            "pipeline_count": len(all_configs),
            "command": command,
            "tasks": tasks,
        }
        print(json.dumps(output, indent=2))
        return

    # Persist the plan
    in_cloud_run = check_cloud_run_environment()
    validate_credentials(in_cloud_run)

    def _create():
        from dlt_saga.utility.naming import get_execution_plan_schema
        from dlt_saga.utility.orchestration.execution_plan import (
            ExecutionMetadata,
            ExecutionPlanManager,
        )

        plan_dataset = get_execution_plan_schema()
        _prepare_destinations(selected_configs)
        destination = _create_orchestration_destination()

        plan_manager = ExecutionPlanManager(
            destination=destination, schema=plan_dataset
        )
        select_str = " ".join(select) if select else None
        context = get_execution_context()
        metadata = ExecutionMetadata(
            select_criteria=select_str,
            command=command,
            environment=context.get_environment() or get_environment(),
            profile=profile,
            target=target,
        )
        execution_id = plan_manager.create_execution_plan(
            all_configs, metadata=metadata
        )

        output = {
            "execution_id": execution_id,
            "task_count": task_count,
            "pipeline_count": len(all_configs),
            "command": command,
            "plan_dataset": plan_dataset,
        }
        print(json.dumps(output, indent=2))

    execute_with_impersonation(profile_target, _create)


@app.command()
def worker(
    execution_id: Optional[str] = typer.Option(
        None, "--execution-id", help="Execution plan ID (fallback: SAGA_EXECUTION_ID)"
    ),
    task_index: Optional[int] = typer.Option(
        None,
        "--task-index",
        help="Task index to execute (fallback: CLOUD_RUN_TASK_INDEX, SAGA_TASK_INDEX)",
    ),
    command: Optional[str] = typer.Option(
        None,
        "--command",
        "-c",
        help="Command to execute: ingest, historize, or run (fallback: SAGA_WORKER_COMMAND, default: ingest)",
    ),
    verbose: bool = typer.Option(False, "--verbose", help="Enable debug logging"),
    profile: Optional[str] = typer.Option(
        None, "--profile", help="Profile to use from profiles.yml"
    ),
    target: Optional[str] = typer.Option(
        None, "--target", help="Target within profile"
    ),
    force: bool = typer.Option(
        False, "--force", "-f", help="Force execution even if data hasn't changed"
    ),
    full_refresh: bool = typer.Option(
        False, "--full-refresh", help="Drop state/tables and reload from scratch"
    ),
    start_value_override: Optional[str] = typer.Option(
        None,
        "--start-value-override",
        help="Override start value for incremental loading (backfill)",
    ),
    end_value_override: Optional[str] = typer.Option(
        None,
        "--end-value-override",
        help="Override end value for incremental loading (backfill)",
    ),
):
    """Execute assigned pipelines from an execution plan.

    Reads task assignment from a persisted execution plan and runs the
    assigned pipelines. CLI args take precedence over environment variables.

    Examples:
        saga worker --execution-id abc123 --task-index 0 --command ingest
        saga worker --execution-id abc123 --task-index 3    # command from SAGA_WORKER_COMMAND
        saga worker                                         # all params from env vars
    """
    setup_logging(verbose)
    profile_target = load_profile_config(profile, target)
    setup_execution_context(
        profile_target,
        force=force,
        full_refresh=full_refresh,
        start_value_override=start_value_override,
        end_value_override=end_value_override,
    )
    execute_with_impersonation(
        profile_target,
        lambda: run_worker_mode(execution_id, task_index, command),
    )


# ---------------------------------------------------------------------------
# Init command
# ---------------------------------------------------------------------------


@app.command()
def init(
    no_input: bool = typer.Option(
        False,
        "--no-input",
        help="Skip all prompts and use defaults (duckdb, schema=dlt_dev, current directory).",
    ),
):
    """Scaffold a new dlt-saga consumer project.

    Creates profiles.yml, saga_project.yml, configs/, .dlt/config.toml,
    and a sample pipeline config. Skips any files that already exist.

    Examples:
        saga init                  # Interactive (asks directory, destination, schema)
        saga init --no-input       # Non-interactive with defaults (DuckDB)
    """
    from dlt_saga.init_command import run_init

    run_init(no_input=no_input)


@app.command("generate-schemas")
def generate_schemas_cmd(
    output_dir: Path = typer.Option(
        Path("schemas"),
        "--output-dir",
        "-o",
        help="Directory to write generated JSON schemas to.",
    ),
):
    """Generate JSON schemas for pipeline configs and project files.

    Introspects config dataclasses from all registered pipeline namespaces
    (built-in dlt_saga pipelines + any external packages in packages.yml)
    and writes JSON Schema files for IDE autocomplete and YAML validation.

    Also generates schemas for saga_project.yml, profiles.yml, and packages.yml.

    Examples:
        saga generate-schemas                  # Write to schemas/ (default)
        saga generate-schemas -o my_schemas/   # Write to custom directory
    """
    from dlt_saga.utility.generate_schemas import generate_schemas

    print(f"Generating schemas to {output_dir}/...")
    exit_code = generate_schemas(output_dir)
    if exit_code == 0:
        print(f"Done. Schemas written to {output_dir}/")
    else:
        print(f"Completed with errors. Schemas written to {output_dir}/")
        raise typer.Exit(code=1)


# ---------------------------------------------------------------------------
# info command
# ---------------------------------------------------------------------------


@app.command()
def info(
    verbose: bool = typer.Option(False, "--verbose", help="Enable debug logging"),
):
    """Show runtime environment: versions, plugins, destinations, config paths."""
    import sys
    from importlib.metadata import version as pkg_version

    from dlt_saga import PLUGIN_API_VERSION
    from dlt_saga.pipelines.registry import (
        _NAMESPACE_REGISTRY,
        _NAMESPACE_SOURCES,
        _ensure_packages_loaded,
    )
    from dlt_saga.project_config import get_config_source_settings as _get_cs_settings
    from dlt_saga.utility.cli.profiles import _find_profiles_file

    setup_logging(verbose)
    _ensure_packages_loaded()

    try:
        saga_ver = pkg_version("dlt-saga")
    except Exception:
        saga_ver = "unknown"
    try:
        dlt_ver = pkg_version("dlt")
    except Exception:
        dlt_ver = "unknown"

    typer.echo(f"dlt-saga {saga_ver}  (plugin API v{PLUGIN_API_VERSION})")
    typer.echo(f"dlt {dlt_ver}  |  Python {sys.version.split()[0]}")
    typer.echo("")

    typer.echo("Destinations:")
    from dlt_saga.destinations.factory import DestinationFactory

    for dest_type in DestinationFactory.get_available_types():
        typer.echo(f"  {dest_type}")
    typer.echo("")

    typer.echo("Pipeline namespaces:")
    for ns in sorted(_NAMESPACE_REGISTRY):
        base = _NAMESPACE_REGISTRY[ns]
        source = _NAMESPACE_SOURCES.get(ns, "unknown")
        typer.echo(f"  {ns:<20} {base}  ({source})")
    typer.echo("")

    settings = _get_cs_settings()
    typer.echo(f"Config source:  type={settings.type}")
    for p in settings.paths:
        exists = "(ok)" if os.path.isdir(p) else "(not found)"
        typer.echo(f"  {p}  {exists}")
    typer.echo("")

    profiles_path = _find_profiles_file()
    typer.echo(f"Profiles file:  {profiles_path or 'not found'}")


# ---------------------------------------------------------------------------
# doctor command
# ---------------------------------------------------------------------------


def _doctor_check(label: str, module_path: str) -> bool:
    """Try to import *module_path* and print OK / FAIL. Returns True on success."""
    import importlib

    try:
        importlib.import_module(module_path)
        typer.echo(f"  OK    {label}")
        return True
    except ImportError as e:
        typer.echo(f"  FAIL  {label}: {e}")
        return False


def _doctor_check_profile(
    profile: Optional[str],
    target: Optional[str],
    verbose: bool,
    emit: Callable[..., None],
) -> tuple:
    """Load and validate profile. Returns (profile_target, context, dest_type) or raises Exit."""
    import traceback

    from dlt_saga.utility.cli.common import resolve_profile_name

    try:
        resolved_profile = resolve_profile_name(profile)
        profile_target = load_profile_config(profile, target)
        setup_execution_context(profile_target)
        context = get_execution_context()
        dest_type = context.get_destination_type() or "unknown"
        env = context.get_environment() or "unknown"
        active_target = target or (profile_target.name if profile_target else "dev")
        emit(
            "\u2713",
            "profiles.yml",
            f"[{resolved_profile} → {active_target}, {dest_type}, {env}]",
        )
        return profile_target, context, dest_type
    except Exception as e:
        emit("\u2717", "profiles.yml", str(e) if verbose else str(e).splitlines()[0])
        if verbose:
            typer.echo(traceback.format_exc())
        typer.echo("")
        typer.echo("Cannot continue without a valid profile.")
        raise typer.Exit(1)


def _doctor_check_project(verbose: bool, emit: Callable[..., None]) -> bool:
    """Validate saga_project.yml. Returns True on success."""
    import traceback

    from dlt_saga.project_config import get_project_config

    try:
        get_project_config()
        emit("\u2713", "saga_project.yml")
        return True
    except Exception as e:
        emit(
            "\u2717", "saga_project.yml", str(e) if verbose else str(e).splitlines()[0]
        )
        if verbose:
            typer.echo(traceback.format_exc())
        return False


def _doctor_check_configs(verbose: bool, emit: Callable[..., None]) -> dict:
    """Discover pipeline configs. Returns selected_configs (empty dict on failure)."""
    import traceback

    try:
        selected, _ = discover_and_select_configs(None)
        total = sum(len(v) for v in selected.values())
        emit(
            "\u2713",
            "Pipeline configs",
            f"{total} pipeline(s) in {len(selected)} group(s)",
        )
        return selected
    except Exception as e:
        emit(
            "\u2717", "Pipeline configs", str(e) if verbose else str(e).splitlines()[0]
        )
        if verbose:
            typer.echo(traceback.format_exc())
        return {}


def _doctor_check_connection(
    dest_type: str,
    context: "ExecutionContext",
    selected_configs: dict,
    profile_target: Optional["ProfileTarget"],
    verbose: bool,
    emit: Callable[..., None],
) -> bool:
    """Test destination connection. Returns True on success."""
    import traceback

    from dlt_saga.destinations.factory import DestinationFactory

    try:
        flat = flatten_configs(selected_configs) if selected_configs else []
        first_cfg = flat[0] if flat else None
        dest_config_dict = (
            {**first_cfg.config_dict, "schema_name": first_cfg.schema_name}
            if first_cfg
            else {}
        )
        destination = DestinationFactory.create_from_context(
            dest_type, context, dest_config_dict
        )

        def _test() -> None:
            destination.connect()
            destination.close()

        execute_with_impersonation(profile_target, _test)
        emit("\u2713", f"Connection ({dest_type})")
        return True
    except Exception as e:
        emit(
            "\u2717",
            f"Connection ({dest_type})",
            str(e) if verbose else str(e).splitlines()[0],
        )
        if verbose:
            typer.echo(traceback.format_exc())
        return False


@app.command()
def doctor(
    verbose: bool = typer.Option(False, "--verbose", help="Enable debug logging"),
    profile: Optional[str] = typer.Option(
        None, "--profile", help="Profile to use from profiles.yml"
    ),
    target: Optional[str] = typer.Option(
        None, "--target", help="Target within profile"
    ),
):
    """Validate configuration and test destination connectivity.

    Checks profiles, project config, pipeline discovery, destination connection,
    and registered pipeline plugins — similar to dbt debug.

    For interactive OAuth (Databricks U2M), the browser will open during the
    connection check, establishing credentials before any pipeline work.
    """
    from importlib.metadata import entry_points

    from dlt_saga.pipelines.registry import _NAMESPACE_REGISTRY, _ensure_packages_loaded

    setup_logging(verbose)

    def _emit(symbol: str, label: str, detail: str = "") -> None:
        suffix = f"  {detail}" if detail else ""
        typer.echo(f"  {symbol}  {label}{suffix}")

    typer.echo("")

    profile_target, context, dest_type = _doctor_check_profile(
        profile, target, verbose, _emit
    )
    ok = _doctor_check_project(verbose, _emit)
    selected_configs = _doctor_check_configs(verbose, _emit)
    ok = (
        _doctor_check_connection(
            dest_type, context, selected_configs, profile_target, verbose, _emit
        )
        and ok
    )

    _ensure_packages_loaded()
    external_ns = {
        ns: base for ns, base in _NAMESPACE_REGISTRY.items() if ns != "dlt_saga"
    }
    pip_eps = list(entry_points(group="dlt_saga.pipelines"))
    if external_ns or pip_eps:
        typer.echo("")
        for ns, base in sorted(external_ns.items()):
            if not _doctor_check(f"  {ns} → {base} (packages.yml)", base):
                ok = False
        for ep in pip_eps:
            base_module = ep.value.split(":")[0]
            if not _doctor_check(f"  {ep.name} ({base_module})", base_module):
                ok = False

    typer.echo("")
    if ok:
        typer.echo("All checks passed.")
    else:
        typer.echo("One or more checks failed. Run with --verbose for details.")
        raise typer.Exit(1)


# ---------------------------------------------------------------------------
# Entry points
# ---------------------------------------------------------------------------


def main_saga():
    """Entry point for the saga CLI."""
    try:
        app()
    except KeyboardInterrupt:
        logger.info("Operation cancelled by user")
        raise SystemExit(1)


if __name__ == "__main__":
    main_saga()
