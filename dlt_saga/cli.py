# CLI for the saga data pipeline framework
#
# Single entry point: saga
# Subcommands: list, ingest, historize, run

import json
import logging
import os
import textwrap
from datetime import datetime
from pathlib import Path
from typing import TYPE_CHECKING, Dict, List, Optional

if TYPE_CHECKING:
    from dlt_saga.session import SessionResult

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
from dlt_saga.utility.cli.logging import configure_cli_logging, reenable_saga_loggers
from dlt_saga.utility.cli.prompts import (
    _confirm_destroy,
    _confirm_full_refresh,
    _confirm_historize_full_refresh,
    _confirm_partial_refresh,
    _confirm_run_full_refresh,
)
from dlt_saga.utility.cli.run_modes import (
    _build_task_assignments,
    _create_orchestration_destination,
    _prepare_destinations,
    _resolve_orchestration_provider,
    _warn_workers_ignored_in_worker_mode,
    run_orchestrator_mode,
    run_worker_mode,
)
from dlt_saga.utility.cli.selectors import format_config_list
from dlt_saga.utility.collisions import TargetCollisionError
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
        "-V",
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


def _validate_historize_flags(
    full_refresh: bool,
    partial_refresh: bool,
    historize_from: Optional[str],
    orchestrate: bool = False,
) -> None:
    """Validate mutual exclusivity and format of historize flags."""
    flag_count = sum([full_refresh, partial_refresh, historize_from is not None])
    if flag_count > 1:
        logger.error(
            "--full-refresh, --partial-refresh, and --historize-from are mutually exclusive"
        )
        raise typer.Exit(1)

    # Partial-refresh scope isn't carried in the execution plan, so workers
    # would silently run plain incremental. Reject rather than mislead — the
    # banner would otherwise imply a partial refresh that never happens.
    if orchestrate and (partial_refresh or historize_from is not None):
        logger.error(
            "--partial-refresh / --historize-from are not supported with "
            "--orchestrate (the refresh scope isn't propagated to workers). "
            "Run the partial refresh locally, without --orchestrate."
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
# Exit-boundary failure reporting
# ---------------------------------------------------------------------------


def _exit_if_failures(result: "SessionResult", command_name: str) -> None:
    """Surface each per-pipeline failure and exit non-zero if any failed.

    Writes the summary with ``typer.echo(err=True)`` rather than the
    ``logging`` module on purpose. Saga's loggers can be silenced mid-run
    by any host that calls ``logging.config.dictConfig(...)`` with the
    default ``disable_existing_loggers=True`` — Airflow's logging init
    is the most common trigger, but it can come from anywhere in the
    import graph. A logger-based summary then vanishes silently and the
    operator is left with a bare ``exit 1`` (see #94, #97). stderr
    bypasses that whole subsystem.

    Also runs the broader saga-logger rescue here as a defense in depth
    so subsequent shutdown logging isn't silently dropped.
    """
    if not result.has_failures:
        return
    reenable_saga_loggers()
    typer.echo(
        f"{command_name} failed: {result.failed}/{len(result.pipeline_results)} "
        "pipeline(s) failed",
        err=True,
    )
    for failure in result.failures:
        error = failure.error or "(no error message)"
        # Put each failure's (possibly multi-line) error in its own block, indented
        # under the pipeline name and preceded by a blank line, so several failures
        # stay visually distinct instead of running together — and each keeps its
        # own issue-specific message and remediation.
        typer.echo(f"\n  {failure.pipeline_name}:", err=True)
        typer.echo(textwrap.indent(error, "      "), err=True)
    raise typer.Exit(1)


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
    verbose: bool = typer.Option(False, "--verbose", "-v", help="Enable debug logging"),
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

    # Set the execution context so discovery resolves schema names within the
    # profile (notably the dev schema). Without it, dev discovery resolves
    # outside the profile and can't see its schema.
    profile_target = load_profile_config(profile, target)
    if profile_target is not None:
        setup_execution_context(profile_target)

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
    verbose: bool = typer.Option(False, "--verbose", "-v", help="Enable debug logging"),
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
        saga validate --select "group:google_sheets"         # Validate specific group
        saga validate --select "tag:daily"                  # Validate by tag
    """
    setup_logging(verbose)
    # Set the execution context so discovery resolves schema names within the
    # profile (notably the dev schema), matching the other commands.
    profile_target = load_profile_config(profile, target)
    if profile_target is not None:
        setup_execution_context(profile_target)

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
    verbose: bool = typer.Option(False, "--verbose", "-v", help="Enable debug logging"),
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
    yes: bool = typer.Option(False, "--yes", "-y", help="Skip confirmation prompts"),
):
    """Run data ingestion pipelines.

    Selects pipelines whose write_disposition has an ingest component
    (append, merge, replace — including append+historize).

    Examples:
        saga ingest                                         # All ingest-enabled
        saga ingest --select "tag:daily"                    # By tag
        saga ingest --select "group:google_sheets"           # By group
        saga ingest --workers 8 --select "tag:daily"        # Parallel
        saga ingest --orchestrate --target prod             # Distributed via Cloud Run
        saga ingest --full-refresh --select "pipeline_name" # Drop and reload
        saga ingest --select "group:api" --start-value-override "2026-01-01"  # Backfill
    """
    setup_logging(verbose)
    in_cloud_run = check_cloud_run_environment()

    # Worker mode: needs execution context set up before dispatching.
    # Cloud Run only forwards a fixed set of env vars to workers, so the
    # worker process never sees the orchestrator's CLI flags. Honor env-var
    # fallbacks for the runtime overrides we propagate (force).
    if (get_env("SAGA_WORKER_MODE") or "").lower() == "true":
        logger.info("Running in worker mode (SAGA_WORKER_MODE=true)")
        profile_target = load_profile_config(profile, target)
        effective_force = force or (get_env("SAGA_FORCE") or "").lower() == "true"
        setup_execution_context(
            profile_target,
            force=effective_force,
            full_refresh=full_refresh,
            start_value_override=start_value_override,
            end_value_override=end_value_override,
        )
        _warn_workers_ignored_in_worker_mode(workers)
        # Orchestrated runs always set SAGA_WORKER_COMMAND (authoritative); the
        # typed subcommand is the fallback for a manual `saga ingest` worker.
        run_worker_mode(worker_command=get_env("SAGA_WORKER_COMMAND") or "ingest")
        return

    _confirm_full_refresh(full_refresh, in_cloud_run, yes)

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
                start_value_override=start_value_override,
                end_value_override=end_value_override,
                force=force,
                workers=workers,
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
    except (AuthenticationError, TargetCollisionError) as e:
        logger.error(str(e))
        raise typer.Exit(1)
    _exit_if_failures(result, "Ingest")


@app.command()
def historize(
    select: Optional[List[str]] = typer.Option(
        None, "--select", "-s", help=_SELECT_HELP
    ),
    verbose: bool = typer.Option(False, "--verbose", "-v", help="Enable debug logging"),
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
):
    """Historize snapshot data into SCD2 tables.

    Selects pipelines whose write_disposition contains "historize"
    (append+historize, replace+historize, or historize).

    Examples:
        saga historize --select "tag:daily"                          # Daily pipelines
        saga historize --select "filesystem__vendor__*"           # Specific pipeline
        saga historize --full-refresh --select "..."                 # Full rebuild
        saga historize --partial-refresh --select "..."              # GDPR-safe rebuild
        saga historize --historize-from 2025-07-01 --select "..."    # From a specific date
        saga historize --verbose --target dev                        # Debug in dev
        saga historize --orchestrate --target prod                   # Distributed via Cloud Run
    """
    setup_logging(verbose)
    in_cloud_run = check_cloud_run_environment()

    _validate_historize_flags(
        full_refresh, partial_refresh, historize_from, orchestrate
    )

    # Worker mode
    if (get_env("SAGA_WORKER_MODE") or "").lower() == "true":
        logger.info("Running in worker mode (SAGA_WORKER_MODE=true)")
        profile_target = load_profile_config(profile, target)
        # historize has no change-detection to override, so there is no `force`
        # (unlike ingest) — it keys on actual snapshot values, not a proxy mtime.
        setup_execution_context(profile_target, full_refresh=full_refresh)
        _warn_workers_ignored_in_worker_mode(workers)
        # Orchestrated runs always set SAGA_WORKER_COMMAND (authoritative); the
        # typed subcommand is the fallback for a manual `saga historize` worker
        # (which previously fell through to ingest).
        run_worker_mode(worker_command=get_env("SAGA_WORKER_COMMAND") or "historize")
        return

    _confirm_historize_full_refresh(full_refresh, in_cloud_run, yes)
    _confirm_partial_refresh(partial_refresh, historize_from, in_cloud_run, yes)

    # Load profile once — shared by orchestrator check and Session below
    profile_target = load_profile_config(profile, target)

    # Orchestrator mode
    if orchestrate:
        setup_execution_context(profile_target, full_refresh=full_refresh)
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
                workers=workers,
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
    except (AuthenticationError, TargetCollisionError) as e:
        logger.error(str(e))
        raise typer.Exit(1)
    _exit_if_failures(result, "Historize")


@app.command()
def destroy(
    select: Optional[List[str]] = typer.Option(
        None, "--select", "-s", help=_SELECT_HELP
    ),
    resource_type: str = typer.Option(
        "all",
        "--resource-type",
        help="Which layer to tear down: ingest, historize, or all",
    ),
    dry_run: bool = typer.Option(
        False,
        "--dry-run",
        help="Show what would be dropped without deleting anything",
    ),
    verbose: bool = typer.Option(False, "--verbose", "-v", help="Enable debug logging"),
    workers: int = typer.Option(
        4, "--workers", "-w", help="Number of parallel workers"
    ),
    profile: Optional[str] = typer.Option(
        None, "--profile", help="Profile to use from profiles.yml"
    ),
    target: Optional[str] = typer.Option(
        None, "--target", help="Target within profile"
    ),
    yes: bool = typer.Option(False, "--yes", "-y", help="Skip confirmation prompts"),
):
    """Remove a pipeline's warehouse footprint (tables + state) without reloading.

    The teardown counterpart to ``--full-refresh``: drop, but no rebuild. Run it
    to decommission a pipeline before deleting or disabling its config, so no
    stale tables or state are left behind. Selects disabled configs too.

    Ownership is authoritative — only tables saga's own state records this
    pipeline as having created are dropped (ingest tables from _saga_load_info,
    historized tables from _saga_historize_log), never a re-derived name — so a
    coincidental name match on another pipeline is never touched.

    Examples:
        saga destroy --select "group:google_sheets" --dry-run   # Preview first
        saga destroy --select "filesystem__old_report__*"       # Both layers
        saga destroy --select "tag:deprecated" --resource-type historize
    """
    setup_logging(verbose)
    in_cloud_run = check_cloud_run_environment()

    if resource_type not in ("all", "ingest", "historize"):
        logger.error(
            "Invalid --resource-type: '%s'. Must be: ingest, historize, or all",
            resource_type,
        )
        raise typer.Exit(1)

    _confirm_destroy(dry_run, resource_type, in_cloud_run, yes)

    profile_target = load_profile_config(profile, target)

    from dlt_saga.session import Session

    try:
        result = Session(
            profile=profile, target=target, _profile_target=profile_target
        ).destroy(
            select=list(select) if select else None,
            resource_type=resource_type,
            workers=workers,
            dry_run=dry_run,
        )
    except AuthenticationError as e:
        logger.error(str(e))
        raise typer.Exit(1)
    _exit_if_failures(result, "Destroy")


@app.command("update-access")
def update_access(
    select: Optional[List[str]] = typer.Option(
        None, "--select", "-s", help=_SELECT_HELP
    ),
    verbose: bool = typer.Option(False, "--verbose", "-v", help="Enable debug logging"),
    workers: int = typer.Option(
        4, "--workers", "-w", help="Number of parallel workers"
    ),
    profile: Optional[str] = typer.Option(
        None, "--profile", help="Profile to use from profiles.yml"
    ),
    target: Optional[str] = typer.Option(
        None, "--target", help="Target within profile"
    ),
    dry_run: bool = typer.Option(
        False,
        "--dry-run",
        help=(
            "Preview the access changes that would be applied without "
            "calling BigQuery's update_dataset / set_iam_policy. Log "
            "output is the same shape as a real run, prefixed with "
            "[DRY RUN] and using `would grant`/`would revoke` verbs."
        ),
    ),
):
    """Update destination access controls without running pipelines.

    Applies table-level access for the configured destination (BigQuery IAM
    policies, Databricks Unity Catalog GRANT/REVOKE).

    Examples:
        saga update-access                                  # All tables
        saga update-access --select "group:google_sheets"    # Specific group
        saga update-access --target prod                    # In production
        saga update-access --dry-run --target prod          # Preview only
    """
    setup_logging(verbose)

    logger.info("=" * 60)
    logger.info("UPDATE ACCESS MODE%s", " (DRY RUN)" if dry_run else "")
    logger.info("Only updating destination access controls")
    logger.info("Pipelines will NOT be executed")
    if dry_run:
        logger.info("DRY RUN: no access changes will be applied")
    logger.info("=" * 60)

    from dlt_saga.session import Session
    from dlt_saga.utility.cli.context import get_execution_context

    result = Session(profile=profile, target=target).update_access(
        select=list(select) if select else None,
        workers=workers,
        dry_run=dry_run,
    )
    # Config errors (lockout / missing-OWNER) and per-table reconcile failures
    # are logged as they occur rather than raised, so the operator sees every
    # broken dataset/table in one pass. Exit non-zero here if any fired.
    context = get_execution_context()
    if (
        result.has_failures
        or context.access_config_error_count > 0
        or context.access_error_count > 0
    ):
        raise typer.Exit(1)


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
    workers: int,
    yes: bool = False,
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
        full_refresh, in_cloud_run, bool(ingest_configs), bool(historize_configs), yes
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
            start_value_override=start_value_override,
            end_value_override=end_value_override,
            force=force,
            workers=workers,
        ),
    )


@app.command()
def run(
    select: Optional[List[str]] = typer.Option(
        None, "--select", "-s", help=_SELECT_HELP
    ),
    verbose: bool = typer.Option(False, "--verbose", "-v", help="Enable debug logging"),
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
        saga run --select "group:api" --start-value-override "2026-01-01"  # Backfill
    """
    setup_logging(verbose)
    in_cloud_run = check_cloud_run_environment()
    select_str = " ".join(select) if select else None

    _validate_historize_flags(
        full_refresh, partial_refresh, historize_from, orchestrate
    )

    # Worker mode
    if (get_env("SAGA_WORKER_MODE") or "").lower() == "true":
        logger.info("Running in worker mode (SAGA_WORKER_MODE=true)")
        profile_target = load_profile_config(profile, target)
        effective_force = force or (get_env("SAGA_FORCE") or "").lower() == "true"
        setup_execution_context(
            profile_target,
            force=effective_force,
            full_refresh=False,
            start_value_override=start_value_override,
            end_value_override=end_value_override,
        )
        _warn_workers_ignored_in_worker_mode(workers)
        # Orchestrated runs always set SAGA_WORKER_COMMAND (authoritative); the
        # typed subcommand is the fallback for a manual `saga run` worker.
        run_worker_mode(worker_command=get_env("SAGA_WORKER_COMMAND") or "run")
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
            workers=workers,
            yes=yes,
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
        full_refresh, in_cloud_run, bool(ingest_cfgs), bool(historize_cfgs), yes
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
    except (AuthenticationError, TargetCollisionError) as e:
        logger.error(str(e))
        raise typer.Exit(1)

    _exit_if_failures(SessionResult(pipeline_results=all_results), "Run")


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
        14,
        "--days",
        "-d",
        min=1,
        help="Number of days of run history to include (must be >= 1)",
    ),
    open_browser: bool = typer.Option(
        True,
        "--open/--no-open",
        help="Open the report in a browser after generating (local output only)",
    ),
    verbose: bool = typer.Option(False, "--verbose", "-v", help="Enable debug logging"),
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
        try:
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

                    # Path.as_uri() builds a valid file:// URI on every OS — an
                    # f-string produces a malformed one on Windows (backslashes,
                    # drive letter: "file://C:\reports\r.html").
                    webbrowser.open(Path(abs_path).as_uri())
        finally:
            # Always release the destination connection, even if collection or
            # generation raises — otherwise the connection leaks on failure.
            destination.close()

    execute_with_impersonation(profile_target, _generate)
    if remote:
        typer.echo(uri)


# ---------------------------------------------------------------------------
# Orchestration commands (plan / worker)
# ---------------------------------------------------------------------------

_PLAN_COMMAND_HELP = "Command that workers will execute: ingest, historize, or run."


def _validate_execution_id(execution_id: str) -> str:
    """Validate execution ID format. Accepts UUIDs or alphanumeric+dash+underscore.

    Raises:
        ValueError: If format is invalid
    """
    import re

    if len(execution_id) > 255:
        raise ValueError(
            f"execution_id must be <= 255 characters (got {len(execution_id)})"
        )
    if not re.match(r"^[a-zA-Z0-9\-_]+$", execution_id):
        raise ValueError(
            "execution_id must contain only alphanumeric characters, dashes, and underscores"
        )
    return execution_id


@app.command()
def plan(
    select: Optional[List[str]] = typer.Option(
        None, "--select", "-s", help=_SELECT_HELP
    ),
    command: str = typer.Option("ingest", "--command", "-c", help=_PLAN_COMMAND_HELP),
    execution_id: Optional[str] = typer.Option(
        None,
        "--execution-id",
        help=(
            "Explicit execution ID for this plan. "
            "If not provided, a UUID is auto-generated. Useful for multi-stage orchestration "
            "where you need to reference the same execution across separate jobs."
        ),
    ),
    verbose: bool = typer.Option(False, "--verbose", "-v", help="Enable debug logging"),
    profile: Optional[str] = typer.Option(
        None, "--profile", help="Profile to use from profiles.yml"
    ),
    target: Optional[str] = typer.Option(
        None, "--target", help="Target within profile"
    ),
    dry_run: bool = typer.Option(
        False, "--dry-run", help="Preview task assignments without persisting"
    ),
    compact: bool = typer.Option(
        False,
        "--compact",
        help=(
            "Emit JSON on a single line instead of pretty-printed. "
            "Use when piping into a parser or when each log entry is "
            "captured separately (e.g. Cloud Logging from a Cloud Run job)."
        ),
    ),
):
    """Create an execution plan without triggering workers.

    Outputs JSON metadata to stdout for external orchestrators.
    Use --dry-run to preview the task assignments without writing to the
    execution plan store. Use --execution-id to provide a deterministic ID
    for multi-stage orchestration workflows (plan → worker).

    Examples:
        saga plan --select "tag:daily" --command run --target prod
        saga plan --select "tag:daily" --dry-run                                    # Preview only
        saga plan --command historize --select "group:filesystem"
        saga plan --select "tag:daily" --compact                                    # Single-line JSON
        saga plan --select "tag:daily" --execution-id my-run-001 --target prod      # Explicit ID
    """
    setup_logging(verbose)

    # Validate execution_id format if provided
    if execution_id:
        try:
            execution_id = _validate_execution_id(execution_id)
        except ValueError as e:
            logger.error("Invalid --execution-id: %s", e)
            raise typer.Exit(1)
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
        print(json.dumps(output, indent=None if compact else 2))
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
        resolved_execution_id = plan_manager.create_execution_plan(
            all_configs, metadata=metadata, execution_id=execution_id
        )

        output = {
            "execution_id": resolved_execution_id,
            "task_count": task_count,
            "pipeline_count": len(all_configs),
            "command": command,
            "plan_dataset": plan_dataset,
        }
        print(json.dumps(output, indent=None if compact else 2))

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
    workers: Optional[int] = typer.Option(
        None,
        "--workers",
        "-w",
        help=(
            "Cap on parallel pipelines within this worker task. "
            "Fallback: SAGA_WORKER_CONCURRENCY env var, then "
            "orchestration.worker_concurrency in saga_project.yml, then 4."
        ),
    ),
    verbose: bool = typer.Option(False, "--verbose", "-v", help="Enable debug logging"),
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
    effective_force = force or (get_env("SAGA_FORCE") or "").lower() == "true"
    setup_execution_context(
        profile_target,
        force=effective_force,
        full_refresh=full_refresh,
        start_value_override=start_value_override,
        end_value_override=end_value_override,
    )
    execute_with_impersonation(
        profile_target,
        lambda: run_worker_mode(execution_id, task_index, command, workers=workers),
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


new_app = typer.Typer(
    help="Scaffold new project components (e.g. a custom pipeline adapter)."
)
app.add_typer(new_app, name="new")


@new_app.command("adapter")
def new_adapter(
    name: Optional[str] = typer.Argument(
        None, help="Adapter name in snake_case (e.g. my_service). Prompted if omitted."
    ),
    group: Optional[str] = typer.Option(
        None,
        "--group",
        "-g",
        help="Pipeline group / config subfolder (e.g. api, crm, ads). "
        "Prompted if omitted (default: api).",
    ),
    kind: Optional[str] = typer.Option(
        None,
        "--kind",
        "-k",
        help="Template: 'generic' (BasePipeline), 'api' (BaseApiPipeline), or "
        "'api-date-window' (DateWindowApiPipeline). "
        "Prompted if omitted (default: generic).",
    ),
    pkg_dir: str = typer.Option(
        "pipelines",
        "--path",
        help="Local package directory that holds pipeline implementations.",
    ),
    namespace: str = typer.Option(
        "local",
        "--namespace",
        help="Namespace registered in packages.yml for this package directory.",
    ),
    no_input: bool = typer.Option(
        False,
        "--no-input",
        help="Skip prompts: require name as an argument, use defaults for kind/group.",
    ),
):
    """Scaffold a custom pipeline adapter from a known-good template.

    Generates a config.py + pipeline.py + starter pipeline config that already
    follow the framework conventions (the real extract_data() contract,
    idempotent incremental loading, secret-URI-aware credentials, reuse of the
    inherited BaseConfig vocabulary), and registers the package in packages.yml.

    Prompts for name, kind, and group when they aren't supplied (pass --no-input
    to skip prompts in scripts/CI).

    Examples:
        saga new adapter                            # fully interactive (prompts)
        saga new adapter my_service                 # generic source under configs/api/
        saga new adapter orders --group database    # under configs/database/
        saga new adapter weather --kind api         # REST API (BaseApiPipeline)
    """
    from dlt_saga.new_adapter_command import (
        effective_namespace,
        existing_adapter_files,
        resolve_inputs,
        run_new_adapter,
    )

    try:
        name, group, kind = resolve_inputs(name, group, kind, no_input)
    except ValueError as exc:
        typer.secho(f"Error: {exc}", fg=typer.colors.YELLOW, err=True)
        raise typer.Exit(code=1)

    # The adapter may reuse a namespace already mapped to this directory.
    ns = effective_namespace(Path.cwd(), namespace, pkg_dir)

    # Collision guard — prominent and left-aligned, before the create summary.
    existing = existing_adapter_files(Path.cwd(), pkg_dir, group, name)
    if existing:
        typer.secho(
            f"Adapter '{ns}.{group}.{name}' already exists at {existing[0]}.",
            fg=typer.colors.YELLOW,
            bold=True,
            err=True,
        )
        typer.secho(
            "Choose a different name or --group, or edit the existing files.",
            fg=typer.colors.YELLOW,
            err=True,
        )
        raise typer.Exit(code=1)

    if not no_input:
        typer.echo("")
        typer.echo("About to create:")
        typer.echo(f"  adapter : {ns}.{group}.{name}   (kind: {kind})")
        typer.echo(f"  package : {pkg_dir}/{group}/{name}/   (config.py, pipeline.py)")
        typer.echo(f"  config  : configs/{group}/{name}.yml")
        if ns != namespace:
            typer.echo(
                f"  note    : reusing namespace '{ns}' already registered "
                f"for ./{pkg_dir} (not adding '{namespace}')"
            )
        if not typer.confirm("Proceed?", default=True):
            typer.echo("Aborted.")
            raise typer.Exit()

    try:
        run_new_adapter(
            name=name,
            group=group,
            kind=kind,
            pkg_dir=pkg_dir,
            namespace=namespace,
        )
    except ValueError as exc:
        typer.secho(f"Error: {exc}", fg=typer.colors.YELLOW, err=True)
        raise typer.Exit(code=1)


@new_app.command("config")
def new_config(
    name: Optional[str] = typer.Argument(
        None, help="Config name in snake_case (e.g. orders). Prompted if omitted."
    ),
    adapter: Optional[str] = typer.Option(
        None,
        "--adapter",
        "-a",
        help="Adapter the config targets (e.g. dlt_saga.api). Prompted if omitted.",
    ),
    group: Optional[str] = typer.Option(
        None,
        "--group",
        "-g",
        help="Pipeline group / config subfolder. Prompted if omitted (default: api).",
    ),
    schema_dir: str = typer.Option(
        "schemas",
        "--schema-dir",
        help="Directory the generated JSON schemas live in (for the modeline).",
    ),
    no_input: bool = typer.Option(
        False,
        "--no-input",
        help="Skip prompts: require name + --adapter, use defaults for group.",
    ),
):
    """Scaffold a pipeline config YAML for an existing adapter.

    Introspects the adapter's config dataclass (the same metadata that powers
    `saga generate-schemas`) and writes configs/<group>/<name>.yml pre-populated
    with the available fields — required ones active, optional ones commented
    with their descriptions — plus a schema modeline when the schema exists.

    Examples:
        saga new config orders --adapter dlt_saga.database --group database
        saga new config sales --adapter dlt_saga.api
    """
    from dlt_saga.new_config_command import resolve_config_inputs, run_new_config

    try:
        name, group, adapter = resolve_config_inputs(name, group, adapter, no_input)
    except ValueError as exc:
        typer.secho(f"Error: {exc}", fg=typer.colors.YELLOW, err=True)
        raise typer.Exit(code=1)

    if not no_input:
        typer.echo("")
        typer.echo("About to create:")
        typer.echo(f"  config  : configs/{group}/{name}.yml")
        typer.echo(f"  adapter : {adapter}")
        if not typer.confirm("Proceed?", default=True):
            typer.echo("Aborted.")
            raise typer.Exit()

    try:
        run_new_config(name=name, adapter=adapter, group=group, schema_dir=schema_dir)
    except ValueError as exc:
        typer.secho(f"Error: {exc}", fg=typer.colors.YELLOW, err=True)
        raise typer.Exit(code=1)


@app.command("generate-schemas")
def generate_schemas_cmd(
    output_dir: Path = typer.Option(
        Path("schemas"),
        "--output-dir",
        "-o",
        help="Directory to write generated JSON schemas to.",
    ),
    link: bool = typer.Option(
        True,
        "--link/--no-link",
        help=(
            "After generating, add/update a yaml-language-server modeline in each "
            "config file pointing at its adapter's schema. Use --no-link to only "
            "write schema files (e.g. in CI)."
        ),
    ),
):
    """Generate JSON schemas for pipeline configs and project files, and link configs to them.

    Introspects config dataclasses from all registered pipeline namespaces
    (built-in dlt_saga pipelines + any external packages in packages.yml)
    and writes JSON Schema files for IDE autocomplete and YAML validation.

    Also generates schemas for saga_project.yml, profiles.yml, and packages.yml.

    By default, every discovered config file is then matched to its adapter's
    schema via a ``# yaml-language-server: $schema=...`` modeline (idempotent,
    editor-agnostic). The project-level files (saga_project.yml, packages.yml,
    and an in-project profiles.yml) are linked the same way; an external
    profiles.yml is left untouched, with a suggested modeline printed instead.
    Generating first guarantees brand-new pipelines have a schema before any
    link is written.

    Examples:
        saga generate-schemas                  # Generate + link configs
        saga generate-schemas --no-link        # Only write schema files
        saga generate-schemas -o my_schemas/   # Write to custom directory
    """
    from dlt_saga.utility.generate_schemas import generate_schemas

    print(f"Generating schemas to {output_dir}/...")
    exit_code = generate_schemas(output_dir)
    if exit_code == 0:
        print(f"Done. Schemas written to {output_dir}/")
    else:
        print(f"Completed with errors. Schemas written to {output_dir}/")

    if link:
        _link_config_schemas(output_dir)

    if exit_code != 0:
        raise typer.Exit(code=1)


def _link_config_schemas(output_dir: Path) -> None:
    """Match each config file to its adapter's schema; print a summary."""
    from dlt_saga.utility.link_schemas import link_config_schemas

    print("Linking config files to their schemas...")
    try:
        results = link_config_schemas(output_dir)
    except Exception as e:
        print(f"  [WARN] Could not link config schemas: {e}")
        return

    changed = [r for r in results if r.changed]
    skipped = [r for r in results if r.schema_filename is None]
    suggested = [r for r in results if r.suggestion]
    for r in changed:
        print(f"  [LINK] {r.config_path} -> {r.schema_filename}")
    for r in skipped:
        print(f"  [SKIP] {r.config_path}: {r.skipped_reason}")
    for r in suggested:
        print(f"  [NOTE] {r.suggestion}")
    print(
        f"Linked {len(changed)} config file(s) "
        f"({len(results) - len(skipped)} matched, {len(skipped)} skipped)."
    )


# ---------------------------------------------------------------------------
# info command
# ---------------------------------------------------------------------------


@app.command()
def info(
    verbose: bool = typer.Option(False, "--verbose", "-v", help="Enable debug logging"),
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
# lint command
# ---------------------------------------------------------------------------


@app.command()
def lint(
    all_adapters: bool = typer.Option(
        False,
        "--all",
        help="Also lint built-in and installed (third-party) adapters "
        "(default: only your project's own adapters).",
    ),
    verbose: bool = typer.Option(False, "--verbose", "-v", help="Enable debug logging"),
):
    """Check adapters and configs for convention anti-patterns.

    Flags secret-by-name config fields, field names that diverge from the
    standard vocabulary, and non-idempotent "load yesterday" date math. Lints
    only your project's own adapters (editable code inside the project) by
    default, since built-in and installed third-party adapters aren't yours to
    change; pass --all to lint everything.

    Exits non-zero when any issue is found, so it can gate CI.
    """
    setup_logging(verbose)

    from dlt_saga.lint import SEVERITY_ERROR, run_lint

    findings = run_lint(include_all=all_adapters)

    if not findings:
        typer.secho("No issues found.", fg=typer.colors.GREEN)
        return

    by_adapter: Dict[str, list] = {}
    for f in findings:
        by_adapter.setdefault(f.adapter, []).append(f)

    for adapter in sorted(by_adapter):
        typer.echo("")
        typer.secho(adapter, bold=True)
        for f in by_adapter[adapter]:
            colour = (
                typer.colors.RED
                if f.severity == SEVERITY_ERROR
                else typer.colors.YELLOW
            )
            where = f" ({f.location})" if f.location else ""
            typer.secho(f"  [{f.code}]{where}", fg=colour)
            typer.echo(f"    {f.message}")
            if f.suggestion:
                typer.echo(f"    -> {f.suggestion}")

    typer.echo("")
    typer.secho(
        f"Found {len(findings)} issue(s) across {len(by_adapter)} adapter(s).",
        fg=typer.colors.YELLOW,
    )
    raise typer.Exit(code=1)


# ---------------------------------------------------------------------------
# doctor command
# ---------------------------------------------------------------------------


@app.command()
def doctor(
    verbose: bool = typer.Option(False, "--verbose", "-v", help="Enable debug logging"),
    profile: Optional[str] = typer.Option(
        None, "--profile", help="Profile to use from profiles.yml"
    ),
    target: Optional[str] = typer.Option(
        None, "--target", help="Target within profile"
    ),
    select: Optional[List[str]] = typer.Option(
        None,
        "--select",
        "-s",
        help=(
            "Selector(s) to scope the config check and list each matched "
            "pipeline's resolved project.schema.table target."
        ),
    ),
):
    """Validate configuration and test destination connectivity.

    Checks the active dlt-saga build, profiles, project config, pipeline
    discovery (with resolved schema), destination connection, and registered
    pipeline plugins — similar to dbt debug.

    Use ``--select`` to print the fully resolved ``project.schema.table`` for
    specific pipelines without running them — handy for confirming a profile's
    ``env_var()`` schema resolved as expected, or which build is active after
    switching between an editable install and the pinned release.

    For interactive OAuth (Databricks U2M), the browser will open during the
    connection check, establishing credentials before any pipeline work.
    """
    from importlib.metadata import entry_points

    from dlt_saga.pipelines.registry import _NAMESPACE_REGISTRY, _ensure_packages_loaded
    from dlt_saga.utility.cli.doctor import (
        _doctor_check,
        _doctor_check_collisions,
        _doctor_check_configs,
        _doctor_check_destination,
        _doctor_check_legacy_keys,
        _doctor_check_profile,
        _doctor_check_project,
        _doctor_emit_version,
    )

    setup_logging(verbose)

    def _emit(symbol: str, label: str, detail: str = "") -> None:
        suffix = f"  {detail}" if detail else ""
        typer.echo(f"  {symbol}  {label}{suffix}")

    typer.echo("")

    _doctor_emit_version(_emit)
    profile_target, context, dest_type = _doctor_check_profile(
        profile, target, verbose, _emit
    )
    ok = _doctor_check_project(verbose, _emit)
    selected_configs = _doctor_check_configs(select, context, verbose, _emit)
    ok = _doctor_check_collisions(selected_configs, verbose, _emit) and ok
    # Advisory (never flips `ok`): nudge migration of deprecated config keys.
    _doctor_check_legacy_keys(selected_configs, verbose, _emit)
    ok = (
        _doctor_check_destination(
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

    # AI context staleness check (non-blocking)
    try:
        from dlt_saga.ai_setup_command import check_staleness

        ai_warning = check_staleness(Path.cwd())
        if ai_warning:
            typer.echo("")
            _emit("!", "AI context", ai_warning)
    except Exception as exc:
        logger.debug("AI context staleness check failed: %s", exc)

    typer.echo("")
    if ok:
        typer.echo("All checks passed.")
    else:
        typer.echo("One or more checks failed. Run with --verbose for details.")
        raise typer.Exit(1)


# ---------------------------------------------------------------------------
# maintenance command
# ---------------------------------------------------------------------------


@app.command()
def maintenance(
    verbose: bool = typer.Option(False, "--verbose", "-v", help="Enable debug logging"),
    profile: Optional[str] = typer.Option(
        None, "--profile", help="Profile to use from profiles.yml"
    ),
    target: Optional[str] = typer.Option(
        None, "--target", help="Target within profile"
    ),
    select: Optional[List[str]] = typer.Option(
        None,
        "--select",
        "-s",
        help="Selector(s) to scope which pipeline schemas are swept.",
    ),
    dry_run: bool = typer.Option(
        False,
        "--dry-run",
        help="Report what would change without writing anything.",
    ),
):
    """Reconcile and compact saga's internal bookkeeping tables.

    Runs three passes across the project's schemas:

    - **Clustering reconcile** — internal log tables created by older versions of
      saga were never clustered, so reads over a large log scan more than they
      need to. This applies the current clustering (metadata-only; the warehouse
      reclusters in the background, so it is safe on live tables).
    - **Log compaction** — the status logs (native_load, execution-plan)
      accumulate superseded rows. This collapses each to the latest row per key,
      deleting only strictly-superseded earlier rows plus dangling stale
      ``started`` rows. Lossless: reads already collapse to the latest row per
      key.
    - **Stale-task reconcile** — a crashed or never-scheduled orchestration task
      leaves a dangling ``running``/``pending`` execution-plan row forever. This
      relabels ones older than the stale cutoff to ``abandoned`` (``failed`` is
      reserved for genuine worker-reported failures).

    All are one-time-style migrations that become cheap no-ops once tables are
    up to date.

    Examples:
        saga maintenance --dry-run                 # Preview
        saga maintenance                           # Apply across all schemas
        saga maintenance --select "group:filesystem"
    """
    from dlt_saga.maintenance import run_maintenance

    setup_logging(verbose)

    profile_target = load_profile_config(profile, target)
    if profile_target is not None:
        setup_execution_context(profile_target)
    context = get_execution_context()

    selected_configs, _ = discover_and_select_configs(select)
    if not selected_configs:
        logger.error("No pipeline configs found matching selectors")
        raise typer.Exit(1)

    execute_with_impersonation(
        profile_target,
        lambda: run_maintenance(context, selected_configs, dry_run),
    )


# ---------------------------------------------------------------------------
# AI setup command
# ---------------------------------------------------------------------------


@app.command("ai-setup")
def ai_setup():
    """Generate an AI context file for this project.

    Writes saga_ai_context.md to the current directory, containing framework
    patterns and implementation guidance that AI coding assistants can use.

    The generated file is version-stamped. Re-run after upgrading dlt-saga to
    keep it current.

    Examples:
        saga ai-setup
    """
    from dlt_saga.ai_setup_command import run_ai_setup

    run_ai_setup()


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
