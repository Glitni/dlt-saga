"""Programmatic API for the dlt-saga framework.

Provides the ``Session`` class for running pipelines from Python code
(Airflow DAGs, Jupyter notebooks, custom orchestrators) without going
through the CLI.

Example::

    import dlt_saga

    session = dlt_saga.Session(target="dev")
    configs = session.discover(select=["tag:daily"])
    result = session.ingest(select=["tag:daily"], workers=4)
    if result.has_failures:
        raise RuntimeError(f"{result.failed} pipeline(s) failed")
"""

import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional, TypeVar

from dlt_saga.pipeline_config import ConfigSource, PipelineConfig
from dlt_saga.pipeline_config.file_config import FilePipelineConfig
from dlt_saga.pipelines.executor import execute_pipeline
from dlt_saga.project_config import get_config_source_settings
from dlt_saga.utility.auth.providers import get_auth_provider
from dlt_saga.utility.cli.common import flatten_configs
from dlt_saga.utility.cli.context import execution_context_scope
from dlt_saga.utility.cli.profiles import ProfileTarget, get_profiles_config
from dlt_saga.utility.cli.reporting import summarize_load_info
from dlt_saga.utility.cli.selectors import PipelineSelector

logger = logging.getLogger(__name__)

_T = TypeVar("_T")


# ---------------------------------------------------------------------------
# Config source plugin loader
# ---------------------------------------------------------------------------


# ---------------------------------------------------------------------------
# Result types
# ---------------------------------------------------------------------------


@dataclass
class PipelineResult:
    """Result of a single pipeline execution."""

    pipeline_name: str
    success: bool
    error: Optional[str] = None
    load_info: Optional[Any] = None
    config: Optional[PipelineConfig] = None
    # True when the failure was a pre-run config/validation error (the run never
    # started) — developer feedback, not a run outcome. Excluded from the
    # `saga report` telemetry recorded by `_record_run`.
    config_error: bool = False


@dataclass
class SessionResult:
    """Aggregated result of a session operation (ingest, historize, or run)."""

    pipeline_results: List[PipelineResult] = field(default_factory=list)

    @property
    def succeeded(self) -> int:
        """Number of pipelines that completed successfully."""
        return sum(1 for r in self.pipeline_results if r.success)

    @property
    def failed(self) -> int:
        """Number of pipelines that failed."""
        return sum(1 for r in self.pipeline_results if not r.success)

    @property
    def has_failures(self) -> bool:
        """Whether any pipeline failed."""
        return any(not r.success for r in self.pipeline_results)

    @property
    def failures(self) -> List[PipelineResult]:
        """Only the failed pipeline results."""
        return [r for r in self.pipeline_results if not r.success]

    def __repr__(self) -> str:
        return f"SessionResult(succeeded={self.succeeded}, failed={self.failed})"


# ---------------------------------------------------------------------------
# Session
# ---------------------------------------------------------------------------


class Session:
    """Programmatic entry point for the dlt-saga framework.

    Encapsulates profile configuration, config discovery, authentication,
    and pipeline execution — everything the CLI does, but callable from
    Python code.

    Args:
        profile: Profile name from profiles.yml. When ``None`` (the default),
            resolved through the standard chain: ``SAGA_PROFILE`` env var →
            ``profile:`` in saga_project.yml → ``"default"``. Explicit values
            are used as-is.
        target: Target within the profile (e.g., ``"dev"``, ``"prod"``).
            If None, uses the profile's default target.
        config_dir: Override path for pipeline config directory. If None,
            uses the path from ``saga_project.yml``.

    Example::

        session = Session(target="dev")
        result = session.ingest(select=["tag:daily"], workers=4)
    """

    def __init__(
        self,
        profile: Optional[str] = None,
        target: Optional[str] = None,
        config_dir: Optional[str] = None,
        _profile_target: Optional[ProfileTarget] = None,
    ):
        from dlt_saga.defaults import apply_dlt_defaults

        apply_dlt_defaults()

        # Load profile (skip if pre-loaded by caller to avoid duplicate logging)
        self._profile_target = (
            _profile_target
            if _profile_target is not None
            else self._load_profile(profile, target)
        )

        # Create config source
        self._config_source = self._create_config_source(config_dir)

        # Resolve auth provider
        auth_provider_name = None
        destination_type = None
        if self._profile_target:
            auth_provider_name = self._profile_target.auth_provider
            destination_type = self._profile_target.destination_type
        self._auth_provider = get_auth_provider(
            auth_provider=auth_provider_name,
            destination_type=destination_type,
        )

        # Load lifecycle hooks (idempotent across multiple Session instances)
        from dlt_saga.hooks.loader import load_hooks

        load_hooks()

        # Re-enable saga loggers if a host's logging.config.dictConfig has
        # disabled them. Notably: Airflow (its config init runs with the default
        # disable_existing_loggers=True), but any caller can do the same. By
        # the time we get here, all transitive imports for this Session have
        # happened — so this is the latest natural choke point to undo a
        # foreign disable before pipelines start logging. See #97.
        from dlt_saga.utility.cli.logging import reenable_saga_loggers

        reenable_saga_loggers()

    # -------------------------------------------------------------------
    # Public API
    # -------------------------------------------------------------------

    def discover(
        self,
        select: Optional[List[str]] = None,
        resource_type: str = "all",
    ) -> List[PipelineConfig]:
        """Discover and select pipeline configs.

        Args:
            select: Selector expressions (dbt-style). None = all.
            resource_type: Filter by ``"ingest"``, ``"historize"``, or ``"all"``.

        Returns:
            Flat list of matching PipelineConfig objects.
        """
        filter_fn = self._resource_type_filter(resource_type)
        # Resolve inside the profile's execution context so environment-aware
        # names (notably the dev schema from a profile's ``env_var()``) bind to
        # the profile rather than the bare ``dlt_dev``/``SAGA_SCHEMA_NAME``
        # fallback. The config source memoizes discovery, so resolving this
        # unscoped would cache the fallback names and poison later scoped runs
        # that share the same Session — the cause of ``saga run`` (which
        # pre-discovers for its pipeline count) landing in ``dlt_dev`` while
        # ``saga ingest`` resolved correctly.
        with execution_context_scope(self._profile_target):
            configs, _ = self._discover_and_select(select, filter_fn)
        return flatten_configs(configs)

    def ingest(
        self,
        select: Optional[List[str]] = None,
        workers: int = 4,
        force: bool = False,
        full_refresh: bool = False,
        start_value_override: Optional[str] = None,
        end_value_override: Optional[str] = None,
    ) -> SessionResult:
        """Run data ingestion pipelines.

        Args:
            select: Selector expressions. None = all ingest-enabled.
            workers: Number of parallel workers.
            force: Force execution even if source hasn't changed.
            full_refresh: Drop state/tables and reload from scratch.
            start_value_override: Override start value for incremental loading.
            end_value_override: Override end value for incremental loading.

        Returns:
            SessionResult with per-pipeline outcomes.
        """
        with execution_context_scope(
            self._profile_target,
            force=force,
            full_refresh=full_refresh,
            start_value_override=start_value_override,
            end_value_override=end_value_override,
        ):

            def _ingest_and_record() -> SessionResult:
                result = self._run_ingest(select, workers)
                self._record_run("ingest", select, result)
                return result

            return self._execute_with_auth(_ingest_and_record)

    def historize(
        self,
        select: Optional[List[str]] = None,
        workers: int = 4,
        full_refresh: bool = False,
        partial_refresh: bool = False,
        historize_from: Optional[str] = None,
    ) -> SessionResult:
        """Historize snapshot data into SCD2 tables.

        Args:
            select: Selector expressions. None = all historize-enabled.
            workers: Number of parallel workers.
            full_refresh: Rebuild historized tables from all raw data.
            partial_refresh: Rebuild from earliest available raw snapshot,
                preserving older SCD2 records.
            historize_from: Reprocess from this ISO date/datetime onwards.

        Returns:
            SessionResult with per-pipeline outcomes.
        """
        with execution_context_scope(
            self._profile_target,
            full_refresh=full_refresh,
        ):

            def _historize_and_record() -> SessionResult:
                result = self._run_historize(
                    select, workers, full_refresh, partial_refresh, historize_from
                )
                self._record_run("historize", select, result)
                return result

            return self._execute_with_auth(_historize_and_record)

    def run(
        self,
        select: Optional[List[str]] = None,
        workers: int = 4,
        force: bool = False,
        full_refresh: bool = False,
        partial_refresh: bool = False,
        historize_from: Optional[str] = None,
        start_value_override: Optional[str] = None,
        end_value_override: Optional[str] = None,
    ) -> SessionResult:
        """Run both ingest and historize sequentially.

        Ingest runs first. If ``full_refresh`` is True and an ingest fails,
        historize is skipped for that pipeline. ``partial_refresh`` and
        ``historize_from`` apply only to the historize phase.

        Args:
            select: Selector expressions. None = all enabled.
            workers: Number of parallel workers.
            force: Force ingest even if source hasn't changed.
            full_refresh: Full refresh for both ingest and historize.
            partial_refresh: Partial refresh for historize only.
            historize_from: Reprocess historize from this date only.
            start_value_override: Override start value for incremental loading.
            end_value_override: Override end value for incremental loading.

        Returns:
            SessionResult combining ingest and historize outcomes.
        """
        with execution_context_scope(
            self._profile_target,
            force=force,
            full_refresh=full_refresh,
            start_value_override=start_value_override,
            end_value_override=end_value_override,
        ):

            def _run_and_record() -> SessionResult:
                result = self._run_both(
                    select, workers, full_refresh, partial_refresh, historize_from
                )
                self._record_run("run", select, result)
                return result

            return self._execute_with_auth(_run_and_record)

    def destroy(
        self,
        select: Optional[List[str]] = None,
        resource_type: str = "all",
        workers: int = 4,
        dry_run: bool = False,
    ) -> SessionResult:
        """Remove selected pipelines' warehouse footprint without reloading.

        The teardown counterpart to ``--full-refresh`` (drop, but no rebuild) —
        for decommissioning a pipeline before its config is deleted or disabled.
        Drops only what state proves each pipeline created (ingest tables from
        ``_saga_load_info``, historized tables from ``_saga_historize_log``), so
        a coincidental name match on another pipeline is never dropped.

        Unlike the run commands, this selects **disabled configs too** — the
        natural point to tear a pipeline down is after it has been disabled.

        Args:
            select: Selector expressions. None = all matching configs.
            resource_type: Which layer(s) to tear down: ``"ingest"``,
                ``"historize"``, or ``"all"``.
            workers: Number of parallel workers.
            dry_run: When True, report what would be dropped without deleting.

        Returns:
            SessionResult with per-pipeline outcomes.
        """
        if resource_type not in ("all", "ingest", "historize"):
            raise ValueError(
                f"Invalid resource_type: '{resource_type}'. "
                f"Must be: 'ingest', 'historize', or 'all'."
            )
        with execution_context_scope(self._profile_target):
            return self._execute_with_auth(
                lambda: self._run_destroy(select, resource_type, workers, dry_run)
            )

    def update_access(
        self,
        select: Optional[List[str]] = None,
        workers: int = 4,
        dry_run: bool = False,
    ) -> SessionResult:
        """Update access controls (e.g., BigQuery IAM policies) without running pipelines.

        Syncs per-pipeline ``schema_access`` (under ``pipelines:`` in
        saga_project.yml) and, if configured, ``orchestration.schema_access``
        on the orchestration schema — letting external orchestrators (Airflow,
        Dagster, Prefect) read the execution plan they triggered.

        Args:
            select: Selector expressions. None = all ingest-enabled.
            workers: Number of parallel workers.
            dry_run: When True, compute the diff and emit log lines as if
                applying, but skip the BigQuery PATCH / set_iam_policy /
                create_dataset calls. Use to preview the change set before
                committing.

        Returns:
            SessionResult with per-pipeline outcomes.
        """
        with execution_context_scope(
            self._profile_target,
            update_access=True,
            dry_run=dry_run,
        ):
            return self._execute_with_auth(
                lambda: self._run_update_access(select, workers)
            )

    def _run_update_access(
        self, select: Optional[List[str]], workers: int
    ) -> SessionResult:
        """Orchestration schema sync first, then per-pipeline access sync.

        Order matters for log readability: with the orchestration sync
        running first, its `Updated access controls for dataset …` line
        appears alongside the per-pipeline sync logs in chronological order,
        rather than at the end after a long quiet pipeline iteration. There's
        no functional dependency between the two — both write to BigQuery
        independently — so the reordering is purely about log clarity.
        """
        self._apply_orchestration_access()
        return self._run_ingest(select, workers)

    @staticmethod
    def _apply_orchestration_access() -> None:
        """Apply ``orchestration.schema_access`` to the orchestration schema.

        No-op when the field is unset or when the destination doesn't support
        orchestration (only BigQuery today — the Cloud Run provider writes the
        execution plan there).
        """
        from dlt_saga.project_config import get_orchestration_config
        from dlt_saga.utility.cli.context import get_execution_context
        from dlt_saga.utility.naming import get_execution_plan_schema, is_production

        orchestration_config = get_orchestration_config()
        if not orchestration_config.schema_access:
            logger.debug(
                "No orchestration.schema_access configured; "
                "skipping orchestration schema access sync"
            )
            return

        # orchestration.* is prod-only infrastructure. In dev the execution-plan
        # tables live in the developer's own schema; applying these (prod service
        # account) grants there would be wrong. Matches the per-pipeline access
        # sync, which is likewise prod-only.
        if not is_production():
            logger.debug("orchestration.schema_access is prod-only; skipping in dev")
            return

        context = get_execution_context()
        destination_type = context.get_destination_type()
        if destination_type != "bigquery":
            logger.warning(
                "orchestration.schema_access is set but destination type "
                "%r does not support orchestration access sync; skipping. "
                "Apply the grant out-of-band for now.",
                destination_type,
            )
            return

        project_id = context.get_database()
        if not project_id:
            logger.warning(
                "Cannot apply orchestration.schema_access: no project "
                "configured in the execution context"
            )
            return

        from dlt_saga.destinations.bigquery.base import BigQueryBaseDestination

        schema = get_execution_plan_schema()
        location = context.get_location() or "EU"

        # No "Applying orchestration.schema_access …" log here — the
        # downstream `Updated access controls for dataset X` line (from
        # `_update_access_if_needed`) names the dataset and shows the diff.
        # Silence on no-op runs is consistent with the per-pipeline path.
        logger.debug(
            "Applying orchestration.schema_access to %s.%s (%d entries)",
            project_id,
            schema,
            len(orchestration_config.schema_access),
        )
        BigQueryBaseDestination._sync_dataset_and_access_static(
            project_id=project_id,
            location=location,
            dataset_name=schema,
            schema_access=orchestration_config.schema_access,
        )

    def maintenance(
        self,
        select: Optional[List[str]] = None,
        dry_run: bool = False,
    ) -> Dict[str, Dict[str, int]]:
        """Reconcile and compact saga's internal bookkeeping tables.

        Runs three passes over the internal log tables (native_load, historize,
        and execution-plan logs): a clustering reconcile for tables that predate
        clustering-at-CREATE, a lossless compaction of the status logs (collapse
        superseded rows to the latest per key), and a stale-task reconcile that
        relabels dangling ``pending``/``running`` execution-plan rows as
        ``abandoned``. All become cheap no-ops once tables are up to date. The
        programmatic counterpart of ``saga maintenance``.

        Args:
            select: Selector expressions scoping which pipeline schemas are
                swept. None = all.
            dry_run: When True, report what would change without writing.

        Returns:
            Per-pass status counts::

                {"clustering": {"absent", "unchanged", "reconciled"},
                 "compaction": {"absent", "collapsed", "orphaned"},
                 "reconcile": {"abandoned"}}
        """
        with execution_context_scope(self._profile_target, dry_run=dry_run):
            return self._execute_with_auth(
                lambda: self._run_maintenance(select, dry_run)
            )

    def _run_maintenance(
        self, select: Optional[List[str]], dry_run: bool
    ) -> Dict[str, Dict[str, int]]:
        from dlt_saga.maintenance import run_maintenance
        from dlt_saga.utility.cli.context import get_execution_context

        selected, _ = self._discover_and_select(select)
        if not selected:
            logger.warning("No pipeline configs found matching selectors")
            return {
                "clustering": {"absent": 0, "unchanged": 0, "reconciled": 0},
                "compaction": {"absent": 0, "collapsed": 0, "orphaned": 0},
                "reconcile": {"abandoned": 0},
            }

        return run_maintenance(get_execution_context(), selected, dry_run)

    # -------------------------------------------------------------------
    # Internal: initialization helpers
    # -------------------------------------------------------------------

    @staticmethod
    def _load_profile(
        profile: Optional[str], target: Optional[str]
    ) -> Optional[ProfileTarget]:
        """Load profile target from profiles.yml.

        ``profile`` may be ``None`` when the caller didn't pass one explicitly
        (CLI without ``--profile``, ``dlt_saga.Session(target=...)``, etc.).
        Resolution goes through the standard chain — ``resolve_profile_name``
        — so ``SAGA_PROFILE`` env var and ``profile:`` in ``saga_project.yml``
        are honoured before falling back to ``"default"``.
        """
        from dlt_saga.utility.cli.common import resolve_profile_name

        profiles_config = get_profiles_config()
        if not profiles_config.profiles_exist():
            logger.debug("No profiles.yml found, using environment variables")
            return None
        resolved = resolve_profile_name(profile)
        try:
            return profiles_config.get_target(resolved, target)
        except Exception as e:
            logger.warning(
                "Failed to load profile: %s. Falling back to environment variables.", e
            )
            return None

    @staticmethod
    def _create_config_source(config_dir: Optional[str]) -> ConfigSource:
        """Create a config source instance.

        Resolution order:
        1. Explicit ``config_dir`` argument (single directory, file type).
        2. ``config_source:`` in ``saga_project.yml``:
           - ``type: file`` (default) → FilePipelineConfig with ``paths`` list.
           - Any other type → load from ``dlt_saga.config_sources`` entry points.
        """
        if config_dir:
            return FilePipelineConfig(root_dir=config_dir)

        settings = get_config_source_settings()

        if settings.type != "file":
            raise ValueError(
                f"Unknown config_source type '{settings.type}'. "
                "Only type: file is supported. "
                "Remove the type: field or set it to 'file'."
            )

        return FilePipelineConfig(root_dir=settings.paths)

    # -------------------------------------------------------------------
    # Internal: discovery
    # -------------------------------------------------------------------

    def _discover_and_select(
        self,
        select: Optional[List[str]],
        filter_fn: Optional[Callable[[PipelineConfig], bool]] = None,
        warn_on_no_match: bool = True,
    ) -> tuple:
        """Discover configs using this session's config source and apply selectors.

        Args:
            select: Selector expressions. None = all.
            filter_fn: Optional pre-selection filter (e.g. ingest-enabled).
            warn_on_no_match: Whether an empty enabled-set match warns. Callers
                that also target the disabled set (``destroy``) pass ``False`` —
                a match may live only in the disabled set, so the enabled-set
                miss is expected and its own no-match message is reported once
                over the union instead.

        Returns:
            Tuple of (selected_enabled, selected_disabled) dicts.
        """
        all_enabled, all_disabled = self._config_source.discover()

        if filter_fn:
            all_enabled = {
                ptype: [c for c in configs if filter_fn(c)]
                for ptype, configs in all_enabled.items()
            }
            all_enabled = {k: v for k, v in all_enabled.items() if v}

        enabled_selector = PipelineSelector(all_enabled)
        selected = enabled_selector.select(select, warn_on_no_match=warn_on_no_match)

        # The disabled set is only probed to report matches that are disabled;
        # a non-match here is expected and must not warn (it would contradict a
        # successful enabled match).
        disabled_selector = PipelineSelector(all_disabled)
        selected_disabled = disabled_selector.select(select, warn_on_no_match=False)

        return selected, selected_disabled

    @staticmethod
    def _resource_type_filter(
        resource_type: str,
    ) -> Optional[Callable[[PipelineConfig], bool]]:
        """Return a filter function for the given resource type."""
        if resource_type == "ingest":
            return lambda c: c.ingest_enabled
        elif resource_type == "historize":
            return lambda c: c.historize_enabled
        elif resource_type == "all":
            return None
        raise ValueError(
            f"Invalid resource_type: '{resource_type}'. "
            f"Must be: 'ingest', 'historize', or 'all'."
        )

    # -------------------------------------------------------------------
    # Internal: auth wrapper
    # -------------------------------------------------------------------

    def _execute_with_auth(self, callback: Callable[[], _T]) -> _T:
        """Validate credentials and run callback with impersonation if needed."""
        self._auth_provider.validate()

        run_as = self._profile_target.run_as if self._profile_target else None

        if run_as:
            with self._auth_provider.impersonate(run_as):
                return callback()
        return callback()

    def _record_run(
        self,
        command: str,
        select: Optional[List[str]],
        result: SessionResult,
    ) -> None:
        """Record this local run's per-pipeline outcomes for `saga report`.

        Local (non-orchestrated) runs would otherwise leave no trace of
        failures — `_saga_load_info` only logs successes. This writes terminal
        outcomes into the same `_saga_execution_plans` / `_saga_executions`
        tables the orchestrated path uses (flagged ``is_orchestrated = False``),
        so failed local runs show up in the report. Best-effort: never raises.

        For a combined ``run`` (ingest + historize) a pipeline can produce two
        results; they're merged into one row per pipeline, failing if either
        phase failed.
        """
        if not result.pipeline_results:
            return
        try:
            from dlt_saga.destinations.factory import DestinationFactory
            from dlt_saga.utility.cli.context import get_execution_context
            from dlt_saga.utility.naming import get_execution_plan_schema
            from dlt_saga.utility.orchestration.execution_plan import (
                ExecutionMetadata,
                ExecutionPlanManager,
            )

            # Merge per pipeline: failed if any phase failed.
            by_pipeline: Dict[str, Dict[str, Any]] = {}
            for r in result.pipeline_results:
                if r.config is None:
                    continue
                # A pre-run config/validation error means the run never started —
                # developer feedback, not a run outcome. Skip it (don't record it
                # as a failed run, and don't spin up a destination/connection just
                # to record nothing).
                if r.config_error:
                    continue
                entry = by_pipeline.get(r.pipeline_name)
                if entry is None:
                    by_pipeline[r.pipeline_name] = {
                        "pipeline_type": r.config.pipeline_group,
                        "pipeline_identifier": r.config.identifier,
                        "table_name": r.config.table_name,
                        "status": "completed" if r.success else "failed",
                        "error_message": r.error,
                    }
                elif not r.success:
                    entry["status"] = "failed"
                    entry["error_message"] = entry["error_message"] or r.error

            records = list(by_pipeline.values())
            if not records:
                return

            context = get_execution_context()
            destination = DestinationFactory.create_from_context(
                context.get_destination_type(), context, {"schema_name": ""}
            )
            metadata = ExecutionMetadata(
                select_criteria=" ".join(select) if select else None,
                command=command,
                environment=context.get_environment(),
                target=self._profile_target.name if self._profile_target else None,
                start_value_override=context.start_value_override,
                end_value_override=context.end_value_override,
            )
            ExecutionPlanManager(
                destination=destination, schema=get_execution_plan_schema()
            ).record_local_run(records, metadata=metadata)
        except Exception as e:  # pragma: no cover - best-effort telemetry
            logger.debug("Could not record local run outcomes: %s", e)

    # -------------------------------------------------------------------
    # Internal: ingest execution
    # -------------------------------------------------------------------

    def _run_ingest(
        self,
        select: Optional[List[str]],
        workers: int,
    ) -> SessionResult:
        """Discover ingest-enabled configs and execute them."""
        configs, _ = self._discover_and_select(
            select, filter_fn=lambda c: c.ingest_enabled
        )
        all_configs = flatten_configs(configs)

        if not all_configs:
            logger.info("No ingest-enabled pipelines matched the selection criteria")
            return SessionResult()

        # `_run_ingest` is also used by `update_access` — only the destination
        # access sync runs in that mode, not the actual extract/load. Pick the
        # log wording to match so an operator running `saga update-access`
        # doesn't see "Running 151 ingest pipeline(s)" and worry pipelines
        # are about to execute. In dry-run, suppress the iteration header
        # entirely — "Syncing" implies action that isn't happening, and the
        # DRY RUN banner + per-dataset/per-table diff lines + summary
        # footer already carry the context.
        from dlt_saga.utility.cli.context import get_execution_context

        context = get_execution_context()
        if context.update_access:
            if not context.dry_run:
                logger.info(
                    "Syncing access for %d pipeline(s) with %d worker(s)",
                    len(all_configs),
                    workers,
                )
        else:
            logger.info(
                "Running %d ingest pipeline(s) with %d worker(s)",
                len(all_configs),
                workers,
            )
        self._prepare_destinations(configs)
        return self._execute_pipelines_tracked(all_configs, workers)

    def _execute_pipelines_tracked(
        self,
        configs: List[PipelineConfig],
        workers: int,
    ) -> SessionResult:
        """Execute ingest pipelines and capture structured results."""
        results: List[PipelineResult] = []
        total = len(configs)

        with ThreadPoolExecutor(max_workers=workers) as executor:
            future_to_config = {}
            for idx, config in enumerate(configs, 1):
                log_prefix = f"[{idx}/{total}]"
                future = executor.submit(
                    self._execute_single_ingest, config, log_prefix
                )
                future_to_config[future] = config

            for future in as_completed(future_to_config):
                results.append(future.result())

        result = SessionResult(pipeline_results=results)
        from dlt_saga.utility.cli.context import get_execution_context

        context = get_execution_context()
        if context.update_access:
            grants = context.access_grants_applied
            revokes = context.access_revokes_applied
            config_errors = context.access_config_error_count
            access_errors = context.access_error_count
            dry_run_suffix = " (DRY RUN — nothing applied)" if context.dry_run else ""
            verb = "would apply" if context.dry_run else "applied"
            errors_suffix = "".join(
                [
                    f", {config_errors} config error(s)" if config_errors else "",
                    f", {access_errors} table error(s)" if access_errors else "",
                ]
            )
            if grants == 0 and revokes == 0:
                # Distinguish "actually nothing to change" from a partial
                # run where everything in scope was already in sync.
                logger.info(
                    "Access sync complete%s: no changes %s; "
                    "%d pipelines processed, %d failed%s",
                    dry_run_suffix,
                    verb,
                    len(results),
                    result.failed,
                    errors_suffix,
                )
            else:
                logger.info(
                    "Access sync complete%s: %s %d grant(s), %d revoke(s); "
                    "%d pipelines processed, %d failed%s",
                    dry_run_suffix,
                    verb,
                    grants,
                    revokes,
                    len(results),
                    result.failed,
                    errors_suffix,
                )
        else:
            logger.info(
                "Ingest complete: %d/%d succeeded, %d failed",
                result.succeeded,
                len(results),
                result.failed,
            )
        return result

    @staticmethod
    def _execute_single_ingest(
        config: PipelineConfig, log_prefix: str = ""
    ) -> PipelineResult:
        """Execute a single ingest pipeline, returning a structured result."""
        from dlt_saga.hooks.registry import (
            ON_PIPELINE_COMPLETE,
            ON_PIPELINE_ERROR,
            ON_PIPELINE_START,
            HookContext,
            get_hook_registry,
        )

        prefix = f"{log_prefix} " if log_prefix else ""
        registry = get_hook_registry()
        registry.fire(
            ON_PIPELINE_START,
            HookContext(
                pipeline_name=config.pipeline_name, config=config, command="ingest"
            ),
        )
        try:
            load_info = execute_pipeline(config, log_prefix=log_prefix)
            if isinstance(load_info, list):
                summary = summarize_load_info(load_info)
                # `summarize_load_info([])` returns ""; previously this
                # produced an empty `[N/total] ` line — visible whenever a
                # pipeline returned `[]` (e.g. native_load skipping its run
                # in update-access mode, or any pipeline with no resources
                # to load).
                if summary:
                    logger.info("%s%s", prefix, summary)
            registry.fire(
                ON_PIPELINE_COMPLETE,
                HookContext(
                    pipeline_name=config.pipeline_name,
                    config=config,
                    command="ingest",
                    result=load_info,
                ),
            )
            return PipelineResult(
                pipeline_name=config.pipeline_name,
                success=True,
                load_info=load_info,
                config=config,
            )
        except Exception as e:
            # ValueError is the project's convention for a configuration/validation
            # error — the run never started, so flag it as developer feedback that
            # must not be recorded as a run outcome. Any other exception is a
            # genuine run failure (recorded).
            config_error = isinstance(e, ValueError)
            # Failure display is single-sourced through the CLI end-of-run summary
            # (_exit_if_failures), which lists every failure on stderr. Genuine
            # failures also get one inline traceback here for real-time debugging;
            # config errors are clean developer feedback, so their display is left
            # to the summary alone (logging them here too would print them twice).
            if not config_error:
                logger.error(
                    "%sPipeline %s failed: %s",
                    prefix,
                    config.pipeline_name,
                    e,
                    exc_info=True,
                )
            registry.fire(
                ON_PIPELINE_ERROR,
                HookContext(
                    pipeline_name=config.pipeline_name,
                    config=config,
                    command="ingest",
                    error=e,
                ),
            )
            return PipelineResult(
                pipeline_name=config.pipeline_name,
                success=False,
                error=str(e),
                config=config,
                config_error=config_error,
            )

    # -------------------------------------------------------------------
    # Internal: historize execution
    # -------------------------------------------------------------------

    def _run_historize(
        self,
        select: Optional[List[str]],
        workers: int,
        full_refresh: bool,
        partial_refresh: bool = False,
        historize_from: Optional[str] = None,
    ) -> SessionResult:
        """Discover historize-enabled configs and execute them."""
        configs, _ = self._discover_and_select(
            select, filter_fn=lambda c: c.historize_enabled
        )
        all_configs = flatten_configs(configs)

        if not all_configs:
            logger.info("No historize-enabled pipelines matched the selection criteria")
            return SessionResult()

        logger.info(
            "Historizing %d pipeline(s) with %d worker(s)", len(all_configs), workers
        )
        return self._execute_historize_tracked(
            all_configs, workers, full_refresh, partial_refresh, historize_from
        )

    def _execute_historize_tracked(
        self,
        configs: List[PipelineConfig],
        workers: int,
        full_refresh: bool,
        partial_refresh: bool = False,
        historize_from: Optional[str] = None,
    ) -> SessionResult:
        """Execute historize pipelines and capture structured results."""
        results: List[PipelineResult] = []
        total = len(configs)

        with ThreadPoolExecutor(max_workers=workers) as executor:
            future_to_config = {}
            for idx, config in enumerate(configs, 1):
                log_prefix = f"[{idx}/{total}]"
                future = executor.submit(
                    self._execute_single_historize,
                    config,
                    full_refresh,
                    log_prefix,
                    partial_refresh,
                    historize_from,
                )
                future_to_config[future] = config

            for future in as_completed(future_to_config):
                results.append(future.result())

        result = SessionResult(pipeline_results=results)
        logger.info(
            "Historization complete: %d/%d succeeded, %d failed",
            result.succeeded,
            len(results),
            result.failed,
        )
        return result

    @staticmethod
    def _execute_single_historize(
        config: PipelineConfig,
        full_refresh: bool,
        log_prefix: str = "",
        partial_refresh: bool = False,
        historize_from: Optional[str] = None,
    ) -> PipelineResult:
        """Execute historization for a single pipeline."""
        from dlt_saga.hooks.registry import (
            ON_PIPELINE_COMPLETE,
            ON_PIPELINE_ERROR,
            ON_PIPELINE_START,
            HookContext,
            get_hook_registry,
        )

        prefix = f"{log_prefix} " if log_prefix else ""
        registry = get_hook_registry()
        registry.fire(
            ON_PIPELINE_START,
            HookContext(
                pipeline_name=config.pipeline_name, config=config, command="historize"
            ),
        )
        try:
            from dlt_saga.historize.factory import build_historize_runner

            runner = build_historize_runner(
                config,
                full_refresh,
                partial_refresh,
                historize_from,
                log_prefix=log_prefix,
            )
            run_result = runner.run()

            if run_result["status"] == "completed":
                mode = run_result["mode"]
                duration = run_result.get("duration", 0)
                timings = run_result.get("timings", {})
                timing_parts = ", ".join(f"{k}: {v:.1f}s" for k, v in timings.items())
                detail = (
                    "all snapshots"
                    if mode == "full_reprocess"
                    else f"{run_result['snapshots_processed']} snapshot(s)"
                )
                stats_parts = []
                if run_result.get("new_or_changed_rows"):
                    stats_parts.append(f"{run_result['new_or_changed_rows']} rows")
                if run_result.get("deleted_rows"):
                    stats_parts.append(f"{run_result['deleted_rows']} deletions")
                stats_str = f", {', '.join(stats_parts)}" if stats_parts else ""
                msg = f"{config.pipeline_name}: {mode} ({detail}{stats_str}, {duration:.1f}s [{timing_parts}])"
                logger.info("%s%s", prefix, msg)
                registry.fire(
                    ON_PIPELINE_COMPLETE,
                    HookContext(
                        pipeline_name=config.pipeline_name,
                        config=config,
                        command="historize",
                        result=run_result,
                    ),
                )
                return PipelineResult(
                    pipeline_name=config.pipeline_name,
                    success=True,
                    load_info=run_result,
                    config=config,
                )
            else:
                # No inline logging here: the runner already logged a traceback for
                # genuine failures, and the CLI end-of-run summary (_exit_if_failures)
                # surfaces every failure — including config errors — so this branch
                # just returns the structured result. Logging again would duplicate
                # the message the summary already prints.
                error = run_result.get("error", "Unknown error")
                registry.fire(
                    ON_PIPELINE_ERROR,
                    HookContext(
                        pipeline_name=config.pipeline_name,
                        config=config,
                        command="historize",
                        error=RuntimeError(error),
                    ),
                )
                return PipelineResult(
                    pipeline_name=config.pipeline_name,
                    success=False,
                    error=error,
                    config=config,
                    config_error=run_result.get("config_error", False),
                )

        except Exception as e:
            logger.error(
                "%sHistorization failed for %s: %s",
                prefix,
                config.pipeline_name,
                e,
                exc_info=True,
            )
            registry.fire(
                ON_PIPELINE_ERROR,
                HookContext(
                    pipeline_name=config.pipeline_name,
                    config=config,
                    command="historize",
                    error=e,
                ),
            )
            return PipelineResult(
                pipeline_name=config.pipeline_name,
                success=False,
                error=str(e),
                config=config,
            )

    # -------------------------------------------------------------------
    # Internal: combined run
    # -------------------------------------------------------------------

    def _run_both(
        self,
        select: Optional[List[str]],
        workers: int,
        full_refresh: bool,
        partial_refresh: bool = False,
        historize_from: Optional[str] = None,
    ) -> SessionResult:
        """Run ingest then historize, combining results."""
        all_results: List[PipelineResult] = []

        # Ingest phase
        ingest_configs, _ = self._discover_and_select(
            select, filter_fn=lambda c: c.ingest_enabled
        )
        ingest_list = flatten_configs(ingest_configs)

        if ingest_list:
            logger.info("Running %d ingest pipeline(s)", len(ingest_list))
            self._prepare_destinations(ingest_configs)
            ingest_result = self._execute_pipelines_tracked(ingest_list, workers)
            all_results.extend(ingest_result.pipeline_results)
            failed_names = {r.pipeline_name for r in ingest_result.failures}
        else:
            failed_names = set()

        # Historize phase
        historize_configs, _ = self._discover_and_select(
            select, filter_fn=lambda c: c.historize_enabled
        )
        historize_list = flatten_configs(historize_configs)

        # Skip historize for pipelines whose ingest failed during full refresh
        if full_refresh and failed_names:
            before = len(historize_list)
            historize_list = [
                c for c in historize_list if c.pipeline_name not in failed_names
            ]
            skipped = before - len(historize_list)
            if skipped:
                logger.warning(
                    "Skipping historize for %d pipeline(s) due to failed ingest",
                    skipped,
                )

        if historize_list:
            logger.info("Historizing %d pipeline(s)", len(historize_list))
            historize_result = self._execute_historize_tracked(
                historize_list, workers, full_refresh, partial_refresh, historize_from
            )
            all_results.extend(historize_result.pipeline_results)

        return SessionResult(pipeline_results=all_results)

    # -------------------------------------------------------------------
    # Internal: destroy (teardown)
    # -------------------------------------------------------------------

    def _run_destroy(
        self,
        select: Optional[List[str]],
        resource_type: str,
        workers: int,
        dry_run: bool,
    ) -> SessionResult:
        """Discover matching configs (enabled + disabled) and tear them down.

        Selection is by selector only — ``resource_type`` gates which layer(s)
        get torn down per config, not which configs are selected (a config no
        longer declaring a historize disposition can still own an orphaned
        historized table). Disabled configs are included: tearing a pipeline
        down after it has been disabled is the expected path.
        """
        # warn_on_no_match=False: destroy targets the disabled set too, so a
        # match living only there must not trip the enabled-set "did not match"
        # warning. The empty-union check below reports a genuine no-match once.
        enabled, disabled = self._discover_and_select(
            select, filter_fn=None, warn_on_no_match=False
        )
        all_configs = flatten_configs(enabled) + flatten_configs(disabled)

        if not all_configs:
            logger.warning("No pipelines matched the selection criteria")
            return SessionResult()

        verb = "Previewing teardown of" if dry_run else "Destroying"
        logger.info(
            "%s %d pipeline(s) [resource-type: %s] with %d worker(s)",
            verb,
            len(all_configs),
            resource_type,
            workers,
        )

        results: List[PipelineResult] = []
        total = len(all_configs)
        with ThreadPoolExecutor(max_workers=workers) as executor:
            future_to_config = {}
            for idx, config in enumerate(all_configs, 1):
                log_prefix = f"[{idx}/{total}]"
                future = executor.submit(
                    self._execute_single_destroy,
                    config,
                    resource_type,
                    dry_run,
                    log_prefix,
                )
                future_to_config[future] = config
            for future in as_completed(future_to_config):
                results.append(future.result())

        result = SessionResult(pipeline_results=results)
        logger.info(
            "Destroy %s: %d/%d succeeded, %d failed",
            "preview complete" if dry_run else "complete",
            result.succeeded,
            len(results),
            result.failed,
        )
        return result

    @staticmethod
    def _execute_single_destroy(
        config: PipelineConfig,
        resource_type: str,
        dry_run: bool,
        log_prefix: str = "",
    ) -> PipelineResult:
        """Tear down a single pipeline's footprint, returning a structured result."""
        from dlt_saga.destroy import (
            destroy_historize_footprint,
            destroy_ingest_footprint,
        )

        prefix = f"{log_prefix} " if log_prefix else ""
        try:
            details: Dict[str, Any] = {}
            # Collect what each layer removes, then emit ONE summary line naming
            # the pipeline — so parallel runs stay readable (one line per
            # pipeline) instead of interleaved per-resource lines.
            removed = []
            if resource_type in ("all", "ingest"):
                ingest = destroy_ingest_footprint(
                    config, dry_run=dry_run, log_prefix=log_prefix
                )
                details["ingest"] = ingest
                for table_id in ingest["table_ids"]:
                    removed.append(f"ingest table {table_id} (+ dlt/load state)")
            if resource_type in ("all", "historize"):
                historize = destroy_historize_footprint(
                    config, dry_run=dry_run, log_prefix=log_prefix
                )
                details["historize"] = historize
                if historize:
                    if historize["dropped"]:
                        removed.append(f"historized table {historize['table_id']}")
                    if historize["log_cleared"]:
                        removed.append("historize log entries")

            if removed:
                logger.info(
                    "%s%s — %s: %s",
                    prefix,
                    config.pipeline_name,
                    "[DRY RUN] would remove" if dry_run else "removed",
                    "; ".join(removed),
                )
            else:
                logger.info("%s%s — nothing to remove", prefix, config.pipeline_name)

            return PipelineResult(
                pipeline_name=config.pipeline_name,
                success=True,
                load_info=details,
                config=config,
            )
        except Exception as e:
            logger.error(
                "%sDestroy failed for %s: %s",
                prefix,
                config.pipeline_name,
                e,
                exc_info=True,
            )
            return PipelineResult(
                pipeline_name=config.pipeline_name,
                success=False,
                error=str(e),
                config=config,
            )

    # -------------------------------------------------------------------
    # Internal: destination preparation
    # -------------------------------------------------------------------

    @staticmethod
    def _prepare_destinations(configs: Dict[str, List[PipelineConfig]]) -> None:
        """Pre-create datasets/schemas for the destinations that will be used."""
        from dlt_saga.utility.cli.context import get_execution_context

        context_dest_type = get_execution_context().get_destination_type()
        configs_by_dest: Dict[str, List[PipelineConfig]] = {}

        for pipeline_configs in configs.values():
            for config in pipeline_configs:
                dest_type = config.config_dict.get(
                    "destination_type", context_dest_type
                )
                configs_by_dest.setdefault(dest_type, []).append(config)

        for dest_type, pipeline_configs in configs_by_dest.items():
            from dlt_saga.destinations.factory import DestinationFactory

            dest_class = DestinationFactory.get_destination_class(dest_type)
            dest_class.prepare_for_execution(pipeline_configs)
