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
from typing import Any, Callable, Dict, List, Optional

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
        profile: Profile name from profiles.yml (default: ``"default"``).
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
        profile: str = "default",
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
            return self._execute_with_auth(lambda: self._run_ingest(select, workers))

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
            return self._execute_with_auth(
                lambda: self._run_historize(
                    select, workers, full_refresh, partial_refresh, historize_from
                )
            )

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
            return self._execute_with_auth(
                lambda: self._run_both(
                    select, workers, full_refresh, partial_refresh, historize_from
                )
            )

    def update_access(
        self,
        select: Optional[List[str]] = None,
        workers: int = 4,
    ) -> SessionResult:
        """Update access controls (e.g., BigQuery IAM policies) without running pipelines.

        Args:
            select: Selector expressions. None = all ingest-enabled.
            workers: Number of parallel workers.

        Returns:
            SessionResult with per-pipeline outcomes.
        """
        with execution_context_scope(
            self._profile_target,
            update_access=True,
        ):
            return self._execute_with_auth(lambda: self._run_ingest(select, workers))

    # -------------------------------------------------------------------
    # Internal: initialization helpers
    # -------------------------------------------------------------------

    @staticmethod
    def _load_profile(profile: str, target: Optional[str]) -> Optional[ProfileTarget]:
        """Load profile target from profiles.yml."""
        profiles_config = get_profiles_config()
        if not profiles_config.profiles_exist():
            logger.debug("No profiles.yml found, using environment variables")
            return None
        try:
            return profiles_config.get_target(profile, target)
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
    ) -> tuple:
        """Discover configs using this session's config source and apply selectors.

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
        selected = enabled_selector.select(select)

        disabled_selector = PipelineSelector(all_disabled)
        selected_disabled = disabled_selector.select(select)

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

    def _execute_with_auth(
        self, callback: Callable[[], SessionResult]
    ) -> SessionResult:
        """Validate credentials and run callback with impersonation if needed."""
        self._auth_provider.validate()

        run_as = self._profile_target.run_as if self._profile_target else None

        if run_as:
            with self._auth_provider.impersonate(run_as):
                return callback()
        return callback()

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

        logger.info(
            "Running %d ingest pipeline(s) with %d worker(s)", len(all_configs), workers
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
            load_info = execute_pipeline(config)
            if isinstance(load_info, list):
                summary = summarize_load_info(load_info)
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
            )
        except Exception as e:
            logger.error("%sPipeline %s failed: %s", prefix, config.pipeline_name, e)
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
                config, full_refresh, partial_refresh, historize_from
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
                )
            else:
                error = run_result.get("error", "Unknown error")
                logger.error("%s%s: %s", prefix, config.pipeline_name, error)
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
