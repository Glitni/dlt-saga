import logging
import os
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

import dlt

from dlt_saga.destinations.factory import DestinationFactory
from dlt_saga.pipelines.target.config import TargetConfig
from dlt_saga.pipelines.target.writer import TargetWriter
from dlt_saga.utility.cli.logging import YELLOW, PrefixedLoggerAdapter, colorize

logger = logging.getLogger(__name__)

# pyarrow is an optional dependency; resolve it once at import rather than
# re-importing inside per-row/per-batch transform closures on multi-million-row
# streams. None when not installed.
try:
    import pyarrow as _pa
except ImportError:
    _pa = None

DEV_MODE = False


def _make_arrow_filter(specs: List[Any]):
    from dlt_saga.utility.filters import apply_filters_to_arrow

    def _arrow_filter(item: Any) -> Any:
        if _pa is None or not isinstance(item, (_pa.Table, _pa.RecordBatch)):
            return item
        return apply_filters_to_arrow(item, specs)

    return _arrow_filter


def _make_dict_predicate(specs: List[Any]):
    from dlt_saga.utility.filters import build_row_predicate

    predicate = build_row_predicate(specs)

    def _dict_predicate(item: Any) -> bool:
        if _pa is not None and isinstance(item, (_pa.Table, _pa.RecordBatch)):
            return True  # already handled by the arrow map
        if not isinstance(item, dict):
            return True
        return predicate(item)

    return _dict_predicate


class BasePipeline:
    def __init__(self, config: Dict[str, Any], log_prefix: Optional[str] = None):
        self.config_dict = config or {}
        self.base_table_name = config.get("base_table_name")
        self.table_name = config.get(
            "table_name", self.base_table_name
        )  # Environment-aware table name
        # Resolved table description sent to dlt during the run, reused at
        # finalize to reconcile table-description drift (dlt only writes it at
        # table creation). None means nothing to reconcile / persist_docs off.
        self._resolved_table_description: Optional[str] = None
        self.pipeline_name = config.get(
            "pipeline_name"
        )  # Always includes pipeline type prefix

        # Store log prefix for use by subclasses
        self.log_prefix = log_prefix

        # Create a prefixed logger if log_prefix is provided
        self.logger = (
            PrefixedLoggerAdapter(logger, log_prefix) if log_prefix else logger
        )

        # Import here to avoid circular dependency
        from dlt_saga.utility.cli.context import get_execution_context

        context = get_execution_context()

        # Initialize TargetConfig with pipeline behavior and destination hints
        # (NOT connection config - that comes from profiles/destination config)
        # Note: full_refresh mode resets state but doesn't change write_disposition
        # After state reset, the pipeline runs normally with its configured disposition/strategy
        target_config = TargetConfig(
            # Loading configuration
            write_disposition=config.get("write_disposition", "replace"),
            replace_strategy=config.get("replace_strategy"),
            # Merge configuration
            merge_key=config.get("merge_key"),
            merge_strategy=config.get("merge_strategy"),
            primary_key=config.get("primary_key"),
            # SCD2 configuration
            valid_from_column=config.get("valid_from_column", "_dlt_valid_from"),
            valid_to_column=config.get("valid_to_column", "_dlt_valid_to"),
            active_record_timestamp=config.get("active_record_timestamp"),
            boundary_timestamp=config.get("boundary_timestamp"),
            row_version_column_name=config.get("row_version_column_name"),
            # Deduplication
            deduplicate=config.get("deduplicate", True),
            dedup_sort=config.get("dedup_sort"),
            dedup_sort_column=config.get("dedup_sort_column"),
            # Hard deletes
            hard_delete_column=config.get("hard_delete_column"),
            # Destination hints (partitioning, clustering, etc.)
            partition_column=config.get("partition_column"),
            cluster_columns=config.get("cluster_columns"),
            # Column hints
            columns=config.get("columns"),
            # Documentation
            description=config.get("description"),
            classification=config.get("classification"),
            persist_docs=config.get("persist_docs"),
        )

        self.target_writer = TargetWriter(target_config)

        # Handle full_refresh BEFORE creating pipeline to avoid loading stale schema
        if context.full_refresh:
            self.logger.info(
                f"Full refresh mode: Resetting destination state for {self.pipeline_name}"
            )

            # 1. Create a temporary pipeline just to drop local state
            temp_pipeline = self._create_pipeline()
            try:
                temp_pipeline.drop()
                self.logger.debug("Dropped local pipeline state")
            except Exception as e:
                self.logger.debug(f"No local state to drop: {e}")

            # 2. Reset destination state (drop remote tables/metadata)
            # Do this BEFORE creating the real pipeline to avoid loading stale schema
            try:
                self.destination.reset_destination_state(
                    pipeline_name=self.pipeline_name, table_name=self.table_name
                )
            except Exception as e:
                self.logger.warning(f"Could not reset destination state: {e}")

        # Create pipeline (will have clean schema after full_refresh)
        self.pipeline = self._create_pipeline()

        # In update-access mode we iterate every pipeline but the work that
        # actually matters (added/removed entries) is logged separately by
        # the destination's access syncs. Suppress the per-pipeline "Running
        # for destination" line in that mode so the diff stands out — keep
        # it for normal ingest where it's a useful progress indicator.
        if not context.update_access:
            self.logger.info(
                "Running for destination "
                + colorize(
                    f"{self.destination_database}.{self.pipeline.dataset_name}.{self.table_name}",
                    YELLOW,
                )
            )

    def _create_pipeline(self) -> dlt.Pipeline:
        """Create pipeline with destination abstraction."""
        from dlt_saga.utility.cli.context import get_execution_context

        context = get_execution_context()

        destination_type = context.get_destination_type()

        # schema_name is already resolved in config_dict by pipeline_config
        schema_name = self.config_dict.get("schema_name")
        if not schema_name or not isinstance(schema_name, str):
            raise ValueError(
                f"schema_name must be a non-empty string, got {schema_name!r}"
            )

        # Delegate all destination-specific resolution to the config class
        self.destination = DestinationFactory.create_from_context(
            destination_type, context, self.config_dict
        )

        # Store destination project for reference/logging
        self.destination_database = self.destination.config.database

        logger.debug(
            f"Created {destination_type} destination: "
            f"project={self.destination_database}, schema={schema_name}"
        )

        # Create dlt destination from our abstraction
        dlt_destination = self.destination.create_dlt_destination()
        dlt_staging = self.destination.create_dlt_staging()

        # Set pipelines_dir to isolate state per destination, project, and dataset
        # This prevents state conflicts when switching between profiles/destinations
        pipelines_dir_suffix = (
            f"{destination_type}_{self.destination_database}_{schema_name}".replace(
                "-", "_"
            )
        )
        pipelines_dir = os.path.expanduser(f"~/.dlt/pipelines_{pipelines_dir_suffix}")

        pipeline_params = {
            "pipeline_name": self.pipeline_name,
            "dataset_name": schema_name,  # dlt's pipeline kwarg is dataset_name
            "destination": dlt_destination,
            "dev_mode": DEV_MODE,
            "pipelines_dir": pipelines_dir,
        }
        if dlt_staging is not None:
            pipeline_params["staging"] = dlt_staging

        return dlt.pipeline(**pipeline_params)

    def _get_last_load_with_data(self, table_name: str) -> Optional[datetime]:
        """Get the timestamp of the last successful load that had data.

        Queries the _saga_load_info table to find the last load that actually wrote
        rows for the specified table.

        Args:
            table_name: The name of the table to query for

        Returns:
            datetime of last load with data, or None if no previous loads exist
        """
        try:
            return self.destination.get_last_load_timestamp(
                self.pipeline.dataset_name, self.pipeline_name, table_name
            )
        except Exception as e:
            logger.debug(f"Could not get last load timestamp: {str(e)}")
            return None

    def _save_load_info(self, load_info_list: List[Dict]) -> None:
        """Save flattened load_info to a table for tracking and debugging.

        Only saves essential fields to avoid creating nested tables.
        Flattens row_counts dict into separate columns.
        """
        try:
            from datetime import datetime, timezone

            # Flatten load_info to essential fields only
            flattened_records = []
            for load_info in load_info_list:
                row_counts = load_info.get("row_counts", {})
                for table_name, row_count in row_counts.items():
                    if not table_name.startswith("_dlt_"):
                        flattened_records.append(
                            {
                                "pipeline_name": load_info.get("pipeline", {}).get(
                                    "pipeline_name"
                                ),
                                "destination_name": load_info.get("destination_name"),
                                "destination_type": load_info.get("destination_type"),
                                "dataset_name": load_info.get("dataset_name"),
                                "table_name": table_name,
                                "row_count": row_count,
                                "started_at": load_info.get("started_at"),
                                "finished_at": load_info.get("finished_at"),
                                "first_run": load_info.get("first_run"),
                                "saved_at": datetime.now(timezone.utc).isoformat(),
                            }
                        )

            if flattened_records:
                self.destination.save_load_info(
                    self.pipeline.dataset_name,
                    flattened_records,
                    pipeline=self.pipeline,
                )

        except Exception as e:
            logger.warning(f"Failed to save load info: {str(e)}", exc_info=True)

    def _build_destination_hints(self, description: str) -> Dict[str, Any]:
        """Build destination-specific hint arguments."""
        hints: Dict[str, Any] = {}

        # Config description overrides the pipeline-generated one and table-level
        # classification is encoded in; the whole thing is gated by
        # persist_docs.table (returns None when off / nothing to write).
        table_description = self.target_writer.config.resolve_table_description(
            description
        )
        # Remember the resolved value so finalize can reconcile table-description
        # drift (dlt applies it only at table creation).
        self._resolved_table_description = table_description
        if table_description:
            hints["table_description"] = table_description

        if (
            self.target_writer.config.partition_column
            and self.destination.supports_partitioning()
        ):
            hints["partition_column"] = self.target_writer.config.partition_column

        if (
            self.target_writer.config.cluster_columns
            and self.destination.supports_clustering()
        ):
            hints["cluster_columns"] = self.target_writer.config.cluster_columns

        if self.target_writer.config.insert_api:
            hints["insert_api"] = self.target_writer.config.insert_api

        logger.debug(f"Applying destination hints: {hints}")
        return hints

    def _capture_extract_timing(self, trace: Any, load_info: Dict[str, Any]) -> None:
        """Capture extract phase timing from trace."""
        if not hasattr(trace, "last_extract_info") or not trace.last_extract_info:
            return

        extract_info = trace.last_extract_info
        if not (
            hasattr(extract_info, "finished_at") and hasattr(extract_info, "started_at")
        ):
            return

        if extract_info.finished_at and extract_info.started_at:
            load_info["actual_extract_duration"] = (
                extract_info.finished_at - extract_info.started_at
            ).total_seconds()

    def _capture_normalize_timing(self, trace: Any, load_info: Dict[str, Any]) -> None:
        """Capture normalize phase timing and row counts from trace."""
        if not hasattr(trace, "last_normalize_info") or not trace.last_normalize_info:
            return

        normalize_info = trace.last_normalize_info

        if hasattr(normalize_info, "finished_at") and hasattr(
            normalize_info, "started_at"
        ):
            if normalize_info.finished_at and normalize_info.started_at:
                load_info["actual_normalize_duration"] = (
                    normalize_info.finished_at - normalize_info.started_at
                ).total_seconds()

        if hasattr(normalize_info, "row_counts"):
            load_info["row_counts"] = normalize_info.row_counts

    def _capture_load_timing(self, trace: Any, load_info: Dict[str, Any]) -> None:
        """Capture load phase timing from trace."""
        if not hasattr(trace, "last_load_info") or not trace.last_load_info:
            return

        load_info_trace = trace.last_load_info
        if not (
            hasattr(load_info_trace, "finished_at")
            and hasattr(load_info_trace, "started_at")
        ):
            return

        if load_info_trace.finished_at and load_info_trace.started_at:
            load_info["actual_load_duration"] = (
                load_info_trace.finished_at - load_info_trace.started_at
            ).total_seconds()

    def _capture_trace_timings(self, load_info: Dict):
        """Capture actual phase timings from dlt trace."""
        if not self.pipeline.last_trace:
            return

        trace = self.pipeline.last_trace
        self._capture_extract_timing(trace, load_info)
        self._capture_normalize_timing(trace, load_info)
        self._capture_load_timing(trace, load_info)

    def _resolve_ingested_at(
        self, item: dict, compiled_regex, snapshot_date_format, extraction_ts
    ):
        """Resolve _dlt_ingested_at value for a single row.

        Priority order:
        1. Regex extraction from file path
        2. File modification date
        3. Extraction timestamp
        """
        # Priority 1: Regex extraction from file path
        if compiled_regex and snapshot_date_format:
            file_name = item.get("_dlt_source_file_name", "")
            if file_name:
                match = compiled_regex.search(str(file_name))
                if match:
                    try:
                        return datetime.strptime(
                            match.group(1), snapshot_date_format
                        ).replace(tzinfo=timezone.utc)
                    except ValueError:
                        logger.warning(
                            "snapshot_date_regex matched '%s' in file '%s' but it "
                            "does not match snapshot_date_format '%s' — "
                            "falling back to file modification date / extraction timestamp.",
                            match.group(1),
                            file_name,
                            snapshot_date_format,
                        )

        # Priority 2: File modification date
        mod_date = item.get("_dlt_source_modification_date")
        if mod_date is not None:
            return mod_date

        # Priority 3: Extraction timestamp
        return extraction_ts

    def _inject_ingested_at(self, resource: Any) -> Any:
        """Inject _dlt_ingested_at column for append- and replace-mode pipelines.

        Adds a timestamp column to enable historization and efficient querying.
        Injected for both ``append`` and ``replace`` (each ``replace`` run
        overwrites the table with a single snapshot stamped at run time), but
        not for ``merge``/``scd2`` — dlt manages its own columns there and an
        injected timestamp would trigger false change detection. Value
        resolution (priority order):

        1. Regex extraction from file path: When snapshot_date_regex +
           snapshot_date_format are configured, extracts date from
           _dlt_source_file_name using the regex.
        2. File modification date: Uses _dlt_source_modification_date
           if available (filesystem sources with metadata injection).
        3. Extraction timestamp: datetime.now(UTC) as final fallback.

        Auto-clusters by _dlt_ingested_at only for ``append`` when no explicit
        cluster_columns are configured. A ``replace`` table holds one snapshot
        with a single ~constant timestamp, so clustering by it is useless.
        """
        base_disposition = self.target_writer.config.write_disposition.replace(
            "+historize", ""
        )
        if base_disposition not in ("append", "replace"):
            return resource

        import re

        # One run timestamp, resolved once outside the row/batch closure so a
        # single physical run maps to a single historize snapshot. (Computing
        # datetime.now() per Arrow batch would split one run into as many
        # spurious SCD2 snapshots as there are batches.) tz-aware throughout so
        # both the dict and Arrow paths land on a warehouse TIMESTAMP — a naive
        # DATETIME snapshot column can't be compared against the TIMESTAMP
        # literals the incremental historize SQL emits (a type error on BigQuery).
        extraction_dt = datetime.now(timezone.utc)
        extraction_ts = extraction_dt.isoformat()
        snapshot_date_regex = self.config_dict.get("snapshot_date_regex")
        snapshot_date_format = self.config_dict.get("snapshot_date_format")
        compiled_regex = (
            re.compile(snapshot_date_regex) if snapshot_date_regex else None
        )
        resolve = self._resolve_ingested_at
        # Guard so the Arrow-path regex warning fires at most once per resource.
        warned_arrow_regex = [False]

        def _add_ingested_at(item):
            if _pa is not None and isinstance(item, (_pa.Table, _pa.RecordBatch)):
                # Arrow sources (e.g. sql_database) carry no per-row file
                # metadata, so the regex / modification-date tiers don't apply —
                # every row gets the single run-level extraction timestamp.
                if compiled_regex and not warned_arrow_regex[0]:
                    warned_arrow_regex[0] = True
                    logger.warning(
                        "snapshot_date_regex is set but this source yields Arrow "
                        "batches without per-row file metadata; _dlt_ingested_at "
                        "uses the run extraction timestamp for all rows."
                    )
                # pa.repeat builds the constant column directly — no N-length
                # Python list. "us" matches Python datetime, BigQuery TIMESTAMP,
                # and dlt's default timestamp precision.
                ts_array = _pa.repeat(
                    _pa.scalar(extraction_dt, type=_pa.timestamp("us", tz="UTC")),
                    item.num_rows,
                )
                return item.append_column("_dlt_ingested_at", ts_array)

            # Dict row
            item["_dlt_ingested_at"] = resolve(
                item, compiled_regex, snapshot_date_format, extraction_ts
            )
            return item

        resource.add_map(_add_ingested_at)
        result = resource

        # Auto-cluster by _dlt_ingested_at if no explicit cluster_columns
        # configured. Append only: a replace table holds one snapshot with a
        # single ~constant timestamp, so clustering by it is useless.
        if (
            base_disposition == "append"
            and not self.target_writer.config.cluster_columns
            and self.destination.supports_clustering()
        ):
            self.target_writer.config.cluster_columns = ["_dlt_ingested_at"]
            logger.debug("Auto-clustering append pipeline by _dlt_ingested_at")

        return result

    def _apply_filters(self, resource: Any) -> Any:
        """Apply declarative ``filters:`` from the pipeline config.

        Translates the YAML spec into a row predicate and registers two
        transforms on the resource: an ``add_map`` that filters PyArrow
        tables row-by-row, and an ``add_filter`` for dict rows.  Both are
        installed because dlt routes arrow and dict items differently and
        a single transform can't cleanly drop sub-rows from an arrow batch
        while also dropping individual dict items.

        See ``dlt_saga/utility/filters.py`` for the spec / operator set.
        """
        from dlt_saga.utility.filters import parse_filters

        raw = self.config_dict.get("filters")
        if not raw:
            return resource

        specs = parse_filters(raw, context=self.pipeline_name)
        if not specs:
            return resource

        self.logger.debug("Applying %d row filter(s)", len(specs))
        resource.add_map(_make_arrow_filter(specs))
        resource.add_filter(_make_dict_predicate(specs))
        return resource

    def _apply_row_limit(self, resource: Any) -> Any:
        """Apply dev_row_limit from profile if configured (best-effort cap).

        Uses dlt's native resource.add_limit() to stop extraction early in dev.
        Has no effect when dev_row_limit is not set (e.g. in production).

        For transformer resources (e.g. filesystem pipelines), the limit is
        applied to the top-level source resource since dlt's add_limit() only
        works on non-transformer resources.
        """
        from dlt_saga.utility.cli.context import get_execution_context

        limit = get_execution_context().get_dev_row_limit()
        if limit:
            self.logger.debug(f"Applying dev_row_limit={limit}")
            if resource.is_transformer:
                # Walk up the pipe chain to find the top-level source pipe
                from dlt.extract.items_transform import LimitItem

                pipe = resource._pipe
                while pipe.parent:
                    pipe = pipe.parent
                pipe.remove_by_type(LimitItem)
                pipe.append_step(
                    LimitItem(max_items=limit, max_time=None, count_rows=True)
                )
            else:
                resource.add_limit(limit)
        return resource

    def _process_resource_data(
        self, resource, description: str
    ) -> tuple[Dict, List[str]]:
        """Process a single resource and return load info and loaded tables."""
        # Default to no table nesting (store nested data as JSON columns).
        # Individual pipelines can override this per-resource if needed.
        if resource.max_table_nesting is None:
            resource.max_table_nesting = 0
        resource = self._inject_ingested_at(resource)
        resource = self._apply_filters(resource)
        resource = self._apply_row_limit(resource)
        hints = self._build_destination_hints(description)
        adapted_resource = self.destination.apply_hints(resource, **hints)
        processed_data = self.target_writer.apply_hints(adapted_resource)

        load_info = self.destination.run_pipeline(
            self.pipeline, processed_data
        ).asdict()

        self._capture_trace_timings(load_info)

        # Exclude dlt system tables (_dlt_loads, _dlt_pipeline_state, _dlt_version):
        # they must never be granted to end users, get user table options, or be
        # documented. Matches the filter already applied in _save_load_info. Real
        # nested child tables (`<table>__<child>`) are not _dlt_-prefixed and are
        # kept.
        loaded_tables: List[str] = []
        if "row_counts" in load_info:
            loaded_tables.extend(
                t for t in load_info["row_counts"].keys() if not t.startswith("_dlt_")
            )

        return load_info, loaded_tables

    def _finalize_pipeline_run(
        self, all_load_info: List[Dict], loaded_tables: List[str]
    ) -> float:
        """Finalize pipeline run with access management and load info saving.

        Returns:
            Finalization duration in seconds
        """
        import time

        finalize_start = time.time()
        self._manage_table_access(loaded_tables)
        self._sync_destination_table_options(loaded_tables)
        self._reconcile_descriptions(loaded_tables)

        if all_load_info:
            self._save_load_info(all_load_info)

        return time.time() - finalize_start

    def _reconcile_descriptions(self, loaded_tables: List[str]) -> None:
        """Reconcile declared column/table descriptions against the main table.

        dlt writes descriptions only at column creation, so config edits don't
        propagate to existing tables (and DuckDB gets none at all). This aligns
        the live table with the config after each load, idempotently — a run
        with unchanged config performs a single metadata read and no writes.

        Column descriptions come from config; the table description reuses the
        value resolved during the run (so it matches what dlt wrote at create).
        Best-effort: never fails a successful load.
        """
        if not self.destination.supports_description_reconcile():
            return
        if self.table_name not in loaded_tables:
            return

        cfg = self.target_writer.config
        column_descriptions = cfg.get_column_description_map()
        if not column_descriptions and self._resolved_table_description is None:
            return

        dataset = self.pipeline.dataset_name
        try:
            self.destination.reconcile_descriptions(
                dataset,
                self.table_name,
                descriptions=column_descriptions,
                table_description=self._resolved_table_description,
            )
        except Exception as exc:
            logger.warning(
                "Failed to reconcile descriptions for %s.%s: %s",
                dataset,
                self.table_name,
                exc,
            )

    def _sync_destination_table_options(self, loaded_tables: List[str]) -> None:
        """Reconcile destination-level table options for each loaded table.

        Currently only BigQuery uses this hook (to sync
        ``partition_expiration_days`` since dlt's adapter doesn't apply it on
        ALTER). The base ``Destination.sync_table_options`` is a no-op, so
        destinations without per-table options pay nothing.

        Errors are caught and logged — sync is best-effort and must never
        fail a successful load.
        """
        if not loaded_tables:
            return
        dataset = self.pipeline.dataset_name
        for table in loaded_tables:
            try:
                self.destination.sync_table_options(dataset, table)
            except Exception as exc:
                logger.warning(
                    "Failed to sync destination table options for %s.%s: %s",
                    dataset,
                    table,
                    exc,
                )

    def _add_timing_breakdown(
        self,
        all_load_info: List[Dict],
        extraction_duration: float,
        load_duration: float,
        finalize_duration: float,
    ):
        """Add timing breakdown to all load_info entries."""
        for load_info in all_load_info:
            load_info["extraction_duration"] = extraction_duration
            load_info["load_duration"] = load_duration
            load_info["finalize_duration"] = finalize_duration

    def update_access_only(self) -> Dict:
        """Update table-level access controls without running the pipeline.

        Used when --update-access flag is set. Skips extraction and loading,
        only applies access controls to existing tables.

        Note: Dataset-level access is handled by _prepare_destinations() which runs
        once for all unique datasets before the pipeline loop. This method only
        handles table-level access to avoid redundant dataset syncs.

        Returns:
            Dict with operation summary
        """
        logger.debug(f"Updating table access controls for {self.base_table_name}")

        # Get the table ID(s) that would be created by this pipeline
        # For most pipelines, this is just the base table name
        table_ids = [self.table_name]

        # Apply table-level access controls to existing table(s)
        self._manage_table_access(table_ids)

        return {
            "operation": "update_access",
            "table_name": self.base_table_name,
            "tables_updated": table_ids,
        }

    def run(self) -> Any:
        from dlt_saga.utility.cli.context import get_execution_context

        context = get_execution_context()

        # If update_access mode, skip extraction/loading and only update access
        if context.update_access:
            return self.update_access_only()

        try:
            import time
            from datetime import datetime, timezone

            all_load_info: List[Dict[str, Any]] = []
            loaded_tables: List[str] = []

            # Wall-clock captured BEFORE the source is read. Recorded as each
            # load record's started_at (below) so change detection compares
            # source modification times against when this run began reading —
            # not dlt's load-package start, which is after extraction.
            extraction_started_at = datetime.now(timezone.utc)
            extraction_start = time.time()
            extraction_end = None
            load_start = None

            # Process all resources
            for resource, description in self.extract_data():
                if not all_load_info:  # First iteration
                    # NB: for lazy resources (generators/dlt sources) extract_data()
                    # returns before any data is read, so this marks resource
                    # *construction*, not the actual source read — which happens
                    # inside _process_resource_data below and is counted as load.
                    # "extraction_duration" is therefore a lower bound for such
                    # sources; eager sources (e.g. Arrow tables) measure fully.
                    extraction_end = time.time()
                    load_start = extraction_end

                load_info, resource_tables = self._process_resource_data(
                    resource, description
                )
                all_load_info.append(load_info)
                loaded_tables.extend(resource_tables)

            load_end = time.time()

            # Handle case where no data was extracted
            if extraction_end is None:
                extraction_end = time.time()
            if load_start is None:
                load_start = extraction_end
                load_end = load_start

            # Record the run's extraction-start as each load record's started_at
            # (dlt reports its load-package start, which is *after* extraction).
            # This does two things:
            #  1) Fixes a change-detection race: sources compare their file/sheet
            #     modification time against MAX(started_at). With the load-start,
            #     a source modified during extract/load has mtime < started_at and
            #     is skipped forever; with the extraction-start it is re-extracted.
            #  2) Makes the reported run duration (finished_at - started_at) span
            #     extraction→load instead of the load step alone — extraction is
            #     often the bulk of the wall-clock (e.g. a slow API pull).
            for load_info in all_load_info:
                load_info["started_at"] = extraction_started_at

            # Dedupe (order-preserving) so access grants, option sync, and doc
            # reconcile run once per real table even if a table surfaced under
            # more than one resource.
            loaded_tables = list(dict.fromkeys(loaded_tables))

            # Finalize and calculate timings
            finalize_duration = self._finalize_pipeline_run(
                all_load_info, loaded_tables
            )

            extraction_duration = extraction_end - extraction_start
            load_duration = load_end - load_start

            self._add_timing_breakdown(
                all_load_info, extraction_duration, load_duration, finalize_duration
            )

            return all_load_info

        except Exception as e:
            if not isinstance(e, ValueError):
                logger.error(f"Pipeline execution failed: {str(e)}", exc_info=True)
            raise

    def _manage_table_access(self, table_ids: List[str]) -> None:
        """Manage table access based on destination capabilities.

        Args:
            table_ids: List of table IDs that were loaded
        """
        # Only manage access if destination supports it
        if not self.destination.supports_access_management():
            logger.debug("Destination does not support access management, skipping")
            return

        # Get access config from source configuration (dict)
        access_config = self.config_dict.get("access")
        # None means "don't manage access", [] means "revoke all access"
        if access_config is None:
            logger.debug(
                "No access configuration found in config, skipping access management"
            )
            return

        # Check environment - only apply access control in production
        # Get environment from profile context (or SAGA_ENVIRONMENT env var as fallback)
        from dlt_saga.utility.naming import get_environment

        dlt_environment = get_environment().lower()

        if dlt_environment != "prod":
            logger.debug(
                f"Skipping access management for non-production environment: {dlt_environment}"
            )
            return

        try:
            # Get access manager from destination
            access_manager = self.destination.get_access_manager()

            # Set the dataset_id for this pipeline's access management (BigQuery-specific)
            if hasattr(access_manager, "dataset_id"):
                access_manager.dataset_id = self.pipeline.dataset_name  # type: ignore[attr-defined]

            # Apply access control (only logs if changes are made)
            # revoke_extra=True by default - access is managed declaratively
            access_manager.manage_access_for_tables(
                table_ids=table_ids,
                access_config=access_config,
            )

        except Exception as e:
            # Log error but don't fail the pipeline
            logger.error(f"Failed to manage table access: {str(e)}", exc_info=True)

    def filter_excluded_columns(self, record: Dict, exclude_columns: List[str]) -> Dict:
        """Remove excluded columns from a record.

        Args:
            record: The record to filter
            exclude_columns: List of top-level column names to exclude

        Returns:
            The filtered record (modified in place)
        """
        if exclude_columns:
            for col in exclude_columns:
                record.pop(col, None)
        return record

    def extract_data(self) -> List[Tuple[Any, str]]:
        """Extract data - to be implemented by child classes"""
        raise NotImplementedError("Subclasses must implement extract_data method")
