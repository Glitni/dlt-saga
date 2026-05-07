"""Historize runner — orchestrates the historization of a single pipeline.

Coordinates state management, SQL generation, and execution to transform
raw snapshot data into SCD2 historized tables.
"""

import logging
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from dlt_saga.historize.config import HistorizeConfig
from dlt_saga.historize.sql import HistorizeSqlBuilder
from dlt_saga.historize.state import HistorizeLogEntry, HistorizeStateManager

logger = logging.getLogger(__name__)


class HistorizeRunner:
    """Orchestrates historization for a single pipeline.

    Flow:
    1. Validate configuration
    2. Determine mode (full reprocess vs incremental vs partial refresh)
    3. Discover value columns from INFORMATION_SCHEMA
    4. Discover unprocessed snapshots
    5. Execute historization SQL
    6. Log results
    """

    def __init__(
        self,
        pipeline_name: str,
        historize_config: HistorizeConfig,
        destination: Any,
        database: str,
        schema: str,
        source_table_name: str,
        target_table_name: str,
        config_dict: dict = None,
        full_refresh: bool = False,
        partial_refresh: bool = False,
        historize_from: Optional[str] = None,
    ):
        self.pipeline_name = pipeline_name
        self.config = historize_config
        self.config_dict = config_dict or {}
        self.destination = destination
        self.database = database
        self.schema = schema
        self.source_table_name = source_table_name
        self.target_table_name = target_table_name
        self.full_refresh = full_refresh
        self.partial_refresh = partial_refresh
        self.historize_from = historize_from

        # Build fully qualified source table ID
        # Only use source_* fields for historize-only pipelines (write_disposition: "historize")
        # For append+historize, the source is the ingested table
        write_disposition = (self.config_dict).get("write_disposition", "")
        is_historize_only = write_disposition == "historize"
        src_table = self.config_dict.get("source_table") if is_historize_only else None
        if src_table:
            self._src_database = self.config_dict.get("source_database") or database
            self._src_schema = self.config_dict.get("source_schema")
            self._src_table = src_table
            self.source_table_id = destination.get_full_table_id(
                self._src_schema, src_table
            )
        else:
            self._src_database = database
            self._src_schema = schema
            self._src_table = source_table_name
            self.source_table_id = destination.get_full_table_id(
                schema, source_table_name
            )

        # Target table in same dataset (or override)
        target_schema = self.config.output_dataset or schema
        self.target_schema = target_schema
        self.target_table_id = destination.get_full_table_id(
            target_schema, target_table_name
        )

        self.state_manager = HistorizeStateManager(destination, database, target_schema)
        self.sql_builder = HistorizeSqlBuilder(
            config=self.config,
            destination=destination,
            source_table_id=self.source_table_id,
            target_table_id=self.target_table_id,
            primary_key=self.config.primary_key,
            source_database=self._src_database,
            source_schema=self._src_schema,
            source_table=self._src_table,
            target_table_name=target_table_name,
            target_schema=target_schema,
        )

    def run(self) -> Dict[str, Any]:
        """Execute historization and return stats.

        Returns:
            Dict with keys: mode, snapshots_processed, new_or_changed_rows,
            deleted_rows, duration, status
        """
        started_at = datetime.now(timezone.utc)
        run_start = time.time()
        try:
            stats = self._execute_run(started_at, run_start)
            stats["duration"] = time.time() - run_start
            stats["status"] = "completed"
            return stats
        except ValueError as e:
            # Configuration errors — no traceback, let CLI handle display
            return {
                "mode": "failed",
                "snapshots_processed": 0,
                "new_or_changed_rows": 0,
                "deleted_rows": 0,
                "duration": time.time() - run_start,
                "status": "failed",
                "error": str(e),
            }
        except Exception as e:
            # Unexpected errors — include traceback for debugging
            logger.error(
                f"Historization failed for {self.pipeline_name}: {e}", exc_info=True
            )
            return {
                "mode": "failed",
                "snapshots_processed": 0,
                "new_or_changed_rows": 0,
                "deleted_rows": 0,
                "duration": time.time() - run_start,
                "status": "failed",
                "error": str(e),
            }

    def _execute_run(self, started_at: datetime, run_start: float) -> Dict[str, Any]:
        """Core historization logic: state check, mode selection, execution.

        Separated from run() to keep cyclomatic complexity within limits.
        Raises ValueError for config errors, Exception for unexpected failures.
        """
        timings = {}

        self.config.validate(self.config_dict)
        self._validate_destination_capabilities()

        # Init phase: fetch state (creates log table on first access if needed)
        t = time.time()
        state = self.state_manager.get_pipeline_state(self.pipeline_name)

        # Config-changed guard runs for ALL modes (including partial refresh).
        # PK/track_columns/ignore_columns changes affect ALL SCD2 records — partial refresh
        # is not a substitute for a full-refresh after semantic config changes.
        if self.state_manager.config_changed(state, self.config):
            if not self.full_refresh:
                self._raise_config_changed_error(state)

        is_partial = self.partial_refresh or self.historize_from is not None

        # Early exit for incremental with no new snapshots.
        # Skipped for partial refresh — it reprocesses a specific window
        # regardless of incremental state.
        new_snapshots = None
        if not self.full_refresh and not is_partial and state.has_successful_run:
            new_snapshots = self.state_manager.discover_unprocessed_snapshots(
                state=state,
                source_table_id=self.source_table_id,
                snapshot_column=self.config.snapshot_column,
            )
            if not new_snapshots:
                timings["init"] = time.time() - t
                logger.info(f"No new snapshots to historize for {self.pipeline_name}")
                return {
                    "mode": "incremental",
                    "snapshots_processed": 0,
                    "new_or_changed_rows": 0,
                    "deleted_rows": 0,
                    "timings": timings,
                }

        value_columns = self._discover_value_columns()

        # Mode selection:
        # - partial_refresh/historize_from + no prior run → full reprocess
        #   (nothing to clone; warn if historize_from was specified)
        # - partial_refresh/historize_from + has prior run → partial refresh
        # - full_refresh or first run → full reprocess
        # - otherwise → incremental
        needs_full = self.full_refresh or not state.has_successful_run
        timings["init"] = time.time() - t

        t = time.time()
        if needs_full:
            self._setup_full_reprocess(value_columns, is_partial, state)
        timings["setup"] = time.time() - t

        t = time.time()
        if is_partial and not needs_full:
            stats = self._run_partial_refresh(value_columns, started_at)
        elif needs_full:
            stats = self._run_full_reprocess(value_columns, started_at)
        else:
            stats = self._run_incremental(
                value_columns, started_at, state, new_snapshots
            )
        timings["execute"] = time.time() - t

        stats["timings"] = timings
        return stats

    def _setup_full_reprocess(
        self,
        value_columns: List[str],
        is_partial: bool,
        state: "HistorizeStateManager.PipelineState",
    ) -> None:
        """Prepare for full reprocess: warn if historize_from is being ignored, clear log, create table."""
        if is_partial and not self.partial_refresh and not state.has_successful_run:
            logger.warning(
                f"No prior historization run found for {self.pipeline_name}. "
                f"--historize-from {self.historize_from} will be ignored; "
                "running full reprocess instead."
            )
        self.state_manager.clear_log_entries(self.pipeline_name)
        self._create_target_table(value_columns, replace=True)

    def _validate_destination_capabilities(self) -> None:
        """Validate that the destination supports the configured historize options."""
        dest_type = type(self.destination).__name__
        table_format = self.config.table_format or "native"

        if (
            self.config.partition_column
            and self.config.partition_column != "_dlt_valid_from"
            and not self.destination.supports_partitioning()
        ):
            raise ValueError(
                f"Destination {dest_type} does not support partitioning. "
                f"Remove 'partition_column' from the historize section of "
                f"pipeline '{self.pipeline_name}'."
            )
        if self.config.cluster_columns and not self.destination.supports_clustering():
            raise ValueError(
                f"Destination {dest_type} does not support clustering. "
                f"Remove 'cluster_columns' from the historize section of "
                f"pipeline '{self.pipeline_name}'."
            )

        # Databricks Iceberg does not support cluster_columns
        if (
            table_format == "iceberg"
            and dest_type == "DatabricksDestination"
            and self.config.cluster_columns
        ):
            raise ValueError(
                f"Databricks Iceberg tables do not support cluster_columns. "
                f"Remove 'cluster_columns' from the historize section of "
                f"pipeline '{self.pipeline_name}', or use table_format: delta_uniform "
                f"if you need both Delta and Iceberg compatibility."
            )

    def _raise_config_changed_error(self, state) -> None:
        """Raise a ValueError with a human-readable diff of config changes."""
        previous = self.state_manager.decode_fingerprint(state.config_fingerprint)
        current = self.state_manager.decode_fingerprint(
            self.state_manager.compute_fingerprint(self.config)
        )
        changes = []
        for key in current:
            # Skip keys absent from previous (new fingerprint keys added after first run —
            # these are reported by config_changed only when both dicts have the key)
            if key in previous and previous[key] != current[key]:
                changes.append(f"  {key}: {previous[key]} → {current[key]}")
        diff = "\n".join(changes)
        is_partial = self.partial_refresh or self.historize_from is not None
        if is_partial:
            suffix = (
                "Note: --partial-refresh and --historize-from are not substitutes "
                "for semantic config changes (PK, track_columns, ignore_columns).\n"
                "Run 'saga historize --full-refresh' to rebuild the historized table."
            )
        else:
            suffix = (
                "Run 'saga historize --full-refresh' to rebuild the historized table."
            )
        raise ValueError(f"Historization config changed:\n{diff}\n{suffix}")

    def _discover_value_columns(self) -> List[str]:
        """Discover non-PK, non-system columns from the source table."""
        sql = self.sql_builder.build_discover_columns_sql()
        rows = list(self.destination.execute_sql(sql, self.schema))
        columns = [row.column_name for row in rows]

        if not columns:
            raise ValueError(
                f"No value columns found in {self.source_table_id}. "
                f"Check that the table exists and has non-system columns."
            )

        hash_columns = self.sql_builder._get_hash_columns(columns)
        if not hash_columns:
            hint = ""
            if self.config.ignore_columns:
                hint = (
                    f" ignore_columns {self.config.ignore_columns} covers all "
                    f"value columns — every row will hash the same, so no SCD2 "
                    f"changes will ever be detected."
                )
            elif self.config.track_columns:
                hint = (
                    f" track_columns {self.config.track_columns} does not match "
                    f"any columns in the source table."
                )
            raise ValueError(
                f"Change-detection hash for '{self.pipeline_name}' would be built "
                f"over zero columns.{hint} Adjust track_columns or ignore_columns."
            )

        logger.debug(f"Discovered {len(columns)} value columns for change detection")
        return columns

    def _create_target_table(
        self, value_columns: List[str], replace: bool = False
    ) -> None:
        """Create the historized target table."""
        if replace:
            # DROP first — BigQuery/Databricks rejects CREATE OR REPLACE TABLE
            # when the new spec changes partitioning or clustering.
            drop_sql = self.sql_builder.build_drop_target_table_sql()
            self.destination.execute_sql(drop_sql, self.schema)

        # Ensure the target schema exists when it differs from the source schema
        # (happens when placement=schema_suffix or when output_dataset is set explicitly)
        if self.target_schema != self.schema:
            self.destination.ensure_schema_exists(self.target_schema)

        sql = self.sql_builder.build_create_target_table_sql(
            value_columns, replace=replace
        )
        self.destination.execute_sql(sql, self.schema)
        action = "Replaced" if replace else "Created"
        logger.info(f"{action} historized table: {self.target_table_id}")

    def _run_full_reprocess(
        self, value_columns: List[str], started_at: datetime
    ) -> Dict[str, Any]:
        """Execute full reprocess: rebuild entire historized table from all raw data."""
        logger.info(f"Full reprocess historization for {self.pipeline_name}")

        sql = self.sql_builder.build_full_reprocess_sql(value_columns)
        logger.debug(f"Full reprocess SQL ({len(sql)} chars)")

        self.destination.execute_sql(sql, self.schema)

        finished_at = datetime.now(timezone.utc)

        # Single query: stats from target + max snapshot from source
        # (max snapshot must come from source, not target, because unchanged
        # snapshots don't produce rows in the target table)
        tgt = self.target_table_id
        src = self.source_table_id
        cast_max = self.destination.cast_to_string(
            f"MAX({self.config.snapshot_column})"
        )
        stats_sql = f"""
            WITH target_stats AS (
                SELECT SUM(CASE WHEN NOT _dlt_is_deleted THEN 1 ELSE 0 END) AS new_or_changed_rows,
                       SUM(CASE WHEN _dlt_is_deleted THEN 1 ELSE 0 END) AS deleted_rows
                FROM {tgt}
            ),
            source_stats AS (
                SELECT {cast_max} AS last_snapshot
                FROM {src}
            )
            SELECT * FROM target_stats CROSS JOIN source_stats
        """
        rows = list(self.destination.execute_sql(stats_sql, self.schema))
        row = rows[0] if rows else None
        new_or_changed = row.new_or_changed_rows if row else 0
        deleted = row.deleted_rows if row else 0
        max_snapshot = row.last_snapshot if row else None

        # Log the run
        fingerprint = self.state_manager.compute_fingerprint(self.config)
        self.state_manager.write_log_entry(
            HistorizeLogEntry(
                pipeline_name=self.pipeline_name,
                source_table=self.source_table_id,
                target_table=self.target_table_id,
                snapshot_value=max_snapshot,
                new_or_changed_rows=new_or_changed,
                deleted_rows=deleted,
                config_fingerprint=fingerprint,
                is_full_reprocess=True,
                started_at=started_at,
                finished_at=finished_at,
                status="completed",
            )
        )

        logger.info(
            f"Full reprocess complete for {self.pipeline_name}: "
            f"{new_or_changed:,} rows, {deleted:,} deletions"
        )

        return {
            "mode": "full_reprocess",
            "snapshots_processed": "all",
            "new_or_changed_rows": new_or_changed,
            "deleted_rows": deleted,
        }

    def _run_incremental(
        self,
        value_columns: List[str],
        started_at: datetime,
        state: "HistorizeStateManager.PipelineState",
        new_snapshots: List[str],
    ) -> Dict[str, Any]:
        """Execute incremental historization: process only new snapshots."""
        logger.info(
            f"Incremental historization for {self.pipeline_name}: "
            f"{len(new_snapshots)} new snapshot(s)"
        )

        # Use last historized snapshot from pre-fetched state as reference for LAG
        last_historized = state.last_snapshot_value

        # Build and execute incremental SQL
        sql = self.sql_builder.build_incremental_sql(
            value_columns=value_columns,
            new_snapshots=new_snapshots,
            last_historized_snapshot=last_historized,
        )
        logger.debug(f"Incremental SQL ({len(sql)} chars)")

        t_sql = time.time()
        self.destination.execute_sql(sql, self.schema)
        logger.debug(f"Incremental SQL executed in {time.time() - t_sql:.1f}s")

        finished_at = datetime.now(timezone.utc)

        # Get stats: count new/changed rows inserted + deletions marked
        # Query target table for rows affected by processed snapshots
        t_stats = time.time()
        snapshot_list = ", ".join(f"TIMESTAMP '{s}'" for s in new_snapshots)
        tgt = self.target_table_id
        stats_sql = f"""
            SELECT
                SUM(CASE WHEN NOT _dlt_is_deleted THEN 1 ELSE 0 END) AS new_or_changed_rows,
                SUM(CASE WHEN _dlt_is_deleted THEN 1 ELSE 0 END) AS deleted_rows
            FROM {tgt}
            WHERE _dlt_valid_from IN ({snapshot_list})
        """
        rows = list(self.destination.execute_sql(stats_sql, self.schema))
        row = rows[0] if rows else None
        new_or_changed = row.new_or_changed_rows if row else 0
        deleted = row.deleted_rows if row else 0
        logger.debug(f"Stats query executed in {time.time() - t_stats:.1f}s")

        # Log with last snapshot value for state tracking
        fingerprint = self.state_manager.compute_fingerprint(self.config)
        last_snapshot = new_snapshots[-1]
        self.state_manager.write_log_entry(
            HistorizeLogEntry(
                pipeline_name=self.pipeline_name,
                source_table=self.source_table_id,
                target_table=self.target_table_id,
                snapshot_value=last_snapshot,
                new_or_changed_rows=new_or_changed,
                deleted_rows=deleted,
                config_fingerprint=fingerprint,
                is_full_reprocess=False,
                started_at=started_at,
                finished_at=finished_at,
                status="completed",
            )
        )

        logger.info(
            f"Incremental historization complete for {self.pipeline_name}: "
            f"{len(new_snapshots)} snapshot(s), {new_or_changed} changes, "
            f"{deleted} deletions"
        )

        return {
            "mode": "incremental",
            "snapshots_processed": len(new_snapshots),
            "new_or_changed_rows": new_or_changed,
            "deleted_rows": deleted,
        }

    def _run_partial_refresh(
        self,
        value_columns: List[str],
        started_at: datetime,
    ) -> Dict[str, Any]:
        """Clone-and-swap partial re-historization.

        1. Resolve effective_from_date (min raw snapshot or clamped historize_from)
        2. Discover snapshots to reprocess + LAG reference (before cloning)
        3. Clone → rollback → incremental on staging → swap → update log
        """
        import uuid

        run_id = uuid.uuid4().hex[:8]
        target_schema = self.config.output_dataset or self.schema

        # Resolve effective boundary
        effective_from_date, clamped = self._resolve_effective_from_date()
        if effective_from_date is None:
            raise ValueError(
                f"No raw snapshots found in {self.source_table_id}; "
                "cannot determine reprocessing boundary."
            )
        if clamped:
            logger.warning(
                f"--historize-from {self.historize_from} predates earliest raw snapshot "
                f"({effective_from_date}). Using {effective_from_date} as effective start date."
            )

        # Discover snapshots to reprocess before cloning (avoids clone cost on early exit)
        snapshots_to_reprocess, lag_reference = self._discover_reprocess_snapshots(
            effective_from_date, target_schema
        )
        if not snapshots_to_reprocess:
            logger.info(
                f"No snapshots to reprocess from {effective_from_date} for "
                f"{self.pipeline_name}; nothing to do."
            )
            return {
                "mode": "partial_refresh",
                "snapshots_processed": 0,
                "new_or_changed_rows": 0,
                "deleted_rows": 0,
            }

        logger.info(
            f"Partial re-historization for {self.pipeline_name} from "
            f"{effective_from_date}: {len(snapshots_to_reprocess)} snapshot(s) to reprocess"
        )

        return self._execute_clone_and_swap(
            run_id=run_id,
            target_schema=target_schema,
            effective_from_date=effective_from_date,
            snapshots_to_reprocess=snapshots_to_reprocess,
            lag_reference=lag_reference,
            value_columns=value_columns,
            started_at=started_at,
        )

    def _execute_clone_and_swap(
        self,
        run_id: str,
        target_schema: str,
        effective_from_date: str,
        snapshots_to_reprocess: List[str],
        lag_reference: Optional[str],
        value_columns: List[str],
        started_at: datetime,
    ) -> Dict[str, Any]:
        """Clone target, rollback + re-run on staging, swap into place.

        Manages the staging/backup/keys lifecycle with try/finally cleanup.
        """
        staging_name = f"{self.target_table_name}__staging_{run_id}"
        staging_table_id = self.destination.get_full_table_id(
            target_schema, staging_name
        )
        backup_name = f"{self.target_table_name}__backup_{run_id}"
        backup_table_id = self.destination.get_full_table_id(target_schema, backup_name)
        keys_table_id = self.destination.get_full_table_id(
            target_schema, f"_rehistorize_keys_{run_id}"
        )

        swap_succeeded = False

        try:
            self.destination.clone_table(self.target_table_id, staging_table_id)
            logger.debug(f"Cloned {self.target_table_id} → {staging_table_id}")

            staging_builder = HistorizeSqlBuilder(
                config=self.config,
                destination=self.destination,
                source_table_id=self.source_table_id,
                target_table_id=staging_table_id,
                primary_key=self.config.primary_key,
                source_database=self._src_database,
                source_schema=self._src_schema,
                source_table=self._src_table,
            )

            self._rollback_staging(
                staging_builder, effective_from_date, run_id, target_schema
            )

            incremental_sql = staging_builder.build_incremental_sql(
                value_columns=value_columns,
                new_snapshots=snapshots_to_reprocess,
                last_historized_snapshot=lag_reference,
            )
            self.destination.execute_sql(incremental_sql, target_schema)
            logger.debug("Incremental re-run on staging complete")

            new_or_changed, deleted = self._collect_partial_stats(
                staging_table_id, snapshots_to_reprocess, target_schema
            )

            self._swap_tables(staging_table_id, backup_table_id, target_schema)
            swap_succeeded = True

            self._write_partial_log(
                effective_from_date,
                snapshots_to_reprocess,
                new_or_changed,
                deleted,
                started_at,
            )
            return {
                "mode": "partial_refresh",
                "snapshots_processed": len(snapshots_to_reprocess),
                "new_or_changed_rows": new_or_changed,
                "deleted_rows": deleted,
            }

        except Exception:
            if not swap_succeeded:
                flag = (
                    "--partial-refresh"
                    if self.partial_refresh
                    else f"--historize-from {effective_from_date}"
                )
                logger.info(
                    f"Original table is untouched. Re-run with {flag} to retry."
                )
            raise
        finally:
            try:
                self.destination.execute_sql(
                    f"DROP TABLE IF EXISTS {keys_table_id}", target_schema
                )
            except Exception as e:
                logger.debug(f"Could not drop keys table {keys_table_id}: {e}")

            if not swap_succeeded:
                try:
                    self.destination.execute_sql(
                        f"DROP TABLE IF EXISTS {staging_table_id}", target_schema
                    )
                except Exception as e:
                    logger.debug(
                        f"Could not drop staging table {staging_table_id}: {e}"
                    )

    def _rollback_staging(
        self,
        staging_builder: HistorizeSqlBuilder,
        effective_from_date: str,
        run_id: str,
        target_schema: str,
    ) -> None:
        """Execute rollback phases on the staging table.

        Phase 1: create affected-keys table (DDL, outside transaction).
        Phases 2-3: DELETE + UPDATE (in transaction if supported).
        """
        rollback_stmts = staging_builder.build_rollback_sql(
            effective_from_date, run_id, target_schema
        )

        # Phase 1: create affected-keys table (DDL, must be outside transaction)
        self.destination.execute_sql(rollback_stmts[0], target_schema)

        # Phases 2-3: DELETE + UPDATE
        if self.destination.supports_transactions():
            self.destination.execute_sql("BEGIN TRANSACTION", target_schema)
            try:
                self.destination.execute_sql(rollback_stmts[1], target_schema)
                self.destination.execute_sql(rollback_stmts[2], target_schema)
                self.destination.execute_sql("COMMIT", target_schema)
            except Exception:
                try:
                    self.destination.execute_sql("ROLLBACK", target_schema)
                except Exception as rb_exc:
                    logger.debug(
                        "ROLLBACK failed after rollback statement error: %s",
                        rb_exc,
                    )
                raise
        else:
            self.destination.execute_sql(rollback_stmts[1], target_schema)
            self.destination.execute_sql(rollback_stmts[2], target_schema)

    def _collect_partial_stats(
        self,
        staging_table_id: str,
        snapshots: List[str],
        target_schema: str,
    ) -> tuple:
        """Query row counts from the staging table for reprocessed snapshots."""
        snapshot_list = ", ".join(f"TIMESTAMP '{s}'" for s in snapshots)
        stats_sql = f"""
            SELECT
                SUM(CASE WHEN NOT _dlt_is_deleted THEN 1 ELSE 0 END) AS new_or_changed_rows,
                SUM(CASE WHEN _dlt_is_deleted THEN 1 ELSE 0 END) AS deleted_rows
            FROM {staging_table_id}
            WHERE _dlt_valid_from IN ({snapshot_list})
        """
        stat_rows = list(self.destination.execute_sql(stats_sql, target_schema))
        stat_row = stat_rows[0] if stat_rows else None
        new_or_changed = stat_row.new_or_changed_rows if stat_row else 0
        deleted = stat_row.deleted_rows if stat_row else 0
        return new_or_changed, deleted

    def _swap_tables(
        self,
        staging_table_id: str,
        backup_table_id: str,
        target_schema: str,
    ) -> None:
        """Swap staging into place: target → backup, staging → target, drop backup."""
        self.destination.rename_table(self.target_table_id, backup_table_id)
        try:
            self.destination.rename_table(staging_table_id, self.target_table_id)
        except Exception:
            logger.error(
                "Staging → target rename failed; restoring original from backup"
            )
            try:
                self.destination.rename_table(backup_table_id, self.target_table_id)
            except Exception:
                logger.error(
                    f"CRITICAL: Could not restore target from backup. "
                    f"Manual recovery: rename {backup_table_id} → "
                    f"{self.target_table_id}"
                )
            raise
        self.destination.execute_sql(
            f"DROP TABLE IF EXISTS {backup_table_id}", target_schema
        )
        logger.debug(f"Swap complete: {staging_table_id} is now {self.target_table_id}")

    def _write_partial_log(
        self,
        effective_from_date: str,
        snapshots_to_reprocess: List[str],
        new_or_changed: int,
        deleted: int,
        started_at: datetime,
    ) -> None:
        """Clear stale log entries and write new entry after successful swap."""
        self.state_manager.clear_log_entries_from(
            self.pipeline_name, effective_from_date
        )
        finished_at = datetime.now(timezone.utc)
        fingerprint = self.state_manager.compute_fingerprint(self.config)
        self.state_manager.write_log_entry(
            HistorizeLogEntry(
                pipeline_name=self.pipeline_name,
                source_table=self.source_table_id,
                target_table=self.target_table_id,
                snapshot_value=snapshots_to_reprocess[-1],
                new_or_changed_rows=new_or_changed,
                deleted_rows=deleted,
                config_fingerprint=fingerprint,
                is_full_reprocess=False,
                started_at=started_at,
                finished_at=finished_at,
                status="completed",
            )
        )
        logger.info(
            f"Partial re-historization complete for {self.pipeline_name}: "
            f"{len(snapshots_to_reprocess)} snapshot(s), "
            f"{new_or_changed} changes, {deleted} deletions"
        )

    def _resolve_effective_from_date(self) -> tuple:
        """Resolve the effective reprocessing boundary.

        Queries MIN(snapshot_col) from the source table.
        For --partial-refresh: returns min raw snapshot.
        For --historize-from: returns max(historize_from, min_raw_snapshot).

        Returns:
            Tuple of (effective_from_date_str, was_clamped):
            - effective_from_date_str: ISO timestamp string (format: YYYY-MM-DD HH:MM:SS)
            - was_clamped: True if historize_from was adjusted to min_raw_snapshot
        """
        src = self.source_table_id
        snapshot_col = self.config.snapshot_column
        target_schema = self.config.output_dataset or self.schema

        sql = f"SELECT MIN({snapshot_col}) AS min_snap FROM {src}"
        rows = list(self.destination.execute_sql(sql, target_schema))
        min_snap = rows[0].min_snap if rows else None

        if min_snap is None:
            return None, False

        # Normalize to timezone-aware datetime for safe comparison
        if hasattr(min_snap, "tzinfo") and min_snap.tzinfo is None:
            min_snap = min_snap.replace(tzinfo=timezone.utc)

        if self.partial_refresh:
            return min_snap.strftime("%Y-%m-%d %H:%M:%S"), False

        # historize_from case: compare with min_raw_snapshot
        hist_dt = datetime.fromisoformat(self.historize_from)
        if hist_dt.tzinfo is None:
            hist_dt = hist_dt.replace(tzinfo=timezone.utc)

        if hist_dt < min_snap:
            return min_snap.strftime("%Y-%m-%d %H:%M:%S"), True

        return self.historize_from, False

    def _discover_reprocess_snapshots(
        self, effective_from_date: str, target_schema: str
    ) -> tuple:
        """Discover snapshots to reprocess and the LAG reference snapshot.

        Args:
            effective_from_date: ISO timestamp string for the lower bound (inclusive).
            target_schema: Schema/dataset for query context.

        Returns:
            Tuple of (snapshots_list, lag_reference):
            - snapshots_list: Chronologically ordered list of snapshot values >= boundary
            - lag_reference: Last snapshot value before the boundary, or None
        """
        src = self.source_table_id
        snapshot_col = self.config.snapshot_column
        cast_expr = self.destination.cast_to_string(snapshot_col)
        safe_date = effective_from_date.replace("'", "''")

        # Snapshots to reprocess
        forward_sql = f"""
            SELECT DISTINCT {cast_expr} AS snapshot_val
            FROM {src}
            WHERE {snapshot_col} >= TIMESTAMP '{safe_date}'
            ORDER BY snapshot_val
        """
        rows = list(self.destination.execute_sql(forward_sql, target_schema))
        snapshots = [row.snapshot_val for row in rows]

        # Last snapshot before the boundary (used as LAG reference for incremental SQL)
        lag_sql = f"""
            SELECT {cast_expr} AS snapshot_val
            FROM {src}
            WHERE {snapshot_col} < TIMESTAMP '{safe_date}'
            ORDER BY {snapshot_col} DESC
            LIMIT 1
        """
        lag_rows = list(self.destination.execute_sql(lag_sql, target_schema))
        lag_reference = lag_rows[0].snapshot_val if lag_rows else None

        return snapshots, lag_reference
