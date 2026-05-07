"""State management for the historize command.

Tracks which snapshots have been historized via the _saga_historize_log table,
detects primary key configuration changes, and determines what work needs to be done.
"""

import logging
from dataclasses import dataclass
from typing import Any, List, Optional

logger = logging.getLogger(__name__)


def _escape_sql_string(value: str) -> str:
    """Escape a string value for safe use in SQL single-quoted literals."""
    return value.replace("'", "''").replace("\\", "\\\\")


@dataclass
class HistorizeLogEntry:
    """A single entry in the _saga_historize_log table."""

    pipeline_name: str
    source_table: str
    target_table: str
    snapshot_value: str
    new_or_changed_rows: int
    deleted_rows: int
    config_fingerprint: str  # base64-encoded JSON of config affecting historization
    is_full_reprocess: bool
    started_at: Any
    finished_at: Any
    status: str  # 'completed' | 'failed'
    runner_version: int = (
        1  # Schema version for future-proofing (bump with ALTER TABLE)
    )


class HistorizeStateManager:
    """Manages historization state via _saga_historize_log table.

    Responsibilities:
    - Create the log table if it doesn't exist
    - Discover unprocessed snapshots by comparing ingest log with historize log
    - Detect primary key configuration changes
    - Write per-snapshot results to the log
    """

    def __init__(self, destination: Any, database: str, schema: str):
        from dlt_saga.project_config import get_historize_log_table_name

        self.destination = destination
        self.database = database
        self.schema = schema
        self.log_table_name = get_historize_log_table_name()
        self.log_table_id = destination.get_full_table_id(schema, self.log_table_name)

    # Schema version for the log table.  Increment when columns are added so
    # that ensure_log_table() can evolve existing tables via ALTER TABLE.
    _LOG_TABLE_VERSION = 1

    def _create_table_ddl(self) -> str:
        """Generate DDL to create the log table using destination type names."""
        d = self.destination
        q = self.log_table_id
        return f"""
            CREATE TABLE IF NOT EXISTS {q} (
                pipeline_name {d.type_name("string")},
                source_table {d.type_name("string")},
                target_table {d.type_name("string")},
                snapshot_value {d.type_name("string")},
                new_or_changed_rows {d.type_name("int64")},
                deleted_rows {d.type_name("int64")},
                config_fingerprint {d.type_name("string")},
                is_full_reprocess {d.type_name("bool")},
                started_at {d.type_name("timestamp")},
                finished_at {d.type_name("timestamp")},
                status {d.type_name("string")},
                runner_version {d.type_name("int64")}
            )
        """

    def ensure_log_table(self) -> None:
        """Create the historize log table if it doesn't exist."""
        ddl = self._create_table_ddl()
        self.destination.execute_sql(ddl, self.schema)
        logger.debug(f"Ensured historize log table exists: {self.log_table_id}")

    @dataclass
    class PipelineState:
        """Cached state from a single log table query."""

        last_snapshot_value: Optional[str] = None
        last_finished_at: Optional[Any] = None
        config_fingerprint: Optional[str] = None  # base64-encoded JSON
        has_successful_run: bool = False

    def get_pipeline_state(
        self, pipeline_name: str
    ) -> "HistorizeStateManager.PipelineState":
        """Fetch all relevant state for a pipeline in a single query.

        Returns snapshot_value, finished_at, and config_fingerprint from the
        last successful run.
        Creates the log table on first access if it doesn't exist.
        """
        q = self.log_table_id
        safe_name = _escape_sql_string(pipeline_name)
        sql = f"""
            SELECT snapshot_value, finished_at, config_fingerprint
            FROM {q}
            WHERE pipeline_name = '{safe_name}'
              AND status = 'completed'
            ORDER BY finished_at DESC
            LIMIT 1
        """
        try:
            rows = list(self.destination.execute_sql(sql, self.schema))
            if rows:
                return self.PipelineState(
                    last_snapshot_value=rows[0].snapshot_value,
                    last_finished_at=rows[0].finished_at,
                    config_fingerprint=rows[0].config_fingerprint,
                    has_successful_run=True,
                )
            return self.PipelineState()
        except Exception:
            # Table likely doesn't exist — create it and return empty state
            self.ensure_log_table()
            return self.PipelineState()

    @staticmethod
    def compute_fingerprint(config: Any) -> str:
        """Compute a base64-encoded fingerprint of config settings that affect historization.

        Changes to any of these settings require a full refresh, as they alter
        how change detection works or what the output contains.
        """
        import base64
        import json

        fingerprint_data = {
            "primary_key": sorted(config.primary_key),
            "track_columns": sorted(config.track_columns)
            if config.track_columns
            else [],
            "ignore_columns": sorted(config.ignore_columns),
            "snapshot_column": config.snapshot_column,
            "track_deletions": config.track_deletions,
            "table_format": config.table_format or "native",
        }
        return base64.b64encode(json.dumps(fingerprint_data).encode()).decode()

    @staticmethod
    def decode_fingerprint(encoded: str) -> dict:
        """Decode a base64-encoded config fingerprint back to a dict."""
        import base64
        import json

        return json.loads(base64.b64decode(encoded).decode())

    def config_changed(
        self, state: "HistorizeStateManager.PipelineState", config: Any
    ) -> bool:
        """Check if historization-affecting config has changed since last run.

        Comparison is done key-by-key against the stored fingerprint so that
        newly added fingerprint keys (e.g. table_format) don't produce false
        positives for pipelines whose stored fingerprint pre-dates the key.
        """
        if not state.has_successful_run:
            return False
        previous = self.decode_fingerprint(state.config_fingerprint)
        current = self.decode_fingerprint(self.compute_fingerprint(config))
        for key, value in current.items():
            if key in previous and previous[key] != value:
                return True
        return False

    def write_log_entry(self, entry: HistorizeLogEntry) -> None:
        """Write a single log entry to the historize log table."""
        from datetime import datetime

        def _fmt(v: Any) -> str:
            if v is None:
                return "NULL"
            if isinstance(v, bool):
                return "TRUE" if v else "FALSE"
            if isinstance(v, int):
                return str(v)
            if isinstance(v, datetime):
                return f"TIMESTAMP '{v.isoformat()}'"
            return f"'{_escape_sql_string(str(v))}'"

        q = self.log_table_id
        sql = f"""
            INSERT INTO {q}
            (pipeline_name, source_table, target_table, snapshot_value,
             new_or_changed_rows, deleted_rows,
             config_fingerprint, is_full_reprocess, started_at, finished_at, status,
             runner_version)
            VALUES (
                {_fmt(entry.pipeline_name)},
                {_fmt(entry.source_table)},
                {_fmt(entry.target_table)},
                {_fmt(entry.snapshot_value)},
                {_fmt(entry.new_or_changed_rows)},
                {_fmt(entry.deleted_rows)},
                {_fmt(entry.config_fingerprint)},
                {_fmt(entry.is_full_reprocess)},
                {_fmt(entry.started_at)},
                {_fmt(entry.finished_at)},
                {_fmt(entry.status)},
                {_fmt(entry.runner_version)}
            )
        """
        try:
            self.destination.execute_sql(sql, self.schema)
        except Exception as exc:
            # The runner_version column may not exist on a pre-existing log table
            # (deployments created before this field was added). Add the column and retry.
            if (
                "runner_version" in str(exc).lower()
                or "unknown column" in str(exc).lower()
                or "column" in str(exc).lower()
            ):
                logger.debug(
                    "Log table missing runner_version column — adding it: %s", exc
                )
                try:
                    alter_sql = (
                        f"ALTER TABLE {q} ADD COLUMN runner_version "
                        f"{self.destination.type_name('int64')}"
                    )
                    self.destination.execute_sql(alter_sql, self.schema)
                    self.destination.execute_sql(sql, self.schema)
                except Exception as alter_exc:
                    logger.warning(
                        "Could not add runner_version column to log table: %s",
                        alter_exc,
                    )
            else:
                raise

    def clear_log_entries(self, pipeline_name: str) -> None:
        """Delete all log entries for a pipeline (used during full refresh)."""
        q = self.log_table_id
        safe_name = _escape_sql_string(pipeline_name)
        sql = f"""
            DELETE FROM {q}
            WHERE pipeline_name = '{safe_name}'
        """
        try:
            self.destination.execute_sql(sql, self.schema)
            logger.info(f"Cleared historize log entries for {pipeline_name}")
        except Exception:
            # Table might not exist yet
            logger.debug(f"No historize log entries to clear for {pipeline_name}")

    def clear_log_entries_from(self, pipeline_name: str, historize_from: str) -> None:
        """Delete log entries for snapshots on or after historize_from.

        Uses CAST(snapshot_value AS TIMESTAMP) for robust chronological comparison
        across destinations with differing string representations.

        Args:
            pipeline_name: Pipeline identifier.
            historize_from: ISO timestamp string for the lower bound (inclusive).
        """
        q = self.log_table_id
        safe_name = _escape_sql_string(pipeline_name)
        safe_from = _escape_sql_string(historize_from)
        sql = f"""
            DELETE FROM {q}
            WHERE pipeline_name = '{safe_name}'
              AND CAST(snapshot_value AS TIMESTAMP) >= TIMESTAMP '{safe_from}'
        """
        try:
            self.destination.execute_sql(sql, self.schema)
            logger.info(
                f"Cleared historize log entries from {historize_from} for {pipeline_name}"
            )
        except Exception:
            logger.debug(
                f"No historize log entries to clear from {historize_from} "
                f"for {pipeline_name}"
            )

    def discover_unprocessed_snapshots(
        self,
        state: "HistorizeStateManager.PipelineState",
        source_table_id: str,
        snapshot_column: str,
    ) -> List[str]:
        """Discover snapshot values in the raw table that haven't been historized yet.

        Args:
            state: Pre-fetched pipeline state from get_pipeline_state()
            source_table_id: Fully qualified source table ID
            snapshot_column: Column containing snapshot timestamps

        Returns:
            List of snapshot values (as strings) to process, ordered chronologically
        """
        src = source_table_id
        cast_expr = self.destination.cast_to_string(snapshot_column)

        if not state.has_successful_run:
            sql = f"""
                SELECT DISTINCT {cast_expr} AS snapshot_val
                FROM {src}
                ORDER BY snapshot_val
            """
        else:
            safe_val = _escape_sql_string(state.last_snapshot_value)
            sql = f"""
                SELECT DISTINCT {cast_expr} AS snapshot_val
                FROM {src}
                WHERE {snapshot_column} > TIMESTAMP '{safe_val}'
                ORDER BY snapshot_val
            """

        rows = list(self.destination.execute_sql(sql, self.schema))
        snapshots = [row.snapshot_val for row in rows]
        logger.debug(f"Discovered {len(snapshots)} unprocessed snapshot(s)")
        return snapshots
