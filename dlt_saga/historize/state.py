"""State management for the historize command.

Tracks which snapshots have been historized via the _saga_historize_log table,
detects primary key configuration changes, and determines what work needs to be done.
"""

import logging
from dataclasses import dataclass
from typing import Any, List, Optional

from dlt_saga.utility.cli.logging import PrefixedLoggerAdapter
from dlt_saga.utility.sql import looks_like_missing_table

logger = logging.getLogger(__name__)

# Cluster columns for the historize-log table — single source of truth shared by
# the create-time DDL (``_create_table_ddl``) and ``saga maintenance``'s
# clustering reconcile, so new and reconciled tables match. Reads filter on
# pipeline_name (never the started_at partition).
LOG_CLUSTER_COLUMNS = ["pipeline_name"]


@dataclass
class HistorizeLogEntry:
    """A single entry in the _saga_historize_log table."""

    pipeline_name: str
    source_table: str
    target_table: str
    snapshot_value: Optional[str]  # NULL for a failed run (no baseline established)
    new_or_changed_rows: int
    deleted_rows: int
    config_fingerprint: str  # base64-encoded JSON of config affecting historization
    is_full_reprocess: bool
    started_at: Any
    finished_at: Any
    status: str  # 'completed' | 'failed'


@dataclass
class LateArrivals:
    """Snapshots found at or below the watermark that arrived after the last run.

    ``min_snapshot`` is the earliest late snapshot value inside the accepted
    lateness window — the rewind/replay boundary (None when every late snapshot
    fell outside the window). ``snapshot_count`` is how many distinct in-window
    late snapshot values were found. ``ignored_count``/``ignored_min`` describe
    late snapshots older than ``late_arrival_window_days``, which are reported
    but never replayed automatically.
    """

    min_snapshot: Optional[str]
    snapshot_count: int
    ignored_count: int = 0
    ignored_min: Optional[str] = None


@dataclass
class SnapshotDiscovery:
    """What incremental discovery found in the source above the watermark.

    ``null_pk_columns`` rides along because discovery already scans exactly the
    rows the run will process, with the same filter — checking the primary key
    for NULLs there costs no extra query, and the offending snapshot is known.
    Empty when no primary key was passed or every key value is populated.
    """

    snapshots: List[str]
    null_pk_columns: List[str]
    null_pk_snapshot: Optional[str] = None


class HistorizeStateManager:
    """Manages historization state via _saga_historize_log table.

    Responsibilities:
    - Create the log table if it doesn't exist
    - Discover unprocessed snapshots by comparing ingest log with historize log
    - Detect primary key configuration changes
    - Write per-snapshot results to the log
    """

    def __init__(
        self,
        destination: Any,
        database: str,
        schema: str,
        log_prefix: Optional[str] = None,
    ):
        from dlt_saga.project_config import get_historize_log_table_name

        self.destination = destination
        self.database = database
        self.schema = schema
        self.log_table_name = get_historize_log_table_name()
        self.log_table_id = destination.get_full_table_id(schema, self.log_table_name)
        self.logger = (
            PrefixedLoggerAdapter(logger, log_prefix) if log_prefix else logger
        )

    def _create_table_ddl(self) -> str:
        """Generate DDL to create the log table using destination type names.

        Reads filter on ``pipeline_name`` (never ``started_at``), so the physical
        layout clusters on it to keep reads pruned as the log grows.
        ``partition_cluster_ddl`` reconciles per destination (BigQuery keeps the
        ``started_at`` partition and adds ``CLUSTER BY``; Databricks uses liquid
        clustering only; DuckDB emits neither).
        """
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
                status {d.type_name("string")}
            )
            {d.partition_cluster_ddl("started_at", LOG_CLUSTER_COLUMNS)}
        """

    def ensure_log_table(self) -> None:
        """Create the historize log table if it doesn't exist."""
        ddl = self._create_table_ddl()
        self.destination.execute_sql(ddl, self.schema)
        self.logger.debug(f"Ensured historize log table exists: {self.log_table_id}")

    @dataclass
    class PipelineState:
        """Cached state from a single log table query."""

        last_snapshot_value: Optional[str] = None
        last_finished_at: Optional[Any] = None
        last_started_at: Optional[Any] = None
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
        safe_name = self.destination.escape_string_literal(pipeline_name)
        # snapshot_value IS NOT NULL excludes empty-source runs: a full reprocess
        # over an empty/fully-filtered source has no max snapshot, so it does not
        # establish a baseline. Reading such an entry as the last successful run
        # would feed NULL into discover_unprocessed_snapshots (crash) and mask the
        # real prior baseline. Also self-heals pre-existing tables poisoned with a
        # NULL-snapshot completed row.
        sql = f"""
            SELECT snapshot_value, finished_at, started_at, config_fingerprint
            FROM {q}
            WHERE pipeline_name = '{safe_name}'
              AND status = 'completed'
              AND snapshot_value IS NOT NULL
            ORDER BY finished_at DESC
            LIMIT 1
        """
        try:
            rows = list(self.destination.execute_sql(sql, self.schema))
        except Exception as exc:
            # Only treat "table doesn't exist" as a recoverable case. Permission
            # denials, network errors, and SQL errors propagate so the operator
            # sees them instead of silently re-processing all history.
            if not looks_like_missing_table(exc):
                raise
            self.ensure_log_table()
            return self.PipelineState()

        if rows:
            return self.PipelineState(
                last_snapshot_value=rows[0].snapshot_value,
                last_finished_at=rows[0].finished_at,
                last_started_at=rows[0].started_at,
                config_fingerprint=rows[0].config_fingerprint,
                has_successful_run=True,
            )
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
            # Output column names rename the SCD2 columns. Changing one after the
            # historized table exists would emit SQL referencing columns the table
            # doesn't have, so it mandates a full refresh like the other keys here.
            "valid_from_column": config.valid_from_column,
            "valid_to_column": config.valid_to_column,
            "is_deleted_column": config.is_deleted_column,
            "partition_column": config.partition_column,
            # merge_key scopes deletion / reappearance detection. Changing it
            # changes which historical rows would be marked as deleted vs. open
            # for any historical replay, so it mandates a full refresh.
            "merge_key": sorted(config.merge_key) if config.merge_key else [],
            # Filters affect *which* rows enter the historized table.  A
            # change requires a full rebuild — otherwise rows that no
            # longer pass the filter would survive as stale history.
            # Serialised with sort_keys so dict-order isn't load-bearing.
            "filters": json.dumps(config.filters or [], sort_keys=True),
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
            return f"'{self.destination.escape_string_literal(str(v))}'"

        q = self.log_table_id
        sql = f"""
            INSERT INTO {q}
            (pipeline_name, source_table, target_table, snapshot_value,
             new_or_changed_rows, deleted_rows,
             config_fingerprint, is_full_reprocess, started_at, finished_at, status)
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
                {_fmt(entry.status)}
            )
        """
        self.destination.execute_sql(sql, self.schema)

    def get_historized_targets(self, pipeline_name: str) -> List[str]:
        """Return the distinct target tables this pipeline's historize layer wrote.

        Ownership is read from the log rather than re-derived from the config's
        naming: the log records the ``target_table`` each run actually wrote
        under this ``pipeline_name``. ``saga destroy`` uses this so it can never
        drop a table it doesn't own — a coincidental name match on another
        pipeline's table is impossible, because that table was never logged
        under this pipeline_name. It also stays correct across placement /
        ``table_name`` renames, since the log holds the name used at write
        time (the real orphan) rather than the current derived name. Mirrors
        ``Destination.get_ingested_targets`` for the ingest layer.

        Returns an empty list when the log table doesn't exist (nothing was
        ever historized) or the pipeline has no entries.
        """
        q = self.log_table_id
        safe_name = self.destination.escape_string_literal(pipeline_name)
        sql = f"""
            SELECT DISTINCT target_table
            FROM {q}
            WHERE pipeline_name = '{safe_name}'
              AND target_table IS NOT NULL
        """
        try:
            rows = list(self.destination.execute_sql(sql, self.schema))
        except Exception as exc:
            if not looks_like_missing_table(exc):
                raise
            return []
        return [r.target_table for r in rows if getattr(r, "target_table", None)]

    def clear_log_entries(self, pipeline_name: str) -> None:
        """Delete all log entries for a pipeline (used during full refresh)."""
        q = self.log_table_id
        safe_name = self.destination.escape_string_literal(pipeline_name)
        sql = f"""
            DELETE FROM {q}
            WHERE pipeline_name = '{safe_name}'
        """
        try:
            self.destination.execute_sql(sql, self.schema)
            # Debug, not info: callers (full-refresh / destroy) own the
            # user-facing message.
            self.logger.debug(f"Cleared historize log entries for {pipeline_name}")
        except Exception as exc:
            # A missing log table is fine (nothing to clear on first run). But a
            # permission/transient failure must propagate: leaving stale
            # `completed` entries makes the next run go incremental against a
            # freshly rebuilt table — silent data divergence during full refresh.
            if not looks_like_missing_table(exc):
                raise
            self.logger.debug(f"No historize log entries to clear for {pipeline_name}")

    def clear_log_entries_from(self, pipeline_name: str, historize_from: str) -> None:
        """Delete log entries for snapshots on or after historize_from.

        Uses CAST(snapshot_value AS TIMESTAMP) for robust chronological comparison
        across destinations with differing string representations.

        Args:
            pipeline_name: Pipeline identifier.
            historize_from: ISO timestamp string for the lower bound (inclusive).
        """
        q = self.log_table_id
        safe_name = self.destination.escape_string_literal(pipeline_name)
        safe_from = self.destination.escape_string_literal(historize_from)
        sql = f"""
            DELETE FROM {q}
            WHERE pipeline_name = '{safe_name}'
              AND CAST(snapshot_value AS TIMESTAMP) >= TIMESTAMP '{safe_from}'
        """
        try:
            self.destination.execute_sql(sql, self.schema)
            self.logger.info(
                f"Cleared historize log entries from {historize_from} for {pipeline_name}"
            )
        except Exception as exc:
            # Missing log table is fine; a real failure must propagate rather
            # than silently leaving stale entries for the range being reprocessed.
            if not looks_like_missing_table(exc):
                raise
            self.logger.debug(
                f"No historize log entries to clear from {historize_from} "
                f"for {pipeline_name}"
            )

    def discover_unprocessed_snapshots(
        self,
        state: "HistorizeStateManager.PipelineState",
        source_table_id: str,
        snapshot_column: str,
        filter_sql: Optional[str] = None,
        primary_key: Optional[List[str]] = None,
        min_snapshot_inclusive: Optional[str] = None,
    ) -> SnapshotDiscovery:
        """Discover snapshot values in the raw table that haven't been historized yet.

        Args:
            state: Pre-fetched pipeline state from get_pipeline_state()
            source_table_id: Fully qualified source table ID
            snapshot_column: Column containing snapshot timestamps
            filter_sql: Optional pre-rendered SQL WHERE body (no leading
                ``WHERE``) applied to the source read so the discovered
                snapshots match what historize will actually process.
            primary_key: When given, the same scan also reports which of these
                columns contain NULL (see :class:`SnapshotDiscovery`). Folded in
                here rather than probed separately because this query already
                reads exactly the rows the run will process.
            min_snapshot_inclusive: When set, discover every snapshot at or
                after this value instead of strictly above the watermark — the
                replay window for a late-arrival rewind (see
                :meth:`find_late_arrivals`). Includes already-historized
                snapshots by design: the rewind replays them.

        Returns:
            A :class:`SnapshotDiscovery` whose ``snapshots`` are ordered
            chronologically.
        """
        from dlt_saga.historize.sql import null_pk_alias
        from dlt_saga.utility.filters import and_filter, filter_where_clause

        src = source_table_id
        q_snapshot = self.destination.quote_identifier(snapshot_column)
        cast_expr = self.destination.cast_to_string(q_snapshot)
        pk_cols = list(primary_key or [])

        # DISTINCT vs GROUP BY on the same expression are equivalent; grouping is
        # what lets the NULL-key flags ride along per snapshot. The GROUP BY
        # repeats the expression rather than referencing the alias — portable
        # across all three destinations.
        null_flags = "".join(
            f", MAX(CASE WHEN {self.destination.quote_identifier(pk)} IS NULL "
            f"THEN 1 ELSE 0 END) AS {null_pk_alias(i)}"
            for i, pk in enumerate(pk_cols)
        )
        projection = f"{cast_expr} AS snapshot_val{null_flags}"
        grouping = f"GROUP BY {cast_expr}" if pk_cols else ""

        if min_snapshot_inclusive is not None:
            safe_val = self.destination.escape_string_literal(min_snapshot_inclusive)
            base_where = f"{q_snapshot} >= TIMESTAMP '{safe_val}'"
            where = f" WHERE {and_filter(filter_sql, base_where)}"
        elif not state.has_successful_run:
            where = filter_where_clause(filter_sql)
        else:
            safe_val = self.destination.escape_string_literal(state.last_snapshot_value)
            base_where = f"{q_snapshot} > TIMESTAMP '{safe_val}'"
            where = f" WHERE {and_filter(filter_sql, base_where)}"

        distinct = "" if pk_cols else "DISTINCT "
        sql = f"""
            SELECT {distinct}{projection}
            FROM {src}{where}
            {grouping}
            ORDER BY snapshot_val
        """

        rows = list(self.destination.execute_sql(sql, self.schema))
        snapshots = [row.snapshot_val for row in rows]
        self.logger.debug(f"Discovered {len(snapshots)} unprocessed snapshot(s)")

        null_pk_columns: List[str] = []
        null_pk_snapshot: Optional[str] = None
        for row in rows:
            offending = [
                pk for i, pk in enumerate(pk_cols) if getattr(row, null_pk_alias(i), 0)
            ]
            if offending:
                null_pk_columns = offending
                null_pk_snapshot = row.snapshot_val
                break

        return SnapshotDiscovery(
            snapshots=snapshots,
            null_pk_columns=null_pk_columns,
            null_pk_snapshot=null_pk_snapshot,
        )

    def find_late_arrivals(
        self,
        state: "HistorizeStateManager.PipelineState",
        source_table_id: str,
        snapshot_column: str,
        arrival_column: str,
        filter_sql: Optional[str] = None,
        window_days: Optional[int] = None,
    ) -> Optional[LateArrivals]:
        """Find snapshots at or below the watermark that arrived after the last run.

        The incremental watermark assumes the snapshot column advances
        monotonically for the whole source. A row whose snapshot value is at or
        below the watermark but whose ``arrival_column`` (warehouse load time)
        postdates the previous run's start is a late arrival — watermark-only
        discovery would never see it. Comparing against the previous run's
        *start* (not finish) is deliberately conservative: a row loaded while
        that run was executing may or may not have been read, so it is offered
        again; the replay is idempotent, so the worst case is one redundant
        replay.

        Kept as a separate query rather than folded into
        :meth:`discover_unprocessed_snapshots`'s WHERE: an OR across the two
        predicates would defeat snapshot-column partition pruning on the common
        no-late-arrivals path, while this query prunes on ``arrival_column``
        (append-mode raw tables are auto-clustered on ``_dlt_ingested_at``).

        ``window_days`` bounds accepted lateness: late snapshots more than that
        many days behind the watermark are counted (``ignored_count``) but never
        offered for replay. Split with conditional aggregation so bounded and
        unbounded detection cost the same single scan.

        Returns None when there are no late arrivals (or no prior run to
        compare against).
        """
        if not state.has_successful_run or state.last_started_at is None:
            return None

        from dlt_saga.utility.filters import and_filter

        q_snapshot = self.destination.quote_identifier(snapshot_column)
        q_arrival = self.destination.quote_identifier(arrival_column)
        cast_expr = self.destination.cast_to_string(q_snapshot)
        safe_wm = self.destination.escape_string_literal(state.last_snapshot_value)
        started = state.last_started_at
        started_iso = (
            started.isoformat() if hasattr(started, "isoformat") else str(started)
        )
        safe_started = self.destination.escape_string_literal(started_iso)

        base_where = (
            f"{q_snapshot} <= TIMESTAMP '{safe_wm}' "
            f"AND {q_arrival} > TIMESTAMP '{safe_started}'"
        )

        window_bound = self._window_lower_bound(state.last_snapshot_value, window_days)
        if window_bound is None:
            projection = f"""MIN({cast_expr}) AS min_snapshot,
                   COUNT(DISTINCT {cast_expr}) AS snapshot_count,
                   CAST(NULL AS {self.destination.type_name("string")}) AS ignored_min,
                   0 AS ignored_count"""
        else:
            safe_bound = self.destination.escape_string_literal(window_bound)
            in_window = f"{q_snapshot} >= TIMESTAMP '{safe_bound}'"
            projection = f"""MIN(CASE WHEN {in_window} THEN {cast_expr} END) AS min_snapshot,
                   COUNT(DISTINCT CASE WHEN {in_window} THEN {cast_expr} END) AS snapshot_count,
                   MIN(CASE WHEN NOT ({in_window}) THEN {cast_expr} END) AS ignored_min,
                   COUNT(DISTINCT CASE WHEN NOT ({in_window}) THEN {cast_expr} END) AS ignored_count"""

        sql = f"""
            SELECT {projection}
            FROM {source_table_id}
            WHERE {and_filter(filter_sql, base_where)}
        """
        rows = list(self.destination.execute_sql(sql, self.schema))
        if not rows:
            return None
        row = rows[0]
        if row.min_snapshot is None and not (row.ignored_count or 0):
            return None
        return LateArrivals(
            min_snapshot=row.min_snapshot,
            snapshot_count=int(row.snapshot_count or 0),
            ignored_count=int(row.ignored_count or 0),
            ignored_min=row.ignored_min,
        )

    def _window_lower_bound(
        self, watermark: Optional[str], window_days: Optional[int]
    ) -> Optional[str]:
        """Render the accepted-lateness floor: watermark minus window_days.

        The watermark string is our own ``cast_to_string`` rendering of a
        timestamp, so it parses back with ``fromisoformat`` on every supported
        destination. If it ever doesn't, the window degrades to unbounded with
        a warning rather than blocking detection.
        """
        if window_days is None or watermark is None:
            return None
        from datetime import datetime, timedelta

        try:
            bound = datetime.fromisoformat(watermark) - timedelta(days=window_days)
        except ValueError:
            self.logger.warning(
                f"Could not parse watermark '{watermark}' to apply "
                f"late_arrival_window_days={window_days}; treating the window "
                f"as unbounded for this run"
            )
            return None
        return bound.isoformat(sep=" ")
