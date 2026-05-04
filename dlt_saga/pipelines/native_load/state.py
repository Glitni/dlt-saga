"""State management for the native_load adapter.

INSERT-only design: parallel pipelines sharing the same log table cannot trigger
BigQuery DML serialisation conflicts because they only INSERT. The companion view
collapses multiple events per (pipeline, uri) to the latest one for inspection.
"""

import hashlib
import logging
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Optional

from dlt_saga.pipelines.native_load._sql import esc_sql_literal
from dlt_saga.project_config import (
    get_native_load_log_table_name,
    get_native_load_log_view_name,
)

if TYPE_CHECKING:
    from dlt_saga.destinations.base import Destination

logger = logging.getLogger(__name__)

_STALE_HOURS = 24
_BULK_INSERT_CHUNK = 1000


def make_load_id(pipeline_name: str, uri: str, generation: Optional[int]) -> str:
    """Create a deterministic load ID from (pipeline, uri, generation).

    The same triple always produces the same 32-char hex string.
    A file overwritten in-place (new generation) produces a different ID.
    """
    payload = (
        f"{pipeline_name}\x00{uri}\x00{generation if generation is not None else ''}"
    )
    return hashlib.sha256(payload.encode()).hexdigest()[:32]


def _ts_literal(iso: str) -> str:
    """Format an ISO timestamp as a SQL TIMESTAMP literal."""
    return f"TIMESTAMP '{esc_sql_literal(iso)}'"


def _str_or_null(v: Optional[str]) -> str:
    if v is None:
        return "NULL"
    return f"'{esc_sql_literal(v)}'"


def _int_or_null(v: Optional[int]) -> str:
    if v is None:
        return "NULL"
    return str(int(v))


class NativeLoadStateManager:
    """INSERT-only state log for the native_load adapter.

    Tracks which source URIs (and their GCS generation) have been loaded for
    each pipeline. Never UPDATEs or DELETEs except for --full-refresh via
    clear_pipeline_state().
    """

    def __init__(self, destination: "Destination", dataset: str) -> None:
        self._dest = destination
        self._dataset = dataset
        self._table = get_native_load_log_table_name()
        self._view = get_native_load_log_view_name()

    # ------------------------------------------------------------------
    # Table / view setup
    # ------------------------------------------------------------------

    def ensure_table_exists(self) -> None:
        """Idempotent CREATE TABLE + companion view.

        Table schema uses destination-portable type names and partition/cluster DDL.
        """
        d = self._dest
        t_str = d.type_name("string")
        t_int = d.type_name("int64")
        t_ts = d.type_name("timestamp")

        table_id = d.get_full_table_id(self._dataset, self._table)
        partition_clause = d.partition_ddl("started_at", t_ts)

        ddl_parts = [
            f"CREATE TABLE IF NOT EXISTS {table_id} (",
            f"  load_id        {t_str}   NOT NULL,",
            f"  pipeline_name  {t_str}   NOT NULL,",
            f"  source_uri     {t_str}   NOT NULL,",
            f"  generation     {t_int},",
            f"  size_bytes     {t_int},",
            f"  job_id         {t_str},",
            f"  loaded_rows    {t_int},",
            f"  cursor_value   {t_str},",
            f"  status         {t_str}   NOT NULL,",
            f"  error_message  {t_str},",
            f"  started_at     {t_ts}    NOT NULL,",
            f"  finished_at    {t_ts}",
            ")",
        ]
        if partition_clause:
            ddl_parts.append(partition_clause)

        # Databricks: CLUSTER BY and PARTITIONED BY conflict — only add cluster
        # when there's no partition clause (or if the destination ignores it).
        # Safely skip cluster on the state log table to keep DDL portable.
        # (State log performance is not critical.)

        ddl = "\n".join(ddl_parts)
        d.execute_sql(ddl, self._dataset)

        self._ensure_view()

    def _ensure_view(self) -> None:
        view_sql = (
            f"SELECT * EXCEPT(rn) FROM ("
            f"  SELECT *, ROW_NUMBER() OVER ("
            f"    PARTITION BY pipeline_name, source_uri "
            f"    ORDER BY COALESCE(finished_at, started_at) DESC"
            f"  ) AS rn "
            f"  FROM {self._table}"
            f") WHERE rn = 1"
        )
        try:
            self._dest.create_or_replace_view(self._dataset, self._view, view_sql)
        except Exception as exc:
            logger.debug("Could not create companion view %s: %s", self._view, exc)

    # ------------------------------------------------------------------
    # Read methods
    # ------------------------------------------------------------------

    def get_last_cursor(self, pipeline_name: str) -> Optional[str]:
        """Return the most recent successful cursor_value for this pipeline."""
        table_id = self._dest.get_full_table_id(self._dataset, self._table)
        pn = esc_sql_literal(pipeline_name)
        sql = (
            f"SELECT cursor_value "
            f"FROM {table_id} "
            f"WHERE pipeline_name = '{pn}' AND status = 'success' "
            f"AND cursor_value IS NOT NULL "
            f"ORDER BY COALESCE(finished_at, started_at) DESC "
            f"LIMIT 1"
        )
        try:
            rows = self._dest.execute_sql(sql, self._dataset)
            if rows:
                row = list(rows)[0] if hasattr(rows, "__iter__") else rows
                val = (
                    row[0]
                    if hasattr(row, "__getitem__")
                    else getattr(row, "cursor_value", None)
                )
                return str(val) if val is not None else None
        except Exception as exc:
            logger.debug("Could not get last cursor: %s", exc)
        return None

    def get_loaded_uri_generations(
        self,
        pipeline_name: str,
        cursor_min: Optional[str] = None,
    ) -> set:
        """Return the set of (uri, generation) pairs already loaded or in-progress.

        'In-progress' means a 'started' row younger than STALE_HOURS — these are
        assumed to be from a concurrent run and are not re-attempted.
        'Stale started' rows (older than STALE_HOURS) are treated as orphaned and
        excluded so the next run retries them.
        """
        table_id = self._dest.get_full_table_id(self._dataset, self._table)
        pn = esc_sql_literal(pipeline_name)
        stale_cutoff = self._dest.timestamp_n_days_ago(_STALE_HOURS // 24)

        cursor_filter = ""
        if cursor_min:
            cm = esc_sql_literal(cursor_min)
            cursor_filter = f"AND (cursor_value IS NULL OR cursor_value >= '{cm}')"

        sql = (
            f"SELECT DISTINCT source_uri, generation "
            f"FROM {table_id} "
            f"WHERE pipeline_name = '{pn}' "
            f"AND ("
            f"  status = 'success' "
            f"  OR (status = 'started' AND started_at >= {stale_cutoff})"
            f") "
            f"{cursor_filter}"
        )
        try:
            rows = self._dest.execute_sql(sql, self._dataset)
            result = set()
            for row in rows:
                if hasattr(row, "source_uri"):
                    result.add((row.source_uri, row.generation))
                else:
                    result.add((row[0], row[1]))
            return result
        except Exception as exc:
            logger.debug("Could not get loaded uri/generations: %s", exc)
            return set()

    def get_loaded_uris(
        self, pipeline_name: str, cursor_min: Optional[str] = None
    ) -> set:
        """Thin wrapper — returns just the URI strings (generation-blind)."""
        return {
            uri for uri, _ in self.get_loaded_uri_generations(pipeline_name, cursor_min)
        }

    # ------------------------------------------------------------------
    # Write methods
    # ------------------------------------------------------------------

    def record_load_start(
        self,
        pipeline_name: str,
        uri: str,
        cursor_value: Optional[str],
        generation: Optional[int],
    ) -> tuple:
        """Single-row wrapper around the bulk insert."""
        load_ids, started_at = self.record_loads_started_bulk(
            pipeline_name, [(uri, cursor_value, generation)]
        )
        return load_ids[0], started_at

    def record_loads_started_bulk(
        self,
        pipeline_name: str,
        rows: list,
    ) -> tuple:
        """Bulk-INSERT 'started' rows.

        Args:
            pipeline_name: Pipeline identifier.
            rows: list of (uri, cursor_value, generation) tuples.

        Returns:
            (load_ids, started_at_iso) where load_ids is aligned with rows.
        """
        now = datetime.now(timezone.utc)
        started_at_iso = now.isoformat()
        load_ids = [make_load_id(pipeline_name, uri, gen) for uri, _, gen in rows]
        self._bulk_insert_rows(
            pipeline_name,
            rows,
            load_ids,
            "started",
            None,
            None,
            None,
            started_at_iso,
            None,
        )
        return load_ids, started_at_iso

    def record_loads_success_bulk(
        self,
        pipeline_name: str,
        rows: list,
        load_ids: list,
        job_id: str,
        size_bytes_by_load_id: dict,
        loaded_rows_by_load_id: dict,
        started_at: str,
    ) -> None:
        """Bulk-INSERT 'success' rows with all columns populated."""
        finished = datetime.now(timezone.utc).isoformat()
        self._bulk_insert_rows(
            pipeline_name,
            rows,
            load_ids,
            "success",
            job_id,
            size_bytes_by_load_id,
            loaded_rows_by_load_id,
            started_at,
            finished,
        )

    def record_loads_failed_bulk(
        self,
        pipeline_name: str,
        rows: list,
        load_ids: list,
        error: str,
        started_at: str,
    ) -> None:
        """Bulk-INSERT 'failed' rows."""
        finished = datetime.now(timezone.utc).isoformat()
        self._bulk_insert_rows(
            pipeline_name,
            rows,
            load_ids,
            "failed",
            None,
            None,
            None,
            started_at,
            finished,
            error_message=error,
        )

    def clear_pipeline_state(self, pipeline_name: str) -> None:
        """Delete all log rows for this pipeline. Called from --full-refresh only."""
        table_id = self._dest.get_full_table_id(self._dataset, self._table)
        pn = esc_sql_literal(pipeline_name)
        self._dest.execute_sql(
            f"DELETE FROM {table_id} WHERE pipeline_name = '{pn}'",
            self._dataset,
        )
        logger.debug("Cleared native_load state for pipeline %r", pipeline_name)

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _bulk_insert_rows(
        self,
        pipeline_name: str,
        rows: list,
        load_ids: list,
        status: str,
        job_id: Optional[str],
        size_bytes_by_load_id: Optional[dict],
        loaded_rows_by_load_id: Optional[dict],
        started_at: str,
        finished_at: Optional[str],
        error_message: Optional[str] = None,
    ) -> None:
        table_id = self._dest.get_full_table_id(self._dataset, self._table)
        col_list = (
            "load_id, pipeline_name, source_uri, generation, size_bytes, job_id, "
            "loaded_rows, cursor_value, status, error_message, started_at, finished_at"
        )

        chunks = [
            rows[i : i + _BULK_INSERT_CHUNK]
            for i in range(0, len(rows), _BULK_INSERT_CHUNK)
        ]
        for chunk_rows in chunks:
            value_rows = []
            for (uri, cursor_value, generation), load_id in zip(chunk_rows, load_ids):
                size = (size_bytes_by_load_id or {}).get(load_id)
                loaded = (loaded_rows_by_load_id or {}).get(load_id)

                value_rows.append(
                    f"("
                    f"'{esc_sql_literal(load_id)}', "
                    f"'{esc_sql_literal(pipeline_name)}', "
                    f"'{esc_sql_literal(uri)}', "
                    f"{_int_or_null(generation)}, "
                    f"{_int_or_null(size)}, "
                    f"{_str_or_null(job_id)}, "
                    f"{_int_or_null(loaded)}, "
                    f"{_str_or_null(cursor_value)}, "
                    f"'{esc_sql_literal(status)}', "
                    f"{_str_or_null(error_message)}, "
                    f"{_ts_literal(started_at)}, "
                    f"{_ts_literal(finished_at) if finished_at else 'NULL'}"
                    f")"
                )

            sql = f"INSERT INTO {table_id} ({col_list}) VALUES " + ",\n".join(
                value_rows
            )
            self._dest.execute_sql(sql, self._dataset)
