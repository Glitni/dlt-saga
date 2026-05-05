"""SQL generation for historization.

Generates destination-agnostic SQL for both incremental and full-reprocess
historization modes. Uses destination dialect methods for quoting, hashing,
and DDL constructs.

Adapts patterns from:
- temp/scd2_backfill.py (full reprocess with hash-based change detection)
- temp/adf_snapshot_historization.sql (incremental with deletion detection)
"""

import logging
from typing import Any, List, Optional

from dlt_saga.historize.config import HistorizeConfig

logger = logging.getLogger(__name__)

# System columns excluded from change detection hashing
SYSTEM_COLUMNS = {
    "_dlt_id",
    "_dlt_load_id",
    "_dlt_valid_from",
    "_dlt_valid_to",
    "_dlt_is_deleted",
    "_dlt_source_file_name",
    "_dlt_source_modification_date",
    "_dlt_ingested_at",
}


class HistorizeSqlBuilder:
    """Generates SQL for historizing snapshot data into SCD2 format.

    Supports two modes:
    - Full reprocess: Processes ALL raw data, INSERT into historized table
    - Incremental: Processes only new snapshots since last run, MERGE + INSERT
    """

    def __init__(
        self,
        config: HistorizeConfig,
        destination: Any,
        source_table_id: str,
        target_table_id: str,
        primary_key: List[str],
        source_database: str = "",
        source_schema: str = "",
        source_table: str = "",
    ):
        self.config = config
        self.destination = destination
        self.source_table_id = source_table_id
        self.target_table_id = target_table_id
        self.primary_key = primary_key
        self.source_database = source_database
        self.source_schema = source_schema
        self.source_table = source_table
        self._output_exclude = SYSTEM_COLUMNS | {config.snapshot_column}

    def _get_hash_columns(self, value_columns: List[str]) -> List[str]:
        """Return the subset of value_columns used for change-detection hashing.

        If track_columns is set, only those columns are considered (applied first).
        ignore_columns are then subtracted from the result.
        """
        if self.config.track_columns:
            candidate_set = set(self.config.track_columns) - set(
                self.config.ignore_columns
            )
            return [c for c in value_columns if c in candidate_set]
        ignore_set = set(self.config.ignore_columns)
        return [c for c in value_columns if c not in ignore_set]

    def _q(self, name: str) -> str:
        """Quote a table identifier using the destination's quoting style."""
        return self.destination.quote_identifier(name)

    def _pk_cols_sql(self) -> str:
        """Comma-separated primary key column list."""
        return ", ".join(self.primary_key)

    def _pk_join(self, left: str, right: str) -> str:
        """JOIN condition for primary keys between two aliases."""
        return " AND ".join(f"{left}.{pk} = {right}.{pk}" for pk in self.primary_key)

    def build_rollback_sql(
        self, effective_from_date: str, run_id: str, dataset: str
    ) -> List[str]:
        """SQL statements to roll back SCD2 records >= effective_from_date.

        Returns statements in order:
          [0] CREATE TABLE affected_keys (DDL — execute outside transaction)
          [1] DELETE SCD2 rows with _dlt_valid_from in the window
          [2] UPDATE reopen boundary records whose _dlt_valid_to falls in the window

        The caller wraps [1] and [2] in BEGIN/COMMIT if the destination supports
        transactions. The caller drops the affected-keys table in a finally block.

        Args:
            effective_from_date: ISO timestamp string for the rollback boundary (inclusive).
            run_id: Short unique identifier used in the affected-keys table name.
            dataset: Dataset/schema where the affected-keys table will be created.
        """
        pk_cols = self._pk_cols_sql()
        tgt = self.target_table_id
        src = self.source_table_id
        snapshot_col = self.config.snapshot_column
        safe_date = effective_from_date.replace("'", "''")

        keys_table = self.destination.get_full_table_id(
            dataset, f"_rehistorize_keys_{run_id}"
        )

        # PK membership check via IN subquery (no table aliases needed)
        if len(self.primary_key) == 1:
            pk = self.primary_key[0]
            pk_in_keys = f"{pk} IN (SELECT {pk} FROM {keys_table})"
        else:
            pk_in_keys = f"({pk_cols}) IN (SELECT {pk_cols} FROM {keys_table})"

        create_keys = f"""
            CREATE TABLE {keys_table} AS
            SELECT DISTINCT {pk_cols}
            FROM {src}
            WHERE {snapshot_col} >= TIMESTAMP '{safe_date}'
        """

        delete_sql = f"""
            DELETE FROM {tgt}
            WHERE _dlt_valid_from >= TIMESTAMP '{safe_date}'
              AND {pk_in_keys}
        """

        update_sql = f"""
            UPDATE {tgt}
            SET _dlt_valid_to = NULL
            WHERE _dlt_valid_from < TIMESTAMP '{safe_date}'
              AND _dlt_valid_to >= TIMESTAMP '{safe_date}'
              AND {pk_in_keys}
        """

        return [create_keys, delete_sql, update_sql]

    def build_discover_columns_sql(self) -> str:
        """SQL to discover value columns from the destination's schema catalog.

        Returns SQL that fetches column names from the source table,
        excluding PKs and system columns.
        """
        pk_set = set(self.primary_key)
        exclude = self._output_exclude | pk_set
        exclude_list = ", ".join(f"'{c}'" for c in exclude)

        base_query = self.destination.columns_query(
            self.source_database, self.source_schema, self.source_table
        )

        # Wrap to filter out excluded columns
        return f"""
            SELECT column_name FROM ({base_query}) sub
            WHERE column_name NOT IN ({exclude_list})
        """

    def build_drop_target_table_sql(self) -> str:
        """DROP TABLE IF EXISTS for the historized target table."""
        return f"DROP TABLE IF EXISTS {self.target_table_id}"

    def build_create_target_table_sql(
        self, value_columns: List[str], replace: bool = False
    ) -> str:
        """SQL to create the historized target table."""
        all_data_cols = list(self.primary_key) + list(value_columns)
        create_stmt = (
            "CREATE OR REPLACE TABLE" if replace else "CREATE TABLE IF NOT EXISTS"
        )
        src = self.source_table_id
        tgt = self.target_table_id
        ts_type = self.destination.type_name("timestamp")
        bool_type = self.destination.type_name("bool")

        partition = ""
        if self.config.partition_column:
            partition = self.destination.partition_ddl(self.config.partition_column)

        cluster = ""
        if self.config.cluster_columns:
            cluster = self.destination.cluster_ddl(self.config.cluster_columns)

        return f"""
            {create_stmt} {tgt}
            {partition}
            {cluster}
            AS
            SELECT
                {", ".join(all_data_cols)},
                CAST(NULL AS {ts_type}) AS _dlt_valid_from,
                CAST(NULL AS {ts_type}) AS _dlt_valid_to,
                CAST(NULL AS {bool_type}) AS _dlt_is_deleted
            FROM {src}
            WHERE FALSE
        """

    def build_full_reprocess_sql(self, value_columns: List[str]) -> str:
        """Build SQL for full reprocess mode.

        Processes ALL raw data:
        1. Hash + dedup all snapshots
        2. Detect all changes across full history
        3. Detect all gaps and deletions
        4. INSERT into historized table (table recreated beforehand)
        """
        pk_cols = self._pk_cols_sql()
        snapshot_col = self.config.snapshot_column
        hash_columns = self._get_hash_columns(value_columns)
        hash_expr = self.destination.hash_expression(hash_columns)
        all_output_cols = list(self.primary_key) + list(value_columns)
        output_cols_from_c = ", ".join(f"c.{col}" for col in all_output_cols)
        src = self.source_table_id
        tgt = self.target_table_id

        track_deletions_sql = ""
        deletion_union = ""
        if self.config.track_deletions:
            track_deletions_sql = self._build_deletion_tracking_sql(
                pk_cols, snapshot_col, src
            )
            # Separate deletion marker rows: one row per disappearance,
            # carrying data from the last-seen snapshot.
            # _dlt_valid_to is set to the reappearance date (from changes CTE)
            # or NULL if the key never returns.
            output_cols_from_del = ", ".join(
                f"del_src.{col}" for col in all_output_cols
            )
            pk_join_del = self._pk_join("d", "del_src")
            pk_join_del_c = self._pk_join("d", "reappear")
            deletion_union = f"""UNION ALL
SELECT
  {output_cols_from_del},
  d.disappeared_at AS _dlt_valid_from,
  (SELECT MIN(reappear.{snapshot_col}) FROM changes reappear
   WHERE {pk_join_del_c}
     AND reappear.{snapshot_col} >= d.disappeared_at
  ) AS _dlt_valid_to,
  TRUE AS _dlt_is_deleted
FROM disappearances d
INNER JOIN deduped del_src ON {pk_join_del}
  AND del_src.{snapshot_col} = d.last_seen"""

        return f"""
-- Full Reprocess Historization
-- Transforms all raw snapshot data into SCD2 format

CREATE TEMP TABLE _historize_result AS
WITH
-- All unique snapshot dates
all_snapshots AS (
  SELECT DISTINCT {snapshot_col} AS snapshot_date
  FROM {src}
),

-- Hash value columns for change detection
hashed AS (
  SELECT *,
    {hash_expr} AS _row_hash
  FROM {src}
),

-- Deduplicate within each snapshot (one row per PK per snapshot)
deduped AS (
  SELECT * FROM (
    SELECT h.*,
      ROW_NUMBER() OVER (
        PARTITION BY {pk_cols}, {snapshot_col}
        ORDER BY _row_hash
      ) AS _dedup_rn
    FROM hashed h
  )
  WHERE _dedup_rn = 1
),

-- Compare to previous snapshot + detect gaps
with_context AS (
  SELECT d.*,
    LAG(_row_hash) OVER (pk_order) AS _prev_hash,
    LAG({snapshot_col}) OVER (pk_order) AS _prev_snapshot,
    (SELECT MAX(snapshot_date) FROM all_snapshots
     WHERE snapshot_date < d.{snapshot_col}) AS _expected_prev_snapshot
  FROM deduped d
  WINDOW pk_order AS (PARTITION BY {pk_cols} ORDER BY {snapshot_col})
),

-- Keep rows where: first appearance, value changed, OR reappeared after gap
changes AS (
  SELECT * FROM with_context
  WHERE _prev_hash IS NULL
     OR _row_hash != _prev_hash
     OR _prev_snapshot < _expected_prev_snapshot
),

{track_deletions_sql}

-- Pre-compute next change snapshot per PK
changes_with_next AS (
  SELECT c.*,
    LEAD(c.{snapshot_col}) OVER (
      PARTITION BY {pk_cols} ORDER BY c.{snapshot_col}
    ) AS _next_change_snapshot
  FROM changes c
),

-- Build SCD2 with valid_to (deletion flag always FALSE for change rows)
scd2 AS (
  SELECT
    {output_cols_from_c},
    c.{snapshot_col} AS _dlt_valid_from,
    {self._build_scd2_columns_sql(pk_cols, snapshot_col)}
  FROM changes_with_next c
)
SELECT * FROM scd2
{deletion_union};

-- Insert into historized table
INSERT INTO {tgt} SELECT * FROM _historize_result;
"""

    def _build_deletion_tracking_sql(
        self, pk_cols: str, snapshot_col: str, src: str
    ) -> str:
        """Build the CTE for tracking deletions (keys disappearing from snapshots)."""
        return f"""
-- Track key presence across snapshots for deletion detection
key_presence AS (
  SELECT DISTINCT {pk_cols}, {snapshot_col} AS snapshot_date
  FROM {src}
),
with_next_presence AS (
  SELECT kp.*,
    LEAD(snapshot_date) OVER (PARTITION BY {pk_cols} ORDER BY snapshot_date)
      AS next_key_snapshot,
    (SELECT MIN(snapshot_date) FROM all_snapshots
     WHERE snapshot_date > kp.snapshot_date) AS next_overall_snapshot
  FROM key_presence kp
),
disappearances AS (
  SELECT {pk_cols}, snapshot_date AS last_seen,
    next_overall_snapshot AS disappeared_at
  FROM with_next_presence
  WHERE next_overall_snapshot IS NOT NULL
    AND (next_key_snapshot IS NULL OR next_key_snapshot > next_overall_snapshot)
),
"""

    def _build_scd2_columns_sql(self, pk_cols: str, snapshot_col: str) -> str:
        """Build the _dlt_valid_to and _dlt_is_deleted expressions.

        Closed rows always have _dlt_is_deleted=FALSE. Deletions are tracked
        via separate deletion marker rows (added in the outer query).
        """
        if self.config.track_deletions:
            pk_join_d_c = self._pk_join("d", "c")
            return f"""COALESCE(
      (SELECT MIN(d.disappeared_at) FROM disappearances d
       WHERE {pk_join_d_c}
         AND d.last_seen >= c.{snapshot_col}
         AND d.disappeared_at <= COALESCE(c._next_change_snapshot, TIMESTAMP '9999-12-31')
      ),
      c._next_change_snapshot
    ) AS _dlt_valid_to,
    FALSE AS _dlt_is_deleted"""
        else:
            return """c._next_change_snapshot AS _dlt_valid_to,
    FALSE AS _dlt_is_deleted"""

    def build_incremental_sql(
        self,
        value_columns: List[str],
        new_snapshots: List[str],
        last_historized_snapshot: Optional[str] = None,
    ) -> str:
        """Build SQL for incremental historization.

        Processes only new snapshots:
        1. Select new snapshot data + reference snapshot (last historized)
        2. Hash and dedup
        3. Detect changes via LAG() comparison (reference provides baseline)
        4. Detect deletions
        5. MERGE existing active records, INSERT new changes
        """
        pk_cols = self._pk_cols_sql()
        snapshot_col = self.config.snapshot_column
        hash_columns = self._get_hash_columns(value_columns)
        hash_expr = self.destination.hash_expression(hash_columns)
        all_output_cols = list(self.primary_key) + list(value_columns)
        output_cols_from_c = ", ".join(f"c.{col}" for col in all_output_cols)
        src = self.source_table_id
        tgt = self.target_table_id

        # Build snapshot filter (cast values to TIMESTAMP, not the column,
        # so partition/cluster pruning can work on the source table)
        snapshot_values = ", ".join(f"TIMESTAMP '{s}'" for s in new_snapshots)
        if last_historized_snapshot:
            snapshot_filter = f"{snapshot_col} IN (TIMESTAMP '{last_historized_snapshot}', {snapshot_values})"
            reference_filter = (
                f"AND c.{snapshot_col} != TIMESTAMP '{last_historized_snapshot}'"
            )
        else:
            snapshot_filter = f"{snapshot_col} IN ({snapshot_values})"
            reference_filter = ""

        track_deletions_sql = ""
        deletion_union = ""
        if self.config.track_deletions:
            track_deletions_sql = self._build_incremental_deletion_sql(
                pk_cols, snapshot_col, reference_filter, src
            )
            deletion_union = f"""UNION ALL
  SELECT
    {", ".join(f"del_src.{col}" for col in (list(self.primary_key) + list(value_columns)))},
    del.deleted_at AS _dlt_valid_from,
    TRUE AS _dlt_is_deleted
  FROM deletions del
  INNER JOIN deduped del_src ON {self._pk_join("del", "del_src")}
    AND del_src.{snapshot_col} = del.last_seen"""

        output_cols_bare = ", ".join(all_output_cols)
        pk_join_t_n = self._pk_join("t", "n")

        return f"""
-- Incremental Historization
-- Processes new snapshots and merges into historized table

CREATE TEMP TABLE _historize_incremental AS
WITH
-- All snapshots being processed (including reference)
all_snapshots AS (
  SELECT DISTINCT {snapshot_col} AS snapshot_date
  FROM {src}
  WHERE {snapshot_filter}
),

-- Hash value columns
hashed AS (
  SELECT *,
    {hash_expr} AS _row_hash
  FROM {src}
  WHERE {snapshot_filter}
),

-- Dedup within each snapshot
deduped AS (
  SELECT * FROM (
    SELECT h.*,
      ROW_NUMBER() OVER (
        PARTITION BY {pk_cols}, {snapshot_col}
        ORDER BY _row_hash
      ) AS _dedup_rn
    FROM hashed h
  )
  WHERE _dedup_rn = 1
),

-- Compare to previous via LAG
with_context AS (
  SELECT d.*,
    LAG(_row_hash) OVER (pk_order) AS _prev_hash,
    LAG({snapshot_col}) OVER (pk_order) AS _prev_snapshot,
    (SELECT MAX(snapshot_date) FROM all_snapshots
     WHERE snapshot_date < d.{snapshot_col}) AS _expected_prev_snapshot
  FROM deduped d
  WINDOW pk_order AS (PARTITION BY {pk_cols} ORDER BY {snapshot_col})
),

-- Keep changed/new rows, exclude reference snapshot from output
changes AS (
  SELECT * FROM with_context c
  WHERE (_prev_hash IS NULL
     OR _row_hash != _prev_hash
     OR _prev_snapshot < _expected_prev_snapshot)
    {reference_filter}
),

{track_deletions_sql}

-- Collect new change rows + deletion markers (without valid_to yet)
new_records_raw AS (
  SELECT
    {output_cols_from_c},
    c.{snapshot_col} AS _dlt_valid_from,
    FALSE AS _dlt_is_deleted
  FROM changes c
  {deletion_union}
),

-- Sequence within the batch: LEAD computes valid_to from the next row
-- per PK. Handles multi-snapshot batches where a key can disappear
-- and reappear, or change multiple times.
new_records AS (
  SELECT
    {output_cols_bare},
    _dlt_valid_from,
    LEAD(_dlt_valid_from) OVER (
      PARTITION BY {pk_cols} ORDER BY _dlt_valid_from
    ) AS _dlt_valid_to,
    _dlt_is_deleted
  FROM new_records_raw
)
SELECT * FROM new_records;

-- Merge: close existing open records using the earliest new event per PK.
-- MIN ensures at most one USING row per PK, avoiding duplicate-match errors
-- when a batch contains both a deletion and a reappearance for the same key.
MERGE INTO {tgt} t
USING (
  SELECT {pk_cols}, MIN(_dlt_valid_from) AS _dlt_valid_from
  FROM _historize_incremental
  GROUP BY {pk_cols}
) n
ON {pk_join_t_n} AND t._dlt_valid_to IS NULL
  AND n._dlt_valid_from > t._dlt_valid_from
WHEN MATCHED THEN
  UPDATE SET _dlt_valid_to = n._dlt_valid_from;

-- Insert all new records (changes + deletion markers)
INSERT INTO {tgt}
SELECT * FROM _historize_incremental;

"""

    def _build_incremental_deletion_sql(
        self,
        pk_cols: str,
        snapshot_col: str,
        reference_filter: str,
        src: str,
    ) -> str:
        """Build deletion detection CTEs for incremental mode."""
        return f"""
-- Detect deletions: keys present in reference but missing in new snapshots
deletion_candidates AS (
  SELECT DISTINCT {pk_cols}, {snapshot_col} AS snapshot_date
  FROM {src}
  WHERE {self._snapshot_filter_for_deletions(snapshot_col)}
),
with_next_key AS (
  SELECT dc.*,
    LEAD(snapshot_date) OVER (PARTITION BY {pk_cols} ORDER BY snapshot_date) AS next_key_snapshot,
    (SELECT MIN(snapshot_date) FROM all_snapshots
     WHERE snapshot_date > dc.snapshot_date) AS next_overall_snapshot
  FROM deletion_candidates dc
),
deletions AS (
  SELECT {pk_cols}, snapshot_date AS last_seen, next_overall_snapshot AS deleted_at
  FROM with_next_key
  WHERE next_overall_snapshot IS NOT NULL
    AND (next_key_snapshot IS NULL OR next_key_snapshot > next_overall_snapshot)
),
"""

    def _snapshot_filter_for_deletions(self, snapshot_col: str) -> str:
        """Build the WHERE clause for deletion candidate detection."""
        return f"{snapshot_col} IN (SELECT snapshot_date FROM all_snapshots)"
