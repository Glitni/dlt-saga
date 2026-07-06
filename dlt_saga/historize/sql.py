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

from dlt_saga.destinations.base import MaterializationHints
from dlt_saga.historize.config import HistorizeConfig
from dlt_saga.utility.filters import and_filter, filter_where_clause

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
        target_table_name: str = "",
        target_schema: str = "",
        filter_sql: Optional[str] = None,
    ):
        self.config = config
        self.destination = destination
        self.source_table_id = source_table_id
        self.target_table_id = target_table_id
        self.primary_key = primary_key
        self.source_database = source_database
        self.source_schema = source_schema
        self.source_table = source_table
        self.target_table_name = target_table_name
        self.target_schema = target_schema
        # SCD2 output column names (configurable; validated as SQL identifiers in HistorizeConfig).
        self.valid_from = config.valid_from_column
        self.valid_to = config.valid_to_column
        self.is_deleted = config.is_deleted_column
        # Exclude both the default _dlt_* system columns AND the configured SCD2 names
        # from value-column discovery, so a source that happens to contain a literal
        # valid_from/valid_to/is_deleted column doesn't get propagated into the output.
        self._output_exclude = SYSTEM_COLUMNS | {
            config.snapshot_column,
            self.valid_from,
            self.valid_to,
            self.is_deleted,
        }
        # Pre-rendered source-side WHERE body shared with the runner; ``None`` when
        # no historize.filters: block is configured.  Spliced at each source-read
        # site via the module-level ``filter_where_clause`` / ``and_filter`` helpers.
        self.filter_sql: Optional[str] = filter_sql or None

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
        """Quote a column or table identifier using the destination's quoting style."""
        return self.destination.quote_identifier(name)

    def _qcols(self, cols: List[str]) -> str:
        """Comma-separated, destination-quoted column list."""
        return ", ".join(self._q(c) for c in cols)

    def _pk_cols_sql(self) -> str:
        """Comma-separated primary key column list, destination-quoted."""
        return self._qcols(self.primary_key)

    def _all_columns_sql(self, value_columns: List[str]) -> str:
        """Explicit, name-aligned column list for the historized-target INSERTs:
        primary key + value columns + the SCD2 system columns, destination-quoted.

        Order matches the ``_historize_result`` / ``_historize_incremental`` temp
        tables. Listing columns by name (rather than a positional ``SELECT *``)
        keeps the insert correct when a later run rediscovers value columns in a
        different order than the target table was created with.
        """
        cols = list(self.primary_key) + list(value_columns)
        return (
            f"{self._qcols(cols)}, "
            f"{self._q(self.valid_from)}, {self._q(self.valid_to)}, "
            f"{self._q(self.is_deleted)}"
        )

    def _pk_join(self, left: str, right: str) -> str:
        """JOIN condition for primary keys between two aliases, destination-quoted."""
        return " AND ".join(
            f"{left}.{self._q(pk)} = {right}.{self._q(pk)}" for pk in self.primary_key
        )

    @property
    def _merge_key(self) -> List[str]:
        """Merge key columns (empty when not configured)."""
        return list(self.config.merge_key or [])

    def _merge_key_cols_raw_with_comma(self) -> str:
        """Quoted merge_key columns + trailing comma (or ``''``).

        Used in the ``all_snapshots`` projection to keep original column names
        — so that downstream ``snapshot_sequence`` can both reference them in
        ``PARTITION BY`` and alias them on the way out.
        """
        if not self._merge_key:
            return ""
        return self._qcols(self._merge_key) + ", "

    def _merge_key_cols_aliased_with_comma(self) -> str:
        """``<col> AS seq_<col>, ...`` projection prefix for snapshot_sequence, or ``''``.

        Aliasing with a ``seq_`` prefix keeps merge_key columns distinct from the
        same-named columns in downstream JOINs (otherwise unqualified references
        in ``WINDOW`` clauses become ambiguous when ``merge_key ⊆ primary_key``).
        """
        if not self._merge_key:
            return ""
        return (
            ", ".join(f"{self._q(c)} AS {self._q(f'seq_{c}')}" for c in self._merge_key)
            + ", "
        )

    def _merge_key_partition_clause(self) -> str:
        """``PARTITION BY <merge_key>`` clause (uses original column names — used
        inside ``snapshot_sequence`` where the source is ``all_snapshots``)."""
        if not self._merge_key:
            return ""
        return f"PARTITION BY {self._qcols(self._merge_key)} "

    def _merge_key_join_extra(self, ss_alias: str, other_alias: str) -> str:
        """Extra ``AND`` predicates joining ``snapshot_sequence`` (left) to another
        alias on the seq-prefixed merge_key columns."""
        if not self._merge_key:
            return ""
        preds = " AND ".join(
            f"{ss_alias}.{self._q(f'seq_{c}')} = {other_alias}.{self._q(c)}"
            for c in self._merge_key
        )
        return f" AND {preds}"

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
        q_snapshot = self._q(self.config.snapshot_column)
        safe_date = self.destination.escape_string_literal(effective_from_date)

        keys_table = self.destination.get_full_table_id(
            dataset, f"_rehistorize_keys_{run_id}"
        )

        # PK membership check via IN subquery (no table aliases needed)
        if len(self.primary_key) == 1:
            q_pk = self._q(self.primary_key[0])
            pk_in_keys = f"{q_pk} IN (SELECT {q_pk} FROM {keys_table})"
        else:
            pk_in_keys = f"({pk_cols}) IN (SELECT {pk_cols} FROM {keys_table})"

        create_keys = f"""
            CREATE TABLE {keys_table} AS
            SELECT DISTINCT {pk_cols}
            FROM {src}
            WHERE {and_filter(self.filter_sql, f"{q_snapshot} >= TIMESTAMP '{safe_date}'")}
        """

        delete_sql = f"""
            DELETE FROM {tgt}
            WHERE {self.valid_from} >= TIMESTAMP '{safe_date}'
              AND {pk_in_keys}
        """

        update_sql = f"""
            UPDATE {tgt}
            SET {self.valid_to} = NULL
            WHERE {self.valid_from} < TIMESTAMP '{safe_date}'
              AND {self.valid_to} >= TIMESTAMP '{safe_date}'
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
        """SQL to create the historized target table.

        Delegates to destination.build_historize_create_table_sql so each
        destination can emit format-specific DDL (Iceberg OPTIONS, USING DELTA, etc.).
        """
        all_data_cols = list(self.primary_key) + list(value_columns)
        create_stmt = (
            "CREATE OR REPLACE TABLE" if replace else "CREATE TABLE IF NOT EXISTS"
        )
        src = self.source_table_id
        tgt = self.target_table_id
        ts_type = self.destination.type_name("timestamp")
        bool_type = self.destination.type_name("bool")

        select_body = (
            f"SELECT\n"
            f"    {self._qcols(all_data_cols)},\n"
            f"    CAST(NULL AS {ts_type}) AS {self.valid_from},\n"
            f"    CAST(NULL AS {ts_type}) AS {self.valid_to},\n"
            f"    CAST(NULL AS {bool_type}) AS {self.is_deleted}\n"
            f"FROM {src}\n"
            f"WHERE FALSE"
        )

        hints = MaterializationHints(
            partition_column=self.config.partition_column,
            cluster_columns=self.config.cluster_columns,
            table_format=self.config.table_format or "native",
            table_name=self.target_table_name,
            schema=self.target_schema,
            source_database=self.source_database,
            source_schema=self.source_schema,
            source_table=self.source_table,
            valid_from_column=self.valid_from,
            valid_to_column=self.valid_to,
            is_deleted_column=self.is_deleted,
        )

        return self.destination.build_historize_create_table_sql(
            create_clause=create_stmt,
            target_table_id=tgt,
            select_body=select_body,
            hints=hints,
        )

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
        q_snapshot = self._q(snapshot_col)
        hash_columns = self._get_hash_columns(value_columns)
        hash_expr = self.destination.hash_expression(hash_columns)
        all_output_cols = list(self.primary_key) + list(value_columns)
        output_cols_from_c = ", ".join(f"c.{self._q(col)}" for col in all_output_cols)
        all_columns_sql = self._all_columns_sql(value_columns)
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
                f"del_src.{self._q(col)}" for col in all_output_cols
            )
            pk_join_del = self._pk_join("d", "del_src")
            pk_join_del_c = self._pk_join("d", "reappear")
            deletion_union = f"""UNION ALL
SELECT
  {output_cols_from_del},
  d.disappeared_at AS {self.valid_from},
  (SELECT MIN(reappear.{q_snapshot}) FROM changes reappear
   WHERE {pk_join_del_c}
     AND reappear.{q_snapshot} >= d.disappeared_at
  ) AS {self.valid_to},
  TRUE AS {self.is_deleted}
FROM disappearances d
INNER JOIN deduped del_src ON {pk_join_del}
  AND del_src.{q_snapshot} = d.last_seen"""

        return f"""
-- Full Reprocess Historization
-- Transforms all raw snapshot data into SCD2 format

CREATE TEMP TABLE _historize_result AS
WITH
-- All unique snapshot dates (per merge_key group when configured).
all_snapshots AS (
  SELECT DISTINCT {self._merge_key_cols_raw_with_comma()}{q_snapshot} AS snapshot_date
  FROM {src}{filter_where_clause(self.filter_sql)}
),

-- Previous/next snapshot per distinct snapshot. Precomputed here so downstream
-- CTEs can JOIN instead of using a correlated scalar subquery in the same clause
-- as a window function (unsupported on Spark/Databricks).
-- When merge_key is set, the window is partitioned by it so a missed delivery
-- in one group doesn't introduce a "next overall snapshot" for sibling groups.
snapshot_sequence AS (
  SELECT {self._merge_key_cols_aliased_with_comma()}snapshot_date AS seq_snapshot_date,
    LAG(snapshot_date) OVER ({self._merge_key_partition_clause()}ORDER BY snapshot_date) AS prev_snapshot_date,
    LEAD(snapshot_date) OVER ({self._merge_key_partition_clause()}ORDER BY snapshot_date) AS next_snapshot_date
  FROM all_snapshots
),

-- Hash value columns for change detection
hashed AS (
  SELECT *,
    {hash_expr} AS _row_hash
  FROM {src}{filter_where_clause(self.filter_sql)}
),

-- Deduplicate within each snapshot (one row per PK per snapshot)
deduped AS (
  SELECT * FROM (
    SELECT h.*,
      ROW_NUMBER() OVER (
        PARTITION BY {pk_cols}, {q_snapshot}
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
    LAG({q_snapshot}) OVER (pk_order) AS _prev_snapshot,
    ss.prev_snapshot_date AS _expected_prev_snapshot
  FROM deduped d
  LEFT JOIN snapshot_sequence ss ON ss.seq_snapshot_date = d.{q_snapshot}{self._merge_key_join_extra("ss", "d")}
  WINDOW pk_order AS (PARTITION BY {pk_cols} ORDER BY {q_snapshot})
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
    LEAD(c.{q_snapshot}) OVER (
      PARTITION BY {pk_cols} ORDER BY c.{q_snapshot}
    ) AS _next_change_snapshot
  FROM changes c
),

-- Build SCD2 with valid_to (deletion flag always FALSE for change rows)
scd2 AS (
  SELECT
    {output_cols_from_c},
    c.{q_snapshot} AS {self.valid_from},
    {self._build_scd2_columns_sql(pk_cols, snapshot_col)}
  FROM changes_with_next c
)
SELECT * FROM scd2
{deletion_union};

-- Insert into historized table (explicit column list — name-aligned).
INSERT INTO {tgt} ({all_columns_sql})
SELECT {all_columns_sql} FROM _historize_result;
"""

    def _build_deletion_tracking_sql(
        self, pk_cols: str, snapshot_col: str, src: str
    ) -> str:
        """Build the CTE for tracking deletions (keys disappearing from snapshots)."""
        q_snapshot = self._q(snapshot_col)
        return f"""
-- Track key presence across snapshots for deletion detection
key_presence AS (
  SELECT DISTINCT {pk_cols}, {q_snapshot} AS snapshot_date
  FROM {src}{filter_where_clause(self.filter_sql)}
),
with_next_presence AS (
  SELECT kp.*,
    LEAD(snapshot_date) OVER (PARTITION BY {pk_cols} ORDER BY snapshot_date)
      AS next_key_snapshot,
    ss.next_snapshot_date AS next_overall_snapshot
  FROM key_presence kp
  LEFT JOIN snapshot_sequence ss ON ss.seq_snapshot_date = kp.snapshot_date{self._merge_key_join_extra("ss", "kp")}
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
        """Build the valid-to and is-deleted expressions.

        Closed rows always have is_deleted=FALSE. Deletions are tracked
        via separate deletion marker rows (added in the outer query).
        """
        q_snapshot = self._q(snapshot_col)
        if self.config.track_deletions:
            pk_join_d_c = self._pk_join("d", "c")
            return f"""COALESCE(
      (SELECT MIN(d.disappeared_at) FROM disappearances d
       WHERE {pk_join_d_c}
         AND d.last_seen >= c.{q_snapshot}
         AND d.disappeared_at <= COALESCE(c._next_change_snapshot, TIMESTAMP '9999-12-31')
      ),
      c._next_change_snapshot
    ) AS {self.valid_to},
    FALSE AS {self.is_deleted}"""
        else:
            return f"""c._next_change_snapshot AS {self.valid_to},
    FALSE AS {self.is_deleted}"""

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
        q_snapshot = self._q(snapshot_col)
        hash_columns = self._get_hash_columns(value_columns)
        hash_expr = self.destination.hash_expression(hash_columns)
        all_output_cols = list(self.primary_key) + list(value_columns)
        output_cols_from_c = ", ".join(f"c.{self._q(col)}" for col in all_output_cols)
        src = self.source_table_id
        tgt = self.target_table_id

        # New-snapshot rows are read from the source; the change-detection
        # baseline (the "reference" snapshot) is drawn from the historized
        # target's currently-open rows rather than from the source. This is what
        # makes replace-mode sources correct: a `replace` staging table only ever
        # holds the latest snapshot, so the reference snapshot is absent from it
        # and every key would otherwise appear new (re-versioning unchanged rows
        # on every run). For append sources the target's open rows carry the same
        # values as the reference snapshot, so the result is unchanged. Values are
        # cast to TIMESTAMP (not the column) to preserve source-side pruning.
        snapshot_values = ", ".join(f"TIMESTAMP '{s}'" for s in new_snapshots)
        new_snapshot_filter = f"{q_snapshot} IN ({snapshot_values})"
        if last_historized_snapshot:
            reference_filter = (
                f"AND c.{q_snapshot} != TIMESTAMP '{last_historized_snapshot}'"
            )
            baseline_union = (
                "\n  UNION ALL\n"
                f"  SELECT {self._qcols(all_output_cols)}, "
                f"TIMESTAMP '{last_historized_snapshot}' AS {q_snapshot}\n"
                f"  FROM {tgt}\n"
                f"  WHERE {self.valid_to} IS NULL AND NOT {self.is_deleted}"
            )
        else:
            reference_filter = ""
            baseline_union = ""

        track_deletions_sql = ""
        deletion_union = ""
        if self.config.track_deletions:
            track_deletions_sql = self._build_incremental_deletion_sql(
                pk_cols, snapshot_col
            )
            deletion_union = f"""UNION ALL
  SELECT
    {", ".join(f"del_src.{self._q(col)}" for col in (list(self.primary_key) + list(value_columns)))},
    del.deleted_at AS {self.valid_from},
    TRUE AS {self.is_deleted}
  FROM deletions del
  INNER JOIN deduped del_src ON {self._pk_join("del", "del_src")}
    AND del_src.{q_snapshot} = del.last_seen"""

        output_cols_bare = self._qcols(all_output_cols)
        all_columns_sql = self._all_columns_sql(value_columns)
        pk_join_t_n = self._pk_join("t", "n")

        return f"""
-- Incremental Historization
-- Processes new snapshots and merges into historized table

CREATE TEMP TABLE _historize_incremental AS
WITH
-- New-snapshot rows from the source unioned with the change-detection baseline
-- (the reference snapshot) taken from the historized target's open rows. The
-- baseline is projected to the same columns and stamped at the reference
-- snapshot value, so downstream CTEs treat it as just another snapshot. Reading
-- the baseline from the target (not the source) is what keeps replace-mode
-- sources from re-versioning every unchanged row.
source_rows AS (
  SELECT {self._qcols(all_output_cols)}, {q_snapshot}
  FROM {src}
  WHERE {and_filter(self.filter_sql, new_snapshot_filter)}{baseline_union}
),

-- All snapshots being processed (including reference, per merge_key group when configured)
all_snapshots AS (
  SELECT DISTINCT {self._merge_key_cols_raw_with_comma()}{q_snapshot} AS snapshot_date
  FROM source_rows
),

-- Previous/next snapshot per distinct snapshot (JOINed below instead of using a
-- correlated subquery alongside window functions — unsupported on Spark/Databricks).
-- When merge_key is set the window is partitioned by it.
snapshot_sequence AS (
  SELECT {self._merge_key_cols_aliased_with_comma()}snapshot_date AS seq_snapshot_date,
    LAG(snapshot_date) OVER ({self._merge_key_partition_clause()}ORDER BY snapshot_date) AS prev_snapshot_date,
    LEAD(snapshot_date) OVER ({self._merge_key_partition_clause()}ORDER BY snapshot_date) AS next_snapshot_date
  FROM all_snapshots
),

-- Hash value columns
hashed AS (
  SELECT *,
    {hash_expr} AS _row_hash
  FROM source_rows
),

-- Dedup within each snapshot
deduped AS (
  SELECT * FROM (
    SELECT h.*,
      ROW_NUMBER() OVER (
        PARTITION BY {pk_cols}, {q_snapshot}
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
    LAG({q_snapshot}) OVER (pk_order) AS _prev_snapshot,
    ss.prev_snapshot_date AS _expected_prev_snapshot
  FROM deduped d
  LEFT JOIN snapshot_sequence ss ON ss.seq_snapshot_date = d.{q_snapshot}{self._merge_key_join_extra("ss", "d")}
  WINDOW pk_order AS (PARTITION BY {pk_cols} ORDER BY {q_snapshot})
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
    c.{q_snapshot} AS {self.valid_from},
    FALSE AS {self.is_deleted}
  FROM changes c
  {deletion_union}
),

-- Sequence within the batch: LEAD computes valid_to from the next row
-- per PK. Handles multi-snapshot batches where a key can disappear
-- and reappear, or change multiple times.
new_records AS (
  SELECT
    {output_cols_bare},
    {self.valid_from},
    LEAD({self.valid_from}) OVER (
      PARTITION BY {pk_cols} ORDER BY {self.valid_from}
    ) AS {self.valid_to},
    {self.is_deleted}
  FROM new_records_raw
)
SELECT * FROM new_records;

-- Merge: close existing open records using the earliest new event per PK.
-- MIN ensures at most one USING row per PK, avoiding duplicate-match errors
-- when a batch contains both a deletion and a reappearance for the same key.
MERGE INTO {tgt} t
USING (
  SELECT {pk_cols}, MIN({self.valid_from}) AS {self.valid_from}
  FROM _historize_incremental
  GROUP BY {pk_cols}
) n
ON {pk_join_t_n} AND t.{self.valid_to} IS NULL
  AND n.{self.valid_from} > t.{self.valid_from}
WHEN MATCHED THEN
  UPDATE SET {self.valid_to} = n.{self.valid_from};

-- Insert all new records (changes + deletion markers). Explicit column list so
-- the insert aligns by name against a target created in an earlier run.
INSERT INTO {tgt} ({all_columns_sql})
SELECT {all_columns_sql} FROM _historize_incremental;

"""

    def _build_incremental_deletion_sql(
        self,
        pk_cols: str,
        snapshot_col: str,
    ) -> str:
        """Build deletion detection CTEs for incremental mode.

        Reads from ``source_rows`` (new snapshots + the target-derived baseline),
        so a key present in the baseline but missing from the new snapshots is
        detected as a deletion. ``source_rows`` is already filtered and scoped to
        the snapshots being processed, so no extra filter is needed here.
        """
        q_snapshot = self._q(snapshot_col)
        return f"""
-- Detect deletions: keys present in reference but missing in new snapshots
deletion_candidates AS (
  SELECT DISTINCT {pk_cols}, {q_snapshot} AS snapshot_date
  FROM source_rows
),
with_next_key AS (
  SELECT dc.*,
    LEAD(snapshot_date) OVER (PARTITION BY {pk_cols} ORDER BY snapshot_date) AS next_key_snapshot,
    ss.next_snapshot_date AS next_overall_snapshot
  FROM deletion_candidates dc
  LEFT JOIN snapshot_sequence ss ON ss.seq_snapshot_date = dc.snapshot_date{self._merge_key_join_extra("ss", "dc")}
),
deletions AS (
  SELECT {pk_cols}, snapshot_date AS last_seen, next_overall_snapshot AS deleted_at
  FROM with_next_key
  WHERE next_overall_snapshot IS NOT NULL
    AND (next_key_snapshot IS NULL OR next_key_snapshot > next_overall_snapshot)
),
"""
