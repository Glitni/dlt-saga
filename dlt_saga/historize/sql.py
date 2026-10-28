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
from dlt_saga.utility.system_columns import HISTORIZE_SYSTEM_COLUMNS

logger = logging.getLogger(__name__)


# Internal alias a pseudo-column snapshot value is projected under; see
# HistorizeSqlBuilder._snap. Prefixed so it can't collide with a source column.
SNAPSHOT_ALIAS = "_saga_snapshot"

# Stamp for "present before this batch" in scd1 deletion detection: earlier than
# any real snapshot, never written to the target.
SCD1_BASELINE_SNAPSHOT = "0001-01-01 00:00:00"
SCD1_FINAL_DELETION_RANK = "9999-12-31 23:59:59"


def snapshot_range_filter(destination: Any, column: str, snapshots: List[str]) -> str:
    """Render a ``column >= first AND column <= last`` bound for a snapshot batch.

    Every site that scopes work to a discovered batch bounds it by range rather
    than by an inlined list of each value. The two select the same rows:
    discovery reads *all* distinct snapshot values its filter admits between the
    batch's bounds, so a value inside the range but absent from the list is
    absent from the data too. The range is a fixed handful of characters where
    the list grows with the batch — a snapshot column stamped per row (rather
    than per run) accumulates tens of thousands of distinct values in a backlog,
    which pushes the statement past the destination's query-length limit.

    The upper bound also pins the batch against rows landing mid-run: a snapshot
    arriving above ``last`` stays for the next run, matching the explicit
    ``snapshot_upper_bound`` the full-reprocess path takes.

    Args:
        destination: Destination supplying literal escaping.
        column: Already-quoted column reference to bound.
        snapshots: Chronologically ordered snapshot values; must be non-empty.
    """
    if not snapshots:
        raise ValueError(
            "Cannot bound an empty snapshot batch — callers skip the run when "
            "discovery finds no snapshots"
        )
    lower = destination.escape_string_literal(snapshots[0])
    upper = destination.escape_string_literal(snapshots[-1])
    return f"{column} >= TIMESTAMP '{lower}' AND {column} <= TIMESTAMP '{upper}'"


def null_pk_alias(index: int) -> str:
    """Result alias for the NULL-check flag of primary-key column ``index``.

    Positional and indexed (like the ``_rk_`` correlated-subquery aliases below)
    so no config-supplied identifier has to survive a round-trip through
    result-set metadata, where destinations may normalize case or reject a name
    that is legal as a column but not as an alias.

    One definition on purpose: both the full-refresh probe (:mod:`sql`) and the
    incremental discovery query (:mod:`state`) emit these flags, and readers map
    them back with ``getattr(row, alias, 0)`` — a divergence would silently read
    as "no NULL keys found" rather than failing.
    """
    return f"_null_pk_{index}"


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
        self._output_exclude = HISTORIZE_SYSTEM_COLUMNS | {
            config.snapshot_column,
            self.valid_from,
            self.valid_to,
            self.is_deleted,
        }
        # A snapshot column may be a destination pseudo-column: on an
        # ingestion-time partitioned BigQuery table, _PARTITIONTIME is often the
        # only timestamp the source has. Pseudo-columns resolve on a direct scan
        # of their own table but survive neither SELECT * nor a projection, so
        # the generated CTEs carry the value under an alias instead: _snap_src
        # names it where the source itself is read, _snap everywhere downstream.
        # For an ordinary column the two are identical and the SQL is unchanged.
        self._snapshot_is_pseudo = bool(
            destination.is_pseudo_column(config.snapshot_column)
        )
        self._snap_src = self._q(config.snapshot_column)
        self._snap = (
            self._q(SNAPSHOT_ALIAS) if self._snapshot_is_pseudo else self._snap_src
        )

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

    def _snapshot_projection(self) -> str:
        """Snapshot value as projected by a CTE that reads the source directly."""
        if self._snapshot_is_pseudo:
            return f"{self._snap_src} AS {self._snap}"
        return self._snap_src

    def _snapshot_star_projection(self) -> str:
        """Extra projection carrying the snapshot value through a ``SELECT *``.

        ``SELECT *`` doesn't return pseudo-columns, so a source read that relies
        on it has to name the value explicitly. Empty for an ordinary column,
        which ``*`` already carries.
        """
        if self._snapshot_is_pseudo:
            return f",\n    {self._snap_src} AS {self._snap}"
        return ""

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

    def _snapshot_sequence_ctes(self, snapshot_expr: str, from_clause: str) -> str:
        """The ``all_snapshots`` + ``snapshot_sequence`` CTEs, or ``''``.

        They give each snapshot its previous/next snapshot overall, which gap
        detection and deletion detection compare a key's own appearances
        against. Both read a key's absence from a snapshot, so they exist only
        under ``delivery: full`` — with incremental deliveries absence means
        nothing, and the distinct-snapshot scan is skipped.

        Precomputed so downstream CTEs can JOIN instead of using a correlated
        scalar subquery in the same clause as a window function (unsupported
        on Spark/Databricks). When merge_key is set, the window is partitioned
        by it so a missed delivery in one group doesn't introduce a "next
        overall snapshot" for sibling groups.

        Args:
            snapshot_expr: Expression yielding each row's snapshot value.
            from_clause: What to read it from (table or CTE, plus any WHERE).
        """
        if not self.config.is_full_delivery:
            return ""
        return f"""-- All unique snapshot dates (per merge_key group when configured).
all_snapshots AS (
  SELECT DISTINCT {self._merge_key_cols_raw_with_comma()}{snapshot_expr} AS snapshot_date
  FROM {from_clause}
),

-- Previous/next snapshot per distinct snapshot.
snapshot_sequence AS (
  SELECT {self._merge_key_cols_aliased_with_comma()}snapshot_date AS seq_snapshot_date,
    LAG(snapshot_date) OVER ({self._merge_key_partition_clause()}ORDER BY snapshot_date) AS prev_snapshot_date,
    LEAD(snapshot_date) OVER ({self._merge_key_partition_clause()}ORDER BY snapshot_date) AS next_snapshot_date
  FROM all_snapshots
),

"""

    def _gap_detection_sql(self) -> tuple:
        """``(projection, join, condition)`` for gap-driven reappearance, or blanks.

        Under ``delivery: full``, a key whose previous appearance is older than
        the previous snapshot overall was absent in between, so it opens a new
        version on its return even with unchanged values. Under ``delivery:
        incremental`` that absence means nothing and only a value change opens
        a version.
        """
        if not self.config.is_full_delivery:
            return "", "", ""
        q_snapshot = self._snap
        projection = (
            f",\n    LAG({q_snapshot}) OVER (pk_order) AS _prev_snapshot,\n"
            f"    ss.prev_snapshot_date AS _expected_prev_snapshot"
        )
        join = (
            f"\n  LEFT JOIN snapshot_sequence ss ON ss.seq_snapshot_date = "
            f"d.{q_snapshot}{self._merge_key_join_extra('ss', 'd')}"
        )
        condition = "\n     OR _prev_snapshot < _expected_prev_snapshot"
        return projection, join, condition

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
        tgt = self.target_table_id
        src = self.source_table_id
        q_snapshot = self._snap_src
        safe_date = self.destination.escape_string_literal(effective_from_date)

        keys_table = self.destination.get_full_table_id(
            dataset, f"_rehistorize_keys_{run_id}"
        )

        # PK membership check against the affected-keys table.
        if len(self.primary_key) == 1:
            # Single column: portable IN (subquery) on every dialect.
            q_pk = self._q(self.primary_key[0])
            keys_select = f"SELECT DISTINCT {q_pk}"
            pk_in_keys = f"{q_pk} IN (SELECT {q_pk} FROM {keys_table})"
        else:
            # Multi-column ``(a, b) IN (subquery)`` is rejected by BigQuery, so
            # use a correlated EXISTS instead. The keys columns are aliased with
            # a ``_rk_`` prefix so the unqualified PK references in the correlated
            # predicate bind to the outer (DELETE/UPDATE target) table — avoiding
            # a target alias, which isn't portable across destination dialects.
            key_aliases = [f"_rk_{i}" for i in range(len(self.primary_key))]
            keys_select = "SELECT DISTINCT " + ", ".join(
                f"{self._q(pk)} AS {self._q(alias)}"
                for pk, alias in zip(self.primary_key, key_aliases)
            )
            correlated = " AND ".join(
                f"k.{self._q(alias)} = {self._q(pk)}"
                for pk, alias in zip(self.primary_key, key_aliases)
            )
            pk_in_keys = f"EXISTS (SELECT 1 FROM {keys_table} AS k WHERE {correlated})"

        create_keys = f"""
            CREATE TABLE {keys_table} AS
            {keys_select}
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

    def select_value_columns(self, source_columns: List[str]) -> List[str]:
        """Return the source columns carried into the historized target.

        Drops the primary key (emitted separately, ahead of the value columns)
        and the framework's system columns. Source order is preserved.

        Takes the column list rather than querying for it: the runner reads the
        source's columns once per run and every consumer filters that one
        result, so a column can't be visible to one check and not another.
        """
        exclude = self._output_exclude | set(self.primary_key)
        return [col for col in source_columns if col not in exclude]

    @property
    def _has_deleted_column(self) -> bool:
        """Whether the target carries the deleted flag (always for scd2)."""
        return not self.config.is_scd1 or bool(self.config.track_deletions)

    def build_drop_target_table_sql(self) -> str:
        """DROP TABLE IF EXISTS for the historized target table."""
        return f"DROP TABLE IF EXISTS {self.target_table_id}"

    def build_create_target_table_sql(
        self,
        value_columns: List[str],
        replace: bool = False,
        cluster_columns: Optional[List[str]] = None,
    ) -> str:
        """SQL to create the historized target table.

        Delegates to destination.build_historize_create_table_sql so each
        destination can emit format-specific DDL (Iceberg OPTIONS, USING DELTA, etc.).

        Args:
            value_columns: Non-key source columns carried into the target.
            replace: Emit ``CREATE OR REPLACE`` instead of ``IF NOT EXISTS``.
            cluster_columns: Clustering for the new table, already resolved by
                the runner (explicit config or the layout default); ``None``
                or empty creates an unclustered table.
        """
        all_data_cols = list(self.primary_key) + list(value_columns)
        create_stmt = (
            "CREATE OR REPLACE TABLE" if replace else "CREATE TABLE IF NOT EXISTS"
        )
        src = self.source_table_id
        tgt = self.target_table_id
        ts_type = self.destination.type_name("timestamp")
        bool_type = self.destination.type_name("bool")

        # scd1 keeps one row per key, so it has no validity end — only when
        # the current version took effect, plus a deleted flag when deletions
        # are tracked.
        system_cols = [f"CAST(NULL AS {ts_type}) AS {self.valid_from}"]
        if not self.config.is_scd1:
            system_cols.append(f"CAST(NULL AS {ts_type}) AS {self.valid_to}")
        if self._has_deleted_column:
            system_cols.append(f"CAST(NULL AS {bool_type}) AS {self.is_deleted}")
        select_list = ",\n".join(
            f"    {col}" for col in [self._qcols(all_data_cols), *system_cols]
        )
        select_body = f"SELECT\n{select_list}\nFROM {src}\nWHERE FALSE"

        hints = MaterializationHints(
            partition_column=self.config.partition_column,
            cluster_columns=list(cluster_columns) if cluster_columns else None,
            table_format=self.config.table_format or "native",
            table_name=self.target_table_name,
            schema=self.target_schema,
            source_database=self.source_database,
            source_schema=self.source_schema,
            source_table=self.source_table,
            valid_from_column=self.valid_from,
            valid_to_column=None if self.config.is_scd1 else self.valid_to,
            is_deleted_column=self.is_deleted if self._has_deleted_column else None,
        )

        return self.destination.build_historize_create_table_sql(
            create_clause=create_stmt,
            target_table_id=tgt,
            select_body=select_body,
            hints=hints,
        )

    def _source_filter_with_bound(
        self, q_snapshot: str, snapshot_upper_bound: Optional[str]
    ) -> Optional[str]:
        """Combine the historize.filters WHERE body with an optional upper bound.

        Returns a WHERE body (no leading ``WHERE``) that ANDs
        ``snapshot_column <= bound`` onto the configured filter, or just the
        filter when no bound is given.
        """
        if snapshot_upper_bound is None:
            return self.filter_sql
        bound = self.destination.escape_string_literal(snapshot_upper_bound)
        return and_filter(self.filter_sql, f"{q_snapshot} <= TIMESTAMP '{bound}'")

    def null_pk_probe_aliases(self) -> List[str]:
        """Column aliases returned by :meth:`build_null_pk_probe_sql`, in PK order."""
        return [null_pk_alias(i) for i in range(len(self.primary_key))]

    def build_null_pk_probe_sql(self) -> Optional[str]:
        """Build a one-row probe for NULL primary-key values across the source.

        SCD2 identity is the primary key, and SQL equality is never true for NULL:
        the change-detection window functions treat ``NULL = NULL`` as one group
        (so a NULL-keyed entity does produce change rows), but the MERGE that
        closes the previous open row, and the deletion-marker joins, never match
        it. Each run therefore adds another permanently-open row for the same
        NULL key. There is no NULL-safe reading of "which entity is this" to fall
        back on, so the run must refuse to start instead.

        Only the full-reprocess and partial-refresh paths need this: they read the
        whole filtered source and have no earlier scan of it to piggyback on. An
        incremental run gets the same answer for free out of
        ``discover_unprocessed_snapshots``.

        Returns ``None`` when there is no primary key to check. Otherwise a
        ``LIMIT 1`` existence probe reading only the PK columns.
        """
        if not self.primary_key:
            return None
        flags = ", ".join(
            f"CASE WHEN {self._q(pk)} IS NULL THEN 1 ELSE 0 END AS {alias}"
            for pk, alias in zip(self.primary_key, self.null_pk_probe_aliases())
        )
        any_null = " OR ".join(f"{self._q(pk)} IS NULL" for pk in self.primary_key)
        where = and_filter(self.filter_sql, f"({any_null})")
        return f"SELECT {flags} FROM {self.source_table_id} WHERE {where} LIMIT 1"

    def build_full_reprocess_sql(
        self, value_columns: List[str], snapshot_upper_bound: Optional[str] = None
    ) -> str:
        """Build SQL for full reprocess mode.

        Processes ALL raw data:
        1. Hash + dedup all snapshots
        2. Detect all changes across full history
        3. Detect all gaps and deletions
        4. INSERT into historized table (table recreated beforehand)

        Args:
            value_columns: Non-PK columns to carry into the historized table.
            snapshot_upper_bound: When set, every source read is bounded to
                ``snapshot_column <= this value``. The runner resolves the max
                snapshot once and passes it here so the recorded baseline
                exactly matches what was processed — a snapshot arriving after
                the max read isn't silently skipped (it's ``> bound``, picked
                up on the next run).
        """
        pk_cols = self._pk_cols_sql()
        snapshot_col = self.config.snapshot_column
        # Downstream CTEs read the projected value; the source-side filter is
        # applied where the source itself is scanned.
        q_snapshot = self._snap
        source_filter = self._source_filter_with_bound(
            self._snap_src, snapshot_upper_bound
        )
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
            # source_filter carries any snapshot upper bound, so the deletion
            # read stays in lockstep with the change-detection reads.
            track_deletions_sql = self._disappearance_ctes(
                self._snap_src, f"{src}{filter_where_clause(source_filter)}"
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

        gap_projection, gap_join, gap_condition = self._gap_detection_sql()

        return f"""
-- Full Reprocess Historization
-- Transforms all raw snapshot data into SCD2 format

CREATE OR REPLACE TEMP TABLE _historize_result AS
WITH
{self._snapshot_sequence_ctes(self._snap_src, f"{src}{filter_where_clause(source_filter)}")}-- Hash value columns for change detection
hashed AS (
  SELECT *,
    {hash_expr} AS _row_hash{self._snapshot_star_projection()}
  FROM {src}{filter_where_clause(source_filter)}
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

-- Compare to previous snapshot (+ detect gaps under full delivery)
with_context AS (
  SELECT d.*,
    LAG(_row_hash) OVER (pk_order) AS _prev_hash{gap_projection}
  FROM deduped d{gap_join}
  WINDOW pk_order AS (PARTITION BY {pk_cols} ORDER BY {q_snapshot})
),

-- Keep rows where: first appearance, value changed, OR reappeared after gap
changes AS (
  SELECT * FROM with_context
  WHERE _prev_hash IS NULL
     OR _row_hash != _prev_hash{gap_condition}
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

    def _disappearance_ctes(self, snapshot_expr: str, from_clause: str) -> str:
        """``key_presence`` / ``with_next_presence`` / ``disappearances`` CTEs.

        The one definition of "this key disappeared": a key whose next
        appearance skips the next snapshot of its group (overall, or per
        merge_key) disappeared at that snapshot. ``disappearances`` yields the
        primary key, ``last_seen``, ``disappeared_at`` and ``reappeared_at``
        (NULL when the key doesn't come back). Needs the
        ``snapshot_sequence`` CTE, which only exists under ``delivery: full``.

        Args:
            snapshot_expr: Expression yielding each row's snapshot value.
            from_clause: What to read key appearances from (table or CTE,
                plus any WHERE), already scoped to the rows being processed.
        """
        pk_cols = self._pk_cols_sql()
        return f"""-- Key presence across snapshots, for deletion detection
key_presence AS (
  SELECT DISTINCT {pk_cols}, {snapshot_expr} AS snapshot_date
  FROM {from_clause}
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
    next_overall_snapshot AS disappeared_at, next_key_snapshot AS reappeared_at
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
        q_snapshot = self._snap
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
        rollback_prefix: bool = False,
    ) -> str:
        """Build SQL for incremental historization.

        Processes only new snapshots:
        1. Select new snapshot data + reference snapshot (last historized)
        2. Hash and dedup
        3. Detect changes via LAG() comparison (reference provides baseline)
        4. Detect deletions
        5. MERGE existing active records, INSERT new changes

        Args:
            rollback_prefix: When True, prepend a re-entrancy rollback (DELETE +
                reopen scoped to the batch's snapshot boundary) so a crashed-then-
                retried run converges to a single clean run rather than duplicating
                history. Only the in-place incremental path passes True; the
                partial-refresh staging path leaves it False because it already
                rolls back via ``build_rollback_sql`` against a clone (adding the
                prefix there would run an unscoped rollback over unaffected keys).
        """
        pk_cols = self._pk_cols_sql()
        # source_rows projects the snapshot value once; everything downstream
        # (including the target-derived baseline union) reads that projection.
        q_snapshot = self._snap
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
        new_snapshot_filter = snapshot_range_filter(
            self.destination, self._snap_src, new_snapshots
        )
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
            # source_rows = new snapshots + the target-derived baseline, so a
            # key present in the baseline but missing from the new snapshots
            # is detected; it is already filtered and scoped.
            track_deletions_sql = self._disappearance_ctes(q_snapshot, "source_rows")
            deletion_union = f"""UNION ALL
  SELECT
    {", ".join(f"del_src.{self._q(col)}" for col in (list(self.primary_key) + list(value_columns)))},
    del.disappeared_at AS {self.valid_from},
    TRUE AS {self.is_deleted}
  FROM disappearances del
  INNER JOIN deduped del_src ON {self._pk_join("del", "del_src")}
    AND del_src.{q_snapshot} = del.last_seen"""

        output_cols_bare = self._qcols(all_output_cols)
        all_columns_sql = self._all_columns_sql(value_columns)
        pk_join_t_n = self._pk_join("t", "n")

        # Re-entrancy rollback prefix. The incremental write (CREATE TEMP TABLE →
        # MERGE → INSERT) and the subsequent log write are not one atomic unit, so a
        # crash in between (or mid-script — BigQuery scripts aren't atomic across
        # statements) leaves the watermark un-advanced; the next run re-discovers the
        # same snapshots and re-runs this SQL in place, duplicating every inserted row
        # (the MERGE's `n.valid_from > t.valid_from` is equal, not greater, for the
        # already-inserted rows, so it won't re-close them). Undoing anything a prior
        # attempt at this exact batch wrote makes the whole operation idempotent.
        #
        # The batch's snapshot boundary alone scopes this correctly, with no
        # affected-keys table (unlike build_rollback_sql). In the normal case new
        # snapshots are strictly after the last historized snapshot, which bounds
        # every legit target valid_from/valid_to, so nothing legitimate is >=
        # min_new and both statements are no-ops on a clean run. A late-arrival
        # replay deliberately starts the batch at or below the watermark: there
        # the same two statements act as the rewind — rows from the replayed
        # window are deleted and pre-boundary rows reopened, leaving the target's
        # open rows exactly at the pre-boundary state the replay rebuilds from.
        # The DELETE and UPDATE target disjoint row sets, so they're
        # order-independent and each is itself re-entrant.
        rollback_prefix_sql = ""
        if rollback_prefix:
            min_new = self.destination.escape_string_literal(new_snapshots[0])
            rollback_prefix_sql = f"""-- Re-entrancy rollback: undo a prior crashed attempt at this snapshot batch.
DELETE FROM {tgt}
WHERE {self.valid_from} >= TIMESTAMP '{min_new}';

UPDATE {tgt}
SET {self.valid_to} = NULL
WHERE {self.valid_to} >= TIMESTAMP '{min_new}';

"""

        gap_projection, gap_join, gap_condition = self._gap_detection_sql()

        return f"""
-- Incremental Historization
-- Processes new snapshots and merges into historized table

{rollback_prefix_sql}CREATE OR REPLACE TEMP TABLE _historize_incremental AS
WITH
-- New-snapshot rows from the source unioned with the change-detection baseline
-- (the reference snapshot) taken from the historized target's open rows. The
-- baseline is projected to the same columns and stamped at the reference
-- snapshot value, so downstream CTEs treat it as just another snapshot. Reading
-- the baseline from the target (not the source) is what keeps replace-mode
-- sources from re-versioning every unchanged row.
source_rows AS (
  SELECT {self._qcols(all_output_cols)}, {self._snapshot_projection()}
  FROM {src}
  WHERE {and_filter(self.filter_sql, new_snapshot_filter)}{baseline_union}
),

{self._snapshot_sequence_ctes(q_snapshot, "source_rows")}-- Hash value columns
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

-- Compare to previous via LAG (+ detect gaps under full delivery)
with_context AS (
  SELECT d.*,
    LAG(_row_hash) OVER (pk_order) AS _prev_hash{gap_projection}
  FROM deduped d{gap_join}
  WINDOW pk_order AS (PARTITION BY {pk_cols} ORDER BY {q_snapshot})
),

-- Keep changed/new rows, exclude reference snapshot from output
changes AS (
  SELECT * FROM with_context c
  WHERE (_prev_hash IS NULL
     OR _row_hash != _prev_hash{gap_condition})
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

    def _scd1_rank_sql(self, descending: bool, snapshot_ref: str) -> str:
        """``ORDER BY`` terms ranking a key's versions (scd1).

        ``descending`` lists the highest-ranked version first; ascending is its
        exact reverse, so the winner is the last row of an ascending window.
        ``snapshot_column`` isn't carried into the target under its own name —
        it becomes ``valid_from`` — so it is ranked through ``snapshot_ref``:
        ``valid_from``, or with deletions a rank column that lets a deletion
        lasting to the end of the batch outrank the stored row. After
        ``order_by``, a source
        row outranks the stored one on an exact tie, and the value hash makes
        the order total.
        """
        snapshot = self.config.snapshot_column.lower()
        refs = [
            snapshot_ref if col.lower() == snapshot else self._q(col)
            for col in self.config.effective_order_by
        ]
        nulls, direction = (
            ("NULLS LAST", "DESC") if descending else ("NULLS FIRST", "ASC")
        )
        terms = [f"{ref} {direction} {nulls}" for ref in refs]
        terms += [f"_saga_from_batch {direction}", f"_saga_hash {direction}"]
        return ", ".join(terms)

    def _scd1_deletion_ctes(self, data_cols: List[str], has_stored: bool) -> str:
        """CTEs turning a key's absence from a snapshot into a deletion candidate.

        Only for scd1 with ``track_deletions`` (which requires ``delivery:
        full``). A key's appearances are the batch rows plus, when the target
        has rows, every stored key that isn't deleted — stamped at a sentinel
        before any real snapshot, meaning "present before this batch". A key
        whose next appearance skips the next snapshot of its group (per
        merge_key) disappeared at that snapshot: the candidate carries the
        values it was last seen with, flagged deleted. Ranking then decides
        between it and any later appearance, exactly as between versions.

        A disappearance the key doesn't come back from within the batch ranks
        above everything else: absent through the batch's last snapshot, the
        key is deleted there. Ranked at its own snapshot instead, it would lose
        to a stored row dated later — which a partial refresh or late-arrival
        replay over a window the key has left can hold.

        With stored rows, ``touched_keys`` limits the stored side of the
        ranking to keys a winner can come from: every other stored key keeps
        its row unchanged, so ranking it would only cost.
        """
        q_vf = self._q(self.valid_from)
        q_del = self._q(self.is_deleted)
        pk_cols = self._pk_cols_sql()
        touched_keys = f"""

-- Keys a winner can come from: those in the batch, and those that disappeared.
touched_keys AS (
  SELECT DISTINCT {pk_cols} FROM (
    SELECT {pk_cols} FROM batch
    UNION ALL
    SELECT {pk_cols} FROM disappearances
  ) AS k
),"""
        baseline = ""
        if has_stored:
            baseline = f"""
  UNION ALL
  SELECT {self._qcols(data_cols)}, TIMESTAMP '{SCD1_BASELINE_SNAPSHOT}'
  FROM stored
  WHERE NOT {q_del}"""
        seen_cols = ", ".join(f"s.{self._q(c)}" for c in data_cols)
        return f"""-- Every appearance of a key, with the values it had (stored keys: before the batch).
seen_rows AS (
  SELECT {self._qcols(data_cols)}, {q_vf} AS _saga_seen_at
  FROM batch{baseline}
),

{self._snapshot_sequence_ctes("_saga_seen_at", "seen_rows")}{self._disappearance_ctes("_saga_seen_at", "seen_rows")}
-- A deletion is a version: the last-seen values, flagged, from the disappearance on.
-- One lasting to the end of the batch is the key's state there, so it outranks
-- every other version, including a stored one dated after the disappearance.
deletion_rows AS (
  SELECT {seen_cols}, d.disappeared_at AS {q_vf}, TRUE AS {q_del},
    1 AS _saga_from_batch,
    CASE WHEN d.reappeared_at IS NULL THEN TIMESTAMP '{SCD1_FINAL_DELETION_RANK}'
      ELSE d.disappeared_at END AS _saga_rank_at
  FROM disappearances d
  INNER JOIN seen_rows s ON {self._pk_join("d", "s")}
    AND s._saga_seen_at = d.last_seen
),{touched_keys if has_stored else ""}

"""

    def build_scd1_sql(
        self,
        value_columns: List[str],
        snapshots: Optional[List[str]] = None,
        snapshot_upper_bound: Optional[str] = None,
        target_is_empty: bool = False,
    ) -> str:
        """Build SQL that keeps the latest version per key (``strategy: scd1``).

        Candidate versions are the source rows in scope plus the stored row of
        every key they touch, ranked per key by ``order_by``. Ranking against
        the stored row instead of overwriting it makes the outcome independent
        of the order batches arrive in: a version ranked below the stored one
        loses, however late it lands. On an exact tie the source row wins, so
        re-processing a corrected source row takes effect.

        The winner's ``valid_from`` is where its values took effect: the
        earliest version in the unbroken stretch of identical values that ends
        at the winner. When that stretch includes the stored row, the stored
        row already says so and nothing is written — an unchanged key is
        never rewritten, however often the source re-delivers it.

        With ``track_deletions``, a key's disappearance is one more candidate
        version (see :meth:`_scd1_deletion_ctes`), flagged ``is_deleted`` and
        carrying its last-seen values; the deleted flag counts as part of the
        values for change detection. Every stored key that isn't deleted then
        counts as present before the batch, since a key missing from the batch
        is exactly what a deletion looks like; only keys in the batch or
        disappearing from it are ranked.

        Serves every mode: a full reprocess runs it against the freshly
        recreated target (``target_is_empty``: no stored rows to rank against,
        plain INSERT); incremental runs and partial refreshes run it against
        the existing target with a snapshot range. Nothing is written outside
        the final statement that applies the winners, so a crashed-then-retried
        run converges without the rollback scd2 needs.

        The script ends by selecting the number of rows written (and of those,
        deletions), which the runner reads as the run's stats.

        Args:
            value_columns: Non-PK columns carried into the target.
            snapshots: Chronologically ordered batch to process; bounds the
                source read by range. ``None`` reads the whole source.
            snapshot_upper_bound: Upper bound for a whole-source read, so the
                recorded baseline matches what was processed.
            target_is_empty: The target holds no rows (full reprocess).
        """
        deletions = bool(self.config.track_deletions)
        pk_cols = self._pk_cols_sql()
        data_cols = list(self.primary_key) + list(value_columns)
        system_cols = [self.valid_from] + ([self.is_deleted] if deletions else [])
        all_cols = data_cols + system_cols
        all_cols_sql = self._qcols(all_cols)
        q_vf = self._q(self.valid_from)
        q_del = self._q(self.is_deleted)
        src = self.source_table_id
        tgt = self.target_table_id

        if snapshots is not None:
            source_filter: Optional[str] = and_filter(
                self.filter_sql,
                snapshot_range_filter(self.destination, self._snap_src, snapshots),
            )
        else:
            source_filter = self._source_filter_with_bound(
                self._snap_src, snapshot_upper_bound
            )

        batch_deleted = f", FALSE AS {q_del}" if deletions else ""
        # With deletions every candidate carries the snapshot it ranks at
        # (see _scd1_deletion_ctes); otherwise that is valid_from itself.
        rank_at = f", {self._snap_src} AS _saga_rank_at" if deletions else ""
        snapshot_ref = "_saga_rank_at" if deletions else q_vf
        deletion_union = (
            "\n  UNION ALL\n  SELECT * FROM deletion_rows" if deletions else ""
        )

        if target_is_empty:
            stored_ctes = ""
            stored_union = ""
            apply_sql = f"""INSERT INTO {tgt} ({all_cols_sql})
SELECT {all_cols_sql} FROM _historize_scd1;"""
        else:
            target_cols = ", ".join(f"t.{self._q(c)}" for c in all_cols)
            if deletions:
                stored_ctes = f"""-- Every stored row: a stored key missing from the batch may have been deleted.
stored AS (
  SELECT {target_cols}, 0 AS _saga_from_batch, t.{q_vf} AS _saga_rank_at
  FROM {tgt} t
),

"""
                stored_union = f"""
  UNION ALL
  SELECT st.* FROM stored st
  INNER JOIN touched_keys k ON {self._pk_join("st", "k")}"""
            else:
                stored_ctes = f"""batch_keys AS (
  SELECT DISTINCT {pk_cols} FROM batch
),

-- The stored version of every key in the batch competes with the batch's own.
stored AS (
  SELECT {target_cols}, 0 AS _saga_from_batch
  FROM {tgt} t
  INNER JOIN batch_keys k ON {self._pk_join("t", "k")}
),

"""
                stored_union = "\n  UNION ALL\n  SELECT * FROM stored"
            update_set = ",\n    ".join(
                f"{self._q(c)} = n.{self._q(c)}"
                for c in list(value_columns) + system_cols
            )
            insert_values = ", ".join(f"n.{self._q(c)}" for c in all_cols)
            apply_sql = f"""MERGE INTO {tgt} t
USING _historize_scd1 n
ON {self._pk_join("t", "n")}
WHEN MATCHED THEN
  UPDATE SET
    {update_set}
WHEN NOT MATCHED THEN
  INSERT ({all_cols_sql})
  VALUES ({insert_values});"""

        deletion_ctes = (
            self._scd1_deletion_ctes(data_cols, has_stored=not target_is_empty)
            if deletions
            else ""
        )
        output_deleted = f", {q_del}" if deletions else ""
        counts = (
            f"SUM(CASE WHEN {q_del} THEN 0 ELSE 1 END) AS new_or_changed_rows,\n"
            f"  SUM(CASE WHEN {q_del} THEN 1 ELSE 0 END) AS deleted_rows"
            if deletions
            else "COUNT(*) AS new_or_changed_rows"
        )
        # The deleted flag is part of a version's values: deleting a key, or
        # bringing it back unchanged, is a change.
        hash_expr = self.destination.hash_expression(
            list(value_columns) + ([self.is_deleted] if deletions else [])
        )
        rank_asc = self._scd1_rank_sql(descending=False, snapshot_ref=snapshot_ref)
        rank_desc = self._scd1_rank_sql(descending=True, snapshot_ref=snapshot_ref)

        return f"""
-- SCD1 Historization
-- Keeps the latest version per key

CREATE OR REPLACE TEMP TABLE _historize_scd1 AS
WITH
-- Source rows in scope; the snapshot value becomes the version's valid_from.
batch AS (
  SELECT {self._qcols(data_cols)}, {self._snap_src} AS {q_vf}{batch_deleted},
    1 AS _saga_from_batch{rank_at}
  FROM {src}{filter_where_clause(source_filter)}
),

{stored_ctes}{deletion_ctes}candidates AS (
  SELECT * FROM batch{deletion_union}{stored_union}
),

hashed AS (
  SELECT c.*, {hash_expr} AS _saga_hash
  FROM candidates c
),

-- A key's rows within one snapshot: only the top-ranked can win, and the others
-- would split its stretch of identical values from the stored row's.
snapshot_best AS (
  SELECT h.*,
    ROW_NUMBER() OVER (
      PARTITION BY {pk_cols}, {q_vf}, _saga_from_batch
      ORDER BY {rank_desc}
    ) AS _saga_snapshot_rn
  FROM hashed h
),

-- Rank each key's versions; flag where the values differ from the next-lower one.
sequenced AS (
  SELECT b.*,
    ROW_NUMBER() OVER (
      PARTITION BY {pk_cols} ORDER BY {rank_desc}
    ) AS _saga_rn,
    CASE WHEN _saga_hash = LAG(_saga_hash) OVER (
      PARTITION BY {pk_cols} ORDER BY {rank_asc}
    ) THEN 0 ELSE 1 END AS _saga_changed
  FROM snapshot_best b
  WHERE _saga_snapshot_rn = 1
),

-- Number the stretches of identical values, in rank order.
stretched AS (
  SELECT s.*,
    SUM(_saga_changed) OVER (
      PARTITION BY {pk_cols} ORDER BY {rank_asc}
      ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS _saga_stretch
  FROM sequenced s
),

-- Where each stretch began, and whether the stored row is part of it.
with_since AS (
  SELECT st.*,
    MIN({q_vf}) OVER (PARTITION BY {pk_cols}, _saga_stretch) AS _saga_since,
    MIN(_saga_from_batch) OVER (PARTITION BY {pk_cols}, _saga_stretch)
      AS _saga_stretch_from_batch
  FROM stretched st
)
-- The winner, unless the stored row already holds its values.
SELECT {self._qcols(data_cols)}, _saga_since AS {q_vf}{output_deleted}
FROM with_since
WHERE _saga_rn = 1 AND _saga_stretch_from_batch = 1;

{apply_sql}

SELECT {counts}
FROM _historize_scd1;
"""
