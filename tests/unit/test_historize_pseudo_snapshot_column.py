"""Unit tests for a pseudo-column used as the historize snapshot column.

On an ingestion-time partitioned BigQuery table, `_PARTITIONTIME` is often the
only timestamp available. It resolves on a direct scan of its own table but is
returned by neither `SELECT *` nor a projection, so the generated SQL reads it
off the source under an alias and references that alias in every CTE.

An ordinary snapshot column must keep producing byte-identical SQL — the alias
machinery may not leak into the common path.
"""

from unittest.mock import MagicMock

import pytest

from dlt_saga.historize.config import HistorizeConfig
from dlt_saga.historize.sql import SNAPSHOT_ALIAS, HistorizeSqlBuilder

PSEUDO = "_PARTITIONTIME"
ALIAS = f"`{SNAPSHOT_ALIAS}`"
VALUE_COLUMNS = ["city", "state"]


def _stub_destination():
    dest = MagicMock()
    dest.quote_identifier.side_effect = lambda s: f"`{s}`"
    dest.hash_expression.side_effect = lambda cols: f"HASH({', '.join(cols)})"
    dest.is_pseudo_column.side_effect = lambda n: n.lower().startswith("_partition")
    dest.cast_to_string.side_effect = lambda expr: f"CAST({expr} AS STRING)"
    dest.escape_string_literal.side_effect = lambda v: v
    dest.type_name.side_effect = lambda t: t.upper()
    dest.get_full_table_id.side_effect = lambda ds, tbl: f"proj.{ds}.{tbl}"
    return dest


def _builder(snapshot_column):
    config = HistorizeConfig.from_dict(
        {
            "primary_key": ["id"],
            "snapshot_column": snapshot_column,
            "track_deletions": True,
        },
        top_level_primary_key=["id"],
    )
    return HistorizeSqlBuilder(
        config=config,
        destination=_stub_destination(),
        source_table_id="proj.ds.src",
        target_table_id="proj.ds.tgt",
        primary_key=["id"],
        source_database="proj",
        source_schema="ds",
        source_table="src",
        target_table_name="tgt",
        target_schema="ds",
    )


@pytest.mark.unit
class TestFullReprocessWithPseudoSnapshotColumn:
    def setup_method(self):
        self.sql = _builder(PSEUDO).build_full_reprocess_sql(VALUE_COLUMNS)

    def test_source_scan_projects_the_pseudo_column_under_the_alias(self):
        # SELECT * drops it, so the hashing CTE has to name it explicitly.
        assert f"`{PSEUDO}` AS {ALIAS}" in self.sql

    def test_snapshot_reads_off_the_source_use_the_pseudo_name(self):
        assert f"SELECT DISTINCT `{PSEUDO}` AS snapshot_date" in self.sql

    def test_downstream_ctes_reference_the_alias(self):
        # These all read a projected CTE, where the pseudo-column no longer exists.
        assert f"PARTITION BY `id`, {ALIAS}" in self.sql
        assert f"LAG({ALIAS}) OVER (pk_order)" in self.sql
        assert f"ss.seq_snapshot_date = d.{ALIAS}" in self.sql
        assert f"c.{ALIAS} AS _dlt_valid_from" in self.sql

    def test_neither_name_reaches_the_target(self):
        # The SCD2 projection lists its output columns explicitly, so the alias
        # stays inside the CTE plumbing.
        insert = self.sql[self.sql.index("INSERT INTO") :]
        assert SNAPSHOT_ALIAS not in insert
        assert PSEUDO not in insert


@pytest.mark.unit
class TestIncrementalWithPseudoSnapshotColumn:
    def setup_method(self):
        self.sql = _builder(PSEUDO).build_incremental_sql(
            VALUE_COLUMNS,
            new_snapshots=["2026-09-16 00:00:00"],
            last_historized_snapshot="2026-09-15 00:00:00",
        )

    def test_source_rows_projects_the_alias(self):
        assert f"`{PSEUDO}` AS {ALIAS}" in self.sql

    def test_source_side_filter_uses_the_pseudo_name(self):
        # Pruning must hit the partition pseudo-column on the source itself.
        assert (
            f"`{PSEUDO}` >= TIMESTAMP '2026-09-16 00:00:00' "
            f"AND `{PSEUDO}` <= TIMESTAMP '2026-09-16 00:00:00'"
        ) in self.sql

    def test_target_derived_baseline_is_stamped_under_the_alias(self):
        # The baseline comes from the historized table, which has no such column.
        assert f"TIMESTAMP '2026-09-15 00:00:00' AS {ALIAS}" in self.sql

    def test_downstream_ctes_reference_the_alias(self):
        assert f"PARTITION BY `id`, {ALIAS}" in self.sql
        assert f"c.{ALIAS} AS _dlt_valid_from" in self.sql


@pytest.mark.unit
class TestOrdinarySnapshotColumnIsUnaffected:
    def test_no_alias_appears_anywhere(self):
        builder = _builder("delivery_date")
        full = builder.build_full_reprocess_sql(VALUE_COLUMNS)
        incremental = builder.build_incremental_sql(
            VALUE_COLUMNS, new_snapshots=["2026-09-16 00:00:00"]
        )
        rollback = " ".join(builder.build_rollback_sql("2026-09-16", "r1", "ds"))
        for sql in (full, incremental, rollback):
            assert SNAPSHOT_ALIAS not in sql
            assert "`delivery_date`" in sql

    def test_rollback_reads_the_pseudo_column_directly(self):
        # build_rollback_sql scans the source table itself — no projection, so
        # the pseudo-column is referenced by name.
        rollback = " ".join(
            _builder(PSEUDO).build_rollback_sql("2026-09-16", "r1", "ds")
        )
        assert f"`{PSEUDO}` >= TIMESTAMP '2026-09-16'" in rollback
        assert SNAPSHOT_ALIAS not in rollback
