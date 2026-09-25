"""Unit tests for snapshot-batch range bounding.

A batch of discovered snapshots is bounded by ``>= first AND <= last`` rather
than by an inlined list of every value, so statement size stays constant as a
backlog grows. Covers the helper itself and the incremental SQL that uses it.
"""

from unittest.mock import MagicMock

import pytest

from dlt_saga.historize.config import HistorizeConfig
from dlt_saga.historize.sql import HistorizeSqlBuilder, snapshot_range_filter


def _stub_destination():
    """Minimal stub for HistorizeSqlBuilder — only the methods actually called."""
    dest = MagicMock()
    dest.quote_identifier.side_effect = lambda s: f"`{s}`"
    dest.escape_string_literal.side_effect = lambda s: s.replace("'", "\\'")
    dest.hash_expression.side_effect = lambda cols: f"HASH({', '.join(cols)})"
    dest.is_pseudo_column.return_value = False
    dest.cast_to_string.side_effect = lambda expr: f"CAST({expr} AS STRING)"
    dest.type_name.side_effect = lambda t: t.upper()
    dest.get_full_table_id.side_effect = lambda ds, tbl: f"proj.{ds}.{tbl}"
    return dest


def _builder():
    config = HistorizeConfig.from_dict({}, top_level_primary_key=["id"])
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


def _snapshots(count: int):
    """``count`` distinct per-minute snapshot values, chronologically ordered."""
    return [f"2026-09-25 {h:02d}:{m:02d}:00" for h in range(24) for m in range(60)][
        :count
    ]


@pytest.mark.unit
class TestSnapshotRangeFilter:
    def test_bounds_by_first_and_last(self):
        snapshots = [
            "2026-01-01 00:00:00",
            "2026-01-02 00:00:00",
            "2026-01-03 00:00:00",
        ]
        assert snapshot_range_filter(_stub_destination(), "`snap`", snapshots) == (
            "`snap` >= TIMESTAMP '2026-01-01 00:00:00' "
            "AND `snap` <= TIMESTAMP '2026-01-03 00:00:00'"
        )

    def test_single_snapshot_bounds_both_ends(self):
        assert snapshot_range_filter(
            _stub_destination(), "`snap`", ["2026-01-01 00:00:00"]
        ) == (
            "`snap` >= TIMESTAMP '2026-01-01 00:00:00' "
            "AND `snap` <= TIMESTAMP '2026-01-01 00:00:00'"
        )

    def test_literals_are_escaped(self):
        # The inlined-list form skipped escaping; every singular literal site
        # escapes, and so does this one.
        out = snapshot_range_filter(_stub_destination(), "`snap`", ["it's", "later"])
        assert "TIMESTAMP 'it\\'s'" in out

    def test_empty_batch_is_rejected(self):
        with pytest.raises(ValueError, match="empty snapshot batch"):
            snapshot_range_filter(_stub_destination(), "`snap`", [])


@pytest.mark.unit
class TestIncrementalSqlBoundsByRange:
    def test_source_read_uses_the_range(self):
        sql = _builder().build_incremental_sql(
            value_columns=["name"],
            new_snapshots=["2026-01-01 00:00:00", "2026-01-02 00:00:00"],
        )
        assert (
            "`_dlt_ingested_at` >= TIMESTAMP '2026-01-01 00:00:00' "
            "AND `_dlt_ingested_at` <= TIMESTAMP '2026-01-02 00:00:00'"
        ) in sql

    def test_interior_snapshots_are_not_inlined(self):
        # Only the two bounds reach the SQL; everything between them is implied.
        snapshots = _snapshots(50)
        sql = _builder().build_incremental_sql(
            value_columns=["name"], new_snapshots=snapshots
        )
        for interior in snapshots[1:-1]:
            assert interior not in sql

    def test_statement_size_is_flat_in_batch_size(self):
        # The regression this guards: a per-row snapshot column accumulates tens
        # of thousands of distinct values in a backlog, and one literal apiece
        # pushed the statement past BigQuery's 1 MB query-length limit.
        small = _builder().build_incremental_sql(
            value_columns=["name"], new_snapshots=_snapshots(2)
        )
        large = _builder().build_incremental_sql(
            value_columns=["name"], new_snapshots=_snapshots(1000)
        )
        assert len(small) == len(large)
