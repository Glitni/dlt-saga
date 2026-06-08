"""Regression tests: historize SQL must not put a correlated scalar subquery in
the same clause as a window function (unsupported on Spark/Databricks).

The previous/next overall snapshot is precomputed in a `snapshot_sequence` CTE
and JOINed, instead of `(SELECT MAX/MIN(snapshot_date) FROM all_snapshots ...)`
inline next to LAG/LEAD window functions.
"""

from unittest.mock import MagicMock

import pytest

from dlt_saga.historize.config import HistorizeConfig
from dlt_saga.historize.sql import HistorizeSqlBuilder


def _stub_destination():
    dest = MagicMock()
    dest.quote_identifier.side_effect = lambda s: f"`{s}`"
    dest.hash_expression.side_effect = lambda cols: f"HASH({', '.join(cols)})"
    dest.cast_to_string.side_effect = lambda expr: f"CAST({expr} AS STRING)"
    dest.type_name.side_effect = lambda t: t.upper()
    dest.get_full_table_id.side_effect = lambda ds, tbl: f"proj.{ds}.{tbl}"
    dest.columns_query.side_effect = lambda db, sc, t: "SELECT column_name FROM info"
    dest.build_historize_create_table_sql.side_effect = lambda **kw: kw["select_body"]
    return dest


def _builder(track_deletions=True):
    config = HistorizeConfig.from_dict(
        {"track_deletions": track_deletions}, top_level_primary_key=["id"]
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


_FORBIDDEN = ("SELECT MAX(snapshot_date)", "SELECT MIN(snapshot_date)")


@pytest.mark.unit
@pytest.mark.parametrize("track_deletions", [True, False])
class TestNoCorrelatedSubqueryWithWindow:
    def test_full_reprocess(self, track_deletions):
        sql = _builder(track_deletions).build_full_reprocess_sql(["a", "b"])
        assert "snapshot_sequence" in sql
        for pat in _FORBIDDEN:
            assert pat not in sql, f"correlated subquery still present: {pat}"

    def test_incremental(self, track_deletions):
        sql = _builder(track_deletions).build_incremental_sql(
            ["a", "b"],
            new_snapshots=["2024-01-02"],
            last_historized_snapshot="2024-01-01",
        )
        assert "snapshot_sequence" in sql
        for pat in _FORBIDDEN:
            assert pat not in sql, f"correlated subquery still present: {pat}"
