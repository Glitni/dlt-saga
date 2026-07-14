"""Unit tests: the incremental SQL rollback prefix is emitted only when asked.

The in-place incremental path (``_run_incremental``) opts in with
``rollback_prefix=True`` to make a crash + retry idempotent; the partial-refresh
staging path leaves it ``False`` because it rolls back against a clone via
``build_rollback_sql`` (adding an unscoped boundary rollback there would touch
unaffected keys). These tests guard that toggle and the temp-table DDL.
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
    # Backslash-escape apostrophes (BigQuery-style) so interpolation yields a string.
    dest.escape_string_literal.side_effect = lambda s: s.replace("'", "\\'")
    return dest


def _builder():
    config = HistorizeConfig.from_dict(
        {"track_deletions": True}, top_level_primary_key=["id"]
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
class TestRollbackPrefixToggle:
    def test_prefix_absent_by_default(self):
        sql = _builder().build_incremental_sql(
            ["a", "b"],
            new_snapshots=["2024-01-02"],
            last_historized_snapshot="2024-01-01",
        )
        assert "DELETE FROM proj.ds.tgt" not in sql
        assert "SET _dlt_valid_to = NULL" not in sql
        # Temp table is re-run-safe regardless of the prefix toggle.
        assert "CREATE OR REPLACE TEMP TABLE _historize_incremental" in sql

    def test_prefix_present_when_enabled(self):
        sql = _builder().build_incremental_sql(
            ["a", "b"],
            new_snapshots=["2024-01-02", "2024-01-03"],
            last_historized_snapshot="2024-01-01",
            rollback_prefix=True,
        )
        # DELETE + reopen scoped to the batch's minimum (first, ascending) snapshot.
        assert (
            "DELETE FROM proj.ds.tgt\nWHERE _dlt_valid_from >= TIMESTAMP '2024-01-02'"
            in sql
        )
        assert (
            "SET _dlt_valid_to = NULL\nWHERE _dlt_valid_to >= TIMESTAMP '2024-01-02'"
            in sql
        )
        # The prefix runs before the temp-table build.
        assert sql.index("DELETE FROM proj.ds.tgt") < sql.index(
            "CREATE OR REPLACE TEMP TABLE"
        )

    def test_prefix_noop_when_no_new_snapshots(self):
        sql = _builder().build_incremental_sql(
            ["a", "b"],
            new_snapshots=[],
            last_historized_snapshot="2024-01-01",
            rollback_prefix=True,
        )
        assert "DELETE FROM proj.ds.tgt" not in sql
