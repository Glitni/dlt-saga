"""Unit tests for historize-stage row filters.

Covers:
- HistorizeConfig.filters parsing and from_dict round-trip
- HistorizeSqlBuilder injects WHERE into source reads in both full-reprocess
  and incremental SQL, and into the rollback affected-keys query
- HistorizeStateManager.discover_unprocessed_snapshots honours filter_sql
- compute_fingerprint detects a change to historize.filters
"""

from unittest.mock import MagicMock

import pytest

from dlt_saga.historize.config import HistorizeConfig
from dlt_saga.historize.sql import HistorizeSqlBuilder
from dlt_saga.historize.state import HistorizeStateManager
from dlt_saga.utility.filters import and_filter, filter_where_clause


def _stub_destination():
    """Minimal stub for HistorizeSqlBuilder — only the methods actually called."""
    dest = MagicMock()
    dest.quote_identifier.side_effect = lambda s: f"`{s}`"
    dest.hash_expression.side_effect = lambda cols: f"HASH({', '.join(cols)})"
    dest.cast_to_string.side_effect = lambda expr: f"CAST({expr} AS STRING)"
    dest.type_name.side_effect = lambda t: t.upper()
    dest.get_full_table_id.side_effect = lambda ds, tbl: f"proj.{ds}.{tbl}"
    dest.columns_query.side_effect = lambda db, sc, t: "SELECT column_name FROM info"
    dest.build_historize_create_table_sql.side_effect = lambda **kw: (
        f"CREATE TABLE {kw['target_table_id']} AS {kw['select_body']}"
    )
    return dest


def _make_builder(filter_sql=None):
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
        filter_sql=filter_sql,
    )


# ---------------------------------------------------------------------------
# HistorizeConfig.filters
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestHistorizeConfigFilters:
    def test_filters_default_none(self):
        config = HistorizeConfig.from_dict({}, top_level_primary_key=["id"])
        assert config.filters is None

    def test_filters_round_trip(self):
        raw = [{"column": "tenant_id", "value": "tenant_a"}]
        config = HistorizeConfig.from_dict(
            {"filters": raw}, top_level_primary_key=["id"]
        )
        assert config.filters == raw

    def test_filters_list_in_op(self):
        raw = [{"column": "status", "op": "in", "value": ["active", "pending"]}]
        config = HistorizeConfig.from_dict(
            {"filters": raw}, top_level_primary_key=["id"]
        )
        assert config.filters == raw


# ---------------------------------------------------------------------------
# Shared module-level filter helpers (used by both historize sql.py and state.py)
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestFilterHelpers:
    def test_filter_where_clause_empty_without_filter(self):
        assert filter_where_clause(None) == ""
        assert filter_where_clause("") == ""

    def test_filter_where_clause_with_filter(self):
        assert filter_where_clause("`tenant_id` = 'tenant_a'") == (
            " WHERE `tenant_id` = 'tenant_a'"
        )

    def test_and_filter_passthrough_when_unset(self):
        assert and_filter(None, "x > 1") == "x > 1"
        assert and_filter("", "x > 1") == "x > 1"

    def test_and_filter_wraps_existing(self):
        assert and_filter("`tenant_id` = 'tenant_a'", "x > 1") == (
            "x > 1 AND (`tenant_id` = 'tenant_a')"
        )


# ---------------------------------------------------------------------------
# SQL injection sites
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestSqlInjection:
    def test_full_reprocess_injects_filter_into_source_ctes(self):
        b = _make_builder(filter_sql="`tenant_id` = 'tenant_a'")
        sql = b.build_full_reprocess_sql(value_columns=["name", "amount"])

        # all_snapshots, hashed, and key_presence (deletion tracking) all
        # read from src — each should have the filter WHERE.
        assert sql.count(" FROM proj.ds.src WHERE `tenant_id` = 'tenant_a'") == 3

    def test_full_reprocess_without_filter_unchanged(self):
        b = _make_builder()
        sql = b.build_full_reprocess_sql(value_columns=["name", "amount"])
        # No filter clause should appear after FROM proj.ds.src
        assert " FROM proj.ds.src WHERE" not in sql

    def test_incremental_ands_filter_into_source_rows(self):
        b = _make_builder(filter_sql="`tenant_id` = 'tenant_a'")
        sql = b.build_incremental_sql(
            value_columns=["name"],
            new_snapshots=["2026-01-01 00:00:00"],
            last_historized_snapshot=None,
        )
        # The source read is consolidated into a single source_rows CTE; the
        # historize filter is AND'd into its WHERE exactly once. Downstream CTEs
        # read from the (already-filtered) source_rows.
        assert "source_rows AS (" in sql
        assert sql.count("AND (`tenant_id` = 'tenant_a')") == 1
        assert "FROM source_rows" in sql
        # The only direct read of the raw source is inside source_rows, and it
        # carries the filter — nothing bypasses it.
        assert sql.count("FROM proj.ds.src") == 1

    def test_incremental_deletion_cte_reads_filtered_source(self):
        b = _make_builder(filter_sql="`tenant_id` = 'tenant_a'")
        sql = b.build_incremental_sql(
            value_columns=["name"],
            new_snapshots=["2026-01-01 00:00:00"],
            last_historized_snapshot=None,
        )
        # Deletion detection flows through source_rows, so it inherits the filter
        # without re-stating it (rather than reading the raw source directly).
        assert "deletion_candidates" in sql
        deletion_block = sql.split("deletion_candidates AS (")[1].split(")")[0]
        assert "FROM source_rows" in deletion_block
        assert "proj.ds.src" not in deletion_block

    def test_rollback_ands_filter_into_keys_query(self):
        b = _make_builder(filter_sql="`tenant_id` = 'tenant_a'")
        stmts = b.build_rollback_sql(
            effective_from_date="2026-01-01 00:00:00",
            run_id="abc123",
            dataset="ds",
        )
        # Affected-keys is statement [0]; its WHERE should AND the filter
        # so we only collect keys for *this* tenant.
        create_keys = stmts[0]
        assert "AND (`tenant_id` = 'tenant_a')" in create_keys


# ---------------------------------------------------------------------------
# state.discover_unprocessed_snapshots filter_sql
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestDiscoverUnprocessedSnapshots:
    def _stub(self):
        sm = HistorizeStateManager.__new__(HistorizeStateManager)
        sm.destination = MagicMock()
        sm.destination.cast_to_string.side_effect = lambda e: f"CAST({e} AS STRING)"
        sm.destination.execute_sql.return_value = []
        sm.schema = "ds"
        sm.logger = MagicMock()
        return sm

    def test_no_filter_first_run(self):
        sm = self._stub()
        state = MagicMock()
        state.has_successful_run = False
        sm.discover_unprocessed_snapshots(
            state=state,
            source_table_id="proj.ds.src",
            snapshot_column="event_ts",
        )
        sql = sm.destination.execute_sql.call_args[0][0]
        assert "FROM proj.ds.src" in sql
        assert "WHERE" not in sql.upper().split("ORDER BY")[0]

    def test_filter_applied_first_run(self):
        sm = self._stub()
        state = MagicMock()
        state.has_successful_run = False
        sm.discover_unprocessed_snapshots(
            state=state,
            source_table_id="proj.ds.src",
            snapshot_column="event_ts",
            filter_sql="`tenant_id` = 'tenant_a'",
        )
        sql = sm.destination.execute_sql.call_args[0][0]
        assert "FROM proj.ds.src WHERE `tenant_id` = 'tenant_a'" in sql

    def test_filter_anded_subsequent_run(self):
        sm = self._stub()
        state = MagicMock()
        state.has_successful_run = True
        state.last_snapshot_value = "2026-01-01 00:00:00"
        sm.discover_unprocessed_snapshots(
            state=state,
            source_table_id="proj.ds.src",
            snapshot_column="event_ts",
            filter_sql="`tenant_id` = 'tenant_a'",
        )
        sql = sm.destination.execute_sql.call_args[0][0]
        # Existing snapshot WHERE clause remains; filter is AND'd.
        assert "event_ts > TIMESTAMP" in sql
        assert "AND (`tenant_id` = 'tenant_a')" in sql


# ---------------------------------------------------------------------------
# Fingerprint
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestFingerprintIncludesFilters:
    def test_filter_change_changes_fingerprint(self):
        a = HistorizeConfig.from_dict(
            {"filters": [{"column": "tenant_id", "value": "tenant_a"}]},
            top_level_primary_key=["id"],
        )
        b = HistorizeConfig.from_dict(
            {"filters": [{"column": "tenant_id", "value": "tenant_b"}]},
            top_level_primary_key=["id"],
        )
        assert HistorizeStateManager.compute_fingerprint(
            a
        ) != HistorizeStateManager.compute_fingerprint(b)

    def test_no_filter_vs_empty_filter_match(self):
        a = HistorizeConfig.from_dict({}, top_level_primary_key=["id"])
        b = HistorizeConfig.from_dict({"filters": []}, top_level_primary_key=["id"])
        # Both serialise to []; treat as identical.
        assert HistorizeStateManager.compute_fingerprint(
            a
        ) == HistorizeStateManager.compute_fingerprint(b)

    def test_filter_order_changes_fingerprint(self):
        """Order of filters can matter semantically (AND-joined, but each
        entry is independent) — we don't sort, so a re-ordering is a change.

        This is conservative: arguably re-ordering AND'd filters is a no-op
        semantically, but the runner re-renders the WHERE in order and we'd
        rather prompt full-refresh than miss a real change.
        """
        a = HistorizeConfig.from_dict(
            {
                "filters": [
                    {"column": "tenant_id", "value": "tenant_a"},
                    {"column": "status", "value": "active"},
                ]
            },
            top_level_primary_key=["id"],
        )
        b = HistorizeConfig.from_dict(
            {
                "filters": [
                    {"column": "status", "value": "active"},
                    {"column": "tenant_id", "value": "tenant_a"},
                ]
            },
            top_level_primary_key=["id"],
        )
        assert HistorizeStateManager.compute_fingerprint(
            a
        ) != HistorizeStateManager.compute_fingerprint(b)
