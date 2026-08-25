"""Unit tests for the NULL-primary-key guard on historize runs.

The primary key is the SCD2 identity and SQL equality is never true for NULL:
change detection partitions by the PK (where NULL groups with NULL, so change
rows *are* produced), but the MERGE closing the previous open row
(``ON t.pk = n.pk``) and the deletion-marker joins never match. Each run
therefore leaves another permanently-open row for the same NULL key. The guard
refuses the run instead — before a full refresh drops the existing history.

An incremental run gets the answer out of ``discover_unprocessed_snapshots``,
which already scans exactly the rows it will process; only full/partial refresh
(no such earlier scan) falls back to a separate probe.
"""

import logging
from unittest.mock import MagicMock

import pytest

from dlt_saga.historize.config import HistorizeConfig
from dlt_saga.historize.runner import HistorizeRunner
from dlt_saga.historize.sql import HistorizeSqlBuilder
from dlt_saga.historize.state import HistorizeStateManager, SnapshotDiscovery


def _stub_destination():
    dest = MagicMock()
    dest.quote_identifier.side_effect = lambda s: f"`{s}`"
    dest.escape_string_literal.side_effect = lambda s: s.replace("'", "''")
    dest.hash_expression.side_effect = lambda cols: f"HASH({', '.join(cols)})"
    return dest


def _make_builder(primary_key=None, filter_sql=None):
    primary_key = primary_key or ["id"]
    config = HistorizeConfig.from_dict({}, top_level_primary_key=primary_key)
    return HistorizeSqlBuilder(
        config=config,
        destination=_stub_destination(),
        source_table_id="proj.ds.src",
        target_table_id="proj.ds.tgt",
        primary_key=primary_key,
        source_database="proj",
        source_schema="ds",
        source_table="src",
        target_table_name="tgt",
        target_schema="ds",
        filter_sql=filter_sql,
    )


@pytest.mark.unit
class TestBuildNullPkProbeSql:
    def test_single_key(self):
        sql = _make_builder().build_null_pk_probe_sql()
        assert "FROM proj.ds.src" in sql
        assert "(`id` IS NULL)" in sql
        assert "CASE WHEN `id` IS NULL THEN 1 ELSE 0 END AS _null_pk_0" in sql
        assert sql.rstrip().endswith("LIMIT 1")

    def test_composite_key_checks_every_column(self):
        sql = _make_builder(primary_key=["tenant", "id"]).build_null_pk_probe_sql()
        assert "(`tenant` IS NULL OR `id` IS NULL)" in sql
        assert "AS _null_pk_0" in sql
        assert "AS _null_pk_1" in sql

    def test_aliases_are_positional_and_match_key_order(self):
        builder = _make_builder(primary_key=["tenant", "id"])
        assert builder.null_pk_probe_aliases() == ["_null_pk_0", "_null_pk_1"]

    def test_historize_filters_are_applied(self):
        """A row excluded by historize.filters is never historized, so a NULL key
        there must not block the run — that's the documented escape hatch."""
        sql = _make_builder(filter_sql="`tenant` = 'a'").build_null_pk_probe_sql()
        assert "`tenant` = 'a'" in sql
        assert "(`id` IS NULL)" in sql

    def test_no_primary_key_returns_none(self):
        builder = _make_builder()
        builder.primary_key = []
        assert builder.build_null_pk_probe_sql() is None


def _stub_state_manager(rows=None):
    sm = HistorizeStateManager.__new__(HistorizeStateManager)
    sm.destination = MagicMock()
    sm.destination.cast_to_string.side_effect = lambda e: f"CAST({e} AS STRING)"
    sm.destination.quote_identifier.side_effect = lambda s: f"`{s}`"
    sm.destination.escape_string_literal.side_effect = lambda s: s
    sm.destination.execute_sql.return_value = rows or []
    sm.schema = "ds"
    sm.logger = MagicMock()
    return sm


@pytest.mark.unit
class TestDiscoveryChecksPrimaryKeys:
    """The NULL-key check rides along on the scan discovery already performs."""

    def _discover(self, sm, primary_key=None, has_run=False):
        state = MagicMock()
        state.has_successful_run = has_run
        state.last_snapshot_value = "2026-01-01 00:00:00"
        return sm.discover_unprocessed_snapshots(
            state=state,
            source_table_id="proj.ds.src",
            snapshot_column="event_ts",
            primary_key=primary_key,
        )

    def test_flags_are_grouped_per_snapshot(self):
        sm = _stub_state_manager()
        self._discover(sm, primary_key=["tenant", "id"])
        sql = sm.destination.execute_sql.call_args[0][0]
        assert "MAX(CASE WHEN `tenant` IS NULL THEN 1 ELSE 0 END) AS _null_pk_0" in sql
        assert "MAX(CASE WHEN `id` IS NULL THEN 1 ELSE 0 END) AS _null_pk_1" in sql
        # GROUP BY repeats the expression rather than the alias — portable.
        assert "GROUP BY CAST(`event_ts` AS STRING)" in sql
        assert "DISTINCT" not in sql

    def test_without_primary_key_the_query_is_unchanged(self):
        sm = _stub_state_manager()
        self._discover(sm)
        sql = sm.destination.execute_sql.call_args[0][0]
        assert "SELECT DISTINCT CAST(`event_ts` AS STRING) AS snapshot_val" in sql
        assert "_null_pk_" not in sql
        assert "GROUP BY" not in sql

    def test_watermark_and_flags_combine(self):
        sm = _stub_state_manager()
        self._discover(sm, primary_key=["id"], has_run=True)
        sql = sm.destination.execute_sql.call_args[0][0]
        assert "`event_ts` > TIMESTAMP '2026-01-01 00:00:00'" in sql
        assert "_null_pk_0" in sql

    def test_clean_rows_report_no_offending_columns(self):
        rows = [MagicMock(snapshot_val="2026-01-01", _null_pk_0=0)]
        sm = _stub_state_manager(rows)
        result = self._discover(sm, primary_key=["id"])
        assert result.snapshots == ["2026-01-01"]
        assert result.null_pk_columns == []
        assert result.null_pk_snapshot is None

    def test_offending_snapshot_is_reported(self):
        rows = [
            MagicMock(snapshot_val="2026-01-01", _null_pk_0=0),
            MagicMock(snapshot_val="2026-01-02", _null_pk_0=1),
        ]
        sm = _stub_state_manager(rows)
        result = self._discover(sm, primary_key=["id"])
        assert result.snapshots == ["2026-01-01", "2026-01-02"]
        assert result.null_pk_columns == ["id"]
        assert result.null_pk_snapshot == "2026-01-02"


def _make_runner(probe_rows, primary_key=None):
    """A runner with just enough state for ``_guard_null_primary_keys``."""
    primary_key = primary_key or ["id"]
    builder = _make_builder(primary_key=primary_key)
    dest = builder.destination
    dest.execute_sql.return_value = probe_rows

    runner = object.__new__(HistorizeRunner)
    runner.logger = logging.getLogger(__name__)
    runner.destination = dest
    runner.sql_builder = builder
    runner.config = builder.config
    runner.pipeline_name = "grp__tbl"
    runner.schema = "ds"
    runner.source_table_id = "proj.ds.src"
    return runner, dest


@pytest.mark.unit
class TestGuardNullPrimaryKeysFullRefresh:
    """Full/partial refresh has no earlier source scan, so it probes."""

    def test_clean_source_passes(self):
        runner, dest = _make_runner(probe_rows=[])
        runner._guard_null_primary_keys(None)  # must not raise
        dest.execute_sql.assert_called_once()

    def test_null_key_raises_config_error_naming_the_column(self):
        row = MagicMock(_null_pk_0=1)
        runner, _ = _make_runner(probe_rows=[row])
        with pytest.raises(ValueError) as excinfo:
            runner._guard_null_primary_keys(None)
        message = str(excinfo.value)
        assert "'id'" in message
        assert "proj.ds.src" in message
        assert "historize.filters" in message

    def test_composite_key_names_only_the_null_columns(self):
        row = MagicMock(_null_pk_0=0, _null_pk_1=1)
        runner, _ = _make_runner(probe_rows=[row], primary_key=["tenant", "id"])
        with pytest.raises(ValueError) as excinfo:
            runner._guard_null_primary_keys(None)
        message = str(excinfo.value)
        assert "'id'" in message
        assert "'tenant'" not in message


@pytest.mark.unit
class TestGuardNullPrimaryKeysIncremental:
    """An incremental run reuses snapshot discovery — it must not re-query."""

    def test_clean_discovery_passes_without_a_query(self):
        runner, dest = _make_runner(probe_rows=[])
        runner._guard_null_primary_keys(
            SnapshotDiscovery(snapshots=["2026-01-01"], null_pk_columns=[])
        )
        dest.execute_sql.assert_not_called()

    def test_flagged_discovery_raises_without_a_query(self):
        runner, dest = _make_runner(probe_rows=[])
        discovery = SnapshotDiscovery(
            snapshots=["2026-01-01", "2026-01-02"],
            null_pk_columns=["id"],
            null_pk_snapshot="2026-01-02",
        )
        with pytest.raises(ValueError) as excinfo:
            runner._guard_null_primary_keys(discovery)
        message = str(excinfo.value)
        assert "'id'" in message
        # The offending snapshot is named — discovery knows it for free.
        assert "2026-01-02" in message
        dest.execute_sql.assert_not_called()
