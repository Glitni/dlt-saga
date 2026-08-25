"""Unit tests for the NULL-primary-key guard on historize runs.

The primary key is the SCD2 identity and SQL equality is never true for NULL:
change detection partitions by the PK (where NULL groups with NULL, so change
rows *are* produced), but the MERGE closing the previous open row
(``ON t.pk = n.pk``) and the deletion-marker joins never match. Each run
therefore leaves another permanently-open row for the same NULL key. The guard
refuses the run instead — before a full refresh drops the existing history.
"""

import logging
from unittest.mock import MagicMock

import pytest

from dlt_saga.historize.config import HistorizeConfig
from dlt_saga.historize.runner import HistorizeRunner
from dlt_saga.historize.sql import HistorizeSqlBuilder


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

    def test_snapshot_filter_scopes_the_probe(self):
        builder = _make_builder()
        snapshot_filter = builder.snapshot_in_clause("`snap`", ["2026-01-01"])
        sql = builder.build_null_pk_probe_sql(snapshot_filter)
        assert "`snap` IN (TIMESTAMP '2026-01-01')" in sql

    def test_no_primary_key_returns_none(self):
        builder = _make_builder()
        builder.primary_key = []
        assert builder.build_null_pk_probe_sql() is None


@pytest.mark.unit
class TestSnapshotInClause:
    def test_escapes_literals(self):
        clause = _make_builder().snapshot_in_clause("`snap`", ["a'b"])
        assert clause == "`snap` IN (TIMESTAMP 'a''b')"

    def test_multiple_snapshots(self):
        clause = _make_builder().snapshot_in_clause("`snap`", ["s1", "s2"])
        assert clause == "`snap` IN (TIMESTAMP 's1', TIMESTAMP 's2')"


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
class TestGuardNullPrimaryKeys:
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

    def test_incremental_probe_is_scoped_to_the_new_snapshots(self):
        runner, dest = _make_runner(probe_rows=[])
        runner._guard_null_primary_keys(["2026-01-01", "2026-01-02"])
        probe_sql = dest.execute_sql.call_args[0][0]
        assert "TIMESTAMP '2026-01-01'" in probe_sql
        assert "TIMESTAMP '2026-01-02'" in probe_sql

    def test_full_refresh_probe_is_unscoped(self):
        runner, dest = _make_runner(probe_rows=[])
        runner._guard_null_primary_keys(None)
        probe_sql = dest.execute_sql.call_args[0][0]
        assert "IN (TIMESTAMP" not in probe_sql
