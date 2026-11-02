"""Unit tests for historize schema evolution.

``HistorizeRunner._reconcile_target_columns`` brings an existing historized
table in line with the source before a non-full run writes into it: added
columns are created on the target, dropped columns are read as typed NULLs,
and a changed type is refused. The log records each run's source columns so a
dropped column is warned about once, not on every run.
"""

import logging
from unittest.mock import MagicMock

import pytest

from dlt_saga.historize.config import HistorizeConfig
from dlt_saga.historize.runner import HistorizeRunner
from dlt_saga.historize.sql import HistorizeSqlBuilder
from dlt_saga.historize.state import HistorizeLogEntry, HistorizeStateManager

pytestmark = pytest.mark.unit


def _make_runner(source, target, previous=None, **config_kwargs) -> HistorizeRunner:
    """``source``/``target``: ``[(name, type)]`` as the catalog lists them."""
    dest = MagicMock()
    dest.list_table_columns.return_value = list(target)
    dest.quote_identifier.side_effect = lambda n: f'"{n}"'
    dest.is_pseudo_column.return_value = False

    runner = object.__new__(HistorizeRunner)
    runner.logger = logging.getLogger("dlt_saga.historize.runner")
    runner.pipeline_name = "grp__tbl"
    runner.destination = dest
    runner.database = "proj"
    runner.schema = "ds"
    runner.target_schema = "ds"
    runner.target_table_name = "tbl_historized"
    runner.source_table_id = "proj.ds.src"
    runner.target_table_id = "proj.ds.tbl_historized"
    runner.config = HistorizeConfig(
        primary_key=["id"], snapshot_column="snap", **config_kwargs
    )
    runner._source_columns_cache = [name for name, _ in source]
    runner._source_column_types = {name.lower(): t for name, t in source}
    runner.state_manager = MagicMock()
    runner.state_manager.get_last_source_columns.return_value = previous
    runner.sql_builder = HistorizeSqlBuilder(
        config=runner.config,
        destination=dest,
        source_table_id=runner.source_table_id,
        target_table_id=runner.target_table_id,
        primary_key=["id"],
    )
    return runner


def _value_columns(runner):
    return runner.sql_builder.select_value_columns(runner._source_columns())


SYSTEM = [
    ("_dlt_valid_from", "TIMESTAMP"),
    ("_dlt_valid_to", "TIMESTAMP"),
    ("_dlt_is_deleted", "BOOLEAN"),
]


class TestAddedColumns:
    def test_added_with_the_source_type(self):
        runner = _make_runner(
            source=[
                ("id", "INTEGER"),
                ("snap", "TIMESTAMP"),
                ("a", "VARCHAR"),
                ("b", "DECIMAL(12,2)"),
            ],
            target=[("id", "INTEGER"), ("a", "VARCHAR"), *SYSTEM],
        )
        value_columns = runner._reconcile_target_columns(_value_columns(runner))

        runner.destination.add_column.assert_called_once_with(
            "ds", "tbl_historized", "b", "DECIMAL(12,2)"
        )
        assert value_columns == ["a", "b"]
        assert runner.sql_builder._absent_source_columns == {}

    def test_matching_is_case_insensitive(self):
        runner = _make_runner(
            source=[("id", "INTEGER"), ("snap", "TIMESTAMP"), ("Amount", "INTEGER")],
            target=[("ID", "INTEGER"), ("amount", "INTEGER"), *SYSTEM],
        )
        runner._reconcile_target_columns(_value_columns(runner))
        runner.destination.add_column.assert_not_called()


class TestDroppedColumns:
    def test_read_as_typed_null_and_appended_to_value_columns(self):
        runner = _make_runner(
            source=[("id", "INTEGER"), ("snap", "TIMESTAMP"), ("a", "VARCHAR")],
            target=[("id", "INTEGER"), ("a", "VARCHAR"), ("gone", "DOUBLE"), *SYSTEM],
        )
        value_columns = runner._reconcile_target_columns(_value_columns(runner))

        assert value_columns == ["a", "gone"]
        assert runner.sql_builder._absent_source_columns == {"gone": "DOUBLE"}
        runner.destination.add_column.assert_not_called()

    def test_system_and_key_columns_are_never_dropped_columns(self):
        runner = _make_runner(
            source=[("id", "INTEGER"), ("snap", "TIMESTAMP"), ("a", "VARCHAR")],
            target=[("id", "INTEGER"), ("a", "VARCHAR"), *SYSTEM],
        )
        assert runner._reconcile_target_columns(_value_columns(runner)) == ["a"]
        assert runner.sql_builder._absent_source_columns == {}

    def test_warns_when_the_last_run_still_had_the_column(self, caplog):
        runner = _make_runner(
            source=[("id", "INTEGER"), ("snap", "TIMESTAMP"), ("a", "VARCHAR")],
            target=[("id", "INTEGER"), ("a", "VARCHAR"), ("gone", "DOUBLE"), *SYSTEM],
            previous=["id", "snap", "a", "gone"],
        )
        with caplog.at_level(logging.DEBUG, logger="dlt_saga.historize.runner"):
            runner._reconcile_target_columns(_value_columns(runner))
        warnings = [r for r in caplog.records if r.levelno == logging.WARNING]
        assert len(warnings) == 1 and "gone" in warnings[0].getMessage()

    def test_warns_when_the_last_run_is_unknown(self, caplog):
        runner = _make_runner(
            source=[("id", "INTEGER"), ("snap", "TIMESTAMP"), ("a", "VARCHAR")],
            target=[("id", "INTEGER"), ("a", "VARCHAR"), ("gone", "DOUBLE"), *SYSTEM],
            previous=None,
        )
        with caplog.at_level(logging.WARNING):
            runner._reconcile_target_columns(_value_columns(runner))
        assert any("gone" in r.getMessage() for r in caplog.records)

    def test_only_debug_once_already_absent_last_run(self, caplog):
        runner = _make_runner(
            source=[("id", "INTEGER"), ("snap", "TIMESTAMP"), ("a", "VARCHAR")],
            target=[("id", "INTEGER"), ("a", "VARCHAR"), ("gone", "DOUBLE"), *SYSTEM],
            previous=["id", "snap", "a"],
        )
        with caplog.at_level(logging.DEBUG, logger="dlt_saga.historize.runner"):
            runner._reconcile_target_columns(_value_columns(runner))
        assert not [r for r in caplog.records if r.levelno == logging.WARNING]
        assert any(
            r.levelno == logging.DEBUG and "gone" in r.getMessage()
            for r in caplog.records
        )


class TestTypeChange:
    def test_refused_naming_column_and_both_types(self):
        runner = _make_runner(
            source=[
                ("id", "INTEGER"),
                ("snap", "TIMESTAMP"),
                ("a", "VARCHAR"),
                ("b", "INTEGER"),
            ],
            target=[("id", "INTEGER"), ("a", "VARCHAR"), ("b", "VARCHAR"), *SYSTEM],
        )
        with pytest.raises(ValueError) as excinfo:
            runner._reconcile_target_columns(_value_columns(runner))
        msg = str(excinfo.value)
        assert "b: VARCHAR in the historized table, INTEGER in the source" in msg
        assert "--full-refresh" in msg
        runner.destination.add_column.assert_not_called()

    def test_primary_key_type_is_checked_too(self):
        runner = _make_runner(
            source=[("id", "VARCHAR"), ("snap", "TIMESTAMP"), ("a", "VARCHAR")],
            target=[("id", "INTEGER"), ("a", "VARCHAR"), *SYSTEM],
        )
        with pytest.raises(ValueError, match="id: INTEGER in the historized table"):
            runner._reconcile_target_columns(_value_columns(runner))

    def test_spelling_differences_are_not_a_change(self):
        runner = _make_runner(
            source=[("id", "INTEGER"), ("snap", "TIMESTAMP"), ("a", "decimal(12, 2)")],
            target=[("id", "INTEGER"), ("a", "DECIMAL(12, 2)"), *SYSTEM],
        )
        runner._reconcile_target_columns(_value_columns(runner))  # must not raise


class TestSqlProjection:
    def _builder(self, strategy="scd2"):
        dest = MagicMock()
        dest.quote_identifier.side_effect = lambda n: f'"{n}"'
        dest.is_pseudo_column.return_value = False
        dest.hash_expression.side_effect = lambda cols: f"HASH({', '.join(cols)})"
        dest.escape_string_literal.side_effect = lambda s: s
        dest.type_name.side_effect = lambda t: t.upper()
        config = HistorizeConfig(
            primary_key=["id"],
            snapshot_column="snap",
            strategy=strategy,
            track_deletions=False,
        )
        return HistorizeSqlBuilder(
            config=config,
            destination=dest,
            source_table_id="src",
            target_table_id="tgt",
            primary_key=["id"],
        )

    def test_incremental_reads_absent_column_as_typed_null(self):
        builder = self._builder()
        builder.set_absent_source_columns({"gone": "DOUBLE"})
        sql = builder.build_incremental_sql(
            value_columns=["a", "gone"], new_snapshots=["2026-01-02 00:00:00"]
        )
        assert 'CAST(NULL AS DOUBLE) AS "gone"' in sql
        # Only the source read is rewritten; the target-side reads keep the name.
        assert sql.count('CAST(NULL AS DOUBLE) AS "gone"') == 1

    def test_scd1_reads_absent_column_as_typed_null(self):
        builder = self._builder("scd1")
        builder.set_absent_source_columns({"gone": "DOUBLE"})
        sql = builder.build_scd1_sql(
            value_columns=["a", "gone"], snapshots=["2026-01-02 00:00:00"]
        )
        assert 'CAST(NULL AS DOUBLE) AS "gone"' in sql

    def test_unset_leaves_sql_unchanged(self):
        builder = self._builder()
        sql = builder.build_incremental_sql(
            value_columns=["a"], new_snapshots=["2026-01-02 00:00:00"]
        )
        assert "CAST(NULL" not in sql


def _entry(**overrides):
    fields = dict(
        pipeline_name="p",
        source_table="s",
        target_table="t",
        snapshot_value="2026-01-01",
        new_or_changed_rows=1,
        deleted_rows=0,
        config_fingerprint="fp",
        is_full_reprocess=False,
        started_at=None,
        finished_at=None,
        status="completed",
        source_columns='["id", "a"]',
    )
    fields.update(overrides)
    return HistorizeLogEntry(**fields)


def _manager(existing_columns):
    dest = MagicMock()
    dest.get_full_table_id.return_value = "cat.schema._saga_historize_log"
    dest.escape_string_literal.side_effect = lambda s: s
    dest.type_name.side_effect = lambda t: t.upper()
    dest.list_table_columns.return_value = [(c, "STRING") for c in existing_columns]
    return HistorizeStateManager(dest, "db", "schema")


class TestLogSourceColumns:
    def test_insert_carries_source_columns(self):
        mgr = _manager(["pipeline_name", "source_columns"])
        mgr.write_log_entry(_entry())
        sql = mgr.destination.execute_sql.call_args.args[0]
        assert "source_columns" in sql and '["id", "a"]' in sql

    def test_log_table_without_the_column_is_migrated_on_first_write(self):
        mgr = _manager(["pipeline_name", "status"])
        mgr.destination.execute_sql.side_effect = [RuntimeError("no column"), None]

        mgr.write_log_entry(_entry())

        mgr.destination.add_column.assert_called_once_with(
            "schema", "_saga_historize_log", "source_columns", "STRING"
        )
        assert mgr.destination.execute_sql.call_count == 2

    def test_other_write_failures_propagate_without_retry(self):
        mgr = _manager(["pipeline_name", "source_columns"])
        mgr.destination.execute_sql.side_effect = RuntimeError("PERMISSION_DENIED")

        with pytest.raises(RuntimeError, match="PERMISSION_DENIED"):
            mgr.write_log_entry(_entry())
        mgr.destination.add_column.assert_not_called()
        assert mgr.destination.execute_sql.call_count == 1

    def test_last_source_columns_parsed(self):
        mgr = _manager(["source_columns"])
        row = MagicMock()
        row.source_columns = '["id", "a"]'
        mgr.destination.execute_sql.return_value = [row]
        assert mgr.get_last_source_columns("p") == ["id", "a"]

    @pytest.mark.parametrize("stored", [None, "", "not json"])
    def test_last_source_columns_unknown(self, stored):
        mgr = _manager(["source_columns"])
        row = MagicMock()
        row.source_columns = stored
        mgr.destination.execute_sql.return_value = [row]
        assert mgr.get_last_source_columns("p") is None

    def test_last_source_columns_never_raises(self):
        mgr = _manager([])
        mgr.destination.execute_sql.side_effect = RuntimeError("Unrecognized name")
        assert mgr.get_last_source_columns("p") is None
