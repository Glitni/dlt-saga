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

from dlt_saga.historize.column_types import parse_type
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
    dest.normalize_type_name.side_effect = lambda t: " ".join(t.upper().split())
    dest.supports_nested_evolution.return_value = False
    dest.dlt_type_to_native.side_effect = lambda t: {"text": "VARCHAR"}.get(
        t, t.upper()
    )

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

    def test_derived_column_is_checked_against_its_declared_type(self):
        runner = _make_runner(
            source=[("id", "INTEGER"), ("snap", "TIMESTAMP"), ("payload", "VARCHAR")],
            target=[
                ("id", "INTEGER"),
                ("payload", "VARCHAR"),
                ("amount", "VARCHAR"),
                *SYSTEM,
            ],
            json_columns={
                "amount": {"column": "payload", "path": "amount", "data_type": "bigint"}
            },
        )
        with pytest.raises(ValueError, match="amount: VARCHAR in the historized"):
            runner._reconcile_target_columns(_value_columns(runner))

    def test_duckdb_spells_its_catalog_aliases_canonically(self):
        from dlt_saga.destinations.duckdb.destination import DuckDBDestination

        dest = object.__new__(DuckDBDestination)
        assert dest.normalize_type_name("TIMESTAMP WITH TIME ZONE") == "TIMESTAMPTZ"
        assert dest.normalize_type_name("timestamptz") == "TIMESTAMPTZ"
        assert dest.normalize_type_name("DECIMAL(12, 2)") == "DECIMAL(12, 2)"

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


class TestNestedFields:
    """A struct column's field drift is taken in place, like column drift,
    on a destination that supports it; a retyped nested scalar is still
    refused, with its path."""

    def _nested_runner(self, source_type, target_type, previous=None, supports=True):
        runner = _make_runner(
            source=[("id", "INTEGER"), ("snap", "TIMESTAMP"), ("meta", source_type)],
            target=[("id", "INTEGER"), ("meta", target_type), *SYSTEM],
            previous=previous,
        )
        dest = runner.destination
        dest.supports_nested_evolution.return_value = supports
        dest.reshape_expression.side_effect = lambda expr, s, t: f"RESHAPE({expr})"
        return runner

    def test_added_field_widens_the_target_and_reshapes_the_read(self):
        runner = self._nested_runner("STRUCT<a STRING, b INT64>", "STRUCT<a STRING>")

        columns = runner._reconcile_target_columns(_value_columns(runner))

        dest = runner.destination
        args = dest.widen_column_type.call_args[0]
        assert args[:3] == ("ds", "tbl_historized", "meta")
        assert args[3] == parse_type("STRUCT<a STRING>")
        assert args[4] == parse_type("STRUCT<a STRING, b INT64>")
        assert runner.sql_builder.reshaped_source_columns == {"meta": 'RESHAPE("meta")'}
        assert columns == ["meta"]
        dest.add_column.assert_not_called()

    def test_reshape_targets_the_merged_type(self):
        runner = self._nested_runner("STRUCT<b INT64, a STRING>", "STRUCT<a STRING>")

        runner._reconcile_target_columns(_value_columns(runner))

        _, source_type, target_type = runner.destination.reshape_expression.call_args[0]
        assert source_type == parse_type("STRUCT<b INT64, a STRING>")
        assert target_type == parse_type("STRUCT<a STRING, b INT64>")

    def test_reorder_alone_reshapes_without_widening(self):
        runner = self._nested_runner(
            "STRUCT<b INT64, a STRING>", "STRUCT<a STRING, b INT64>"
        )

        runner._reconcile_target_columns(_value_columns(runner))

        runner.destination.widen_column_type.assert_not_called()
        assert "meta" in runner.sql_builder.reshaped_source_columns

    def test_removed_field_reshapes_and_warns_the_first_time(self, caplog):
        runner = self._nested_runner(
            "STRUCT<a STRING>",
            "STRUCT<a STRING, b INT64>",
            previous=["meta", "meta.a", "meta.b"],
        )

        with caplog.at_level(logging.DEBUG, logger="dlt_saga.historize.runner"):
            runner._reconcile_target_columns(_value_columns(runner))

        runner.destination.widen_column_type.assert_not_called()
        assert "meta" in runner.sql_builder.reshaped_source_columns
        assert "no longer has nested field(s) ['meta.b']" in caplog.text

    def test_removed_field_only_debug_once_already_absent(self, caplog):
        runner = self._nested_runner(
            "STRUCT<a STRING>", "STRUCT<a STRING, b INT64>", previous=["meta", "meta.a"]
        )

        with caplog.at_level(logging.DEBUG, logger="dlt_saga.historize.runner"):
            runner._reconcile_target_columns(_value_columns(runner))

        assert "no longer has nested field" not in caplog.text
        assert "still absent" in caplog.text

    def test_retyped_nested_scalar_is_refused_with_its_path(self):
        runner = self._nested_runner("STRUCT<a INT64>", "STRUCT<a STRING>")

        with pytest.raises(ValueError) as excinfo:
            runner._reconcile_target_columns(_value_columns(runner))

        assert "meta.a: STRING in the historized table, INT64 in the source" in str(
            excinfo.value
        )
        runner.destination.widen_column_type.assert_not_called()

    def test_key_column_is_never_reconciled(self):
        runner = _make_runner(
            source=[
                ("id", "STRUCT<a STRING, b INT64>"),
                ("snap", "TIMESTAMP"),
                ("v", "VARCHAR"),
            ],
            target=[("id", "STRUCT<a STRING>"), ("v", "VARCHAR"), *SYSTEM],
        )
        runner.destination.supports_nested_evolution.return_value = True

        with pytest.raises(ValueError, match="id: STRUCT<a STRING> in the historized"):
            runner._reconcile_target_columns(_value_columns(runner))

    def test_destination_without_support_refuses_as_before(self):
        runner = self._nested_runner(
            "STRUCT<a STRING, b INT64>", "STRUCT<a STRING>", supports=False
        )

        with pytest.raises(
            ValueError, match="meta: STRUCT<a STRING> in the historized"
        ):
            runner._reconcile_target_columns(_value_columns(runner))

    def test_unparseable_spelling_falls_back_to_refusing(self):
        runner = self._nested_runner("STRUCT<a STRING, b>", "STRUCT<a STRING>")

        with pytest.raises(
            ValueError, match="meta: STRUCT<a STRING> in the historized"
        ):
            runner._reconcile_target_columns(_value_columns(runner))

    def test_recorded_source_columns_include_nested_paths(self):
        import json

        runner = self._nested_runner("ARRAY<STRUCT<a STRING, b STRUCT<x BOOL>>>", "X")

        assert json.loads(runner._recorded_source_columns()) == [
            "id",
            "snap",
            "meta",
            "meta.a",
            "meta.b",
            "meta.b.x",
        ]

    def test_sql_projection_reads_through_the_reshape(self):
        runner = self._nested_runner("STRUCT<a STRING>", "STRUCT<a STRING>")
        builder = runner.sql_builder
        builder.set_reshaped_source_columns({"meta": "STRUCT(x AS a)"})

        assert (
            builder._source_cols_sql(["meta", "v"]) == 'STRUCT(x AS a) AS "meta", "v"'
        )
