"""Unit tests for ``nullable_key_columns`` — composite keys with a NULL part.

A key column is NOT NULL at ingest (dlt's key hints set ``nullable: false``) and
refused by historize when NULL (plain ``=`` never matches, so the previous
version would never close). Declaring the column in ``nullable_key_columns``
makes NULL one more key value on both layers: the ingest writer keeps the column
nullable, and historize compares it through the destination's null-safe
equality and leaves it out of the NULL-key guard.
"""

import logging
from unittest.mock import MagicMock

import dlt
import pytest

from dlt_saga.destinations.bigquery.destination import BigQueryDestination
from dlt_saga.destinations.databricks.destination import DatabricksDestination
from dlt_saga.destinations.duckdb.destination import DuckDBDestination
from dlt_saga.historize.config import HistorizeConfig
from dlt_saga.historize.runner import HistorizeRunner
from dlt_saga.historize.sql import HistorizeSqlBuilder
from dlt_saga.pipelines.base_pipeline import build_target_config
from dlt_saga.pipelines.target.config import TargetConfig
from dlt_saga.pipelines.target.writer import TargetWriter


@pytest.mark.unit
class TestTargetConfig:
    def test_accepts_a_subset_of_the_primary_key(self):
        cfg = TargetConfig(
            write_disposition="append",
            primary_key=["customer_id", "region"],
            nullable_key_columns=["region"],
        )
        assert cfg.nullable_key_columns == ["region"]

    def test_string_is_normalized_to_a_list(self):
        cfg = TargetConfig(
            write_disposition="append",
            primary_key="region",
            nullable_key_columns="region",
        )
        assert cfg.nullable_key_columns == ["region"]

    def test_rejects_a_column_outside_the_primary_key(self):
        with pytest.raises(ValueError, match="subset of primary_key"):
            TargetConfig(
                write_disposition="append",
                primary_key=["customer_id"],
                nullable_key_columns=["region"],
            )

    def test_rejects_without_a_primary_key(self):
        with pytest.raises(ValueError, match="subset of primary_key"):
            TargetConfig(write_disposition="append", nullable_key_columns=["region"])


def _key_nullability(write_disposition, nullable_key_columns=None):
    @dlt.resource(name="t")
    def rows():
        yield {"customer_id": 1, "region": None, "value": 1}

    # Through the pipeline's own builder, so a key the builder forgets to pass
    # along is caught here rather than by a live load.
    config = build_target_config(
        {
            "write_disposition": write_disposition,
            "primary_key": ["customer_id", "region"],
            "nullable_key_columns": nullable_key_columns,
        }
    )
    TargetWriter(config).apply_hints(rows)
    columns = rows.compute_table_schema()["columns"]
    return {name: columns[name].get("nullable") for name in ("customer_id", "region")}


@pytest.mark.unit
class TestWriterKeepsDeclaredKeyColumnsNullable:
    """dlt marks key columns ``nullable: false`` unless the column already carries
    an explicit nullability — which is what the writer sets for declared ones."""

    @pytest.mark.parametrize(
        "write_disposition", ["append", "append+historize", "merge", "merge+historize"]
    )
    def test_key_columns_are_not_null_by_default(self, write_disposition):
        assert _key_nullability(write_disposition) == {
            "customer_id": False,
            "region": False,
        }

    @pytest.mark.parametrize(
        "write_disposition", ["append", "append+historize", "merge", "merge+historize"]
    )
    def test_declared_column_stays_nullable(self, write_disposition):
        assert _key_nullability(write_disposition, ["region"]) == {
            "customer_id": False,
            "region": True,
        }


@pytest.mark.unit
class TestDestinationNullSafeEquals:
    @pytest.mark.parametrize("cls", [BigQueryDestination, DuckDBDestination])
    def test_sql_standard_form(self, cls):
        dest = object.__new__(cls)
        assert dest.null_safe_equals("a.x", "b.x") == "a.x IS NOT DISTINCT FROM b.x"

    def test_databricks_uses_spark_operator(self):
        dest = object.__new__(DatabricksDestination)
        assert dest.null_safe_equals("a.x", "b.x") == "a.x <=> b.x"


@pytest.mark.unit
class TestHistorizeConfig:
    def test_inherits_from_top_level(self):
        cfg = HistorizeConfig.from_dict(
            {},
            top_level_primary_key=["customer_id", "region"],
            top_level_nullable_key_columns=["region"],
        )
        assert cfg.nullable_key_columns == ["region"]
        assert cfg.is_nullable_key("region")
        assert cfg.is_nullable_key("Region")
        assert not cfg.is_nullable_key("customer_id")

    def test_historize_section_overrides_top_level(self):
        cfg = HistorizeConfig.from_dict(
            {"nullable_key_columns": []},
            top_level_primary_key=["customer_id", "region"],
            top_level_nullable_key_columns=["region"],
        )
        assert cfg.nullable_key_columns == []
        assert not cfg.is_nullable_key("region")

    def test_validate_rejects_a_column_outside_the_primary_key(self):
        cfg = HistorizeConfig.from_dict(
            {},
            top_level_primary_key=["customer_id"],
            top_level_nullable_key_columns=["region"],
        )
        with pytest.raises(ValueError, match="subset of primary_key"):
            cfg.validate({"write_disposition": "historize"})

    def test_validate_accepts_a_subset(self):
        cfg = HistorizeConfig.from_dict(
            {},
            top_level_primary_key=["customer_id", "region"],
            top_level_nullable_key_columns=["region"],
        )
        cfg.validate({"write_disposition": "historize"})


def _stub_destination():
    dest = MagicMock()
    dest.quote_identifier.side_effect = lambda s: f"`{s}`"
    dest.escape_string_literal.side_effect = lambda s: s.replace("'", "''")
    dest.hash_expression.side_effect = lambda cols: f"HASH({', '.join(cols)})"
    dest.null_safe_equals.side_effect = lambda a, b: f"{a} NSEQ {b}"
    dest.is_pseudo_column.return_value = False
    return dest


def _make_builder(primary_key, nullable_key_columns=None, **overrides):
    config = HistorizeConfig.from_dict(
        overrides,
        top_level_primary_key=primary_key,
        top_level_nullable_key_columns=nullable_key_columns,
    )
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
    )


@pytest.mark.unit
class TestSqlBuilderKeyJoins:
    def test_plain_equality_without_the_option(self):
        builder = _make_builder(["customer_id", "region"])
        assert builder._pk_join("t", "n") == (
            "t.`customer_id` = n.`customer_id` AND t.`region` = n.`region`"
        )
        builder.destination.null_safe_equals.assert_not_called()

    def test_only_declared_columns_compare_null_safe(self):
        builder = _make_builder(["customer_id", "region"], ["region"])
        assert builder._pk_join("t", "n") == (
            "t.`customer_id` = n.`customer_id` AND t.`region` NSEQ n.`region`"
        )

    def test_every_key_join_in_the_scd2_script_is_null_safe(self):
        builder = _make_builder(
            ["customer_id", "region"], ["region"], track_deletions=True
        )
        sql = builder.build_incremental_sql(
            value_columns=["value"],
            new_snapshots=["2024-01-01", "2024-01-02"],
            last_historized_snapshot="2023-12-31",
        )
        assert "`region` = " not in sql
        assert "NSEQ" in sql

    def test_scd1_merge_is_null_safe(self):
        builder = _make_builder(["customer_id", "region"], ["region"], strategy="scd1")
        sql = builder.build_scd1_sql(
            value_columns=["value"], snapshots=["2024-01-01"], target_is_empty=False
        )
        assert "t.`region` NSEQ n.`region`" in sql
        assert "`region` = " not in sql

    def test_rehistorize_key_membership_is_null_safe(self):
        builder = _make_builder(["customer_id", "region"], ["region"])
        statements = builder.build_rollback_sql(
            effective_from_date="2024-01-01", run_id="r1", dataset="ds"
        )
        sql = " ".join(statements)
        assert "k.`_rk_1` NSEQ `region`" in sql
        assert "k.`_rk_0` = `customer_id`" in sql


@pytest.mark.unit
class TestNullKeyGuardSkipsDeclaredColumns:
    def test_probe_reads_only_guarded_columns(self):
        builder = _make_builder(["customer_id", "region"], ["region"])
        sql = builder.build_null_pk_probe_sql()
        assert "(`customer_id` IS NULL)" in sql
        assert "`region`" not in sql
        assert builder.null_pk_probe_aliases() == ["_null_pk_0"]

    def test_no_probe_when_every_key_column_is_nullable(self):
        builder = _make_builder(["region"], ["region"])
        assert builder.build_null_pk_probe_sql() is None

    def test_full_refresh_guard_passes_a_null_in_a_declared_column(self):
        builder = _make_builder(["customer_id", "region"], ["region"])
        runner = object.__new__(HistorizeRunner)
        runner.logger = logging.getLogger(__name__)
        runner.destination = builder.destination
        runner.sql_builder = builder
        runner.config = builder.config
        runner.pipeline_name = "grp__tbl"
        runner.schema = "ds"
        runner.source_table_id = "proj.ds.src"
        runner.source_relation = "proj.ds.src"
        # The probe matches no row: only customer_id is checked, and it is populated.
        builder.destination.execute_sql.return_value = []

        runner._guard_null_primary_keys(None)

        probe_sql = builder.destination.execute_sql.call_args[0][0]
        assert "`region`" not in probe_sql
