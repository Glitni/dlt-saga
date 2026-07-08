"""Unit tests for destinations/base.py default implementations."""

from unittest.mock import MagicMock

import pytest

from dlt_saga.destinations.base import (
    DerivedColumn,
    Destination,
    MaterializationHints,
    NativeLoadResult,
    NativeLoadSpec,
)


class _ConcreteDestination(Destination):
    """Minimal concrete subclass used to exercise Destination's default methods."""

    def __init__(self):
        self.config = MagicMock()
        self._sql_executed: list[str] = []

    def create_dlt_destination(self):
        return None

    def apply_hints(self, resource, **hints):
        return resource

    def get_access_manager(self):
        return None

    def supports_access_management(self):
        return False

    def supports_partitioning(self):
        return False

    def supports_clustering(self):
        return False

    def get_full_table_id(self, dataset, table):
        return f"{dataset}.{table}"

    def execute_sql(self, sql, schema_name=None):
        self._sql_executed.append(sql)
        return []


@pytest.mark.unit
class TestEscapeStringLiteral:
    """Base escaping is standard SQL (double the quote). DuckDB inherits it;
    BigQuery/Databricks override with the backslash helper (tested below).
    """

    def test_single_quote_doubled(self):
        assert _ConcreteDestination().escape_string_literal("it's") == "it''s"

    def test_backslash_left_literal(self):
        # Standard SQL treats backslash as a literal character.
        assert _ConcreteDestination().escape_string_literal("a\\b") == "a\\b"

    def test_control_chars_left_as_is(self):
        # Newlines/tabs are valid inside a single-quoted standard-SQL literal.
        assert (
            _ConcreteDestination().escape_string_literal("a\nb\rc\td") == "a\nb\rc\td"
        )

    def test_nul_dropped(self):
        assert _ConcreteDestination().escape_string_literal("a\x00b") == "ab"

    def test_plain_value_unchanged(self):
        assert _ConcreteDestination().escape_string_literal("dlt_google_sheets") == (
            "dlt_google_sheets"
        )


@pytest.mark.unit
class TestBackslashEscapeHelper:
    """The C-style helper BigQuery/Databricks use — GoogleSQL/Spark reject '',
    so the single quote becomes \\' and control chars become escape sequences.
    """

    def test_single_quote_backslash_escaped(self):
        assert _ConcreteDestination()._backslash_escape_string_literal("it's") == (
            "it\\'s"
        )

    def test_backslash_escaped_first(self):
        assert _ConcreteDestination()._backslash_escape_string_literal("a\\b") == (
            "a\\\\b"
        )

    def test_control_chars_become_escape_sequences(self):
        assert _ConcreteDestination()._backslash_escape_string_literal(
            "a\nb\rc\td"
        ) == ("a\\nb\\rc\\td")

    def test_nul_dropped(self):
        assert _ConcreteDestination()._backslash_escape_string_literal("a\x00b") == (
            "ab"
        )


@pytest.mark.unit
class TestDuckDBEscapeStringLiteral:
    def _duck(self):
        from dlt_saga.destinations.duckdb.config import DuckDBDestinationConfig
        from dlt_saga.destinations.duckdb.destination import DuckDBDestination

        return DuckDBDestination(
            DuckDBDestinationConfig(
                project_id="t", location="local", database_path=":memory:"
            )
        )

    def test_single_quote_doubled(self):
        assert self._duck().escape_string_literal("it's") == "it''s"

    def test_backslash_left_literal(self):
        # DuckDB treats backslash literally; escaping it would store `\\` text.
        assert self._duck().escape_string_literal("a\\b") == "a\\b"

    def test_nul_dropped(self):
        assert self._duck().escape_string_literal("a\x00b") == "ab"


@pytest.mark.unit
class TestDataclasses:
    def test_derived_column(self):
        col = DerivedColumn(name="my_col", sql_expr="LOWER(name)", sql_type="STRING")
        assert col.name == "my_col"
        assert col.sql_expr == "LOWER(name)"
        assert col.sql_type == "STRING"

    def test_native_load_spec_defaults(self):
        spec = NativeLoadSpec(
            target_dataset="ds",
            target_table="tbl",
            source_uris=["gs://b/f.parquet"],
            file_type="parquet",
            autodetect_schema=True,
            derived_columns=[],
            target_exists=False,
            partition_column=None,
            cluster_columns=None,
            format_options={},
            staging_dataset="ds_staging",
            chunk_label="chunk_0",
        )
        assert spec.write_disposition == "append"
        assert spec.column_hints == {}
        assert spec.target_location is None
        assert spec.table_format == "delta"

    def test_native_load_result_defaults(self):
        result = NativeLoadResult(rows_loaded=100, job_id="job_123")
        assert result.rows_loaded == 100
        assert result.job_id == "job_123"
        assert result.rows_by_uri == {}

    def test_materialization_hints_defaults(self):
        hints = MaterializationHints()
        assert hints.partition_column is None
        assert hints.cluster_columns is None
        assert hints.table_format == "native"
        assert hints.table_name == ""
        assert hints.schema == ""
        assert hints.source_database == ""
        assert hints.source_schema == ""
        assert hints.source_table == ""
        assert hints.valid_from_column == "_dlt_valid_from"
        assert hints.valid_to_column == "_dlt_valid_to"
        assert hints.is_deleted_column == "_dlt_is_deleted"

    def test_materialization_hints_explicit_fields(self):
        hints = MaterializationHints(
            partition_column="snapshot_date",
            cluster_columns=["id"],
            table_format="iceberg",
            table_name="orders_historized",
            schema="dlt_sales",
            source_database="proj",
            source_schema="dlt_sales",
            source_table="orders",
            valid_from_column="valid_from",
            valid_to_column="valid_to",
            is_deleted_column="is_deleted",
        )
        assert hints.partition_column == "snapshot_date"
        assert hints.cluster_columns == ["id"]
        assert hints.table_format == "iceberg"
        assert hints.table_name == "orders_historized"
        assert hints.schema == "dlt_sales"
        assert hints.source_database == "proj"
        assert hints.source_schema == "dlt_sales"
        assert hints.source_table == "orders"
        assert hints.valid_from_column == "valid_from"
        assert hints.valid_to_column == "valid_to"
        assert hints.is_deleted_column == "is_deleted"


@pytest.mark.unit
class TestBuildHistorizeCreateTableSqlDefault:
    """The base default emits a vanilla CTAS without partition/cluster DDL.

    The base's ``partition_ddl`` / ``cluster_ddl`` return empty strings —
    destinations opt in by overriding. So even when the hints carry a
    partition column, the default impl's output excludes it.
    """

    def test_emits_plain_ctas(self):
        sql = _ConcreteDestination().build_historize_create_table_sql(
            "CREATE TABLE IF NOT EXISTS",
            "proj.ds.tgt",
            "SELECT 1 AS x WHERE FALSE",
            MaterializationHints(),
        )
        assert (
            sql
            == "CREATE TABLE IF NOT EXISTS proj.ds.tgt\nAS\nSELECT 1 AS x WHERE FALSE"
        )

    def test_ignores_partition_when_dialect_returns_empty(self):
        """Hints carry the partition column, but the base ``partition_ddl``
        returns ``""`` for unsupported destinations — that's a signal to
        skip the clause entirely."""
        sql = _ConcreteDestination().build_historize_create_table_sql(
            "CREATE TABLE IF NOT EXISTS",
            "proj.ds.tgt",
            "SELECT 1",
            MaterializationHints(partition_column="snapshot_date"),
        )
        assert "PARTITION" not in sql

    def test_create_or_replace_clause_passes_through(self):
        sql = _ConcreteDestination().build_historize_create_table_sql(
            "CREATE OR REPLACE TABLE",
            "proj.ds.tgt",
            "SELECT 1",
            MaterializationHints(),
        )
        assert sql.startswith("CREATE OR REPLACE TABLE proj.ds.tgt")


class _PartitionClusterDestination(_ConcreteDestination):
    """Concrete destination whose dialect supports both partition and cluster."""

    def partition_ddl(self, column, col_type=None):
        return f"PARTITION BY {column}"

    def cluster_ddl(self, columns):
        return f"CLUSTER BY {', '.join(columns)}"


@pytest.mark.unit
class TestPartitionClusterDdlDefault:
    def test_returns_empty_when_dialect_unsupported(self):
        # Base partition_ddl / cluster_ddl return "".
        assert _ConcreteDestination().partition_cluster_ddl("dt", ["id"]) == ""

    def test_combines_both_when_supported(self):
        result = _PartitionClusterDestination().partition_cluster_ddl(
            "dt", ["id", "ts"]
        )
        assert result == "PARTITION BY dt\nCLUSTER BY id, ts"

    def test_partition_only(self):
        assert (
            _PartitionClusterDestination().partition_cluster_ddl("dt", None)
            == "PARTITION BY dt"
        )

    def test_cluster_only(self):
        assert (
            _PartitionClusterDestination().partition_cluster_ddl(None, ["id"])
            == "CLUSTER BY id"
        )


@pytest.mark.unit
class TestLifecycleMethods:
    def test_connect_is_noop(self):
        _ConcreteDestination().connect()

    def test_close_is_noop(self):
        _ConcreteDestination().close()

    def test_create_dlt_staging_returns_none(self):
        assert _ConcreteDestination().create_dlt_staging() is None

    def test_run_pipeline_delegates_to_pipeline_run(self):
        dest = _ConcreteDestination()
        pipeline = MagicMock()
        pipeline.run.return_value = "load_info"
        assert dest.run_pipeline(pipeline, [1, 2]) == "load_info"
        pipeline.run.assert_called_once_with([1, 2])

    def test_save_load_info_with_no_pipeline_is_noop(self):
        dest = _ConcreteDestination()
        dest.save_load_info("ds", [{"k": "v"}], pipeline=None)
        assert dest._sql_executed == []

    def test_get_client_pool_returns_none(self):
        assert _ConcreteDestination().get_client_pool() is None

    def test_prepare_for_execution_is_noop(self):
        _ConcreteDestination.prepare_for_execution([])

    def test_reset_destination_state_is_noop(self):
        _ConcreteDestination().reset_destination_state("pipeline", "table")

    def test_supports_transactions_returns_false(self):
        assert _ConcreteDestination().supports_transactions() is False


@pytest.mark.unit
class TestSqlDialectDefaults:
    def test_quote_identifier_uses_ansi_double_quotes(self):
        assert _ConcreteDestination().quote_identifier("my_table") == '"my_table"'

    def test_quote_identifier_doubles_embedded_double_quote(self):
        assert _ConcreteDestination().quote_identifier('a"b') == '"a""b"'

    def test_hash_expression_has_no_portable_default(self):
        # No SQL-standard hashing function — each destination must supply one.
        with pytest.raises(NotImplementedError):
            _ConcreteDestination().hash_expression(["col_a", "col_b"])

    def test_partition_ddl_returns_empty_string(self):
        assert _ConcreteDestination().partition_ddl("date_col") == ""

    def test_cluster_ddl_returns_empty_string(self):
        assert _ConcreteDestination().cluster_ddl(["col_a"]) == ""

    @pytest.mark.parametrize(
        "logical, expected",
        [
            ("string", "VARCHAR"),
            ("int64", "BIGINT"),
            ("bool", "BOOLEAN"),
            ("timestamp", "TIMESTAMP"),
        ],
    )
    def test_type_name_maps_logical_types_to_standard_sql(self, logical, expected):
        assert _ConcreteDestination().type_name(logical) == expected

    def test_type_name_unknown_uppercases_fallback(self):
        assert _ConcreteDestination().type_name("float64") == "FLOAT64"

    def test_cast_to_string(self):
        assert (
            _ConcreteDestination().cast_to_string("my_col") == "CAST(my_col AS VARCHAR)"
        )

    def test_json_type_name(self):
        assert _ConcreteDestination().json_type_name() == "JSON"

    def test_parse_json_expression_uses_standard_cast(self):
        assert (
            _ConcreteDestination().parse_json_expression("col") == "CAST(col AS JSON)"
        )

    def test_current_timestamp_expression(self):
        assert (
            _ConcreteDestination().current_timestamp_expression()
            == "CURRENT_TIMESTAMP()"
        )

    def test_timestamp_n_days_ago_includes_days(self):
        result = _ConcreteDestination().timestamp_n_days_ago(7)
        assert "7" in result
        assert "CURRENT_TIMESTAMP()" in result


@pytest.mark.unit
class TestSchemaAndViewHelpers:
    def test_ensure_schema_exists_executes_create_schema(self):
        dest = _ConcreteDestination()
        dest.ensure_schema_exists("my_schema")
        assert any("CREATE SCHEMA IF NOT EXISTS" in s for s in dest._sql_executed)
        assert any('"my_schema"' in s for s in dest._sql_executed)

    def test_create_or_replace_view_executes_ddl(self):
        dest = _ConcreteDestination()
        dest.create_or_replace_view("ds", "my_view", "SELECT 1")
        assert any("CREATE OR REPLACE VIEW" in s for s in dest._sql_executed)
        assert any("my_view" in s for s in dest._sql_executed)
        assert any("SELECT 1" in s for s in dest._sql_executed)


@pytest.mark.unit
class TestNativeLoadContractDefaults:
    def test_supports_native_load_returns_false(self):
        assert _ConcreteDestination().supports_native_load() is False

    def test_supported_native_load_uri_schemes_returns_empty_set(self):
        assert _ConcreteDestination().supported_native_load_uri_schemes() == set()

    def test_dlt_type_to_native_uppercases_unknown(self):
        assert _ConcreteDestination().dlt_type_to_native("timestamp") == "TIMESTAMP"
        assert _ConcreteDestination().dlt_type_to_native("FLOAT64") == "FLOAT64"

    def test_list_tables_by_pattern_returns_empty_list(self):
        assert _ConcreteDestination().list_tables_by_pattern("ds", "my_%") == []

    def test_native_load_chunk_raises_not_implemented(self):
        with pytest.raises(NotImplementedError):
            _ConcreteDestination().native_load_chunk(MagicMock())

    def test_native_load_file_name_expr_raises_not_implemented(self):
        with pytest.raises(NotImplementedError):
            _ConcreteDestination().native_load_file_name_expr()

    def test_table_exists_raises_not_implemented(self):
        with pytest.raises(NotImplementedError):
            _ConcreteDestination().table_exists("ds", "tbl")

    def test_drop_table_raises_not_implemented(self):
        with pytest.raises(NotImplementedError):
            _ConcreteDestination().drop_table("ds", "tbl")

    def test_list_table_columns_raises_not_implemented(self):
        with pytest.raises(NotImplementedError):
            _ConcreteDestination().list_table_columns("ds", "tbl")

    def test_add_column_raises_not_implemented(self):
        with pytest.raises(NotImplementedError):
            _ConcreteDestination().add_column("ds", "tbl", "col", "STRING")

    def test_execute_sql_with_job_raises_not_implemented(self):
        with pytest.raises(NotImplementedError):
            _ConcreteDestination().execute_sql_with_job("SELECT 1")
