"""Unit tests for Databricks-specific NativeLoadPipeline dispatch.

Covers: empty-target creation DDL, COPY INTO SQL shape,
format options mapping, table_format branching, LOCATION clause,
multi-prefix safety, and per-file row counts.
"""

from unittest.mock import MagicMock

import pytest

from dlt_saga.destinations.base import DerivedColumn, NativeLoadSpec
from dlt_saga.destinations.databricks.destination import DatabricksDestination

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_dest() -> DatabricksDestination:
    dest = MagicMock(spec=DatabricksDestination)
    dest.quote_identifier.side_effect = lambda s: f"`{s}`"
    dest.get_full_table_id.side_effect = lambda ds, tbl: f"cat.{ds}.{tbl}"
    dest.partition_ddl.side_effect = lambda col, **kw: f"PARTITIONED BY ({col})"
    dest.cluster_ddl.side_effect = lambda cols: f"CLUSTER BY ({', '.join(cols)})"
    dest.execute_sql.return_value = []
    dest.execute_sql_with_job.return_value = (
        [("num_affected_rows", 5, "num_inserted_rows", 5)],
        "qid-001",
    )
    return dest


def _make_spec(
    source_uris=None,
    file_type="parquet",
    target_exists=False,
    table_format="delta",
    target_location=None,
    partition_column=None,
    cluster_columns=None,
    write_disposition="append",
    derived_columns=None,
) -> NativeLoadSpec:
    if source_uris is None:
        source_uris = ["gs://bucket/prefix/file1.parquet"]
    if derived_columns is None:
        derived_columns = [
            DerivedColumn(
                "_dlt_ingested_at", "TIMESTAMP '2026-01-01T00:00:00'", "TIMESTAMP"
            ),
            DerivedColumn("_dlt_source_file_name", "_metadata.file_path", "STRING"),
        ]
    spec = NativeLoadSpec(
        target_dataset="ds",
        target_table="tbl",
        source_uris=source_uris,
        file_type=file_type,
        autodetect_schema=True,
        derived_columns=derived_columns,
        target_exists=target_exists,
        partition_column=partition_column,
        cluster_columns=cluster_columns,
        format_options={},
        staging_dataset="ds_staging",
        chunk_label="chunk 1/1",
        write_disposition=write_disposition,
        table_format=table_format,
        target_location=target_location,
    )
    spec._source_uri = "gs://bucket/prefix/"  # type: ignore[attr-defined]
    return spec


# ---------------------------------------------------------------------------
# _native_load_create_empty_target — table format DDL
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestCreateEmptyTargetDDL:
    def test_delta_default(self):
        dest = MagicMock(spec=DatabricksDestination)
        dest.quote_identifier.side_effect = lambda s: f"`{s}`"
        dest.get_full_table_id.return_value = "cat.ds.tbl"
        dest.partition_ddl.return_value = ""
        dest.cluster_ddl.return_value = ""
        spec = _make_spec()

        DatabricksDestination._native_load_create_empty_target(dest, spec, "cat.ds.tbl")

        sql = dest.execute_sql.call_args[0][0]
        assert "USING DELTA" in sql
        assert "USING ICEBERG" not in sql
        assert "TBLPROPERTIES" not in sql

    def test_iceberg_format(self):
        dest = MagicMock(spec=DatabricksDestination)
        dest.quote_identifier.side_effect = lambda s: f"`{s}`"
        dest.get_full_table_id.return_value = "cat.ds.tbl"
        dest.partition_ddl.return_value = "PARTITIONED BY (date_col)"
        spec = _make_spec(table_format="iceberg", partition_column="date_col")

        DatabricksDestination._native_load_create_empty_target(dest, spec, "cat.ds.tbl")

        sql = dest.execute_sql.call_args[0][0]
        assert "USING ICEBERG" in sql
        assert "PARTITIONED BY (date_col)" in sql
        # Iceberg should not have CLUSTER BY
        assert "CLUSTER BY" not in sql

    def test_delta_uniform_tblproperties(self):
        dest = MagicMock(spec=DatabricksDestination)
        dest.quote_identifier.side_effect = lambda s: f"`{s}`"
        dest.get_full_table_id.return_value = "cat.ds.tbl"
        dest.partition_ddl.return_value = ""
        dest.cluster_ddl.return_value = ""
        spec = _make_spec(table_format="delta_uniform")

        DatabricksDestination._native_load_create_empty_target(dest, spec, "cat.ds.tbl")

        sql = dest.execute_sql.call_args[0][0]
        assert "USING DELTA" in sql
        assert "TBLPROPERTIES" in sql
        assert "universalFormat" in sql
        assert "iceberg" in sql

    def test_delta_uniform_with_location_orders_location_before_tblproperties(self):
        dest = MagicMock(spec=DatabricksDestination)
        dest.quote_identifier.side_effect = lambda s: f"`{s}`"
        dest.get_full_table_id.return_value = "cat.ds.tbl"
        dest.partition_ddl.return_value = ""
        dest.cluster_ddl.return_value = ""
        spec = _make_spec(
            table_format="delta_uniform",
            target_location="abfss://lake@a.dfs.core.windows.net/raw/g/tbl/",
        )

        DatabricksDestination._native_load_create_empty_target(dest, spec, "cat.ds.tbl")

        sql = dest.execute_sql.call_args[0][0]
        # Conventional Databricks ordering: LOCATION before TBLPROPERTIES
        assert sql.index("LOCATION") < sql.index("TBLPROPERTIES")

    def test_location_clause_appended(self):
        dest = MagicMock(spec=DatabricksDestination)
        dest.quote_identifier.side_effect = lambda s: f"`{s}`"
        dest.get_full_table_id.return_value = "cat.ds.tbl"
        dest.partition_ddl.return_value = ""
        dest.cluster_ddl.return_value = ""
        loc = "abfss://lake@account.dfs.core.windows.net/raw/g/tbl/"
        spec = _make_spec(target_location=loc)

        DatabricksDestination._native_load_create_empty_target(dest, spec, "cat.ds.tbl")

        sql = dest.execute_sql.call_args[0][0]
        assert "LOCATION" in sql
        assert "abfss://lake@account.dfs.core.windows.net/raw/g/tbl/" in sql

    def test_no_location_when_none(self):
        dest = MagicMock(spec=DatabricksDestination)
        dest.quote_identifier.side_effect = lambda s: f"`{s}`"
        dest.get_full_table_id.return_value = "cat.ds.tbl"
        dest.partition_ddl.return_value = ""
        dest.cluster_ddl.return_value = ""
        spec = _make_spec(target_location=None)

        DatabricksDestination._native_load_create_empty_target(dest, spec, "cat.ds.tbl")

        sql = dest.execute_sql.call_args[0][0]
        assert "LOCATION" not in sql

    def test_partition_and_cluster_warns_uses_cluster(self, caplog):
        import logging

        dest = MagicMock(spec=DatabricksDestination)
        dest.quote_identifier.side_effect = lambda s: f"`{s}`"
        dest.get_full_table_id.return_value = "cat.ds.tbl"
        dest.partition_ddl.return_value = "PARTITIONED BY (date_col)"
        dest.cluster_ddl.return_value = "CLUSTER BY (id)"
        spec = _make_spec(partition_column="date_col", cluster_columns=["id"])

        with caplog.at_level(logging.WARNING):
            DatabricksDestination._native_load_create_empty_target(
                dest, spec, "cat.ds.tbl"
            )

        sql = dest.execute_sql.call_args[0][0]
        assert "CLUSTER BY (id)" in sql
        assert "PARTITIONED BY" not in sql

    def test_framework_columns_in_ddl(self):
        dest = MagicMock(spec=DatabricksDestination)
        dest.quote_identifier.side_effect = lambda s: f"`{s}`"
        dest.get_full_table_id.return_value = "cat.ds.tbl"
        dest.partition_ddl.return_value = ""
        dest.cluster_ddl.return_value = ""
        spec = _make_spec()

        DatabricksDestination._native_load_create_empty_target(dest, spec, "cat.ds.tbl")

        sql = dest.execute_sql.call_args[0][0]
        assert "_dlt_ingested_at" in sql
        assert "_dlt_source_file_name" in sql


# ---------------------------------------------------------------------------
# _build_copy_into — SQL shape and prefix selection
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestBuildCopyInto:
    def test_single_parent_uses_parent_prefix(self):
        dest = MagicMock(spec=DatabricksDestination)
        dest.quote_identifier.side_effect = lambda s: f"`{s}`"
        spec = _make_spec(
            source_uris=[
                "gs://bucket/prefix/day=01/file1.parquet",
                "gs://bucket/prefix/day=01/file2.parquet",
            ]
        )

        sql = DatabricksDestination._build_copy_into(dest, spec, "cat.ds.tbl")

        # All URIs share parent gs://bucket/prefix/day=01/
        assert "gs://bucket/prefix/day=01/" in sql
        # Files listed as basenames
        assert "'file1.parquet'" in sql
        assert "'file2.parquet'" in sql

    def test_multi_parent_falls_back_to_source_uri(self):
        dest = MagicMock(spec=DatabricksDestination)
        dest.quote_identifier.side_effect = lambda s: f"`{s}`"
        spec = _make_spec(
            source_uris=[
                "gs://bucket/prefix/day=01/file1.parquet",
                "gs://bucket/prefix/day=02/file2.parquet",
            ]
        )
        spec._source_uri = "gs://bucket/prefix/"  # type: ignore[attr-defined]

        sql = DatabricksDestination._build_copy_into(dest, spec, "cat.ds.tbl")

        # Falls back to source root
        assert "gs://bucket/prefix/'" in sql
        # Relative paths used
        assert "'day=01/file1.parquet'" in sql
        assert "'day=02/file2.parquet'" in sql

    def test_metadata_file_path_in_select(self):
        dest = MagicMock(spec=DatabricksDestination)
        dest.quote_identifier.side_effect = lambda s: f"`{s}`"
        spec = _make_spec()

        sql = DatabricksDestination._build_copy_into(dest, spec, "cat.ds.tbl")

        assert "_metadata.file_path" in sql

    def test_merge_schema_always_true(self):
        dest = MagicMock(spec=DatabricksDestination)
        dest.quote_identifier.side_effect = lambda s: f"`{s}`"
        spec = _make_spec()
        sql = DatabricksDestination._build_copy_into(dest, spec, "cat.ds.tbl")
        assert "'mergeSchema' = 'true'" in sql
        assert "'mergeSchema' = 'false'" not in sql

    def test_jsonl_format_becomes_json(self):
        dest = MagicMock(spec=DatabricksDestination)
        dest.quote_identifier.side_effect = lambda s: f"`{s}`"
        spec = _make_spec(file_type="jsonl")
        sql = DatabricksDestination._build_copy_into(dest, spec, "cat.ds.tbl")
        assert "FILEFORMAT = JSON" in sql
        assert "FILEFORMAT = JSONL" not in sql

    def test_json_excludes_rescued_data(self):
        dest = MagicMock(spec=DatabricksDestination)
        dest.quote_identifier.side_effect = lambda s: f"`{s}`"
        spec = _make_spec(file_type="jsonl")
        sql = DatabricksDestination._build_copy_into(dest, spec, "cat.ds.tbl")
        assert "EXCEPT (_rescued_data)" in sql

    def test_parquet_no_rescued_data_exclusion(self):
        dest = MagicMock(spec=DatabricksDestination)
        dest.quote_identifier.side_effect = lambda s: f"`{s}`"
        spec = _make_spec(file_type="parquet")
        sql = DatabricksDestination._build_copy_into(dest, spec, "cat.ds.tbl")
        assert "EXCEPT" not in sql


# ---------------------------------------------------------------------------
# _format_databricks_copy_options — format options mapping
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestFormatDatabricksCopyOptions:
    def _call(self, format_options: dict) -> str:
        dest = MagicMock(spec=DatabricksDestination)
        spec = _make_spec()
        spec.format_options = format_options
        return DatabricksDestination._format_databricks_copy_options(dest, spec)

    def test_field_delimiter_mapped(self):
        result = self._call({"field_delimiter": ";"})
        assert "'delimiter' = ';'" in result

    def test_skip_leading_rows_maps_to_header(self):
        result = self._call({"skip_leading_rows": 1})
        assert "'header' = 'true'" in result

    def test_zero_skip_leading_rows_no_header(self):
        result = self._call({"skip_leading_rows": 0})
        assert "header" not in result

    def test_encoding_mapped(self):
        result = self._call({"encoding": "ISO-8859-1"})
        assert "'encoding' = 'ISO-8859-1'" in result

    def test_quote_character_mapped(self):
        result = self._call({"quote_character": '"'})
        assert "'quote' = '\"'" in result

    def test_null_marker_mapped(self):
        result = self._call({"null_marker": "NULL"})
        assert "'nullValue' = 'NULL'" in result

    def test_empty_options_returns_default(self):
        result = self._call({})
        assert "mergeSchema" in result


# ---------------------------------------------------------------------------
# drop_table_external
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestDropTableExternal:
    def test_purge_sql_emitted(self):
        dest = MagicMock(spec=DatabricksDestination)
        dest.get_full_table_id.return_value = "cat.ds.tbl"

        DatabricksDestination.drop_table_external(dest, "ds", "tbl")

        sql = dest.execute_sql.call_args[0][0]
        assert "PURGE" in sql
        assert "DROP TABLE IF EXISTS" in sql


# ---------------------------------------------------------------------------
# Phase 14 — replace write disposition
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestReplaceModeManagedTable:
    def test_replace_managed_emits_create_or_replace(self):
        dest = MagicMock(spec=DatabricksDestination)
        dest.quote_identifier.side_effect = lambda s: f"`{s}`"
        dest.get_full_table_id.return_value = "cat.ds.tbl"
        dest.partition_ddl.return_value = ""
        dest.cluster_ddl.return_value = ""
        spec = _make_spec(write_disposition="replace", target_location=None)

        DatabricksDestination._native_load_create_empty_target(dest, spec, "cat.ds.tbl")

        sql = dest.execute_sql.call_args[0][0]
        assert "CREATE OR REPLACE TABLE" in sql
        assert "CREATE TABLE IF NOT EXISTS" not in sql
        dest.drop_table_external.assert_not_called()

    def test_append_managed_emits_create_if_not_exists(self):
        dest = MagicMock(spec=DatabricksDestination)
        dest.quote_identifier.side_effect = lambda s: f"`{s}`"
        dest.get_full_table_id.return_value = "cat.ds.tbl"
        dest.partition_ddl.return_value = ""
        dest.cluster_ddl.return_value = ""
        spec = _make_spec(write_disposition="append", target_location=None)

        DatabricksDestination._native_load_create_empty_target(dest, spec, "cat.ds.tbl")

        sql = dest.execute_sql.call_args[0][0]
        assert "CREATE TABLE IF NOT EXISTS" in sql
        assert "CREATE OR REPLACE" not in sql


@pytest.mark.unit
class TestReplaceModeExternalTable:
    def test_replace_external_existing_table_truncates(self):
        """Routine replace on an existing external table uses TRUNCATE (no PURGE)."""
        dest = MagicMock(spec=DatabricksDestination)
        dest.quote_identifier.side_effect = lambda s: f"`{s}`"
        dest.get_full_table_id.return_value = "cat.ds.tbl"
        dest.table_exists.return_value = True
        loc = "abfss://lake@account.dfs.core.windows.net/raw/g/tbl/"
        spec = _make_spec(write_disposition="replace", target_location=loc)

        DatabricksDestination._native_load_create_empty_target(dest, spec, "cat.ds.tbl")

        dest.drop_table_external.assert_not_called()
        sql = dest.execute_sql.call_args[0][0]
        assert sql.strip().upper().startswith("TRUNCATE TABLE")
        assert "CREATE" not in sql

    def test_replace_external_first_run_creates(self):
        """First-run replace on external table (no table yet) emits CREATE TABLE IF NOT EXISTS."""
        dest = MagicMock(spec=DatabricksDestination)
        dest.quote_identifier.side_effect = lambda s: f"`{s}`"
        dest.get_full_table_id.return_value = "cat.ds.tbl"
        dest.table_exists.return_value = False
        dest.partition_ddl.return_value = ""
        dest.cluster_ddl.return_value = ""
        loc = "abfss://lake@account.dfs.core.windows.net/raw/g/tbl/"
        spec = _make_spec(write_disposition="replace", target_location=loc)

        DatabricksDestination._native_load_create_empty_target(dest, spec, "cat.ds.tbl")

        dest.drop_table_external.assert_not_called()
        sql = dest.execute_sql.call_args[0][0]
        assert "CREATE TABLE IF NOT EXISTS" in sql
        assert "CREATE OR REPLACE" not in sql
        assert "LOCATION" in sql
