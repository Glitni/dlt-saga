"""Unit tests for BigQuery-specific NativeLoadPipeline dispatch.

Covers: external-table flow, CTAS on first run, INSERT on subsequent runs,
framework cols auto-added, schema evolution guard, per-file row counts.
"""

from unittest.mock import MagicMock

import pytest

from dlt_saga.destinations.base import DerivedColumn, NativeLoadSpec
from dlt_saga.destinations.bigquery.destination import BigQueryDestination

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_dest() -> BigQueryDestination:
    dest = MagicMock(spec=BigQueryDestination)
    dest.quote_identifier.side_effect = lambda s: f"`{s}`"
    dest.get_full_table_id.side_effect = lambda ds, tbl: f"proj.{ds}.{tbl}"
    dest.partition_ddl.side_effect = lambda col, col_type=None: (
        f"PARTITION BY DATE({col})"
    )
    dest.cluster_ddl.side_effect = lambda cols: f"CLUSTER BY ({', '.join(cols)})"
    dest.execute_sql_with_job.return_value = ([], "job-001")
    dest.execute_sql.return_value = []
    dest.list_table_columns.return_value = []
    return dest


def _make_spec(
    file_type="parquet",
    target_exists=False,
    partition_column=None,
    cluster_columns=None,
    derived_columns=None,
    source_uris=None,
    column_hints=None,
    write_disposition="append",
) -> NativeLoadSpec:
    if source_uris is None:
        source_uris = ["gs://bucket/prefix/file1.parquet"]
    if derived_columns is None:
        derived_columns = [
            DerivedColumn(
                "_dlt_ingested_at", "TIMESTAMP '2026-01-01T00:00:00'", "TIMESTAMP"
            ),
            DerivedColumn("_dlt_source_file_name", "_FILE_NAME", "STRING"),
        ]
    return NativeLoadSpec(
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
        column_hints=column_hints or {},
    )


# ---------------------------------------------------------------------------
# native_load_chunk — dispatch flow
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestNativeLoadChunkBQ:
    def test_creates_external_table_then_drops(self):
        dest = _make_dest()
        dest.list_table_columns.return_value = [("col1", "STRING")]
        dest._patch_ext_for_hints.return_value = [("col1", "STRING")]
        dest._native_load_create_target.return_value = (5, "job-001")
        dest._native_load_rowcounts.return_value = {}
        spec = _make_spec(target_exists=False)

        BigQueryDestination.native_load_chunk(dest, spec)

        dest.create_external_table.assert_called_once()
        dest.drop_table.assert_called_once()
        drop_call = dest.drop_table.call_args[0]
        assert drop_call[0] == "ds_staging"

    def test_ctas_on_first_run(self):
        dest = _make_dest()
        dest.list_table_columns.return_value = [("col1", "STRING")]
        dest._patch_ext_for_hints.return_value = [("col1", "STRING")]
        dest._native_load_create_target.return_value = (10, "job-001")
        dest._native_load_rowcounts.return_value = {}
        spec = _make_spec(target_exists=False)

        result = BigQueryDestination.native_load_chunk(dest, spec)

        dest._native_load_create_target.assert_called_once()
        dest._native_load_insert_into_target.assert_not_called()
        assert result.rows_loaded == 10

    def test_insert_on_subsequent_run(self):
        dest = _make_dest()
        dest.list_table_columns.return_value = [("col1", "STRING")]
        dest._patch_ext_for_hints.return_value = [("col1", "STRING")]
        dest._native_load_insert_into_target.return_value = (7, "job-002")
        dest._native_load_rowcounts.return_value = {}
        spec = _make_spec(target_exists=True)

        result = BigQueryDestination.native_load_chunk(dest, spec)

        dest._native_load_insert_into_target.assert_called_once()
        dest._native_load_create_target.assert_not_called()
        assert result.rows_loaded == 7

    def test_framework_cols_auto_added_when_missing(self):
        import sys

        dest = _make_dest()
        dest.config = MagicMock()
        dest.config.job_project_id = "proj"
        dest.config.project_id = "proj"
        dest.config.location = "EU"
        dest.list_table_columns.return_value = [
            ("col1", "STRING")
        ]  # target: missing fw cols
        spec = _make_spec(target_exists=True)

        mock_bq = MagicMock()
        mock_job = MagicMock()
        mock_job.job_id = "job-fw"
        mock_job.num_dml_affected_rows = 3
        mock_bq.Client.return_value.query.return_value = mock_job

        # Patch via sys.modules since google.cloud.bigquery is imported locally in the method
        sys.modules["google.cloud.bigquery"] = mock_bq
        try:
            BigQueryDestination._native_load_insert_into_target(
                dest, spec, "proj.ds_staging.ext", [("col1", "STRING")]
            )
        finally:
            del sys.modules["google.cloud.bigquery"]

        assert dest.add_column.call_count == 2
        added_cols = [c[0][2] for c in dest.add_column.call_args_list]
        assert "_dlt_ingested_at" in added_cols
        assert "_dlt_source_file_name" in added_cols

    def test_ext_table_dropped_on_exception(self):
        dest = _make_dest()
        dest.list_table_columns.return_value = [("col1", "STRING")]
        dest._patch_ext_for_hints.return_value = [("col1", "STRING")]
        dest._native_load_create_target.side_effect = RuntimeError("BQ error")
        spec = _make_spec(target_exists=False)

        with pytest.raises(RuntimeError):
            BigQueryDestination.native_load_chunk(dest, spec)

        dest.drop_table.assert_called_once()

    def test_csv_no_autodetect_first_run_raises(self):
        dest = _make_dest()
        spec = _make_spec(file_type="csv", target_exists=False)
        spec.autodetect_schema = False

        with pytest.raises(ValueError, match="autodetect_schema"):
            BigQueryDestination._native_load_create_target(
                dest, spec, "proj.ds_staging.ext", []
            )


# ---------------------------------------------------------------------------
# _native_load_create_target — CTAS SQL shape
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestNativeLoadCreateTarget:
    def test_select_includes_source_columns(self):
        dest = _make_dest()
        dest.execute_sql.return_value = [MagicMock(cnt=3)]
        spec = _make_spec(target_exists=False)
        ext_cols = [("my_col", "STRING")]

        BigQueryDestination._native_load_create_target(
            dest, spec, "proj.ds_staging.ext", ext_cols
        )

        sql = dest.execute_sql_with_job.call_args[0][0]
        assert "my_col" in sql
        assert "CREATE TABLE" in sql

    def test_partition_ddl_in_ctas(self):
        dest = _make_dest()
        dest.execute_sql.return_value = [MagicMock(cnt=0)]
        spec = _make_spec(partition_column="event_date")
        ext_cols = [("event_date", "DATE")]

        BigQueryDestination._native_load_create_target(
            dest, spec, "proj.ds_staging.ext", ext_cols
        )

        sql = dest.execute_sql_with_job.call_args[0][0]
        assert "PARTITION BY" in sql

    def test_cluster_ddl_in_ctas(self):
        dest = _make_dest()
        dest.execute_sql.return_value = [MagicMock(cnt=0)]
        spec = _make_spec(cluster_columns=["id", "region"])
        ext_cols = [("id", "STRING")]

        BigQueryDestination._native_load_create_target(
            dest, spec, "proj.ds_staging.ext", ext_cols
        )

        sql = dest.execute_sql_with_job.call_args[0][0]
        assert "CLUSTER BY" in sql

    def test_derived_columns_in_select(self):
        dest = _make_dest()
        dest.execute_sql.return_value = [MagicMock(cnt=0)]
        spec = _make_spec()
        ext_cols = [("col1", "STRING")]

        BigQueryDestination._native_load_create_target(
            dest, spec, "proj.ds_staging.ext", ext_cols
        )

        sql = dest.execute_sql_with_job.call_args[0][0]
        assert "_dlt_ingested_at" in sql
        assert "_dlt_source_file_name" in sql or "_FILE_NAME" in sql


# ---------------------------------------------------------------------------
# _native_load_rowcounts
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestNativeLoadRowcountsBQ:
    def test_returns_counts_per_uri(self):
        dest = _make_dest()
        dest.execute_sql.return_value = [
            MagicMock(uri="gs://bucket/prefix/f1.parquet", cnt=100),
            MagicMock(uri="gs://bucket/prefix/f2.parquet", cnt=200),
        ]
        dest.get_full_table_id.return_value = "proj.ds.tbl"
        spec = _make_spec()

        result = BigQueryDestination._native_load_rowcounts(dest, spec)

        assert result["gs://bucket/prefix/f1.parquet"] == 100
        assert result["gs://bucket/prefix/f2.parquet"] == 200

    def test_returns_empty_when_no_file_name_col(self):
        dest = _make_dest()
        spec = _make_spec(
            derived_columns=[
                DerivedColumn(
                    "_dlt_ingested_at", "TIMESTAMP '2026-01-01'", "TIMESTAMP"
                ),
                # no _dlt_source_file_name
            ]
        )
        result = BigQueryDestination._native_load_rowcounts(dest, spec)
        assert result == {}
        dest.execute_sql.assert_not_called()

    def test_returns_empty_on_query_failure(self):
        dest = _make_dest()
        dest.execute_sql.side_effect = RuntimeError("BQ error")
        spec = _make_spec()

        result = BigQueryDestination._native_load_rowcounts(dest, spec)
        assert result == {}

    def test_sql_uses_ingested_at_filter(self):
        dest = _make_dest()
        dest.execute_sql.return_value = []
        spec = _make_spec()

        BigQueryDestination._native_load_rowcounts(dest, spec)

        sql = dest.execute_sql.call_args[0][0]
        assert "WHERE" in sql
        assert "_dlt_ingested_at" in sql


# ---------------------------------------------------------------------------
# Phase 14 — replace write disposition
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestReplaceModeCreateTarget:
    def test_replace_emits_create_or_replace_table(self):
        dest = _make_dest()
        dest.execute_sql.return_value = [MagicMock(cnt=5)]
        spec = _make_spec(target_exists=False, write_disposition="replace")
        ext_cols = [("col1", "STRING")]

        BigQueryDestination._native_load_create_target(
            dest, spec, "proj.ds_staging.ext", ext_cols
        )

        sql = dest.execute_sql_with_job.call_args[0][0]
        assert "CREATE OR REPLACE TABLE" in sql

    def test_append_emits_create_table(self):
        dest = _make_dest()
        dest.execute_sql.return_value = [MagicMock(cnt=0)]
        spec = _make_spec(target_exists=False, write_disposition="append")
        ext_cols = [("col1", "STRING")]

        BigQueryDestination._native_load_create_target(
            dest, spec, "proj.ds_staging.ext", ext_cols
        )

        sql = dest.execute_sql_with_job.call_args[0][0]
        assert "CREATE TABLE" in sql
        assert "CREATE OR REPLACE TABLE" not in sql


@pytest.mark.unit
class TestSchemaEvolutionAlwaysOn:
    def test_new_column_auto_added_without_schema_evolution_flag(self):
        """New columns are always added — no schema_evolution field needed."""
        import sys

        dest = _make_dest()
        dest.config = MagicMock()
        dest.config.job_project_id = "proj"
        dest.config.project_id = "proj"
        dest.config.location = "EU"
        # Target has col1; source also has new_col (not in target)
        dest.list_table_columns.return_value = [("col1", "STRING")]
        spec = _make_spec(target_exists=True)

        mock_bq = MagicMock()
        mock_job = MagicMock()
        mock_job.job_id = "job-se"
        mock_job.num_dml_affected_rows = 1
        mock_bq.Client.return_value.query.return_value = mock_job

        sys.modules["google.cloud.bigquery"] = mock_bq
        try:
            BigQueryDestination._native_load_insert_into_target(
                dest,
                spec,
                "proj.ds_staging.ext",
                [("col1", "STRING"), ("new_col", "INT64")],
            )
        finally:
            del sys.modules["google.cloud.bigquery"]

        # add_column called for framework cols + new data col
        added = [c[0][2] for c in dest.add_column.call_args_list]
        assert "new_col" in added
