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
    # `config` isn't a spec attribute (it's set in __init__), so attach a plain
    # MagicMock and seed the fields read during DDL building.
    dest.config = MagicMock()
    dest.config.partition_expiration_days = None
    dest.config.table_format = "native"
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
    omni_location=None,
    source_connection=None,
    external_schema=None,
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
        target_schema="ds",
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
        omni_location=omni_location,
        source_connection=source_connection,
        external_schema=external_schema,
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
        from unittest.mock import patch

        dest = _make_dest()
        dest.config = MagicMock()
        dest.config.job_project_id = "proj"
        dest.config.project_id = "proj"
        dest.config.location = "EU"
        dest.list_table_columns.return_value = [
            ("col1", "STRING")
        ]  # target: missing fw cols
        spec = _make_spec(target_exists=True)

        mock_job = MagicMock()
        mock_job.job_id = "job-fw"
        mock_job.num_dml_affected_rows = 3

        # Patch the attribute directly — `from google.cloud import bigquery`
        # reads `google.cloud.bigquery` as an attribute once the module has
        # been loaded, so a sys.modules-only patch wouldn't be picked up.
        with patch("google.cloud.bigquery.Client") as mock_client:
            mock_client.return_value.query.return_value = mock_job
            BigQueryDestination._native_load_insert_into_target(
                dest, spec, "proj.ds_staging.ext", [("col1", "STRING")]
            )

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
# create_external_table — CSV options
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestCreateExternalTableCsvOptions:
    """Verify CSV format_options propagate through to bigquery.CSVOptions."""

    def _invoke_with_format_options(self, format_options: dict):
        dest = _make_dest()
        dest.config = MagicMock()
        dest.config.job_project_id = "proj"
        dest.config.project_id = "proj"
        dest.config.location = "EU"

        # create_external_table obtains its client from the pooled _client() seam;
        # the real bigquery.ExternalConfig/Table are still used to build the table.
        captured = []
        client = MagicMock()
        client.create_table.side_effect = lambda t: captured.append(t)
        dest._client.return_value = client
        # CSV option translation is a real static helper; run it, don't mock it.
        dest._build_csv_external_options.side_effect = (
            BigQueryDestination._build_csv_external_options
        )

        BigQueryDestination.create_external_table(
            dest,
            dataset="ds",
            name="ext",
            source_uris=["gs://bucket/f.csv"],
            source_format="CSV",
            autodetect=True,
            format_options=format_options,
        )
        assert len(captured) == 1
        return captured[0].external_data_configuration.csv_options

    def test_allow_quoted_newlines_propagates(self):
        opts = self._invoke_with_format_options({"allow_quoted_newlines": True})
        assert opts.allow_quoted_newlines is True

    def test_allow_jagged_rows_propagates(self):
        opts = self._invoke_with_format_options({"allow_jagged_rows": True})
        assert opts.allow_jagged_rows is True

    def test_preserve_ascii_control_characters_propagates(self):
        opts = self._invoke_with_format_options(
            {"preserve_ascii_control_characters": True}
        )
        assert opts.preserve_ascii_control_characters is True

    def test_new_bools_default_unset(self):
        # Only field_delimiter passed — the new flags should not be enabled.
        opts = self._invoke_with_format_options({"field_delimiter": ";"})
        assert not opts.allow_quoted_newlines
        assert not opts.allow_jagged_rows
        assert not opts.preserve_ascii_control_characters

    def test_combined_flags_propagate(self):
        opts = self._invoke_with_format_options(
            {
                "field_delimiter": ";",
                "allow_quoted_newlines": True,
                "allow_jagged_rows": True,
                "preserve_ascii_control_characters": True,
            }
        )
        assert opts.field_delimiter == ";"
        assert opts.allow_quoted_newlines is True
        assert opts.allow_jagged_rows is True
        assert opts.preserve_ascii_control_characters is True


@pytest.mark.unit
class TestCreateExternalTableExplicitSchema:
    """Explicit positional schema builds an all-STRING, autodetect-off ext table."""

    def test_schema_sets_fields_and_disables_autodetect(self):
        dest = _make_dest()
        dest.config = MagicMock()
        dest.config.project_id = "proj"
        captured = []
        client = MagicMock()
        client.create_table.side_effect = lambda t: captured.append(t)
        dest._client.return_value = client

        BigQueryDestination.create_external_table(
            dest,
            dataset="ds",
            name="ext",
            source_uris=["s3://bucket/f.txt.gz"],
            source_format="CSV",
            autodetect=False,
            schema=[("app_id", "STRING"), ("txn_id", "STRING")],
        )

        ext_config = captured[0].external_data_configuration
        assert ext_config.autodetect is False
        assert [f.name for f in ext_config.schema] == ["app_id", "txn_id"]
        assert {f.field_type for f in ext_config.schema} == {"STRING"}

    def test_schema_on_table_when_connection(self):
        # BigLake (connection-backed) external tables require the schema on the
        # Table, not on ExternalConfig — else BigQuery 400s.
        dest = _make_dest()
        dest.config = MagicMock()
        dest.config.project_id = "proj"
        captured = []
        client = MagicMock()
        client.create_table.side_effect = lambda t: captured.append(t)
        dest._client.return_value = client

        BigQueryDestination.create_external_table(
            dest,
            dataset="ds",
            name="ext",
            source_uris=["s3://bucket/f.txt.gz"],
            source_format="CSV",
            autodetect=False,
            connection_id="projects/proj/locations/aws-eu-west-1/connections/c",
            schema=[("app_id", "STRING"), ("txn_id", "STRING")],
        )

        table = captured[0]
        assert [f.name for f in table.schema] == ["app_id", "txn_id"]
        assert not table.external_data_configuration.schema
        assert table.external_data_configuration.autodetect is False


@pytest.mark.unit
class TestNativeLoadChunkExternalSchema:
    def test_external_schema_passed_to_create_external_table(self):
        dest = _make_dest()
        dest.list_table_columns.return_value = [("app_id", "STRING")]
        dest._patch_ext_for_hints.return_value = [("app_id", "STRING")]
        dest._native_load_create_target.return_value = (1, "job")
        dest._native_load_rowcounts.return_value = {}
        schema = [("app_id", "STRING"), ("txn_id", "STRING")]
        spec = _make_spec(file_type="csv", target_exists=False, external_schema=schema)

        BigQueryDestination.native_load_chunk(dest, spec)

        _, kwargs = dest.create_external_table.call_args
        assert kwargs["schema"] == schema


@pytest.mark.unit
class TestNativeLoadInsertCrossCloud:
    """Cross-cloud append: CTAS to a destination-region transfer table, then
    in-region INSERT (a single statement can't span the Omni + destination
    regions)."""

    def test_ctas_transfer_then_in_region_insert_then_drop(self):
        dest = _make_dest()
        dest.config = MagicMock()
        dest.config.project_id = "proj"
        dest.get_full_table_id.side_effect = lambda ds, tbl: f"proj.{ds}.{tbl}"

        job = MagicMock()
        job.job_id = "job-x"
        job.num_dml_affected_rows = 3
        client = MagicMock()
        client.query.return_value = job
        dest._client.return_value = client

        spec = _make_spec(target_exists=True, omni_location="aws-eu-west-1")

        rows, job_id = BigQueryDestination._native_load_insert_cross_cloud(
            dest,
            spec,
            "proj.omni.ext",
            "proj.ds.tbl",
            "`a`, `b`",
            "`a`, `b`",
            "",
            "aws-eu-west-1",
        )

        # 1. CTAS materializes a transfer table, run in the Omni region.
        ctas_args = dest.execute_sql_with_job.call_args
        assert ctas_args[0][0].startswith("CREATE TABLE")
        assert "FROM proj.omni.ext" in ctas_args[0][0]
        assert ctas_args[1]["location"] == "aws-eu-west-1"
        # 2. In-region INSERT from the transfer table (no location override).
        insert_sql = client.query.call_args[0][0]
        assert insert_sql.startswith("INSERT INTO proj.ds.tbl")
        assert "FROM proj.ds.tbl__xfer_" in insert_sql
        dest._client.assert_called_once_with()  # default (destination) location
        # 3. Transfer table dropped.
        dest.drop_table.assert_called_once()
        assert rows == 3
        assert job_id == "job-x"

    def test_same_cloud_insert_stays_single_statement(self):
        # No omni_location → _native_load_insert_into_target keeps the direct
        # INSERT and never builds a transfer table.
        from unittest.mock import patch

        dest = _make_dest()
        dest.config = MagicMock()
        dest.config.project_id = "proj"
        dest.config.location = "EU"
        dest.list_table_columns.return_value = [("col1", "STRING")]
        spec = _make_spec(target_exists=True)  # omni_location=None

        job = MagicMock()
        job.job_id = "job-gcs"
        job.num_dml_affected_rows = 4
        with patch("google.cloud.bigquery.Client") as mock_client:
            mock_client.return_value.query.return_value = job
            BigQueryDestination._native_load_insert_into_target(
                dest, spec, "proj.ds_staging.ext", [("col1", "STRING")]
            )

        # Direct INSERT ... FROM the external table; no transfer CTAS.
        dest.execute_sql_with_job.assert_not_called()
        dest.drop_table.assert_not_called()


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

    def test_partition_expiration_options_in_ctas(self):
        dest = _make_dest()
        dest.execute_sql.return_value = [MagicMock(cnt=0)]
        dest.config.partition_expiration_days = 90
        spec = _make_spec(partition_column="event_date")
        ext_cols = [("event_date", "DATE")]

        BigQueryDestination._native_load_create_target(
            dest, spec, "proj.ds_staging.ext", ext_cols
        )

        sql = dest.execute_sql_with_job.call_args[0][0]
        assert "OPTIONS (partition_expiration_days = 90)" in sql

    def test_partition_expiration_options_omitted_without_partition_column(self):
        # Without a partition column the OPTIONS clause makes no sense; BigQuery
        # would reject it. Skip silently.
        dest = _make_dest()
        dest.execute_sql.return_value = [MagicMock(cnt=0)]
        dest.config.partition_expiration_days = 90
        spec = _make_spec(partition_column=None)
        ext_cols = [("col1", "STRING")]

        BigQueryDestination._native_load_create_target(
            dest, spec, "proj.ds_staging.ext", ext_cols
        )

        sql = dest.execute_sql_with_job.call_args[0][0]
        assert "partition_expiration_days" not in sql

    def test_partition_expiration_options_omitted_when_unset(self):
        dest = _make_dest()
        dest.execute_sql.return_value = [MagicMock(cnt=0)]
        # partition_expiration_days defaults to None via _make_dest.
        spec = _make_spec(partition_column="event_date")
        ext_cols = [("event_date", "DATE")]

        BigQueryDestination._native_load_create_target(
            dest, spec, "proj.ds_staging.ext", ext_cols
        )

        sql = dest.execute_sql_with_job.call_args[0][0]
        assert "partition_expiration_days" not in sql

    def test_partition_expiration_options_skipped_on_iceberg(self):
        dest = _make_dest()
        dest.execute_sql.return_value = [MagicMock(cnt=0)]
        dest.config.partition_expiration_days = 90
        dest.config.table_format = "iceberg"
        spec = _make_spec(partition_column="event_date")
        ext_cols = [("event_date", "DATE")]

        BigQueryDestination._native_load_create_target(
            dest, spec, "proj.ds_staging.ext", ext_cols
        )

        sql = dest.execute_sql_with_job.call_args[0][0]
        assert "partition_expiration_days" not in sql


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
        from unittest.mock import patch

        dest = _make_dest()
        dest.config = MagicMock()
        dest.config.job_project_id = "proj"
        dest.config.project_id = "proj"
        dest.config.location = "EU"
        # Target has col1; source also has new_col (not in target)
        dest.list_table_columns.return_value = [("col1", "STRING")]
        spec = _make_spec(target_exists=True)

        mock_job = MagicMock()
        mock_job.job_id = "job-se"
        mock_job.num_dml_affected_rows = 1

        with patch("google.cloud.bigquery.Client") as mock_client:
            mock_client.return_value.query.return_value = mock_job
            BigQueryDestination._native_load_insert_into_target(
                dest,
                spec,
                "proj.ds_staging.ext",
                [("col1", "STRING"), ("new_col", "INT64")],
            )

        # add_column called for framework cols + new data col
        added = [c[0][2] for c in dest.add_column.call_args_list]
        assert "new_col" in added


# ---------------------------------------------------------------------------
# Cross-cloud (S3 / BigQuery Omni) routing
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestSupportedSchemes:
    def test_bigquery_supports_gs_and_s3(self):
        dest = _make_dest()
        assert BigQueryDestination.supported_native_load_uri_schemes(dest) == {
            "gs",
            "s3",
        }


@pytest.mark.unit
class TestResolveConnectionId:
    def test_short_form_expanded_to_full_path(self):
        dest = _make_dest()
        dest.config.project_id = "proj"
        result = BigQueryDestination._resolve_connection_id(
            dest, "aws-eu-west-1.my-conn", "aws-eu-west-1"
        )
        assert result == ("projects/proj/locations/aws-eu-west-1/connections/my-conn")

    def test_full_path_passed_through(self):
        dest = _make_dest()
        full = "projects/99/locations/aws-eu-west-1/connections/my-conn"
        result = BigQueryDestination._resolve_connection_id(dest, full, "aws-eu-west-1")
        assert result == full


@pytest.mark.unit
class TestClientLocationOverride:
    def test_client_uses_override_location(self):
        dest = _make_dest()
        dest.config.job_project_id = "proj"
        dest.config.location = "EU"
        BigQueryDestination._client(dest, "aws-eu-west-1")
        dest.get_client_pool.return_value.get_client.assert_called_once_with(
            "proj", "aws-eu-west-1"
        )

    def test_client_defaults_to_config_location(self):
        dest = _make_dest()
        dest.config.job_project_id = "proj"
        dest.config.location = "EU"
        BigQueryDestination._client(dest)
        dest.get_client_pool.return_value.get_client.assert_called_once_with(
            "proj", "EU"
        )


@pytest.mark.unit
class TestNativeLoadChunkCrossCloud:
    def _omni_spec(self, target_exists=False):
        return _make_spec(
            target_exists=target_exists,
            source_uris=["s3://bucket/prefix/file1.parquet"],
            omni_location="aws-eu-west-1",
            source_connection="aws-eu-west-1.my-conn",
        )

    def _dest_for_omni(self):
        dest = _make_dest()
        dest.list_table_columns.return_value = [("col1", "STRING")]
        dest._patch_ext_for_hints.return_value = [("col1", "STRING")]
        dest._resolve_connection_id.return_value = (
            "projects/proj/locations/aws-eu-west-1/connections/my-conn"
        )
        dest._native_load_create_target.return_value = (5, "job-omni")
        dest._native_load_insert_into_target.return_value = (5, "job-omni")
        dest._native_load_rowcounts.return_value = {}
        return dest

    def test_omni_staging_dataset_ensured_in_omni_region(self):
        dest = self._dest_for_omni()
        BigQueryDestination.native_load_chunk(dest, self._omni_spec())
        dest._ensure_dataset.assert_called_once_with("ds_staging", "aws-eu-west-1")

    def test_external_table_created_with_connection_and_omni_location(self):
        dest = self._dest_for_omni()
        BigQueryDestination.native_load_chunk(dest, self._omni_spec())
        _, kwargs = dest.create_external_table.call_args
        assert kwargs["location"] == "aws-eu-west-1"
        assert kwargs["connection_id"] == (
            "projects/proj/locations/aws-eu-west-1/connections/my-conn"
        )

    def test_ext_columns_listed_in_omni_region(self):
        dest = self._dest_for_omni()
        BigQueryDestination.native_load_chunk(dest, self._omni_spec())
        _, kwargs = dest.list_table_columns.call_args
        assert kwargs["location"] == "aws-eu-west-1"

    def test_ctas_receives_omni_query_location(self):
        dest = self._dest_for_omni()
        BigQueryDestination.native_load_chunk(
            dest, self._omni_spec(target_exists=False)
        )
        _, kwargs = dest._native_load_create_target.call_args
        assert kwargs["query_location"] == "aws-eu-west-1"

    def test_insert_receives_omni_query_location(self):
        dest = self._dest_for_omni()
        BigQueryDestination.native_load_chunk(dest, self._omni_spec(target_exists=True))
        _, kwargs = dest._native_load_insert_into_target.call_args
        assert kwargs["query_location"] == "aws-eu-west-1"

    def test_gcs_path_uses_no_omni_location_or_connection(self):
        dest = _make_dest()
        dest.list_table_columns.return_value = [("col1", "STRING")]
        dest._patch_ext_for_hints.return_value = [("col1", "STRING")]
        dest._native_load_create_target.return_value = (5, "job-gcs")
        dest._native_load_rowcounts.return_value = {}
        BigQueryDestination.native_load_chunk(dest, _make_spec(target_exists=False))
        dest._ensure_dataset.assert_not_called()
        _, kwargs = dest.create_external_table.call_args
        assert kwargs["location"] is None
        assert kwargs["connection_id"] is None
