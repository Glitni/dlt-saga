"""Unit tests for BigQuery-specific NativeLoadPipeline dispatch.

Covers: external-table flow, CTAS on first run, INSERT on subsequent runs,
framework cols auto-added, schema evolution guard, per-file row counts.
"""

from unittest.mock import MagicMock, patch

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
    dest._transfer_table.side_effect = lambda spec, select_sql, location=None: (
        BigQueryDestination._transfer_table(dest, spec, select_sql, location)
    )
    dest._native_load_rowcounts.side_effect = lambda spec, transfer_id=None: (
        BigQueryDestination._native_load_rowcounts(dest, spec, transfer_id)
    )
    dest._run_insert.side_effect = lambda sql, job_config=None: (
        BigQueryDestination._run_insert(dest, sql, job_config)
    )
    dest._reconcile_native_load_target_columns.side_effect = lambda spec, ext_cols: (
        BigQueryDestination._reconcile_native_load_target_columns(dest, spec, ext_cols)
    )
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
    incremental=False,
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
        incremental=incremental,
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
        dest._native_load_insert_into_target.return_value = (7, "job-002", {})
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
class TestInsertConflictRetry:
    """An INSERT BigQuery aborted over a concurrent update committed nothing,
    so it is rerun; anything else — above all a transport error, after which
    the job may have committed — is raised as is."""

    def _job(self, state="DONE", message=None):
        job = MagicMock()
        job.state = state
        job.error_result = {"message": message} if message else None
        if message or state != "DONE":
            job.result.side_effect = RuntimeError(message or "connection reset")
        return job

    def _run(self, *jobs):
        dest = _make_dest()
        dest._client.return_value.query.side_effect = list(jobs)
        with patch("dlt_saga.destinations.bigquery.destination.time.sleep") as sleep:
            result = BigQueryDestination._run_insert(dest, "INSERT ...")
        return result, dest._client.return_value.query.call_count, sleep

    def test_concurrent_update_abort_is_retried(self):
        conflict = self._job(
            message="Transaction is aborted due to concurrent update against table t"
        )
        ok = self._job()
        result, queries, sleep = self._run(conflict, ok)
        assert result is ok
        assert queries == 2
        sleep.assert_called_once()

    def test_other_job_errors_are_not_retried(self):
        with pytest.raises(RuntimeError, match="Syntax error"):
            self._run(self._job(message="Syntax error at [1:1]"))

    def test_transport_error_is_not_retried(self):
        """The job isn't known to be done, so it may still commit."""
        with pytest.raises(RuntimeError, match="connection reset"):
            self._run(self._job(state="RUNNING"))

    def test_gives_up_after_the_retry_budget(self):
        from dlt_saga.destinations.bigquery.destination import _CONFLICT_RETRIES

        conflicts = [
            self._job(
                message="Could not serialize access to table due to concurrent update"
            )
            for _ in range(_CONFLICT_RETRIES + 1)
        ]
        with pytest.raises(RuntimeError, match="concurrent update"):
            self._run(*conflicts)


@pytest.mark.unit
class TestConcurrentNativeLoadSupport:
    def test_bigquery_supports_concurrent_chunks(self):
        assert BigQueryDestination.supports_concurrent_native_load(_make_dest())

    def test_column_lock_is_per_target(self):
        from dlt_saga.destinations.bigquery.destination import _target_columns_lock

        assert _target_columns_lock("p.d.t") is _target_columns_lock("p.d.t")
        assert _target_columns_lock("p.d.t") is not _target_columns_lock("p.d.u")


@pytest.mark.unit
class TestNativeLoadInsertIntoTarget:
    """Incremental and cross-cloud appends stage through a transient transfer
    table: CTAS from the external table, INSERT from it, count per file from
    it, drop it. Other appends insert straight from the external table."""

    def _dest(self, rows_by_uri=None):
        dest = _make_dest()
        dest.config.project_id = "proj"
        dest.list_table_columns.return_value = [("col1", "STRING")]
        job = MagicMock()
        job.job_id = "job-x"
        job.num_dml_affected_rows = 3
        dest._client.return_value.query.return_value = job
        dest.execute_sql.return_value = [
            MagicMock(uri=uri, cnt=cnt) for uri, cnt in (rows_by_uri or {}).items()
        ]
        return dest, job

    def _insert(self, dest, spec, query_location=None):
        return BigQueryDestination._native_load_insert_into_target(
            dest,
            spec,
            "proj.ds_staging.ext",
            [("col1", "STRING")],
            query_location=query_location,
        )

    def _insert_sql(self, dest):
        return dest._client.return_value.query.call_args[0][0]

    def test_non_incremental_inserts_straight_from_the_external_table(self):
        """Nothing records per-file counts, so a transfer table would only add
        a second pass over the chunk."""
        dest, _ = self._dest()

        rows, job_id, rows_by_uri = self._insert(dest, _make_spec(target_exists=True))

        dest.execute_sql_with_job.assert_not_called()
        assert "FROM proj.ds_staging.ext" in self._insert_sql(dest)
        job_config = dest._client.return_value.query.call_args[1]["job_config"]
        assert job_config.default_dataset.dataset_id == "ds_staging"
        assert (rows, job_id, rows_by_uri) == (3, "job-x", {})

    def test_incremental_stages_in_staging_under_the_ext_prefix(self):
        """So the orphan sweep reclaims it if a crash skips the drop."""
        dest, _ = self._dest()

        self._insert(dest, _make_spec(target_exists=True, incremental=True))

        ctas = dest.execute_sql_with_job.call_args[0][0]
        assert ctas.startswith("CREATE TABLE proj.ds_staging.tbl__ext_xfer_")
        assert "FROM proj.ds_staging.ext" in ctas
        assert "FROM proj.ds_staging.tbl__ext_xfer_" in self._insert_sql(dest)
        dataset, table = dest.drop_table.call_args[0]
        assert dataset == "ds_staging" and table.startswith("tbl__ext_xfer_")

    def test_transfer_table_expires_on_its_own(self):
        """A crash between CTAS and drop must not leave a data copy behind."""
        dest, _ = self._dest()

        self._insert(dest, _make_spec(target_exists=True, incremental=True))

        ctas = dest.execute_sql_with_job.call_args[0][0]
        assert (
            "OPTIONS(expiration_timestamp = TIMESTAMP_ADD(CURRENT_TIMESTAMP()" in ctas
        )

    def test_counts_come_from_the_transfer_table(self):
        dest, _ = self._dest({"gs://b/f1.parquet": 2, "gs://b/f2.parquet": 1})

        _, _, rows_by_uri = self._insert(
            dest, _make_spec(target_exists=True, incremental=True)
        )

        count_sql = dest.execute_sql.call_args[0][0]
        assert "FROM proj.ds_staging.tbl__ext_xfer_" in count_sql
        assert rows_by_uri == {"gs://b/f1.parquet": 2, "gs://b/f2.parquet": 1}

    def test_cross_cloud_ctas_in_omni_region_then_in_region_insert(self):
        dest, _ = self._dest()
        spec = _make_spec(target_exists=True, omni_location="aws-eu-west-1")

        self._insert(dest, spec, "aws-eu-west-1")

        ctas, ctas_kwargs = dest.execute_sql_with_job.call_args
        assert ctas[0].startswith("CREATE TABLE proj.ds.tbl__xfer_")
        assert ctas_kwargs["location"] == "aws-eu-west-1"
        assert "FROM proj.ds.tbl__xfer_" in self._insert_sql(dest)
        dest._client.assert_called_once_with()  # default (destination) location
        dataset, table = dest.drop_table.call_args[0]
        assert dataset == "ds" and table.startswith("tbl__xfer_")

    def test_transfer_table_dropped_when_insert_fails(self):
        dest, job = self._dest()
        job.result.side_effect = RuntimeError("insert failed")

        with pytest.raises(RuntimeError, match="insert failed"):
            self._insert(dest, _make_spec(target_exists=True, incremental=True))

        dest.drop_table.assert_called_once()

    @pytest.mark.parametrize("incremental", [False, True])
    def test_insert_waits_without_client_timeout(self, incremental):
        """Giving up on a running INSERT client-side would record its files as
        failed while the job still commits, so the next run duplicates them."""
        dest, job = self._dest()

        self._insert(dest, _make_spec(target_exists=True, incremental=incremental))

        job.result.assert_called_once_with()


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
        spec = _make_spec(incremental=True)

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
        spec = _make_spec(incremental=True)

        result = BigQueryDestination._native_load_rowcounts(dest, spec)
        assert result == {}

    def test_sql_uses_ingested_at_filter(self):
        dest = _make_dest()
        dest.execute_sql.return_value = []
        spec = _make_spec(incremental=True)

        BigQueryDestination._native_load_rowcounts(dest, spec)

        sql = dest.execute_sql.call_args[0][0]
        assert "WHERE" in sql
        assert "_dlt_ingested_at" in sql

    def test_transfer_table_needs_no_ingested_at_filter(self):
        dest = _make_dest()
        dest.execute_sql.return_value = []

        BigQueryDestination._native_load_rowcounts(
            dest, _make_spec(incremental=True), transfer_id="proj.st.xfer"
        )

        sql = dest.execute_sql.call_args[0][0]
        assert "FROM proj.st.xfer" in sql
        assert "WHERE" not in sql

    def test_skipped_when_not_incremental(self):
        """Only incremental runs record per-file counts."""
        dest = _make_dest()

        assert BigQueryDestination._native_load_rowcounts(dest, _make_spec()) == {}
        dest.execute_sql.assert_not_called()


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
        dest._native_load_insert_into_target.return_value = (5, "job-omni", {})
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


# ---------------------------------------------------------------------------
# native_load_count_rows — min_rows pre-count
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestNativeLoadCountRowsBQ:
    """Counting must read what the load would read, and write nothing."""

    def _dest_returning(self, count):
        dest = _make_dest()
        dest.list_table_columns.return_value = [("col1", "STRING")]
        dest._render_native_load_where.return_value = ""
        row = MagicMock()
        row.cnt = count
        dest.execute_sql.return_value = [row]
        return dest

    def test_returns_the_counted_rows(self):
        dest = self._dest_returning(4321)

        result = BigQueryDestination.native_load_count_rows(dest, _make_spec())

        assert result == 4321

    def test_counts_over_an_external_table_then_drops_it(self):
        dest = self._dest_returning(1)

        BigQueryDestination.native_load_count_rows(dest, _make_spec())

        dest.create_external_table.assert_called_once()
        assert dest.create_external_table.call_args.kwargs["source_uris"] == [
            "gs://bucket/prefix/file1.parquet"
        ]
        dest.drop_table.assert_called_once()

    def test_writes_nothing_to_the_target(self):
        """The whole point — a count that materialised rows would be the very
        write the guard exists to prevent.
        """
        dest = self._dest_returning(1)

        BigQueryDestination.native_load_count_rows(dest, _make_spec())

        dest.execute_sql_with_job.assert_not_called()
        sql = dest.execute_sql.call_args[0][0]
        assert sql.upper().startswith("SELECT COUNT(*)")

    def test_external_table_is_dropped_even_when_the_count_fails(self):
        dest = self._dest_returning(1)
        dest.execute_sql.side_effect = RuntimeError("query failed")

        with pytest.raises(RuntimeError):
            BigQueryDestination.native_load_count_rows(dest, _make_spec())

        dest.drop_table.assert_called_once()

    def test_applies_the_load_filters(self):
        dest = self._dest_returning(1)
        dest._render_native_load_where.return_value = "`region` = 'eu'"

        BigQueryDestination.native_load_count_rows(dest, _make_spec())

        sql = dest.execute_sql.call_args[0][0]
        assert "WHERE `region` = 'eu'" in sql

    def test_empty_result_counts_as_zero(self):
        dest = self._dest_returning(1)
        dest.execute_sql.return_value = []

        assert BigQueryDestination.native_load_count_rows(dest, _make_spec()) == 0

    def test_cross_cloud_counts_in_the_omni_region(self):
        dest = self._dest_returning(7)
        dest._resolve_connection_id.return_value = "projects/p/connections/c"
        spec = _make_spec(
            source_uris=["s3://bucket/prefix/file1.parquet"],
            omni_location="aws-eu-west-1",
            source_connection="aws-eu-west-1.my-conn",
        )

        BigQueryDestination.native_load_count_rows(dest, spec)

        dest._ensure_dataset.assert_called_once_with("ds_staging", "aws-eu-west-1")
        assert (
            dest.create_external_table.call_args.kwargs["location"] == "aws-eu-west-1"
        )
        assert dest.execute_sql.call_args.kwargs["location"] == "aws-eu-west-1"

    def test_count_table_is_named_so_the_orphan_sweeper_reclaims_it(self):
        """The pipeline sweeps `{table}__ext_%` in the staging dataset. The
        finally above drops the count table normally, but a SIGKILL mid-count
        skips it — a differently-prefixed name would linger forever.
        """
        dest = self._dest_returning(1)

        BigQueryDestination.native_load_count_rows(dest, _make_spec())

        created = dest.create_external_table.call_args.kwargs["name"]
        assert created.startswith("tbl__ext_")

    def test_hint_driven_columns_are_patched_as_the_load_patches_them(self):
        """The load resolves its WHERE against the patched columns; the count
        must resolve against the same ones, not merely equivalent ones.
        """
        dest = self._dest_returning(1)
        dest._patch_ext_for_hints.return_value = [("col1", "STRING")]
        spec = _make_spec(column_hints={"col1": "bigint"})

        BigQueryDestination.native_load_count_rows(dest, spec)

        dest._patch_ext_for_hints.assert_called_once()
        # The WHERE is rendered from the patched columns, not the raw listing.
        assert dest._render_native_load_where.call_args[0][1] == [("col1", "STRING")]

    def test_precount_is_advertised_as_supported(self):
        assert BigQueryDestination.supports_native_load_precount(_make_dest()) is True
