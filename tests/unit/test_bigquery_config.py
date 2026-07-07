"""Unit tests for BigQueryDestinationConfig and partition_expiration_days plumbing."""

from unittest.mock import MagicMock, patch

import pytest

from dlt_saga.destinations.bigquery.config import BigQueryDestinationConfig
from dlt_saga.destinations.bigquery.destination import BigQueryDestination


@pytest.mark.unit
class TestInsertLoadInfoDml:
    """Load-info rows go in one multi-row INSERT, not one DML job per row."""

    def test_batches_rows_into_single_insert(self):
        client = MagicMock()
        rows = [
            {"pipeline_name": "p1", "table_name": "t1", "row_count": 5},
            {"pipeline_name": "p2", "table_name": "t2", "row_count": 6},
        ]
        BigQueryDestination._insert_load_info_dml(client, "proj.ds.tbl", rows)

        client.query.assert_called_once()  # one job for both rows
        sql = client.query.call_args.args[0]
        assert sql.count("VALUES") == 1
        assert "@pipeline_name_0" in sql and "@pipeline_name_1" in sql

        params = client.query.call_args.kwargs["job_config"].query_parameters
        by_name = {p.name: p.value for p in params}
        assert by_name["pipeline_name_0"] == "p1"
        assert by_name["pipeline_name_1"] == "p2"
        assert by_name["row_count_0"] == 5
        # shared load_id, per-row _dlt_id
        assert by_name["_dlt_load_id_0"] == by_name["_dlt_load_id_1"]
        assert by_name["_dlt_id_0"] != by_name["_dlt_id_1"]


@pytest.mark.unit
class TestBigQueryClientPool:
    """The pool reuses clients per (thread, project, location)."""

    def test_caches_per_project_and_location(self):
        import dlt_saga.utility.gcp.client_pool as client_pool

        pool = client_pool.bigquery_pool
        with patch("google.cloud.bigquery.Client") as mock_cls:
            mock_cls.side_effect = lambda **kw: MagicMock()
            c1 = pool.get_client("proj", "EU")
            c2 = pool.get_client("proj", "EU")
            c3 = pool.get_client("proj", "US")
        assert c1 is c2  # same (project, location) → cached
        assert c3 is not c1  # different location → distinct client
        assert mock_cls.call_count == 2
        mock_cls.assert_any_call(project="proj", location="EU")
        mock_cls.assert_any_call(project="proj", location="US")

    def test_location_defaults_to_none(self):
        import dlt_saga.utility.gcp.client_pool as client_pool

        with patch("google.cloud.bigquery.Client") as mock_cls:
            client_pool.bigquery_pool.get_client("proj")
        mock_cls.assert_called_once_with(project="proj", location=None)


@pytest.mark.unit
class TestBigQueryDestinationConfig:
    def test_partition_expiration_defaults_to_none(self):
        cfg = BigQueryDestinationConfig(project_id="p")
        assert cfg.partition_expiration_days is None

    def test_partition_expiration_accepts_positive_int(self):
        cfg = BigQueryDestinationConfig(project_id="p", partition_expiration_days=90)
        assert cfg.partition_expiration_days == 90

    @pytest.mark.parametrize("bad_value", [0, -1, -100])
    def test_partition_expiration_negative_raises(self, bad_value):
        with pytest.raises(ValueError, match="partition_expiration_days must be >= 1"):
            BigQueryDestinationConfig(
                project_id="p", partition_expiration_days=bad_value
            )

    def test_from_dict_reads_partition_expiration(self):
        cfg = BigQueryDestinationConfig.from_dict(
            {"project_id": "p", "partition_expiration_days": 365}
        )
        assert cfg.partition_expiration_days == 365

    def test_from_dict_omits_partition_expiration(self):
        cfg = BigQueryDestinationConfig.from_dict({"project_id": "p"})
        assert cfg.partition_expiration_days is None


@pytest.mark.unit
class TestBigQueryFromContextPartitionExpiration:
    """Resolution: pipeline config > profile destination_config > None."""

    def _make_context(self, profile_partition_days=None):
        ctx = MagicMock()
        ctx.get_database.return_value = "proj"
        ctx.get_location.return_value = "EU"
        ctx.get_table_format.return_value = "native"
        ctx.get_storage_path.return_value = None
        if profile_partition_days is None:
            ctx.profile_target.destination_config = {}
        else:
            ctx.profile_target.destination_config = {
                "partition_expiration_days": profile_partition_days
            }
        ctx.profile_target.billing_project = None
        return ctx

    def test_pipeline_value_wins(self):
        ctx = self._make_context(profile_partition_days=30)
        cfg = BigQueryDestinationConfig.from_context(
            ctx, {"partition_expiration_days": 365}
        )
        assert cfg.partition_expiration_days == 365

    def test_profile_default_used_when_pipeline_absent(self):
        ctx = self._make_context(profile_partition_days=30)
        cfg = BigQueryDestinationConfig.from_context(ctx, {})
        assert cfg.partition_expiration_days == 30

    def test_none_when_both_absent(self):
        ctx = self._make_context(profile_partition_days=None)
        cfg = BigQueryDestinationConfig.from_context(ctx, {})
        assert cfg.partition_expiration_days is None

    def test_no_profile_target_resolves_pipeline_only(self):
        ctx = MagicMock()
        ctx.get_database.return_value = "proj"
        ctx.get_location.return_value = "EU"
        ctx.get_table_format.return_value = "native"
        ctx.get_storage_path.return_value = None
        ctx.profile_target = None
        cfg = BigQueryDestinationConfig.from_context(
            ctx, {"partition_expiration_days": 90}
        )
        assert cfg.partition_expiration_days == 90


@pytest.mark.unit
class TestBigQueryExecuteSqlNoClientTimeout:
    """execute_sql must not impose a client-side timeout: result() waits for the
    job to complete server-side. A hard client timeout previously killed long
    historize MERGEs while the job kept committing server-side (#7)."""

    def _make_dest(self):
        return BigQueryDestination(BigQueryDestinationConfig(project_id="proj"))

    def test_result_called_without_timeout(self):
        dest = self._make_dest()
        with patch("google.cloud.bigquery.Client") as mock_client_cls:
            query_job = mock_client_cls.return_value.query.return_value
            query_job.result.return_value = "rows"
            result = dest.execute_sql("SELECT 1", schema_name="ds")
        assert result == "rows"
        # No timeout kwarg (and no positional) — wait for job completion.
        query_job.result.assert_called_once_with()


@pytest.mark.unit
class TestApplyNativeHintsPartitionExpiration:
    """`_apply_native_hints` should propagate partition_expiration_days into bigquery_adapter."""

    def _make_dest(self, partition_expiration_days=None, table_format="native"):
        dest = MagicMock(spec=BigQueryDestination)
        dest.config = MagicMock()
        dest.config.partition_expiration_days = partition_expiration_days
        dest.config.table_format = table_format
        return dest

    def test_partition_expiration_passed_to_adapter(self):
        dest = self._make_dest(partition_expiration_days=120)
        with patch("dlt.destinations.adapters.bigquery_adapter") as mock_adapter:
            BigQueryDestination._apply_native_hints(
                dest,
                resource=MagicMock(),
                hints={"partition_column": "event_date"},
            )
        assert mock_adapter.called
        kwargs = mock_adapter.call_args.kwargs
        assert kwargs["partition"] == "event_date"
        assert kwargs["partition_expiration_days"] == 120

    def test_partition_expiration_omitted_without_partition_column(self):
        # partition_expiration_days only makes sense on a partitioned table.
        dest = self._make_dest(partition_expiration_days=120)
        with patch("dlt.destinations.adapters.bigquery_adapter") as mock_adapter:
            result = BigQueryDestination._apply_native_hints(
                dest,
                resource=MagicMock(),
                hints={"table_description": "desc"},
            )
        # Adapter is called (table_description is set) but partition_expiration_days isn't passed.
        if mock_adapter.called:
            assert "partition_expiration_days" not in mock_adapter.call_args.kwargs
        else:
            # If no adapter call at all, that's also acceptable — the resource passes through.
            assert result is not None

    def test_no_adapter_kwarg_when_config_unset(self):
        dest = self._make_dest(partition_expiration_days=None)
        with patch("dlt.destinations.adapters.bigquery_adapter") as mock_adapter:
            BigQueryDestination._apply_native_hints(
                dest,
                resource=MagicMock(),
                hints={"partition_column": "event_date"},
            )
        kwargs = mock_adapter.call_args.kwargs
        assert "partition_expiration_days" not in kwargs


@pytest.mark.unit
class TestSyncTableOptionsPartitionExpiration:
    """`sync_table_options` ALTERs an existing table when the declared value differs."""

    def _make_dest(
        self,
        partition_expiration_days=None,
        table_format="native",
    ):
        dest = MagicMock(spec=BigQueryDestination)
        dest.config = MagicMock()
        dest.config.project_id = "proj"
        dest.config.job_project_id = "proj"
        dest.config.location = "EU"
        dest.config.partition_expiration_days = partition_expiration_days
        dest.config.table_format = table_format
        return dest

    def _make_table(self, expiration_ms, partitioned=True):
        tbl = MagicMock()
        if partitioned:
            tbl.time_partitioning = MagicMock()
            tbl.time_partitioning.expiration_ms = expiration_ms
        else:
            tbl.time_partitioning = None
        return tbl

    def _invoke(self, dest, table_mock=None, not_found=False):
        from google.cloud.exceptions import NotFound

        # sync_table_options obtains its client from the pooled _client() seam.
        client = MagicMock()
        if not_found:
            client.get_table.side_effect = NotFound("nope")
        else:
            client.get_table.return_value = table_mock
        dest._client.return_value = client
        BigQueryDestination.sync_table_options(dest, "ds", "tbl")
        return client

    def test_sets_expiration_when_table_unbounded(self):
        dest = self._make_dest(partition_expiration_days=90)
        tbl = self._make_table(expiration_ms=None)
        client = self._invoke(dest, table_mock=tbl)
        assert tbl.time_partitioning.expiration_ms == 90 * 86_400_000
        client.update_table.assert_called_once_with(tbl, ["time_partitioning"])

    def test_clears_expiration_when_config_unset(self):
        dest = self._make_dest(partition_expiration_days=None)
        tbl = self._make_table(expiration_ms=30 * 86_400_000)
        client = self._invoke(dest, table_mock=tbl)
        assert tbl.time_partitioning.expiration_ms is None
        client.update_table.assert_called_once_with(tbl, ["time_partitioning"])

    def test_updates_expiration_when_value_differs(self):
        dest = self._make_dest(partition_expiration_days=90)
        tbl = self._make_table(expiration_ms=30 * 86_400_000)
        client = self._invoke(dest, table_mock=tbl)
        assert tbl.time_partitioning.expiration_ms == 90 * 86_400_000
        client.update_table.assert_called_once()

    def test_idempotent_when_value_matches(self):
        dest = self._make_dest(partition_expiration_days=90)
        tbl = self._make_table(expiration_ms=90 * 86_400_000)
        client = self._invoke(dest, table_mock=tbl)
        client.update_table.assert_not_called()

    def test_noop_when_table_missing(self):
        dest = self._make_dest(partition_expiration_days=90)
        client = self._invoke(dest, not_found=True)
        client.update_table.assert_not_called()

    def test_noop_on_iceberg(self):
        dest = self._make_dest(partition_expiration_days=90, table_format="iceberg")
        # Iceberg branch returns before acquiring a client at all.
        BigQueryDestination.sync_table_options(dest, "ds", "tbl")
        dest._client.assert_not_called()

    def test_warns_when_table_not_partitioned(self, caplog):
        import logging

        dest = self._make_dest(partition_expiration_days=90)
        tbl = self._make_table(expiration_ms=None, partitioned=False)
        with caplog.at_level(
            logging.WARNING, logger="dlt_saga.destinations.bigquery.destination"
        ):
            client = self._invoke(dest, table_mock=tbl)
        client.update_table.assert_not_called()
        assert any(
            "not partitioned" in record.getMessage() for record in caplog.records
        )

    def test_no_warning_when_unpartitioned_and_config_unset(self, caplog):
        import logging

        dest = self._make_dest(partition_expiration_days=None)
        tbl = self._make_table(expiration_ms=None, partitioned=False)
        with caplog.at_level(
            logging.WARNING, logger="dlt_saga.destinations.bigquery.destination"
        ):
            client = self._invoke(dest, table_mock=tbl)
        client.update_table.assert_not_called()
        assert not any(
            "not partitioned" in record.getMessage() for record in caplog.records
        )


@pytest.mark.unit
class TestCreateDltDestinationProjectId:
    """`create_dlt_destination` must hand dlt the data project_id, not the
    billing project. dlt has a single `project_id` field that controls both
    the client's billing project and dataset/table reference resolution
    (dlt/destinations/impl/bigquery/sql_client.py:96-200) — passing the
    billing project makes dlt's internal `create_dataset` call 403 when
    the running principal lacks `bigquery.datasets.create` on it.
    """

    def test_passes_data_project_not_billing_project(self):
        cfg = BigQueryDestinationConfig(
            project_id="data-proj", billing_project_id="billing-proj"
        )
        dest = BigQueryDestination(cfg)
        with patch("dlt.destinations.bigquery") as mock_bq:
            dest.create_dlt_destination()
        kwargs = mock_bq.call_args.kwargs
        assert kwargs["project_id"] == "data-proj"
        # Guard against future regressions that would re-route the
        # billing project into dlt's project_id slot.
        assert kwargs["project_id"] != cfg.job_project_id or (
            cfg.project_id == cfg.job_project_id
        )

    def test_falls_back_to_project_id_when_no_billing(self):
        cfg = BigQueryDestinationConfig(project_id="only-proj")
        dest = BigQueryDestination(cfg)
        with patch("dlt.destinations.bigquery") as mock_bq:
            dest.create_dlt_destination()
        assert mock_bq.call_args.kwargs["project_id"] == "only-proj"
