"""Unit tests for BigQueryDestinationConfig and partition_expiration_days plumbing."""

from unittest.mock import MagicMock, patch

import pytest

from dlt_saga.destinations.bigquery.config import BigQueryDestinationConfig
from dlt_saga.destinations.bigquery.destination import BigQueryDestination


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

        with patch("google.cloud.bigquery.Client") as mock_client_cls:
            client = mock_client_cls.return_value
            if not_found:
                client.get_table.side_effect = NotFound("nope")
            else:
                client.get_table.return_value = table_mock
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
        # Iceberg branch returns before instantiating a Client at all.
        with patch("google.cloud.bigquery.Client") as mock_client_cls:
            BigQueryDestination.sync_table_options(dest, "ds", "tbl")
            mock_client_cls.assert_not_called()

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
