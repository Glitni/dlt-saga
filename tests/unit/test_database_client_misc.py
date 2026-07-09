"""Database source: config error wording, test_connection routing, and the
falsy-``0`` incremental watermark fix."""

from unittest.mock import MagicMock, patch

import pytest

from dlt_saga.pipelines.database.client import DatabaseClient
from dlt_saga.pipelines.database.config import DatabaseConfig
from dlt_saga.pipelines.database.pipeline import DatabasePipeline

pytestmark = pytest.mark.unit


class TestConfigErrorMessages:
    """Errors name the real config keys (source_database), not a phantom
    'database' key that doesn't exist on the config surface."""

    def test_sqlite_missing_source_database(self):
        with pytest.raises(ValueError, match="source_database"):
            DatabaseConfig(database_type="sqlite", source_table="t")

    def test_sqlite_error_avoids_bare_database_key(self):
        with pytest.raises(ValueError) as exc:
            DatabaseConfig(database_type="sqlite", source_table="t")
        assert "'database'" not in str(exc.value)

    def test_generic_db_missing_host_names_source_database(self):
        with pytest.raises(ValueError, match="'host' and 'source_database'"):
            DatabaseConfig(database_type="postgres", source_table="t")


class TestConnectionRouting:
    """test_connection must probe via the same backend fetch_data would use —
    ConnectorX can't reach DuckDB or BigQuery-with-ADC."""

    def test_duckdb_uses_native_client_not_connectorx(self):
        client = DatabaseClient(
            DatabaseConfig(
                database_type="duckdb", source_database=":memory:", source_table="t"
            )
        )
        with (
            patch.object(client, "_fetch_from_duckdb") as duck,
            patch("dlt_saga.pipelines.database.client.cx.read_sql") as cx_read,
        ):
            assert client.test_connection() is True
        duck.assert_called_once()
        cx_read.assert_not_called()

    def test_bigquery_adc_uses_bigquery_client_not_connectorx(self):
        client = DatabaseClient(
            DatabaseConfig(database_type="bigquery", source_table="t")
        )
        with (
            patch.object(client, "_has_bigquery_credentials", return_value=False),
            patch.object(client, "_fetch_from_bigquery") as bq,
            patch("dlt_saga.pipelines.database.client.cx.read_sql") as cx_read,
        ):
            assert client.test_connection() is True
        bq.assert_called_once()
        cx_read.assert_not_called()

    def test_other_database_uses_connectorx(self):
        client = DatabaseClient(
            DatabaseConfig(
                connection_string="postgresql://u:p@h/d",
                database_type="postgres",
                source_table="t",
            )
        )
        with patch("dlt_saga.pipelines.database.client.cx.read_sql") as cx_read:
            assert client.test_connection() is True
        cx_read.assert_called_once()


class TestFalsyWatermark:
    """A legitimate incremental watermark of 0 must not be treated as 'no
    previous load' and replaced by initial_value."""

    def _pipeline(self, max_value):
        # Bypass BasePipeline.__init__ (destination/target wiring); wire only what
        # extract_data() touches.
        pipeline = object.__new__(DatabasePipeline)
        pipeline.logger = MagicMock()
        pipeline.table_name = "events"
        pipeline.destination_database = "proj"
        pipeline.config_dict = {
            "incremental": True,
            "incremental_column": "seq",
            "initial_value": 100,
        }
        pipeline.pipeline = MagicMock(dataset_name="dlt_db")
        pipeline.destination = MagicMock()
        pipeline.destination.get_max_column_value.return_value = max_value

        captured = {}

        def fake_fetch(**kwargs):
            captured.update(kwargs)
            return "arrow-table"

        pipeline.client = MagicMock()
        pipeline.client.fetch_data.side_effect = fake_fetch
        pipeline._get_source_description = MagicMock(return_value="postgres table: t")
        return pipeline, captured

    def test_zero_watermark_used_not_initial_value(self):
        pipeline, captured = self._pipeline(max_value=0)
        with patch(
            "dlt_saga.pipelines.database.pipeline.dlt.resource",
            side_effect=lambda tbl, **kw: tbl,
        ):
            pipeline.extract_data()
        # 0 is a real watermark — must be forwarded, not swallowed to initial_value.
        assert captured["incremental_value"] == 0

    def test_none_watermark_falls_back_to_initial_value(self):
        pipeline, captured = self._pipeline(max_value=None)
        with patch(
            "dlt_saga.pipelines.database.pipeline.dlt.resource",
            side_effect=lambda tbl, **kw: tbl,
        ):
            pipeline.extract_data()
        assert captured["incremental_value"] == 100
