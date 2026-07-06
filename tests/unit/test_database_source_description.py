"""DatabasePipeline._get_source_description must not crash on a SecretStr connection_string.

`connection_string` is coerced to SecretStr in DatabaseConfig.__post_init__, so the
old `connection_string.split("://")` raised AttributeError — and only after the full
fetch had completed. The description now derives the scheme from the client's resolved
connection string.
"""

import pytest

from dlt_saga.pipelines.database.client import DatabaseClient
from dlt_saga.pipelines.database.config import DatabaseConfig
from dlt_saga.pipelines.database.pipeline import DatabasePipeline


def _pipeline_for(config: DatabaseConfig) -> DatabasePipeline:
    """Build a DatabasePipeline shell (no BasePipeline __init__) with a real client."""
    pipeline = object.__new__(DatabasePipeline)
    pipeline.source_config = config
    pipeline.client = DatabaseClient(config)
    return pipeline


@pytest.mark.unit
class TestSourceDescription:
    def test_connection_string_secretstr_does_not_crash(self):
        config = DatabaseConfig(
            connection_string="postgresql://user:pass@host:5432/db",
            source_table="orders",
        )
        # connection_string is coerced to SecretStr — the old code crashed here.
        pipeline = _pipeline_for(config)

        assert pipeline._get_source_description() == "postgresql table: orders"

    def test_connection_string_scheme_only_when_no_table(self):
        config = DatabaseConfig(
            connection_string="mysql://user:pass@host/db",
            query="SELECT 1",
        )
        pipeline = _pipeline_for(config)

        assert pipeline._get_source_description() == "mysql query"

    def test_description_does_not_leak_credentials(self):
        config = DatabaseConfig(
            connection_string="postgresql://user:s3cret@host:5432/db",
            source_table="orders",
        )
        pipeline = _pipeline_for(config)

        description = pipeline._get_source_description()

        assert "s3cret" not in description
        assert "user" not in description

    def test_component_based_config_still_described(self):
        config = DatabaseConfig(
            database_type="postgres",
            host="db.example.com",
            source_database="analytics",
            source_schema="public",
            source_table="orders",
            username="u",
            password="p",
        )
        pipeline = _pipeline_for(config)

        assert (
            pipeline._get_source_description()
            == "postgres database analytics, table: public.orders"
        )
