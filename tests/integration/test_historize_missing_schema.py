"""A historize-only pipeline creates the schema it writes to.

Ingest gets its schema from the dlt load; historize-only has no load, so the
schema holding the historize log and target may not exist on the first run.
"""

from dlt_saga.historize.config import HistorizeConfig
from dlt_saga.historize.runner import HistorizeRunner
from tests.integration.conftest import (
    SCHEMA,
    SNAPSHOT_1,
    SOURCE_TABLE,
    TARGET_TABLE,
    seed_raw_table,
)

NEW_SCHEMA = "not_created_yet"


def _schema_exists(destination, schema):
    rows = destination.connection.execute(
        "SELECT 1 FROM information_schema.schemata WHERE schema_name = ?", [schema]
    ).fetchall()
    return bool(rows)


def test_historize_only_creates_missing_schema(duckdb_destination):
    seed_raw_table(duckdb_destination, [SNAPSHOT_1])
    assert not _schema_exists(duckdb_destination, NEW_SCHEMA)

    runner = HistorizeRunner(
        pipeline_name="test__raw_companies",
        historize_config=HistorizeConfig(
            primary_key=["company_id"], snapshot_column="snapshot_date"
        ),
        destination=duckdb_destination,
        database="test",
        schema=NEW_SCHEMA,
        source_table_name=SOURCE_TABLE,
        target_table_name=TARGET_TABLE,
        config_dict={
            "write_disposition": "historize",
            "primary_key": ["company_id"],
            "source_schema": SCHEMA,
            "source_table": SOURCE_TABLE,
        },
    )
    stats = runner.run()

    assert stats["status"] == "completed", stats.get("error")
    count = duckdb_destination.connection.execute(
        f'SELECT COUNT(*) FROM "{NEW_SCHEMA}"."{TARGET_TABLE}"'
    ).fetchone()[0]
    assert count == len(SNAPSHOT_1["rows"])
