"""Shared fixtures for integration tests using DuckDB destination."""

import pytest

from dlt_saga.destinations.duckdb.config import DuckDBDestinationConfig
from dlt_saga.destinations.duckdb.destination import DuckDBDestination
from dlt_saga.historize.config import HistorizeConfig
from dlt_saga.historize.runner import HistorizeRunner
from dlt_saga.utility.cli.context import clear_execution_context


def pytest_collection_modifyitems(config, items):
    """Auto-apply the ``integration`` marker to every test in this directory.

    This keeps ``pytest -m integration`` in CI working without requiring each
    test file to declare ``pytestmark = pytest.mark.integration`` manually.
    """
    marker = pytest.mark.integration
    for item in items:
        if "tests/integration/" in item.nodeid.replace("\\", "/"):
            item.add_marker(marker)


# ---------------------------------------------------------------------------
# Test data: company snapshots matching manual integration test runbooks
# ---------------------------------------------------------------------------

SNAPSHOT_1 = {
    "date": "2026-01-01",
    "rows": [
        ("Acme Corp", 1, "New York"),
        ("Beta Inc", 2, "London"),
        ("Charlie LLC", 3, "Paris"),
    ],
}

SNAPSHOT_2 = {
    "date": "2026-01-02",
    "rows": [
        ("Acme Corp", 1, "Boston"),  # changed city
        ("Beta Inc", 2, "London"),
        ("Charlie LLC", 3, "Paris"),
    ],
}

SNAPSHOT_3 = {
    "date": "2026-01-03",
    "rows": [
        # Acme deleted
        ("Beta Inc", 2, "London"),
        ("Charlie LLC", 3, "Paris"),
        ("Delta Ltd", 4, "Berlin"),  # new
    ],
}

SNAPSHOT_4 = {
    "date": "2026-01-04",
    "rows": [
        ("Beta Inc", 2, "Tokyo"),  # changed city
        # Charlie deleted
        ("Delta Ltd", 4, "Berlin"),
        ("Gamma Co", 5, "Madrid"),  # new
    ],
}

SNAPSHOT_5 = {
    "date": "2026-01-05",
    "rows": [
        ("Acme Corp", 1, "Chicago"),  # reappears with new city
        ("Beta Inc", 2, "Tokyo"),
        ("Delta Ltd", 4, "Berlin"),
    ],
}

SCHEMA = "test"
SOURCE_TABLE = "raw_companies"
TARGET_TABLE = "raw_companies_historized"


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture(autouse=True)
def _clean_global_state():
    """Reset global state between integration tests."""
    yield
    clear_execution_context()


@pytest.fixture
def duckdb_destination():
    """Create an in-memory DuckDB destination for testing."""
    config = DuckDBDestinationConfig(
        project_id="test",
        location="local",
        schema_name=SCHEMA,
        database_path=":memory:",
    )
    dest = DuckDBDestination(config)
    yield dest
    dest.close()


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def seed_raw_table(destination, snapshots):
    """Seed the raw table with snapshot data.

    Args:
        destination: DuckDBDestination instance
        snapshots: list of snapshot dicts with 'date' and 'rows' keys
    """
    ddl = f"""
        CREATE SCHEMA IF NOT EXISTS "{SCHEMA}";
        CREATE TABLE IF NOT EXISTS "{SCHEMA}"."{SOURCE_TABLE}" (
            company_name VARCHAR,
            company_id INTEGER,
            city VARCHAR,
            snapshot_date TIMESTAMP
        );
    """
    destination.execute_sql(ddl, SCHEMA)

    for snapshot in snapshots:
        ts = snapshot["date"]
        for name, cid, city in snapshot["rows"]:
            sql = f"""
                INSERT INTO "{SCHEMA}"."{SOURCE_TABLE}"
                VALUES ('{name}', {cid}, '{city}', TIMESTAMP '{ts}')
            """
            destination.execute_sql(sql, SCHEMA)


def make_historize_runner(
    destination,
    full_refresh=False,
    partial_refresh=False,
    historize_from=None,
    primary_key=None,
    snapshot_column="snapshot_date",
    track_deletions=True,
    ignore_columns=None,
    write_disposition="historize",
):
    """Create a HistorizeRunner for testing."""
    config = HistorizeConfig(
        primary_key=primary_key or ["company_id"],
        snapshot_column=snapshot_column,
        track_deletions=track_deletions,
        ignore_columns=ignore_columns or [],
    )
    return HistorizeRunner(
        pipeline_name="test__raw_companies",
        historize_config=config,
        destination=destination,
        database="test",
        schema=SCHEMA,
        source_table_name=SOURCE_TABLE,
        target_table_name=TARGET_TABLE,
        config_dict={
            "write_disposition": write_disposition,
            "primary_key": primary_key or ["company_id"],
        },
        full_refresh=full_refresh,
        partial_refresh=partial_refresh,
        historize_from=historize_from,
    )


def query_historized(destination, order_by="company_id, _dlt_valid_from"):
    """Query the historized table and return rows as list of dicts."""
    sql = f'SELECT * FROM "{SCHEMA}"."{TARGET_TABLE}" ORDER BY {order_by}'
    conn = destination.connection
    result = conn.execute(sql)
    columns = [desc[0] for desc in result.description]
    return [dict(zip(columns, row)) for row in result.fetchall()]


def assert_row(row, **expected):
    """Assert that a row dict contains the expected values."""
    for key, value in expected.items():
        actual = row[key]
        assert actual == value, f"row[{key!r}]: {actual!r} != {value!r}"


def get_rows_for(rows, company_id):
    """Filter historized rows by company_id."""
    return [r for r in rows if r["company_id"] == company_id]


def run_historize(destination, snapshots, full_refresh=True, **kwargs):
    """Seed data and run historization in one step."""
    seed_raw_table(destination, snapshots)
    runner = make_historize_runner(destination, full_refresh=full_refresh, **kwargs)
    return runner.run()


def query_log(destination):
    """Query the historize log table and return rows as list of dicts."""
    sql = f'SELECT * FROM "{SCHEMA}"."_saga_historize_log" ORDER BY started_at'
    conn = destination.connection
    result = conn.execute(sql)
    columns = [desc[0] for desc in result.description]
    return [dict(zip(columns, row)) for row in result.fetchall()]
