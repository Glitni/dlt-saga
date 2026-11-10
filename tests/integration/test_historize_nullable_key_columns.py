"""Integration tests for ``nullable_key_columns`` on DuckDB.

With ``region`` declared nullable, ``(customer_id, NULL)`` is one entity: its
versions chain (one open row at a time), it can be deleted and reappear, and it
never collides with ``(customer_id, 'eu')``. The NULL-key guard still refuses a
NULL in a key column that is not declared.
"""

from dlt_saga.historize.config import HistorizeConfig
from dlt_saga.historize.runner import HistorizeRunner
from tests.integration.conftest import SCHEMA

SOURCE_TABLE = "raw_prices"
TARGET_TABLE = "raw_prices_historized"

S1 = "2024-01-01 00:00:00"
S2 = "2024-01-02 00:00:00"
S3 = "2024-01-03 00:00:00"


def _seed(dest, snapshots):
    """``snapshots``: ``{snapshot: [(customer_id, region | None, price), ...]}``."""
    dest.execute_sql(
        f"""
        CREATE SCHEMA IF NOT EXISTS "{SCHEMA}";
        CREATE TABLE IF NOT EXISTS "{SCHEMA}"."{SOURCE_TABLE}" (
            customer_id INTEGER,
            region VARCHAR,
            price INTEGER,
            snapshot_date TIMESTAMP
        );
        """,
        SCHEMA,
    )
    for snapshot, rows in snapshots.items():
        for customer_id, region, price in rows:
            customer_sql = "NULL" if customer_id is None else str(customer_id)
            region_sql = "NULL" if region is None else f"'{region}'"
            dest.execute_sql(
                f"""
                INSERT INTO "{SCHEMA}"."{SOURCE_TABLE}"
                VALUES ({customer_sql}, {region_sql}, {price}, TIMESTAMP '{snapshot}')
                """,
                SCHEMA,
            )


def _runner(dest, *, full_refresh=False, nullable=("region",), **overrides):
    settings = {
        "primary_key": ["customer_id", "region"],
        "nullable_key_columns": list(nullable),
        "snapshot_column": "snapshot_date",
        "track_deletions": True,
        **overrides,
    }
    return HistorizeRunner(
        pipeline_name="test__raw_prices",
        historize_config=HistorizeConfig(**settings),
        destination=dest,
        database="test",
        schema=SCHEMA,
        source_table_name=SOURCE_TABLE,
        target_table_name=TARGET_TABLE,
        config_dict={
            "write_disposition": "historize",
            "primary_key": ["customer_id", "region"],
        },
        full_refresh=full_refresh,
    )


def _run(dest, **kwargs):
    stats = _runner(dest, **kwargs).run()
    assert stats["status"] == "completed", stats.get("error")
    return stats


def _rows(dest, order_by="customer_id, region NULLS FIRST, _dlt_valid_from"):
    result = dest.connection.execute(
        f'SELECT * FROM "{SCHEMA}"."{TARGET_TABLE}" ORDER BY {order_by}'
    )
    columns = [d[0] for d in result.description]
    return [dict(zip(columns, row)) for row in result.fetchall()]


def _key(rows, customer_id, region):
    return [
        r for r in rows if r["customer_id"] == customer_id and r["region"] == region
    ]


class TestNullKeyPartIsOneEntity:
    def test_full_reprocess_chains_versions(self, duckdb_destination):
        _seed(
            duckdb_destination,
            {
                S1: [(1, None, 10), (1, "eu", 20)],
                S2: [(1, None, 11), (1, "eu", 20)],
            },
        )
        _run(duckdb_destination, full_refresh=True)

        null_key = _key(_rows(duckdb_destination), 1, None)
        assert [(r["price"], r["_dlt_valid_to"] is None) for r in null_key] == [
            (10, False),
            (11, True),
        ]
        assert len(_key(_rows(duckdb_destination), 1, "eu")) == 1

    def test_incremental_closes_the_open_row(self, duckdb_destination):
        _seed(duckdb_destination, {S1: [(1, None, 10), (1, "eu", 20)]})
        _run(duckdb_destination, full_refresh=True)

        _seed(duckdb_destination, {S2: [(1, None, 11), (1, "eu", 20)]})
        _run(duckdb_destination)

        null_key = _key(_rows(duckdb_destination), 1, None)
        open_rows = [r for r in null_key if r["_dlt_valid_to"] is None]
        assert len(null_key) == 2
        assert [r["price"] for r in open_rows] == [11]

    def test_unchanged_null_key_opens_no_version(self, duckdb_destination):
        _seed(duckdb_destination, {S1: [(1, None, 10)]})
        _run(duckdb_destination, full_refresh=True)
        _seed(duckdb_destination, {S2: [(1, None, 10)]})
        _run(duckdb_destination)

        assert len(_key(_rows(duckdb_destination), 1, None)) == 1

    def test_deletion_and_reappearance(self, duckdb_destination):
        _seed(duckdb_destination, {S1: [(1, None, 10), (1, "eu", 20)]})
        _run(duckdb_destination, full_refresh=True)

        # The NULL-region entity disappears, the 'eu' one stays.
        _seed(duckdb_destination, {S2: [(1, "eu", 20)]})
        _run(duckdb_destination)

        null_key = _key(_rows(duckdb_destination), 1, None)
        assert [
            (r["_dlt_is_deleted"], r["_dlt_valid_to"] is None) for r in null_key
        ] == [
            (False, False),
            (True, True),
        ]
        assert [
            r["_dlt_is_deleted"] for r in _key(_rows(duckdb_destination), 1, "eu")
        ] == [False]

        # It comes back: the deletion marker closes, one new open row.
        _seed(duckdb_destination, {S3: [(1, None, 12), (1, "eu", 20)]})
        _run(duckdb_destination)

        null_key = _key(_rows(duckdb_destination), 1, None)
        open_rows = [r for r in null_key if r["_dlt_valid_to"] is None]
        assert len(null_key) == 3
        assert [(r["price"], r["_dlt_is_deleted"]) for r in open_rows] == [(12, False)]


class TestScd1:
    def test_latest_version_per_null_keyed_entity(self, duckdb_destination):
        _seed(duckdb_destination, {S1: [(1, None, 10), (1, "eu", 20)]})
        _run(
            duckdb_destination, full_refresh=True, strategy="scd1", track_deletions=None
        )
        _seed(duckdb_destination, {S2: [(1, None, 11), (1, "eu", 20)]})
        _run(duckdb_destination, strategy="scd1", track_deletions=None)

        rows = _rows(duckdb_destination)
        assert [(r["customer_id"], r["region"], r["price"]) for r in rows] == [
            (1, None, 11),
            (1, "eu", 20),
        ]


class TestGuardStillAppliesToUndeclaredColumns:
    def test_null_in_an_undeclared_key_column_is_refused(self, duckdb_destination):
        _seed(duckdb_destination, {S1: [(None, "eu", 10)]})

        stats = _runner(duckdb_destination, full_refresh=True).run()

        assert stats["status"] == "failed"
        assert "customer_id" in stats["error"]
        assert "region" not in stats["error"]

    def test_without_the_option_a_null_region_is_refused(self, duckdb_destination):
        _seed(duckdb_destination, {S1: [(1, None, 10)]})

        stats = _runner(duckdb_destination, full_refresh=True, nullable=()).run()

        assert stats["status"] == "failed"
        assert "region" in stats["error"]
