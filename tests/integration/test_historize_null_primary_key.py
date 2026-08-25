"""Integration tests for the NULL-primary-key guard (DuckDB).

A NULL primary key has no SCD2 identity: change detection partitions by the PK
(NULL groups with NULL, so change rows are produced) but the MERGE closing the
previous open row never matches, so every run leaves another permanently-open
row for the same key. The guard rejects the run as a config error — and does so
before a full refresh has dropped the existing history.
"""

from tests.integration.conftest import (
    SCHEMA,
    SNAPSHOT_1,
    SNAPSHOT_2,
    SOURCE_TABLE,
    make_historize_runner,
    query_historized,
    run_historize,
    seed_raw_table,
)


def _insert_null_key_row(destination, snapshot_date, city="Nowhere"):
    """Add a row whose primary key (company_id) is NULL."""
    destination.execute_sql(
        f"""
        INSERT INTO "{SCHEMA}"."{SOURCE_TABLE}"
        VALUES ('Orphan Ltd', NULL, '{city}', TIMESTAMP '{snapshot_date}')
        """,
        SCHEMA,
    )


class TestNullPrimaryKeyRejected:
    def test_full_reprocess_fails_as_config_error(self, duckdb_destination):
        seed_raw_table(duckdb_destination, [SNAPSHOT_1])
        _insert_null_key_row(duckdb_destination, SNAPSHOT_1["date"])

        result = make_historize_runner(duckdb_destination, full_refresh=True).run()

        assert result["status"] == "failed"
        assert result["config_error"] is True
        assert "company_id" in result["error"]

    def test_incremental_fails_when_a_new_snapshot_has_a_null_key(
        self, duckdb_destination
    ):
        run_historize(duckdb_destination, [SNAPSHOT_1])
        seed_raw_table(duckdb_destination, [SNAPSHOT_2])
        _insert_null_key_row(duckdb_destination, SNAPSHOT_2["date"])

        result = make_historize_runner(duckdb_destination).run()

        assert result["status"] == "failed"
        assert result["config_error"] is True

    def test_existing_history_survives_a_rejected_full_refresh(
        self, duckdb_destination
    ):
        """The guard runs before clear_log_entries + CREATE OR REPLACE, so a
        rejected full refresh must leave the previous history intact."""
        run_historize(duckdb_destination, [SNAPSHOT_1])
        before = query_historized(duckdb_destination)
        assert before

        _insert_null_key_row(duckdb_destination, SNAPSHOT_2["date"])
        result = make_historize_runner(duckdb_destination, full_refresh=True).run()
        assert result["status"] == "failed"

        assert query_historized(duckdb_destination) == before


class TestNullPrimaryKeyAccepted:
    def test_null_value_column_is_not_a_null_key(self, duckdb_destination):
        """Only the primary key is checked — a NULL in a value column is normal
        snapshot data and must historize as usual."""
        seed_raw_table(duckdb_destination, [SNAPSHOT_1])
        duckdb_destination.execute_sql(
            f"""
            INSERT INTO "{SCHEMA}"."{SOURCE_TABLE}"
            VALUES ('Orphan Ltd', 99, NULL, TIMESTAMP '{SNAPSHOT_1["date"]}')
            """,
            SCHEMA,
        )

        result = make_historize_runner(duckdb_destination, full_refresh=True).run()

        assert result["status"] == "completed"
        assert any(r["company_id"] == 99 for r in query_historized(duckdb_destination))

    def test_filters_exclude_null_keys_and_the_run_proceeds(self, duckdb_destination):
        """The documented escape hatch: rows the filter drops are never
        historized, so their NULL key must not block the run."""
        seed_raw_table(duckdb_destination, [SNAPSHOT_1])
        _insert_null_key_row(duckdb_destination, SNAPSHOT_1["date"])

        result = make_historize_runner(
            duckdb_destination,
            full_refresh=True,
            filters=[{"column": "company_id", "op": "is_not_null"}],
        ).run()

        assert result["status"] == "completed"
        rows = query_historized(duckdb_destination)
        assert rows
        assert all(r["company_id"] is not None for r in rows)

    def test_incremental_ignores_a_null_key_outside_the_new_snapshots(
        self, duckdb_destination
    ):
        """The probe is scoped to the snapshots the run reads. A NULL key landing
        in an already-processed snapshot is below the watermark — never
        discovered, so it must not block later runs."""
        run_historize(duckdb_destination, [SNAPSHOT_1])
        _insert_null_key_row(duckdb_destination, SNAPSHOT_1["date"])
        seed_raw_table(duckdb_destination, [SNAPSHOT_2])

        result = make_historize_runner(duckdb_destination).run()

        assert result["status"] == "completed"
        assert result["snapshots_processed"] == 1
