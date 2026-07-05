"""Integration tests for combined ingest+historize flow."""

from tests.integration.conftest import (
    SCHEMA,
    SNAPSHOT_1,
    SNAPSHOT_2,
    SNAPSHOT_3,
    SOURCE_TABLE,
    get_rows_for,
    make_historize_runner,
    query_historized,
    run_historize,
)


class TestHistorizeOnly:
    """Test historize-only flow (write_disposition: 'historize')."""

    def test_historize_only_pipeline(self, duckdb_destination):
        """Pipeline with write_disposition='historize' reads from source table."""
        result = run_historize(
            duckdb_destination,
            [SNAPSHOT_1, SNAPSHOT_2, SNAPSHOT_3],
            write_disposition="historize",
        )

        assert result["status"] == "completed"
        assert len(query_historized(duckdb_destination)) == 6


def _seed_replace_snapshot(destination, rows, ingested_at):
    """Overwrite the raw table with a single snapshot stamped with
    _dlt_ingested_at — mirroring what one `replace` ingest run produces.

    The `_dlt_ingested_at` column is the historize default snapshot_column, and
    is what BasePipeline._inject_ingested_at stamps at ingest time for replace.
    """
    destination.execute_sql(
        f"""
        CREATE SCHEMA IF NOT EXISTS "{SCHEMA}";
        DROP TABLE IF EXISTS "{SCHEMA}"."{SOURCE_TABLE}";
        CREATE TABLE "{SCHEMA}"."{SOURCE_TABLE}" (
            company_name VARCHAR,
            company_id INTEGER,
            city VARCHAR,
            _dlt_ingested_at TIMESTAMP
        );
        """,
        SCHEMA,
    )
    for name, cid, city in rows:
        destination.execute_sql(
            f"""
            INSERT INTO "{SCHEMA}"."{SOURCE_TABLE}"
            VALUES ('{name}', {cid}, '{city}', TIMESTAMP '{ingested_at}')
            """,
            SCHEMA,
        )


class TestReplaceHistorize:
    """replace+historize lifecycle on the default _dlt_ingested_at column.

    Each `replace` run overwrites the staging table with one snapshot stamped
    by `_dlt_ingested_at`; historize then picks up each run as a new snapshot.
    Regression guard for the "Unrecognized name: _dlt_ingested_at" failure.
    """

    def test_replace_then_incremental_historize(self, duckdb_destination):
        # Run 1: replace writes snapshot A, full reprocess historizes it.
        _seed_replace_snapshot(
            duckdb_destination,
            [("Acme Corp", 1, "New York"), ("Beta Inc", 2, "London")],
            ingested_at="2026-01-01 00:00:00",
        )
        result = make_historize_runner(
            duckdb_destination,
            full_refresh=True,
            snapshot_column="_dlt_ingested_at",
        ).run()
        assert result["status"] == "completed"
        assert result["mode"] == "full_reprocess"

        # Run 2: replace overwrites with snapshot B (Acme moved city), then
        # incremental historize captures the change as a new SCD2 version.
        _seed_replace_snapshot(
            duckdb_destination,
            [("Acme Corp", 1, "Boston"), ("Beta Inc", 2, "London")],
            ingested_at="2026-01-02 00:00:00",
        )
        runner = make_historize_runner(
            duckdb_destination,
            full_refresh=False,
            snapshot_column="_dlt_ingested_at",
        )
        result = runner.run()
        assert result["status"] == "completed"
        assert result["mode"] == "incremental"

        acme = get_rows_for(query_historized(duckdb_destination), 1)
        # New York closed, Boston open.
        assert [r["city"] for r in acme] == ["New York", "Boston"]
        assert acme[0]["_dlt_valid_to"] is not None
        assert acme[1]["_dlt_valid_to"] is None

        # Beta Inc did NOT change between snapshots A and B. Because the source is
        # `replace` mode, snapshot A's rows no longer exist in the staging table;
        # the change-detection baseline must come from the historized target's
        # open rows, otherwise Beta gets a spurious second (identical) version.
        # Regression guard for "replace+historize re-versions unchanged rows".
        beta = get_rows_for(query_historized(duckdb_destination), 2)
        assert [r["city"] for r in beta] == ["London"]
        assert beta[0]["_dlt_valid_from"] < acme[1]["_dlt_valid_from"]  # from run 1
        assert beta[0]["_dlt_valid_to"] is None  # still the single open version
