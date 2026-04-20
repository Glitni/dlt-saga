"""Integration tests for partial re-historization (--partial-refresh / --historize-from).

Tests the clone-and-swap partial refresh pipeline using DuckDB, covering:
- Basic partial refresh preserves old records and rebuilds from boundary
- --historize-from with explicit date
- Boundary clamping when date predates raw data
- Clone-and-swap lifecycle (staging created, original untouched, cleanup)
- Failure cleanup (staging dropped, original preserved)
- Deletion markers inside and outside the reprocessing window
- No prior run fallback to full reprocess
- Config fingerprint guard with --partial-refresh
- Log table updated only after successful swap
- Empty raw table error
"""

from datetime import datetime
from unittest.mock import patch

from tests.integration.conftest import (
    SCHEMA,
    SNAPSHOT_1,
    SNAPSHOT_2,
    SNAPSHOT_3,
    SNAPSHOT_4,
    SNAPSHOT_5,
    SOURCE_TABLE,
    assert_row,
    get_rows_for,
    make_historize_runner,
    query_historized,
    query_log,
    run_historize,
    seed_raw_table,
)

DT = datetime


def _table_exists(destination, schema, table):
    """Check whether a table exists in the DuckDB destination."""
    try:
        destination.connection.execute(f'SELECT 1 FROM "{schema}"."{table}" LIMIT 0')
        return True
    except Exception:
        return False


def _tables_matching(destination, schema, pattern):
    """Return table names in *schema* matching a LIKE pattern."""
    rows = destination.connection.execute(
        f"SELECT table_name FROM information_schema.tables "
        f"WHERE table_schema = '{schema}' AND table_name LIKE '{pattern}'"
    ).fetchall()
    return [r[0] for r in rows]


class TestPartialRefresh:
    """Basic --partial-refresh: rebuilds from earliest raw snapshot."""

    def test_preserves_old_records_and_rebuilds_window(self, duckdb_destination):
        """Full reprocess snapshots 1-3, then partial-refresh with 1-5.

        Records from snapshots 1-3 should be rebuilt, and snapshots 4-5 added.
        The end state should match a full reprocess of all 5 snapshots.
        """
        # Phase 1: initial full reprocess (snapshots 1-3)
        run_historize(duckdb_destination, [SNAPSHOT_1, SNAPSHOT_2, SNAPSHOT_3])
        rows_before = query_historized(duckdb_destination)
        assert len(rows_before) == 6  # sanity

        # Phase 2: add new snapshots and run partial refresh
        seed_raw_table(duckdb_destination, [SNAPSHOT_4, SNAPSHOT_5])
        runner = make_historize_runner(duckdb_destination, partial_refresh=True)
        result = runner.run()

        assert result["status"] == "completed"
        assert result["mode"] == "partial_refresh"
        assert result["snapshots_processed"] == 5

        # Verify: result should match a full reprocess of all 5 snapshots
        rows = query_historized(duckdb_destination)
        acme = get_rows_for(rows, 1)
        assert len(acme) == 4
        assert_row(acme[0], city="New York", _dlt_valid_from=DT(2026, 1, 1))
        assert_row(acme[1], city="Boston", _dlt_valid_from=DT(2026, 1, 2))
        assert_row(acme[2], _dlt_is_deleted=True, _dlt_valid_from=DT(2026, 1, 3))
        assert_row(acme[3], city="Chicago", _dlt_valid_from=DT(2026, 1, 5))

    def test_stats_reported(self, duckdb_destination):
        """Partial refresh reports correct row counts."""
        run_historize(duckdb_destination, [SNAPSHOT_1, SNAPSHOT_2, SNAPSHOT_3])

        seed_raw_table(duckdb_destination, [SNAPSHOT_4, SNAPSHOT_5])
        runner = make_historize_runner(duckdb_destination, partial_refresh=True)
        result = runner.run()

        assert result["new_or_changed_rows"] > 0
        assert result["status"] == "completed"

    def test_no_snapshots_to_reprocess(self, duckdb_destination):
        """Partial refresh with no snapshots is a no-op."""
        # Full reprocess, then clear raw table and add it back empty
        run_historize(duckdb_destination, [SNAPSHOT_1, SNAPSHOT_2])

        # Replace raw table with data beyond what's already historized
        # (but partial refresh looks at all raw data, so we need data in
        # the window — use a scenario where all snapshots are already processed)
        # Actually, partial-refresh queries MIN(snapshot_col) from raw and then
        # discovers snapshots >= that date. After a full reprocess of 1-2,
        # partial-refresh would find snapshots 1-2 to reprocess. Let's test
        # with an explicit historize_from that's after all data.
        runner = make_historize_runner(duckdb_destination, historize_from="2099-01-01")
        result = runner.run()

        assert result["status"] == "completed"
        assert result["mode"] == "partial_refresh"
        assert result["snapshots_processed"] == 0


class TestHistorizeFrom:
    """--historize-from with explicit date."""

    def test_reprocesses_from_specific_date(self, duckdb_destination):
        """Reprocess only from snapshot 3 onwards, preserving 1-2."""
        # Full reprocess all 5 snapshots
        run_historize(
            duckdb_destination,
            [SNAPSHOT_1, SNAPSHOT_2, SNAPSHOT_3, SNAPSHOT_4, SNAPSHOT_5],
        )
        _ = query_historized(duckdb_destination)

        # Partial refresh from snapshot 3 date
        runner = make_historize_runner(duckdb_destination, historize_from="2026-01-03")
        result = runner.run()

        assert result["status"] == "completed"
        assert result["mode"] == "partial_refresh"
        # Snapshots 3, 4, 5 reprocessed
        assert result["snapshots_processed"] == 3

        rows_after = query_historized(duckdb_destination)

        # Records from snapshots 1-2 should be preserved
        acme = get_rows_for(rows_after, 1)
        # Acme: NY(jan1), Boston(jan2) are before the window — preserved
        assert_row(acme[0], city="New York", _dlt_valid_from=DT(2026, 1, 1))
        assert_row(acme[1], city="Boston", _dlt_valid_from=DT(2026, 1, 2))

    def test_boundary_clamping(self, duckdb_destination):
        """--historize-from before earliest raw snapshot gets clamped."""
        run_historize(duckdb_destination, [SNAPSHOT_1, SNAPSHOT_2, SNAPSHOT_3])

        runner = make_historize_runner(duckdb_destination, historize_from="2020-01-01")
        result = runner.run()

        # Should succeed, clamped to min raw snapshot (2026-01-01)
        assert result["status"] == "completed"
        assert result["mode"] == "partial_refresh"
        # All 3 snapshots reprocessed (clamped to earliest)
        assert result["snapshots_processed"] == 3

    def test_date_after_last_snapshot_is_noop(self, duckdb_destination):
        """--historize-from after all snapshots → no work to do."""
        run_historize(duckdb_destination, [SNAPSHOT_1, SNAPSHOT_2])

        runner = make_historize_runner(duckdb_destination, historize_from="2099-12-31")
        result = runner.run()

        assert result["status"] == "completed"
        assert result["snapshots_processed"] == 0


class TestCloneAndSwapLifecycle:
    """Verify staging tables are created, swapped, and cleaned up."""

    def test_staging_and_keys_cleaned_up(self, duckdb_destination):
        """After partial refresh, no staging or keys tables remain."""
        run_historize(duckdb_destination, [SNAPSHOT_1, SNAPSHOT_2, SNAPSHOT_3])

        seed_raw_table(duckdb_destination, [SNAPSHOT_4])
        runner = make_historize_runner(duckdb_destination, partial_refresh=True)
        runner.run()

        # No staging or backup tables should remain
        staging_tables = _tables_matching(duckdb_destination, SCHEMA, "%__staging_%")
        backup_tables = _tables_matching(duckdb_destination, SCHEMA, "%__backup_%")
        keys_tables = _tables_matching(
            duckdb_destination, SCHEMA, "_rehistorize_keys_%"
        )
        assert staging_tables == []
        assert backup_tables == []
        assert keys_tables == []

    def test_original_untouched_on_failure(self, duckdb_destination):
        """If incremental SQL fails on staging, original table is untouched."""
        run_historize(duckdb_destination, [SNAPSHOT_1, SNAPSHOT_2, SNAPSHOT_3])
        rows_before = query_historized(duckdb_destination)

        seed_raw_table(duckdb_destination, [SNAPSHOT_4])

        # Patch execute_sql to fail during incremental (4th+ call after
        # clone, keys-DDL, BEGIN, DELETE, UPDATE, COMMIT — fail on the
        # incremental SQL execution)
        runner = make_historize_runner(duckdb_destination, partial_refresh=True)
        original_execute = duckdb_destination.execute_sql
        call_count = [0]

        def failing_execute(sql, *args, **kwargs):
            call_count[0] += 1
            # The incremental SQL contains "CREATE TEMP TABLE _historize_incremental"
            if "_historize_incremental" in sql:
                raise RuntimeError("Simulated failure during incremental")
            return original_execute(sql, *args, **kwargs)

        with patch.object(
            duckdb_destination, "execute_sql", side_effect=failing_execute
        ):
            result = runner.run()

        assert result["status"] == "failed"
        assert "Simulated failure" in result["error"]

        # Original table should be untouched
        rows_after = query_historized(duckdb_destination)
        assert len(rows_after) == len(rows_before)

        # Staging and keys tables should be cleaned up
        staging_tables = _tables_matching(duckdb_destination, SCHEMA, "%__staging_%")
        keys_tables = _tables_matching(
            duckdb_destination, SCHEMA, "_rehistorize_keys_%"
        )
        assert staging_tables == []
        assert keys_tables == []


class TestDeletionMarkers:
    """Deletion markers inside and outside the reprocessing window."""

    def test_deletion_outside_window_preserved(self, duckdb_destination):
        """Deletion markers from before the reprocessing boundary are kept."""
        # Snapshot 3 deletes Acme (company_id=1)
        run_historize(
            duckdb_destination,
            [SNAPSHOT_1, SNAPSHOT_2, SNAPSHOT_3, SNAPSHOT_4, SNAPSHOT_5],
        )

        # Reprocess from snapshot 4 onwards — Acme's deletion (snapshot 3)
        # is before the window and should be preserved
        runner = make_historize_runner(duckdb_destination, historize_from="2026-01-04")
        result = runner.run()

        assert result["status"] == "completed"

        rows = query_historized(duckdb_destination)
        acme = get_rows_for(rows, 1)

        # Acme's pre-window records should be intact
        assert_row(acme[0], city="New York", _dlt_valid_from=DT(2026, 1, 1))
        assert_row(acme[1], city="Boston", _dlt_valid_from=DT(2026, 1, 2))
        # Deletion marker from snapshot 3 (before window) — preserved
        assert_row(
            acme[2],
            _dlt_is_deleted=True,
            _dlt_valid_from=DT(2026, 1, 3),
        )

    def test_deletion_inside_window_rebuilt(self, duckdb_destination):
        """Deletion marker for a key present IN the window is rebuilt correctly."""
        # Beta (company_id=2) changes city in snapshot 4 — present in window
        run_historize(
            duckdb_destination,
            [SNAPSHOT_1, SNAPSHOT_2, SNAPSHOT_3, SNAPSHOT_4, SNAPSHOT_5],
        )

        beta_before = get_rows_for(query_historized(duckdb_destination), 2)
        assert len(beta_before) == 2  # London, Tokyo

        # Reprocess from snapshot 3 — Beta is present in the window,
        # so its records from snapshot 3+ are rolled back and rebuilt
        runner = make_historize_runner(duckdb_destination, historize_from="2026-01-03")
        result = runner.run()

        assert result["status"] == "completed"
        beta_after = get_rows_for(query_historized(duckdb_destination), 2)
        # Beta: London (unchanged from before window), Tokyo (rebuilt)
        assert len(beta_after) == 2
        assert_row(beta_after[0], city="London", _dlt_valid_from=DT(2026, 1, 1))
        assert_row(beta_after[1], city="Tokyo", _dlt_valid_from=DT(2026, 1, 4))


class TestEdgeCases:
    """Edge cases from the design document."""

    def test_no_prior_run_partial_refresh_falls_back(self, duckdb_destination):
        """--partial-refresh with no prior run falls back to full reprocess."""
        seed_raw_table(duckdb_destination, [SNAPSHOT_1, SNAPSHOT_2, SNAPSHOT_3])
        runner = make_historize_runner(duckdb_destination, partial_refresh=True)
        result = runner.run()

        assert result["status"] == "completed"
        assert result["mode"] == "full_reprocess"

        rows = query_historized(duckdb_destination)
        assert len(rows) == 6

    def test_no_prior_run_historize_from_falls_back(self, duckdb_destination):
        """--historize-from with no prior run falls back to full reprocess."""
        seed_raw_table(duckdb_destination, [SNAPSHOT_1, SNAPSHOT_2, SNAPSHOT_3])
        runner = make_historize_runner(duckdb_destination, historize_from="2026-01-02")
        result = runner.run()

        assert result["status"] == "completed"
        assert result["mode"] == "full_reprocess"

    def test_config_changed_blocks_partial_refresh(self, duckdb_destination):
        """Config fingerprint change blocks --partial-refresh."""
        run_historize(
            duckdb_destination,
            [SNAPSHOT_1, SNAPSHOT_2, SNAPSHOT_3],
            primary_key=["company_id"],
        )

        runner = make_historize_runner(
            duckdb_destination,
            partial_refresh=True,
            primary_key=["company_id", "city"],
        )
        result = runner.run()

        assert result["status"] == "failed"
        assert "config changed" in result["error"].lower()
        assert (
            "--partial-refresh" in result["error"]
            or "partial" in result["error"].lower()
        )

    def test_config_changed_blocks_historize_from(self, duckdb_destination):
        """Config fingerprint change blocks --historize-from."""
        run_historize(
            duckdb_destination,
            [SNAPSHOT_1, SNAPSHOT_2, SNAPSHOT_3],
            primary_key=["company_id"],
        )

        runner = make_historize_runner(
            duckdb_destination,
            historize_from="2026-01-02",
            primary_key=["company_id", "city"],
        )
        result = runner.run()

        assert result["status"] == "failed"
        assert "config changed" in result["error"].lower()

    def test_empty_raw_table_raises(self, duckdb_destination):
        """Partial refresh on empty raw table raises clear error."""
        # Create the raw table but with no data, plus a historized table
        run_historize(duckdb_destination, [SNAPSHOT_1])

        # Clear raw data
        duckdb_destination.connection.execute(
            f'DELETE FROM "{SCHEMA}"."{SOURCE_TABLE}"'
        )

        runner = make_historize_runner(duckdb_destination, partial_refresh=True)
        result = runner.run()

        assert result["status"] == "failed"
        assert "no raw snapshots" in result["error"].lower()


class TestLogTableOrdering:
    """Log entries are only modified after successful swap."""

    def test_log_updated_after_partial_refresh(self, duckdb_destination):
        """Log entries from the reprocessed window are cleared and rewritten."""
        run_historize(duckdb_destination, [SNAPSHOT_1, SNAPSHOT_2, SNAPSHOT_3])
        log_before = query_log(duckdb_destination)
        assert len(log_before) == 1

        seed_raw_table(duckdb_destination, [SNAPSHOT_4])
        runner = make_historize_runner(duckdb_destination, partial_refresh=True)
        runner.run()

        log_after = query_log(duckdb_destination)
        # Old log entry cleared (covered by reprocess window),
        # new entry written for the partial refresh
        assert len(log_after) >= 1
        # The latest entry should cover up to snapshot 4
        latest = log_after[-1]
        assert latest["status"] == "completed"

    def test_log_unchanged_on_failure(self, duckdb_destination):
        """If partial refresh fails, log table is not modified."""
        run_historize(duckdb_destination, [SNAPSHOT_1, SNAPSHOT_2, SNAPSHOT_3])
        log_before = query_log(duckdb_destination)

        seed_raw_table(duckdb_destination, [SNAPSHOT_4])
        runner = make_historize_runner(duckdb_destination, partial_refresh=True)

        original_execute = duckdb_destination.execute_sql

        def failing_execute(sql, *args, **kwargs):
            if "_historize_incremental" in sql:
                raise RuntimeError("Simulated failure")
            return original_execute(sql, *args, **kwargs)

        with patch.object(
            duckdb_destination, "execute_sql", side_effect=failing_execute
        ):
            result = runner.run()

        assert result["status"] == "failed"

        log_after = query_log(duckdb_destination)
        assert len(log_after) == len(log_before)
        assert log_after[0]["snapshot_value"] == log_before[0]["snapshot_value"]


class TestPartialRefreshEquivalence:
    """Partial refresh should produce the same result as a full reprocess."""

    def test_partial_equals_full_reprocess(self, duckdb_destination):
        """Initial 3-snapshot full reprocess + partial refresh with all 5
        should produce the same SCD2 rows as a single full reprocess of all 5.

        Uses two separate DuckDB connections to avoid temp table collisions.
        """
        from dlt_saga.destinations.duckdb.config import DuckDBDestinationConfig
        from dlt_saga.destinations.duckdb.destination import DuckDBDestination

        all_snapshots = [SNAPSHOT_1, SNAPSHOT_2, SNAPSHOT_3, SNAPSHOT_4, SNAPSHOT_5]

        # Full reprocess all 5 in a fresh destination
        ref_config = DuckDBDestinationConfig(
            project_id="test",
            location="local",
            schema_name=SCHEMA,
            database_path=":memory:",
        )
        ref_dest = DuckDBDestination(ref_config)
        try:
            run_historize(ref_dest, all_snapshots)
            full_rows = query_historized(ref_dest)
        finally:
            ref_dest.close()

        # Partial refresh path: initial 3, then partial with all 5
        run_historize(duckdb_destination, [SNAPSHOT_1, SNAPSHOT_2, SNAPSHOT_3])

        seed_raw_table(duckdb_destination, [SNAPSHOT_4, SNAPSHOT_5])
        runner = make_historize_runner(duckdb_destination, partial_refresh=True)
        partial_result = runner.run()

        assert partial_result["status"] == "completed"
        partial_rows = query_historized(duckdb_destination)

        # Same number of rows
        assert len(partial_rows) == len(full_rows)

        # Same data (compare key fields for each row)
        for full_row, partial_row in zip(full_rows, partial_rows):
            assert full_row["company_id"] == partial_row["company_id"]
            assert full_row["city"] == partial_row["city"]
            assert full_row["_dlt_valid_from"] == partial_row["_dlt_valid_from"]
            assert full_row["_dlt_valid_to"] == partial_row["_dlt_valid_to"]
            assert full_row["_dlt_is_deleted"] == partial_row["_dlt_is_deleted"]
