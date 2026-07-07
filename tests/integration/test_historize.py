"""Integration tests for historize command using DuckDB destination."""

from datetime import datetime

from dlt_saga.historize.state import HistorizeLogEntry
from tests.integration.conftest import (
    SNAPSHOT_1,
    SNAPSHOT_2,
    SNAPSHOT_3,
    SNAPSHOT_4,
    SNAPSHOT_5,
    assert_row,
    get_rows_for,
    make_historize_runner,
    query_historized,
    query_log,
    run_historize,
    seed_raw_table,
)

DT = datetime


class TestFullReprocess:
    """Test full reprocess historization."""

    def test_full_reprocess_basic(self, duckdb_destination):
        """3 snapshots → 6 SCD2 rows with correct validity periods.

        Deletions produce a separate marker row rather than flagging the
        closed row.
        """
        result = run_historize(duckdb_destination, [SNAPSHOT_1, SNAPSHOT_2, SNAPSHOT_3])

        assert result["status"] == "completed"
        assert result["mode"] == "full_reprocess"

        rows = query_historized(duckdb_destination)
        assert len(rows) == 6

        acme = get_rows_for(rows, 1)
        assert len(acme) == 3
        assert_row(
            acme[0],
            city="New York",
            _dlt_valid_from=DT(2026, 1, 1),
            _dlt_valid_to=DT(2026, 1, 2),
            _dlt_is_deleted=False,
        )
        assert_row(
            acme[1],
            city="Boston",
            _dlt_valid_from=DT(2026, 1, 2),
            _dlt_valid_to=DT(2026, 1, 3),
            _dlt_is_deleted=False,
        )
        # Separate deletion marker row (open-ended — Acme never returns)
        assert_row(
            acme[2],
            city="Boston",
            _dlt_valid_from=DT(2026, 1, 3),
            _dlt_valid_to=None,
            _dlt_is_deleted=True,
        )

        assert_row(
            get_rows_for(rows, 2)[0],
            city="London",
            _dlt_valid_to=None,
            _dlt_is_deleted=False,
        )
        assert_row(
            get_rows_for(rows, 3)[0],
            city="Paris",
            _dlt_valid_to=None,
            _dlt_is_deleted=False,
        )
        assert_row(
            get_rows_for(rows, 4)[0],
            city="Berlin",
            _dlt_valid_from=DT(2026, 1, 3),
            _dlt_valid_to=None,
            _dlt_is_deleted=False,
        )

    def test_stats_in_log(self, duckdb_destination):
        """Verify _saga_historize_log has correct stats after full reprocess."""
        run_historize(duckdb_destination, [SNAPSHOT_1, SNAPSHOT_2, SNAPSHOT_3])

        log = query_log(duckdb_destination)
        assert len(log) == 1
        assert_row(
            log[0],
            pipeline_name="test__raw_companies",
            status="completed",
            new_or_changed_rows=5,
            deleted_rows=1,
        )


class TestEmptySource:
    """An empty/fully-filtered source must not crash or poison state (#2)."""

    def test_empty_source_records_no_baseline(self, duckdb_destination):
        """Full reprocess over an empty source completes with 0 rows and writes
        no log entry — a NULL-snapshot 'completed' row would poison state."""
        seed_raw_table(duckdb_destination, [])  # create the table, no rows
        result = make_historize_runner(duckdb_destination, full_refresh=True).run()

        assert result["status"] == "completed"
        assert result["mode"] == "full_reprocess"
        assert result["new_or_changed_rows"] == 0
        assert result["deleted_rows"] == 0

        # No baseline recorded, so the next run is free to reprocess.
        assert query_log(duckdb_destination) == []

    def test_empty_then_populated_source(self, duckdb_destination):
        """After an empty run, a later run with data historizes normally — the
        old code crashed here with escape_string_literal(None)."""
        seed_raw_table(duckdb_destination, [])
        make_historize_runner(duckdb_destination, full_refresh=True).run()

        # In production each run is a fresh process; here both runs share one
        # DuckDB connection, so drop the connection-scoped full-reprocess temp
        # table to simulate that process boundary (it is otherwise re-used across
        # both full reprocesses and collides — a DuckDB-test-only artifact).
        duckdb_destination.connection.execute("DROP TABLE IF EXISTS _historize_result")

        seed_raw_table(duckdb_destination, [SNAPSHOT_1, SNAPSHOT_2, SNAPSHOT_3])
        result = make_historize_runner(duckdb_destination, full_refresh=False).run()

        assert result["status"] == "completed"
        # No usable baseline existed, so this is a full reprocess, not incremental.
        assert result["mode"] == "full_reprocess"
        assert len(query_historized(duckdb_destination)) == 6

    def test_recovers_from_poisoned_null_log_entry(self, duckdb_destination):
        """A pre-existing NULL-snapshot 'completed' entry (left by the old buggy
        empty-source path) is ignored as a baseline, so the next run self-heals
        instead of crashing."""
        seed_raw_table(duckdb_destination, [SNAPSHOT_1, SNAPSHOT_2, SNAPSHOT_3])
        runner = make_historize_runner(duckdb_destination, full_refresh=False)

        # Reproduce the poisoned state a pre-fix empty run left behind.
        runner.state_manager.ensure_log_table()
        runner.state_manager.write_log_entry(
            HistorizeLogEntry(
                pipeline_name="test__raw_companies",
                source_table=runner.source_table_id,
                target_table=runner.target_table_id,
                snapshot_value=None,
                new_or_changed_rows=0,
                deleted_rows=0,
                config_fingerprint=runner.state_manager.compute_fingerprint(
                    runner.config
                ),
                is_full_reprocess=True,
                started_at=DT(2026, 1, 1),
                finished_at=DT(2026, 1, 1),
                status="completed",
            )
        )

        result = runner.run()

        assert result["status"] == "completed"
        assert result["mode"] == "full_reprocess"
        assert len(query_historized(duckdb_destination)) == 6


class TestIncremental:
    """Test incremental historization."""

    def test_incremental_adds_new_rows(self, duckdb_destination):
        """Full reprocess 3 snapshots, add snapshot 4, incremental adds new rows."""
        run_historize(duckdb_destination, [SNAPSHOT_1, SNAPSHOT_2, SNAPSHOT_3])

        seed_raw_table(duckdb_destination, [SNAPSHOT_4])
        runner = make_historize_runner(duckdb_destination, full_refresh=False)
        result = runner.run()

        assert result["status"] == "completed"
        assert result["mode"] == "incremental"
        assert result["snapshots_processed"] == 1

        rows = query_historized(duckdb_destination)
        # Acme(3: NY, Boston, deletion) + Beta(2: London→Tokyo)
        # + Charlie(2: Paris closed + deletion) + Delta(1) + Gamma(1: new) = 9
        assert len(rows) == 9

    def test_no_new_snapshots(self, duckdb_destination):
        """Incremental with no new data returns 0 snapshots processed."""
        run_historize(duckdb_destination, [SNAPSHOT_1, SNAPSHOT_2, SNAPSHOT_3])

        runner = make_historize_runner(duckdb_destination, full_refresh=False)
        result = runner.run()

        assert result["status"] == "completed"
        assert result["snapshots_processed"] == 0


class TestDeletionDetection:
    """Test deletion tracking behavior."""

    def test_deletion_sets_is_deleted(self, duckdb_destination):
        """Disappeared keys get _dlt_is_deleted=True."""
        run_historize(duckdb_destination, [SNAPSHOT_1, SNAPSHOT_2, SNAPSHOT_3])

        rows = query_historized(duckdb_destination)
        deleted = [r for r in rows if r["_dlt_is_deleted"] is True]
        assert len(deleted) == 1
        assert_row(deleted[0], company_id=1, city="Boston")

    def test_track_deletions_disabled(self, duckdb_destination):
        """With track_deletions=False, no deletion records are created."""
        run_historize(
            duckdb_destination,
            [SNAPSHOT_1, SNAPSHOT_2, SNAPSHOT_3],
            track_deletions=False,
        )

        rows = query_historized(duckdb_destination)
        deleted = [r for r in rows if r["_dlt_is_deleted"] is True]
        assert len(deleted) == 0

    def test_reappearance_after_deletion(self, duckdb_destination):
        """Key deleted in snapshot 3, reappears in snapshot 5 with new values."""
        run_historize(
            duckdb_destination,
            [SNAPSHOT_1, SNAPSHOT_2, SNAPSHOT_3, SNAPSHOT_4, SNAPSHOT_5],
        )

        acme = get_rows_for(query_historized(duckdb_destination), 1)
        assert len(acme) == 4
        assert_row(acme[0], city="New York", _dlt_is_deleted=False)
        assert_row(
            acme[1], city="Boston", _dlt_is_deleted=False, _dlt_valid_to=DT(2026, 1, 3)
        )
        # Deletion marker closed by reappearance in snapshot 5
        assert_row(
            acme[2],
            city="Boston",
            _dlt_is_deleted=True,
            _dlt_valid_from=DT(2026, 1, 3),
            _dlt_valid_to=DT(2026, 1, 5),
        )
        assert_row(
            acme[3],
            city="Chicago",
            _dlt_is_deleted=False,
            _dlt_valid_to=None,
            _dlt_valid_from=DT(2026, 1, 5),
        )


class TestMultiSnapshotIncremental:
    """Test incremental with multiple snapshots in one batch."""

    def test_deletion_and_reappearance_in_same_batch(self, duckdb_destination):
        """Key disappears in snapshot 3, reappears in snapshot 5 — both in one batch."""
        # Full reprocess with snapshots 1-2 (no deletions yet)
        run_historize(duckdb_destination, [SNAPSHOT_1, SNAPSHOT_2])

        # Process snapshots 3-5 incrementally in one batch:
        # snapshot 3: Acme disappears, Delta appears
        # snapshot 4: Beta changes, Charlie disappears, Gamma appears
        # snapshot 5: Acme reappears with new city
        seed_raw_table(duckdb_destination, [SNAPSHOT_3, SNAPSHOT_4, SNAPSHOT_5])
        runner = make_historize_runner(duckdb_destination, full_refresh=False)
        result = runner.run()

        assert result["status"] == "completed"
        assert result["mode"] == "incremental"
        assert result["snapshots_processed"] == 3

        acme = get_rows_for(query_historized(duckdb_destination), 1)
        assert len(acme) == 4
        # Original: New York (closed by value change)
        assert_row(
            acme[0],
            city="New York",
            _dlt_valid_from=DT(2026, 1, 1),
            _dlt_valid_to=DT(2026, 1, 2),
            _dlt_is_deleted=False,
        )
        # Boston: closed by disappearance in snapshot 3
        assert_row(
            acme[1],
            city="Boston",
            _dlt_valid_from=DT(2026, 1, 2),
            _dlt_valid_to=DT(2026, 1, 3),
            _dlt_is_deleted=False,
        )
        # Deletion marker: closed by reappearance in snapshot 5
        assert_row(
            acme[2],
            city="Boston",
            _dlt_valid_from=DT(2026, 1, 3),
            _dlt_valid_to=DT(2026, 1, 5),
            _dlt_is_deleted=True,
        )
        # Reappearance: Chicago, open
        assert_row(
            acme[3],
            city="Chicago",
            _dlt_valid_from=DT(2026, 1, 5),
            _dlt_valid_to=None,
            _dlt_is_deleted=False,
        )


class TestConfigChanges:
    """Test config change detection."""

    def test_pk_change_requires_full_refresh(self, duckdb_destination):
        """Changing PK without --full-refresh fails with config change error."""
        run_historize(
            duckdb_destination,
            [SNAPSHOT_1, SNAPSHOT_2, SNAPSHOT_3],
            primary_key=["company_id"],
        )

        runner = make_historize_runner(
            duckdb_destination,
            full_refresh=False,
            primary_key=["company_id", "city"],
        )
        result = runner.run()

        assert result["status"] == "failed"
        assert "config changed" in result["error"].lower()

    def test_column_rename_requires_full_refresh(self, duckdb_destination):
        """Renaming an SCD2 output column without --full-refresh is caught by the
        config fingerprint (would otherwise emit SQL against columns the existing
        historized table doesn't have)."""
        run_historize(
            duckdb_destination,
            [SNAPSHOT_1, SNAPSHOT_2, SNAPSHOT_3],
        )

        runner = make_historize_runner(
            duckdb_destination,
            full_refresh=False,
            valid_to_column="valid_to",
        )
        result = runner.run()

        assert result["status"] == "failed"
        assert "config changed" in result["error"].lower()


class TestCustomColumnNames:
    """End-to-end historization with renamed SCD2 output columns."""

    def test_full_reprocess_with_renamed_columns(self, duckdb_destination):
        result = run_historize(
            duckdb_destination,
            [SNAPSHOT_1, SNAPSHOT_2, SNAPSHOT_3],
            valid_from_column="valid_from",
            valid_to_column="valid_to",
            is_deleted_column="is_deleted",
        )
        assert result["status"] == "completed"

        rows = query_historized(duckdb_destination, order_by="company_id, valid_from")
        assert rows, "expected historized rows"

        # Renamed columns present; defaults absent.
        assert {"valid_from", "valid_to", "is_deleted"}.issubset(rows[0].keys())
        for default in ("_dlt_valid_from", "_dlt_valid_to", "_dlt_is_deleted"):
            assert default not in rows[0]

        # SCD2 semantics still correct: Acme (id=1) is deleted in SNAPSHOT_3, so it
        # has an open deletion marker carrying the renamed flag.
        acme = get_rows_for(rows, 1)
        assert any(r["is_deleted"] and r["valid_to"] is None for r in acme)
