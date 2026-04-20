"""Integration tests for historize command using DuckDB destination."""

from datetime import datetime

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
