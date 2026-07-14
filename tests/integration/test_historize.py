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

        # Both runs share one DuckDB connection (in production each is a fresh
        # process). The full-reprocess temp table is CREATE OR REPLACE, so the
        # second reprocess re-creates it cleanly rather than colliding.
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


class TestCrossCatalogSource:
    """An external source in another catalog is read from that catalog (#2).

    `source_database` already qualified column discovery; the data read now uses
    it too, instead of silently hitting the destination's own catalog. Proven
    end-to-end on DuckDB via an attached in-memory catalog.
    """

    def test_external_source_read_from_other_catalog(self, duckdb_destination):
        from dlt_saga.historize.config import HistorizeConfig
        from dlt_saga.historize.runner import HistorizeRunner
        from tests.integration.conftest import SCHEMA

        conn = duckdb_destination.connection
        # A separate catalog holding the external delivery.
        conn.execute("ATTACH ':memory:' AS ext_cat")
        conn.execute("CREATE SCHEMA ext_cat.ext")
        conn.execute(
            "CREATE TABLE ext_cat.ext.orders_raw "
            "(order_id INTEGER, status VARCHAR, snapshot_date TIMESTAMP)"
        )
        conn.execute(
            "INSERT INTO ext_cat.ext.orders_raw VALUES "
            "(1, 'new', TIMESTAMP '2026-01-01'), "
            "(1, 'shipped', TIMESTAMP '2026-01-02')"
        )

        runner = HistorizeRunner(
            pipeline_name="test__ext_orders",
            historize_config=HistorizeConfig(
                primary_key=["order_id"], snapshot_column="snapshot_date"
            ),
            destination=duckdb_destination,
            database="memory",  # the destination's own default catalog
            schema=SCHEMA,
            source_table_name="orders_raw",
            target_table_name="ext_orders_historized",
            config_dict={
                "write_disposition": "historize",
                "primary_key": ["order_id"],
                "source_table": "orders_raw",
                "source_schema": "ext",
                "source_database": "ext_cat",  # a different catalog
            },
        )

        # The source read is qualified with the external catalog.
        assert runner.source_table_id == '"ext_cat"."ext"."orders_raw"'

        result = runner.run()
        assert result["status"] == "completed"

        # The status change was historized — i.e. the read actually hit the
        # external-catalog table (not an own-catalog table, which doesn't exist).
        rows = duckdb_destination.connection.execute(
            f'SELECT status, _dlt_valid_to FROM "{SCHEMA}"."ext_orders_historized" '
            "ORDER BY _dlt_valid_from"
        ).fetchall()
        statuses = [r[0] for r in rows]
        assert "new" in statuses
        assert "shipped" in statuses


class TestFullReprocessSnapshotOrdering:
    """The recorded baseline must match what was actually reprocessed (#3-med).

    If the max snapshot is read *after* the reprocess, a snapshot landing in
    between gets recorded as processed but was never historized — the next run
    discovers nothing past it and it is skipped forever. Resolving the max first
    and bounding the reprocess by it closes that window.
    """

    def test_snapshot_arriving_mid_run_not_lost(self, duckdb_destination):
        from unittest.mock import patch

        # max source snapshot at start = 2026-01-02
        seed_raw_table(duckdb_destination, [SNAPSHOT_1, SNAPSHOT_2])
        runner = make_historize_runner(duckdb_destination, full_refresh=True)

        original = duckdb_destination.execute_sql
        injected = {"done": False}

        def hook(sql, *args, **kwargs):
            result = original(sql, *args, **kwargs)
            # Simulate a newer snapshot landing right after the reprocess read.
            # With the old ordering the subsequent max read would pick it up and
            # record it as the baseline though it was never processed.
            if not injected["done"] and "_historize_result" in sql:
                injected["done"] = True
                seed_raw_table(duckdb_destination, [SNAPSHOT_3])  # 2026-01-03
            return result

        with patch.object(duckdb_destination, "execute_sql", side_effect=hook):
            result = runner.run()

        assert result["status"] == "completed"
        assert injected["done"]  # the reprocess ran and we injected afterwards

        # Baseline is the pre-injection max, not the snapshot that arrived mid-run.
        log = query_log(duckdb_destination)
        assert len(log) == 1
        assert "2026-01-02" in str(log[-1]["snapshot_value"])
        assert "2026-01-03" not in str(log[-1]["snapshot_value"])

        # Delta (company 4, introduced in snapshot 3) was not historized yet.
        assert get_rows_for(query_historized(duckdb_destination), 4) == []

        # A subsequent incremental run picks up the snapshot that arrived mid-run
        # (it is > the recorded baseline), rather than skipping it forever.
        duckdb_destination.connection.execute("DROP TABLE IF EXISTS _historize_result")
        r2 = make_historize_runner(duckdb_destination, full_refresh=False).run()
        assert r2["status"] == "completed"
        assert r2["mode"] == "incremental"
        assert get_rows_for(query_historized(duckdb_destination), 4) != []


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


def _crash_before_log_write(*args, **kwargs):
    """Stand-in for write_log_entry that fails as if the process died.

    Simulates a crash after the incremental SQL commits but before the watermark
    advances: run() catches it into a failed result, no completed log entry is
    written, so the next run re-discovers the same snapshot batch and re-runs the
    incremental SQL in place — the scenario the rollback prefix must survive.
    """
    raise RuntimeError("simulated crash before log write")


class TestIncrementalIdempotency:
    """Incremental historization is re-entrant across a crash + retry.

    Without the rollback prefix, a retry of an already-applied snapshot batch
    duplicates every change/deletion row (the MERGE's `n.valid_from > t.valid_from`
    is equal, not greater, for the rows the crashed run inserted, so it won't
    re-close them, and the INSERT blindly re-inserts). Each of these asserts the
    retried table matches the clean single-run result — no duplicated history.
    """

    def test_crash_then_retry_produces_no_duplicates(self, duckdb_destination):
        """Crash after the incremental SQL commits → retry converges to 6 rows."""
        # Baseline full reprocess over snapshots 1-2 (records 2026-01-02 as baseline).
        run_historize(duckdb_destination, [SNAPSHOT_1, SNAPSHOT_2])

        # New snapshot 3; run incrementally but "crash" before the watermark advances.
        seed_raw_table(duckdb_destination, [SNAPSHOT_3])
        crashed = make_historize_runner(duckdb_destination, full_refresh=False)
        crashed.state_manager.write_log_entry = _crash_before_log_write
        assert crashed.run()["status"] == "failed"

        # The SQL committed: snapshot-3 rows are already in the target (same end
        # state as a full reprocess of 1-3, i.e. 6 rows). The watermark did NOT
        # advance, so the batch is re-discoverable.
        assert len(query_historized(duckdb_destination)) == 6

        # Retry with a fresh runner: re-discovers snapshot 3 and re-runs in place.
        retry = make_historize_runner(duckdb_destination, full_refresh=False)
        result = retry.run()
        assert result["status"] == "completed"
        assert result["mode"] == "incremental"
        assert result["snapshots_processed"] == 1

        rows = query_historized(duckdb_destination)
        assert len(rows) == 6  # not 6 + re-inserted duplicates
        dedup_keys = [
            (r["company_id"], r["_dlt_valid_from"], r["_dlt_is_deleted"]) for r in rows
        ]
        assert len(dedup_keys) == len(set(dedup_keys)), (
            "duplicate SCD2 rows after retry"
        )

        # Acme's SCD2 chain is intact: NY → Boston → deletion marker (open).
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
        assert_row(
            acme[2],
            city="Boston",
            _dlt_valid_from=DT(2026, 1, 3),
            _dlt_valid_to=None,
            _dlt_is_deleted=True,
        )

    def test_clean_incremental_prefix_is_noop(self, duckdb_destination):
        """On a clean run the prefix is a no-op — result unchanged (6 rows)."""
        run_historize(duckdb_destination, [SNAPSHOT_1, SNAPSHOT_2])
        seed_raw_table(duckdb_destination, [SNAPSHOT_3])
        result = make_historize_runner(duckdb_destination, full_refresh=False).run()
        assert result["status"] == "completed"
        assert len(query_historized(duckdb_destination)) == 6

    def test_multi_snapshot_batch_retry_is_stable(self, duckdb_destination):
        """A disappear+reappear batch survives crash + retry without duplication."""
        run_historize(duckdb_destination, [SNAPSHOT_1, SNAPSHOT_2])

        seed_raw_table(duckdb_destination, [SNAPSHOT_3, SNAPSHOT_4, SNAPSHOT_5])
        crashed = make_historize_runner(duckdb_destination, full_refresh=False)
        crashed.state_manager.write_log_entry = _crash_before_log_write
        assert crashed.run()["status"] == "failed"

        retry = make_historize_runner(duckdb_destination, full_refresh=False)
        assert retry.run()["status"] == "completed"

        rows = query_historized(duckdb_destination)
        dedup_keys = [
            (r["company_id"], r["_dlt_valid_from"], r["_dlt_is_deleted"]) for r in rows
        ]
        assert len(dedup_keys) == len(set(dedup_keys)), (
            "duplicate SCD2 rows after retry"
        )

        # Acme's full chain is identical to the clean multi-snapshot batch result.
        acme = get_rows_for(rows, 1)
        assert len(acme) == 4
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
        assert_row(
            acme[2],
            city="Boston",
            _dlt_valid_from=DT(2026, 1, 3),
            _dlt_valid_to=DT(2026, 1, 5),
            _dlt_is_deleted=True,
        )
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
