"""Integration tests for late-arriving snapshot detection and replay (DuckDB).

A snapshot that lands in the source after later snapshots have already been
historized sits at or below the incremental watermark, where watermark-only
discovery never looks. Detection compares the arrival column
(``_dlt_ingested_at``) against the previous run's start time; a hit rewinds
the target to the earliest late snapshot and replays every snapshot from that
boundary through the ordinary incremental path.

The raw table here carries a ``_dlt_ingested_at`` arrival column so detection
has something to compare. Rows seeded before a run get a fixed past arrival
stamp; late rows get a fresh ``now()`` stamp, which necessarily postdates
every prior run's start.
"""

from datetime import datetime, timezone

from tests.integration.conftest import (
    SCHEMA,
    SOURCE_TABLE,
    assert_row,
    get_rows_for,
    make_historize_runner,
    query_historized,
    query_log,
)

DT = datetime

OLD_ARRIVAL = "2026-01-01 00:00:00"


def seed_with_arrival(destination, rows, arrival=None):
    """Seed rows of (company_name, company_id, city, snapshot_ts)."""
    ddl = f"""
        CREATE SCHEMA IF NOT EXISTS "{SCHEMA}";
        CREATE TABLE IF NOT EXISTS "{SCHEMA}"."{SOURCE_TABLE}" (
            company_name VARCHAR,
            company_id INTEGER,
            city VARCHAR,
            snapshot_date TIMESTAMP,
            _dlt_ingested_at TIMESTAMP
        );
    """
    destination.execute_sql(ddl, SCHEMA)

    arrival_ts = arrival or datetime.now(timezone.utc).isoformat()
    for name, cid, city, snap in rows:
        destination.execute_sql(
            f"""
            INSERT INTO "{SCHEMA}"."{SOURCE_TABLE}" VALUES
            ('{name}', {cid}, '{city}', TIMESTAMP '{snap}',
             TIMESTAMP '{arrival_ts}')
            """,
            SCHEMA,
        )


def run(destination, full_refresh=False, **kwargs):
    """Run historize with late-arrival detection opted in (the feature under test)."""
    kwargs.setdefault("detect_late_arrivals", True)
    runner = make_historize_runner(destination, full_refresh=full_refresh, **kwargs)
    return runner.run()


class TestLateArrivals:
    def test_late_snapshot_replayed(self, duckdb_destination):
        """A snapshot arriving below the watermark is rewound and replayed."""
        d = duckdb_destination
        seed_with_arrival(
            d,
            [("A", 1, "New York", "2026-01-01"), ("B", 2, "London", "2026-01-01")],
            arrival=OLD_ARRIVAL,
        )
        assert run(d, full_refresh=True)["status"] == "completed"

        # A delivers 2026-01-02; B's file is late.
        seed_with_arrival(d, [("A", 1, "Boston", "2026-01-02")], arrival=OLD_ARRIVAL)
        result = run(d)
        assert result["status"] == "completed"
        assert result["snapshots_processed"] == 1  # watermark advances to 01-02

        # B's 2026-01-02 file lands after the watermark has passed it.
        seed_with_arrival(d, [("B", 2, "Tokyo", "2026-01-02")])
        result = run(d)
        assert result["status"] == "completed"
        assert result["mode"] == "incremental"
        assert result["snapshots_processed"] == 1  # replayed 2026-01-02

        rows = query_historized(d)
        b = get_rows_for(rows, 2)
        assert len(b) == 2
        assert_row(
            b[0],
            city="London",
            _dlt_valid_from=DT(2026, 1, 1),
            _dlt_valid_to=DT(2026, 1, 2),
        )
        assert_row(
            b[1], city="Tokyo", _dlt_valid_from=DT(2026, 1, 2), _dlt_valid_to=None
        )

        # A's history was replayed, not duplicated.
        a = get_rows_for(rows, 1)
        assert len(a) == 2
        assert_row(
            a[1], city="Boston", _dlt_valid_from=DT(2026, 1, 2), _dlt_valid_to=None
        )
        assert sum(1 for r in a if r["_dlt_valid_to"] is None) == 1

        # Watermark unchanged: the replay's max snapshot is still 2026-01-02.
        log = query_log(d)
        assert log[-1]["snapshot_value"].startswith("2026-01-02")

    def test_late_snapshot_between_processed(self, duckdb_destination):
        """Replay covers the late snapshot AND already-processed later ones."""
        d = duckdb_destination
        seed_with_arrival(d, [("A", 1, "New York", "2026-01-01")], arrival=OLD_ARRIVAL)
        assert run(d, full_refresh=True)["status"] == "completed"
        seed_with_arrival(d, [("A", 1, "Oslo", "2026-01-03")], arrival=OLD_ARRIVAL)
        assert run(d)["snapshots_processed"] == 1

        # A 2026-01-02 file lands late, between two processed snapshots.
        seed_with_arrival(d, [("A", 1, "Bergen", "2026-01-02")])
        result = run(d)
        assert result["status"] == "completed"
        assert result["snapshots_processed"] == 2  # replayed 01-02 and 01-03

        a = get_rows_for(query_historized(d), 1)
        assert [(r["city"], r["_dlt_valid_from"], r["_dlt_valid_to"]) for r in a] == [
            ("New York", DT(2026, 1, 1), DT(2026, 1, 2)),
            ("Bergen", DT(2026, 1, 2), DT(2026, 1, 3)),
            ("Oslo", DT(2026, 1, 3), None),
        ]

    def test_late_older_than_all_history(self, duckdb_destination):
        """Late data predating all history rebuilds cleanly (no baseline)."""
        d = duckdb_destination
        seed_with_arrival(d, [("A", 1, "Boston", "2026-01-02")], arrival=OLD_ARRIVAL)
        assert run(d, full_refresh=True)["status"] == "completed"

        seed_with_arrival(d, [("A", 1, "New York", "2026-01-01")])
        result = run(d)
        assert result["status"] == "completed"
        assert result["snapshots_processed"] == 2

        a = get_rows_for(query_historized(d), 1)
        assert [(r["city"], r["_dlt_valid_from"], r["_dlt_valid_to"]) for r in a] == [
            ("New York", DT(2026, 1, 1), DT(2026, 1, 2)),
            ("Boston", DT(2026, 1, 2), None),
        ]

    def test_no_false_positive_detection(self, duckdb_destination):
        """A quiet incremental run detects nothing and replays nothing."""
        d = duckdb_destination
        seed_with_arrival(d, [("A", 1, "New York", "2026-01-01")], arrival=OLD_ARRIVAL)
        assert run(d, full_refresh=True)["status"] == "completed"

        result = run(d)
        assert result["status"] == "completed"
        assert result["snapshots_processed"] == 0

    def test_default_warns_but_does_not_replay(self, duckdb_destination, caplog):
        """The default (unset) detects late arrivals and warns with the config
        suggestion, but never rewinds; explicit false silences detection."""
        import logging

        from dlt_saga.historize.config import HistorizeConfig

        assert HistorizeConfig(primary_key=["id"]).detect_late_arrivals is None

        d = duckdb_destination
        kwargs = dict(track_deletions=False)  # deletions orthogonal to this test
        seed_with_arrival(
            d,
            [("A", 1, "New York", "2026-01-01"), ("B", 2, "London", "2026-01-01")],
            arrival=OLD_ARRIVAL,
        )
        assert run(d, full_refresh=True, **kwargs)["status"] == "completed"
        seed_with_arrival(d, [("A", 1, "Boston", "2026-01-02")], arrival=OLD_ARRIVAL)
        assert run(d, **kwargs)["snapshots_processed"] == 1

        seed_with_arrival(d, [("B", 2, "Tokyo", "2026-01-02")])
        runner = make_historize_runner(d, track_deletions=False)  # default: unset
        with caplog.at_level(logging.WARNING):
            result = runner.run()
        assert result["snapshots_processed"] == 0
        assert "late-arriving snapshot(s) found" in caplog.text
        assert "detect_late_arrivals: true" in caplog.text

        b = get_rows_for(query_historized(d), 2)
        assert len(b) == 1  # still only the 2026-01-01 version, no Tokyo
        assert b[0]["city"] == "London"

        # Explicit false: detection (and the warning) fully off.
        caplog.clear()
        runner = make_historize_runner(
            d, track_deletions=False, detect_late_arrivals=False
        )
        with caplog.at_level(logging.WARNING):
            result = runner.run()
        assert result["snapshots_processed"] == 0
        assert "late-arriving" not in caplog.text

    def test_late_arrival_window_bounds_replay(self, duckdb_destination):
        """Late snapshots inside late_arrival_window_days replay; older ones
        are ignored (warned) and never enter history."""
        d = duckdb_destination
        kwargs = dict(late_arrival_window_days=5)
        seed_with_arrival(d, [("A", 1, "New York", "2026-01-01")], arrival=OLD_ARRIVAL)
        assert run(d, full_refresh=True, **kwargs)["status"] == "completed"
        seed_with_arrival(d, [("A", 1, "Oslo", "2026-01-10")], arrival=OLD_ARRIVAL)
        assert run(d, **kwargs)["snapshots_processed"] == 1  # watermark -> 01-10

        # Two late files: 01-08 is within 5 days of the watermark, 01-02 is not.
        seed_with_arrival(
            d,
            [("A", 1, "Bergen", "2026-01-08"), ("A", 1, "Stavanger", "2026-01-02")],
        )
        result = run(d, **kwargs)
        assert result["status"] == "completed"
        assert result["snapshots_processed"] == 2  # replayed 01-08 and 01-10

        a = get_rows_for(query_historized(d), 1)
        cities = [r["city"] for r in a]
        assert "Stavanger" not in cities  # out-of-window late file ignored
        assert [(r["city"], r["_dlt_valid_from"], r["_dlt_valid_to"]) for r in a] == [
            ("New York", DT(2026, 1, 1), DT(2026, 1, 8)),
            ("Bergen", DT(2026, 1, 8), DT(2026, 1, 10)),
            ("Oslo", DT(2026, 1, 10), None),
        ]

    def test_replay_refused_when_raw_expired(self, duckdb_destination):
        """The retention guard: if raw lost a historized snapshot inside the
        replay window (partition expiration), the rewind is refused and the
        target left untouched."""
        d = duckdb_destination
        seed_with_arrival(d, [("A", 1, "New York", "2026-01-01")], arrival=OLD_ARRIVAL)
        assert run(d, full_refresh=True)["status"] == "completed"
        seed_with_arrival(d, [("A", 1, "Oslo", "2026-01-03")], arrival=OLD_ARRIVAL)
        assert run(d)["snapshots_processed"] == 1

        # Simulate partition expiration: raw loses the already-historized
        # 2026-01-03 snapshot, then a late 2026-01-02 file lands.
        d.execute_sql(
            f"""DELETE FROM "{SCHEMA}"."{SOURCE_TABLE}"
                WHERE snapshot_date = TIMESTAMP '2026-01-03'""",
            SCHEMA,
        )
        seed_with_arrival(d, [("A", 1, "Bergen", "2026-01-02")])
        result = run(d)
        assert result["status"] == "completed"
        assert result["snapshots_processed"] == 0  # replay refused, no rewind

        # Target unchanged: no Bergen version, Oslo still open.
        a = get_rows_for(query_historized(d), 1)
        assert [(r["city"], r["_dlt_valid_to"]) for r in a] == [
            ("New York", DT(2026, 1, 3)),
            ("Oslo", None),
        ]

    def test_late_partition_with_merge_key(self, duckdb_destination):
        """The motivating scenario: independently-delivered partitions.

        merge_key scopes deletion detection per feed, so B is not marked
        deleted when only A delivers a snapshot — and B's late file for that
        snapshot is replayed into proper versions when it lands.
        """
        d = duckdb_destination
        kwargs = dict(
            primary_key=["company_name", "company_id"],
            merge_key=["company_name"],
        )
        seed_with_arrival(
            d,
            [("A", 1, "New York", "2026-01-01"), ("B", 2, "London", "2026-01-01")],
            arrival=OLD_ARRIVAL,
        )
        assert run(d, full_refresh=True, **kwargs)["status"] == "completed"

        seed_with_arrival(d, [("A", 1, "Boston", "2026-01-02")], arrival=OLD_ARRIVAL)
        assert run(d, **kwargs)["snapshots_processed"] == 1

        seed_with_arrival(d, [("B", 2, "Tokyo", "2026-01-02")])
        result = run(d, **kwargs)
        assert result["status"] == "completed"
        assert result["snapshots_processed"] == 1

        rows = query_historized(d)
        b = get_rows_for(rows, 2)
        # No deletion marker for B anywhere; its late change is versioned.
        assert not any(r["_dlt_is_deleted"] for r in b)
        assert [(r["city"], r["_dlt_valid_from"], r["_dlt_valid_to"]) for r in b] == [
            ("London", DT(2026, 1, 1), DT(2026, 1, 2)),
            ("Tokyo", DT(2026, 1, 2), None),
        ]
