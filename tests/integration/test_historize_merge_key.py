"""Integration test for historize.merge_key — scoped deletion / reappearance.

Scenario: a source unions two upstream instances ("A" and "B") that snapshot
independently into the same table. Without merge_key, a snapshot date present
in B would mark every absent key in A as deleted on that date. With
``merge_key: [instance]``, deletion detection is scoped per-instance and A's
rows survive the gap intact.
"""

import pytest

from dlt_saga.destinations.duckdb.config import DuckDBDestinationConfig
from dlt_saga.destinations.duckdb.destination import DuckDBDestination
from dlt_saga.historize.config import HistorizeConfig
from dlt_saga.historize.runner import HistorizeRunner

SCHEMA = "test_mk"
SOURCE_TABLE = "raw_orders"
TARGET_TABLE = "raw_orders_historized"


@pytest.fixture
def duckdb_dest():
    cfg = DuckDBDestinationConfig(
        project_id="test",
        location="local",
        schema_name=SCHEMA,
        database_path=":memory:",
    )
    dest = DuckDBDestination(cfg)
    yield dest
    dest.close()


def _seed(dest, rows):
    """Seed (instance, id, status, snapshot_date) rows into the source table."""
    dest.execute_sql(
        f'CREATE SCHEMA IF NOT EXISTS "{SCHEMA}"',
        SCHEMA,
    )
    dest.execute_sql(
        f"""
        CREATE TABLE IF NOT EXISTS "{SCHEMA}"."{SOURCE_TABLE}" (
            instance VARCHAR,
            id INTEGER,
            status VARCHAR,
            snapshot_date TIMESTAMP
        )
        """,
        SCHEMA,
    )
    for instance, oid, status, day in rows:
        dest.execute_sql(
            f"""
            INSERT INTO "{SCHEMA}"."{SOURCE_TABLE}"
            VALUES ('{instance}', {oid}, '{status}', TIMESTAMP '{day}')
            """,
            SCHEMA,
        )


def _make_runner(dest, *, merge_key=None):
    config = HistorizeConfig(
        primary_key=["instance", "id"],
        snapshot_column="snapshot_date",
        track_deletions=True,
        merge_key=list(merge_key) if merge_key else None,
    )
    return HistorizeRunner(
        pipeline_name="test__raw_orders",
        historize_config=config,
        destination=dest,
        database="test",
        schema=SCHEMA,
        source_table_name=SOURCE_TABLE,
        target_table_name=TARGET_TABLE,
        config_dict={
            "write_disposition": "historize",
            "primary_key": ["instance", "id"],
        },
        full_refresh=True,
    )


def _query_historized(dest):
    sql = f"""
        SELECT instance, id, status, _dlt_valid_from, _dlt_valid_to, _dlt_is_deleted
        FROM "{SCHEMA}"."{TARGET_TABLE}"
        ORDER BY instance, id, _dlt_valid_from
    """
    conn = dest.connection
    result = conn.execute(sql)
    cols = [c[0] for c in result.description]
    return [dict(zip(cols, r)) for r in result.fetchall()]


# Two instances. Instance A delivers Day 1, 3 (skips Day 2).
# Instance B delivers Day 1, 2 (skips Day 3). Neither truly deletes any row.
ROWS = [
    # Day 1 — both deliver
    ("A", 1, "pending", "2026-01-01"),
    ("A", 2, "shipped", "2026-01-01"),
    ("B", 10, "pending", "2026-01-01"),
    ("B", 11, "shipped", "2026-01-01"),
    # Day 2 — only B
    ("B", 10, "pending", "2026-01-02"),
    ("B", 11, "shipped", "2026-01-02"),
    # Day 3 — only A (A=1 changes status to 'shipped')
    ("A", 1, "shipped", "2026-01-03"),
    ("A", 2, "shipped", "2026-01-03"),
]


def _deletions_for(rows, instance):
    return [r for r in rows if r["instance"] == instance and r["_dlt_is_deleted"]]


class TestMergeKeyWithoutScope:
    """Without merge_key, the absence of A's snapshot on Day 2 is treated as
    A's keys having disappeared on Day 2 — a false positive."""

    def test_unscoped_run_marks_a_as_deleted_on_skipped_day(self, duckdb_dest):
        _seed(duckdb_dest, ROWS)
        result = _make_runner(duckdb_dest).run()
        assert result["status"] == "completed"
        historized = _query_historized(duckdb_dest)
        # Bug-by-design: A's keys appear as deleted somewhere along the way
        # because instance B has a Day-2 snapshot that A is "missing" from.
        a_deletions = _deletions_for(historized, "A")
        assert a_deletions, (
            "expected at least one false-positive deletion for instance A — "
            "scenario premise broken if not"
        )


class TestMergeKeyScopedDeletion:
    """With ``merge_key: [instance]``, sibling-instance snapshots no longer
    drive deletion detection for this instance."""

    def test_a_not_flagged_deleted_when_only_b_delivered(self, duckdb_dest):
        _seed(duckdb_dest, ROWS)
        result = _make_runner(duckdb_dest, merge_key=["instance"]).run()
        assert result["status"] == "completed"
        historized = _query_historized(duckdb_dest)
        # No instance was actually deleted in the source — neither should
        # appear with _dlt_is_deleted=True anywhere.
        assert _deletions_for(historized, "A") == []
        assert _deletions_for(historized, "B") == []

    def test_real_value_change_still_detected(self, duckdb_dest):
        """A=1 changed status pending→shipped between Day 1 and Day 3.
        That's a real value change and must still produce a closed historical row."""
        _seed(duckdb_dest, ROWS)
        _make_runner(duckdb_dest, merge_key=["instance"]).run()
        historized = _query_historized(duckdb_dest)
        a1 = [r for r in historized if r["instance"] == "A" and r["id"] == 1]
        # Two rows: pending (closed at Day 3) and shipped (open).
        assert len(a1) == 2
        statuses = {r["status"] for r in a1}
        assert statuses == {"pending", "shipped"}
        # Exactly one open row.
        open_rows = [r for r in a1 if r["_dlt_valid_to"] is None]
        assert len(open_rows) == 1
        assert open_rows[0]["status"] == "shipped"

    def test_real_deletion_still_detected(self, duckdb_dest):
        """If an instance actually drops a key while continuing to deliver
        other rows, deletion must still fire (scoped to that instance)."""
        rows = list(ROWS) + [
            # Day 4 — A delivers id=2 but no longer id=1 (real deletion of id=1).
            ("A", 2, "shipped", "2026-01-04"),
        ]
        _seed(duckdb_dest, rows)
        _make_runner(duckdb_dest, merge_key=["instance"]).run()
        historized = _query_historized(duckdb_dest)
        # A=1 should now have a deletion marker.
        a1_deletions = [
            r
            for r in historized
            if r["instance"] == "A" and r["id"] == 1 and r["_dlt_is_deleted"]
        ]
        assert len(a1_deletions) == 1
        # Marker should sit at the Day-4 boundary (next A snapshot after A=1's last appearance on Day 3).
        from datetime import datetime

        assert a1_deletions[0]["_dlt_valid_from"] == datetime(2026, 1, 4, 0, 0)
