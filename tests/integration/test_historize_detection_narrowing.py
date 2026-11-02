"""Integration tests: narrowing change detection needs no full refresh.

A noisy column (one that changes on every delivery) can be silenced by adding
it to ``ignore_columns`` after the historized table exists. The edit applies
from the next run on; versions already written stay. Widening detection again
still demands ``--full-refresh``.
"""

import pytest

from dlt_saga.destinations.duckdb.config import DuckDBDestinationConfig
from dlt_saga.destinations.duckdb.destination import DuckDBDestination
from dlt_saga.historize.config import HistorizeConfig
from dlt_saga.historize.runner import HistorizeRunner

SCHEMA = "test_narrowing"
SOURCE_TABLE = "events"
TARGET_TABLE = "events_historized"
SOURCE = f'"{SCHEMA}"."{SOURCE_TABLE}"'

S1 = "2026-01-01 00:00:00"
S2 = "2026-01-02 00:00:00"
S3 = "2026-01-03 00:00:00"


@pytest.fixture
def dest():
    cfg = DuckDBDestinationConfig(
        project_id="test",
        location="local",
        schema_name=SCHEMA,
        database_path=":memory:",
    )
    destination = DuckDBDestination(cfg)
    destination.execute_sql(
        f"""
        CREATE TABLE {SOURCE} (
            id INTEGER,
            payload VARCHAR,
            noise VARCHAR,
            snapshot_at TIMESTAMP
        )
        """,
        SCHEMA,
    )
    yield destination
    destination.close()


def _insert(dest, snapshot, rows):
    values = ", ".join(
        f"({i}, {payload!r}, {noise!r}, TIMESTAMP '{snapshot}')"
        for i, payload, noise in rows
    )
    dest.execute_sql(f"INSERT INTO {SOURCE} VALUES {values}", SCHEMA)


def _runner(dest, full_refresh=False, **settings):
    config = HistorizeConfig(
        primary_key=["id"], snapshot_column="snapshot_at", **settings
    )
    return HistorizeRunner(
        pipeline_name="test__events",
        historize_config=config,
        destination=dest,
        database="test",
        schema=SCHEMA,
        source_table_name=SOURCE_TABLE,
        target_table_name=TARGET_TABLE,
        config_dict={"write_disposition": "historize", "primary_key": ["id"]},
        full_refresh=full_refresh,
    )


def _run(dest, **kwargs):
    stats = _runner(dest, **kwargs).run()
    assert stats["status"] == "completed", stats.get("error")
    return stats


def _versions(dest, key):
    result = dest.connection.execute(
        f'SELECT noise, _dlt_valid_from FROM "{SCHEMA}"."{TARGET_TABLE}" '
        f"WHERE id = {key} ORDER BY _dlt_valid_from"
    )
    return result.fetchall()


class TestNarrowing:
    def test_ignoring_a_noisy_column_applies_from_the_next_run(self, dest):
        _insert(dest, S1, [(1, "a", "n1")])
        _run(dest, full_refresh=True)
        _insert(dest, S2, [(1, "a", "n2")])
        _run(dest)
        assert len(_versions(dest, 1)) == 2

        _insert(dest, S3, [(1, "a", "n3")])
        stats = _run(dest, ignore_columns=["noise"])

        assert stats["mode"] == "incremental"
        assert stats["new_or_changed_rows"] == 0
        # The two versions already written stay as they were.
        assert [row[0] for row in _versions(dest, 1)] == ["n1", "n2"]

    def test_tracking_a_subset_applies_from_the_next_run(self, dest):
        _insert(dest, S1, [(1, "a", "n1")])
        _run(dest, full_refresh=True)
        _insert(dest, S2, [(1, "a", "n2")])
        stats = _run(dest, track_columns=["payload"])

        assert stats["new_or_changed_rows"] == 0

    def test_widening_again_demands_a_full_refresh(self, dest):
        _insert(dest, S1, [(1, "a", "n1")])
        _run(dest, full_refresh=True, ignore_columns=["noise"])
        _insert(dest, S2, [(1, "a", "n2")])

        stats = _runner(dest).run()

        assert stats["status"] == "failed"
        assert "Historization config changed" in stats["error"]
        assert "ignore_columns" in stats["error"]
        assert "--full-refresh" in stats["error"]
        assert len(_versions(dest, 1)) == 1

    def test_narrowing_does_not_mask_a_blocking_change(self, dest):
        _insert(dest, S1, [(1, "a", "n1")])
        _run(dest, full_refresh=True)
        _insert(dest, S2, [(1, "a", "n2")])

        stats = _runner(dest, ignore_columns=["noise"], track_deletions=False).run()

        assert stats["status"] == "failed"
        assert "track_deletions" in stats["error"]
