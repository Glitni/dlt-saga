"""Integration tests for historize ``strategy: scd1`` — latest version per key.

The source is an append-only event log: each row is one version of a key,
stamped with the time it landed (``ingested_at``, the snapshot column). A
re-delivered version (e.g. a corrected or anonymized replay) lands again with a
newer ``ingested_at`` and the same ``event_time``.
"""

import pytest

from dlt_saga.destinations.duckdb.config import DuckDBDestinationConfig
from dlt_saga.destinations.duckdb.destination import DuckDBDestination
from dlt_saga.historize.config import HistorizeConfig
from dlt_saga.historize.runner import HistorizeRunner

SCHEMA = "test_scd1"
SOURCE_TABLE = "events"
TARGET_TABLE = "events_historized"


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
        CREATE TABLE "{SCHEMA}"."{SOURCE_TABLE}" (
            id INTEGER,
            payload VARCHAR,
            event_time TIMESTAMP,
            ingested_at TIMESTAMP,
            landing_offset INTEGER
        )
        """,
        SCHEMA,
    )
    yield destination
    destination.close()


def _insert(dest, rows):
    """Insert ``(id, payload, event_time, ingested_at, landing_offset)`` rows."""
    values = ", ".join(
        f"({i}, {'NULL' if p is None else repr(p)}, TIMESTAMP '{et}', "
        f"TIMESTAMP '{ia}', {off})"
        for i, p, et, ia, off in rows
    )
    dest.execute_sql(f'INSERT INTO "{SCHEMA}"."{SOURCE_TABLE}" VALUES {values}', SCHEMA)


def _runner(dest, *, full_refresh=False, partial_refresh=False, **overrides):
    settings = {
        "strategy": "scd1",
        "primary_key": ["id"],
        "snapshot_column": "ingested_at",
        **overrides,
    }
    return HistorizeRunner(
        pipeline_name="test__events",
        historize_config=HistorizeConfig(**settings),
        destination=dest,
        database="test",
        schema=SCHEMA,
        source_table_name=SOURCE_TABLE,
        target_table_name=TARGET_TABLE,
        config_dict={"write_disposition": "historize", "primary_key": ["id"]},
        full_refresh=full_refresh,
        partial_refresh=partial_refresh,
    )


def _run(dest, **kwargs):
    stats = _runner(dest, **kwargs).run()
    assert stats["status"] == "completed", stats.get("error")
    return stats


def _target(dest):
    result = dest.connection.execute(
        f'SELECT * FROM "{SCHEMA}"."{TARGET_TABLE}" ORDER BY id'
    )
    cols = [c[0] for c in result.description]
    return [dict(zip(cols, r)) for r in result.fetchall()]


def _payloads(dest):
    return {row["id"]: row["payload"] for row in _target(dest)}


class TestScd1Shape:
    def test_one_row_per_key_without_scd2_columns(self, dest):
        _insert(
            dest,
            [
                (1, "v1", "2026-01-01", "2026-01-01 00:00", 1),
                (1, "v2", "2026-01-02", "2026-01-02 00:00", 2),
                (2, "only", "2026-01-01", "2026-01-01 00:00", 3),
            ],
        )
        stats = _run(dest, full_refresh=True)

        rows = _target(dest)
        assert [r["id"] for r in rows] == [1, 2]
        assert set(rows[0]) == {
            "id",
            "payload",
            "event_time",
            "landing_offset",
            "_dlt_valid_from",
        }
        assert _payloads(dest) == {1: "v2", 2: "only"}
        assert str(rows[0]["_dlt_valid_from"]) == "2026-01-02 00:00:00"
        assert stats["new_or_changed_rows"] == 2
        assert stats["deleted_rows"] == 0


class TestScd1Incremental:
    def test_newer_version_overwrites_and_new_key_inserts(self, dest):
        _insert(
            dest,
            [
                (1, "v1", "2026-01-01", "2026-01-01 00:00", 1),
                (2, "keep", "2026-01-01", "2026-01-01 00:00", 2),
            ],
        )
        _run(dest, full_refresh=True)

        _insert(
            dest,
            [
                (1, "anonymized", "2026-01-01", "2026-01-05 00:00", 3),
                (3, "new", "2026-01-05", "2026-01-05 00:00", 4),
            ],
        )
        stats = _run(dest)

        assert stats["mode"] == "incremental"
        assert stats["new_or_changed_rows"] == 2
        assert _payloads(dest) == {1: "anonymized", 2: "keep", 3: "new"}
        assert len(_target(dest)) == 3

    def test_rerun_with_nothing_new_changes_nothing(self, dest):
        _insert(dest, [(1, "v1", "2026-01-01", "2026-01-01 00:00", 1)])
        _run(dest, full_refresh=True)
        before = _target(dest)

        stats = _run(dest)

        assert stats["snapshots_processed"] == 0
        assert _target(dest) == before


class TestScd1OrderBy:
    def test_lower_ranked_version_loses_however_late_it_lands(self, dest):
        """A version ranked below the stored one doesn't overwrite it."""
        _insert(dest, [(1, "newest", "2026-01-05", "2026-01-05 00:00", 2)])
        _run(dest, full_refresh=True, order_by=["event_time"])

        # An older event lands after the newer one was already merged.
        _insert(dest, [(1, "stale", "2026-01-01", "2026-01-06 00:00", 1)])
        _run(dest, order_by=["event_time"])

        assert _payloads(dest) == {1: "newest"}

    def test_tiebreak_columns_decide_within_one_batch(self, dest):
        """Two versions with the same event_time: landing_offset decides."""
        _insert(
            dest,
            [
                (1, "original", "2026-01-01", "2026-01-02 00:00", 1),
                (1, "replayed", "2026-01-01", "2026-01-02 00:00", 7),
            ],
        )
        _run(dest, full_refresh=True, order_by=["event_time", "landing_offset"])

        assert _payloads(dest) == {1: "replayed"}

    def test_null_ranks_lowest(self, dest):
        _insert(
            dest,
            [
                (1, "has_offset", "2026-01-01", "2026-01-02 00:00", 1),
                (1, "no_offset", "2026-01-01", "2026-01-02 00:00", 1),
            ],
        )
        dest.execute_sql(
            f'UPDATE "{SCHEMA}"."{SOURCE_TABLE}" SET landing_offset = NULL '
            f"WHERE payload = 'no_offset'",
            SCHEMA,
        )
        _run(dest, full_refresh=True, order_by=["landing_offset"])

        assert _payloads(dest) == {1: "has_offset"}

    def test_order_by_column_not_carried_into_target_is_refused(self, dest):
        _insert(dest, [(1, "v1", "2026-01-01", "2026-01-01 00:00", 1)])
        stats = _runner(dest, full_refresh=True, order_by=["id"]).run()

        assert stats["status"] == "failed"
        assert "order_by" in stats["error"]

    def test_snapshot_column_matched_case_insensitively(self, dest):
        _insert(
            dest,
            [
                (1, "old", "2026-01-01", "2026-01-01 00:00", 1),
                (1, "new", "2026-01-01", "2026-01-02 00:00", 1),
            ],
        )
        _run(dest, full_refresh=True, order_by=["Ingested_At"])

        assert _payloads(dest) == {1: "new"}


def _valid_from(dest):
    return {row["id"]: str(row["_dlt_valid_from"]) for row in _target(dest)}


class TestScd1ChangeDetection:
    """valid_from marks where the current values took effect, not when last seen."""

    def test_full_reprocess_dates_the_current_values_from_their_first_delivery(
        self, dest
    ):
        _insert(
            dest,
            [
                (1, "a", "2026-01-01", "2026-01-01 00:00", 1),
                (1, "a", "2026-01-01", "2026-01-02 00:00", 1),
                (1, "b", "2026-01-01", "2026-01-03 00:00", 1),
                (1, "b", "2026-01-01", "2026-01-04 00:00", 1),
            ],
        )
        _run(dest, full_refresh=True)

        assert _payloads(dest) == {1: "b"}
        assert _valid_from(dest) == {1: "2026-01-03 00:00:00"}

    def test_values_that_change_back_start_a_new_stretch(self, dest):
        _insert(
            dest,
            [
                (1, "a", "2026-01-01", "2026-01-01 00:00", 1),
                (1, "b", "2026-01-01", "2026-01-02 00:00", 1),
                (1, "a", "2026-01-01", "2026-01-03 00:00", 1),
            ],
        )
        _run(dest, full_refresh=True)

        assert _valid_from(dest) == {1: "2026-01-03 00:00:00"}

    def test_unchanged_redelivery_writes_nothing(self, dest):
        _insert(dest, [(1, "a", "2026-01-01", "2026-01-01 00:00", 1)])
        _run(dest, full_refresh=True)

        _insert(dest, [(1, "a", "2026-01-01", "2026-01-02 00:00", 1)])
        stats = _run(dest)

        assert stats["new_or_changed_rows"] == 0
        assert _valid_from(dest) == {1: "2026-01-01 00:00:00"}

    def test_change_after_unchanged_redeliveries_is_dated_to_the_change(self, dest):
        _insert(dest, [(1, "a", "2026-01-01", "2026-01-01 00:00", 1)])
        _run(dest, full_refresh=True)

        _insert(
            dest,
            [
                (1, "a", "2026-01-01", "2026-01-02 00:00", 1),
                (1, "b", "2026-01-01", "2026-01-03 00:00", 1),
                (1, "b", "2026-01-01", "2026-01-04 00:00", 1),
            ],
        )
        stats = _run(dest)

        assert stats["new_or_changed_rows"] == 1
        assert _payloads(dest) == {1: "b"}
        assert _valid_from(dest) == {1: "2026-01-03 00:00:00"}


class TestScd1Refreshes:
    def test_partial_refresh_applies_a_corrected_source_row(self, dest):
        """Re-processing a corrected row with an unchanged rank takes effect."""
        _insert(dest, [(1, "wrong", "2026-01-01", "2026-01-01 00:00", 1)])
        _run(dest, full_refresh=True)

        dest.execute_sql(
            f'UPDATE "{SCHEMA}"."{SOURCE_TABLE}" SET payload = \'fixed\'',
            SCHEMA,
        )
        stats = _run(dest, partial_refresh=True)

        assert stats["mode"] == "partial_refresh"
        assert _payloads(dest) == {1: "fixed"}
        assert len(_target(dest)) == 1

    def test_full_refresh_drops_a_version_gone_from_the_source(self, dest):
        _insert(
            dest,
            [
                (1, "v1", "2026-01-01", "2026-01-01 00:00", 1),
                (1, "bad", "2026-01-02", "2026-01-02 00:00", 2),
            ],
        )
        _run(dest, full_refresh=True)
        dest.execute_sql(
            f'DELETE FROM "{SCHEMA}"."{SOURCE_TABLE}" WHERE payload = \'bad\'',
            SCHEMA,
        )

        _run(dest, full_refresh=True)

        assert _payloads(dest) == {1: "v1"}


class TestScd1LateArrivals:
    def test_late_version_is_merged_by_rank_without_rewind(self, dest):
        """A late snapshot below the watermark is re-merged; rank still decides."""
        common = {
            "snapshot_column": "event_time",
            "arrival_column": "ingested_at",
            "detect_late_arrivals": True,
        }
        _insert(
            dest,
            [
                (1, "current", "2026-01-05", "2000-01-01 00:00", 1),
                (2, "current", "2026-01-05", "2000-01-01 00:00", 2),
            ],
        )
        _run(dest, full_refresh=True, **common)

        # Both land after the first run, below the watermark (2026-01-05).
        _insert(
            dest,
            [
                (1, "stale", "2026-01-01", "2099-01-01 00:00", 3),
                (3, "late_new_key", "2026-01-02", "2099-01-01 00:00", 4),
            ],
        )
        _run(dest, **common)

        assert _payloads(dest) == {1: "current", 2: "current", 3: "late_new_key"}


class TestStrategySwitch:
    def test_switching_strategy_requires_full_refresh(self, dest):
        _insert(dest, [(1, "v1", "2026-01-01", "2026-01-01 00:00", 1)])
        _run(dest, full_refresh=True)

        runner = HistorizeRunner(
            pipeline_name="test__events",
            historize_config=HistorizeConfig(
                primary_key=["id"], snapshot_column="ingested_at"
            ),
            destination=dest,
            database="test",
            schema=SCHEMA,
            source_table_name=SOURCE_TABLE,
            target_table_name=TARGET_TABLE,
            config_dict={"write_disposition": "historize", "primary_key": ["id"]},
        )
        stats = runner.run()

        assert stats["status"] == "failed"
        assert "strategy: scd1 → scd2" in stats["error"]
        assert "--full-refresh" in stats["error"]
