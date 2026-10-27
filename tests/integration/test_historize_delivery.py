"""Integration tests for historize ``delivery: full | incremental`` (scd2).

Under ``full`` each snapshot holds every record, so a record missing from a
snapshot was deleted or absent. Under ``incremental`` each snapshot holds only
the records sent in it, so a missing record means nothing: no deletion markers,
and a re-sent unchanged record doesn't open a new version.
"""

import pytest

from dlt_saga.destinations.duckdb.config import DuckDBDestinationConfig
from dlt_saga.destinations.duckdb.destination import DuckDBDestination
from dlt_saga.historize.config import HistorizeConfig
from dlt_saga.historize.runner import HistorizeRunner

SCHEMA = "test_delivery"
SOURCE_TABLE = "deliveries"
TARGET_TABLE = "deliveries_historized"


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
            status VARCHAR,
            delivered_at TIMESTAMP
        )
        """,
        SCHEMA,
    )
    yield destination
    destination.close()


def _insert(dest, rows):
    """Insert ``(id, status, delivered_at)`` rows."""
    values = ", ".join(f"({i}, '{st}', TIMESTAMP '{at}')" for i, st, at in rows)
    dest.execute_sql(f'INSERT INTO "{SCHEMA}"."{SOURCE_TABLE}" VALUES {values}', SCHEMA)


def _run(dest, *, full_refresh=False, **historize):
    settings = {"primary_key": ["id"], "snapshot_column": "delivered_at", **historize}
    stats = HistorizeRunner(
        pipeline_name="test__deliveries",
        historize_config=HistorizeConfig(**settings),
        destination=dest,
        database="test",
        schema=SCHEMA,
        source_table_name=SOURCE_TABLE,
        target_table_name=TARGET_TABLE,
        config_dict={"write_disposition": "historize", "primary_key": ["id"]},
        full_refresh=full_refresh,
    ).run()
    return stats


def _history(dest, key):
    result = dest.connection.execute(
        f"""
        SELECT status, CAST(_dlt_valid_from AS VARCHAR), CAST(_dlt_valid_to AS VARCHAR),
               _dlt_is_deleted
        FROM "{SCHEMA}"."{TARGET_TABLE}"
        WHERE id = {key}
        ORDER BY _dlt_valid_from, _dlt_is_deleted
        """
    )
    return result.fetchall()


# Record 1 is sent on day 1 and re-sent unchanged on day 3; only record 2 is
# sent on day 2.
RESENT_UNCHANGED = [
    (1, "open", "2026-01-01"),
    (2, "open", "2026-01-02"),
    (1, "open", "2026-01-03"),
]


class TestIncrementalDelivery:
    def test_missing_record_is_neither_deleted_nor_reopened(self, dest):
        _insert(dest, RESENT_UNCHANGED)
        stats = _run(dest, full_refresh=True, delivery="incremental")

        assert stats["status"] == "completed", stats.get("error")
        assert stats["deleted_rows"] == 0
        assert _history(dest, 1) == [("open", "2026-01-01 00:00:00", None, False)]

    def test_value_change_opens_a_version(self, dest):
        _insert(dest, [(1, "open", "2026-01-01"), (1, "closed", "2026-01-03")])
        _run(dest, full_refresh=True, delivery="incremental")

        assert _history(dest, 1) == [
            ("open", "2026-01-01 00:00:00", "2026-01-03 00:00:00", False),
            ("closed", "2026-01-03 00:00:00", None, False),
        ]

    def test_incremental_runs_leave_unsent_records_open(self, dest):
        _insert(dest, [(1, "open", "2026-01-01"), (2, "open", "2026-01-01")])
        _run(dest, full_refresh=True, delivery="incremental")

        _insert(dest, [(2, "closed", "2026-01-02")])
        stats = _run(dest, delivery="incremental")

        assert stats["mode"] == "incremental"
        assert stats["deleted_rows"] == 0
        assert _history(dest, 1) == [("open", "2026-01-01 00:00:00", None, False)]
        assert _history(dest, 2)[-1] == ("closed", "2026-01-02 00:00:00", None, False)

        _insert(dest, [(1, "open", "2026-01-03")])
        _run(dest, delivery="incremental")

        assert _history(dest, 1) == [("open", "2026-01-01 00:00:00", None, False)]


class TestFullDeliveryUnchanged:
    """The same source read as full snapshots: absence is meaningful."""

    def test_absence_marks_a_deletion_and_return_reopens(self, dest):
        _insert(dest, RESENT_UNCHANGED)
        _run(dest, full_refresh=True)

        assert _history(dest, 1) == [
            ("open", "2026-01-01 00:00:00", "2026-01-02 00:00:00", False),
            ("open", "2026-01-02 00:00:00", "2026-01-03 00:00:00", True),
            ("open", "2026-01-03 00:00:00", None, False),
        ]

    def test_return_after_gap_reopens_even_without_deletion_tracking(self, dest):
        _insert(dest, RESENT_UNCHANGED)
        _run(dest, full_refresh=True, track_deletions=False)

        assert [row[1] for row in _history(dest, 1)] == [
            "2026-01-01 00:00:00",
            "2026-01-03 00:00:00",
        ]


class TestDeliverySwitch:
    def test_switching_delivery_requires_full_refresh(self, dest):
        _insert(dest, [(1, "open", "2026-01-01")])
        _run(dest, full_refresh=True)

        stats = _run(dest, delivery="incremental")

        assert stats["status"] == "failed"
        assert "delivery: full → incremental" in stats["error"]
        assert "--full-refresh" in stats["error"]

    def test_switch_error_explains_the_track_deletions_change(self, dest):
        _insert(dest, [(1, "open", "2026-01-01")])
        _run(dest, full_refresh=True)

        stats = _run(dest, delivery="incremental")

        assert "track_deletions follows delivery" in stats["error"]
