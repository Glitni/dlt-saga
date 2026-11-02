"""Integration tests for ``historize.json_columns`` on DuckDB.

The source is a streaming landing table: envelope columns plus a ``payload``
string holding the event as JSON. Derived columns are projected out of it and
used like any source column.
"""

import json
import logging
from datetime import datetime, timezone

import pytest

from dlt_saga.destinations.duckdb.config import DuckDBDestinationConfig
from dlt_saga.destinations.duckdb.destination import DuckDBDestination
from dlt_saga.historize.config import HistorizeConfig
from dlt_saga.historize.runner import HistorizeRunner

SCHEMA = "test_json_columns"
SOURCE_TABLE = "events"
TARGET_TABLE = "events_historized"

JSON_COLUMNS = {
    "event_time": {"column": "payload", "path": "eventTime", "data_type": "timestamp"},
    "user_id": {"column": "payload", "path": "user_id"},
    "tenant": {"column": "payload", "path": "meta.tenant"},
    "amount": {"column": "payload", "path": "amount", "data_type": "bigint"},
}


@pytest.fixture
def dest():
    destination = DuckDBDestination(
        DuckDBDestinationConfig(
            project_id="test",
            location="local",
            schema_name=SCHEMA,
            database_path=":memory:",
        )
    )
    # Pin the session zone: valid_from is a naive TIMESTAMP, and DuckDB renders
    # a zone-aware snapshot value into it in the session's zone.
    destination.connection.execute("SET TimeZone = 'UTC'")
    destination.execute_sql(
        f"""
        CREATE TABLE "{SCHEMA}"."{SOURCE_TABLE}" (
            landing_key INTEGER,
            payload VARCHAR,
            ingested_at TIMESTAMP,
            landing_offset INTEGER
        )
        """,
        SCHEMA,
    )
    yield destination
    destination.close()


def _insert(dest, rows):
    """Insert ``(landing_key, payload, ingested_at, landing_offset)`` rows.

    ``payload`` is a dict (serialized to JSON), a raw string, or None.
    """
    for key, payload, ingested_at, offset in rows:
        if isinstance(payload, dict):
            payload = json.dumps(payload)
        dest.connection.execute(
            f'INSERT INTO "{SCHEMA}"."{SOURCE_TABLE}" VALUES (?, ?, ?, ?)',
            [key, payload, ingested_at, offset],
        )


def _runner(
    dest, *, full_refresh=False, partial_refresh=False, primary_key=None, **overrides
):
    primary_key = primary_key or ["landing_key"]
    settings = {
        "primary_key": primary_key,
        "snapshot_column": "ingested_at",
        "json_columns": JSON_COLUMNS,
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
        config_dict={"write_disposition": "historize", "primary_key": primary_key},
        full_refresh=full_refresh,
        partial_refresh=partial_refresh,
    )


def _run(dest, **kwargs):
    stats = _runner(dest, **kwargs).run()
    assert stats["status"] == "completed", stats.get("error")
    return stats


def _rows(dest, order_by="landing_key, _dlt_valid_from"):
    result = dest.connection.execute(
        f'SELECT * FROM "{SCHEMA}"."{TARGET_TABLE}" ORDER BY {order_by}'
    )
    columns = [d[0] for d in result.description]
    return [dict(zip(columns, row)) for row in result.fetchall()]


def _event(user, tenant="t1", amount=1, event_time="2026-01-01T10:00:00Z"):
    return {
        "eventTime": event_time,
        "user_id": user,
        "meta": {"tenant": tenant},
        "amount": amount,
    }


@pytest.mark.integration
class TestExtraction:
    def test_values_extracted_and_typed(self, dest):
        _insert(dest, [(1, _event("u1", amount=42), "2026-01-01 12:00:00", 1)])
        _run(dest)

        (row,) = _rows(dest)
        assert row["user_id"] == "u1"
        assert row["tenant"] == "t1"
        assert row["amount"] == 42
        assert row["event_time"] == datetime(2026, 1, 1, 10, tzinfo=timezone.utc)
        # The JSON column itself is still carried.
        assert json.loads(row["payload"])["user_id"] == "u1"

        types = dict(
            dest.connection.execute(
                "SELECT column_name, data_type FROM information_schema.columns "
                f"WHERE table_schema = '{SCHEMA}' AND table_name = '{TARGET_TABLE}'"
            ).fetchall()
        )
        assert types["user_id"] == "VARCHAR"
        assert types["amount"] == "BIGINT"
        assert types["event_time"] == "TIMESTAMP WITH TIME ZONE"

    def test_missing_null_malformed_and_unconvertible_become_null(self, dest, caplog):
        _insert(
            dest,
            [
                (1, {"user_id": None, "meta": {}}, "2026-01-01 12:00:00", 1),
                (2, "not json", "2026-01-01 12:00:00", 2),
                (3, None, "2026-01-01 12:00:00", 3),
                (
                    4,
                    _event({"nested": "object"}, amount="many"),
                    "2026-01-01 12:00:00",
                    4,
                ),
            ],
        )
        with caplog.at_level(logging.WARNING):
            _run(dest)

        rows = _rows(dest)
        for row in rows:
            assert row["user_id"] is None
            assert row["amount"] is None
        assert [r["tenant"] for r in rows] == [None, None, None, "t1"]

        warnings = [
            r.getMessage() for r in caplog.records if r.levelno == logging.WARNING
        ]
        assert any("'amount'" in w and "don't convert to bigint" in w for w in warnings)
        # A missing or null value isn't a failed conversion.
        assert not any("'event_time'" in w for w in warnings)


@pytest.mark.integration
class TestScd2:
    def test_incremental_versions_on_derived_change(self, dest, caplog):
        _insert(dest, [(1, _event("u1", tenant="t1"), "2026-01-01 00:00:00", 1)])
        _run(dest)
        _insert(dest, [(1, _event("u1", tenant="t2"), "2026-01-02 00:00:00", 2)])
        with caplog.at_level(logging.WARNING):
            stats = _run(dest)

        assert stats["mode"] == "incremental"
        rows = _rows(dest)
        assert [r["tenant"] for r in rows] == ["t1", "t2"]
        assert rows[0]["_dlt_valid_to"] is not None
        assert rows[1]["_dlt_valid_to"] is None

    def test_incremental_run_warns_about_failed_conversions(self, dest, caplog):
        _insert(dest, [(1, _event("u1"), "2026-01-01 00:00:00", 1)])
        _run(dest)
        _insert(dest, [(1, _event("u1", amount="x"), "2026-01-02 00:00:00", 2)])
        with caplog.at_level(logging.WARNING):
            _run(dest)
        warnings = [
            r.getMessage() for r in caplog.records if r.levelno == logging.WARNING
        ]
        assert any("'amount'" in w for w in warnings)

    def test_partial_refresh(self, dest, caplog):
        _insert(
            dest,
            [
                (1, _event("u1", tenant="t1"), "2026-01-01 00:00:00", 1),
                (1, _event("u1", tenant="t2", amount="x"), "2026-01-02 00:00:00", 2),
            ],
        )
        _run(dest)
        with caplog.at_level(logging.WARNING):
            stats = _run(dest, partial_refresh=True)
        assert stats["mode"] == "partial_refresh"
        assert [r["tenant"] for r in _rows(dest)] == ["t1", "t2"]
        warnings = [
            r.getMessage() for r in caplog.records if r.levelno == logging.WARNING
        ]
        assert any("'amount'" in w for w in warnings)

    def test_derived_primary_key(self, dest):
        """Two landing keys carrying the same user are one entity."""
        _insert(
            dest,
            [
                (1, _event("u1", tenant="t1"), "2026-01-01 00:00:00", 1),
                (2, _event("u2", tenant="t1"), "2026-01-01 00:00:00", 2),
                (3, _event("u1", tenant="t2"), "2026-01-02 00:00:00", 3),
                (4, _event("u2", tenant="t1"), "2026-01-02 00:00:00", 4),
            ],
        )
        _run(
            dest,
            primary_key=["user_id"],
            ignore_columns=["landing_key", "landing_offset", "payload"],
        )
        rows = _rows(dest, order_by="user_id, _dlt_valid_from")
        assert [(r["user_id"], r["tenant"]) for r in rows] == [
            ("u1", "t1"),
            ("u1", "t2"),
            ("u2", "t1"),
        ]

    def test_filter_on_derived_column_compares_typed(self, dest):
        _insert(
            dest,
            [
                (1, _event("u1", amount=5), "2026-01-01 00:00:00", 1),
                (2, _event("u2", amount=50), "2026-01-01 00:00:00", 2),
            ],
        )
        _run(dest, filters=[{"column": "amount", "op": "in", "value": [50]}])
        assert [r["user_id"] for r in _rows(dest)] == ["u2"]

    def test_derived_snapshot_column(self, dest):
        _insert(
            dest,
            [
                (1, _event("u1", event_time="2026-01-05T00:00:00Z"), "2026-01-10", 1),
                (
                    1,
                    _event("u1", tenant="t2", event_time="2026-01-06T00:00:00Z"),
                    "2026-01-10",
                    2,
                ),
            ],
        )
        _run(dest, snapshot_column="event_time", track_deletions=False)
        rows = _rows(dest)
        assert [r["tenant"] for r in rows] == ["t1", "t2"]
        assert rows[1]["_dlt_valid_from"] == datetime(2026, 1, 6)


@pytest.mark.integration
class TestScd1:
    def test_order_by_derived_column(self, dest):
        """A re-delivered event with an older event_time doesn't win."""
        _insert(
            dest,
            [
                (
                    1,
                    _event("u1", event_time="2026-01-02T00:00:00Z"),
                    "2026-01-01 00:00:00",
                    1,
                ),
                (
                    1,
                    _event("u1-old", event_time="2026-01-01T00:00:00Z"),
                    "2026-01-02 00:00:00",
                    2,
                ),
            ],
        )
        _run(
            dest,
            strategy="scd1",
            order_by=["event_time", "ingested_at"],
            full_refresh=True,
        )
        (row,) = _rows(dest, order_by="landing_key")
        assert row["user_id"] == "u1"

    def test_incremental(self, dest):
        _insert(dest, [(1, _event("u1", amount=1), "2026-01-01 00:00:00", 1)])
        _run(dest, strategy="scd1")
        _insert(dest, [(1, _event("u1", amount=2), "2026-01-02 00:00:00", 2)])
        stats = _run(dest, strategy="scd1")
        assert stats["mode"] == "incremental"
        (row,) = _rows(dest, order_by="landing_key")
        assert row["amount"] == 2


@pytest.mark.integration
class TestGuards:
    def test_output_name_colliding_with_source_column(self, dest):
        _insert(dest, [(1, _event("u1"), "2026-01-01 00:00:00", 1)])
        stats = _runner(
            dest,
            json_columns={
                "landing_offset": {"column": "payload", "path": "landing_offset"}
            },
        ).run()
        assert stats["status"] == "failed"
        assert "'landing_offset' is already a column of the source" in stats["error"]

    def test_column_not_in_source(self, dest):
        _insert(dest, [(1, _event("u1"), "2026-01-01 00:00:00", 1)])
        stats = _runner(
            dest, json_columns={"user_id": {"column": "body", "path": "user_id"}}
        ).run()
        assert stats["status"] == "failed"
        assert "reads column 'body'" in stats["error"]

    def test_column_may_not_be_another_derived_column(self, dest):
        _insert(dest, [(1, _event("u1"), "2026-01-01 00:00:00", 1)])
        stats = _runner(
            dest,
            json_columns={
                "meta": {"column": "payload", "path": "meta"},
                "tenant": {"column": "meta", "path": "tenant"},
            },
        ).run()
        assert stats["status"] == "failed"
        assert "reads column 'meta'" in stats["error"]


@pytest.mark.integration
class TestConfigChange:
    def test_adding_an_entry_adds_the_column(self, dest):
        without_amount = {k: v for k, v in JSON_COLUMNS.items() if k != "amount"}
        _insert(dest, [(1, _event("u1", amount=5), "2026-01-01 00:00:00", 1)])
        _run(dest, json_columns=without_amount)
        _insert(dest, [(1, _event("u1", amount=5), "2026-01-02 00:00:00", 2)])

        _run(dest)

        rows = _rows(dest)
        assert [r["amount"] for r in rows] == [None, 5]
        assert rows[0]["_dlt_valid_to"] == rows[1]["_dlt_valid_from"]

    def test_removing_an_entry_keeps_its_history(self, dest):
        without_amount = {k: v for k, v in JSON_COLUMNS.items() if k != "amount"}
        _insert(dest, [(1, _event("u1", amount=5), "2026-01-01 00:00:00", 1)])
        _run(dest)
        _insert(dest, [(1, _event("u2", amount=5), "2026-01-02 00:00:00", 2)])

        _run(dest, json_columns=without_amount)

        rows = _rows(dest)
        assert [(r["user_id"], r["amount"]) for r in rows] == [("u1", 5), ("u2", None)]

    def test_retyping_an_entry_needs_full_refresh(self, dest):
        _insert(dest, [(1, _event("u1"), "2026-01-01 00:00:00", 1)])
        _run(dest)
        _insert(dest, [(1, _event("u1"), "2026-01-02 00:00:00", 2)])
        retyped = {
            **JSON_COLUMNS,
            "amount": {"column": "payload", "path": "amount", "data_type": "double"},
        }
        stats = _runner(dest, json_columns=retyped).run()
        assert stats["status"] == "failed"
        assert "json_column_types" in stats["error"]
        assert "--full-refresh" in stats["error"]

        _run(dest, json_columns=retyped, full_refresh=True)

    def test_redefining_a_keyed_entry_needs_full_refresh(self, dest):
        primary_key = ["landing_key", "user_id"]
        _insert(dest, [(1, _event("u1"), "2026-01-01 00:00:00", 1)])
        _run(dest, primary_key=primary_key)
        _insert(dest, [(1, _event("u1"), "2026-01-02 00:00:00", 2)])
        redefined = {
            **JSON_COLUMNS,
            "user_id": {"column": "payload", "path": "meta.tenant"},
        }
        stats = _runner(dest, primary_key=primary_key, json_columns=redefined).run()
        assert stats["status"] == "failed"
        assert "json_columns" in stats["error"]
        assert "--full-refresh" in stats["error"]
