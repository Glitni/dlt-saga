"""Integration tests for ``historize.exclude_columns`` on DuckDB.

An excluded column never reaches the historized table and never opens a
version. Excluding one after the table exists is forward-only: the column
stays with its history, new versions carry NULL, nothing is versioned by the
exclusion itself, and the run is not refused. Un-excluding is.
"""

import logging

import pytest

from dlt_saga.destinations.duckdb.config import DuckDBDestinationConfig
from dlt_saga.destinations.duckdb.destination import DuckDBDestination
from dlt_saga.historize.config import HistorizeConfig
from dlt_saga.historize.runner import HistorizeRunner

SCHEMA = "test_exclude"
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
            name VARCHAR,
            payload VARCHAR,
            snapshot_at TIMESTAMP
        )
        """,
        SCHEMA,
    )
    yield destination
    destination.close()


def _literal(value):
    if value is None:
        return "NULL"
    if isinstance(value, str):
        return repr(value)
    return str(value)


def _insert(dest, snapshot, rows):
    """``rows``: ``(id, name, payload)`` tuples for one snapshot."""
    values = ", ".join(
        f"({', '.join(_literal(v) for v in row)}, TIMESTAMP '{snapshot}')"
        for row in rows
    )
    dest.execute_sql(f"INSERT INTO {SOURCE} VALUES {values}", SCHEMA)


def _runner(dest, strategy="scd2", **kwargs):
    overrides = {
        k: kwargs.pop(k)
        for k in list(kwargs)
        if k not in ("full_refresh", "partial_refresh", "historize_from")
    }
    settings = {
        "strategy": strategy,
        "primary_key": ["id"],
        "snapshot_column": "snapshot_at",
        **overrides,
    }
    if strategy == "scd1":
        settings.setdefault("track_deletions", False)
    return HistorizeRunner(
        pipeline_name="test__events",
        historize_config=HistorizeConfig(**settings),
        destination=dest,
        database="test",
        schema=SCHEMA,
        source_table_name=SOURCE_TABLE,
        target_table_name=TARGET_TABLE,
        config_dict={"write_disposition": "historize", "primary_key": ["id"]},
        **kwargs,
    )


def _run(dest, strategy="scd2", **kwargs):
    stats = _runner(dest, strategy, **kwargs).run()
    assert stats["status"] == "completed", stats.get("error")
    return stats


def _target(dest):
    result = dest.connection.execute(
        f'SELECT * FROM "{SCHEMA}"."{TARGET_TABLE}" ORDER BY id, _dlt_valid_from'
    )
    cols = [c[0] for c in result.description]
    return [dict(zip(cols, r)) for r in result.fetchall()]


def _columns(dest):
    return {name for name, _ in dest.list_table_columns(SCHEMA, TARGET_TABLE)}


def _versions(dest, key):
    return [r for r in _target(dest) if r["id"] == key]


@pytest.mark.parametrize("strategy", ["scd2", "scd1"])
class TestExcludedFromTheStart:
    def test_column_is_absent_and_opens_no_versions(self, dest, strategy):
        _insert(dest, S1, [(1, "a", "p1")])
        _run(dest, strategy, full_refresh=True, exclude_columns=["payload"])
        assert "payload" not in _columns(dest)
        assert "name" in _columns(dest)

        # Only the excluded column changes: nothing to version.
        _insert(dest, S2, [(1, "a", "p2")])
        _run(dest, strategy, exclude_columns=["payload"])
        one = _versions(dest, 1)
        assert len(one) == 1 and str(one[0]["_dlt_valid_from"]) == S1

        # A tracked column changes: versioned as usual.
        _insert(dest, S3, [(1, "b", "p2")])
        _run(dest, strategy, exclude_columns=["payload"])
        one = _versions(dest, 1)
        if strategy == "scd2":
            assert [r["name"] for r in one] == ["a", "b"]
        else:
            assert len(one) == 1 and one[0]["name"] == "b"
            assert str(one[0]["_dlt_valid_from"]) == S3


class TestExcludedAfterTheFact:
    def test_forward_only_kept_with_history_null_in_new_versions(self, dest, caplog):
        _insert(dest, S1, [(1, "a", "p1"), (2, "b", "p1")])
        _run(dest, full_refresh=True)
        _insert(dest, S2, [(1, "a", "p2"), (2, "b", "p2")])
        _run(dest)
        assert len(_target(dest)) == 4

        # Excluding the column is not a config change the run refuses, and
        # the exclusion itself versions nothing — even though the payload
        # column differs between the stored open rows and the batch.
        _insert(dest, S3, [(1, "a", "p3"), (2, "c", "p3")])
        with caplog.at_level(logging.INFO, logger="dlt_saga.historize.runner"):
            stats = _run(dest, exclude_columns=["payload"])
        assert stats["mode"] == "incremental"
        assert any(
            "narrowed" in r.getMessage() and "exclude_columns" in r.getMessage()
            for r in caplog.records
        )
        assert not [r for r in caplog.records if r.levelno == logging.WARNING]

        assert "payload" in _columns(dest)
        # Key 1: nothing tracked changed — its versions are untouched.
        one = _versions(dest, 1)
        assert [(r["payload"], r["name"]) for r in one] == [("p1", "a"), ("p2", "a")]
        # Key 2: name changed — the new version carries NULL in the excluded column.
        two = _versions(dest, 2)
        assert [(r["payload"], r["name"]) for r in two] == [
            ("p1", "b"),
            ("p2", "b"),
            (None, "c"),
        ]

    def test_full_refresh_rebuilds_without_the_column(self, dest):
        _insert(dest, S1, [(1, "a", "p1")])
        _run(dest, full_refresh=True)
        _run(dest, full_refresh=True, exclude_columns=["payload"])
        assert "payload" not in _columns(dest)

    def test_hand_dropped_column_is_left_alone(self, dest):
        _insert(dest, S1, [(1, "a", "p1")])
        _run(dest, full_refresh=True)
        dest.execute_sql(
            f'ALTER TABLE "{SCHEMA}"."{TARGET_TABLE}" DROP COLUMN payload', SCHEMA
        )
        _insert(dest, S2, [(1, "b", "p2")])
        _run(dest, exclude_columns=["payload"])
        assert "payload" not in _columns(dest)
        assert [r["name"] for r in _versions(dest, 1)] == ["a", "b"]

    def test_un_excluding_is_refused(self, dest):
        _insert(dest, S1, [(1, "a", "p1")])
        _run(dest, full_refresh=True, exclude_columns=["payload"])
        _insert(dest, S2, [(1, "a", "p2")])
        stats = _runner(dest).run()
        assert stats["status"] == "failed"
        assert "exclude_columns: ['payload'] → []" in stats["error"]
        assert "--full-refresh" in stats["error"]


class TestWithJsonColumns:
    def test_json_source_column_excluded_derived_columns_kept(self, dest):
        _insert(dest, S1, [(1, "a", '{"amount": 5}')])
        kwargs = dict(
            json_columns={
                "amount": {"column": "payload", "path": "amount", "data_type": "bigint"}
            },
            exclude_columns=["payload"],
        )
        _run(dest, full_refresh=True, **kwargs)
        assert "payload" not in _columns(dest)
        assert _versions(dest, 1)[0]["amount"] == 5

        _insert(dest, S2, [(1, "a", '{"amount": 7}')])
        _run(dest, **kwargs)
        assert [r["amount"] for r in _versions(dest, 1)] == [5, 7]
