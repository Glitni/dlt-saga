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
# The same landing table with a JSON-typed payload column.
JSON_SOURCE_TABLE = "events_json"
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
    for table, payload_type in ((SOURCE_TABLE, "VARCHAR"), (JSON_SOURCE_TABLE, "JSON")):
        destination.execute_sql(
            f"""
            CREATE TABLE "{SCHEMA}"."{table}" (
                landing_key INTEGER,
                payload {payload_type},
                ingested_at TIMESTAMP,
                landing_offset INTEGER
            )
            """,
            SCHEMA,
        )
    yield destination
    destination.close()


def _insert(dest, rows, table=SOURCE_TABLE):
    """Insert ``(landing_key, payload, ingested_at, landing_offset)`` rows.

    ``payload`` is a dict (serialized to JSON), a raw string, or None.
    """
    for key, payload, ingested_at, offset in rows:
        if isinstance(payload, dict):
            payload = json.dumps(payload)
        dest.connection.execute(
            f'INSERT INTO "{SCHEMA}"."{table}" VALUES (?, TRY_CAST(? AS {_payload_type(table)}), ?, ?)',
            [key, payload, ingested_at, offset],
        )


def _payload_type(table):
    return "JSON" if table == JSON_SOURCE_TABLE else "VARCHAR"


def _runner(
    dest,
    *,
    full_refresh=False,
    partial_refresh=False,
    historize_from=None,
    primary_key=None,
    source_table=SOURCE_TABLE,
    **overrides,
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
        source_table_name=source_table,
        target_table_name=TARGET_TABLE,
        config_dict={"write_disposition": "historize", "primary_key": primary_key},
        full_refresh=full_refresh,
        partial_refresh=partial_refresh,
        historize_from=historize_from,
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


def _target_types(dest):
    return dict(
        dest.connection.execute(
            "SELECT column_name, data_type FROM information_schema.columns "
            f"WHERE table_schema = '{SCHEMA}' AND table_name = '{TARGET_TABLE}'"
        ).fetchall()
    )


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

        types = _target_types(dest)
        assert types["user_id"] == "VARCHAR"
        assert types["amount"] == "BIGINT"
        assert types["event_time"] == "TIMESTAMP WITH TIME ZONE"

    def test_values_extracted_from_a_json_typed_column(self, dest):
        _insert(
            dest,
            [(1, _event("u1", amount=42), "2026-01-01 12:00:00", 1)],
            JSON_SOURCE_TABLE,
        )
        _run(dest, source_table=JSON_SOURCE_TABLE)

        (row,) = _rows(dest)
        assert (row["user_id"], row["tenant"], row["amount"]) == ("u1", "t1", 42)
        assert row["event_time"] == datetime(2026, 1, 1, 10, tzinfo=timezone.utc)

    def test_missing_null_malformed_and_unconvertible_become_null(self, dest, caplog):
        _insert(
            dest,
            [
                (1, {"user_id": None, "meta": {}}, "2026-01-01 12:00:00", 1),
                (2, "not json", "2026-01-01 12:00:00", 2),
                (3, None, "2026-01-01 12:00:00", 3),
                (4, _event("u4", amount="many"), "2026-01-01 12:00:00", 4),
            ],
        )
        with caplog.at_level(logging.WARNING):
            _run(dest)

        rows = _rows(dest)
        assert [r["user_id"] for r in rows] == [None, None, None, "u4"]
        assert [r["amount"] for r in rows] == [None] * 4
        assert [r["tenant"] for r in rows] == [None, None, None, "t1"]

        warnings = [
            r.getMessage() for r in caplog.records if r.levelno == logging.WARNING
        ]
        assert any("'amount'" in w and "don't convert to bigint" in w for w in warnings)
        # A missing or null value isn't a failed conversion.
        assert not any("'event_time'" in w for w in warnings)

    def test_an_object_at_the_path_is_text_and_fails_a_conversion_visibly(
        self, dest, caplog
    ):
        """A path leading to a document is never silently NULL."""
        _insert(
            dest,
            [(1, _event({"nested": "object"}, amount={"v": 1}), "2026-01-01", 1)],
        )
        with caplog.at_level(logging.WARNING):
            _run(dest)

        (row,) = _rows(dest)
        assert json.loads(row["user_id"]) == {"nested": "object"}
        assert row["amount"] is None
        assert any(
            "'amount'" in r.getMessage() and "don't convert to bigint" in r.getMessage()
            for r in caplog.records
            if r.levelno == logging.WARNING
        )


@pytest.mark.integration
class TestKeysThatAreNotIdentifiers:
    """A key with a hyphen, a space or a dot is quoted for the destination."""

    def test_json_columns_and_filter(self, dest):
        _insert(
            dest,
            [
                (
                    1,
                    {"headers": {"content-type": "text/html", "a.b": 1}},
                    "2026-01-01",
                    1,
                ),
                (
                    2,
                    {"headers": {"content-type": "text/csv", "a.b": 2}},
                    "2026-01-01",
                    2,
                ),
            ],
        )
        _run(
            dest,
            json_columns={
                "content_type": {"column": "payload", "path": "headers.content-type"},
                "dotted": {
                    "column": "payload",
                    "path": ["headers", "a.b"],
                    "data_type": "bigint",
                },
            },
            filters=[
                {
                    "column": "payload",
                    "path": "headers.content-type",
                    "value": "text/csv",
                }
            ],
        )
        (row,) = _rows(dest)
        assert (row["content_type"], row["dotted"]) == ("text/csv", 2)


DOCUMENT_COLUMNS = {
    "meta": {"column": "payload", "path": "meta", "data_type": "json"},
    "user_id": {"column": "payload", "path": "user_id"},
}


@pytest.mark.integration
class TestDocuments:
    """``data_type: json`` keeps the value at the path as a JSON document."""

    @pytest.mark.parametrize("source_table", [SOURCE_TABLE, JSON_SOURCE_TABLE])
    def test_document_column_is_json_typed(self, dest, source_table):
        _insert(dest, [(1, _event("u1", tenant="t9"), "2026-01-01", 1)], source_table)
        _run(dest, json_columns=DOCUMENT_COLUMNS, source_table=source_table)

        assert _target_types(dest)["meta"] == "JSON"
        (row,) = _rows(dest)
        assert json.loads(row["meta"]) == {"tenant": "t9"}

    def test_scalars_arrays_and_absent_values(self, dest):
        _insert(
            dest,
            [
                (1, {"meta": "plain"}, "2026-01-01", 1),
                (2, {"meta": [1, "two"]}, "2026-01-01", 2),
                (3, {"meta": None}, "2026-01-01", 3),
                (4, {}, "2026-01-01", 4),
                (5, "not json", "2026-01-01", 5),
                (6, None, "2026-01-01", 6),
            ],
        )
        _run(dest, json_columns=DOCUMENT_COLUMNS)

        metas = [r["meta"] for r in _rows(dest)]
        assert [json.loads(m) if m is not None else None for m in metas] == [
            "plain",
            [1, "two"],
            None,
            None,
            None,
            None,
        ]

    def test_versions_open_on_the_document_alone(self, dest):
        """The envelope excluded, a volatile field outside the document is inert."""
        settings = {
            "json_columns": DOCUMENT_COLUMNS,
            "exclude_columns": ["payload"],
            "ignore_columns": ["landing_offset"],
        }
        _insert(dest, [(1, _event("u1", tenant="t1", amount=1), "2026-01-01", 1)])
        _run(dest, **settings)
        _insert(dest, [(1, _event("u1", tenant="t1", amount=2), "2026-01-02", 2)])
        _run(dest, **settings)
        assert len(_rows(dest)) == 1

        _insert(dest, [(1, _event("u1", tenant="t2", amount=2), "2026-01-03", 3)])
        _run(dest, **settings)
        rows = _rows(dest)
        assert [json.loads(r["meta"])["tenant"] for r in rows] == ["t1", "t2"]
        assert "payload" not in rows[0]

    def test_switching_an_entry_to_json_needs_full_refresh(self, dest):
        _insert(dest, [(1, _event("u1"), "2026-01-01", 1)])
        _run(dest)
        _insert(dest, [(1, _event("u1"), "2026-01-02", 2)])
        as_document = {
            **JSON_COLUMNS,
            "tenant": {"column": "payload", "path": "meta", "data_type": "json"},
        }
        stats = _runner(dest, json_columns=as_document).run()
        assert stats["status"] == "failed"
        assert "json_columns.tenant" in stats["error"]
        assert "--full-refresh" in stats["error"]

        _run(dest, json_columns=as_document, full_refresh=True)
        assert _target_types(dest)["tenant"] == "JSON"


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
        assert "json_columns.amount" in stats["error"]
        assert "--full-refresh" in stats["error"]

        _run(dest, json_columns=retyped, full_refresh=True)

    def test_repathing_an_entry_needs_full_refresh(self, dest):
        _insert(dest, [(1, _event("u1"), "2026-01-01 00:00:00", 1)])
        _run(dest)
        _insert(dest, [(1, _event("u1"), "2026-01-02 00:00:00", 2)])
        repathed = {
            **JSON_COLUMNS,
            "tenant": {"column": "payload", "path": "user_id"},
        }
        stats = _runner(dest, json_columns=repathed).run()
        assert stats["status"] == "failed"
        assert "json_columns.tenant" in stats["error"]
        assert "--full-refresh" in stats["error"]
        assert len(_rows(dest)) == 1

    def test_readding_an_entry_with_another_type_is_refused(self, dest):
        """A removed entry's column stays on the target with its old type."""
        without_amount = {k: v for k, v in JSON_COLUMNS.items() if k != "amount"}
        _insert(dest, [(1, _event("u1", amount=5), "2026-01-01 00:00:00", 1)])
        _run(dest)
        _insert(dest, [(1, _event("u2", amount=5), "2026-01-02 00:00:00", 2)])
        _run(dest, json_columns=without_amount)
        before = _rows(dest)

        as_text = {
            **JSON_COLUMNS,
            "amount": {"column": "payload", "path": "amount", "data_type": "text"},
        }
        _insert(dest, [(1, _event("u3", amount="many"), "2026-01-03 00:00:00", 3)])
        stats = _runner(dest, json_columns=as_text).run()

        assert stats["status"] == "failed"
        assert (
            "amount: BIGINT in the historized table, VARCHAR in the source"
            in (stats["error"])
        )
        assert "--full-refresh" in stats["error"]
        assert _rows(dest) == before

    def test_removing_an_entry_warns_once(self, dest, caplog):
        without_amount = {k: v for k, v in JSON_COLUMNS.items() if k != "amount"}
        _insert(dest, [(1, _event("u1"), "2026-01-01 00:00:00", 1)])
        _run(dest)
        _insert(dest, [(1, _event("u2"), "2026-01-02 00:00:00", 2)])
        with caplog.at_level(logging.WARNING):
            _run(dest, json_columns=without_amount)
        assert any(
            "amount" in r.getMessage() and r.levelno == logging.WARNING
            for r in caplog.records
        )

        _insert(dest, [(1, _event("u3"), "2026-01-03 00:00:00", 3)])
        caplog.clear()
        with caplog.at_level(logging.WARNING):
            _run(dest, json_columns=without_amount)
        assert not [r for r in caplog.records if r.levelno == logging.WARNING]

    def test_historize_from_probes_only_its_window(self, dest, caplog):
        _insert(dest, [(1, _event("u1", amount="many"), "2026-01-01 00:00:00", 1)])
        with caplog.at_level(logging.WARNING):
            _run(dest)
        assert any("'amount'" in r.getMessage() for r in caplog.records)

        _insert(dest, [(1, _event("u1", amount=5), "2026-01-02 00:00:00", 2)])
        caplog.clear()
        with caplog.at_level(logging.WARNING):
            stats = _run(dest, historize_from="2026-01-02 00:00:00")
        assert stats["mode"] == "partial_refresh"
        assert not [r for r in caplog.records if "'amount'" in r.getMessage()]
