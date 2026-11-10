"""Integration tests for historize schema evolution on DuckDB.

The historized table is created from the source's columns on the first run.
When the source later gains or loses a column, an incremental run reconciles
the target before writing: a gained column is added (earlier rows hold NULL),
a lost column stays with its history and is read from the source as NULL, and
a changed type is refused with a pointer at ``--full-refresh``. None of it is a
config change, so none of it trips the full-refresh prompt.
"""

import logging

import pytest

from dlt_saga.destinations.duckdb.config import DuckDBDestinationConfig
from dlt_saga.destinations.duckdb.destination import DuckDBDestination
from dlt_saga.historize.config import HistorizeConfig
from dlt_saga.historize.runner import HistorizeRunner

SCHEMA = "test_evolution"
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
            other VARCHAR,
            snapshot_at TIMESTAMP
        )
        """,
        SCHEMA,
    )
    yield destination
    destination.close()


def _sql(dest, sql):
    dest.execute_sql(sql, SCHEMA)


def _literal(value):
    if value is None:
        return "NULL"
    if isinstance(value, str):
        return repr(value)
    return str(value)


def _insert(dest, snapshot, rows, columns=("id", "payload")):
    """Insert rows for one snapshot; ``rows`` match ``columns`` positionally."""
    col_list = ", ".join(f'"{c}"' for c in (*columns, "snapshot_at"))
    values = ", ".join(
        f"({', '.join(_literal(v) for v in row)}, TIMESTAMP '{snapshot}')"
        for row in rows
    )
    _sql(dest, f"INSERT INTO {SOURCE} ({col_list}) VALUES {values}")


def _runner(dest, strategy, **kwargs):
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


def _run(dest, strategy, **kwargs):
    stats = _runner(dest, strategy, **kwargs).run()
    assert stats["status"] == "completed", stats.get("error")
    return stats


def _target(dest):
    result = dest.connection.execute(
        f'SELECT * FROM "{SCHEMA}"."{TARGET_TABLE}" ORDER BY id, _dlt_valid_from'
    )
    cols = [c[0] for c in result.description]
    return [dict(zip(cols, r)) for r in result.fetchall()]


def _columns(dest, table):
    return {name: dtype for name, dtype in dest.list_table_columns(SCHEMA, table)}


def _log(dest):
    result = dest.connection.execute(
        f'SELECT status, config_fingerprint, source_columns FROM "{SCHEMA}".'
        f'"_saga_historize_log" ORDER BY started_at'
    )
    return [
        dict(zip(("status", "fingerprint", "source_columns"), r))
        for r in result.fetchall()
    ]


def _versions(dest, key):
    return [r for r in _target(dest) if r["id"] == key]


class TestScd2AddedColumn:
    def test_column_added_and_versioned_from_first_value(self, dest):
        _insert(dest, S1, [(1, "a"), (2, "b")])
        _run(dest, "scd2", full_refresh=True)
        assert "extra" not in _columns(dest, TARGET_TABLE)

        _sql(dest, f"ALTER TABLE {SOURCE} ADD COLUMN extra VARCHAR")
        _insert(dest, S2, [(1, "a", "x"), (2, "b", None)], ("id", "payload", "extra"))
        stats = _run(dest, "scd2")

        assert stats["mode"] == "incremental"
        assert _columns(dest, TARGET_TABLE)["extra"] == "VARCHAR"
        # Key 1 gained a value: unknown → known is a change, versioned at S2.
        one = _versions(dest, 1)
        assert [(r["extra"], str(r["_dlt_valid_to"])) for r in one] == [
            (None, S2),
            ("x", "None"),
        ]
        # Key 2 is still unknown in the new column: nothing changed.
        two = _versions(dest, 2)
        assert len(two) == 1 and two[0]["extra"] is None

    def test_not_a_config_change(self, dest):
        _insert(dest, S1, [(1, "a")])
        _run(dest, "scd2", full_refresh=True)
        _sql(dest, f"ALTER TABLE {SOURCE} ADD COLUMN extra VARCHAR")
        _insert(dest, S2, [(1, "a", "x")], ("id", "payload", "extra"))
        _run(dest, "scd2")

        log = _log(dest)
        assert [e["status"] for e in log] == ["completed", "completed"]
        assert log[0]["fingerprint"] == log[1]["fingerprint"]
        assert '"extra"' in log[1]["source_columns"]

    def test_ignored_column_is_carried_but_opens_no_version(self, dest):
        _insert(dest, S1, [(1, "a")])
        _run(dest, "scd2", full_refresh=True, ignore_columns=["extra"])
        _sql(dest, f"ALTER TABLE {SOURCE} ADD COLUMN extra VARCHAR")
        _insert(dest, S2, [(1, "a", "x")], ("id", "payload", "extra"))
        _run(dest, "scd2", ignore_columns=["extra"])

        assert "extra" in _columns(dest, TARGET_TABLE)
        one = _versions(dest, 1)
        assert len(one) == 1 and one[0]["extra"] is None

    def test_partial_refresh_reconciles_before_cloning(self, dest):
        _insert(dest, S1, [(1, "a")])
        _run(dest, "scd2", full_refresh=True)
        _insert(dest, S2, [(1, "b")])
        _run(dest, "scd2")

        _sql(dest, f"ALTER TABLE {SOURCE} ADD COLUMN extra VARCHAR")
        _sql(dest, f"UPDATE {SOURCE} SET extra = 'x' WHERE snapshot_at = '{S2}'")
        stats = _run(dest, "scd2", partial_refresh=True, historize_from=S2)

        assert stats["mode"] == "partial_refresh"
        one = _versions(dest, 1)
        assert [(r["payload"], r["extra"]) for r in one] == [("a", None), ("b", "x")]


class TestScd2DroppedColumn:
    def test_next_version_carries_null_and_history_is_kept(self, dest, caplog):
        _insert(dest, S1, [(1, "a"), (2, "b")])
        _run(dest, "scd2", full_refresh=True)

        _sql(dest, f"ALTER TABLE {SOURCE} DROP COLUMN payload")
        _insert(dest, S2, [(1,), (2,)], ("id",))
        with caplog.at_level(logging.WARNING):
            _run(dest, "scd2")

        assert "payload" in _columns(dest, TARGET_TABLE)
        one = _versions(dest, 1)
        assert [(r["payload"], str(r["_dlt_valid_from"])) for r in one] == [
            ("a", S1),
            (None, S2),
        ]
        warnings = [r for r in caplog.records if r.levelno == logging.WARNING]
        assert any("payload" in r.getMessage() for r in warnings)

    def test_warns_only_the_first_time(self, dest, caplog):
        _insert(dest, S1, [(1, "a")])
        _run(dest, "scd2", full_refresh=True)
        _sql(dest, f"ALTER TABLE {SOURCE} DROP COLUMN payload")
        _insert(dest, S2, [(1,)], ("id",))
        _run(dest, "scd2")

        _insert(dest, S3, [(1,)], ("id",))
        caplog.clear()
        with caplog.at_level(logging.DEBUG, logger="dlt_saga.historize.runner"):
            _run(dest, "scd2")

        assert not [r for r in caplog.records if r.levelno == logging.WARNING]
        assert any(
            "payload" in r.getMessage() and r.levelno == logging.DEBUG
            for r in caplog.records
        )
        # NULL → NULL is not a change.
        assert len(_versions(dest, 1)) == 2

    def test_partial_refresh_reads_the_column_as_null(self, dest):
        _insert(dest, S1, [(1, "a")])
        _run(dest, "scd2", full_refresh=True)
        _insert(dest, S2, [(1, "b")])
        _run(dest, "scd2")

        # The clone-and-swap reads the source through its own SQL builder,
        # which must project the dropped column the same way the main one does.
        _sql(dest, f"ALTER TABLE {SOURCE} DROP COLUMN payload")
        _insert(dest, S3, [(1,)], ("id",))
        stats = _run(dest, "scd2", historize_from=S2)

        assert stats["mode"] == "partial_refresh"
        one = _versions(dest, 1)
        assert [(r["payload"], str(r["_dlt_valid_from"])) for r in one] == [
            ("a", S1),
            (None, S2),
        ]

    def test_full_refresh_rebuilds_without_the_column(self, dest):
        _insert(dest, S1, [(1, "a")])
        _run(dest, "scd2", full_refresh=True)
        _sql(dest, f"ALTER TABLE {SOURCE} DROP COLUMN payload")
        _run(dest, "scd2", full_refresh=True)

        assert "payload" not in _columns(dest, TARGET_TABLE)


class TestScd1:
    def test_added_column_moves_valid_from_only_where_a_value_arrived(self, dest):
        _insert(dest, S1, [(1, "a"), (2, "b")])
        _run(dest, "scd1", full_refresh=True)

        _sql(dest, f"ALTER TABLE {SOURCE} ADD COLUMN extra VARCHAR")
        _insert(dest, S2, [(1, "a", "x"), (2, "b", None)], ("id", "payload", "extra"))
        _run(dest, "scd1")

        rows = {r["id"]: r for r in _target(dest)}
        assert rows[1]["extra"] == "x"
        assert str(rows[1]["_dlt_valid_from"]) == S2
        assert rows[2]["extra"] is None
        assert str(rows[2]["_dlt_valid_from"]) == S1

    def test_dropped_column_is_nulled_on_the_updated_row(self, dest, caplog):
        _insert(dest, S1, [(1, "a"), (2, "b")])
        _run(dest, "scd1", full_refresh=True)

        _sql(dest, f"ALTER TABLE {SOURCE} DROP COLUMN payload")
        _insert(dest, S2, [(1,)], ("id",))
        with caplog.at_level(logging.WARNING):
            _run(dest, "scd1")

        rows = {r["id"]: r for r in _target(dest)}
        # Key 1 was re-delivered without the column: its current row says so.
        assert rows[1]["payload"] is None
        assert str(rows[1]["_dlt_valid_from"]) == S2
        # Key 2 wasn't touched; its row still describes the version it holds.
        assert rows[2]["payload"] == "b"
        assert str(rows[2]["_dlt_valid_from"]) == S1
        assert any("payload" in r.getMessage() for r in caplog.records)


@pytest.mark.parametrize("strategy", ["scd2", "scd1"])
class TestTypeChange:
    def test_refused_before_any_write(self, dest, strategy):
        _insert(dest, S1, [(1, "a")])
        _run(dest, strategy, full_refresh=True)
        before = _target(dest)

        _sql(dest, f"ALTER TABLE {SOURCE} DROP COLUMN payload")
        _sql(dest, f"ALTER TABLE {SOURCE} ADD COLUMN payload INTEGER")
        _insert(dest, S2, [(1, 7)])
        stats = _runner(dest, strategy).run()

        assert stats["status"] == "failed"
        assert "payload" in stats["error"]
        assert "VARCHAR" in stats["error"] and "INTEGER" in stats["error"]
        assert "--full-refresh" in stats["error"]
        assert _target(dest) == before
        assert [e["status"] for e in _log(dest)] == ["completed", "failed"]

    def test_full_refresh_takes_the_new_type(self, dest, strategy):
        _insert(dest, S1, [(1, "a")])
        _run(dest, strategy, full_refresh=True)
        _sql(dest, f"ALTER TABLE {SOURCE} DROP COLUMN payload")
        _sql(dest, f"ALTER TABLE {SOURCE} ADD COLUMN payload INTEGER")
        _insert(dest, S2, [(1, 7)])

        _run(dest, strategy, full_refresh=True)

        assert _columns(dest, TARGET_TABLE)["payload"] == "INTEGER"


@pytest.mark.integration
class TestNestedFields:
    """A struct column's fields come and go the way columns do, one level
    down: a field the source gains widens the target's struct and opens a
    version only where a value arrived, a field it loses is read as NULL, a
    field order change is nothing, a retyped field is refused."""

    def _add_meta(self, dest, type_="STRUCT(a VARCHAR)"):
        _sql(dest, f"ALTER TABLE {SOURCE} ADD COLUMN meta {type_}")

    def _insert_meta(self, dest, snapshot, rows):
        """``rows``: ``(id, payload, meta literal)``."""
        values = ", ".join(
            f"({i}, {_literal(p)}, {m}, TIMESTAMP '{snapshot}')" for i, p, m in rows
        )
        _sql(
            dest,
            f"INSERT INTO {SOURCE} (id, payload, meta, snapshot_at) VALUES {values}",
        )

    def _retype_meta(self, dest, new_type, using):
        _sql(
            dest,
            f"ALTER TABLE {SOURCE} ALTER COLUMN meta SET DATA TYPE {new_type} USING {using}",
        )

    @pytest.mark.parametrize("strategy", ["scd2", "scd1"])
    def test_added_field_opens_a_version_only_where_a_value_arrived(
        self, dest, strategy
    ):
        self._add_meta(dest)
        self._insert_meta(dest, S1, [(1, "a", "{'a': 'x'}"), (2, "b", "{'a': 'y'}")])
        _run(dest, strategy, full_refresh=True)

        self._retype_meta(
            dest,
            "STRUCT(a VARCHAR, b INTEGER)",
            "struct_pack(a := struct_extract(meta, 'a'), b := NULL::INTEGER)",
        )
        self._insert_meta(
            dest,
            S2,
            [
                (1, "a", "{'a': 'x', 'b': 7}"),
                (2, "b", "{'a': 'y', 'b': NULL::INTEGER}"),
            ],
        )
        _run(dest, strategy)

        assert _columns(dest, TARGET_TABLE)["meta"] == "STRUCT(a VARCHAR, b INTEGER)"
        one, two = _versions(dest, 1), _versions(dest, 2)
        if strategy == "scd2":
            assert [v["meta"] for v in one] == [
                {"a": "x", "b": None},
                {"a": "x", "b": 7},
            ]
            assert [v["meta"] for v in two] == [{"a": "y", "b": None}]
        else:
            assert one[0]["meta"] == {"a": "x", "b": 7}
            assert str(one[0]["_dlt_valid_from"]).startswith("2026-01-02")
            assert str(two[0]["_dlt_valid_from"]).startswith("2026-01-01")
        fingerprints = {e["fingerprint"] for e in _log(dest)}
        assert len(fingerprints) == 1

    def test_added_field_inside_an_array_of_structs(self, dest):
        self._add_meta(dest, "STRUCT(a VARCHAR)[]")
        self._insert_meta(
            dest, S1, [(1, "a", "[{'a': 'x'}]"), (2, "b", "[{'a': 'y'}]")]
        )
        _run(dest, "scd2", full_refresh=True)

        self._retype_meta(
            dest,
            "STRUCT(a VARCHAR, b INTEGER)[]",
            "list_transform(meta, "
            + DuckDBDestination.lambda_sql(
                "e", "struct_pack(a := struct_extract(e, 'a'), b := NULL::INTEGER)"
            )
            + ")",
        )
        self._insert_meta(
            dest,
            S2,
            [
                (1, "a", "[{'a': 'x', 'b': 7}]"),
                (2, "b", "[{'a': 'y', 'b': NULL::INTEGER}]"),
            ],
        )
        _run(dest, "scd2")

        assert _columns(dest, TARGET_TABLE)["meta"] == "STRUCT(a VARCHAR, b INTEGER)[]"
        assert [v["meta"] for v in _versions(dest, 1)] == [
            [{"a": "x", "b": None}],
            [{"a": "x", "b": 7}],
        ]
        assert len(_versions(dest, 2)) == 1

    def test_removed_field_is_read_as_null_and_history_is_kept(self, dest, caplog):
        self._add_meta(dest, "STRUCT(a VARCHAR, b INTEGER)")
        self._insert_meta(dest, S1, [(1, "a", "{'a': 'x', 'b': 7}")])
        _run(dest, "scd2", full_refresh=True)

        self._retype_meta(
            dest, "STRUCT(a VARCHAR)", "struct_pack(a := struct_extract(meta, 'a'))"
        )
        self._insert_meta(dest, S2, [(1, "a", "{'a': 'x'}")])
        with caplog.at_level(logging.WARNING, logger="dlt_saga.historize.runner"):
            _run(dest, "scd2")

        assert _columns(dest, TARGET_TABLE)["meta"] == "STRUCT(a VARCHAR, b INTEGER)"
        assert [v["meta"] for v in _versions(dest, 1)] == [
            {"a": "x", "b": 7},
            {"a": "x", "b": None},
        ]
        assert "no longer has nested field(s) ['meta.b']" in caplog.text

        caplog.clear()
        self._insert_meta(dest, S3, [(1, "a", "{'a': 'z'}")])
        with caplog.at_level(logging.WARNING, logger="dlt_saga.historize.runner"):
            _run(dest, "scd2")
        assert "no longer has nested field" not in caplog.text
        assert len(_versions(dest, 1)) == 3

    def test_reordered_fields_open_no_version(self, dest):
        self._add_meta(dest, "STRUCT(a VARCHAR, b INTEGER)")
        self._insert_meta(dest, S1, [(1, "a", "{'a': 'x', 'b': 7}")])
        _run(dest, "scd2", full_refresh=True)

        self._retype_meta(
            dest,
            "STRUCT(b INTEGER, a VARCHAR)",
            "struct_pack(b := struct_extract(meta, 'b'), a := struct_extract(meta, 'a'))",
        )
        self._insert_meta(dest, S2, [(1, "a", "{'b': 7, 'a': 'x'}")])
        _run(dest, "scd2")

        assert _columns(dest, TARGET_TABLE)["meta"] == "STRUCT(a VARCHAR, b INTEGER)"
        assert len(_versions(dest, 1)) == 1

    def test_retyped_nested_field_is_refused_before_any_write(self, dest):
        self._add_meta(dest)
        self._insert_meta(dest, S1, [(1, "a", "{'a': 'x'}")])
        _run(dest, "scd2", full_refresh=True)
        before = _target(dest)

        self._retype_meta(dest, "STRUCT(a INTEGER)", "struct_pack(a := NULL::INTEGER)")
        self._insert_meta(dest, S2, [(1, "a", "{'a': 1}")])
        stats = _runner(dest, "scd2").run()

        assert stats["status"] == "failed"
        assert (
            "meta.a: VARCHAR in the historized table, INTEGER in the source"
            in stats["error"]
        )
        assert "--full-refresh" in stats["error"]
        assert _target(dest) == before

    def test_partial_refresh_reads_through_the_reshape(self, dest):
        self._add_meta(dest)
        self._insert_meta(dest, S1, [(1, "a", "{'a': 'x'}")])
        _run(dest, "scd2", full_refresh=True)
        self._insert_meta(dest, S2, [(1, "a", "{'a': 'y'}")])
        _run(dest, "scd2")

        self._retype_meta(
            dest,
            "STRUCT(a VARCHAR, b INTEGER)",
            "struct_pack(a := struct_extract(meta, 'a'), b := NULL::INTEGER)",
        )
        _sql(
            dest,
            f"UPDATE {SOURCE} SET meta = {{'a': 'y', 'b': 7}} WHERE snapshot_at = '{S2}'",
        )
        stats = _run(dest, "scd2", partial_refresh=True, historize_from=S2)

        assert stats["mode"] == "partial_refresh"
        assert [v["meta"] for v in _versions(dest, 1)] == [
            {"a": "x", "b": None},
            {"a": "y", "b": 7},
        ]
