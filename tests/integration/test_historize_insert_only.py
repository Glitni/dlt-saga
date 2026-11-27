"""Integration tests for historize ``strategy: insert_only``.

The source is an append-only event table: a key is one event, delivered in
files (``file_date``, the snapshot column) and stamped when loaded
(``ingested_at``, the arrival column). An event can be delivered again, in
the same file or a later one, with another ``received_at``.
"""

import pytest

from dlt_saga.destinations.duckdb.config import DuckDBDestinationConfig
from dlt_saga.destinations.duckdb.destination import DuckDBDestination
from dlt_saga.historize.config import HistorizeConfig
from dlt_saga.historize.runner import HistorizeRunner

SCHEMA = "test_insert_only"
SOURCE_TABLE = "events"
TARGET_TABLE = "events_historized"
PIPELINE = "test__events"


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
            received_at TIMESTAMP,
            file_date TIMESTAMP,
            ingested_at TIMESTAMP
        )
        """,
        SCHEMA,
    )
    yield destination
    destination.close()


def _insert(dest, rows, ingested_at="2026-01-01 00:00"):
    """Insert ``(id, payload, event_time, received_at, file_date)`` rows."""
    values = ", ".join(
        f"({i}, {p!r}, TIMESTAMP '{et}', TIMESTAMP '{ra}', TIMESTAMP '{fd}', "
        f"TIMESTAMP '{ingested_at}')"
        for i, p, et, ra, fd in rows
    )
    dest.execute_sql(f'INSERT INTO "{SCHEMA}"."{SOURCE_TABLE}" VALUES {values}', SCHEMA)


def _runner(dest, *, full_refresh=False, historize_from=None, **overrides):
    settings = {
        "strategy": "insert_only",
        "primary_key": ["id"],
        "snapshot_column": "file_date",
        "arrival_column": "ingested_at",
        **overrides,
    }
    runner = HistorizeRunner(
        pipeline_name=PIPELINE,
        historize_config=HistorizeConfig(**settings),
        destination=dest,
        database="test",
        schema=SCHEMA,
        source_table_name=SOURCE_TABLE,
        target_table_name=TARGET_TABLE,
        config_dict={"write_disposition": "historize", "primary_key": ["id"]},
        full_refresh=full_refresh,
        historize_from=historize_from,
    )
    if settings.get("partition_column"):
        # DuckDB has no partitioning; the stored-key bound is a plain predicate
        # and works without it.
        runner._validate_destination_capabilities = lambda: None
    return runner


def _run(dest, **kwargs):
    stats = _runner(dest, **kwargs).run()
    assert stats["status"] == "completed", stats.get("error")
    return stats


def _target(dest):
    result = dest.connection.execute(
        f'SELECT * FROM "{SCHEMA}"."{TARGET_TABLE}" ORDER BY id, received_at'
    )
    cols = [c[0] for c in result.description]
    return [dict(zip(cols, r)) for r in result.fetchall()]


def _payloads(dest):
    return [(row["id"], row["payload"]) for row in _target(dest)]


def _completed_log_rows(dest):
    result = dest.connection.execute(
        f'SELECT snapshot_value FROM "{SCHEMA}"."_saga_historize_log" '
        f"WHERE pipeline_name = '{PIPELINE}' AND status = 'completed' "
        f"ORDER BY finished_at"
    )
    return [r[0] for r in result.fetchall()]


@pytest.mark.integration
class TestInsertOnlyShape:
    def test_one_row_per_key_with_only_valid_from(self, dest):
        _insert(
            dest,
            [
                (1, "a", "2026-01-01", "2026-01-01 10:00", "2026-01-01 12:00"),
                (2, "b", "2026-01-01", "2026-01-01 10:00", "2026-01-01 12:00"),
            ],
        )
        stats = _run(dest)

        rows = _target(dest)
        assert _payloads(dest) == [(1, "a"), (2, "b")]
        assert set(rows[0]) == {
            "id",
            "payload",
            "event_time",
            "received_at",
            "ingested_at",
            "_dlt_valid_from",
        }
        assert str(rows[0]["_dlt_valid_from"]) == "2026-01-01 12:00:00"
        assert stats["mode"] == "full_reprocess"
        assert stats["new_or_changed_rows"] == 2
        assert stats["deleted_rows"] == 0

    def test_duplicate_in_one_run_keeps_the_earliest_delivery(self, dest):
        _insert(
            dest,
            [
                (1, "later", "2026-01-01", "2026-01-02 10:00", "2026-01-02 12:00"),
                (1, "first", "2026-01-01", "2026-01-01 10:00", "2026-01-01 12:00"),
            ],
        )
        _run(dest)

        assert _payloads(dest) == [(1, "first")]


@pytest.mark.integration
class TestInsertOnlyIncremental:
    def test_stored_key_is_never_replaced_and_new_key_is_inserted(self, dest):
        _insert(dest, [(1, "first", "2026-01-01", "2026-01-01 10:00", "2026-01-01")])
        _run(dest)

        _insert(
            dest,
            [
                (1, "again", "2026-01-01", "2026-01-03 10:00", "2026-01-03"),
                (2, "new", "2026-01-03", "2026-01-03 10:00", "2026-01-03"),
            ],
            ingested_at="2026-01-03 00:00",
        )
        stats = _run(dest)

        assert stats["mode"] == "incremental"
        assert stats["new_or_changed_rows"] == 1
        assert _payloads(dest) == [(1, "first"), (2, "new")]

    def test_rerun_with_nothing_new_changes_nothing(self, dest):
        _insert(dest, [(1, "first", "2026-01-01", "2026-01-01 10:00", "2026-01-01")])
        _run(dest)
        before = _target(dest)

        stats = _run(dest)

        assert stats["snapshots_processed"] == 0
        assert _target(dest) == before

    def test_full_refresh_rebuilds_from_the_source(self, dest):
        _insert(dest, [(1, "first", "2026-01-01", "2026-01-01 10:00", "2026-01-01")])
        _run(dest)
        dest.execute_sql(
            f'DELETE FROM "{SCHEMA}"."{TARGET_TABLE}"',
            SCHEMA,
        )

        _run(dest, full_refresh=True)

        assert _payloads(dest) == [(1, "first")]


@pytest.mark.integration
class TestInsertOnlyBatches:
    ROWS = [
        (1, "first", "2026-01-01", "2026-01-01 10:00", "2026-01-01"),
        (2, "b", "2026-01-02", "2026-01-02 10:00", "2026-01-02"),
        (1, "again", "2026-01-01", "2026-01-03 10:00", "2026-01-03"),
        (3, "c", "2026-01-03", "2026-01-03 10:00", "2026-01-03"),
    ]

    def test_backlog_is_split_and_each_batch_recorded(self, dest):
        _insert(dest, self.ROWS)
        _run(dest, batch_max_rows=1)

        assert _completed_log_rows(dest) == [
            "2026-01-01 00:00:00",
            "2026-01-02 00:00:00",
            "2026-01-03 00:00:00",
        ]
        assert _payloads(dest) == [(1, "first"), (2, "b"), (3, "c")]

    def test_batching_does_not_change_the_result(self, dest):
        _insert(dest, self.ROWS)
        _run(dest, batch_max_rows=1)
        batched = _target(dest)

        _run(dest, full_refresh=True)

        assert _target(dest) == batched
        assert len(_completed_log_rows(dest)) == 1


@pytest.mark.integration
class TestInsertOnlyLateRows:
    def test_rows_below_the_watermark_are_inserted(self, dest):
        _insert(dest, [(1, "first", "2026-01-05", "2026-01-05 10:00", "2026-01-05")])
        _run(dest)

        _insert(
            dest,
            [
                (1, "older copy", "2026-01-05", "2026-01-02 10:00", "2026-01-02"),
                (2, "late", "2026-01-02", "2026-01-02 10:00", "2026-01-02"),
            ],
            ingested_at="2100-01-01 00:00",
        )
        stats = _run(dest)

        assert stats["new_or_changed_rows"] == 1
        assert stats["snapshots_processed"] == 1
        assert _payloads(dest) == [(1, "first"), (2, "late")]
        assert _completed_log_rows(dest)[-1] == "2026-01-05 00:00:00"

    def test_window_bounds_how_far_back_late_rows_are_looked_for(self, dest):
        _insert(dest, [(1, "first", "2026-01-05", "2026-01-05 10:00", "2026-01-05")])
        _run(dest, late_arrival_window_days=1)

        _insert(
            dest,
            [
                (2, "too old", "2026-01-02", "2026-01-02 10:00", "2026-01-02"),
                (3, "recent", "2026-01-04", "2026-01-04 10:00", "2026-01-04 12:00"),
            ],
            ingested_at="2100-01-01 00:00",
        )
        _run(dest, late_arrival_window_days=1)

        assert _payloads(dest) == [(1, "first"), (3, "recent")]

    def test_backlog_is_one_batch_without_a_limit(self, dest):
        _insert(
            dest,
            [
                (1, "a", "2026-01-01", "2026-01-01 10:00", "2026-01-01"),
                (2, "b", "2026-01-02", "2026-01-02 10:00", "2026-01-02"),
            ],
        )
        _run(dest)

        assert _completed_log_rows(dest) == ["2026-01-02 00:00:00"]

    def test_unconvertible_json_value_in_a_late_row_is_warned_about(self, dest, caplog):
        json_columns = {
            "amount": {"column": "payload", "path": "amount", "data_type": "bigint"}
        }
        _insert(dest, [(1, '{"amount": 5}', "2026-01-05", "2026-01-05", "2026-01-05")])
        _run(dest, json_columns=json_columns)

        _insert(
            dest,
            [(2, '{"amount": "x"}', "2026-01-02", "2026-01-02", "2026-01-02")],
            ingested_at="2100-01-01 00:00",
        )
        with caplog.at_level("WARNING"):
            _run(dest, json_columns=json_columns)

        assert "json_columns 'amount' has values" in caplog.text
        assert [row["amount"] for row in _target(dest)] == [5, None]

    def test_detection_disabled_leaves_them_out(self, dest):
        _insert(dest, [(1, "first", "2026-01-05", "2026-01-05 10:00", "2026-01-05")])
        _run(dest)

        _insert(
            dest,
            [(2, "late", "2026-01-02", "2026-01-02 10:00", "2026-01-02")],
            ingested_at="2100-01-01 00:00",
        )
        _run(dest, detect_late_arrivals=False)

        assert _payloads(dest) == [(1, "first")]


@pytest.mark.integration
class TestInsertOnlyWindow:
    def _seed(self, dest):
        _insert(dest, [(1, "first", "2026-01-10", "2026-01-10 10:00", "2026-01-10")])

    def test_duplicate_inside_the_window_is_not_stored_again(self, dest):
        self._seed(dest)
        _run(dest, partition_column="received_at", dedup_window_days=1)

        _insert(
            dest,
            [(1, "again", "2026-01-10", "2026-01-11 09:00", "2026-01-11")],
            ingested_at="2026-01-11 00:00",
        )
        _run(dest, partition_column="received_at", dedup_window_days=1)

        assert _payloads(dest) == [(1, "first")]

    def test_duplicate_outside_the_window_is_stored_again(self, dest):
        self._seed(dest)
        _run(dest, partition_column="received_at", dedup_window_days=1)

        _insert(
            dest,
            [(1, "again", "2026-01-10", "2026-01-20 09:00", "2026-01-20")],
            ingested_at="2026-01-20 00:00",
        )
        _run(dest, partition_column="received_at", dedup_window_days=1)

        assert _payloads(dest) == [(1, "first"), (1, "again")]

    def test_without_a_window_the_check_covers_the_whole_table(self, dest):
        self._seed(dest)
        _run(dest)

        _insert(
            dest,
            [(1, "again", "2026-01-10", "2026-01-20 09:00", "2026-01-20")],
            ingested_at="2026-01-20 00:00",
        )
        _run(dest)

        assert _payloads(dest) == [(1, "first")]

    def test_non_date_partition_column_is_refused_before_any_write(self, dest):
        self._seed(dest)
        stats = _runner(dest, partition_column="payload", dedup_window_days=0).run()

        assert stats["status"] == "failed"
        assert "date or timestamp" in stats["error"]


@pytest.mark.integration
class TestInsertOnlyPartialRefresh:
    def test_window_is_deleted_and_reinserted_from_the_source(self, dest):
        _insert(
            dest,
            [
                (1, "kept", "2026-01-01", "2026-01-01 10:00", "2026-01-01"),
                (2, "stale", "2026-01-05", "2026-01-05 10:00", "2026-01-05"),
            ],
        )
        _run(dest)
        dest.execute_sql(
            f'UPDATE "{SCHEMA}"."{SOURCE_TABLE}" SET payload = \'fixed\' WHERE id = 2',
            SCHEMA,
        )

        stats = _run(dest, historize_from="2026-01-03 00:00:00")

        assert stats["mode"] == "partial_refresh"
        assert _payloads(dest) == [(1, "kept"), (2, "fixed")]
