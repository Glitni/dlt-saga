"""Integration tests for ``saga destroy`` teardown against DuckDB.

Covers the two ownership-driven layers:

* historize — ``HistorizeRunner.destroy`` / ``get_historized_targets`` read the
  target table from ``_saga_historize_log``; a table sitting at the derived
  name but not recorded under this pipeline is never dropped.
* ingest — ``Destination.get_ingested_targets`` reads the loaded table from
  ``_saga_load_info``.
"""

from dlt_saga.project_config import get_load_info_table_name

from .conftest import (
    SCHEMA,
    SNAPSHOT_1,
    SNAPSHOT_2,
    TARGET_TABLE,
    make_historize_runner,
    query_log,
    run_historize,
    seed_raw_table,
)

PIPELINE = "test__raw_companies"


# ---------------------------------------------------------------------------
# Historize layer
# ---------------------------------------------------------------------------


def test_get_historized_targets_reads_the_log(duckdb_destination):
    dest = duckdb_destination
    run_historize(dest, [SNAPSHOT_1, SNAPSHOT_2])

    runner = make_historize_runner(dest)
    # The log records a target for this pipeline (used as the ownership gate).
    owned = runner.state_manager.get_historized_targets(PIPELINE)
    assert len(owned) == 1
    assert TARGET_TABLE in owned[0]
    # Unknown pipeline owns nothing even though the log table exists.
    assert runner.state_manager.get_historized_targets("nope__nope") == []


def test_get_historized_targets_missing_log_table(duckdb_destination):
    """No historize has ever run — the log table doesn't exist yet."""
    runner = make_historize_runner(duckdb_destination)
    assert runner.state_manager.get_historized_targets(PIPELINE) == []


def test_historize_destroy_drops_recorded_target(duckdb_destination):
    dest = duckdb_destination
    run_historize(dest, [SNAPSHOT_1, SNAPSHOT_2])
    assert dest.table_exists(SCHEMA, TARGET_TABLE)

    runner = make_historize_runner(dest)
    result = runner.destroy()

    assert result["table"] == TARGET_TABLE
    assert result["dropped"] is True
    assert result["log_cleared"] is True
    assert not dest.table_exists(SCHEMA, TARGET_TABLE)
    # Log entries for the pipeline are cleared, so it now owns nothing.
    assert runner.state_manager.get_historized_targets(PIPELINE) == []


def test_historize_destroy_dry_run_changes_nothing(duckdb_destination):
    dest = duckdb_destination
    run_historize(dest, [SNAPSHOT_1, SNAPSHOT_2])

    runner = make_historize_runner(dest)
    result = runner.destroy(dry_run=True)

    # The would-drop target is reported, but nothing is actually removed.
    assert result["table"] == TARGET_TABLE
    assert result["dropped"] is True
    assert dest.table_exists(SCHEMA, TARGET_TABLE)
    assert query_log(dest), "dry-run must not clear the historize log"


def test_historize_destroy_ignores_unowned_table(duckdb_destination):
    """A table at the derived name but with no log record is never dropped.

    This is the false-positive guard: existence at a config's derived name is
    not proof of ownership, so destroy leaves it alone.
    """
    dest = duckdb_destination
    seed_raw_table(dest, [SNAPSHOT_1])
    # A table squatting on the derived historized name, created out-of-band
    # (no historize run, so no _saga_historize_log entry names it).
    dest.execute_sql(f'CREATE TABLE "{SCHEMA}"."{TARGET_TABLE}" (id INTEGER)', SCHEMA)

    runner = make_historize_runner(dest)
    assert runner.state_manager.get_historized_targets(PIPELINE) == []

    result = runner.destroy()
    assert result["dropped"] is False
    assert result["log_cleared"] is False
    assert dest.table_exists(SCHEMA, TARGET_TABLE), "unowned table must survive"


def test_historize_destroy_no_footprint_is_noop(duckdb_destination):
    runner = make_historize_runner(duckdb_destination)
    result = runner.destroy()
    assert result["table"] is None
    assert result["dropped"] is False
    assert result["log_cleared"] is False


# ---------------------------------------------------------------------------
# Ingest layer — ownership read from _saga_load_info
# ---------------------------------------------------------------------------


def _seed_load_info(dest, rows):
    li = get_load_info_table_name()
    dest.execute_sql(f'CREATE SCHEMA IF NOT EXISTS "{SCHEMA}"', SCHEMA)
    dest.execute_sql(
        f'CREATE TABLE IF NOT EXISTS "{SCHEMA}"."{li}" '
        "(pipeline_name VARCHAR, table_name VARCHAR)",
        SCHEMA,
    )
    for pipeline_name, table_name in rows:
        dest.execute_sql(
            f'INSERT INTO "{SCHEMA}"."{li}" VALUES '
            f"('{pipeline_name}', '{table_name}')",
            SCHEMA,
        )


def test_get_ingested_targets_reads_load_info(duckdb_destination):
    dest = duckdb_destination
    _seed_load_info(
        dest,
        [
            (PIPELINE, "raw_companies"),
            (PIPELINE, "raw_companies"),  # duplicate run — deduped
            ("other__thing", "something_else"),
        ],
    )
    assert dest.get_ingested_targets(SCHEMA, PIPELINE) == ["raw_companies"]
    assert dest.get_ingested_targets(SCHEMA, "unknown__pipe") == []


def test_get_ingested_targets_missing_load_info_table(duckdb_destination):
    assert duckdb_destination.get_ingested_targets(SCHEMA, PIPELINE) == []
