"""Integration tests for historize ``strategy: scd1`` with ``track_deletions``.

The source delivers full snapshots: every record that exists at a snapshot is in
it, so a key missing from a later snapshot was deleted. A deleted key keeps its
row, flagged, with the values it was last seen with.
"""

import pytest

from dlt_saga.destinations.duckdb.config import DuckDBDestinationConfig
from dlt_saga.destinations.duckdb.destination import DuckDBDestination
from dlt_saga.historize.config import HistorizeConfig
from dlt_saga.historize.runner import HistorizeRunner

SCHEMA = "test_scd1_del"
SOURCE_TABLE = "accounts"
TARGET_TABLE = "accounts_historized"


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
            tenant VARCHAR,
            id INTEGER,
            plan VARCHAR,
            snapshot_date TIMESTAMP
        )
        """,
        SCHEMA,
    )
    yield destination
    destination.close()


def _insert(dest, day, rows, tenant="A"):
    """Insert one snapshot: ``rows`` is a list of ``(id, plan)``."""
    values = ", ".join(
        f"('{tenant}', {i}, '{plan}', TIMESTAMP '{day}')" for i, plan in rows
    )
    dest.execute_sql(f'INSERT INTO "{SCHEMA}"."{SOURCE_TABLE}" VALUES {values}', SCHEMA)


def _run(
    dest, *, full_refresh=False, partial_refresh=False, historize_from=None, **historize
):
    settings = {
        "strategy": "scd1",
        "track_deletions": True,
        "primary_key": ["tenant", "id"],
        "snapshot_column": "snapshot_date",
        **historize,
    }
    stats = HistorizeRunner(
        pipeline_name="test__accounts",
        historize_config=HistorizeConfig(**settings),
        destination=dest,
        database="test",
        schema=SCHEMA,
        source_table_name=SOURCE_TABLE,
        target_table_name=TARGET_TABLE,
        config_dict={"write_disposition": "historize"},
        full_refresh=full_refresh,
        partial_refresh=partial_refresh,
        historize_from=historize_from,
    ).run()
    assert stats["status"] == "completed", stats.get("error")
    return stats


def _target(dest):
    result = dest.connection.execute(
        f"""
        SELECT tenant, id, plan, CAST(_dlt_valid_from AS VARCHAR) AS valid_from,
               _dlt_is_deleted AS deleted
        FROM "{SCHEMA}"."{TARGET_TABLE}"
        ORDER BY tenant, id
        """
    )
    return {
        (t, i): (plan, vf, deleted) for t, i, plan, vf, deleted in result.fetchall()
    }


def _columns(dest):
    result = dest.connection.execute(
        f'SELECT * FROM "{SCHEMA}"."{TARGET_TABLE}" LIMIT 0'
    )
    return [c[0] for c in result.description]


class TestScd1DeletionsFullReprocess:
    def test_missing_key_is_flagged_with_last_seen_values(self, dest):
        _insert(dest, "2026-01-01", [(1, "free"), (2, "pro")])
        _insert(dest, "2026-01-02", [(1, "free")])

        stats = _run(dest, full_refresh=True)

        assert _target(dest) == {
            ("A", 1): ("free", "2026-01-01 00:00:00", False),
            ("A", 2): ("pro", "2026-01-02 00:00:00", True),
        }
        assert stats["new_or_changed_rows"] == 1
        assert stats["deleted_rows"] == 1
        assert "_dlt_is_deleted" in _columns(dest)
        assert "_dlt_valid_to" not in _columns(dest)

    def test_reappearance_clears_the_flag(self, dest):
        _insert(dest, "2026-01-01", [(1, "free"), (2, "pro")])
        _insert(dest, "2026-01-02", [(1, "free")])
        _insert(dest, "2026-01-03", [(1, "free"), (2, "pro")])

        _run(dest, full_refresh=True)

        assert _target(dest)[("A", 2)] == ("pro", "2026-01-03 00:00:00", False)


class TestScd1DeletionsIncremental:
    def test_key_missing_from_new_snapshot_is_deleted(self, dest):
        _insert(dest, "2026-01-01", [(1, "free"), (2, "pro")])
        _run(dest, full_refresh=True)

        _insert(dest, "2026-01-02", [(1, "free")])
        stats = _run(dest)

        assert stats["new_or_changed_rows"] == 0
        assert stats["deleted_rows"] == 1
        assert _target(dest) == {
            ("A", 1): ("free", "2026-01-01 00:00:00", False),
            ("A", 2): ("pro", "2026-01-02 00:00:00", True),
        }

    def test_staying_deleted_writes_nothing(self, dest):
        _insert(dest, "2026-01-01", [(1, "free"), (2, "pro")])
        _run(dest, full_refresh=True)
        _insert(dest, "2026-01-02", [(1, "free")])
        _run(dest)
        before = _target(dest)

        _insert(dest, "2026-01-03", [(1, "free")])
        stats = _run(dest)

        assert (stats["new_or_changed_rows"], stats["deleted_rows"]) == (0, 0)
        assert _target(dest) == before

    def test_reappearance_in_a_later_run_clears_the_flag(self, dest):
        _insert(dest, "2026-01-01", [(1, "free"), (2, "pro")])
        _run(dest, full_refresh=True)
        _insert(dest, "2026-01-02", [(1, "free")])
        _run(dest)

        _insert(dest, "2026-01-03", [(1, "free"), (2, "pro")])
        stats = _run(dest)

        assert stats["new_or_changed_rows"] == 1
        assert _target(dest)[("A", 2)] == ("pro", "2026-01-03 00:00:00", False)

    def test_deleted_and_back_within_one_batch_stays_current(self, dest):
        _insert(dest, "2026-01-01", [(1, "free"), (2, "pro")])
        _run(dest, full_refresh=True)

        _insert(dest, "2026-01-02", [(1, "free")])
        _insert(dest, "2026-01-03", [(1, "free"), (2, "pro")])
        _run(dest)

        assert _target(dest)[("A", 2)] == ("pro", "2026-01-03 00:00:00", False)

    def test_partial_refresh_converges(self, dest):
        _insert(dest, "2026-01-01", [(1, "free"), (2, "pro")])
        _insert(dest, "2026-01-02", [(1, "free")])
        _run(dest, full_refresh=True)
        before = _target(dest)

        stats = _run(dest, partial_refresh=True)

        assert (stats["new_or_changed_rows"], stats["deleted_rows"]) == (0, 0)
        assert _target(dest) == before

    def test_key_gone_from_a_refreshed_window_is_deleted(self, dest):
        _insert(dest, "2026-01-01", [(1, "free"), (2, "free")])
        _insert(dest, "2026-01-02", [(1, "free"), (2, "free")])
        _insert(dest, "2026-01-03", [(1, "free"), (2, "pro")])
        _run(dest, full_refresh=True)
        assert _target(dest)[("A", 2)] == ("pro", "2026-01-03 00:00:00", False)

        # The source is corrected: key 2 was gone from 2026-01-02 on, so the
        # stored row is dated after the disappearance the refresh finds.
        dest.execute_sql(
            f"""DELETE FROM "{SCHEMA}"."{SOURCE_TABLE}"
            WHERE id = 2 AND snapshot_date >= TIMESTAMP '2026-01-02'""",
            SCHEMA,
        )
        stats = _run(dest, historize_from="2026-01-02")

        # Last seen before the window as far as the target knows: its stored values.
        assert stats["deleted_rows"] == 1
        assert _target(dest)[("A", 2)] == ("pro", "2026-01-02 00:00:00", True)


class TestScd1DeletionsOrderByTiebreaker:
    """``order_by`` past the snapshot column ranks rows within one snapshot."""

    @pytest.fixture
    def versioned(self, dest):
        dest.execute_sql(
            f'ALTER TABLE "{SCHEMA}"."{SOURCE_TABLE}" ADD COLUMN version INTEGER',
            SCHEMA,
        )
        return dest

    @staticmethod
    def _insert_versioned(dest, day, rows):
        """``rows`` is a list of ``(id, plan, version)``."""
        values = ", ".join(
            f"('A', {i}, '{plan}', TIMESTAMP '{day}', {v})" for i, plan, v in rows
        )
        dest.execute_sql(
            f'INSERT INTO "{SCHEMA}"."{SOURCE_TABLE}" VALUES {values}', SCHEMA
        )

    def test_highest_version_within_a_snapshot_wins_and_is_stable(self, versioned):
        order_by = ["snapshot_date", "version"]
        self._insert_versioned(
            versioned, "2026-01-01", [(1, "pro", 2), (1, "free", 1), (2, "pro", 1)]
        )
        _run(versioned, full_refresh=True, order_by=order_by)
        assert _target(versioned)[("A", 1)] == ("pro", "2026-01-01 00:00:00", False)

        # Same duplicates again, key 2 gone: only the deletion is written.
        self._insert_versioned(versioned, "2026-01-02", [(1, "pro", 2), (1, "free", 1)])
        stats = _run(versioned, order_by=order_by)

        assert (stats["new_or_changed_rows"], stats["deleted_rows"]) == (0, 1)
        assert _target(versioned) == {
            ("A", 1): ("pro", "2026-01-01 00:00:00", False),
            ("A", 2): ("pro", "2026-01-02 00:00:00", True),
        }


class TestScd1DeletionsMergeKey:
    def test_snapshot_of_one_tenant_doesnt_delete_another(self, dest):
        _insert(dest, "2026-01-01", [(1, "free")], tenant="A")
        _insert(dest, "2026-01-01", [(1, "pro")], tenant="B")
        _run(dest, full_refresh=True, merge_key=["tenant"])

        _insert(dest, "2026-01-02", [(1, "free")], tenant="A")
        stats = _run(dest, merge_key=["tenant"])

        assert stats["deleted_rows"] == 0
        assert _target(dest)[("B", 1)] == ("pro", "2026-01-01 00:00:00", False)

    def test_without_merge_key_the_other_tenant_is_deleted(self, dest):
        _insert(dest, "2026-01-01", [(1, "free")], tenant="A")
        _insert(dest, "2026-01-01", [(1, "pro")], tenant="B")
        _run(dest, full_refresh=True)

        _insert(dest, "2026-01-02", [(1, "free")], tenant="A")
        _run(dest)

        assert _target(dest)[("B", 1)] == ("pro", "2026-01-02 00:00:00", True)
