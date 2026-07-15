"""Integration tests for internal-log compaction against real DuckDB.

The unit tests in tests/unit/test_log_compaction.py mock the destination, so they
assert SQL shape, not behavior. These run the collapse / orphan DELETEs against
in-memory DuckDB to prove they are lossless: the companion view and discovery
reads return the same rows before and after compaction, and only strictly
superseded / dangling rows are removed.
"""

import pytest

from dlt_saga.destinations.duckdb.config import DuckDBDestinationConfig
from dlt_saga.destinations.duckdb.destination import DuckDBDestination
from dlt_saga.maintenance import compact_log, reconcile_stale_tasks
from dlt_saga.pipelines.native_load.state import NativeLoadStateManager
from dlt_saga.utility.internal_tables import resolve_compactable_logs

_DS = "nl_state"


@pytest.fixture
def mgr():
    dest = DuckDBDestination(
        DuckDBDestinationConfig(
            project_id="test",
            location="local",
            schema_name=_DS,
            database_path=":memory:",
        )
    )
    m = NativeLoadStateManager(dest, _DS)
    m.ensure_table_exists()
    yield m
    dest.close()


def _native_spec():
    logs = resolve_compactable_logs([_DS], None)
    return next(log for log in logs if log.label == "native_load log")


def _rowcount(dest, spec):
    table_id = dest.get_full_table_id(spec.schema, spec.table)
    return int(list(dest.execute_sql(f"SELECT COUNT(*) FROM {table_id}", _DS))[0][0])


@pytest.mark.integration
class TestNativeLoadCollapse:
    def test_started_then_success_collapses_started_losslessly(self, mgr):
        dest = mgr._dest
        spec = _native_spec()
        rows = [("gs://b/f1.parquet", "20240101", 1)]
        ids, started = mgr.record_loads_started_bulk("p", rows)
        mgr.record_loads_success_bulk("p", rows, ids, "job", {}, {}, started)

        loaded_before = mgr.get_loaded_uri_generations("p")
        result = compact_log(dest, spec, dry_run=False)

        assert result == {"collapsed": 1, "orphaned": 0}
        assert _rowcount(dest, spec) == 1  # only the success row remains
        # Lossless: the file is still seen as loaded (view/discovery unchanged).
        assert mgr.get_loaded_uri_generations("p") == loaded_before

    def test_is_idempotent(self, mgr):
        dest = mgr._dest
        spec = _native_spec()
        rows = [("gs://b/f1.parquet", "20240101", 1)]
        ids, started = mgr.record_loads_started_bulk("p", rows)
        mgr.record_loads_success_bulk("p", rows, ids, "job", {}, {}, started)

        compact_log(dest, spec, dry_run=False)
        second = compact_log(dest, spec, dry_run=False)
        assert second == {"collapsed": 0, "orphaned": 0}

    def test_dry_run_deletes_nothing(self, mgr):
        dest = mgr._dest
        spec = _native_spec()
        rows = [("gs://b/f1.parquet", "20240101", 1)]
        ids, started = mgr.record_loads_started_bulk("p", rows)
        mgr.record_loads_success_bulk("p", rows, ids, "job", {}, {}, started)

        result = compact_log(dest, spec, dry_run=True)
        assert result == {"collapsed": 1, "orphaned": 0}
        assert _rowcount(dest, spec) == 2  # nothing removed

    def test_terminal_failed_row_is_kept(self, mgr):
        # started + failed: the started collapses, the terminal failed row stays,
        # and the file remains re-loadable (not shadowed as loaded/in-progress).
        dest = mgr._dest
        spec = _native_spec()
        rows = [("gs://b/f1.parquet", "20240101", 1)]
        ids, started = mgr.record_loads_started_bulk("p", rows)
        mgr.record_loads_failed_bulk("p", rows, ids, error="boom", started_at=started)

        result = compact_log(dest, spec, dry_run=False)
        assert result == {"collapsed": 1, "orphaned": 0}
        assert _rowcount(dest, spec) == 1
        assert ("gs://b/f1.parquet", 1) not in mgr.get_loaded_uri_generations("p")

    def test_latest_started_of_distinct_keys_untouched(self, mgr):
        # Two in-progress files (each a lone latest 'started', recent): neither is
        # superseded and neither is a stale orphan, so both survive.
        dest = mgr._dest
        spec = _native_spec()
        mgr.record_loads_started_bulk(
            "p",
            [("gs://b/a.parquet", "20240101", 1), ("gs://b/b.parquet", "20240101", 1)],
        )
        result = compact_log(dest, spec, dry_run=False)
        assert result == {"collapsed": 0, "orphaned": 0}
        assert _rowcount(dest, spec) == 2


@pytest.mark.integration
class TestNativeLoadOrphanCleanup:
    def _insert_started(self, dest, spec, uri, started_at_iso):
        table_id = dest.get_full_table_id(spec.schema, spec.table)
        dest.execute_sql(
            f"INSERT INTO {table_id} "
            f"(load_id, pipeline_name, source_uri, generation, status, started_at) "
            f"VALUES ('id-{uri}', 'p', '{uri}', 1, 'started', "
            f"TIMESTAMP '{started_at_iso}')",
            _DS,
        )

    def test_stale_dangling_started_is_removed(self, mgr):
        dest = mgr._dest
        spec = _native_spec()
        self._insert_started(dest, spec, "gs://b/orphan.parquet", "2000-01-01 00:00:00")

        result = compact_log(dest, spec, dry_run=False)
        assert result == {"collapsed": 0, "orphaned": 1}
        assert _rowcount(dest, spec) == 0

    def test_recent_dangling_started_is_kept(self, mgr):
        # A fresh lone 'started' is an in-progress run, not an orphan.
        dest = mgr._dest
        spec = _native_spec()
        mgr.record_loads_started_bulk("p", [("gs://b/inflight.parquet", "20240101", 1)])

        result = compact_log(dest, spec, dry_run=False)
        assert result == {"collapsed": 0, "orphaned": 0}
        assert _rowcount(dest, spec) == 1

    def test_stale_started_with_terminal_is_not_an_orphan(self, mgr):
        # A key that eventually succeeded is not a dangling orphan even if old:
        # the collapse removes the superseded started, the success is kept, and
        # the orphan pass leaves nothing behind.
        dest = mgr._dest
        spec = _native_spec()
        table_id = dest.get_full_table_id(spec.schema, spec.table)
        dest.execute_sql(
            f"INSERT INTO {table_id} "
            f"(load_id, pipeline_name, source_uri, generation, status, "
            f"started_at, finished_at) VALUES "
            f"('i1','p','gs://b/done.parquet',1,'started',"
            f"TIMESTAMP '2000-01-01 00:00:00', NULL),"
            f"('i1','p','gs://b/done.parquet',1,'success',"
            f"TIMESTAMP '2000-01-01 00:00:00', TIMESTAMP '2000-01-01 00:00:05')",
            _DS,
        )
        result = compact_log(dest, spec, dry_run=False)
        assert result == {"collapsed": 1, "orphaned": 0}
        assert ("gs://b/done.parquet", 1) in mgr.get_loaded_uri_generations("p")


@pytest.mark.integration
class TestExecutionPlanCollapse:
    """The generic collapse on the execution-plan spec (different key + ordering,
    no orphan rule), exercised on a real DuckDB table."""

    def _spec(self):
        logs = resolve_compactable_logs([], "orch")
        return next(log for log in logs if log.label == "execution-plan log")

    def _dest(self):
        return DuckDBDestination(
            DuckDBDestinationConfig(
                project_id="test",
                location="local",
                schema_name="orch",
                database_path=":memory:",
            )
        )

    def test_pending_running_collapse_to_completed(self):
        dest = self._dest()
        try:
            spec = self._spec()
            dest.execute_sql("CREATE SCHEMA IF NOT EXISTS orch")
            table_id = dest.get_full_table_id(spec.schema, spec.table)
            dest.execute_sql(
                f"CREATE TABLE {table_id} ("
                f"execution_id VARCHAR, task_index INTEGER, "
                f"pipeline_identifier VARCHAR, status VARCHAR, "
                f"log_timestamp TIMESTAMP)",
                "orch",
            )
            dest.execute_sql(
                f"INSERT INTO {table_id} VALUES "
                # key e1/0/api__a: pending -> running -> completed
                f"('e1',0,'api__a','pending', TIMESTAMP '2024-01-01 00:00:00'),"
                f"('e1',0,'api__a','running', TIMESTAMP '2024-01-01 00:00:01'),"
                f"('e1',0,'api__a','completed', TIMESTAMP '2024-01-01 00:00:02'),"
                # key e1/1/api__b: single terminal row (untouched)
                f"('e1',1,'api__b','completed', TIMESTAMP '2024-01-01 00:00:02')",
                "orch",
            )
            result = compact_log(dest, spec, dry_run=False)
            assert result == {"collapsed": 2, "orphaned": 0}
            remaining = list(
                dest.execute_sql(
                    f"SELECT status FROM {table_id} ORDER BY task_index, status",
                    "orch",
                )
            )
            statuses = sorted(r[0] for r in remaining)
            assert statuses == ["completed", "completed"]
        finally:
            dest.close()


@pytest.mark.integration
class TestExecutionPlanReconcile:
    """Stale-task reconcile: dangling running/pending → abandoned, on real DuckDB."""

    def _spec(self):
        logs = resolve_compactable_logs([], "orch")
        return next(log for log in logs if log.label == "execution-plan log")

    def _dest(self):
        return DuckDBDestination(
            DuckDBDestinationConfig(
                project_id="test",
                location="local",
                schema_name="orch",
                database_path=":memory:",
            )
        )

    def test_dangling_tasks_abandoned_recent_and_terminal_untouched(self):
        dest = self._dest()
        try:
            spec = self._spec()
            dest.execute_sql("CREATE SCHEMA IF NOT EXISTS orch")
            table_id = dest.get_full_table_id(spec.schema, spec.table)
            dest.execute_sql(
                f"CREATE TABLE {table_id} ("
                f"execution_id VARCHAR, task_index INTEGER, "
                f"pipeline_identifier VARCHAR, status VARCHAR, "
                f"log_timestamp TIMESTAMP, error_message VARCHAR)",
                "orch",
            )
            dest.execute_sql(
                f"INSERT INTO {table_id} VALUES "
                # task 0: stale pending -> running, dangling (crashed)
                f"('e1',0,'a','pending', TIMESTAMP '2024-01-01 00:00:00', NULL),"
                f"('e1',0,'a','running', TIMESTAMP '2024-01-01 00:00:01', NULL),"
                # task 1: stale lone pending (never started)
                f"('e1',1,'b','pending', TIMESTAMP '2024-01-01 00:00:00', NULL),"
                # task 2: recent running (in-flight) -> must be left alone
                f"('e1',2,'c','running', now(), NULL),"
                # task 3: stale completed -> terminal, untouched
                f"('e1',3,'d','completed', TIMESTAMP '2024-01-01 00:00:02', NULL)",
                "orch",
            )

            n = reconcile_stale_tasks(dest, spec, dry_run=False)
            assert n == 2  # task 0's running + task 1's pending

            # The dangling running/pending predecessors are still present until
            # compaction; check the *latest* row per task via the view semantics.
            latest = list(
                dest.execute_sql(
                    f"SELECT task_index, status, error_message FROM {table_id} "
                    f"QUALIFY ROW_NUMBER() OVER ("
                    f"  PARTITION BY execution_id, task_index, pipeline_identifier "
                    f"  ORDER BY log_timestamp DESC) = 1 "
                    f"ORDER BY task_index",
                    "orch",
                )
            )
            by_task = {r[0]: (r[1], r[2]) for r in latest}
            assert by_task[0][0] == "abandoned"
            assert "presumed crashed" in by_task[0][1]
            assert by_task[1][0] == "abandoned"
            assert "never started" in by_task[1][1]
            assert by_task[2][0] == "running"  # recent, in-flight — untouched
            assert by_task[3][0] == "completed"  # terminal — untouched

            # Idempotent: a second pass abandons nothing new.
            assert reconcile_stale_tasks(dest, spec, dry_run=False) == 0

            # And compaction then collapses task 0's now-superseded pending.
            collapsed = compact_log(dest, spec, dry_run=False)["collapsed"]
            assert collapsed == 1
        finally:
            dest.close()
