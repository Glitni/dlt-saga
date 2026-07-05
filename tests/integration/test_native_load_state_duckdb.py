"""Integration tests for native_load state semantics against real DuckDB.

The unit tests in tests/unit/test_native_load_state.py mock execute_sql, so they
can't exercise the actual SQL. These run the queries against in-memory DuckDB to
validate latest-event-wins dedup — notably that a trailing 'failed' row re-enables
a file for retry rather than being shadowed by its own 'started' row.
"""

import pytest

from dlt_saga.destinations.duckdb.config import DuckDBDestinationConfig
from dlt_saga.destinations.duckdb.destination import DuckDBDestination
from dlt_saga.pipelines.native_load.state import NativeLoadStateManager

_DS = "nl_state"


@pytest.fixture
def state_manager():
    dest = DuckDBDestination(
        DuckDBDestinationConfig(
            project_id="test",
            location="local",
            schema_name=_DS,
            database_path=":memory:",
        )
    )
    mgr = NativeLoadStateManager(dest, _DS)
    mgr.ensure_table_exists()
    yield mgr
    dest.close()


@pytest.mark.integration
class TestFailedFileShadowing:
    def test_trailing_failed_row_re_enables_file(self, state_manager):
        # A failed chunk writes both a 'started' and a later 'failed' row for the
        # same file. The file must NOT be treated as loaded/in-progress — a retry
        # has to pick it back up.
        uri = "gs://b/f1.parquet"
        rows = [(uri, "20240101", 1)]
        load_ids, started = state_manager.record_loads_started_bulk("p", rows)
        state_manager.record_loads_failed_bulk(
            "p", rows, load_ids, error="boom", started_at=started
        )
        assert (uri, 1) not in state_manager.get_loaded_uri_generations("p")

    def test_success_row_marks_file_loaded(self, state_manager):
        uri = "gs://b/ok.parquet"
        rows = [(uri, "20240101", 1)]
        load_ids, started = state_manager.record_loads_started_bulk("p", rows)
        state_manager.record_loads_success_bulk(
            "p", rows, load_ids, "job", {}, {}, started
        )
        assert (uri, 1) in state_manager.get_loaded_uri_generations("p")

    def test_fresh_started_row_is_in_progress(self, state_manager):
        # A lone fresh 'started' row (no terminal event yet) is assumed to be a
        # concurrent run and is not re-attempted.
        uri = "gs://b/inflight.parquet"
        state_manager.record_loads_started_bulk("p", [(uri, "20240101", 1)])
        assert (uri, 1) in state_manager.get_loaded_uri_generations("p")

    def test_success_then_new_generation_failure_only_reloads_new_gen(
        self, state_manager
    ):
        # gen 1 succeeded; the file was overwritten (gen 2) and that load failed.
        # Only gen 2 should be re-enabled — gen 1 stays loaded.
        uri = "gs://b/versioned.parquet"
        g1 = [(uri, "20240101", 1)]
        ids1, s1 = state_manager.record_loads_started_bulk("p", g1)
        state_manager.record_loads_success_bulk("p", g1, ids1, "job", {}, {}, s1)
        g2 = [(uri, "20240102", 2)]
        ids2, s2 = state_manager.record_loads_started_bulk("p", g2)
        state_manager.record_loads_failed_bulk(
            "p", g2, ids2, error="boom", started_at=s2
        )

        loaded = state_manager.get_loaded_uri_generations("p")
        assert (uri, 1) in loaded
        assert (uri, 2) not in loaded
