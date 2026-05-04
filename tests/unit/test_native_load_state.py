"""Unit tests for native_load state manager."""

from unittest.mock import MagicMock

import pytest

from dlt_saga.pipelines.native_load.state import (
    _BULK_INSERT_CHUNK,
    NativeLoadStateManager,
    make_load_id,
)


@pytest.mark.unit
class TestMakeLoadId:
    def test_deterministic(self):
        a = make_load_id("pipeline", "gs://b/f.parquet", 123)
        b = make_load_id("pipeline", "gs://b/f.parquet", 123)
        assert a == b

    def test_generation_sensitive(self):
        a = make_load_id("pipeline", "gs://b/f.parquet", 1)
        b = make_load_id("pipeline", "gs://b/f.parquet", 2)
        assert a != b

    def test_none_vs_zero_generation(self):
        a = make_load_id("pipeline", "gs://b/f.parquet", None)
        b = make_load_id("pipeline", "gs://b/f.parquet", 0)
        assert a != b

    def test_pipeline_name_sensitive(self):
        a = make_load_id("pipeline_a", "gs://b/f.parquet", 1)
        b = make_load_id("pipeline_b", "gs://b/f.parquet", 1)
        assert a != b

    def test_uri_sensitive(self):
        a = make_load_id("pipeline", "gs://b/f1.parquet", 1)
        b = make_load_id("pipeline", "gs://b/f2.parquet", 1)
        assert a != b

    def test_returns_32_chars(self):
        assert len(make_load_id("pipeline", "gs://b/f.parquet", 1)) == 32


@pytest.mark.unit
class TestNativeLoadStateManager:
    def _make_dest(self) -> MagicMock:
        dest = MagicMock()
        dest.type_name.side_effect = lambda t: t.upper()
        dest.partition_ddl.return_value = "PARTITION BY DATE(started_at)"
        dest.cluster_ddl.return_value = ""
        dest.get_full_table_id.side_effect = lambda d, t: f"`proj.{d}.{t}`"
        dest.execute_sql.return_value = []
        dest.create_or_replace_view.return_value = None
        dest.timestamp_n_days_ago.return_value = (
            "TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 DAY)"
        )
        return dest

    def test_ensure_table_exists_calls_execute_sql(self):
        dest = self._make_dest()
        mgr = NativeLoadStateManager(dest, "my_dataset")
        mgr.ensure_table_exists()
        assert dest.execute_sql.called

    def test_ensure_table_exists_creates_view(self):
        dest = self._make_dest()
        mgr = NativeLoadStateManager(dest, "my_dataset")
        mgr.ensure_table_exists()
        dest.create_or_replace_view.assert_called_once()

    def test_ensure_table_view_failure_is_non_fatal(self):
        dest = self._make_dest()
        dest.create_or_replace_view.side_effect = RuntimeError("view creation failed")
        mgr = NativeLoadStateManager(dest, "my_dataset")
        mgr.ensure_table_exists()  # should not raise

    def test_clear_pipeline_state(self):
        dest = self._make_dest()
        mgr = NativeLoadStateManager(dest, "my_dataset")
        mgr.clear_pipeline_state("my_pipeline")
        sql_calls = [c[0][0] for c in dest.execute_sql.call_args_list]
        assert any("DELETE" in s and "my_pipeline" in s for s in sql_calls)

    def test_record_loads_started_bulk_returns_ids_and_timestamp(self):
        dest = self._make_dest()
        mgr = NativeLoadStateManager(dest, "my_dataset")
        rows = [
            ("gs://b/f1.parquet", "20240101", 1),
            ("gs://b/f2.parquet", "20240101", 2),
        ]
        load_ids, started_at = mgr.record_loads_started_bulk("pipeline", rows)
        assert len(load_ids) == 2
        assert started_at  # non-empty ISO string

    def test_bulk_insert_chunks_at_limit(self):
        dest = self._make_dest()
        mgr = NativeLoadStateManager(dest, "my_dataset")
        n = _BULK_INSERT_CHUNK + 5  # one extra chunk
        rows = [(f"gs://b/f{i}.parquet", None, i) for i in range(n)]
        mgr.record_loads_started_bulk("pipeline", rows)
        # ensure_table_exists hasn't been called; count only INSERT calls
        insert_calls = [
            c[0][0] for c in dest.execute_sql.call_args_list if "INSERT INTO" in c[0][0]
        ]
        assert len(insert_calls) == 2  # two chunks

    def test_get_loaded_uri_generations_empty_on_error(self):
        dest = self._make_dest()
        dest.execute_sql.side_effect = RuntimeError("table not found")
        mgr = NativeLoadStateManager(dest, "my_dataset")
        result = mgr.get_loaded_uri_generations("pipeline")
        assert result == set()

    def test_record_loads_success_bulk_inserts_with_status(self):
        dest = self._make_dest()
        mgr = NativeLoadStateManager(dest, "my_dataset")
        rows = [("gs://b/f1.parquet", "20240101", 1)]
        mgr.record_loads_success_bulk(
            pipeline_name="pipeline",
            rows=rows,
            load_ids=[make_load_id("pipeline", "gs://b/f1.parquet", 1)],
            job_id="job123",
            size_bytes_by_load_id={},
            loaded_rows_by_load_id={},
            started_at="2024-01-01T00:00:00+00:00",
        )
        insert_calls = [
            c[0][0] for c in dest.execute_sql.call_args_list if "INSERT INTO" in c[0][0]
        ]
        assert any("'success'" in s for s in insert_calls)

    def test_record_loads_failed_bulk_inserts_with_status(self):
        dest = self._make_dest()
        mgr = NativeLoadStateManager(dest, "my_dataset")
        rows = [("gs://b/f1.parquet", None, 1)]
        mgr.record_loads_failed_bulk(
            pipeline_name="pipeline",
            rows=rows,
            load_ids=[make_load_id("pipeline", "gs://b/f1.parquet", 1)],
            error="connection timeout",
            started_at="2024-01-01T00:00:00+00:00",
        )
        insert_calls = [
            c[0][0] for c in dest.execute_sql.call_args_list if "INSERT INTO" in c[0][0]
        ]
        assert any("'failed'" in s for s in insert_calls)
        assert any("connection timeout" in s for s in insert_calls)
