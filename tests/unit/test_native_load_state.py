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


@pytest.mark.unit
class TestHelperFunctions:
    def test_ts_literal_formats_timestamp(self):
        from dlt_saga.pipelines.native_load.state import _ts_literal

        result = _ts_literal("2024-01-15T10:30:00+00:00")
        assert result == "TIMESTAMP '2024-01-15T10:30:00+00:00'"

    def test_ts_literal_escapes_single_quotes(self):
        from dlt_saga.pipelines.native_load.state import _ts_literal

        result = _ts_literal("2024-01-15T00:00:00")
        assert result.startswith("TIMESTAMP '")
        assert result.endswith("'")

    def test_str_or_null_none(self):
        from dlt_saga.pipelines.native_load.state import _str_or_null

        assert _str_or_null(None) == "NULL"

    def test_str_or_null_value(self):
        from dlt_saga.pipelines.native_load.state import _str_or_null

        assert _str_or_null("hello") == "'hello'"

    def test_str_or_null_escapes_single_quote(self):
        from dlt_saga.pipelines.native_load.state import _str_or_null

        result = _str_or_null("it's")
        assert "''" in result

    def test_int_or_null_none(self):
        from dlt_saga.pipelines.native_load.state import _int_or_null

        assert _int_or_null(None) == "NULL"

    def test_int_or_null_value(self):
        from dlt_saga.pipelines.native_load.state import _int_or_null

        assert _int_or_null(42) == "42"

    def test_int_or_null_zero(self):
        from dlt_saga.pipelines.native_load.state import _int_or_null

        assert _int_or_null(0) == "0"


def _make_plain_dest() -> MagicMock:
    """Minimal destination mock without a partition clause."""
    dest = MagicMock()
    dest.type_name.side_effect = lambda t: t.upper()
    dest.partition_ddl.return_value = ""
    dest.cluster_ddl.return_value = ""
    dest.get_full_table_id.side_effect = lambda d, t: f"`proj.{d}.{t}`"
    dest.execute_sql.return_value = []
    dest.create_or_replace_view.return_value = None
    dest.timestamp_n_days_ago.return_value = "CURRENT_TIMESTAMP() - INTERVAL '1' DAY"
    return dest


@pytest.mark.unit
class TestEnsureTableExistsNoBatchPartition:
    def test_no_partition_clause_skipped(self):
        dest = _make_plain_dest()
        mgr = NativeLoadStateManager(dest, "my_dataset")
        mgr.ensure_table_exists()
        ddl_calls = [c[0][0] for c in dest.execute_sql.call_args_list]
        assert any("CREATE TABLE IF NOT EXISTS" in s for s in ddl_calls)
        # Partition clause should NOT be appended when empty
        assert not any("PARTITION BY" in s for s in ddl_calls)


@pytest.mark.unit
class TestGetLastCursor:
    def test_returns_none_on_empty_result(self):
        dest = _make_plain_dest()
        dest.execute_sql.return_value = []
        mgr = NativeLoadStateManager(dest, "ds")
        assert mgr.get_last_cursor("pipeline") is None

    def test_returns_value_from_tuple_row(self):
        dest = _make_plain_dest()
        dest.execute_sql.return_value = [("2024-01-15",)]
        mgr = NativeLoadStateManager(dest, "ds")
        assert mgr.get_last_cursor("pipeline") == "2024-01-15"

    def test_returns_none_when_cursor_value_is_none(self):
        dest = _make_plain_dest()
        dest.execute_sql.return_value = [(None,)]
        mgr = NativeLoadStateManager(dest, "ds")
        assert mgr.get_last_cursor("pipeline") is None

    def test_returns_value_from_attribute_row(self):
        dest = _make_plain_dest()

        class _AttrRow:
            cursor_value = "2024-02-01"

        dest.execute_sql.return_value = [_AttrRow()]
        mgr = NativeLoadStateManager(dest, "ds")
        assert mgr.get_last_cursor("pipeline") == "2024-02-01"

    def test_returns_none_on_exception(self):
        dest = _make_plain_dest()
        dest.execute_sql.side_effect = RuntimeError("table not found")
        mgr = NativeLoadStateManager(dest, "ds")
        assert mgr.get_last_cursor("pipeline") is None


@pytest.mark.unit
class TestGetLoadedUriGenerations:
    def test_returns_empty_set_on_no_rows(self):
        dest = _make_plain_dest()
        mgr = NativeLoadStateManager(dest, "ds")
        assert mgr.get_loaded_uri_generations("pipeline") == set()

    def test_returns_pairs_from_tuple_rows(self):
        dest = _make_plain_dest()
        dest.execute_sql.return_value = [
            ("gs://b/f1.parquet", 1),
            ("gs://b/f2.parquet", None),
        ]
        mgr = NativeLoadStateManager(dest, "ds")
        result = mgr.get_loaded_uri_generations("pipeline")
        assert ("gs://b/f1.parquet", 1) in result
        assert ("gs://b/f2.parquet", None) in result

    def test_returns_pairs_from_attribute_rows(self):
        dest = _make_plain_dest()

        class _AttrRow:
            def __init__(self, uri, gen):
                self.source_uri = uri
                self.generation = gen

        dest.execute_sql.return_value = [_AttrRow("gs://b/f1.parquet", 2)]
        mgr = NativeLoadStateManager(dest, "ds")
        result = mgr.get_loaded_uri_generations("pipeline")
        assert ("gs://b/f1.parquet", 2) in result

    def test_cursor_min_included_in_sql(self):
        dest = _make_plain_dest()
        mgr = NativeLoadStateManager(dest, "ds")
        mgr.get_loaded_uri_generations("pipeline", cursor_min="2024-01-01")
        sql = dest.execute_sql.call_args[0][0]
        assert "cursor_value" in sql
        assert "2024-01-01" in sql

    def test_no_cursor_filter_without_cursor_min(self):
        dest = _make_plain_dest()
        mgr = NativeLoadStateManager(dest, "ds")
        mgr.get_loaded_uri_generations("pipeline")
        sql = dest.execute_sql.call_args[0][0]
        assert "cursor_value" not in sql

    def test_returns_empty_on_exception(self):
        dest = _make_plain_dest()
        dest.execute_sql.side_effect = RuntimeError("table gone")
        mgr = NativeLoadStateManager(dest, "ds")
        assert mgr.get_loaded_uri_generations("pipeline") == set()


@pytest.mark.unit
class TestGetLoadedUris:
    def test_returns_uri_strings_without_generation(self):
        dest = _make_plain_dest()
        dest.execute_sql.return_value = [
            ("gs://b/f1.parquet", 1),
            ("gs://b/f2.parquet", 2),
        ]
        mgr = NativeLoadStateManager(dest, "ds")
        result = mgr.get_loaded_uris("pipeline")
        assert result == {"gs://b/f1.parquet", "gs://b/f2.parquet"}

    def test_passes_cursor_min_through(self):
        dest = _make_plain_dest()
        dest.execute_sql.return_value = []
        mgr = NativeLoadStateManager(dest, "ds")
        mgr.get_loaded_uris("pipeline", cursor_min="2024-01-01")
        sql = dest.execute_sql.call_args[0][0]
        assert "2024-01-01" in sql


@pytest.mark.unit
class TestRecordLoadStart:
    def test_returns_load_id_and_started_at(self):
        dest = _make_plain_dest()
        mgr = NativeLoadStateManager(dest, "ds")
        load_id, started_at = mgr.record_load_start(
            "pipeline", "gs://b/f1.parquet", "2024-01-01", 1
        )
        assert len(load_id) == 32
        assert started_at

    def test_inserts_started_row(self):
        dest = _make_plain_dest()
        mgr = NativeLoadStateManager(dest, "ds")
        mgr.record_load_start("pipeline", "gs://b/f1.parquet", None, None)
        insert_calls = [
            c[0][0] for c in dest.execute_sql.call_args_list if "INSERT INTO" in c[0][0]
        ]
        assert len(insert_calls) >= 1

    def test_load_id_matches_bulk_helper(self):
        dest = _make_plain_dest()
        mgr = NativeLoadStateManager(dest, "ds")
        load_id, _ = mgr.record_load_start("pipeline", "gs://b/f1.parquet", None, 5)
        expected = make_load_id("pipeline", "gs://b/f1.parquet", 5)
        assert load_id == expected
