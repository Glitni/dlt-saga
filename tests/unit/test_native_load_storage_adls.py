"""Unit tests for AdlsStorageClient."""

import datetime
from unittest.mock import MagicMock

import pytest

from dlt_saga.pipelines.native_load.storage.adls import (
    AdlsStorageClient,
    _mtime_to_generation,
    _pattern_to_sql_like,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_client(rows=None):
    dest = MagicMock()
    dest.__class__.__name__ = "DatabricksDestination"
    dest.execute_sql.return_value = rows or []
    return AdlsStorageClient(dest)


def _row(path, size=1024, mtime=1_000_000_000_000):
    return (path, size, mtime)


# ---------------------------------------------------------------------------
# _pattern_to_sql_like
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestPatternToSqlLike:
    def test_star_extension(self):
        assert _pattern_to_sql_like("*.parquet") == "%.parquet"

    def test_prefix_wildcard(self):
        # The literal _ in "data_" is a SQL wildcard and must be escaped
        assert _pattern_to_sql_like("data_*.csv") == r"data\_%.csv"

    def test_question_mark(self):
        assert _pattern_to_sql_like("file?.parquet") == "file_.parquet"

    def test_escapes_percent(self):
        assert _pattern_to_sql_like("100%.parquet") == r"100\%.parquet"

    def test_escapes_underscore(self):
        assert _pattern_to_sql_like("my_table.parquet") == r"my\_table.parquet"


# ---------------------------------------------------------------------------
# _mtime_to_generation
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestMtimeToGeneration:
    def test_none_returns_zero(self):
        assert _mtime_to_generation(None) == 0

    def test_int_returned_as_is(self):
        assert _mtime_to_generation(1_700_000_000_000) == 1_700_000_000_000

    def test_datetime_converted_to_epoch_millis(self):
        dt = datetime.datetime(2024, 1, 1, tzinfo=datetime.timezone.utc)
        gen = _mtime_to_generation(dt)
        assert gen == int(dt.timestamp() * 1000)

    def test_different_datetimes_give_different_generations(self):
        dt1 = datetime.datetime(2024, 1, 1, tzinfo=datetime.timezone.utc)
        dt2 = datetime.datetime(2024, 1, 2, tzinfo=datetime.timezone.utc)
        assert _mtime_to_generation(dt1) != _mtime_to_generation(dt2)


# ---------------------------------------------------------------------------
# AdlsStorageClient.list_files
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestAdlsListFiles:
    def test_yields_matching_rows(self):
        uri = "abfss://lake@account.dfs.core.windows.net/raw/"
        rows = [
            _row("abfss://lake@account.dfs.core.windows.net/raw/file1.parquet", 1000),
            _row("abfss://lake@account.dfs.core.windows.net/raw/file2.parquet", 2000),
        ]
        client = _make_client(rows)
        result = list(client.list_files(uri, "*.parquet"))
        assert len(result) == 2
        assert (
            result[0].full_uri
            == "abfss://lake@account.dfs.core.windows.net/raw/file1.parquet"
        )
        assert result[1].size == 2000

    def test_filters_non_matching_basename(self):
        uri = "abfss://lake@account.dfs.core.windows.net/raw/"
        rows = [
            _row("abfss://lake@account.dfs.core.windows.net/raw/file1.parquet"),
            _row("abfss://lake@account.dfs.core.windows.net/raw/file2.csv"),
        ]
        client = _make_client(rows)
        result = list(client.list_files(uri, "*.parquet"))
        assert len(result) == 1
        assert result[0].full_uri.endswith(".parquet")

    def test_list_pattern_accepted(self):
        uri = "abfss://lake@account.dfs.core.windows.net/raw/"
        rows = [
            _row("abfss://lake@account.dfs.core.windows.net/raw/file1.parquet"),
            _row("abfss://lake@account.dfs.core.windows.net/raw/file2.csv"),
            _row("abfss://lake@account.dfs.core.windows.net/raw/file3.jsonl"),
        ]
        client = _make_client(rows)
        result = list(client.list_files(uri, ["*.parquet", "*.csv"]))
        assert len(result) == 2

    def test_relative_path_prefixed_with_uri(self):
        uri = "abfss://lake@account.dfs.core.windows.net/raw/"
        rows = [
            _row("subdir/file1.parquet", 100),  # relative path
        ]
        client = _make_client(rows)
        result = list(client.list_files(uri, "*.parquet"))
        assert len(result) == 1
        assert (
            result[0].full_uri
            == "abfss://lake@account.dfs.core.windows.net/raw/subdir/file1.parquet"
        )

    def test_start_offset_ignored(self):
        uri = "abfss://lake@account.dfs.core.windows.net/raw/"
        rows = [_row("abfss://lake@account.dfs.core.windows.net/raw/file1.parquet")]
        client = _make_client(rows)
        # start_offset provided but ADLS ignores it — should still return results
        result = list(client.list_files(uri, "*.parquet", start_offset="some_offset"))
        assert len(result) == 1

    def test_sql_contains_uri(self):
        uri = "abfss://lake@account.dfs.core.windows.net/raw/"
        client = _make_client()
        list(client.list_files(uri, "*.parquet"))
        call_sql = client._destination.execute_sql.call_args[0][0]
        assert "abfss://lake@account.dfs.core.windows.net/raw/" in call_sql
        assert "LIST" in call_sql
        assert "RECURSIVE" in call_sql

    def test_non_abfss_uri_raises(self):
        client = _make_client()
        with pytest.raises(ValueError, match="abfss"):
            list(client.list_files("gs://bucket/prefix/", "*.parquet"))

    def test_sql_error_wrapped_with_guidance(self):
        client = _make_client()
        client._destination.execute_sql.side_effect = RuntimeError("permission denied")
        with pytest.raises(RuntimeError, match="external location"):
            list(
                client.list_files(
                    "abfss://lake@account.dfs.core.windows.net/raw/", "*.parquet"
                )
            )

    def test_empty_path_rows_skipped(self):
        uri = "abfss://lake@account.dfs.core.windows.net/raw/"
        rows = [
            (None, 0, None),
            ("", 0, None),
            _row("abfss://lake@account.dfs.core.windows.net/raw/file1.parquet"),
        ]
        client = _make_client(rows)
        result = list(client.list_files(uri, "*.parquet"))
        assert len(result) == 1

    def test_generation_from_int_mtime(self):
        uri = "abfss://lake@account.dfs.core.windows.net/raw/"
        rows = [
            _row(
                "abfss://lake@account.dfs.core.windows.net/raw/file1.parquet",
                mtime=9999,
            )
        ]
        client = _make_client(rows)
        result = list(client.list_files(uri, "*.parquet"))
        assert result[0].generation == 9999
