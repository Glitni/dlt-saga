"""Unit tests for saga's DuckDB CSV reader in the filesystem client."""

import io
from unittest.mock import patch

import duckdb
import pytest

from dlt_saga.pipelines.filesystem.client import FilesystemClient, _iter_csv_duckdb
from dlt_saga.pipelines.filesystem.config import FilesystemConfig


class _FakeFileItem(dict):
    """Stand-in for dlt's FileItemDict: dict metadata + a binary open()."""

    def __init__(self, data: bytes, **meta):
        super().__init__(**meta)
        self._data = data

    def open(self):
        return io.BytesIO(self._data)


@pytest.mark.unit
class TestIterCsvDuckdb:
    def _rows(self, data: bytes, chunk_size: int = 5_000, **kwargs):
        with duckdb.connect() as conn:
            return list(_iter_csv_duckdb(conn, _FakeFileItem(data), kwargs, chunk_size))

    def test_rows_are_typed_by_duckdb(self):
        batches = self._rows(b"id,name,day\n1,a,2026-01-02\n2,,\n")
        assert len(batches) == 1
        first, second = batches[0]
        assert first["id"] == 1
        assert first["name"] == "a"
        assert str(first["day"]) == "2026-01-02"
        assert second == {"id": 2, "name": None, "day": None}

    def test_batches_by_chunk_size(self):
        batches = self._rows(b"n\n1\n2\n3\n4\n5\n", chunk_size=2)
        assert [[row["n"] for row in batch] for batch in batches] == [
            [1, 2],
            [3, 4],
            [5],
        ]

    def test_passes_reader_options(self):
        batches = self._rows(b"a;b\n1;2\n", sep=";")
        assert batches == [[{"a": 1, "b": 2}]]

    def test_files_on_one_connection_stay_separate(self):
        with duckdb.connect() as conn:
            first = list(_iter_csv_duckdb(conn, _FakeFileItem(b"x\n1\n"), {}))
            second = list(_iter_csv_duckdb(conn, _FakeFileItem(b"y\n2\n"), {}))
        assert first == [[{"x": 1}]]
        assert second == [[{"y": 2}]]


@pytest.mark.unit
class TestReadCsv:
    def test_reads_local_files_through_duckdb(self, tmp_path):
        (tmp_path / "feed.csv").write_text("id,name\n1,a\n2,b\n", encoding="utf-8")
        client = FilesystemClient(
            FilesystemConfig(
                filesystem_type="file",
                bucket_name=str(tmp_path),
                file_glob="*.csv",
                file_type="csv",
            )
        )
        assert list(client.read_csv()) == [
            {"id": 1, "name": "a"},
            {"id": 2, "name": "b"},
        ]


@pytest.mark.unit
class TestDuckdbReaderOptions:
    @pytest.fixture
    def client(self, tmp_path):
        (tmp_path / "feed.csv").write_text("n\n1\n2\n3\n", encoding="utf-8")
        return FilesystemClient(
            FilesystemConfig(
                filesystem_type="file",
                bucket_name=str(tmp_path),
                file_glob="*.csv",
                file_type="csv",
            )
        )

    def test_chunk_size_sets_batch_size(self, client):
        with patch(
            "dlt_saga.pipelines.filesystem.client._iter_csv_duckdb",
            wraps=_iter_csv_duckdb,
        ) as reader:
            rows = list(client.read_csv(chunk_size=2))
        assert [row["n"] for row in rows] == [1, 2, 3]
        assert reader.call_args.args[3] == 2
        assert "chunk_size" not in reader.call_args.args[2]

    @pytest.mark.parametrize("option", ["use_pyarrow", "filename"])
    def test_unsupported_dlt_options_are_rejected(self, client, option):
        with pytest.raises(ValueError, match=option):
            client.read_csv(**{option: True})
