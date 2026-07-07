"""Unit tests for chunked CSV+metadata streaming in the filesystem client."""

import io

import pytest

from dlt_saga.pipelines.filesystem.client import _iter_csv_records_with_metadata


class _FakeFileItem(dict):
    """Stand-in for dlt's FileItemDict: dict metadata + a binary open()."""

    def __init__(self, data: bytes, **meta):
        super().__init__(**meta)
        self._data = data

    def open(self):
        return io.BytesIO(self._data)


@pytest.mark.unit
class TestIterCsvRecordsWithMetadata:
    def test_streams_rows_and_injects_metadata(self):
        item = _FakeFileItem(
            b"id,name\n01,Alice\n02,Bob\n",
            file_name="a.csv",
            modification_date="2026-01-01T00:00:00",
        )
        rows = list(_iter_csv_records_with_metadata(item, ",", "utf-8"))
        assert rows == [
            {
                "id": "01",  # dtype=str preserves the leading zero
                "name": "Alice",
                "_dlt_source_file_name": "a.csv",
                "_dlt_source_modification_date": "2026-01-01T00:00:00",
            },
            {
                "id": "02",
                "name": "Bob",
                "_dlt_source_file_name": "a.csv",
                "_dlt_source_modification_date": "2026-01-01T00:00:00",
            },
        ]

    def test_chunking_yields_all_rows_in_order(self):
        item = _FakeFileItem(
            b"n\n1\n2\n3\n4\n5\n", file_name="n.csv", modification_date=None
        )
        rows = list(_iter_csv_records_with_metadata(item, ",", "utf-8", chunk_size=2))
        assert [r["n"] for r in rows] == ["1", "2", "3", "4", "5"]

    def test_respects_separator(self):
        item = _FakeFileItem(b"a;b\n1;2\n", file_name="s.csv", modification_date=None)
        rows = list(_iter_csv_records_with_metadata(item, ";", "utf-8"))
        assert rows[0]["a"] == "1"
        assert rows[0]["b"] == "2"

    def test_decodes_with_configured_encoding(self):
        item = _FakeFileItem(
            "name\nBjørn\n".encode("latin-1"),
            file_name="e.csv",
            modification_date=None,
        )
        rows = list(_iter_csv_records_with_metadata(item, ",", "latin-1"))
        assert rows[0]["name"] == "Bjørn"

    def test_empty_file_yields_nothing(self):
        item = _FakeFileItem(b"", file_name="empty.csv", modification_date=None)
        assert list(_iter_csv_records_with_metadata(item, ",", "utf-8")) == []

    def test_header_only_file_yields_nothing(self):
        item = _FakeFileItem(b"id,name\n", file_name="h.csv", modification_date=None)
        assert list(_iter_csv_records_with_metadata(item, ",", "utf-8")) == []
