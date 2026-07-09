"""Unit tests for dlt_saga.utility.tabular.dedupe_headers."""

import logging

import pytest

from dlt_saga.utility.tabular import dedupe_headers

pytestmark = pytest.mark.unit


class TestDedupeHeaders:
    def test_no_duplicates_unchanged(self):
        assert dedupe_headers(["a", "b", "c"]) == ["a", "b", "c"]

    def test_duplicate_gets_numeric_suffix(self):
        # First occurrence kept as-is; repeats suffixed _2, _3 …
        assert dedupe_headers(["a", "a", "a"]) == ["a", "a_2", "a_3"]

    def test_interleaved_duplicates(self):
        assert dedupe_headers(["id", "x", "id", "x", "id"]) == [
            "id",
            "x",
            "id_2",
            "x_2",
            "id_3",
        ]

    def test_suffix_avoids_colliding_with_existing_column(self):
        # "a_2" already exists, so the second "a" must skip past it.
        assert dedupe_headers(["a", "a_2", "a"]) == ["a", "a_2", "a_3"]

    def test_non_string_headers_coerced(self):
        assert dedupe_headers([1, 1, 2]) == ["1", "1_2", "2"]

    def test_empty_input(self):
        assert dedupe_headers([]) == []

    def test_warns_once_naming_collisions(self, caplog):
        with caplog.at_level(logging.WARNING):
            dedupe_headers(["a", "a", "b", "b"], source="sheet 'Data'")
        warnings = [r for r in caplog.records if r.levelno == logging.WARNING]
        assert len(warnings) == 1
        msg = warnings[0].getMessage()
        assert "'a'" in msg and "'b'" in msg
        assert "sheet 'Data'" in msg

    def test_no_warning_without_duplicates(self, caplog):
        with caplog.at_level(logging.WARNING):
            dedupe_headers(["a", "b", "c"])
        assert not caplog.records

    def test_result_has_no_duplicates(self):
        result = dedupe_headers(["x", "x", "x_2", "x"])
        assert len(result) == len(set(result))
