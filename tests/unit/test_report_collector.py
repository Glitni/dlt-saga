"""Unit tests for report-collector error classification.

A missing table is an expected empty section (debug); any other error must warn
loudly so a permission/network failure doesn't render a silently-empty section.
"""

import logging
from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from dlt_saga.report.collector import (
    _as_str_list,
    _query_load_runs,
    _sortable_ts,
    collect_pipeline_metadata,
)


def _fake_config(**config_dict):
    """A minimal stand-in for PipelineConfig for collect_pipeline_metadata."""
    return SimpleNamespace(
        pipeline_name="api__orders",
        pipeline_group="api",
        get_tag_names=lambda: ["daily"],
        raw_write_disposition="append",
        ingest_enabled=True,
        historize_enabled=False,
        enabled=True,
        table_name="orders",
        schema_name="dlt_api",
        adapter=None,
        config_dict=config_dict,
    )


def _dest_raising(exc):
    dest = MagicMock()
    dest.get_full_table_id.return_value = "cat.schema.tbl"
    dest.timestamp_n_days_ago.return_value = "TS"
    dest.execute_sql.side_effect = exc
    return dest


@pytest.mark.unit
class TestReportQueryErrorClassification:
    def test_missing_table_is_debug_not_warning(self, caplog):
        dest = _dest_raising(RuntimeError("Table cat.schema.tbl does not exist"))
        with caplog.at_level(logging.DEBUG, logger="dlt_saga.report.collector"):
            result = _query_load_runs(dest, "schema", 30)
        assert result == []
        assert not any(r.levelno == logging.WARNING for r in caplog.records)

    def test_real_error_warns_but_does_not_raise(self, caplog):
        dest = _dest_raising(RuntimeError("PERMISSION_DENIED"))
        with caplog.at_level(logging.WARNING, logger="dlt_saga.report.collector"):
            result = _query_load_runs(dest, "schema", 30)
        assert result == []
        assert any(
            r.levelno == logging.WARNING and "incomplete" in r.getMessage()
            for r in caplog.records
        )


@pytest.mark.unit
class TestCollectPipelineMetadata:
    def test_reads_doc_metadata_from_config_dict(self):
        cfg = _fake_config(
            description="Orders",
            classification=["pii:false"],
            meta={"data_owner": "team@example.com"},
        )
        (info,) = collect_pipeline_metadata({"api": [cfg]})
        assert info.description == "Orders"
        assert info.classification == ["pii:false"]
        assert info.meta == {"data_owner": "team@example.com"}

    def test_absent_doc_metadata_defaults(self):
        (info,) = collect_pipeline_metadata({"api": [_fake_config()]})
        assert info.description is None
        assert info.classification == []
        assert info.meta is None


@pytest.mark.unit
class TestAsStrList:
    def test_none_is_empty(self):
        assert _as_str_list(None) == []

    def test_scalar_string_wrapped(self):
        assert _as_str_list("pii") == ["pii"]

    def test_list_coerced_to_strings(self):
        assert _as_str_list([1, "b"]) == ["1", "b"]


@pytest.mark.unit
class TestSortableTs:
    """The report sort must not crash comparing a NULL/naive started_at against
    tz-aware warehouse timestamps."""

    def test_none_sorts_before_real_timestamp(self):
        aware = datetime(2026, 1, 1, tzinfo=timezone.utc)
        assert _sortable_ts(None) < _sortable_ts(aware)

    def test_naive_is_treated_as_utc_and_comparable(self):
        naive = datetime(2026, 1, 1)
        aware = datetime(2026, 1, 1, tzinfo=timezone.utc)
        # No TypeError, and equal instants compare equal.
        assert _sortable_ts(naive) == _sortable_ts(aware)

    def test_mixed_list_sorts_without_typeerror(self):
        items = [
            datetime(2026, 3, 1, tzinfo=timezone.utc),
            None,
            datetime(2026, 1, 1),  # naive
        ]
        ordered = sorted(items, key=_sortable_ts, reverse=True)
        assert ordered[0] == datetime(2026, 3, 1, tzinfo=timezone.utc)
        assert ordered[-1] is None
