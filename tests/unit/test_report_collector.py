"""Unit tests for report-collector error classification.

A missing table is an expected empty section (debug); any other error must warn
loudly so a permission/network failure doesn't render a silently-empty section.
"""

import logging
from unittest.mock import MagicMock

import pytest

from dlt_saga.report.collector import _query_load_runs


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
