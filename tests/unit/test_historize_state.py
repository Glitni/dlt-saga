"""Unit tests for HistorizeStateManager error handling.

``clear_log_entries*`` must swallow a missing log table (nothing to clear on the
first run) but propagate any real failure — leaving stale `completed` entries
after a failed clear would make the next run go incremental against a freshly
rebuilt table (silent data divergence during full refresh).
"""

from unittest.mock import MagicMock

import pytest

from dlt_saga.historize.state import HistorizeStateManager


def _make_manager(execute_side_effect):
    dest = MagicMock()
    dest.get_full_table_id.return_value = "cat.schema._saga_historize_log"
    dest.escape_string_literal.side_effect = lambda s: s
    dest.execute_sql.side_effect = execute_side_effect
    return HistorizeStateManager(dest, "db", "schema")


@pytest.mark.unit
class TestClearLogEntries:
    def test_missing_table_swallowed(self):
        mgr = _make_manager(RuntimeError("Table does not exist"))
        mgr.clear_log_entries("pipe")  # no raise

    def test_real_error_propagates(self):
        mgr = _make_manager(RuntimeError("PERMISSION_DENIED"))
        with pytest.raises(RuntimeError, match="PERMISSION_DENIED"):
            mgr.clear_log_entries("pipe")

    def test_clear_from_missing_table_swallowed(self):
        mgr = _make_manager(RuntimeError("no such table"))
        mgr.clear_log_entries_from("pipe", "2026-01-01")  # no raise

    def test_clear_from_real_error_propagates(self):
        mgr = _make_manager(RuntimeError("network down"))
        with pytest.raises(RuntimeError, match="network down"):
            mgr.clear_log_entries_from("pipe", "2026-01-01")
