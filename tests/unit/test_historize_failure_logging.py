"""Historize failures write a status='failed' log entry so they show up in
`saga report` (config errors do not — they aren't run outcomes)."""

import logging
from unittest.mock import MagicMock

import pytest

pytestmark = pytest.mark.unit


def _runner():
    from dlt_saga.historize.runner import HistorizeRunner

    runner = object.__new__(HistorizeRunner)
    runner.logger = logging.getLogger(__name__)
    runner.pipeline_name = "grp__tbl"
    runner.source_table_name = "tbl"
    runner.target_table_name = "tbl_historized"
    runner.source_table_id = "db.schema.tbl"
    runner.target_table_id = "db.schema.tbl_historized"
    runner.full_refresh = False
    runner.config = MagicMock()
    runner.state_manager = MagicMock()
    runner.state_manager.compute_fingerprint.return_value = "fp"
    return runner


class TestHistorizeFailureLogging:
    def test_unexpected_failure_writes_failed_log_entry(self):
        runner = _runner()
        runner._execute_run = MagicMock(side_effect=RuntimeError("boom"))

        result = runner.run()

        assert result["status"] == "failed"
        runner.state_manager.write_log_entry.assert_called_once()
        entry = runner.state_manager.write_log_entry.call_args.args[0]
        assert entry.status == "failed"
        # NULL snapshot → never becomes a baseline in get_pipeline_state.
        assert entry.snapshot_value is None
        assert entry.pipeline_name == "grp__tbl"
        # Fully-qualified ids, consistent with the success-path log entries.
        assert entry.source_table == "db.schema.tbl"
        assert entry.target_table == "db.schema.tbl_historized"

    def test_config_error_does_not_write_log_entry(self):
        runner = _runner()
        runner._execute_run = MagicMock(side_effect=ValueError("no primary_key"))

        result = runner.run()

        assert result["config_error"] is True
        runner.state_manager.write_log_entry.assert_not_called()

    def test_log_write_failure_is_swallowed(self):
        runner = _runner()
        runner._execute_run = MagicMock(side_effect=RuntimeError("boom"))
        runner.state_manager.write_log_entry.side_effect = Exception("log table gone")

        # The original failure is still reported; the best-effort log write
        # error must not mask it.
        result = runner.run()
        assert result["status"] == "failed"
        assert result["error"] == "boom"
