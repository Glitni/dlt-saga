"""Unit tests for the incremental/partial historize target-existence guard.

State is keyed on the pipeline name, so renaming ``output_table`` /
``output_dataset`` still finds the prior run's log entry while the target now
lives under a new name. Without the guard the run fails with a raw "table not
found"; the guard raises an actionable config-changed error instead.
"""

import logging
from unittest.mock import MagicMock

import pytest

from dlt_saga.historize.runner import HistorizeRunner


def _make_runner(table_exists: bool) -> tuple:
    dest = MagicMock()
    dest.table_exists.return_value = table_exists

    runner = object.__new__(HistorizeRunner)
    runner.logger = logging.getLogger(__name__)
    runner.destination = dest
    runner.pipeline_name = "grp__tbl"
    runner.target_schema = "schema"
    runner.target_table_name = "tbl_historized"
    runner.target_table_id = "schema.tbl_historized"
    return runner, dest


@pytest.mark.unit
class TestGuardTargetExists:
    def test_missing_target_on_incremental_raises_friendly_error(self):
        runner, dest = _make_runner(table_exists=False)
        with pytest.raises(ValueError) as excinfo:
            runner._guard_target_exists(needs_full=False)
        # The suggested rebuild is scoped to this pipeline — a bare --full-refresh
        # with no selector would reprocess every historized table.
        assert 'saga historize -s "grp__tbl" --full-refresh' in str(excinfo.value)
        dest.table_exists.assert_called_once_with("schema", "tbl_historized")

    def test_existing_target_on_incremental_passes(self):
        runner, _ = _make_runner(table_exists=True)
        runner._guard_target_exists(needs_full=False)  # must not raise

    def test_full_refresh_skips_check(self):
        """A full reprocess (re)creates the target, so a missing table is fine —
        the guard must not query or raise."""
        runner, dest = _make_runner(table_exists=False)
        runner._guard_target_exists(needs_full=True)  # must not raise
        dest.table_exists.assert_not_called()
