"""Unit tests for the arrival-column presence check in late-arrival detection.

An arrival column the source doesn't have means detection can't run. Skipping is
correct, but an explicit ``detect_late_arrivals: true`` asked for replay —
silently skipping there would report a clean run forever, so the skip must be
visible at WARNING.
"""

import logging
from unittest.mock import MagicMock

import pytest

from dlt_saga.historize.config import HistorizeConfig
from dlt_saga.historize.runner import HistorizeRunner


def _make_runner(detect: bool | None, has_column: bool) -> HistorizeRunner:
    runner = object.__new__(HistorizeRunner)
    runner.logger = logging.getLogger("dlt_saga.historize.runner")
    runner.pipeline_name = "grp__tbl"
    runner.config = HistorizeConfig(
        primary_key=["id"],
        detect_late_arrivals=detect,
        arrival_column="_dlt_ingested_at",
        snapshot_column="snapshot_date",
    )
    runner.config_dict = {"write_disposition": "append+historize"}
    runner.state_manager = MagicMock()
    runner.source_table_id = "proj.ds.src"
    runner._filter_sql = None
    runner._source_can_reference = lambda column: has_column
    return runner


@pytest.mark.unit
class TestMissingArrivalColumnSkip:
    def test_explicit_opt_in_warns(self, caplog):
        runner = _make_runner(detect=True, has_column=False)
        with caplog.at_level(logging.DEBUG):
            assert runner._find_late_arrivals(state=MagicMock()) is None
        records = [r for r in caplog.records if "arrival column" in r.message]
        assert [r.levelno for r in records] == [logging.WARNING]
        runner.state_manager.find_late_arrivals.assert_not_called()

    def test_default_tri_state_stays_at_debug(self, caplog):
        # Unset is the default and fires for every source without an arrival
        # column; warning there would be noise on runs nobody opted in for.
        runner = _make_runner(detect=None, has_column=False)
        with caplog.at_level(logging.DEBUG):
            assert runner._find_late_arrivals(state=MagicMock()) is None
        records = [r for r in caplog.records if "arrival column" in r.message]
        assert [r.levelno for r in records] == [logging.DEBUG]

    def test_present_arrival_column_runs_detection(self):
        runner = _make_runner(detect=True, has_column=True)
        runner._find_late_arrivals(state=MagicMock())
        runner.state_manager.find_late_arrivals.assert_called_once()
