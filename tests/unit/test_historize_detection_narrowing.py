"""Unit tests for config changes that only narrow change detection.

``track_columns`` / ``ignore_columns`` are fingerprinted, but an edit that can
only shrink the hashed column set (a column added to ``ignore_columns``, one
dropped from ``track_columns``) leaves every stored version correct, so it
applies going forward instead of demanding a full refresh. Widening still does.
"""

import logging
from unittest.mock import MagicMock

import pytest

from dlt_saga.historize.config import HistorizeConfig
from dlt_saga.historize.runner import HistorizeRunner
from dlt_saga.historize.state import HistorizeStateManager

pytestmark = pytest.mark.unit


def _config(**kwargs):
    return HistorizeConfig(primary_key=["id"], snapshot_column="snap", **kwargs)


def _manager():
    dest = MagicMock()
    dest.get_full_table_id.return_value = "cat.schema._saga_historize_log"
    return HistorizeStateManager(dest, "db", "schema")


def _state(manager, **previous):
    return HistorizeStateManager.PipelineState(
        config_fingerprint=manager.compute_fingerprint(_config(**previous)),
        has_successful_run=True,
    )


class TestNarrowed:
    @pytest.mark.parametrize(
        "previous, current",
        [
            ({}, {"ignore_columns": ["noise"]}),
            ({"ignore_columns": ["a"]}, {"ignore_columns": ["a", "b"]}),
            ({}, {"track_columns": ["a", "b"]}),
            ({"track_columns": ["a", "b"]}, {"track_columns": ["a"]}),
            (
                {"track_columns": ["a", "b"]},
                {"track_columns": ["a", "b"], "ignore_columns": ["b"]},
            ),
            # Tracking a previously ignored column counts as narrowing only
            # while it stays out of the hash.
            (
                {"ignore_columns": ["b"]},
                {"track_columns": ["a", "b"], "ignore_columns": ["b"]},
            ),
        ],
    )
    def test_forward_only(self, previous, current):
        manager = _manager()
        changes = manager.fingerprint_changes(
            _state(manager, **previous), _config(**current)
        )
        assert changes.blocking == []
        assert set(changes.narrowed) <= {"track_columns", "ignore_columns"}
        assert changes.narrowed
        assert not manager.config_changed(
            _state(manager, **previous), _config(**current)
        )

    @pytest.mark.parametrize(
        "previous, current",
        [
            ({"ignore_columns": ["noise"]}, {}),
            ({"ignore_columns": ["a", "b"]}, {"ignore_columns": ["a"]}),
            ({"track_columns": ["a"]}, {}),
            ({"track_columns": ["a"]}, {"track_columns": ["a", "b"]}),
            ({"track_columns": ["a"]}, {"track_columns": ["b"]}),
            # A column that was ignored enters the hash through track_columns.
            ({"ignore_columns": ["b"]}, {"track_columns": ["a", "b"]}),
            # Ignoring less while tracking a subset: the subset keeps b hashed.
            (
                {"track_columns": ["a", "b"], "ignore_columns": ["b"]},
                {"track_columns": ["b"]},
            ),
        ],
    )
    def test_widening_blocks(self, previous, current):
        manager = _manager()
        changes = manager.fingerprint_changes(
            _state(manager, **previous), _config(**current)
        )
        assert changes.blocking
        assert changes.narrowed == []
        assert manager.config_changed(_state(manager, **previous), _config(**current))

    def test_other_keys_still_block_alongside_a_narrowing(self):
        manager = _manager()
        state = _state(manager)
        changes = manager.fingerprint_changes(
            state, _config(ignore_columns=["noise"], track_deletions=False)
        )
        assert changes.blocking == ["track_deletions"]
        assert changes.narrowed == ["ignore_columns"]

    def test_unchanged_config_reports_nothing(self):
        manager = _manager()
        changes = manager.fingerprint_changes(_state(manager), _config())
        assert (changes.blocking, changes.narrowed) == ([], [])

    def test_first_run_reports_nothing(self):
        manager = _manager()
        state = HistorizeStateManager.PipelineState()
        changes = manager.fingerprint_changes(state, _config(ignore_columns=["x"]))
        assert (changes.blocking, changes.narrowed) == ([], [])


def _runner(previous, current, full_refresh=False):
    runner = object.__new__(HistorizeRunner)
    runner.logger = logging.getLogger("dlt_saga.historize.runner")
    runner.pipeline_name = "grp__tbl"
    runner.full_refresh = full_refresh
    runner.partial_refresh = False
    runner.historize_from = None
    runner.config = _config(**current)
    runner.state_manager = _manager()
    return runner, _state(runner.state_manager, **previous)


class TestRunnerCheck:
    def test_narrowing_is_logged_not_refused(self, caplog):
        runner, state = _runner({}, {"ignore_columns": ["noise"]})
        with caplog.at_level(logging.INFO, logger="dlt_saga.historize.runner"):
            runner._check_config_changes(state)
        messages = [r.getMessage() for r in caplog.records]
        assert any("narrowed" in m and "ignore_columns" in m for m in messages)

    def test_widening_is_refused_with_the_diff(self):
        runner, state = _runner({"ignore_columns": ["noise"]}, {})
        with pytest.raises(ValueError) as excinfo:
            runner._check_config_changes(state)
        msg = str(excinfo.value)
        assert "ignore_columns: ['noise'] → []" in msg
        assert "--full-refresh" in msg

    def test_blocking_message_lists_the_narrowed_keys_too(self):
        runner, state = _runner(
            {}, {"ignore_columns": ["noise"], "track_deletions": False}
        )
        msg = str(pytest.raises(ValueError, runner._check_config_changes, state).value)
        assert "track_deletions" in msg and "ignore_columns" in msg

    def test_full_refresh_passes_any_change(self):
        runner, state = _runner({"ignore_columns": ["noise"]}, {}, full_refresh=True)
        runner._check_config_changes(state)  # must not raise
