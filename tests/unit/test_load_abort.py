"""Aborting a pending package after a terminal load-setup failure.

dlt aborts a package when a *job* fails terminally (with
``load.auto_abort_on_terminal_error``), but a terminal error raised while the
load is being set up leaves the package pending, and the next run replays it
instead of extracting. These tests pin which failures are aborted: only
terminal, setup-time, load-step failures, and only under the same setting.
"""

from unittest.mock import MagicMock, patch

import pytest
from dlt.destinations.exceptions import (
    DatabaseTerminalException,
    DatabaseTransientException,
    DatabaseUndefinedRelation,
)
from dlt.load.exceptions import LoadClientJobTerminalRetry
from dlt.pipeline.exceptions import PipelineStepFailed

from dlt_saga.pipelines.load_abort import abort_after_terminal_load_failure


def _pipeline(*, pending=True):
    pipeline = MagicMock()
    pipeline.pipeline_name = "group__orders"
    pipeline.has_pending_data = pending
    return pipeline


def _failed(pipeline, cause, step="load"):
    return PipelineStepFailed(pipeline, step, "load-1", cause)


@pytest.fixture(autouse=True)
def auto_abort_enabled():
    with patch(
        "dlt_saga.pipelines.load_abort._auto_abort_enabled", return_value=True
    ) as enabled:
        yield enabled


@pytest.mark.unit
class TestAbortAfterTerminalLoadFailure:
    @pytest.mark.parametrize(
        "cause",
        [DatabaseTerminalException(Exception("x")), DatabaseUndefinedRelation("x")],
    )
    def test_terminal_setup_failure_aborts(self, cause):
        pipeline = _pipeline()
        assert abort_after_terminal_load_failure(pipeline, _failed(pipeline, cause))
        pipeline.abort_packages.assert_called_once_with()

    def test_transient_failure_is_left_for_retry(self):
        pipeline = _pipeline()
        exc = _failed(pipeline, DatabaseTransientException(Exception("timeout")))
        assert not abort_after_terminal_load_failure(pipeline, exc)
        pipeline.abort_packages.assert_not_called()

    def test_job_failure_is_left_to_dlt(self):
        """Job-level terminal failures are dlt's auto-abort to handle."""
        pipeline = _pipeline()
        cause = LoadClientJobTerminalRetry("load-1", "job-1", "boom", Exception())
        assert not abort_after_terminal_load_failure(pipeline, _failed(pipeline, cause))
        pipeline.abort_packages.assert_not_called()

    def test_other_steps_are_ignored(self):
        pipeline = _pipeline()
        exc = _failed(pipeline, DatabaseTerminalException(Exception()), "extract")
        assert not abort_after_terminal_load_failure(pipeline, exc)
        pipeline.abort_packages.assert_not_called()

    def test_non_step_exceptions_are_ignored(self):
        pipeline = _pipeline()
        assert not abort_after_terminal_load_failure(pipeline, ValueError("x"))
        pipeline.abort_packages.assert_not_called()

    def test_disabled_setting_leaves_package_pending(self, auto_abort_enabled):
        auto_abort_enabled.return_value = False
        pipeline = _pipeline()
        exc = _failed(pipeline, DatabaseTerminalException(Exception()))
        assert not abort_after_terminal_load_failure(pipeline, exc)
        pipeline.abort_packages.assert_not_called()

    def test_nothing_pending_is_a_noop(self):
        pipeline = _pipeline(pending=False)
        exc = _failed(pipeline, DatabaseTerminalException(Exception()))
        assert not abort_after_terminal_load_failure(pipeline, exc)
        pipeline.abort_packages.assert_not_called()

    def test_abort_failure_warns_and_returns_false(self, caplog):
        pipeline = _pipeline()
        pipeline.abort_packages.side_effect = OSError("locked")
        exc = _failed(pipeline, DatabaseTerminalException(Exception()))
        assert not abort_after_terminal_load_failure(pipeline, exc)
        assert "abort-packages" in caplog.text


@pytest.mark.unit
class TestExecuteDltRunAborts:
    def test_original_error_is_reraised_after_abort(self):
        from dlt_saga.destinations.duckdb.destination import DuckDBDestination

        pipeline = _pipeline()
        exc = _failed(pipeline, DatabaseTerminalException(Exception()))
        pipeline.run.side_effect = exc

        with pytest.raises(PipelineStepFailed) as raised:
            DuckDBDestination.execute_dlt_run(MagicMock(), pipeline, data=[])

        assert raised.value is exc
        pipeline.abort_packages.assert_called_once_with()
