"""Unit tests for worker-mode status handling and command dispatch.

Covers two operational-hazard fixes:
- A crash after the task is marked ``running`` must record ``failed`` (best
  effort) and re-raise, so a stuck ``running`` can't block the whole plan.
- ``run_worker_mode`` must dispatch to the executor for the resolved command,
  with ``SAGA_WORKER_COMMAND`` as the fallback.
"""

from contextlib import ExitStack
from unittest.mock import MagicMock, patch

import pytest

import dlt_saga.cli as cli

_EXECUTORS = (
    "_execute_worker_ingest",
    "_execute_worker_historize",
    "_execute_worker_run",
)


def _patch_collaborators(stack, assignment=None):
    """Patch run_worker_mode's collaborators; return the plan_manager mock."""
    if assignment is None:
        assignment = [MagicMock(name="pipeline_config")]
    plan_manager = MagicMock()
    plan_manager.get_task_assignment.return_value = assignment

    stack.enter_context(
        patch.object(cli, "_get_worker_environment", return_value=("exec-1", 0))
    )
    stack.enter_context(
        patch.object(
            cli, "get_execution_context", return_value=MagicMock(full_refresh=False)
        )
    )
    stack.enter_context(
        patch.object(cli, "_resolve_worker_concurrency", return_value=4)
    )
    stack.enter_context(patch.object(cli, "_create_orchestration_destination"))
    stack.enter_context(patch.object(cli, "_log_assigned_pipelines"))
    stack.enter_context(
        patch(
            "dlt_saga.utility.naming.get_execution_plan_schema", return_value="plan_ds"
        )
    )
    stack.enter_context(
        patch(
            "dlt_saga.utility.orchestration.execution_plan.ExecutionPlanManager",
            return_value=plan_manager,
        )
    )
    return plan_manager


def _statuses(plan_manager):
    return [c.args[2] for c in plan_manager.update_task_status.call_args_list]


@pytest.mark.unit
class TestWorkerCrashStatus:
    def test_crash_marks_task_failed_and_reraises(self):
        with ExitStack() as stack:
            plan_manager = _patch_collaborators(stack)
            stack.enter_context(
                patch.object(
                    cli, "_execute_worker_ingest", side_effect=RuntimeError("boom")
                )
            )
            with pytest.raises(RuntimeError, match="boom"):
                cli.run_worker_mode(worker_command="ingest")

        assert _statuses(plan_manager) == ["running", "failed"]
        failed_call = plan_manager.update_task_status.call_args_list[-1]
        assert "boom" in failed_call.args[3]

    def test_base_exception_also_recorded(self):
        """SystemExit/KeyboardInterrupt (e.g. a SIGTERM-driven shutdown) must
        still flip the task off 'running'."""
        with ExitStack() as stack:
            plan_manager = _patch_collaborators(stack)
            stack.enter_context(
                patch.object(
                    cli, "_execute_worker_historize", side_effect=KeyboardInterrupt()
                )
            )
            with pytest.raises(KeyboardInterrupt):
                cli.run_worker_mode(worker_command="historize")
        assert _statuses(plan_manager) == ["running", "failed"]

    def test_status_write_failure_does_not_mask_original(self):
        with ExitStack() as stack:
            plan_manager = _patch_collaborators(stack)
            # "running" write succeeds; the crash-path "failed" write blows up.
            plan_manager.update_task_status.side_effect = [
                None,
                RuntimeError("plan store down"),
            ]
            stack.enter_context(
                patch.object(
                    cli, "_execute_worker_ingest", side_effect=RuntimeError("boom")
                )
            )
            # The original error propagates, not the status-write failure.
            with pytest.raises(RuntimeError, match="boom"):
                cli.run_worker_mode(worker_command="ingest")

    def test_success_marks_completed(self):
        with ExitStack() as stack:
            plan_manager = _patch_collaborators(stack)
            stack.enter_context(
                patch.object(cli, "_execute_worker_ingest", return_value=[])
            )
            cli.run_worker_mode(worker_command="ingest")
        assert _statuses(plan_manager) == ["running", "completed"]


@pytest.mark.unit
class TestWorkerCommandDispatch:
    @pytest.mark.parametrize(
        "command,expected",
        [
            ("ingest", "_execute_worker_ingest"),
            ("historize", "_execute_worker_historize"),
            ("run", "_execute_worker_run"),
        ],
    )
    def test_dispatches_to_correct_executor(self, command, expected):
        with ExitStack() as stack:
            _patch_collaborators(stack)
            mocks = {
                fn: stack.enter_context(patch.object(cli, fn, return_value=[]))
                for fn in _EXECUTORS
            }
            cli.run_worker_mode(worker_command=command)
        mocks[expected].assert_called_once()
        for fn, mock in mocks.items():
            if fn != expected:
                mock.assert_not_called()

    def test_falls_back_to_env_command(self):
        """No explicit command → SAGA_WORKER_COMMAND selects the executor."""
        with ExitStack() as stack:
            _patch_collaborators(stack)
            stack.enter_context(patch.object(cli, "get_env", return_value="historize"))
            mocks = {
                fn: stack.enter_context(patch.object(cli, fn, return_value=[]))
                for fn in _EXECUTORS
            }
            cli.run_worker_mode()
        mocks["_execute_worker_historize"].assert_called_once()
        mocks["_execute_worker_ingest"].assert_not_called()


@pytest.mark.unit
class TestWorkersIgnoredWarning:
    """--workers has no effect in worker mode (concurrency comes from the plan);
    a non-default value should warn rather than be silently dropped."""

    def test_non_default_workers_warns(self, caplog):
        import logging

        with caplog.at_level(logging.WARNING):
            cli._warn_workers_ignored_in_worker_mode(8)
        assert "no effect in worker mode" in caplog.text

    def test_default_workers_is_silent(self, caplog):
        import logging

        with caplog.at_level(logging.WARNING):
            cli._warn_workers_ignored_in_worker_mode(cli._CLI_DEFAULT_WORKERS)
        assert caplog.records == []
