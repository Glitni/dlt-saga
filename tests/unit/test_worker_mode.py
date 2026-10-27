"""Unit tests for worker-mode status handling and command dispatch.

Covers the worker's operational-hazard fixes:
- A crash after the task is marked ``running`` must record a final status
  (best effort) and re-raise, so a stuck ``running`` can't block the plan.
- The final status is per pipeline: one failing member of a task group must
  not mark the others failed.
- A SIGTERM (task timeout, cancellation) records the unfinished pipelines as
  failed instead of leaving them ``running``.
- ``run_worker_mode`` must dispatch to the executor for the resolved command,
  with ``SAGA_WORKER_COMMAND`` as the fallback.
"""

import signal
from contextlib import ExitStack
from typing import Optional
from unittest.mock import MagicMock, patch

import pytest
import typer

import dlt_saga.utility.cli.run_modes as cli
from dlt_saga.pipeline_config import PipelineConfig

_EXECUTORS = (
    "_execute_worker_ingest",
    "_execute_worker_historize",
    "_execute_worker_run",
)


def _make_config(name: str, write_disposition: str = "append") -> PipelineConfig:
    return PipelineConfig(
        pipeline_group="test",
        pipeline_name=f"test__{name}",
        table_name=name,
        identifier=f"configs/test/{name}.yml",
        config_dict={"base_table_name": name, "write_disposition": write_disposition},
        enabled=True,
        tags=[],
        source_type="file",
        schema_name="",
    )


def _patch_collaborators(stack, assignment=None):
    """Patch run_worker_mode's collaborators; return the plan_manager mock."""
    if assignment is None:
        assignment = [_make_config("only")]
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


def _task_statuses(plan_manager):
    return [c.args[2] for c in plan_manager.update_task_status.call_args_list]


def _final_outcomes(plan_manager):
    plan_manager.record_pipeline_outcomes.assert_called_once()
    return plan_manager.record_pipeline_outcomes.call_args.args[2]


def _failing(*names: str, message: str = "boom"):
    """A ``run_fn`` stand-in failing for the named tables, succeeding otherwise."""

    def run(config, *args) -> Optional[str]:
        return message if config.table_name in names else None

    return run


@pytest.mark.unit
class TestWorkerCrashStatus:
    def test_crash_marks_pipelines_failed_and_reraises(self):
        with ExitStack() as stack:
            plan_manager = _patch_collaborators(stack)
            stack.enter_context(
                patch.object(
                    cli, "_execute_worker_ingest", side_effect=RuntimeError("boom")
                )
            )
            with pytest.raises(RuntimeError, match="boom"):
                cli.run_worker_mode(worker_command="ingest")

        assert _task_statuses(plan_manager) == ["running"]
        status, error = _final_outcomes(plan_manager)["configs/test/only.yml"]
        assert status == "failed"
        assert "boom" in error

    def test_base_exception_also_recorded(self):
        """SystemExit/KeyboardInterrupt must still flip the task off 'running'."""
        with ExitStack() as stack:
            plan_manager = _patch_collaborators(
                stack, [_make_config("only", "historize")]
            )
            stack.enter_context(
                patch.object(
                    cli, "_execute_worker_historize", side_effect=KeyboardInterrupt()
                )
            )
            with pytest.raises(KeyboardInterrupt):
                cli.run_worker_mode(worker_command="historize")
        assert _final_outcomes(plan_manager)["configs/test/only.yml"][0] == "failed"

    def test_status_write_failure_does_not_mask_original(self):
        with ExitStack() as stack:
            plan_manager = _patch_collaborators(stack)
            plan_manager.record_pipeline_outcomes.side_effect = RuntimeError(
                "plan store down"
            )
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
        assert _task_statuses(plan_manager) == ["running"]
        assert _final_outcomes(plan_manager) == {
            "configs/test/only.yml": ("completed", None)
        }


@pytest.mark.unit
class TestPerPipelineStatus:
    """A task group's members are recorded by their own outcome."""

    def test_failure_is_attributed_to_its_own_pipeline(self):
        configs = [_make_config("ok"), _make_config("bad")]
        with ExitStack() as stack:
            plan_manager = _patch_collaborators(stack, configs)
            stack.enter_context(
                patch.object(cli, "_run_pipeline_safe", side_effect=_failing("bad"))
            )
            with pytest.raises(typer.Exit):
                cli.run_worker_mode(worker_command="ingest")

        assert _final_outcomes(plan_manager) == {
            "configs/test/ok.yml": ("completed", None),
            "configs/test/bad.yml": ("failed", "boom"),
        }

    def test_run_command_combines_both_phases_errors(self):
        configs = [_make_config("both", "append+historize")]
        with ExitStack() as stack:
            plan_manager = _patch_collaborators(stack, configs)
            stack.enter_context(
                patch.object(cli, "_run_pipeline_safe", side_effect=_failing("both"))
            )
            stack.enter_context(
                patch.object(
                    cli,
                    "_run_historize_safe",
                    side_effect=_failing("both", message="stale"),
                )
            )
            with pytest.raises(typer.Exit):
                cli.run_worker_mode(worker_command="run")

        assert _final_outcomes(plan_manager) == {
            "configs/test/both.yml": ("failed", "ingest: boom; historize: stale"),
        }


@pytest.mark.unit
class TestTaskProgress:
    def test_interruption_fails_only_unfinished_pipelines(self):
        done, failed, pending = (
            _make_config("done"),
            _make_config("failed"),
            _make_config("pending"),
        )
        progress = cli._TaskProgress([done, failed, pending], "ingest")
        progress.record(done, "ingest", None)
        progress.record(failed, "ingest", "boom")

        assert progress.outcomes(interrupted_by="stopped") == {
            "configs/test/done.yml": ("completed", None),
            "configs/test/failed.yml": ("failed", "boom"),
            "configs/test/pending.yml": ("failed", "stopped"),
        }

    def test_run_pipeline_mid_way_is_unfinished(self):
        """Ingested but not yet historized: the task didn't finish it."""
        config = _make_config("half", "append+historize")
        progress = cli._TaskProgress([config], "run")
        progress.record(config, "ingest", None)

        assert progress.outcomes(interrupted_by="stopped") == {
            "configs/test/half.yml": ("failed", "stopped")
        }

    def test_phase_not_enabled_is_not_awaited(self):
        """An ingest-only pipeline in a `run` task is finished after ingest."""
        config = _make_config("ingest_only")
        progress = cli._TaskProgress([config], "run")
        progress.record(config, "ingest", None)

        assert progress.outcomes(interrupted_by="stopped") == {
            "configs/test/ingest_only.yml": ("completed", None)
        }


@pytest.mark.unit
class TestSigtermHandler:
    def _install(self, progress, plan_manager):
        writer = cli._TaskStatusWriter(plan_manager, "exec-1", 0, progress)
        restore = cli._install_sigterm_handler(writer, 0)
        handler = signal.getsignal(signal.SIGTERM)
        restore()
        return writer, handler

    def test_records_unfinished_as_failed_and_exits(self):
        done, hung = _make_config("done"), _make_config("hung")
        progress = cli._TaskProgress([done, hung], "ingest")
        progress.record(done, "ingest", None)
        plan_manager = MagicMock()
        _, handler = self._install(progress, plan_manager)

        with (
            patch.object(cli.os, "_exit") as exit_mock,
            patch.object(cli.logging, "shutdown"),
        ):
            handler(signal.SIGTERM, None)

        outcomes = plan_manager.record_pipeline_outcomes.call_args.args[2]
        assert outcomes["configs/test/done.yml"] == ("completed", None)
        assert outcomes["configs/test/hung.yml"] == ("failed", cli._SIGTERM_REASON)
        exit_mock.assert_called_once_with(cli._SIGTERM_EXIT_CODE)

    def test_defers_to_a_final_write_already_under_way(self):
        progress = cli._TaskProgress([_make_config("only")], "ingest")
        plan_manager = MagicMock()
        writer, handler = self._install(progress, plan_manager)
        writer.write()

        with patch.object(cli.os, "_exit") as exit_mock:
            handler(signal.SIGTERM, None)

        plan_manager.record_pipeline_outcomes.assert_called_once()
        exit_mock.assert_not_called()

    def test_run_worker_mode_restores_previous_handler(self):
        previous = signal.getsignal(signal.SIGTERM)
        with ExitStack() as stack:
            _patch_collaborators(stack)
            stack.enter_context(
                patch.object(cli, "_execute_worker_ingest", return_value=[])
            )
            cli.run_worker_mode(worker_command="ingest")
        assert signal.getsignal(signal.SIGTERM) == previous


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
