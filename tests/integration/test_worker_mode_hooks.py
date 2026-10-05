"""Lifecycle hooks on the worker path, against a real plan and a real run.

The unit tests drive `_run_pipeline_safe` directly, which proves the wrapper
fires. These go through `saga plan` and `saga worker` — the path a fan-out
container actually takes — because the bug was never in the wrapper: hooks were
registered by `Session`, and a worker builds no `Session`, so firing anywhere
else would have reached an empty registry.
"""

import pytest
from typer.testing import CliRunner

from dlt_saga.cli import app
from dlt_saga.hooks.registry import (
    ON_PIPELINE_COMPLETE,
    ON_PIPELINE_ERROR,
    ON_PIPELINE_START,
    ON_RUN_COMPLETE,
    get_hook_registry,
)
from dlt_saga.init_command import run_init
from dlt_saga.utility.cli.context import clear_execution_context

EXECUTION_ID = "worker-hooks-test"


def _reset_cli_singletons():
    import dlt_saga.project_config as _project_mod
    import dlt_saga.utility.cli.common as _common_mod
    import dlt_saga.utility.cli.profiles as _profiles_mod

    _profiles_mod._profiles_config = None
    _common_mod._config_source = None
    _project_mod._project_config = None


@pytest.fixture
def project(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    _reset_cli_singletons()
    run_init(no_input=True)
    yield tmp_path
    clear_execution_context()
    _reset_cli_singletons()


@pytest.fixture
def fired():
    """Capture every lifecycle event, keyed by name."""
    registry = get_hook_registry()
    registry.clear()

    events = []
    for name in (
        ON_PIPELINE_START,
        ON_PIPELINE_COMPLETE,
        ON_PIPELINE_ERROR,
        ON_RUN_COMPLETE,
    ):
        registry.register(name, lambda ctx, n=name: events.append((n, ctx)))
    yield events

    registry.clear()


def _plan(select="filesystem__sample"):
    return CliRunner().invoke(
        app, ["plan", "--select", select, "--execution-id", EXECUTION_ID]
    )


def _worker(command="ingest"):
    return CliRunner().invoke(
        app,
        [
            "worker",
            "--execution-id",
            EXECUTION_ID,
            "--task-index",
            "0",
            "--command",
            command,
        ],
    )


def _break_the_source(project):
    """Point the glob at nothing and require a row, so the pipeline fails."""
    (project / "configs" / "filesystem" / "sample.yml").write_text(
        "\n".join(
            [
                "tags: [daily]",
                "write_disposition: replace",
                "min_rows: 1",
                "",
                "filesystem_type: file",
                "bucket_name: data",
                'file_glob: "gone/*.csv"',
                "file_type: csv",
                "",
            ]
        ),
        encoding="utf-8",
    )


@pytest.mark.integration
class TestWorkerFiresPipelineHooks:
    def test_a_successful_worker_run_fires_start_and_complete(self, project, fired):
        assert _plan().exit_code == 0
        assert _worker().exit_code == 0

        names = [name for name, _ in fired]
        assert ON_PIPELINE_START in names, "a worker fired nothing"
        assert ON_PIPELINE_COMPLETE in names
        assert ON_PIPELINE_ERROR not in names

        ctx = dict(fired)[ON_PIPELINE_START]
        assert ctx.pipeline_name == "filesystem__sample"
        assert ctx.command == "ingest"
        assert ctx.config is not None, "the handler cannot act without the config"

    def test_a_failing_worker_run_fires_the_error_event(self, project, fired):
        """The case the gap cost: an `on_pipeline_error` handler written for the
        deployment where it never ran.
        """
        _break_the_source(project)
        assert _plan().exit_code == 0
        assert _worker().exit_code != 0

        by_name = dict(fired)
        assert ON_PIPELINE_ERROR in by_name
        assert ON_PIPELINE_COMPLETE not in by_name
        assert by_name[ON_PIPELINE_ERROR].error is not None

    def test_a_worker_does_not_fire_the_per_command_event(self, project, fired):
        """One digest per container is the fan-out problem `saga notify` solves,
        so the per-command event must stay out of worker mode.
        """
        assert _plan().exit_code == 0
        assert _worker().exit_code == 0

        assert ON_RUN_COMPLETE not in [name for name, _ in fired]

    def test_a_raising_handler_does_not_fail_the_worker(self, project, fired):
        """Firing is new on this path, so a bad handler must not become a new
        way for a worker task to fail — and so mark the whole plan failed.
        """

        def explode(ctx):
            raise RuntimeError("the handler is broken")

        get_hook_registry().register(ON_PIPELINE_COMPLETE, explode)

        assert _plan().exit_code == 0
        assert _worker().exit_code == 0, "a broken handler failed the task"


@pytest.mark.integration
class TestWorkerRegistersHooksFromConfig:
    """The whole chain, as a container would run it.

    The tests above register handlers straight into the registry, which is
    precisely the half that was not broken. `load_hooks()` was called from
    `Session.__init__` alone, so a worker process read nobody's `hooks:` block —
    firing anywhere else would have reached an empty registry and changed
    nothing.
    """

    def test_a_hook_declared_in_saga_project_yml_runs_in_a_worker(
        self, project, monkeypatch
    ):
        from dlt_saga.hooks import loader

        receipt = project / "fired.txt"
        (project / "worker_hook_probe.py").write_text(
            "\n".join(
                [
                    "import pathlib",
                    "",
                    "",
                    "def record(ctx):",
                    f"    pathlib.Path(r{str(receipt)!r}).write_text(",
                    '        ctx.pipeline_name + "|" + ctx.command, encoding="utf-8"',
                    "    )",
                    "",
                ]
            ),
            encoding="utf-8",
        )
        path = project / "saga_project.yml"
        path.write_text(
            path.read_text(encoding="utf-8")
            + "\n".join(
                [
                    "",
                    "hooks:",
                    "  on_pipeline_complete:",
                    "    - worker_hook_probe:record",
                    "",
                ]
            ),
            encoding="utf-8",
        )

        # The parsed project config is cached process-wide, so drop it rather
        # than letting test order decide whether the `hooks:` block is seen.
        _reset_cli_singletons()
        monkeypatch.syspath_prepend(str(project))

        # Loading is guarded by a process-wide flag an earlier test will have
        # set. Restored afterwards: leaving it cleared would make the next test
        # that fires an event re-read whatever project it happens to sit in.
        was_loaded = loader._loaded
        loader._reset_loaded()
        get_hook_registry().clear()

        try:
            assert _plan().exit_code == 0
            assert _worker().exit_code == 0

            assert receipt.exists(), "the worker never registered the project's hook"
            assert receipt.read_text(encoding="utf-8") == "filesystem__sample|ingest"
        finally:
            loader._loaded = was_loaded
