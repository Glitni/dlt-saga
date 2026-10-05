"""`on_run_complete` fires once per command invocation, against a real run.

The notifier's unit tests build a RunContext by hand, so they prove the message
but not the wiring. These run the CLI against DuckDB and assert the event fires
exactly once with the outcomes attached — the property the run digest depends
on, and the one that breaks quietly if the hook is ever fired per phase or per
pipeline instead.
"""

import pytest
from typer.testing import CliRunner

from dlt_saga.cli import app
from dlt_saga.hooks.registry import ON_RUN_COMPLETE, get_hook_registry
from dlt_saga.init_command import run_init
from dlt_saga.utility.cli.context import clear_execution_context


def _reset_cli_singletons():
    import dlt_saga.utility.cli.common as _common_mod
    import dlt_saga.utility.cli.profiles as _profiles_mod

    _profiles_mod._profiles_config = None
    _common_mod._config_source = None


@pytest.fixture
def captured_runs():
    """Capture every RunContext fired during the test."""
    registry = get_hook_registry()
    registry.clear()
    _reset_cli_singletons()

    captured = []
    registry.register(ON_RUN_COMPLETE, captured.append)
    yield captured

    registry.clear()
    clear_execution_context()
    _reset_cli_singletons()


@pytest.mark.integration
class TestRunCompleteHook:
    def test_ingest_fires_once_with_outcomes(
        self, tmp_path, monkeypatch, captured_runs
    ):
        monkeypatch.chdir(tmp_path)
        run_init(no_input=True)

        result = CliRunner().invoke(app, ["ingest", "--select", "filesystem__sample"])
        assert result.exit_code == 0, result.output

        assert len(captured_runs) == 1
        ctx = captured_runs[0]
        assert ctx.command == "ingest"
        assert ctx.select == ["filesystem__sample"]
        assert ctx.succeeded == 1
        assert ctx.failed == 0
        assert not ctx.has_failures
        assert ctx.duration_seconds is not None

    def test_combined_run_fires_once_not_once_per_phase(
        self, tmp_path, monkeypatch, captured_runs
    ):
        """`saga run` is two session calls; a digest per phase would double-post."""
        monkeypatch.chdir(tmp_path)
        run_init(no_input=True)

        # Both layers enabled, so ingest *and* historize actually run — with the
        # scaffolded `append` config only one phase would execute and the
        # coalescing would go untested.
        (tmp_path / "configs" / "filesystem" / "sample.yml").write_text(
            "tags: [daily]\n"
            "write_disposition: append+historize\n"
            "primary_key: [id]\n"
            "\n"
            "filesystem_type: file\n"
            "bucket_name: data\n"
            'file_glob: "*.csv"\n'
            "file_type: csv\n",
            encoding="utf-8",
        )

        result = CliRunner().invoke(app, ["run", "--select", "filesystem__sample"])
        assert result.exit_code == 0, result.output

        assert len(captured_runs) == 1, (
            f"expected one digest for one command, got {len(captured_runs)}"
        )
        ctx = captured_runs[0]
        assert ctx.command == "run"
        # One result per phase, both in the single event.
        assert len(ctx.results) == 2
        assert ctx.succeeded == 2

    def test_failed_run_carries_the_failure(self, tmp_path, monkeypatch, captured_runs):
        monkeypatch.chdir(tmp_path)
        run_init(no_input=True)

        # A guard that cannot be satisfied — the cheapest way to fail a real run.
        config = tmp_path / "configs" / "filesystem" / "sample.yml"
        config.write_text(
            "tags: [daily]\n"
            "write_disposition: replace\n"
            "min_rows: 999999\n"
            "\n"
            "filesystem_type: file\n"
            "bucket_name: data\n"
            'file_glob: "*.csv"\n'
            "file_type: csv\n",
            encoding="utf-8",
        )

        result = CliRunner().invoke(app, ["ingest", "--select", "filesystem__sample"])
        assert result.exit_code != 0

        assert len(captured_runs) == 1
        ctx = captured_runs[0]
        assert ctx.has_failures
        assert ctx.failed == 1
        assert "min_rows=999999" in ctx.failures[0].error

    def test_selection_matching_nothing_still_fires(
        self, tmp_path, monkeypatch, captured_runs
    ):
        """A scheduled run whose selector stopped matching is worth hearing about."""
        monkeypatch.chdir(tmp_path)
        run_init(no_input=True)

        CliRunner().invoke(app, ["ingest", "--select", "tag:does-not-exist"])

        assert len(captured_runs) == 1
        assert captured_runs[0].results == []


@pytest.fixture
def captured_pipeline_events():
    """Capture the three per-pipeline events fired by a local run."""
    from dlt_saga.hooks.registry import (
        ON_PIPELINE_COMPLETE,
        ON_PIPELINE_ERROR,
        ON_PIPELINE_START,
    )

    registry = get_hook_registry()
    registry.clear()
    _reset_cli_singletons()

    events = []
    for name in (ON_PIPELINE_START, ON_PIPELINE_COMPLETE, ON_PIPELINE_ERROR):
        registry.register(name, lambda ctx, n=name: events.append((n, ctx)))
    yield events

    registry.clear()
    clear_execution_context()
    _reset_cli_singletons()


@pytest.mark.integration
class TestSessionFiresPipelineHooks:
    """The local path's per-pipeline events, pinned against a real run.

    These had no coverage: every assertion about them was about *where* the
    `registry.fire(...)` calls appeared in source. The call sites have since
    been routed through shared helpers so the worker path cannot drift from
    this one, which is a refactor of working code — so the behaviour it
    preserves needs stating as behaviour.
    """

    def test_a_local_ingest_fires_start_then_complete(
        self, tmp_path, monkeypatch, captured_pipeline_events
    ):
        monkeypatch.chdir(tmp_path)
        run_init(no_input=True)

        result = CliRunner().invoke(app, ["ingest", "--select", "filesystem__sample"])
        assert result.exit_code == 0, result.output

        assert [name for name, _ in captured_pipeline_events] == [
            "on_pipeline_start",
            "on_pipeline_complete",
        ]
        start = captured_pipeline_events[0][1]
        assert start.pipeline_name == "filesystem__sample"
        assert start.command == "ingest"
        assert start.config is not None
        assert captured_pipeline_events[1][1].result is not None

    def test_a_local_failure_fires_the_error_event_with_the_exception(
        self, tmp_path, monkeypatch, captured_pipeline_events
    ):
        monkeypatch.chdir(tmp_path)
        run_init(no_input=True)
        (tmp_path / "configs" / "filesystem" / "sample.yml").write_text(
            "tags: [daily]\n"
            "write_disposition: replace\n"
            "min_rows: 1\n"
            "\n"
            "filesystem_type: file\n"
            "bucket_name: data\n"
            'file_glob: "gone/*.csv"\n'
            "file_type: csv\n",
            encoding="utf-8",
        )

        assert (
            CliRunner()
            .invoke(app, ["ingest", "--select", "filesystem__sample"])
            .exit_code
            != 0
        )

        fired = dict(captured_pipeline_events)
        assert "on_pipeline_error" in fired
        assert "on_pipeline_complete" not in fired
        assert isinstance(fired["on_pipeline_error"].error, Exception)

    def test_a_local_historize_fires_under_its_own_command(
        self, tmp_path, monkeypatch, captured_pipeline_events
    ):
        """`command` distinguishes the two layers, and the historize path has
        its own fire sites — including one on a branch that never raises.
        """
        monkeypatch.chdir(tmp_path)
        run_init(no_input=True)
        (tmp_path / "configs" / "filesystem" / "sample.yml").write_text(
            "tags: [daily]\n"
            "write_disposition: append+historize\n"
            "primary_key: [id]\n"
            "\n"
            "filesystem_type: file\n"
            "bucket_name: data\n"
            'file_glob: "*.csv"\n'
            "file_type: csv\n",
            encoding="utf-8",
        )

        result = CliRunner().invoke(app, ["run", "--select", "filesystem__sample"])
        assert result.exit_code == 0, result.output

        commands = [ctx.command for _, ctx in captured_pipeline_events]
        assert commands.count("ingest") == 2, "start + complete for the ingest phase"
        assert commands.count("historize") == 2, "start + complete for historize"
