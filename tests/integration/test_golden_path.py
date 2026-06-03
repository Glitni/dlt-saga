"""Golden-path integration test: init → list → ingest with DuckDB.

Exercises the full CLI surface end-to-end in a temporary directory so that
regressions in project scaffolding or the filesystem pipeline show up here.
"""

import pytest
from typer.testing import CliRunner

from dlt_saga.cli import app
from dlt_saga.init_command import run_init
from dlt_saga.utility.cli.context import clear_execution_context


def _reset_cli_singletons():
    """Reset lazily-initialised global singletons so each test starts clean."""
    import dlt_saga.utility.cli.common as _common_mod
    import dlt_saga.utility.cli.profiles as _profiles_mod

    _profiles_mod._profiles_config = None
    _common_mod._config_source = None


@pytest.fixture(autouse=True)
def _clean_state():
    _reset_cli_singletons()
    yield
    clear_execution_context()
    _reset_cli_singletons()


class TestGoldenPath:
    """End-to-end: scaffold a DuckDB project then list and ingest."""

    def test_list_discovers_sample_pipeline(self, tmp_path, monkeypatch):
        """After init, saga list shows the sample filesystem pipeline."""
        monkeypatch.chdir(tmp_path)
        run_init(no_input=True)

        runner = CliRunner()
        result = runner.invoke(app, ["list"])
        assert result.exit_code == 0, (
            f"saga list exited {result.exit_code}:\n{result.output}"
        )
        assert "filesystem__sample" in result.output

    def test_ingest_sample_pipeline_succeeds(self, tmp_path, monkeypatch):
        """After init, saga ingest loads the sample CSV into DuckDB without errors."""
        monkeypatch.chdir(tmp_path)
        run_init(no_input=True)

        runner = CliRunner()
        result = runner.invoke(app, ["ingest", "--select", "filesystem__sample"])

        exc_info = ""
        if result.exception:
            import traceback

            exc_info = "\n" + "".join(
                traceback.format_exception(
                    type(result.exception),
                    result.exception,
                    result.exception.__traceback__,
                )
            )

        assert result.exit_code == 0, (
            f"saga ingest exited {result.exit_code}:\n{result.output}{exc_info}"
        )
        assert (tmp_path / "local.duckdb").exists(), (
            "DuckDB file not created after ingest"
        )

    def test_plan_dry_run_emits_pretty_json_by_default(self, tmp_path, monkeypatch):
        """`saga plan --dry-run` pretty-prints JSON with indent=2, so a
        human running it interactively gets a readable block."""
        monkeypatch.chdir(tmp_path)
        run_init(no_input=True)

        runner = CliRunner()
        result = runner.invoke(
            app, ["plan", "--dry-run", "--select", "filesystem__sample"]
        )
        assert result.exit_code == 0, (
            f"saga plan exited {result.exit_code}:\n{result.output}"
        )
        # Pretty-printed: opening brace on its own line, then indented keys.
        assert "{\n  " in result.output

    def test_plan_dry_run_with_compact_emits_single_line(self, tmp_path, monkeypatch):
        """`saga plan --dry-run --compact` collapses to a single line — the
        shape an automation harness reading Cloud Logging entry-by-entry
        wants (one log entry = the whole JSON, no interleaving)."""
        monkeypatch.chdir(tmp_path)
        run_init(no_input=True)

        runner = CliRunner()
        result = runner.invoke(
            app,
            ["plan", "--dry-run", "--compact", "--select", "filesystem__sample"],
        )
        assert result.exit_code == 0, (
            f"saga plan exited {result.exit_code}:\n{result.output}"
        )
        # Locate the JSON line (other log output may precede it).
        import json as _json

        json_lines = [
            line
            for line in result.output.splitlines()
            if line.startswith("{") and line.endswith("}")
        ]
        assert len(json_lines) == 1, (
            f"Expected exactly one single-line JSON object, got:\n{result.output}"
        )
        # Parses and carries the expected dry-run keys.
        parsed = _json.loads(json_lines[0])
        assert parsed["dry_run"] is True
        assert "task_count" in parsed
