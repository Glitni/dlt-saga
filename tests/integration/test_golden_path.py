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

    def test_generate_schemas_writes_and_links(self, tmp_path, monkeypatch):
        """`saga generate-schemas` writes schema files and links the sample
        config to its adapter's schema via a yaml-language-server modeline."""
        monkeypatch.chdir(tmp_path)
        run_init(no_input=True)

        runner = CliRunner()
        result = runner.invoke(app, ["generate-schemas"])
        assert result.exit_code == 0, (
            f"saga generate-schemas exited {result.exit_code}:\n{result.output}"
        )

        # Schema files were produced.
        assert (tmp_path / "schemas" / "filesystem_config.json").exists()

        # The sample config points at its adapter's schema.
        sample = (tmp_path / "configs" / "filesystem" / "sample.yml").read_text(
            encoding="utf-8"
        )
        assert (
            sample.splitlines()[0]
            == "# yaml-language-server: $schema=../../schemas/filesystem_config.json"
        )

    def test_generate_schemas_no_link_leaves_configs_untouched(
        self, tmp_path, monkeypatch
    ):
        """--no-link writes schemas but does not modify config files."""
        monkeypatch.chdir(tmp_path)
        run_init(no_input=True)
        sample_path = tmp_path / "configs" / "filesystem" / "sample.yml"
        # Strip the scaffolded modeline so we can prove --no-link doesn't re-add it.
        body = "\n".join(
            line
            for line in sample_path.read_text(encoding="utf-8").splitlines()
            if "yaml-language-server" not in line
        )
        sample_path.write_text(body + "\n", encoding="utf-8")

        runner = CliRunner()
        result = runner.invoke(app, ["generate-schemas", "--no-link"])
        assert result.exit_code == 0, result.output

        assert (tmp_path / "schemas" / "filesystem_config.json").exists()
        assert "yaml-language-server" not in sample_path.read_text(encoding="utf-8")

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
