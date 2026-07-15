"""End-to-end: two pipelines resolving to one table fail before any run.

Reproduces the reported footgun — a custom ``naming_module`` whose
``generate_table_name`` returns the same name for several configs, so they all
write to one table. The run must fail at the collision guard, before touching
the warehouse, rather than silently interleaving writers.
"""

import sys
import textwrap

import pytest
from typer.testing import CliRunner

from dlt_saga.cli import app
from dlt_saga.init_command import run_init
from dlt_saga.utility.cli.context import clear_execution_context


def _reset_singletons():
    import dlt_saga.pipeline_config.naming as _naming_mod
    import dlt_saga.utility.cli.common as _common_mod
    import dlt_saga.utility.cli.profiles as _profiles_mod

    _profiles_mod._profiles_config = None
    _common_mod._config_source = None
    _naming_mod._naming_module = None


@pytest.fixture(autouse=True)
def _clean_state():
    _reset_singletons()
    yield
    clear_execution_context()
    _reset_singletons()


def _scaffold_colliding_project(tmp_path):
    run_init(no_input=True)

    # A naming module that collapses every table to one name.
    (tmp_path / "collide_naming.py").write_text(
        'def generate_table_name(segments, environment, *, layer="ingest"):\n'
        '    return "shared_table"\n'
    )

    # Point the project at the colliding naming module.
    project = tmp_path / "saga_project.yml"
    project.write_text(project.read_text() + "\nnaming_module: collide_naming\n")

    # Two distinct configs in the same group — default naming would give them
    # distinct tables, but the naming module forces both to "shared_table".
    cfg_dir = tmp_path / "configs" / "filesystem"
    (cfg_dir / "sample.yml").unlink()  # drop the scaffolded sample
    for name in ("alpha", "beta"):
        (cfg_dir / f"{name}.yml").write_text(
            textwrap.dedent(
                """
                tags: [daily]
                write_disposition: append
                filesystem_type: file
                bucket_name: data
                file_glob: "*.csv"
                file_type: csv
                """
            )
        )


class TestIngestCollisionGuard:
    def test_ingest_fails_on_shared_target(self, tmp_path, monkeypatch, caplog):
        monkeypatch.chdir(tmp_path)
        _scaffold_colliding_project(tmp_path)
        sys.path.insert(0, str(tmp_path))
        try:
            runner = CliRunner()
            with caplog.at_level("ERROR", logger="dlt_saga.cli"):
                result = runner.invoke(app, ["ingest", "--select", "filesystem__*"])

            # The guard blocked the run: non-zero exit, and no warehouse touched
            # (the DuckDB file is never created).
            assert result.exit_code == 1, (
                f"expected exit 1, got {result.exit_code}:\n{result.output}"
            )
            assert not (tmp_path / "local.duckdb").exists()

            # Both colliding pipelines are named in the logged error.
            message = caplog.text
            assert "same destination table" in message
            assert "filesystem__alpha" in message
            assert "filesystem__beta" in message
        finally:
            sys.path.remove(str(tmp_path))
            sys.modules.pop("collide_naming", None)

    def test_doctor_reports_collision(self, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        _scaffold_colliding_project(tmp_path)
        sys.path.insert(0, str(tmp_path))
        try:
            runner = CliRunner()
            result = runner.invoke(app, ["doctor"])

            # doctor exits non-zero when a check fails.
            assert result.exit_code == 1, result.output
            assert "Target collisions" in result.output
            assert "shared_table" in result.output
        finally:
            sys.path.remove(str(tmp_path))
            sys.modules.pop("collide_naming", None)
