"""`saga report --summary` against real recorded state.

The unit tests drive the verdicts with hand-built rows. These run actual
pipelines through the CLI so the plan rows come from `record_local_run`, then
generate the report — which proves the summary reads what saga genuinely
writes, through the report's own plans query.
"""

import json

import pytest
from typer.testing import CliRunner

from dlt_saga.cli import app
from dlt_saga.init_command import run_init
from dlt_saga.utility.cli.context import clear_execution_context


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


def _write_config(project, lines):
    (project / "configs" / "filesystem" / "sample.yml").write_text(
        "\n".join([*lines, ""]), encoding="utf-8"
    )


def _break_the_source(project):
    """Point the glob at nothing and require a row, so the run fails."""
    _write_config(
        project,
        [
            "tags: [daily]",
            "write_disposition: replace",
            "min_rows: 1",
            "",
            "filesystem_type: file",
            "bucket_name: data",
            'file_glob: "gone/*.csv"',
            "file_type: csv",
        ],
    )


def _ingest():
    return CliRunner().invoke(app, ["ingest", "--select", "filesystem__sample"])


def _report(*args):
    result = CliRunner().invoke(
        app, ["report", "--no-open", "--output", "r.html", *args]
    )
    assert result.exit_code == 0, result.output


def _summary(project):
    return json.loads((project / "r.json").read_text(encoding="utf-8"))


def _sample(summary):
    (pipeline,) = [
        p
        for p in summary["needs_attention"]
        if p["pipeline_name"] == "filesystem__sample"
    ]
    return pipeline


@pytest.mark.integration
class TestSummaryOverRealState:
    def test_no_summary_without_the_flag(self, project):
        assert _ingest().exit_code == 0
        _report()
        assert (project / "r.html").exists()
        assert not (project / "r.json").exists()

    def test_successful_run_is_ok(self, project):
        assert _ingest().exit_code == 0
        _report("--summary", "--stale-after-minutes", "90")

        summary = _summary(project)
        assert summary["complete"] is True
        assert summary["stale_after_minutes"] == 90
        assert summary["report_file"] == "r.html"
        assert summary["counts"]["ok"] >= 1
        assert not [
            p
            for p in summary["needs_attention"]
            if p["pipeline_name"] == "filesystem__sample"
        ], "a healthy pipeline is counted, not listed"

    def test_failed_run_is_failing_with_its_error(self, project):
        assert _ingest().exit_code == 0
        _break_the_source(project)
        assert _ingest().exit_code != 0
        _report("--summary")

        pipeline = _sample(_summary(project))
        assert pipeline["state"] == "failing"
        assert pipeline["consecutive_failures"] == 1
        assert pipeline["last_success_at"] is not None
        assert pipeline["last_error"]

    def test_report_page_headline_matches_the_summary(self, project):
        _break_the_source(project)
        _ingest()
        _report("--summary")

        html = (project / "r.html").read_text(encoding="utf-8")
        marker = "const REPORT_DATA = "
        start = html.index(marker) + len(marker)
        embedded = json.loads(html[start : html.index(";\n", start)])
        page_failing = {
            p["pipeline_name"]
            for p in embedded["status"]["pipelines"]
            if p["state"] == "failing"
        }
        summary = _summary(project)
        summary_failing = {
            p["pipeline_name"]
            for p in summary["needs_attention"]
            if p["state"] == "failing"
        }
        assert page_failing == summary_failing == {"filesystem__sample"}
        assert summary["counts"]["failing"] == 1
