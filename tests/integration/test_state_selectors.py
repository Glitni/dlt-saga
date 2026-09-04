"""End-to-end: ``state:new`` and ``state:failed`` against a real DuckDB target.

The unit tests cover the selector semantics against a fake destination; this
pins the parts only a real run exercises — that ``Destination.list_tables``
actually answers for DuckDB, and that a pipeline stops being ``state:new`` once
its table exists.
"""

import pytest
from typer.testing import CliRunner

from dlt_saga.cli import app
from dlt_saga.init_command import run_init
from dlt_saga.utility.cli.context import clear_execution_context


def _reset_cli_singletons():
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


def _invoke(runner, args):
    result = runner.invoke(app, args)
    if result.exception and not isinstance(result.exception, SystemExit):
        import traceback

        raise AssertionError(
            f"saga {' '.join(args)} raised:\n"
            + "".join(
                traceback.format_exception(
                    type(result.exception),
                    result.exception,
                    result.exception.__traceback__,
                )
            )
        )
    return result


class TestStateNew:
    def test_new_before_ingest_and_not_new_after(self, tmp_path, monkeypatch):
        """The scaffolded pipeline is new until its table exists."""
        monkeypatch.chdir(tmp_path)
        run_init(no_input=True)
        runner = CliRunner()

        before = _invoke(runner, ["list", "--select", "state:new"])
        assert before.exit_code == 0
        assert "filesystem__sample" in before.output

        ingest = _invoke(runner, ["ingest", "--select", "state:new"])
        assert ingest.exit_code == 0, ingest.output
        assert (tmp_path / "local.duckdb").exists()

        after = _invoke(runner, ["list", "--select", "state:new"])
        assert after.exit_code == 0
        assert "filesystem__sample" not in after.output

    def test_second_ingest_selects_nothing(self, tmp_path, monkeypatch):
        """Re-running ``--select state:new`` runs nothing once the table exists."""
        import duckdb

        monkeypatch.chdir(tmp_path)
        run_init(no_input=True)
        runner = CliRunner()

        def recorded_runs():
            con = duckdb.connect(str(tmp_path / "local.duckdb"))
            try:
                con.execute("use dlt_dev")
                return con.sql("select count(*) from _saga_execution_plans").fetchone()[
                    0
                ]
            finally:
                con.close()

        assert _invoke(runner, ["ingest", "--select", "state:new"]).exit_code == 0
        after_first = recorded_runs()

        second = _invoke(runner, ["ingest", "--select", "state:new"])

        assert second.exit_code == 0
        # No new run was recorded: the selection was empty, not merely a no-op load.
        assert recorded_runs() == after_first

    def test_intersection_with_tag(self, tmp_path, monkeypatch):
        """``state:`` composes with the config-only selectors."""
        monkeypatch.chdir(tmp_path)
        run_init(no_input=True)
        runner = CliRunner()

        matched = _invoke(runner, ["list", "--select", "tag:daily,state:new"])
        assert matched.exit_code == 0
        assert "filesystem__sample" in matched.output

        # A tag no config carries intersects to nothing, warehouse state or not.
        unmatched = _invoke(runner, ["list", "--select", "tag:nope,state:new"])
        assert unmatched.exit_code == 0
        assert "filesystem__sample" not in unmatched.output


class TestStateFailed:
    def test_successful_run_is_not_failed(self, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        run_init(no_input=True)
        runner = CliRunner()

        assert _invoke(runner, ["ingest", "--select", "state:new"]).exit_code == 0
        result = _invoke(runner, ["list", "--select", "state:failed"])

        assert result.exit_code == 0
        assert "filesystem__sample" not in result.output

    def test_latest_outcome_wins(self, tmp_path, monkeypatch):
        """The append-only log is read latest-row-first, per pipeline.

        A later failure supersedes the completed row the successful run wrote,
        which is what makes ``state:failed`` a retry list rather than a
        historical one.
        """
        import duckdb

        monkeypatch.chdir(tmp_path)
        run_init(no_input=True)
        runner = CliRunner()

        assert _invoke(runner, ["ingest", "--select", "state:new"]).exit_code == 0
        assert (
            "filesystem__sample"
            not in _invoke(runner, ["list", "--select", "state:failed"]).output
        )

        # Append a newer failed outcome for the same pipeline, as a failing run
        # would.
        con = duckdb.connect(str(tmp_path / "local.duckdb"))
        try:
            con.execute("use dlt_dev")
            con.execute(
                """
                INSERT INTO _saga_execution_plans (
                    log_timestamp, execution_id, task_index, pipeline_type,
                    pipeline_identifier, table_name, config_json, status,
                    is_orchestrated
                ) VALUES (
                    CURRENT_TIMESTAMP + INTERVAL 1 MINUTE, 'later-run', 0,
                    'filesystem', 'configs/filesystem/sample.yml',
                    'filesystem__sample', '{}', 'failed', FALSE
                )
                """
            )
        finally:
            con.close()

        result = _invoke(runner, ["list", "--select", "state:failed"])
        assert result.exit_code == 0
        assert "filesystem__sample" in result.output


class TestUnsupported:
    def test_validate_rejects_state_selectors(self, tmp_path, monkeypatch):
        """``saga validate`` is offline, so it refuses rather than connecting."""
        from dlt_saga.utility.cli.pipeline_state import StateSelectorError

        monkeypatch.chdir(tmp_path)
        run_init(no_input=True)

        result = CliRunner().invoke(app, ["validate", "--select", "state:new"])

        assert result.exit_code != 0
        assert isinstance(result.exception, StateSelectorError)

    def test_unknown_state_keyword_is_rejected(self, tmp_path, monkeypatch):
        from dlt_saga.utility.cli.pipeline_state import StateSelectorError

        monkeypatch.chdir(tmp_path)
        run_init(no_input=True)

        result = CliRunner().invoke(app, ["list", "--select", "state:brand-new"])

        assert result.exit_code != 0
        assert isinstance(result.exception, StateSelectorError)
