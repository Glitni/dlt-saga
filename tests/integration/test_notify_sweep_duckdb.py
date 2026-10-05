"""`saga notify` against real recorded state.

The unit tests drive the sweep with hand-built rows. These run actual pipelines
through the CLI so the rows come from `record_local_run`, then sweep them — which
is what proves the notifier reads what saga genuinely writes, including the
`notified_at` column being created on a table that predates it.
"""

import logging
from contextlib import contextmanager
from unittest.mock import patch

import duckdb
import pytest
from typer.testing import CliRunner

from dlt_saga.cli import app
from dlt_saga.init_command import run_init
from dlt_saga.utility.cli.context import clear_execution_context
from dlt_saga.utility.orchestration.execution_plan import ExecutionPlanManager


def _reset_cli_singletons():
    import dlt_saga.project_config as _project_mod
    import dlt_saga.utility.cli.common as _common_mod
    import dlt_saga.utility.cli.profiles as _profiles_mod

    _profiles_mod._profiles_config = None
    _common_mod._config_source = None
    # Cached per process, so one project's saga_project.yml would otherwise be
    # read for the next test's tmp_path project.
    _project_mod._project_config = None


@pytest.fixture
def project(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    _reset_cli_singletons()
    run_init(no_input=True)
    yield tmp_path
    clear_execution_context()
    _reset_cli_singletons()


def _ingest(select="filesystem__sample"):
    return CliRunner().invoke(app, ["ingest", "--select", select])


def _configure_slack(project):
    """Give the project a notifier so a sweep has somewhere to deliver.

    Without one `saga notify` reports nothing and claims nothing — correctly, but
    it means the delivered/not-delivered paths go untested.
    """
    path = project / "saga_project.yml"
    block = "\n".join(
        ["", "notifications:", "  slack:", "    webhook_url: env_secret::TEST_HOOK", ""]
    )
    path.write_text(path.read_text(encoding="utf-8") + block, encoding="utf-8")

    import dlt_saga.project_config as _project_mod

    _project_mod._project_config = None


@contextmanager
def _slack(succeeds=True):
    """Intercept the webhook post, so nothing leaves the machine."""
    sent: list = []

    def sender(url, payload, timeout):
        if not succeeds:
            raise RuntimeError("slack is down")
        sent.append(payload)

    with patch("dlt_saga.hooks.notifiers.slack._post_to_slack", side_effect=sender):
        with patch(
            "dlt_saga.utility.secrets.resolve_secret", return_value="https://hook"
        ):
            yield sent


def _notify(caplog, *args):
    """Run `saga notify`, returning its log text.

    The command reports through the logger, not stdout, and this project does
    not assert on rendered CLI output.
    """
    caplog.clear()
    with caplog.at_level(logging.INFO):
        result = CliRunner().invoke(app, ["notify", *args])
    assert result.exit_code == 0, result.output
    return caplog.text


def _fix_the_source(project):
    """Restore a working config so the pipeline loads again.

    `run_init` will not overwrite an existing config file, so the restore has to
    be explicit.
    """
    _write_config(
        project,
        [
            "tags: [daily]",
            "write_disposition: replace",
            "",
            "filesystem_type: file",
            "bucket_name: data",
            'file_glob: "*.csv"',
            "file_type: csv",
        ],
    )


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


def _executions(project):
    conn = duckdb.connect(str(project / "local.duckdb"))
    try:
        conn.execute("use dlt_dev")
        return conn.sql(
            "select execution_id, notified_at from _saga_executions order by created_at"
        ).fetchall()
    finally:
        conn.close()


@pytest.mark.integration
class TestSweepOverRealState:
    def test_clean_run_reports_nothing_and_claims_nothing(self, project, caplog):
        assert _ingest().exit_code == 0

        text = _notify(caplog, "--dry-run")

        assert "Nothing to report" in text
        assert all(row[1] is None for row in _executions(project))

    def test_failed_run_is_reported_and_claimed(self, project, caplog):
        _configure_slack(project)
        _break_the_source(project)
        assert _ingest().exit_code != 0

        with _slack() as sent:
            _notify(caplog)

        assert len(sent) == 1, "nothing was delivered"
        rows = _executions(project)
        assert rows, "the failed run was never recorded"
        assert all(row[1] is not None for row in rows), "nothing was claimed"

    def test_a_second_sweep_does_not_repeat_itself(self, project, caplog):
        """The predicate is 'unreported', so overlapping schedules are safe."""
        _configure_slack(project)
        _break_the_source(project)
        _ingest()
        with _slack():
            assert "Reporting 1 failing" in _notify(caplog)

        with _slack():
            assert "Nothing to report" in _notify(caplog)

    def test_force_reports_an_already_claimed_execution(self, project, caplog):
        _configure_slack(project)
        _break_the_source(project)
        _ingest()
        with _slack():
            _notify(caplog)

        with _slack():
            assert "Reporting 1 failing" in _notify(caplog, "--force")

    def test_dry_run_shows_the_digest(self, project, caplog):
        """A preview that showed only a count would not let you check what is
        about to reach a channel.
        """
        _break_the_source(project)
        _ingest()

        text = _notify(caplog, "--dry-run")

        assert "Digest that would be sent" in text
        assert "failing:" in text
        assert "filesystem__sample" in text

    def test_dry_run_sends_nothing_and_claims_nothing(self, project, caplog):
        _break_the_source(project)
        _ingest()

        assert "Dry run" in _notify(caplog, "--dry-run")
        assert all(row[1] is None for row in _executions(project))
        # ...and the run is therefore still pending for a real sweep.
        with _slack():
            assert "Reporting 1 failing" in _notify(caplog)

    def test_recovery_is_reported_after_the_failure_was_claimed(self, project, caplog):
        """The failure and the recovery land in different sweeps, which is the
        normal case for a scheduled notifier.
        """
        _configure_slack(project)
        _break_the_source(project)
        _ingest()
        with _slack():
            _notify(caplog)

        _fix_the_source(project)
        assert _ingest().exit_code == 0

        # The recovered run had no failures of its own, so there is nothing to
        # say — the failure was already reported in the previous sweep.
        assert "Nothing to report" in _notify(caplog)


@pytest.mark.integration
class TestNotifiedAtColumn:
    def test_column_is_added_to_a_table_that_predates_it(self, project, caplog):
        """Existing deployments have `_saga_executions` without the column."""
        _ingest()

        conn = duckdb.connect(str(project / "local.duckdb"))
        try:
            conn.execute("use dlt_dev")
            conn.execute("ALTER TABLE _saga_executions DROP COLUMN notified_at")
            columns = {r[0] for r in conn.sql("describe _saga_executions").fetchall()}
            assert "notified_at" not in columns
        finally:
            conn.close()

        _notify(caplog, "--dry-run")

        conn = duckdb.connect(str(project / "local.duckdb"))
        try:
            conn.execute("use dlt_dev")
            columns = {r[0] for r in conn.sql("describe _saga_executions").fetchall()}
        finally:
            conn.close()

        assert "notified_at" in columns


@pytest.mark.integration
class TestSurvivesMaintenance:
    """`saga maintenance` compacts the same log the sweep reads.

    If compaction could remove a terminal row before it had been reported, or
    clear the claim on one that had, alerts would go missing — so the
    interaction is pinned rather than assumed from the compactor's docstring.
    """

    def _maintenance(self, caplog):
        with caplog.at_level(logging.INFO):
            result = CliRunner().invoke(app, ["maintenance"])
        assert result.exit_code == 0, result.output

    def test_unreported_failure_survives_compaction(self, project, caplog):
        _break_the_source(project)
        _ingest()

        self._maintenance(caplog)

        # Still reportable afterwards: compaction kept the terminal row.
        with _slack():
            assert "Reporting 1 failing" in _notify(caplog)

    def test_the_claim_survives_compaction(self, project, caplog):
        _configure_slack(project)
        _break_the_source(project)
        _ingest()
        with _slack():
            _notify(caplog)
        assert all(row[1] is not None for row in _executions(project))

        self._maintenance(caplog)

        # Still claimed, so the next sweep does not repeat the digest.
        assert all(row[1] is not None for row in _executions(project))
        with _slack():
            assert "Nothing to report" in _notify(caplog)


@pytest.mark.integration
class TestNothingIsClaimedWithoutDelivery:
    """A claim is only honest if the digest actually went out.

    `notified_at` is the sweep's only memory, so claiming an undelivered digest
    turns one Slack outage — or one sweep scheduled before a notifier was
    configured — into permanently lost alerts: those executions never re-enter
    a later sweep.
    """

    def test_a_failed_send_leaves_the_execution_unclaimed(self, project, caplog):
        _configure_slack(project)
        _break_the_source(project)
        _ingest()

        with _slack(succeeds=False):
            text = _notify(caplog)

        assert "stay unreported" in text
        assert all(row[1] is None for row in _executions(project))

    def test_a_failed_send_is_retried_by_the_next_sweep(self, project, caplog):
        _configure_slack(project)
        _break_the_source(project)
        _ingest()
        with _slack(succeeds=False):
            _notify(caplog)

        with _slack() as sent:
            assert "Reporting 1 failing" in _notify(caplog)

        assert len(sent) == 1
        assert all(row[1] is not None for row in _executions(project))

    def test_an_unconfigured_project_claims_nothing(self, project, caplog):
        """No notifier at all is the same case: nowhere to deliver."""
        _break_the_source(project)
        _ingest()

        text = _notify(caplog)

        assert "no notifications.slack configured" in text
        assert all(row[1] is None for row in _executions(project))


@pytest.mark.integration
class TestTheSweepDoesNotBootstrap:
    """A sweep reads; it should not pay the write path's DDL to do so.

    `ensure_table_exists()` is two `CREATE TABLE IF NOT EXISTS`, the column
    backfill and a view replace — on BigQuery that is several query jobs at
    roughly a second each, against the single job the sweep itself needs, and
    it asks a read-only notifier for DDL rights it has no other use for.
    """

    def test_a_readable_schema_is_swept_without_any_ddl(self, project, caplog):
        _configure_slack(project)
        _break_the_source(project)
        _ingest()

        target = "dlt_saga.utility.orchestration.execution_plan.ExecutionPlanManager"
        with patch(f"{target}.ensure_table_exists") as bootstrap:
            with _slack() as sent:
                assert "Reporting 1 failing" in _notify(caplog)

        bootstrap.assert_not_called()
        assert len(sent) == 1, "the sweep did not actually read anything"

    def test_a_schema_with_no_history_still_reports_nothing(self, project, caplog):
        """Nothing has ever run here, so the first read fails and the fallback
        creates the tables rather than the command erroring.
        """
        target = "dlt_saga.utility.orchestration.execution_plan.ExecutionPlanManager"
        real = ExecutionPlanManager.ensure_table_exists
        with patch(
            f"{target}.ensure_table_exists", autospec=True, side_effect=real
        ) as bootstrap:
            text = _notify(caplog)

        assert bootstrap.call_count == 1, "the fallback never fired"
        assert "Nothing to report" in text

    def test_a_read_only_notifier_is_told_what_it_could_not_read(self, project):
        """The fallback must not mask the error that triggered it.

        A notifier granted read-only access fails the bootstrap as a matter of
        course, and surfacing "cannot CREATE TABLE" would send you after the
        wrong permission instead of the one actually missing.
        """
        target = "dlt_saga.utility.orchestration.execution_plan.ExecutionPlanManager"
        with patch(f"{target}.ensure_table_exists", side_effect=RuntimeError("no DDL")):
            result = CliRunner().invoke(app, ["notify"])

        assert result.exit_code != 0
        assert "no DDL" not in str(result.exception)
