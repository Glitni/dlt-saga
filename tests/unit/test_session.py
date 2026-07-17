"""Tests for the Session programmatic API."""

import logging
from contextlib import ExitStack
from unittest.mock import MagicMock, patch

import pytest

from dlt_saga.project_config import ConfigSourceConfig
from dlt_saga.session import PipelineResult, Session, SessionResult
from dlt_saga.utility.auth.providers import AuthenticationError

# ---------------------------------------------------------------------------
# SessionResult tests
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestSessionResult:
    """Tests for SessionResult dataclass properties."""

    def test_empty_result(self):
        result = SessionResult()
        assert result.succeeded == 0
        assert result.failed == 0
        assert not result.has_failures
        assert result.failures == []

    def test_all_succeeded(self):
        result = SessionResult(
            pipeline_results=[
                PipelineResult(pipeline_name="a", success=True),
                PipelineResult(pipeline_name="b", success=True),
            ]
        )
        assert result.succeeded == 2
        assert result.failed == 0
        assert not result.has_failures

    def test_mixed_results(self):
        result = SessionResult(
            pipeline_results=[
                PipelineResult(pipeline_name="a", success=True),
                PipelineResult(pipeline_name="b", success=False, error="broken"),
                PipelineResult(pipeline_name="c", success=True),
            ]
        )
        assert result.succeeded == 2
        assert result.failed == 1
        assert result.has_failures
        assert len(result.failures) == 1
        assert result.failures[0].pipeline_name == "b"
        assert result.failures[0].error == "broken"

    def test_all_failed(self):
        result = SessionResult(
            pipeline_results=[
                PipelineResult(pipeline_name="a", success=False, error="e1"),
                PipelineResult(pipeline_name="b", success=False, error="e2"),
            ]
        )
        assert result.succeeded == 0
        assert result.failed == 2
        assert result.has_failures

    def test_repr(self):
        result = SessionResult(
            pipeline_results=[
                PipelineResult(pipeline_name="a", success=True),
                PipelineResult(pipeline_name="b", success=False),
            ]
        )
        assert repr(result) == "SessionResult(succeeded=1, failed=1)"


# ---------------------------------------------------------------------------
# PipelineResult tests
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestPipelineResult:
    def test_success_result(self):
        r = PipelineResult(pipeline_name="test", success=True, load_info={"rows": 5})
        assert r.success
        assert r.error is None
        assert r.load_info == {"rows": 5}

    def test_failure_result(self):
        r = PipelineResult(pipeline_name="test", success=False, error="timeout")
        assert not r.success
        assert r.error == "timeout"
        assert r.load_info is None

    def test_config_error_defaults_false(self):
        assert PipelineResult(pipeline_name="t", success=True).config_error is False


# ---------------------------------------------------------------------------
# Config/validation errors are developer feedback, not run outcomes
# ---------------------------------------------------------------------------


def _config(pipeline_name="p"):
    cfg = MagicMock()
    cfg.pipeline_group = "grp"
    cfg.identifier = f"configs/{pipeline_name}.yml"
    cfg.table_name = "tbl"
    return cfg


@pytest.mark.unit
class TestFailureDisplaySingleSourced:
    """Per-pipeline failures are displayed once, by the CLI end-of-run summary
    (`_exit_if_failures`). The session workers no longer re-log the same error
    inline — that printed it twice (once inline, once in the summary). Genuine
    failures still get a single inline traceback for real-time debugging; config
    errors get no inline log, since the summary already surfaces them cleanly."""

    def _cfg(self, name="grp__p"):
        cfg = _config(name)
        cfg.pipeline_name = name
        return cfg

    def _session_errors(self, caplog):
        return [r for r in caplog.records if r.name == "dlt_saga.session"]

    def test_ingest_config_error_not_logged_inline(self, caplog):
        cfg = self._cfg()
        with (
            patch(
                "dlt_saga.session.execute_pipeline",
                side_effect=ValueError("bad config"),
            ),
            caplog.at_level(logging.ERROR, logger="dlt_saga.session"),
        ):
            result = Session._execute_single_ingest(cfg, "")
        assert result.success is False
        assert result.config_error is True
        assert self._session_errors(caplog) == []

    def test_ingest_genuine_error_logged_once_with_traceback(self, caplog):
        cfg = self._cfg()
        with (
            patch(
                "dlt_saga.session.execute_pipeline",
                side_effect=RuntimeError("boom"),
            ),
            caplog.at_level(logging.ERROR, logger="dlt_saga.session"),
        ):
            result = Session._execute_single_ingest(cfg, "")
        assert result.success is False
        assert result.config_error is False
        errors = self._session_errors(caplog)
        assert len(errors) == 1
        assert errors[0].exc_info is not None  # inline traceback for debugging

    def test_historize_failure_not_logged_inline(self, caplog):
        cfg = self._cfg()
        runner = MagicMock()
        runner.run.return_value = {
            "status": "failed",
            "error": "Historization config changed: track_deletions",
            "config_error": True,
        }
        with (
            patch(
                "dlt_saga.historize.factory.build_historize_runner",
                return_value=runner,
            ),
            caplog.at_level(logging.ERROR, logger="dlt_saga.session"),
        ):
            result = Session._execute_single_historize(cfg, full_refresh=False)
        assert result.success is False
        assert result.config_error is True
        assert "config changed" in result.error
        # The runner owns the genuine-failure traceback; the summary shows every
        # failure — so the historize worker logs nothing inline.
        assert self._session_errors(caplog) == []


@pytest.mark.unit
class TestRecordRunSkipsConfigErrors:
    """A pre-run config/validation error never started a run, so `_record_run`
    must not record it in `saga report` telemetry — and must not create a
    destination/connection just to record nothing (which is what hangs on a
    cold Databricks warehouse)."""

    def _session(self):
        session = object.__new__(Session)
        session._profile_target = None
        return session

    def test_config_error_only_creates_no_destination(self):
        session = self._session()
        result = SessionResult(
            pipeline_results=[
                PipelineResult(
                    pipeline_name="p",
                    success=False,
                    error="historize requires a primary_key",
                    config=_config("p"),
                    config_error=True,
                )
            ]
        )
        with patch(
            "dlt_saga.destinations.factory.DestinationFactory.create_from_context"
        ) as create:
            session._record_run("historize", None, result)
        create.assert_not_called()

    def test_real_failure_is_recorded_config_error_skipped(self):
        session = self._session()
        result = SessionResult(
            pipeline_results=[
                PipelineResult(
                    pipeline_name="cfg",
                    success=False,
                    error="requires a primary_key",
                    config=_config("cfg"),
                    config_error=True,
                ),
                PipelineResult(
                    pipeline_name="real",
                    success=False,
                    error="connection reset",
                    config=_config("real"),
                    config_error=False,
                ),
            ]
        )
        with (
            patch(
                "dlt_saga.destinations.factory.DestinationFactory.create_from_context"
            ) as create,
            patch(
                "dlt_saga.utility.orchestration.execution_plan.ExecutionPlanManager"
            ) as manager,
            patch("dlt_saga.utility.cli.context.get_execution_context") as ctx,
            patch(
                "dlt_saga.utility.naming.get_execution_plan_schema",
                return_value="dlt_x",
            ),
        ):
            ctx.return_value = MagicMock()
            session._record_run("run", None, result)

        create.assert_called_once()
        records = manager.return_value.record_local_run.call_args.args[0]
        recorded_names = {rec["pipeline_identifier"] for rec in records}
        assert recorded_names == {"configs/real.yml"}  # config-error one excluded

    def test_backfill_window_forwarded_to_metadata(self):
        """Local runs record the --start/end-value-override backfill window."""
        session = self._session()
        result = SessionResult(
            pipeline_results=[
                PipelineResult(
                    pipeline_name="real",
                    success=True,
                    error=None,
                    config=_config("real"),
                    config_error=False,
                )
            ]
        )
        with (
            patch(
                "dlt_saga.destinations.factory.DestinationFactory.create_from_context"
            ),
            patch(
                "dlt_saga.utility.orchestration.execution_plan.ExecutionPlanManager"
            ) as manager,
            patch("dlt_saga.utility.cli.context.get_execution_context") as ctx,
            patch(
                "dlt_saga.utility.naming.get_execution_plan_schema",
                return_value="dlt_x",
            ),
        ):
            context = MagicMock()
            context.start_value_override = "2026-02-20"
            context.end_value_override = "2026-02-26"
            ctx.return_value = context
            session._record_run("ingest", ["cookiebot*"], result)

        metadata = manager.return_value.record_local_run.call_args.kwargs["metadata"]
        assert metadata.start_value_override == "2026-02-20"
        assert metadata.end_value_override == "2026-02-26"
        assert metadata.select_criteria == "cookiebot*"


@pytest.mark.unit
class TestConfigErrorClassification:
    """ValueError (config/validation) marks config_error=True (run never
    started); any other exception is a genuine run failure."""

    def test_ingest_valueerror_flags_config_error(self):
        with patch(
            "dlt_saga.session.execute_pipeline",
            side_effect=ValueError("missing required field"),
        ):
            r = Session._execute_single_ingest(_config("p"))
        assert r.success is False
        assert r.config_error is True

    def test_ingest_other_exception_is_run_failure(self):
        with patch(
            "dlt_saga.session.execute_pipeline",
            side_effect=RuntimeError("network reset mid-load"),
        ):
            r = Session._execute_single_ingest(_config("p"))
        assert r.success is False
        assert r.config_error is False

    def test_historize_config_error_propagates(self):
        session = object.__new__(Session)
        runner = MagicMock()
        runner.run.return_value = {
            "status": "failed",
            "error": "historize requires a primary_key",
            "config_error": True,
        }
        with patch(
            "dlt_saga.historize.factory.build_historize_runner", return_value=runner
        ):
            r = session._execute_single_historize(_config("p"), full_refresh=False)
        assert r.success is False
        assert r.config_error is True

    def test_historize_run_failure_not_config_error(self):
        session = object.__new__(Session)
        runner = MagicMock()
        runner.run.return_value = {
            "status": "failed",
            "error": "MERGE failed on the warehouse",
        }
        with patch(
            "dlt_saga.historize.factory.build_historize_runner", return_value=runner
        ):
            r = session._execute_single_historize(_config("p"), full_refresh=False)
        assert r.success is False
        assert r.config_error is False


# ---------------------------------------------------------------------------
# Session.__init__ tests
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestSessionInit:
    """Test Session initialization with mocked dependencies."""

    @patch("dlt_saga.defaults.apply_dlt_defaults")
    @patch("dlt_saga.session.get_auth_provider")
    @patch("dlt_saga.session.get_profiles_config")
    @patch("dlt_saga.session.get_config_source_settings")
    def test_init_with_defaults(
        self, mock_settings, mock_profiles, mock_auth, mock_defaults
    ):
        mock_settings.return_value = ConfigSourceConfig(type="file", paths=["configs"])
        profiles_config = MagicMock()
        profiles_config.profiles_exist.return_value = False
        mock_profiles.return_value = profiles_config
        mock_auth.return_value = MagicMock()

        session = Session()

        mock_defaults.assert_called_once()
        assert session._profile_target is None
        assert session._config_source is not None

    @patch("dlt_saga.defaults.apply_dlt_defaults")
    @patch("dlt_saga.session.get_auth_provider")
    @patch("dlt_saga.session.get_profiles_config")
    @patch("dlt_saga.session.get_config_source_settings")
    def test_init_with_profile(
        self, mock_settings, mock_profiles, mock_auth, mock_defaults
    ):
        mock_settings.return_value = ConfigSourceConfig(type="file", paths=["configs"])
        mock_target = MagicMock()
        mock_target.auth_provider = "gcp"
        mock_target.destination_type = "bigquery"
        mock_target.billing_project = None
        profiles_config = MagicMock()
        profiles_config.profiles_exist.return_value = True
        profiles_config.get_target.return_value = mock_target
        mock_profiles.return_value = profiles_config
        mock_auth.return_value = MagicMock()

        session = Session(profile="default", target="dev")

        profiles_config.get_target.assert_called_once_with("default", "dev")
        mock_auth.assert_called_once_with(
            auth_provider="gcp", destination_type="bigquery"
        )
        assert session._profile_target is mock_target

    @patch("dlt_saga.defaults.apply_dlt_defaults")
    @patch("dlt_saga.session.get_auth_provider")
    @patch("dlt_saga.session.get_profiles_config")
    def test_init_with_custom_config_dir(self, mock_profiles, mock_auth, mock_defaults):
        profiles_config = MagicMock()
        profiles_config.profiles_exist.return_value = False
        mock_profiles.return_value = profiles_config
        mock_auth.return_value = MagicMock()

        session = Session(config_dir="/custom/configs")

        assert session._config_source.root_dir == "/custom/configs"

    @patch("dlt_saga.defaults.apply_dlt_defaults")
    @patch("dlt_saga.session.get_auth_provider")
    @patch("dlt_saga.session.get_profiles_config")
    @patch("dlt_saga.session.get_config_source_settings")
    def test_init_with_profile_none_resolves_through_standard_chain(
        self, mock_settings, mock_profiles, mock_auth, mock_defaults, monkeypatch
    ):
        """`Session(target='prod')` (or any caller that passes profile=None
        explicitly, like the update-access CLI) should resolve the profile
        name through the standard chain: SAGA_PROFILE env → saga_project.yml
        profile: → 'default'. Regression test for
        https://github.com/<...>: Session passing None straight to
        ``profiles_config.get_target`` produced 'Profile \\'None\\' not found'
        instead of resolving to 'default'."""
        mock_settings.return_value = ConfigSourceConfig(type="file", paths=["configs"])
        mock_target = MagicMock()
        mock_target.auth_provider = "gcp"
        mock_target.destination_type = "bigquery"
        mock_target.billing_project = None
        profiles_config = MagicMock()
        profiles_config.profiles_exist.return_value = True
        profiles_config.get_target.return_value = mock_target
        mock_profiles.return_value = profiles_config
        mock_auth.return_value = MagicMock()
        # No SAGA_PROFILE env var, no profile: in saga_project.yml → "default".
        monkeypatch.delenv("SAGA_PROFILE", raising=False)
        with patch("dlt_saga.utility.cli.common.get_project_config") as mock_project:
            mock_project.return_value.profile = None
            Session(profile=None, target="prod-impersonation")

        profiles_config.get_target.assert_called_once_with(
            "default", "prod-impersonation"
        )

    @patch("dlt_saga.defaults.apply_dlt_defaults")
    @patch("dlt_saga.session.get_auth_provider")
    @patch("dlt_saga.session.get_profiles_config")
    @patch("dlt_saga.session.get_config_source_settings")
    def test_init_honours_saga_profile_env_when_profile_arg_is_none(
        self, mock_settings, mock_profiles, mock_auth, mock_defaults, monkeypatch
    ):
        """`SAGA_PROFILE=staging` should win when no explicit profile is passed."""
        mock_settings.return_value = ConfigSourceConfig(type="file", paths=["configs"])
        mock_target = MagicMock()
        mock_target.auth_provider = "gcp"
        mock_target.destination_type = "bigquery"
        mock_target.billing_project = None
        profiles_config = MagicMock()
        profiles_config.profiles_exist.return_value = True
        profiles_config.get_target.return_value = mock_target
        mock_profiles.return_value = profiles_config
        mock_auth.return_value = MagicMock()
        monkeypatch.setenv("SAGA_PROFILE", "staging")

        Session(profile=None, target="prod")

        profiles_config.get_target.assert_called_once_with("staging", "prod")

    @patch("dlt_saga.defaults.apply_dlt_defaults")
    @patch("dlt_saga.session.get_auth_provider")
    @patch("dlt_saga.session.get_profiles_config")
    @patch("dlt_saga.session.get_config_source_settings")
    def test_init_explicit_profile_arg_overrides_env_and_project_config(
        self, mock_settings, mock_profiles, mock_auth, mock_defaults, monkeypatch
    ):
        """An explicit ``profile=`` argument takes precedence over both
        SAGA_PROFILE and saga_project.yml ``profile:``."""
        mock_settings.return_value = ConfigSourceConfig(type="file", paths=["configs"])
        mock_target = MagicMock()
        mock_target.auth_provider = "gcp"
        mock_target.destination_type = "bigquery"
        mock_target.billing_project = None
        profiles_config = MagicMock()
        profiles_config.profiles_exist.return_value = True
        profiles_config.get_target.return_value = mock_target
        mock_profiles.return_value = profiles_config
        mock_auth.return_value = MagicMock()
        monkeypatch.setenv("SAGA_PROFILE", "staging")

        Session(profile="explicit_choice", target="prod")

        profiles_config.get_target.assert_called_once_with("explicit_choice", "prod")


# ---------------------------------------------------------------------------
# Session.discover tests
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestSessionDiscover:
    """Test config discovery via Session."""

    def _make_session(self):
        """Create a Session with mocked internals."""
        with (
            patch("dlt_saga.defaults.apply_dlt_defaults"),
            patch("dlt_saga.session.get_auth_provider") as mock_auth,
            patch("dlt_saga.session.get_profiles_config") as mock_profiles,
            patch("dlt_saga.session.get_config_source_settings") as mock_settings,
        ):
            mock_settings.return_value = ConfigSourceConfig(paths=["configs"])
            profiles_config = MagicMock()
            profiles_config.profiles_exist.return_value = False
            mock_profiles.return_value = profiles_config
            mock_auth.return_value = MagicMock()
            session = Session()
        return session

    def test_discover_calls_config_source(self):
        session = self._make_session()

        mock_config1 = MagicMock()
        mock_config1.ingest_enabled = True
        mock_config1.historize_enabled = False
        mock_config2 = MagicMock()
        mock_config2.ingest_enabled = False
        mock_config2.historize_enabled = True

        session._config_source = MagicMock()
        session._config_source.discover.return_value = (
            {"group_a": [mock_config1, mock_config2]},
            {},
        )

        # All
        configs = session.discover()
        assert len(configs) == 2

        # Ingest only
        configs = session.discover(resource_type="ingest")
        assert len(configs) == 1

        # Historize only
        configs = session.discover(resource_type="historize")
        assert len(configs) == 1

    def test_discover_resolves_within_profile_context(self):
        """`Session.discover()` must resolve names inside the session's profile
        execution context, even when no context is active yet.

        Regression: `saga run` pre-discovers pipelines (for its count/confirm)
        before entering a scope. Because discovery is memoized on the config
        source, resolving it unscoped cached the ``dlt_dev`` dev-schema fallback
        and poisoned the later scoped run — so `saga run` landed in the wrong
        dev schema while `saga ingest` did not.
        """
        from types import SimpleNamespace

        from dlt_saga.utility.cli.context import (
            clear_execution_context,
            get_execution_context,
        )

        session = self._make_session()
        session._profile_target = SimpleNamespace(schema="dbt_grindheim")

        seen = {}

        def _record_context():
            # Capture the schema visible via the global context at the exact
            # moment discovery runs (where schema_name would be resolved).
            seen["schema"] = get_execution_context().get_schema()
            return ({}, {})

        session._config_source = MagicMock()
        session._config_source.discover.side_effect = _record_context

        # Mirror `saga run`: no execution context active when discover() is called.
        clear_execution_context()
        try:
            session.discover()
        finally:
            clear_execution_context()

        assert seen["schema"] == "dbt_grindheim"

    def test_discover_invalid_resource_type(self):
        session = self._make_session()
        with pytest.raises(ValueError, match="Invalid resource_type"):
            session.discover(resource_type="bad")


# ---------------------------------------------------------------------------
# Session.ingest tests
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestSessionIngest:
    """Test ingest execution via Session."""

    def _make_session_with_configs(self, configs):
        """Create a Session with pre-configured mock config source."""
        with (
            patch("dlt_saga.defaults.apply_dlt_defaults"),
            patch("dlt_saga.session.get_auth_provider") as mock_auth,
            patch("dlt_saga.session.get_profiles_config") as mock_profiles,
            patch("dlt_saga.session.get_config_source_settings") as mock_settings,
        ):
            mock_settings.return_value = ConfigSourceConfig(paths=["configs"])
            profiles_config = MagicMock()
            profiles_config.profiles_exist.return_value = False
            mock_profiles.return_value = profiles_config
            auth = MagicMock()
            auth.validate.return_value = True
            mock_auth.return_value = auth
            session = Session()

        mock_source = MagicMock()
        mock_source.discover.return_value = (configs, {})
        session._config_source = mock_source
        return session

    @patch("dlt_saga.session.execute_pipeline")
    @patch("dlt_saga.destinations.factory.DestinationFactory")
    def test_ingest_success(self, mock_dest_factory, mock_execute):
        config = MagicMock()
        config.pipeline_name = "test_pipeline"
        config.ingest_enabled = True
        config.historize_enabled = False
        config.schema_name = "dlt_dev"
        config.table_name = "group__test_pipeline"
        config.config_dict = {}

        mock_execute.return_value = [{"rows": 10}]

        session = self._make_session_with_configs({"group": [config]})
        result = session.ingest(workers=1)

        assert result.succeeded == 1
        assert result.failed == 0
        assert not result.has_failures

    @patch("dlt_saga.session.execute_pipeline")
    @patch("dlt_saga.destinations.factory.DestinationFactory")
    def test_ingest_failure(self, mock_dest_factory, mock_execute):
        config = MagicMock()
        config.pipeline_name = "failing_pipeline"
        config.ingest_enabled = True
        config.historize_enabled = False
        config.schema_name = "dlt_dev"
        config.table_name = "group__failing_pipeline"
        config.config_dict = {}

        mock_execute.side_effect = RuntimeError("connection timeout")

        session = self._make_session_with_configs({"group": [config]})
        result = session.ingest(workers=1)

        assert result.succeeded == 0
        assert result.failed == 1
        assert result.has_failures
        assert result.failures[0].error == "connection timeout"

    def test_ingest_no_matching_configs(self):
        session = self._make_session_with_configs({})
        result = session.ingest()
        assert result.succeeded == 0
        assert result.failed == 0

    def test_ingest_auth_failure(self):
        with (
            patch("dlt_saga.defaults.apply_dlt_defaults"),
            patch("dlt_saga.session.get_auth_provider") as mock_auth,
            patch("dlt_saga.session.get_profiles_config") as mock_profiles,
            patch("dlt_saga.session.get_config_source_settings") as mock_settings,
        ):
            mock_settings.return_value = ConfigSourceConfig(paths=["configs"])
            profiles_config = MagicMock()
            profiles_config.profiles_exist.return_value = False
            mock_profiles.return_value = profiles_config
            auth = MagicMock()
            auth.validate.side_effect = AuthenticationError(
                "Credentials not configured"
            )
            mock_auth.return_value = auth
            session = Session()

        with pytest.raises(AuthenticationError, match="Credentials not configured"):
            session.ingest()


# ---------------------------------------------------------------------------
# Session.update_access tests
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestSessionUpdateAccess:
    """Test update_access via Session."""

    @patch("dlt_saga.session.execute_pipeline")
    @patch("dlt_saga.destinations.factory.DestinationFactory")
    def test_update_access_sets_context_flag(self, mock_dest_factory, mock_execute):
        """update_access should set update_access=True in the execution context."""
        with (
            patch("dlt_saga.defaults.apply_dlt_defaults"),
            patch("dlt_saga.session.get_auth_provider") as mock_auth,
            patch("dlt_saga.session.get_profiles_config") as mock_profiles,
            patch("dlt_saga.session.get_config_source_settings") as mock_settings,
        ):
            mock_settings.return_value = ConfigSourceConfig(paths=["configs"])
            profiles_config = MagicMock()
            profiles_config.profiles_exist.return_value = False
            mock_profiles.return_value = profiles_config
            auth = MagicMock()
            auth.validate.return_value = True
            mock_auth.return_value = auth
            session = Session()

        config = MagicMock()
        config.pipeline_name = "test_pipeline"
        config.ingest_enabled = True
        config.historize_enabled = False
        config.schema_name = "dlt_dev"
        config.table_name = "group__test_pipeline"
        config.config_dict = {}

        mock_execute.return_value = {"operation": "update_access"}

        mock_source = MagicMock()
        mock_source.discover.return_value = ({"group": [config]}, {})
        session._config_source = mock_source

        # Capture the context during execution
        from dlt_saga.utility.cli.context import get_execution_context

        captured_update_access = []
        original_execute = session._execute_single_ingest

        def capturing_execute(cfg, log_prefix=""):
            captured_update_access.append(get_execution_context().update_access)
            return original_execute(cfg, log_prefix)

        with patch.object(
            Session, "_execute_single_ingest", side_effect=capturing_execute
        ):
            result = session.update_access(workers=1)

        assert result.succeeded == 1
        assert captured_update_access == [True]


# ---------------------------------------------------------------------------
# Session._apply_orchestration_access tests
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestApplyOrchestrationAccess:
    """Test that orchestration.dataset_access from saga_project.yml is
    applied to the orchestration schema during ``saga update-access``."""

    def test_noop_when_dataset_access_unset(self):
        from dlt_saga.project_config import OrchestrationConfig

        with (
            patch(
                "dlt_saga.project_config.get_orchestration_config",
                return_value=OrchestrationConfig(),  # no dataset_access
            ),
            patch(
                "dlt_saga.destinations.bigquery.base.BigQueryBaseDestination._sync_dataset_and_access_static"
            ) as mock_sync,
        ):
            Session._apply_orchestration_access()

        mock_sync.assert_not_called()

    def test_skips_with_warning_for_non_bigquery_destination(self, caplog):
        from dlt_saga.project_config import OrchestrationConfig

        access = ["READER:serviceAccount:airflow@example.iam.gserviceaccount.com"]
        with (
            patch(
                "dlt_saga.project_config.get_orchestration_config",
                return_value=OrchestrationConfig(schema_access=access),
            ),
            patch("dlt_saga.utility.naming.is_production", return_value=True),
            patch("dlt_saga.utility.cli.context.get_execution_context") as mock_ctx,
            patch(
                "dlt_saga.destinations.bigquery.base.BigQueryBaseDestination._sync_dataset_and_access_static"
            ) as mock_sync,
            caplog.at_level("WARNING"),
        ):
            mock_ctx.return_value.get_destination_type.return_value = "databricks"
            Session._apply_orchestration_access()

        mock_sync.assert_not_called()
        assert any("does not support" in r.message for r in caplog.records)

    def test_skips_in_dev_even_when_configured(self, caplog):
        """orchestration.schema_access is prod-only: in dev it never touches the
        developer's schema, regardless of destination or config."""
        from dlt_saga.project_config import OrchestrationConfig

        access = ["READER:serviceAccount:airflow@example.iam.gserviceaccount.com"]
        with (
            patch(
                "dlt_saga.project_config.get_orchestration_config",
                return_value=OrchestrationConfig(schema_access=access),
            ),
            patch("dlt_saga.utility.naming.is_production", return_value=False),
            patch("dlt_saga.utility.cli.context.get_execution_context") as mock_ctx,
            patch(
                "dlt_saga.destinations.bigquery.base.BigQueryBaseDestination._sync_dataset_and_access_static"
            ) as mock_sync,
        ):
            mock_ctx.return_value.get_destination_type.return_value = "bigquery"
            Session._apply_orchestration_access()

        mock_sync.assert_not_called()

    def test_applies_access_when_set_and_destination_is_bigquery(self):
        from dlt_saga.project_config import OrchestrationConfig

        access = [
            "READER:serviceAccount:airflow@example.iam.gserviceaccount.com",
            "READER:group:data@example.com",
        ]
        with (
            patch(
                "dlt_saga.project_config.get_orchestration_config",
                return_value=OrchestrationConfig(schema_access=access),
            ),
            patch("dlt_saga.utility.naming.is_production", return_value=True),
            patch("dlt_saga.utility.cli.context.get_execution_context") as mock_ctx,
            patch(
                "dlt_saga.utility.naming.get_execution_plan_schema",
                return_value="dlt_orchestration",
            ),
            patch(
                "dlt_saga.destinations.bigquery.base.BigQueryBaseDestination._sync_dataset_and_access_static"
            ) as mock_sync,
        ):
            mock_ctx.return_value.get_destination_type.return_value = "bigquery"
            mock_ctx.return_value.get_database.return_value = "amedia-adp-sources"
            mock_ctx.return_value.get_location.return_value = "EU"

            Session._apply_orchestration_access()

        mock_sync.assert_called_once_with(
            project_id="amedia-adp-sources",
            location="EU",
            dataset_name="dlt_orchestration",
            schema_access=access,
        )

    def test_skips_with_warning_when_no_project_in_context(self, caplog):
        from dlt_saga.project_config import OrchestrationConfig

        access = ["READER:serviceAccount:airflow@example.iam.gserviceaccount.com"]
        with (
            patch(
                "dlt_saga.project_config.get_orchestration_config",
                return_value=OrchestrationConfig(schema_access=access),
            ),
            patch("dlt_saga.utility.naming.is_production", return_value=True),
            patch("dlt_saga.utility.cli.context.get_execution_context") as mock_ctx,
            patch(
                "dlt_saga.destinations.bigquery.base.BigQueryBaseDestination._sync_dataset_and_access_static"
            ) as mock_sync,
            caplog.at_level("WARNING"),
        ):
            mock_ctx.return_value.get_destination_type.return_value = "bigquery"
            mock_ctx.return_value.get_database.return_value = None

            Session._apply_orchestration_access()

        mock_sync.assert_not_called()
        assert any("no project" in r.message.lower() for r in caplog.records)


# ---------------------------------------------------------------------------
# execution_context_scope tests
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestExecutionContextScope:
    """Test the context save/restore mechanism."""

    def test_scope_restores_previous_context(self):
        from dlt_saga.utility.cli.context import (
            clear_execution_context,
            execution_context_scope,
            get_execution_context,
            set_execution_context,
        )

        # Set initial context
        set_execution_context(None, force=True)
        assert get_execution_context().force is True

        # Enter scope with different settings
        with execution_context_scope(None, force=False, full_refresh=True) as ctx:
            assert ctx.force is False
            assert ctx.full_refresh is True
            assert get_execution_context().full_refresh is True

        # Previous context restored
        assert get_execution_context().force is True
        assert get_execution_context().full_refresh is False

        clear_execution_context()

    def test_scope_restores_on_exception(self):
        from dlt_saga.utility.cli.context import (
            clear_execution_context,
            execution_context_scope,
            get_execution_context,
            set_execution_context,
        )

        set_execution_context(None, force=True)

        with pytest.raises(ValueError):
            with execution_context_scope(None, force=False):
                raise ValueError("boom")

        # Still restored
        assert get_execution_context().force is True
        clear_execution_context()


@pytest.mark.unit
class TestExitIfFailures:
    """Regression for #94 / #97.

    The CLI failure summary must reach the operator even when a host
    process (Airflow being the most common example, via its default
    ``disable_existing_loggers=True`` in ``logging.config.dictConfig``)
    has disabled saga's loggers mid-run. Two safeguards are tested here:

    * ``_exit_if_failures`` writes to stderr via ``typer.echo`` so the
      summary is immune to whatever the host did to the logging module.
    * ``reenable_saga_loggers`` flips ``.disabled`` back off on every
      ``dlt_saga.*`` logger so subsequent INFO records flow again.
    """

    def test_failure_reaches_stderr_when_logger_disabled(self, capsys):
        import typer

        import dlt_saga.cli as cli

        result = SessionResult(
            pipeline_results=[
                PipelineResult(pipeline_name="ingest_ok", success=True),
                PipelineResult(
                    pipeline_name="broken",
                    success=False,
                    error="PERMISSION_DENIED: no MANAGE on table",
                ),
            ]
        )

        cli.logger.disabled = True
        try:
            with pytest.raises(typer.Exit):
                cli._exit_if_failures(result, "Run")
        finally:
            cli.logger.disabled = False

        err = capsys.readouterr().err
        assert "Run failed: 1/2 pipeline(s) failed" in err
        assert "broken:" in err
        assert "PERMISSION_DENIED: no MANAGE on table" in err

    def test_each_failure_block_indented_under_its_name(self, capsys):
        """Multiple failures — each error (multi-line included) is indented under
        its own pipeline name so they stay visually distinct."""
        import typer

        import dlt_saga.cli as cli

        result = SessionResult(
            pipeline_results=[
                PipelineResult(
                    pipeline_name="p1",
                    success=False,
                    error="Historization config changed:\n  track_deletions: False → True",
                ),
                PipelineResult(
                    pipeline_name="p2", success=False, error="connection reset"
                ),
            ]
        )
        with pytest.raises(typer.Exit):
            cli._exit_if_failures(result, "Historize")

        err = capsys.readouterr().err
        # Each failure's whole block sits under its own indented name.
        assert (
            "  p1:\n      Historization config changed:\n        track_deletions" in err
        )
        assert "  p2:\n      connection reset" in err

    def test_no_output_when_all_succeed(self, capsys):
        import dlt_saga.cli as cli

        result = SessionResult(
            pipeline_results=[PipelineResult(pipeline_name="ok", success=True)]
        )
        cli._exit_if_failures(result, "Run")  # returns, no raise
        assert capsys.readouterr().err == ""

    def test_rescue_restores_info_flow_after_disable(self):
        """After ``reenable_saga_loggers`` runs, an INFO record on a previously
        disabled ``dlt_saga.*`` logger must reach an attached handler.

        Uses an in-memory handler on a private logger so the test is independent
        of root handlers (which a real foreign ``dictConfig`` would also have
        replaced — out of scope here)."""
        import io
        import logging

        from dlt_saga.utility.cli.logging import reenable_saga_loggers

        target = logging.getLogger("dlt_saga.test_rescue_flow")
        target.setLevel(logging.INFO)
        target.propagate = False
        sink = io.StringIO()
        handler = logging.StreamHandler(sink)
        handler.setLevel(logging.INFO)
        target.addHandler(handler)

        try:
            target.disabled = True
            target.info("silenced before rescue")
            assert sink.getvalue() == ""

            reenable_saga_loggers()

            assert target.disabled is False
            target.info("restored after rescue")
            assert "restored after rescue" in sink.getvalue()
        finally:
            target.removeHandler(handler)
            target.disabled = False

    def test_rescue_leaves_foreign_loggers_alone(self):
        """The rescue must only touch ``dlt_saga.*`` loggers — third-party
        loggers that the host explicitly intends to keep disabled must remain
        disabled."""
        import logging

        from dlt_saga.utility.cli.logging import reenable_saga_loggers

        foreign = logging.getLogger("third_party.test_unaffected")
        foreign.disabled = True
        try:
            reenable_saga_loggers()
            assert foreign.disabled is True
        finally:
            foreign.disabled = False


# ---------------------------------------------------------------------------
# Session.maintenance tests
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestSessionMaintenance:
    """Session.maintenance mirrors the CLI command: discover schemas, then
    delegate to the shared maintenance runner (clustering + compaction) under the
    session's auth."""

    def _session(self):
        session = Session.__new__(Session)
        session._profile_target = None
        session._auth_provider = MagicMock()
        return session

    def test_run_maintenance_delegates_with_selected_configs(self):
        session = self._session()
        configs = {"api": [MagicMock()]}
        session._discover_and_select = MagicMock(return_value=(configs, {}))
        counts = {
            "clustering": {"absent": 0, "unchanged": 0, "reconciled": 3},
            "compaction": {"absent": 0, "collapsed": 4, "orphaned": 1},
            "reconcile": {"abandoned": 2},
        }
        with (
            patch(
                "dlt_saga.maintenance.run_maintenance",
                return_value=counts,
            ) as run,
            patch(
                "dlt_saga.utility.cli.context.get_execution_context",
                return_value=MagicMock(),
            ),
        ):
            result = session._run_maintenance(select=["tag:x"], dry_run=True)
        assert result["clustering"]["reconciled"] == 3
        assert result["compaction"]["collapsed"] == 4
        assert result["reconcile"]["abandoned"] == 2
        run.assert_called_once()
        assert run.call_args.args[1] == configs  # selected configs forwarded
        assert run.call_args.args[2] is True  # dry_run forwarded

    def test_run_maintenance_empty_selection_returns_zero(self):
        session = self._session()
        session._discover_and_select = MagicMock(return_value=({}, {}))
        with patch("dlt_saga.maintenance.run_maintenance") as run:
            result = session._run_maintenance(select=None, dry_run=False)
        assert result == {
            "clustering": {"absent": 0, "unchanged": 0, "reconciled": 0},
            "compaction": {"absent": 0, "collapsed": 0, "orphaned": 0},
            "reconcile": {"abandoned": 0},
        }
        run.assert_not_called()

    def test_maintenance_runs_under_auth(self):
        session = self._session()
        sentinel = {"clustering": {"reconciled": 1}, "compaction": {"collapsed": 0}}
        session._execute_with_auth = MagicMock(return_value=sentinel)
        with patch("dlt_saga.session.execution_context_scope"):
            result = session.maintenance(select=None, dry_run=False)
        assert result == sentinel
        session._execute_with_auth.assert_called_once()


# ---------------------------------------------------------------------------
# Session.validate tests
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestSessionValidate:
    """Session.validate runs the offline config checks (no warehouse, no auth)
    and returns one ValidationResult per selected config."""

    def _session(self):
        session = Session.__new__(Session)
        session._profile_target = None
        session._auth_provider = MagicMock()
        session._config_source = MagicMock()
        return session

    def test_delegates_to_validate_pipeline_configs(self):
        from dlt_saga.validate import ValidationResult

        session = self._session()
        session._discover_and_select = MagicMock(
            return_value=({"g": [MagicMock(), MagicMock()]}, {})
        )
        with (
            patch(
                "dlt_saga.validate.validate_pipeline_configs",
                return_value=[ValidationResult("a"), ValidationResult("b")],
            ) as validate,
            patch("dlt_saga.session.execution_context_scope"),
        ):
            results = session.validate(select=["tag:x"])
        assert len(results) == 2
        # One batch call with the flattened configs + the session's config source.
        assert validate.call_count == 1
        flat_arg = validate.call_args.args[0]
        assert len(flat_arg) == 2

    def test_empty_selection_returns_empty_list(self):
        session = self._session()
        session._discover_and_select = MagicMock(return_value=({}, {}))
        with patch("dlt_saga.session.execution_context_scope"):
            assert session.validate() == []

    def test_does_not_require_auth(self):
        # validate is offline — it must never go through the auth wrapper.
        session = self._session()
        session._discover_and_select = MagicMock(return_value=({}, {}))
        session._execute_with_auth = MagicMock()
        with patch("dlt_saga.session.execution_context_scope"):
            session.validate()
        session._execute_with_auth.assert_not_called()


# ---------------------------------------------------------------------------
# Session.report tests
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestSessionReport:
    """Session.report collects run history and writes/uploads an HTML report,
    returning the resolved location — under the session's auth."""

    def _session(self):
        session = Session.__new__(Session)
        session._profile_target = None
        session._auth_provider = MagicMock()
        return session

    def _config(self):
        cfg = MagicMock()
        cfg.config_dict = {}
        cfg.schema_name = "dlt_dev"
        return cfg

    def _enter_report_patches(
        self, stack, dest, *, remote, generate_return=None, upload_return=None
    ):
        """Enter all of _run_report's collaborators (source modules, since it
        imports them locally) and return the mocks keyed by role."""
        p = lambda *a, **k: stack.enter_context(patch(*a, **k))  # noqa: E731
        return {
            "create": p(
                "dlt_saga.destinations.factory.DestinationFactory.create_from_context",
                return_value=dest,
            ),
            "collect": p(
                "dlt_saga.report.collector.collect_report_data", return_value={}
            ),
            "generate": p(
                "dlt_saga.report.generator.generate_report",
                return_value=generate_return,
            ),
            "is_remote": p(
                "dlt_saga.report.uploader.is_remote_uri", return_value=remote
            ),
            "upload": p(
                "dlt_saga.report.uploader.upload_report", return_value=upload_return
            ),
            "ctx": p(
                "dlt_saga.utility.cli.context.get_execution_context",
                return_value=MagicMock(),
            ),
            "plan_schema": p(
                "dlt_saga.utility.naming.get_execution_plan_schema",
                return_value="dlt_orch",
            ),
        }

    def test_local_returns_generated_path_and_closes_connection(self):
        session = self._session()
        session._discover_and_select = MagicMock(
            return_value=({"g": [self._config()]}, {})
        )
        dest = MagicMock()
        with ExitStack() as stack:
            mocks = self._enter_report_patches(
                stack, dest, remote=False, generate_return="/abs/report.html"
            )
            result = session._run_report(None, "report.html", 14)
        assert result == "/abs/report.html"
        mocks["generate"].assert_called_once()
        dest.connect.assert_called_once()
        dest.close.assert_called_once()  # released even though generation is mocked

    def test_remote_uploads_and_returns_uri(self):
        session = self._session()
        session._discover_and_select = MagicMock(
            return_value=({"g": [self._config()]}, {})
        )
        with ExitStack() as stack:
            mocks = self._enter_report_patches(
                stack, MagicMock(), remote=True, upload_return="gs://b/r.html"
            )
            result = session._run_report(None, "gs://b/r.html", 7)
        assert result == "gs://b/r.html"
        mocks["upload"].assert_called_once()

    def test_empty_selection_raises(self):
        session = self._session()
        session._discover_and_select = MagicMock(return_value=({}, {}))
        with pytest.raises(ValueError, match="No pipeline configs"):
            session._run_report(None, "report.html", 14)

    def test_rejects_days_below_one_before_auth(self):
        session = self._session()
        session._execute_with_auth = MagicMock()
        with pytest.raises(ValueError, match="days must be >= 1"):
            session.report(days=0)
        session._execute_with_auth.assert_not_called()

    def test_runs_under_auth(self):
        session = self._session()
        session._execute_with_auth = MagicMock(return_value="/abs/r.html")
        with patch("dlt_saga.session.execution_context_scope"):
            result = session.report(output="r.html", days=5)
        assert result == "/abs/r.html"
        session._execute_with_auth.assert_called_once()
