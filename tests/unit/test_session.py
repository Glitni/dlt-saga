"""Tests for the Session programmatic API."""

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
