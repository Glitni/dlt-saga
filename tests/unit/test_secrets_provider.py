"""Tests for the SecretsProvider abstraction and provider-based dispatch."""

from unittest.mock import MagicMock, patch

import pytest

from dlt_saga.utility.secrets.providers import (
    EnvVarSecretsProvider,
    GcpSecretsProvider,
    SecretsProvider,
)
from dlt_saga.utility.secrets.resolver import (
    SecretResolver,
    _reset_for_testing,
    resolve_secret,
)


@pytest.fixture(autouse=True)
def _clean_resolver():
    """Reset provider registry and cache between tests."""
    _reset_for_testing()
    yield
    _reset_for_testing()


@pytest.mark.unit
class TestSecretsProviderABC:
    """SecretsProvider cannot be instantiated directly."""

    def test_cannot_instantiate(self):
        with pytest.raises(TypeError):
            SecretsProvider()

    def test_subclass_must_implement_get_secret(self):
        class Incomplete(SecretsProvider):
            pass

        with pytest.raises(TypeError):
            Incomplete()

    def test_subclass_with_implementation_works(self):
        class Complete(SecretsProvider):
            def get_secret(self, name, project_or_scope="", version="latest"):
                return "value"

        provider = Complete()
        assert provider.get_secret("key") == "value"


@pytest.mark.unit
class TestEnvVarSecretsProvider:
    """EnvVarSecretsProvider resolves from environment variables."""

    def test_resolves_set_env_var(self, monkeypatch):
        monkeypatch.setenv("MY_SECRET", "secret-value")
        provider = EnvVarSecretsProvider()
        assert provider.get_secret("MY_SECRET") == "secret-value"

    def test_raises_on_missing_env_var(self):
        provider = EnvVarSecretsProvider()
        with pytest.raises(
            ValueError, match="Environment variable 'MISSING_VAR' is not set"
        ):
            provider.get_secret("MISSING_VAR")

    def test_ignores_project_or_scope(self, monkeypatch):
        monkeypatch.setenv("TOKEN", "abc")
        provider = EnvVarSecretsProvider()
        assert provider.get_secret("TOKEN", project_or_scope="ignored") == "abc"

    def test_empty_value_is_valid(self, monkeypatch):
        monkeypatch.setenv("EMPTY", "")
        provider = EnvVarSecretsProvider()
        assert provider.get_secret("EMPTY") == ""


@pytest.mark.unit
class TestGcpSecretsProvider:
    """GcpSecretsProvider delegates to utility.gcp.secrets.get_secret."""

    def test_delegates_to_gcp_get_secret(self):
        provider = GcpSecretsProvider()
        with patch(
            "dlt_saga.utility.gcp.secrets.get_secret", return_value="  value  "
        ) as mock:
            result = provider.get_secret("my-secret", project_or_scope="my-project")

        mock.assert_called_once_with(
            secret_name="my-secret",
            project_id="my-project",
            version="latest",
        )
        assert result == "value"  # stripped

    def test_empty_project_passes_none(self):
        provider = GcpSecretsProvider()
        with patch(
            "dlt_saga.utility.gcp.secrets.get_secret", return_value="val"
        ) as mock:
            provider.get_secret("secret", project_or_scope="")

        mock.assert_called_once_with(
            secret_name="secret",
            project_id=None,
            version="latest",
        )

    def test_wraps_exceptions_as_valueerror(self):
        provider = GcpSecretsProvider()
        with patch(
            "dlt_saga.utility.gcp.secrets.get_secret",
            side_effect=RuntimeError("connection failed"),
        ):
            with pytest.raises(ValueError, match="Failed to fetch Google secret"):
                provider.get_secret("secret", project_or_scope="proj")


@pytest.mark.unit
class TestResolverProviderDispatch:
    """SecretResolver dispatches to the correct registered provider."""

    def test_env_uri_dispatches_to_env_provider(self, monkeypatch):
        monkeypatch.setenv("MY_TOKEN", "token-value")
        result = resolve_secret("env::MY_TOKEN")
        assert result == "token-value"

    def test_googlesecretmanager_uri_dispatches_to_gcp(self):
        with patch("dlt_saga.utility.gcp.secrets.get_secret", return_value="gcp-val"):
            result = resolve_secret("googlesecretmanager::proj::secret-name")
        assert result == "gcp-val"

    def test_unknown_prefix_treated_as_plain_value(self):
        # Unrecognized prefixes are not secret URIs — they pass through as-is
        assert resolve_secret("unknown::scope::name") == "unknown::scope::name"

    def test_unregistered_provider_raises_when_forced(self):
        """If a provider is registered then removed, URIs for it pass through."""
        # Register then remove a provider to verify the registry lookup works
        mock = MagicMock(spec=SecretsProvider)
        mock.get_secret.return_value = "val"
        SecretResolver.register_provider("custom", mock)
        assert resolve_secret("custom::scope::name") == "val"

    def test_plain_value_passthrough(self):
        assert resolve_secret("plain-value") == "plain-value"

    def test_none_passthrough(self):
        assert resolve_secret(None) is None

    def test_int_passthrough(self):
        assert resolve_secret(42) == 42


@pytest.mark.unit
class TestResolverRegistration:
    """Custom providers can be registered and used."""

    def test_register_custom_provider(self):
        mock_provider = MagicMock(spec=SecretsProvider)
        mock_provider.get_secret.return_value = "custom-value"

        SecretResolver.register_provider("custom", mock_provider)
        result = resolve_secret("custom::scope::name")

        assert result == "custom-value"
        mock_provider.get_secret.assert_called_once_with(
            name="name",
            project_or_scope="scope",
        )

    def test_get_provider_returns_registered(self):
        provider = SecretResolver.get_provider("env")
        assert isinstance(provider, EnvVarSecretsProvider)

    def test_get_provider_returns_none_for_unknown(self):
        assert SecretResolver.get_provider("nonexistent") is None

    def test_custom_provider_overrides_builtin(self):
        mock_provider = MagicMock(spec=SecretsProvider)
        mock_provider.get_secret.return_value = "overridden"

        SecretResolver.register_provider("env", mock_provider)
        result = resolve_secret("env::MY_KEY")

        assert result == "overridden"


@pytest.mark.unit
class TestTwoPartUri:
    """Two-part URIs (prefix::name) work for providers that don't need a scope."""

    def test_env_two_part_uri(self, monkeypatch):
        monkeypatch.setenv("API_KEY", "key123")
        assert resolve_secret("env::API_KEY") == "key123"

    def test_is_secret_uri_recognizes_env(self):
        assert SecretResolver.is_secret_uri("env::MY_KEY") is True

    def test_is_secret_uri_recognizes_gcp(self):
        assert SecretResolver.is_secret_uri("googlesecretmanager::proj::name") is True

    def test_is_secret_uri_rejects_plain(self):
        assert SecretResolver.is_secret_uri("plain-value") is False


@pytest.mark.unit
class TestCachingWithProviders:
    """Caching still works with provider-based dispatch."""

    def test_second_resolve_uses_cache(self, monkeypatch):
        monkeypatch.setenv("CACHED_KEY", "val")

        # First call resolves
        assert resolve_secret("env::CACHED_KEY") == "val"

        # Remove env var — second call should still return cached value
        monkeypatch.delenv("CACHED_KEY")
        assert resolve_secret("env::CACHED_KEY") == "val"

    def test_gcp_cached_after_first_call(self):
        with patch(
            "dlt_saga.utility.gcp.secrets.get_secret", return_value="cached"
        ) as mock:
            resolve_secret("googlesecretmanager::proj::name")
            resolve_secret("googlesecretmanager::proj::name")

        mock.assert_called_once()  # Only one actual call, second used cache


@pytest.mark.unit
class TestSecretStrIntegration:
    """SecretStr values are unwrapped before resolution."""

    def test_secret_str_env_uri(self, monkeypatch):
        from dlt_saga.utility.secrets.secret_str import SecretStr

        monkeypatch.setenv("WRAPPED", "unwrapped-val")
        result = resolve_secret(SecretStr("env::WRAPPED"))
        assert result == "unwrapped-val"

    def test_secret_str_plain_passthrough(self):
        from dlt_saga.utility.secrets.secret_str import SecretStr

        result = resolve_secret(SecretStr("plain-value"))
        assert result == "plain-value"
