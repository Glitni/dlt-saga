"""Unit tests for AzureKeyVaultSecretsProvider."""

from unittest.mock import MagicMock, patch

import pytest

from dlt_saga.utility.secrets.azure import AzureKeyVaultSecretsProvider


@pytest.mark.unit
class TestAzureKeyVaultSecretsProvider:
    def _make_provider(self):
        return AzureKeyVaultSecretsProvider()

    # ------------------------------------------------------------------
    # Missing vault URL
    # ------------------------------------------------------------------

    def test_missing_vault_url_raises_valueerror(self):
        provider = self._make_provider()
        with pytest.raises(ValueError, match="vault URL is required"):
            provider.get_secret("my-secret", project_or_scope="")

    def test_empty_project_or_scope_raises(self):
        provider = self._make_provider()
        with pytest.raises(ValueError):
            provider.get_secret("secret")

    # ------------------------------------------------------------------
    # Successful fetch
    # ------------------------------------------------------------------

    def test_returns_secret_value(self):
        provider = self._make_provider()

        mock_secret = MagicMock()
        mock_secret.value = "super-secret"

        mock_client = MagicMock()
        mock_client.get_secret.return_value = mock_secret

        with (
            patch("azure.identity.DefaultAzureCredential"),
            patch(
                "azure.keyvault.secrets.SecretClient",
                return_value=mock_client,
            ),
        ):
            result = provider.get_secret(
                "my-secret",
                project_or_scope="https://my-vault.vault.azure.net",
            )

        assert result == "super-secret"
        mock_client.get_secret.assert_called_once_with("my-secret", version=None)

    def test_latest_version_passes_none_to_sdk(self):
        provider = self._make_provider()

        mock_secret = MagicMock()
        mock_secret.value = "val"
        mock_client = MagicMock()
        mock_client.get_secret.return_value = mock_secret

        with (
            patch("azure.identity.DefaultAzureCredential"),
            patch(
                "azure.keyvault.secrets.SecretClient",
                return_value=mock_client,
            ),
        ):
            provider.get_secret(
                "s",
                project_or_scope="https://vault.azure.net",
                version="latest",
            )

        mock_client.get_secret.assert_called_once_with("s", version=None)

    def test_explicit_version_passed_to_sdk(self):
        provider = self._make_provider()

        mock_secret = MagicMock()
        mock_secret.value = "versioned"
        mock_client = MagicMock()
        mock_client.get_secret.return_value = mock_secret

        with (
            patch("azure.identity.DefaultAzureCredential"),
            patch(
                "azure.keyvault.secrets.SecretClient",
                return_value=mock_client,
            ),
        ):
            result = provider.get_secret(
                "s",
                project_or_scope="https://vault.azure.net",
                version="abc123",
            )

        mock_client.get_secret.assert_called_once_with("s", version="abc123")
        assert result == "versioned"

    # ------------------------------------------------------------------
    # None secret value
    # ------------------------------------------------------------------

    def test_none_secret_value_raises_valueerror(self):
        provider = self._make_provider()

        mock_secret = MagicMock()
        mock_secret.value = None
        mock_client = MagicMock()
        mock_client.get_secret.return_value = mock_secret

        with (
            patch("azure.identity.DefaultAzureCredential"),
            patch(
                "azure.keyvault.secrets.SecretClient",
                return_value=mock_client,
            ),
        ):
            with pytest.raises(ValueError, match="has no value"):
                provider.get_secret(
                    "empty-secret",
                    project_or_scope="https://vault.azure.net",
                )

    # ------------------------------------------------------------------
    # SDK exceptions wrapped
    # ------------------------------------------------------------------

    def test_sdk_exception_wrapped_as_valueerror(self):
        provider = self._make_provider()

        mock_client = MagicMock()
        mock_client.get_secret.side_effect = RuntimeError("network error")

        with (
            patch("azure.identity.DefaultAzureCredential"),
            patch(
                "azure.keyvault.secrets.SecretClient",
                return_value=mock_client,
            ),
        ):
            with pytest.raises(
                ValueError, match="Failed to fetch Azure Key Vault secret"
            ):
                provider.get_secret(
                    "broken",
                    project_or_scope="https://vault.azure.net",
                )

    def test_vault_url_passed_to_secret_client(self):
        provider = self._make_provider()

        mock_secret = MagicMock()
        mock_secret.value = "v"
        mock_client = MagicMock()
        mock_client.get_secret.return_value = mock_secret

        vault_url = "https://specific-vault.vault.azure.net"

        with (
            patch("azure.identity.DefaultAzureCredential") as mock_cred,
            patch(
                "azure.keyvault.secrets.SecretClient",
                return_value=mock_client,
            ) as mock_client_cls,
        ):
            provider.get_secret("s", project_or_scope=vault_url)

        mock_client_cls.assert_called_once_with(
            vault_url=vault_url, credential=mock_cred.return_value
        )
