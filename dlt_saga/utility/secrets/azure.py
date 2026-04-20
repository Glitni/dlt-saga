"""Azure Key Vault secrets provider.

Fetches secrets from Azure Key Vault using ``DefaultAzureCredential``.
URI format: ``azurekeyvault::<vault_url>::<secret_name>``

Example:
    ``azurekeyvault::https://my-vault.vault.azure.net::databricks-pat``
"""

from __future__ import annotations

import logging

from dlt_saga.utility.secrets.providers import SecretsProvider

logger = logging.getLogger(__name__)


class AzureKeyVaultSecretsProvider(SecretsProvider):
    """Fetches secrets from Azure Key Vault.

    Uses ``azure-identity``'s ``DefaultAzureCredential`` for authentication,
    which supports managed identity, service principal, and developer
    workstation login (``az login``).

    The ``project_or_scope`` argument is the vault URL, e.g.
    ``https://my-vault.vault.azure.net``.
    """

    def get_secret(
        self, name: str, project_or_scope: str = "", version: str = "latest"
    ) -> str:
        """Fetch a secret from Azure Key Vault.

        Args:
            name: Secret name (as defined in Key Vault).
            project_or_scope: Vault URL, e.g.
                ``https://my-vault.vault.azure.net``.
            version: Secret version.  ``"latest"`` (default) resolves to the
                current enabled version.

        Returns:
            The secret value as a string.

        Raises:
            ValueError: If the vault URL is missing or the secret cannot be fetched.
            ImportError: If ``azure-keyvault-secrets`` or ``azure-identity``
                are not installed (``pip install dlt-saga[azure]``).
        """
        try:
            from azure.identity import DefaultAzureCredential
            from azure.keyvault.secrets import SecretClient
        except ImportError:
            raise ImportError(
                "Azure Key Vault secrets require 'dlt-saga[azure]'. "
                "Run: pip install 'dlt-saga[azure]'"
            ) from None

        if not project_or_scope:
            raise ValueError(
                "Azure Key Vault vault URL is required. "
                "Use format: azurekeyvault::https://my-vault.vault.azure.net::secret-name"
            )

        vault_url = project_or_scope
        secret_version = None if version == "latest" else version

        try:
            from azure.core.exceptions import ClientAuthenticationError
        except ImportError:
            ClientAuthenticationError = None  # type: ignore[assignment,misc]

        # azure-identity emits a WARNING with the full credential chain when
        # DefaultAzureCredential fails. Suppress it here — our own error message
        # is clearer and actionable. The original exception is still chained.
        azure_identity_logger = logging.getLogger("azure.identity")
        original_level = azure_identity_logger.level

        try:
            azure_identity_logger.setLevel(logging.ERROR)
            credential = DefaultAzureCredential()
            client = SecretClient(vault_url=vault_url, credential=credential)
            secret = client.get_secret(name, version=secret_version)
            if secret.value is None:
                raise ValueError(f"Secret '{name}' in vault '{vault_url}' has no value")
            logger.debug("Resolved secret from Azure Key Vault: %s/%s", vault_url, name)
            return secret.value
        except ImportError:
            raise
        except ValueError:
            raise
        except Exception as e:
            if ClientAuthenticationError and isinstance(e, ClientAuthenticationError):
                raise ValueError(
                    f"Azure Key Vault authentication failed for '{vault_url}'. "
                    "No valid Azure credential found on this machine. "
                    "Run 'az login' (Azure CLI) to authenticate, or set the "
                    "AZURE_CLIENT_ID / AZURE_CLIENT_SECRET / AZURE_TENANT_ID "
                    "environment variables for service principal auth. "
                    "See: https://aka.ms/azsdk/python/identity/defaultazurecredential/troubleshoot"
                ) from e
            raise ValueError(
                f"Failed to fetch Azure Key Vault secret '{vault_url}/{name}': {e}"
            ) from e
        finally:
            azure_identity_logger.setLevel(original_level)
