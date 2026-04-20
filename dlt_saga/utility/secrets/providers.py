"""Secrets provider abstraction for pluggable secret backends.

Defines the SecretsProvider interface and built-in implementations:
- GcpSecretsProvider: Google Cloud Secret Manager
- EnvVarSecretsProvider: Environment variables (for local dev / CI)

Custom providers can implement SecretsProvider and register via
SecretResolver.register_provider().
"""

import logging
import os
from abc import ABC, abstractmethod

logger = logging.getLogger(__name__)


class SecretsProvider(ABC):
    """Abstract base class for secret resolution backends.

    Implementations fetch secrets from a specific backend (GCP Secret Manager,
    Azure Key Vault, environment variables, etc.).
    """

    @abstractmethod
    def get_secret(
        self, name: str, project_or_scope: str = "", version: str = "latest"
    ) -> str:
        """Fetch a secret value.

        Args:
            name: Secret name or identifier.
            project_or_scope: Provider-specific scope (e.g., GCP project ID,
                Azure vault name). May be empty for providers that don't need it.
            version: Secret version (default: "latest").

        Returns:
            The secret value as a string.

        Raises:
            ValueError: If the secret cannot be fetched.
        """


class GcpSecretsProvider(SecretsProvider):
    """Fetches secrets from Google Cloud Secret Manager.

    Delegates to ``dlt_saga.utility.gcp.secrets.get_secret()`` which handles
    client pooling, retry logic, and thread safety.
    """

    def get_secret(
        self, name: str, project_or_scope: str = "", version: str = "latest"
    ) -> str:
        from dlt_saga.utility.gcp.secrets import get_secret

        try:
            value = get_secret(
                secret_name=name,
                project_id=project_or_scope or None,
                version=version,
            )
            logger.debug(
                "Resolved secret from Google Secret Manager: %s/%s",
                project_or_scope,
                name,
            )
            return value.strip()
        except Exception as e:
            raise ValueError(
                f"Failed to fetch Google secret {project_or_scope}/{name}: {e}"
            ) from e


class EnvVarSecretsProvider(SecretsProvider):
    """Resolves secrets from environment variables.

    Useful for local development and CI where cloud secret managers are
    not available. The ``project_or_scope`` parameter is ignored.

    URI format: ``env::VAR_NAME``
    """

    def get_secret(
        self, name: str, project_or_scope: str = "", version: str = "latest"
    ) -> str:
        value = os.environ.get(name)
        if value is None:
            raise ValueError(
                f"Environment variable '{name}' is not set. "
                f"Set it or use a different secrets provider."
            )
        logger.debug("Resolved secret from environment variable: %s", name)
        return value
