"""Generic secret resolver supporting multiple secret providers.

This module provides a unified interface for resolving secrets from various providers
using a URI-style notation: provider::project_or_scope::secret_name

Supported formats:
- googlesecretmanager::project_id::secret_name - Google Cloud Secret Manager
- azurekeyvault::https://vault.azure.net::secret_name - Azure Key Vault
- env::SECRET_NAME - Environment variables (for local dev / CI)
- Direct values (no prefix) - Plain text values for dev/testing

The double-colon separator avoids ambiguity with URLs (which contain single colons).

Custom providers can be registered via SecretResolver.register_provider().
"""

from __future__ import annotations

import logging
import threading
from typing import TYPE_CHECKING, Any, Dict, Optional

if TYPE_CHECKING:
    from dlt_saga.utility.secrets.providers import SecretsProvider

logger = logging.getLogger(__name__)

# Thread-safe cache for resolved secret values
# Prevents redundant Secret Manager calls when multiple threads need the same secret
_secret_cache: Dict[str, str] = {}
_cache_lock = threading.Lock()

# Provider registry: maps URI prefix to SecretsProvider instance
_provider_registry: Dict[str, Any] = {}  # Any = SecretsProvider (avoid circular import)
_providers_initialized: bool = False


def _ensure_providers() -> None:
    """Lazily register built-in providers on first use."""
    global _providers_initialized
    if _providers_initialized:
        return

    from dlt_saga.utility.secrets.providers import (
        EnvVarSecretsProvider,
        GcpSecretsProvider,
    )

    _provider_registry.setdefault("googlesecretmanager", GcpSecretsProvider())
    _provider_registry.setdefault("env", EnvVarSecretsProvider())

    # Azure Key Vault — lazy import so the package is optional
    try:
        from dlt_saga.utility.secrets.azure import AzureKeyVaultSecretsProvider

        _provider_registry.setdefault("azurekeyvault", AzureKeyVaultSecretsProvider())
    except ImportError:
        pass  # azure extra not installed; provider registered on demand if used

    _providers_initialized = True


class SecretResolver:
    """Resolves secrets from various secret management providers.

    Providers are registered by URI prefix. Built-in providers:
    - ``googlesecretmanager`` — Google Cloud Secret Manager
    - ``env`` — Environment variables

    Custom providers can be registered via ``register_provider()``.
    """

    @staticmethod
    def register_provider(prefix: str, provider: "SecretsProvider") -> None:
        """Register a custom secrets provider for a URI prefix.

        Args:
            prefix: URI prefix (e.g., ``"vault"`` for ``vault:path:secret``).
            provider: SecretsProvider instance to handle this prefix.
        """
        _provider_registry[prefix] = provider

    @staticmethod
    def get_provider(prefix: str) -> Optional["SecretsProvider"]:
        """Get the registered provider for a URI prefix.

        Args:
            prefix: URI prefix to look up.

        Returns:
            The registered SecretsProvider, or None if not found.
        """
        _ensure_providers()
        return _provider_registry.get(prefix)

    @staticmethod
    def is_secret_uri(value: Any) -> bool:
        """Check if a value is a secret URI.

        Args:
            value: Value to check (str or SecretStr)

        Returns:
            True if value is a secret URI string, False otherwise
        """
        from dlt_saga.utility.secrets.secret_str import SecretStr

        if isinstance(value, SecretStr):
            value = value.get_secret_value()
        if not isinstance(value, str):
            return False

        _ensure_providers()
        return any(value.startswith(f"{prefix}::") for prefix in _provider_registry)

    @staticmethod
    def _unwrap(value: Any) -> Any:
        """Unwrap SecretStr, returning the raw string or the original value."""
        from dlt_saga.utility.secrets.secret_str import SecretStr

        if isinstance(value, SecretStr):
            value = value.get_secret_value()
        return value

    @staticmethod
    def resolve(value: Any) -> Any:
        """Resolve a secret if value is in secret URI format, otherwise return as-is.

        Uses process-level caching to prevent redundant API calls when multiple
        threads need the same secret (e.g., during parallel extraction).

        Args:
            value: Value to resolve (can be str, SecretStr, dict, list, or any other type)

        Returns:
            Resolved secret value or original value if not a secret URI

        Raises:
            ValueError: If secret URI format is invalid or secret cannot be fetched
        """
        value = SecretResolver._unwrap(value)

        # Handle non-string values (return as-is)
        if not isinstance(value, str):
            return value

        # Handle plain text values (not secret URIs)
        if not SecretResolver.is_secret_uri(value):
            return value

        # Fast path: check cache without lock
        if value in _secret_cache:
            logger.debug("Using cached secret: %s", value)
            return _secret_cache[value]

        # Slow path: resolve with lock to prevent thundering herd
        with _cache_lock:
            # Double-check after acquiring lock
            if value in _secret_cache:
                return _secret_cache[value]

            try:
                # Parse URI: "provider::name" or "provider::scope::name"
                parts = value.split("::", 2)
                if len(parts) == 2:
                    provider_prefix, secret_name = parts
                    project_or_scope = ""
                elif len(parts) == 3:
                    provider_prefix, project_or_scope, secret_name = parts
                else:
                    raise ValueError(
                        f"Invalid secret URI format: {value}. "
                        f"Expected: provider::name or provider::scope::name"
                    )

                # Dispatch to registered provider
                _ensure_providers()
                provider = _provider_registry.get(provider_prefix)
                if provider is None:
                    raise ValueError(
                        f"No secrets provider registered for prefix '{provider_prefix}'. "
                        f"Registered: {list(_provider_registry.keys())}"
                    )

                resolved = provider.get_secret(
                    name=secret_name,
                    project_or_scope=project_or_scope,
                )

                # Cache the resolved value
                _secret_cache[value] = resolved
                return resolved

            except ValueError:
                raise
            except Exception as e:
                raise ValueError(f"Failed to resolve secret '{value}': {e}") from e

    @staticmethod
    def resolve_dict(data: dict) -> dict:
        """Recursively resolve all secrets in a dictionary.

        Args:
            data: Dictionary that may contain secret URIs

        Returns:
            Dictionary with all secrets resolved

        Example:
            >>> data = {
            ...     "username": "googlesecretmanager::my-project::username",
            ...     "password": "plain_password",
            ...     "nested": {
            ...         "api_key": "googlesecretmanager::my-project::api-key"
            ...     }
            ... }
            >>> resolved = SecretResolver.resolve_dict(data)
            >>> # All secret URIs are replaced with actual secret values
        """
        resolved: Dict[Any, Any] = {}
        for key, value in data.items():
            if isinstance(value, dict):
                resolved[key] = SecretResolver.resolve_dict(value)
            elif isinstance(value, list):
                resolved[key] = [SecretResolver.resolve(item) for item in value]
            else:
                resolved[key] = SecretResolver.resolve(value)
        return resolved


# Convenience function for simple usage
def resolve_secret(value: Any) -> Any:
    """Convenience function to resolve a single secret value.

    Args:
        value: Value to resolve (string, dict, list, or other)

    Returns:
        Resolved value (secret fetched if URI format, otherwise original value)

    Example:
        >>> username = resolve_secret("googlesecretmanager::my-project::username")
        >>> password = resolve_secret("plain_password")  # Returns as-is
        >>> token = resolve_secret("env::MY_API_TOKEN")  # From environment
    """
    return SecretResolver.resolve(value)


def _reset_for_testing() -> None:
    """Reset provider registry and cache. For testing only."""
    global _providers_initialized
    _secret_cache.clear()
    _provider_registry.clear()
    _providers_initialized = False
