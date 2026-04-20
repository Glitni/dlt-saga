"""Auth provider abstraction for pluggable authentication backends.

Defines the AuthProvider interface and built-in implementations:
- GcpAuthProvider: Google Cloud ADC + service account impersonation
- NoopAuthProvider: No-op for environments without cloud auth (local dev / CI)

Custom providers can implement AuthProvider for Azure, AWS, etc.
"""

import logging
from abc import ABC, abstractmethod
from contextlib import contextmanager
from typing import Dict, Generator, Optional, Type

logger = logging.getLogger(__name__)


class AuthenticationError(Exception):
    """Raised when credential validation or impersonation fails.

    This is a configuration-level error (missing/expired credentials,
    impersonation failure) — not an unexpected bug.  CLI commands catch
    it and display the message without a traceback.
    """


class AuthProvider(ABC):
    """Abstract base class for authentication backends.

    Implementations handle credential validation and identity impersonation
    for a specific cloud provider (GCP, Azure, AWS, etc.).
    """

    @abstractmethod
    def validate(self) -> None:
        """Check if credentials are configured and usable.

        Raises:
            AuthenticationError: If credentials are missing or unusable.
        """

    @abstractmethod
    def supports_impersonation(self) -> bool:
        """Whether this provider supports identity impersonation.

        Returns:
            True if impersonation is supported.
        """

    @abstractmethod
    @contextmanager
    def impersonate(self, identity: str) -> Generator[None, None, None]:
        """Context manager that activates impersonation for the given identity.

        Args:
            identity: Provider-specific identity string (e.g., service account
                email for GCP, role ARN for AWS).

        Raises:
            AuthenticationError: If impersonation setup fails.
            NotImplementedError: If impersonation is not supported.

        Yields:
            Nothing. Impersonation is active for the duration of the block.
        """
        yield  # pragma: no cover


class NoopAuthProvider(AuthProvider):
    """Auth provider that performs no authentication.

    Useful for local development, CI, or environments where authentication
    is handled externally (e.g., workload identity, pod service accounts).
    """

    def validate(self) -> None:
        """No-op — nothing to validate."""
        logger.debug("NoopAuthProvider: skipping credential validation")

    def supports_impersonation(self) -> bool:
        return False

    @contextmanager
    def impersonate(self, identity: str) -> Generator[None, None, None]:
        raise NotImplementedError(
            "NoopAuthProvider does not support impersonation. "
            "Set auth_provider in profiles.yml (e.g., auth_provider: gcp) "
            "to use service account impersonation."
        )
        yield  # pragma: no cover


# ---------------------------------------------------------------------------
# Provider registry & factory
# ---------------------------------------------------------------------------

# Maps provider name → AuthProvider class.
# Names are either explicit provider names ("gcp", "azure", "aws") or
# destination_type values used as fallback hints ("bigquery").
_auth_registry: Dict[str, Type[AuthProvider]] = {}


def register_auth_provider(name: str, cls: Type[AuthProvider]) -> None:
    """Register an auth provider class under a name.

    Args:
        name: Provider name (e.g., ``"gcp"``, ``"bigquery"``).
        cls: AuthProvider subclass.
    """
    _auth_registry[name] = cls


def get_auth_provider(
    auth_provider: Optional[str] = None,
    destination_type: Optional[str] = None,
) -> AuthProvider:
    """Resolve an AuthProvider instance.

    Resolution order:
    1. Explicit ``auth_provider`` name (e.g., ``"gcp"``, ``"azure"``)
    2. Fallback: ``destination_type`` as a registry hint (e.g., ``"bigquery"`` → GCP)
    3. ``NoopAuthProvider``

    This two-level lookup lets ``auth_provider`` decouple auth from destination.
    A Databricks-on-GCP profile sets ``auth_provider: gcp`` regardless of
    ``destination_type: databricks``.

    Args:
        auth_provider: Explicit provider name from ``profiles.yml``, or None.
        destination_type: Destination type used as fallback hint when
            ``auth_provider`` is not set.

    Returns:
        An AuthProvider instance.
    """
    _ensure_builtins()

    # 1. Explicit provider name
    if auth_provider and auth_provider in _auth_registry:
        provider_cls = _auth_registry[auth_provider]
        logger.debug(
            "Using auth provider %s (explicit auth_provider=%s)",
            provider_cls.__name__,
            auth_provider,
        )
        return provider_cls()

    # 2. Fallback: destination_type as hint
    if destination_type and destination_type in _auth_registry:
        provider_cls = _auth_registry[destination_type]
        logger.debug(
            "Using auth provider %s (inferred from destination_type=%s)",
            provider_cls.__name__,
            destination_type,
        )
        return provider_cls()

    # 3. No match
    logger.debug(
        "No auth provider for auth_provider=%s, destination_type=%s — using NoopAuthProvider",
        auth_provider,
        destination_type,
    )
    return NoopAuthProvider()


_builtins_registered = False


def _ensure_builtins() -> None:
    """Lazily register built-in auth providers on first use."""
    global _builtins_registered
    if _builtins_registered:
        return
    _builtins_registered = True

    from dlt_saga.utility.auth.gcp import GcpAuthProvider

    # GCP auth for BigQuery destinations
    register_auth_provider("bigquery", GcpAuthProvider)
    # Allow explicit "gcp" name too
    register_auth_provider("gcp", GcpAuthProvider)

    # Databricks auth — lazy import so the azure extra is optional
    from dlt_saga.utility.auth.databricks import DatabricksAuthProvider

    register_auth_provider("databricks", DatabricksAuthProvider)
