"""Databricks authentication provider.

Delegates all credential handling to the ``databricks-sdk``'s ``Config``
class, which supports PAT, OAuth M2M (service principal), and OAuth U2M
(interactive browser login) uniformly.

Auth modes:
    ``pat``           — static personal access token (``access_token`` in profile)
    ``m2m``           — service principal OAuth (``client_id`` / ``client_secret``)
    ``u2m``           — interactive browser OAuth; opens browser on first run,
                       token cached in ``~/.databricks/<hash>.json`` by the SDK
    ``azure_default`` — Azure-native credential chain (``DefaultAzureCredential``);
                       reads ``AZURE_CLIENT_ID`` / ``AZURE_CLIENT_SECRET`` /
                       ``AZURE_TENANT_ID`` like all other Azure services in this
                       framework (Key Vault, ADLS).  Also works with managed
                       identity when running on Azure compute.
    omitted           — SDK auto-detects from environment / token cache / env vars
"""

from __future__ import annotations

import logging
from contextlib import contextmanager
from typing import Any, Generator, Optional

from dlt_saga.utility.auth.providers import AuthenticationError, AuthProvider

logger = logging.getLogger(__name__)


def _build_sdk_config(
    host: str,
    auth_mode: Optional[str],
    access_token: Optional[str],
    client_id: Optional[str],
    client_secret: Optional[str],
) -> Any:
    """Build a ``databricks.sdk.config.Config`` for the given profile settings.

    Args:
        host: Workspace URL, e.g. ``https://adb-1234.12.azuredatabricks.net``.
        auth_mode: One of ``"pat"``, ``"m2m"``, ``"u2m"``, or ``None``.
        access_token: PAT value (PAT mode only).
        client_id: Service principal app ID (M2M mode).
        client_secret: Service principal secret (M2M mode).

    Returns:
        ``databricks.sdk.config.Config`` instance.

    Raises:
        ImportError: If ``databricks-sdk`` is not installed.
    """
    try:
        from databricks.sdk.config import Config
    except ImportError:
        raise ImportError(
            "Databricks auth requires 'dlt-saga[databricks]'. "
            "Run: pip install 'dlt-saga[databricks]'"
        ) from None

    kwargs: dict = {"host": host}

    if auth_mode == "pat":
        if access_token:
            kwargs["token"] = access_token
    elif auth_mode == "m2m":
        if client_id:
            kwargs["client_id"] = client_id
        if client_secret:
            kwargs["client_secret"] = client_secret
    elif auth_mode == "u2m":
        # "external-browser" activates the SDK's U2M credentials provider,
        # which checks the token cache and opens a browser when needed.
        kwargs["auth_type"] = "external-browser"
    # None: let the SDK's DefaultCredentials chain decide

    return Config(**kwargs)


# Azure AD resource (audience) for Databricks — fixed across all Azure tenants.
# See: https://learn.microsoft.com/en-us/azure/databricks/dev-tools/api/latest/aad/
_DATABRICKS_AZURE_RESOURCE = "2ff814a6-3304-4ab8-85cb-cd0e6f879c1d"


def _get_token_via_azure_default(host: str) -> str:
    """Obtain a Databricks access token using ``DefaultAzureCredential``.

    This is the Azure-native equivalent of GCP Application Default Credentials.
    Reads ``AZURE_CLIENT_ID`` / ``AZURE_CLIENT_SECRET`` / ``AZURE_TENANT_ID``
    (service principal) or uses managed identity when running on Azure compute.

    Args:
        host: Workspace URL (used only in error messages).

    Returns:
        Bearer token string (without the ``Bearer `` prefix).

    Raises:
        AuthenticationError: If authentication fails or azure-identity is missing.
    """
    try:
        from azure.identity import DefaultAzureCredential
    except ImportError:
        raise ImportError(
            "azure_default auth mode requires 'dlt-saga[azure]'. "
            "Run: pip install 'dlt-saga[azure]'"
        ) from None

    scope = f"{_DATABRICKS_AZURE_RESOURCE}/.default"
    azure_identity_logger = logging.getLogger("azure.identity")
    original_level = azure_identity_logger.level
    try:
        azure_identity_logger.setLevel(logging.ERROR)
        credential = DefaultAzureCredential()
        token = credential.get_token(scope)
        return token.token
    except ImportError:
        raise
    except Exception as e:
        raise AuthenticationError(
            f"Databricks azure_default authentication failed for '{host}': {e}. "
            "Set AZURE_CLIENT_ID / AZURE_CLIENT_SECRET / AZURE_TENANT_ID for "
            "service principal auth, or run 'az login' for developer workstations."
        ) from e
    finally:
        azure_identity_logger.setLevel(original_level)


def get_databricks_token(
    host: str,
    auth_mode: Optional[str],
    access_token: Optional[str],
    client_id: Optional[str],
    client_secret: Optional[str],
) -> str:
    """Obtain a current access token for the Databricks SQL connector.

    For PAT mode, returns the static token.  For OAuth modes, calls
    ``config.authenticate()`` which handles token refresh internally.

    Args:
        host: Workspace URL (with https:// prefix).
        auth_mode: Auth mode string, or None for SDK auto-detection.
        access_token: PAT value (PAT mode).
        client_id: Service principal client ID (M2M mode).
        client_secret: Service principal secret (M2M mode).

    Returns:
        Bearer token string (without the ``Bearer `` prefix).

    Raises:
        AuthenticationError: If authentication fails.
    """
    if auth_mode == "pat" and access_token:
        return access_token

    if auth_mode == "azure_default":
        return _get_token_via_azure_default(host)

    if auth_mode == "u2m":
        logger.debug(
            "Databricks U2M authentication: a browser window should open. "
            "If it does not, run: databricks auth login --host %s",
            host,
        )

    try:
        config = _build_sdk_config(
            host=host,
            auth_mode=auth_mode,
            access_token=access_token,
            client_id=client_id,
            client_secret=client_secret,
        )
        headers = config.authenticate()
        auth_header = headers.get("Authorization", "")
        if not auth_header.startswith("Bearer "):
            raise AuthenticationError(
                "Databricks SDK did not return a Bearer token. "
                "Check your profile credentials (auth_mode, client_id, client_secret)."
            )
        return auth_header.removeprefix("Bearer ")
    except AuthenticationError:
        raise
    except ImportError:
        raise
    except Exception as e:
        raise AuthenticationError(
            f"Databricks authentication failed: {e}. "
            "Check your profile's server_hostname, auth_mode, and credentials."
        ) from e


class DatabricksAuthProvider(AuthProvider):
    """Auth provider that delegates to the Databricks SDK.

    Validates connectivity by attempting to authenticate against the
    workspace.  Does not support identity impersonation — use Unity Catalog
    RBAC and service principal credentials in profiles.yml instead.
    """

    def validate(self) -> None:
        """Validate that Databricks credentials are configured and usable.

        Raises:
            AuthenticationError: If authentication fails.
        """
        logger.debug("DatabricksAuthProvider: credential validation deferred to SDK")

    def supports_impersonation(self) -> bool:
        return False

    @contextmanager
    def impersonate(self, identity: str) -> Generator[None, None, None]:
        raise NotImplementedError(
            "DatabricksAuthProvider does not support impersonation. "
            "Configure a service principal via 'client_id' / 'client_secret' "
            "in profiles.yml (auth_mode: m2m) for production automation."
        )
        yield  # pragma: no cover
