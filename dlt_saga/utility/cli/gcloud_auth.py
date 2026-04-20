"""Google Cloud authentication utilities.

This module provides utilities for Google Cloud authentication, including:
- Application Default Credentials (ADC) detection and validation
- Service account impersonation setup and teardown for local development
"""

import logging
import os
from pathlib import Path
from typing import Optional

from dlt_saga.utility.cli.logging import YELLOW, colorize

logger = logging.getLogger(__name__)


def get_impersonated_credentials(service_account: str):
    """Get impersonated credentials for a service account.

    Args:
        service_account: Service account email to impersonate

    Returns:
        Impersonated credentials object that can be used with Google Cloud clients

    Raises:
        ImportError: If google-auth library is not available
    """
    import google.auth
    from google.auth import impersonated_credentials

    # Get source credentials (user's ADC)
    source_credentials, _ = google.auth.default()

    # Create impersonated credentials
    target_scopes = ["https://www.googleapis.com/auth/cloud-platform"]

    credentials = impersonated_credentials.Credentials(
        source_credentials=source_credentials,
        target_principal=service_account,
        target_scopes=target_scopes,
        lifetime=3600,  # 1 hour
    )

    logger.debug(f"Created impersonated credentials for {service_account}")
    return credentials


def patch_google_auth_default():
    """Monkey-patch google.auth.default() to return impersonated credentials.

    This makes all libraries that use google.auth.default() automatically use
    impersonated credentials when GOOGLE_IMPERSONATE_SERVICE_ACCOUNT is set.
    """
    import google.auth

    # Store the original default function
    _original_default = google.auth.default
    _cached_credentials = {}

    def impersonating_default(*args, **kwargs):
        """Wrapper for google.auth.default() that returns impersonated credentials."""
        service_account = os.getenv("GOOGLE_IMPERSONATE_SERVICE_ACCOUNT")

        if service_account:
            # Return cached credentials if available for this service account
            if service_account in _cached_credentials:
                credentials, project_id = _cached_credentials[service_account]
                logger.debug(
                    f"google.auth.default() returning cached impersonated credentials for {service_account}"
                )
                return credentials, project_id

            # Get source credentials using the original function
            source_credentials, project_id = _original_default(*args, **kwargs)

            # Create impersonated credentials
            from google.auth import impersonated_credentials

            # Extract scopes from kwargs or use default
            scopes = (
                kwargs.get("scopes")
                or kwargs.get("default_scopes")
                or ["https://www.googleapis.com/auth/cloud-platform"]
            )

            credentials = impersonated_credentials.Credentials(
                source_credentials=source_credentials,
                target_principal=service_account,
                target_scopes=scopes,
                lifetime=3600,  # 1 hour
            )

            _cached_credentials[service_account] = (credentials, project_id)
            logger.debug(
                f"google.auth.default() returning impersonated credentials for {service_account}"
            )
            return credentials, project_id
        else:
            # No impersonation - use original function
            return _original_default(*args, **kwargs)

    # Replace google.auth.default with our wrapper
    google.auth.default = impersonating_default
    logger.debug("Patched google.auth.default() to support impersonation")


def get_adc_path() -> Path:
    """Get the path to the application default credentials file.

    Returns:
        Path to ADC file based on OS
    """
    if os.name == "nt":  # Windows
        # Windows: %APPDATA%\gcloud\application_default_credentials.json
        appdata = os.getenv("APPDATA", "")
        return Path(appdata) / "gcloud" / "application_default_credentials.json"
    else:  # Linux/Mac
        # Unix: ~/.config/gcloud/application_default_credentials.json
        home = Path.home()
        return home / ".config" / "gcloud" / "application_default_credentials.json"


def check_adc_configured() -> bool:
    """Check if Google credentials are available via any supported mechanism.

    Checks in order:
    1. ``GOOGLE_APPLICATION_CREDENTIALS`` env var (service account key file)
    2. ADC file at the gcloud default path
    3. GCP metadata server (Cloud Run, GCE, GKE workload identity)

    Returns:
        True if any credential source is usable, False otherwise
    """
    # 1. Explicit key file
    if os.getenv("GOOGLE_APPLICATION_CREDENTIALS"):
        return True

    # 2. gcloud ADC file
    if get_adc_path().exists():
        return True

    # 3. Metadata server (Cloud Run / GCE / GKE)
    try:
        import google.auth
        import google.auth.exceptions

        google.auth.default()
        return True
    except Exception:
        return False


class ImpersonationManager:
    """Manages service account impersonation lifecycle."""

    def __init__(self):
        """Initialize impersonation manager."""
        self.active_impersonation: Optional[str] = None

    def _activate_impersonation(self, service_account: str) -> None:
        """Activate impersonation by setting env var and patching google.auth."""
        os.environ["GOOGLE_IMPERSONATE_SERVICE_ACCOUNT"] = service_account
        logger.debug(
            f"Set GOOGLE_IMPERSONATE_SERVICE_ACCOUNT environment variable to {service_account}"
        )
        patch_google_auth_default()
        self.active_impersonation = service_account

    def _verify_gcloud_credentials(self) -> None:
        """Verify gcloud auth and ADC are configured.

        Raises:
            RuntimeError: If ADC is not configured.
        """
        if not check_adc_configured():
            raise RuntimeError(
                "Application default credentials not configured. "
                "Please run 'gcloud auth application-default login' first."
            )

    def _verify_impersonation_in_process(self, service_account: str) -> None:
        """Verify impersonation works by requesting credentials in-process.

        Raises:
            RuntimeError: If credential refresh fails.
        """
        import google.auth
        import google.auth.transport.requests

        credentials, project = google.auth.default()
        # Force a token refresh to validate the impersonation chain
        credentials.refresh(google.auth.transport.requests.Request())
        logger.info(f"Impersonation configured successfully for {service_account}")

    def setup_impersonation(self, service_account: str) -> None:
        """Set up service account impersonation.

        Uses in-process monkey-patching of ``google.auth.default()`` so all
        downstream clients (BigQuery, Sheets, Secret Manager) automatically
        use impersonated credentials.  Does NOT modify gcloud config on disk,
        which previously contaminated the ADC file and caused permission
        errors in subsequent non-impersonated runs.

        Args:
            service_account: Service account email to impersonate

        Raises:
            RuntimeError: If prerequisites or verification fail.
        """
        logger.info(
            f"Setting up impersonation for: {colorize(service_account, YELLOW)}"
        )

        # Check if already impersonating (in-process)
        if self.active_impersonation == service_account:
            logger.debug(f"Already impersonating {service_account}, no action needed")
            return

        # Verify prerequisites
        self._verify_gcloud_credentials()

        # Activate in-process impersonation (env var + monkey-patch)
        self._activate_impersonation(service_account)

        # Verify impersonation works by creating test credentials
        self._verify_impersonation_in_process(service_account)

    def teardown_impersonation(self) -> bool:
        """Tear down service account impersonation.

        Cleans up the in-process impersonation state: unsets the environment
        variable so the monkey-patched ``google.auth.default()`` falls
        through to the original credentials.

        Returns:
            True if teardown successful or not needed, False otherwise
        """
        if not self.active_impersonation:
            logger.debug("No active impersonation to tear down")
            return True

        try:
            logger.debug(f"Tearing down impersonation for: {self.active_impersonation}")

            # Unset the environment variable — the monkey-patched
            # google.auth.default() checks this at call time, so unsetting
            # it makes all subsequent calls return the original credentials.
            if "GOOGLE_IMPERSONATE_SERVICE_ACCOUNT" in os.environ:
                del os.environ["GOOGLE_IMPERSONATE_SERVICE_ACCOUNT"]
                logger.debug(
                    "Unset GOOGLE_IMPERSONATE_SERVICE_ACCOUNT environment variable"
                )

            logger.info("Impersonation torn down successfully")
            self.active_impersonation = None
            return True

        except Exception as e:
            logger.error(f"Unexpected error during impersonation teardown: {e}")
            return False

    def __enter__(self):
        """Context manager entry - no-op, setup must be called explicitly."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit - tear down impersonation."""
        self.teardown_impersonation()
        return False  # Don't suppress exceptions
