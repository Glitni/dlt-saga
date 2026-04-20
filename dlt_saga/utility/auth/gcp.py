"""GCP authentication provider.

Wraps the existing gcloud_auth module behind the AuthProvider interface,
providing ADC validation and service account impersonation for GCP.
"""

import logging
from contextlib import contextmanager
from typing import Generator

from dlt_saga.utility.auth.providers import AuthenticationError, AuthProvider

logger = logging.getLogger(__name__)


class GcpAuthProvider(AuthProvider):
    """Google Cloud Platform authentication provider.

    Validates Application Default Credentials (ADC) and supports service
    account impersonation via gcloud CLI + ``google.auth`` monkey-patching.

    Requires the ``[gcp]`` optional dependency.

    Delegates to ``dlt_saga.utility.cli.gcloud_auth`` for the actual
    implementation, keeping all GCP-specific auth logic in one place.
    """

    def validate(self) -> None:
        """Check if GCP Application Default Credentials are configured.

        Raises:
            AuthenticationError: If ADC is not configured.
        """
        from dlt_saga.utility.optional_deps import require_optional

        require_optional("google.auth", "GCP authentication (auth_provider: gcp)")
        from dlt_saga.utility.cli.gcloud_auth import check_adc_configured

        if not check_adc_configured():
            raise AuthenticationError(
                "GCP credentials not configured. "
                "Run 'gcloud auth application-default login' to authenticate."
            )

    def supports_impersonation(self) -> bool:
        return True

    @contextmanager
    def impersonate(self, identity: str) -> Generator[None, None, None]:
        """Activate GCP service account impersonation.

        Sets up gcloud CLI impersonation config, patches ``google.auth.default()``,
        and tears down on exit.

        Args:
            identity: Service account email to impersonate
                (e.g., ``sa@project.iam.gserviceaccount.com``).

        Raises:
            AuthenticationError: If impersonation setup fails.
        """
        from dlt_saga.utility.cli.gcloud_auth import ImpersonationManager

        manager = ImpersonationManager()
        try:
            try:
                manager.setup_impersonation(identity)
            except Exception as e:
                raise AuthenticationError(
                    f"Failed to set up GCP impersonation for {identity}. {e}"
                ) from e
            yield
        finally:
            manager.teardown_impersonation()
