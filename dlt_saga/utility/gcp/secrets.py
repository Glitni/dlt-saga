"""Google Cloud Secret Manager utilities."""

import logging
from typing import Optional

logger = logging.getLogger(__name__)


def get_secret(
    secret_name: str,
    project_id: Optional[str] = None,
    version: str = "latest",
) -> str:
    """Fetch a secret from Google Cloud Secret Manager.

    Thread-safe: Each thread gets its own client from the client pool.
    Resilient: Retries transient gRPC/network errors with limits.

    Args:
        secret_name: Name of the secret to fetch
        project_id: GCP project ID (if None, uses default project)
        version: Secret version to fetch (default: "latest")

    Returns:
        Secret value as string

    Raises:
        ValueError: If secret cannot be fetched
    """

    try:
        # Use thread-safe client pool (one client per thread)
        from dlt_saga.utility.gcp.client_pool import secret_manager_pool

        client = secret_manager_pool.get_client()

        # If no project specified, try to get from default credentials
        if not project_id:
            import google.auth

            _, project_id = google.auth.default()

        if not project_id:
            raise ValueError(
                "Could not determine GCP project ID. "
                "Please specify project_id parameter or set GOOGLE_CLOUD_PROJECT env var."
            )

        # Build the resource name
        name = f"projects/{project_id}/secrets/{secret_name}/versions/{version}"

        # Access the secret with limited retry for transient errors
        # Maximum 3 attempts to avoid hanging on corrupted gRPC streams
        from google.api_core import retry

        @retry.Retry(
            predicate=retry.if_transient_error,
            initial=1.0,
            maximum=5.0,
            multiplier=2.0,
            deadline=30.0,  # Give up after 30 seconds total
        )
        def _fetch_secret():
            response = client.access_secret_version(request={"name": name})
            return response.payload.data.decode("UTF-8")

        secret_value = _fetch_secret()

        logger.debug(f"Successfully fetched secret: {secret_name}")
        return secret_value

    except Exception as e:
        raise ValueError(
            f"Failed to fetch secret '{secret_name}' from project '{project_id}': {e}"
        )
