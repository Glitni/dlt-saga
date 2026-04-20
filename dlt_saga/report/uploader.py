"""Report uploaders for sharing observability reports.

Supports uploading the generated HTML report to remote storage backends.
Currently implemented: Google Cloud Storage (GCS).
Planned: Amazon S3 (s3://), Azure Blob Storage (az://).
"""

import logging

logger = logging.getLogger(__name__)

# URI schemes recognised as remote destinations.
REMOTE_SCHEMES = ("gs://", "s3://", "az://", "slack://")


def is_remote_uri(output: str) -> bool:
    """Return True if *output* is a remote storage URI rather than a local path."""
    return any(output.startswith(scheme) for scheme in REMOTE_SCHEMES)


def upload_report(local_path: str, destination_uri: str) -> str:
    """Upload a local report file to a remote URI and return the canonical URI.

    Dispatches to the appropriate backend based on the URI scheme:

    * ``gs://bucket/path``    → Google Cloud Storage
    * ``s3://bucket/path``    → Amazon S3 *(not yet implemented)*
    * ``az://container/path`` → Azure Blob Storage *(not yet implemented)*

    Args:
        local_path: Path to the locally-generated HTML report.
        destination_uri: Remote URI to upload to.

    Returns:
        The canonical remote URI of the uploaded object.

    Raises:
        ValueError: If the URI scheme is not recognised.
        NotImplementedError: If the backend is recognised but not yet implemented.
    """
    if destination_uri.startswith("gs://"):
        remainder = destination_uri[len("gs://") :]
        bucket_name, _, object_name = remainder.partition("/")
        if not object_name:
            object_name = "saga_report.html"
        return _upload_to_gcs(local_path, bucket_name, object_name)

    if destination_uri.startswith("s3://"):
        raise NotImplementedError(
            "S3 upload is not yet implemented. "
            "Contributions welcome — see dlt_saga/report/uploader.py."
        )

    if destination_uri.startswith("az://"):
        raise NotImplementedError(
            "Azure Blob Storage upload is not yet implemented. "
            "Contributions welcome — see dlt_saga/report/uploader.py."
        )

    if destination_uri.startswith("slack://"):
        raise NotImplementedError(
            "Slack upload is not yet implemented. "
            "Expected format: slack://<channel-name-or-id> "
            "Contributions welcome — see dlt_saga/report/uploader.py."
        )

    raise ValueError(
        f"Unrecognised output URI scheme: '{destination_uri}'. "
        f"Supported schemes: {', '.join(REMOTE_SCHEMES)}"
    )


def _upload_to_gcs(
    local_path: str,
    bucket_name: str,
    object_name: str,
) -> str:
    """Upload a local file to GCS and return the gs:// URI.

    Args:
        local_path: Path to the local file to upload.
        bucket_name: GCS bucket name (without gs:// prefix).
        object_name: Object path within the bucket (e.g. ``saga-report/report.html``).

    Returns:
        ``gs://<bucket>/<object>`` URI of the uploaded object.

    Raises:
        ImportError: If the gcp extras are not installed (``pip install dlt-saga[gcp]``).
        google.cloud.exceptions.GoogleCloudError: On upload failure.
    """
    try:
        from google.cloud import storage  # type: ignore[attr-defined]
    except ImportError as exc:
        raise ImportError(
            "google-cloud-storage is required to upload reports to GCS. "
            "Install it with: pip install dlt-saga[gcp]"
        ) from exc

    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(object_name)

    blob.content_type = "text/html; charset=utf-8"
    blob.upload_from_filename(local_path)

    gcs_uri = f"gs://{bucket_name}/{object_name}"
    logger.info(f"Report uploaded to {gcs_uri}")
    return gcs_uri
