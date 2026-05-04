"""Storage client registry for the native_load adapter."""

from typing import Optional

from dlt_saga.pipelines.native_load.storage.base import StorageClient


def get_storage_client(
    uri: str,
    billing_project: Optional[str] = None,
    destination: Optional[object] = None,
) -> StorageClient:
    """Dispatch by URI scheme to the appropriate storage client.

    Args:
        uri: Source URI (gs://, s3://, or abfss://).
        billing_project: GCS billing project for requester-pays buckets.
        destination: Required for abfss:// URIs — must be a DatabricksDestination
                     so that listing flows through Databricks SQL LIST.
    """
    if uri.startswith("gs://"):
        from dlt_saga.pipelines.native_load.storage.gcs import GcsStorageClient

        return GcsStorageClient(billing_project=billing_project)

    if uri.startswith("abfss://"):
        if (
            destination is None
            or destination.__class__.__name__ != "DatabricksDestination"
        ):
            raise ValueError(
                "ABFSS native load requires the Databricks destination "
                "(listing flows through Databricks SQL LIST via the workspace's "
                "Unity Catalog external location). "
                "Ensure destination_type: databricks is set in profiles.yml."
            )
        from dlt_saga.pipelines.native_load.storage.adls import AdlsStorageClient

        return AdlsStorageClient(destination)  # type: ignore[arg-type]

    if uri.startswith("s3://"):
        raise NotImplementedError(
            "S3 native load is not implemented in v1; contributions welcome"
        )

    scheme = uri.split("://")[0] if "://" in uri else uri[:20]
    raise ValueError(f"Unsupported URI scheme: {scheme!r}")
