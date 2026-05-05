"""GCS storage client for the native_load adapter."""

import fnmatch
import logging
from typing import Iterator, List, Optional, Union

from dlt_saga.pipelines.native_load.storage.base import StorageClient, StorageObject

logger = logging.getLogger(__name__)


class GcsStorageClient(StorageClient):
    """Lists objects from Google Cloud Storage.

    Uses google-cloud-storage with prefix listing and optional start_offset
    for efficient cursor-mode discovery.
    """

    def __init__(self, billing_project: Optional[str] = None) -> None:
        from google.cloud import storage  # type: ignore[attr-defined]

        self._client = storage.Client(project=billing_project)

    def list_files(
        self,
        uri: str,
        pattern: Union[str, List[str]],
        start_offset: Optional[str] = None,
    ) -> Iterator[StorageObject]:
        """List GCS objects matching one or more glob patterns.

        Args:
            uri: gs://bucket/prefix/ root to list from.
            pattern: Glob pattern(s) matched against the blob's basename.
                     A string (e.g. "*.parquet") or a list of patterns.
            start_offset: Optional blob path (within bucket) to start listing from
                          (lexicographically inclusive).

        Yields:
            StorageObject for each matched blob, in lexicographic order.
        """
        if not uri.startswith("gs://"):
            raise ValueError(f"GcsStorageClient requires a gs:// URI, got: {uri!r}")

        path = uri[5:]  # strip "gs://"
        parts = path.split("/", 1)
        bucket_name = parts[0]
        prefix = parts[1] if len(parts) > 1 else ""

        bucket = self._client.bucket(bucket_name)

        list_kwargs: dict = {"prefix": prefix}
        if start_offset:
            list_kwargs["start_offset"] = start_offset

        logger.debug(
            "GCS list_blobs: bucket=%s prefix=%r start_offset=%r pattern=%r",
            bucket_name,
            prefix,
            start_offset,
            pattern,
        )

        patterns = [pattern] if isinstance(pattern, str) else pattern
        for blob in bucket.list_blobs(**list_kwargs):
            basename = blob.name.rsplit("/", 1)[-1]
            if not basename:
                continue
            if not any(fnmatch.fnmatch(basename, p) for p in patterns):
                continue
            yield StorageObject(
                name=blob.name,
                full_uri=f"gs://{bucket_name}/{blob.name}",
                size=blob.size or 0,
                generation=blob.generation or 0,
                updated=blob.updated,
            )
