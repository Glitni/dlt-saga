"""GCS storage client for the native_load adapter."""

import logging
from typing import Iterator, List, Optional, Union

from dlt_saga.pipelines.native_load.storage.base import StorageClient, StorageObject
from dlt_saga.pipelines.native_load.storage.matching import (
    PatternMatcher,
    relative_path,
    supports_delimiter_listing,
)

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
            pattern: Glob pattern(s) matched against each blob's path relative
                     to ``uri`` (e.g. "*.parquet" for the top level only,
                     "**/*.parquet" to recurse). A string or a list of patterns.
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
        matcher = PatternMatcher(pattern)

        list_kwargs: dict = {"prefix": prefix}
        if start_offset:
            list_kwargs["start_offset"] = start_offset
        if supports_delimiter_listing(matcher, prefix):
            # The pattern cannot reach below the prefix, so let GCS skip the
            # subtrees instead of listing and discarding them here.
            list_kwargs["delimiter"] = "/"

        logger.debug(
            "GCS list_blobs: bucket=%s prefix=%r start_offset=%r pattern=%r delimiter=%r",
            bucket_name,
            prefix,
            start_offset,
            pattern,
            list_kwargs.get("delimiter"),
        )

        for blob in bucket.list_blobs(**list_kwargs):
            rel_path = relative_path(blob.name, prefix)
            if not rel_path:
                continue
            if not matcher.matches(rel_path):
                continue
            yield StorageObject(
                name=blob.name,
                full_uri=f"gs://{bucket_name}/{blob.name}",
                size=blob.size or 0,
                generation=blob.generation or 0,
                updated=blob.updated,
            )
