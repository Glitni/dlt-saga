"""Amazon S3 storage client for the native_load adapter.

Lists objects via boto3 for file discovery and incremental dedup. This client is
used only for *listing* — BigQuery reads the object contents itself through a
BigQuery Omni cross-cloud connection, so the AWS credentials here need only
``s3:ListBucket`` on the source prefixes.

``start_offset`` is ignored — S3 has no lexicographic listing equivalent. Use
``partition_prefix_pattern`` on the pipeline config for efficient
date-partitioned source traversal instead (mirrors the ADLS client).
"""

import datetime
import fnmatch
import logging
from typing import Iterator, List, Optional, Union

from dlt_saga.pipelines.native_load.storage.base import StorageClient, StorageObject
from dlt_saga.utility.secrets import resolve_secret

logger = logging.getLogger(__name__)


class S3StorageClient(StorageClient):
    """Lists objects from Amazon S3 via boto3.

    Credentials are supplied as plain values or saga secret URIs (e.g.
    ``googlesecretmanager::project::secret``) and resolved on init. Only listing
    permission is required; content reads happen in BigQuery via the Omni
    connection.
    """

    def __init__(
        self,
        aws_access_key_id: Optional[str] = None,
        aws_secret_access_key: Optional[str] = None,
        region_name: Optional[str] = None,
    ) -> None:
        try:
            import boto3  # type: ignore[import-untyped]
        except ImportError as exc:
            raise ImportError(
                "S3 native load requires boto3. Install the aws extra: "
                "`pip install dlt-saga[aws]`."
            ) from exc

        self._client = boto3.client(
            "s3",
            aws_access_key_id=resolve_secret(aws_access_key_id),
            aws_secret_access_key=resolve_secret(aws_secret_access_key),
            region_name=region_name,
        )

    def list_files(
        self,
        uri: str,
        pattern: Union[str, List[str]],
        start_offset: Optional[str] = None,
    ) -> Iterator[StorageObject]:
        """List S3 objects matching one or more glob patterns.

        Args:
            uri: s3://bucket/prefix/ root to list from.
            pattern: Glob pattern(s) matched against the object key's basename.
            start_offset: Not used for S3; silently ignored.

        Yields:
            StorageObject for each matched object.
        """
        if not uri.startswith("s3://"):
            raise ValueError(f"S3StorageClient requires an s3:// URI, got: {uri!r}")

        path = uri[5:]  # strip "s3://"
        parts = path.split("/", 1)
        bucket_name = parts[0]
        prefix = parts[1] if len(parts) > 1 else ""

        patterns = [pattern] if isinstance(pattern, str) else list(pattern)

        logger.debug(
            "S3 list_objects_v2: bucket=%s prefix=%r pattern=%r",
            bucket_name,
            prefix,
            patterns,
        )

        paginator = self._client.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=bucket_name, Prefix=prefix):
            for obj in page.get("Contents", []):
                key = obj["Key"]
                basename = key.rsplit("/", 1)[-1]
                if not basename:
                    continue  # "directory" placeholder key
                if not any(fnmatch.fnmatch(basename, p) for p in patterns):
                    continue
                last_modified = obj.get("LastModified")
                yield StorageObject(
                    name=key,
                    full_uri=f"s3://{bucket_name}/{key}",
                    size=int(obj.get("Size", 0) or 0),
                    generation=_mtime_to_generation(last_modified),
                    updated=last_modified,
                )


def _mtime_to_generation(mtime: object) -> int:
    """Convert an S3 LastModified datetime to an epoch-millis generation surrogate.

    boto3 returns LastModified as a timezone-aware datetime. Normalising to
    epoch-millis means re-uploading an object (new LastModified) yields a new
    generation, matching the GCS generation contract used for dedup.
    """
    if isinstance(mtime, datetime.datetime):
        return int(mtime.timestamp() * 1000)
    if isinstance(mtime, int):
        return mtime
    return 0
