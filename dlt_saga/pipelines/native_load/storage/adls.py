"""ADLS Gen2 storage client for the native_load adapter.

Uses Databricks SQL ``LIST`` to enumerate files via the workspace's Unity Catalog
external location — no Azure SDK or separate auth surface required.  The active
SQL warehouse that saga already holds for COPY INTO is reused for listing.
"""

import logging
from typing import TYPE_CHECKING, Iterator, List, Optional, Union

from dlt_saga.pipelines.native_load.storage.base import StorageClient, StorageObject
from dlt_saga.pipelines.native_load.storage.matching import (
    PatternMatcher,
    relative_path,
)

if TYPE_CHECKING:
    from dlt_saga.destinations.databricks.destination import DatabricksDestination

logger = logging.getLogger(__name__)


class AdlsStorageClient(StorageClient):
    """Lists ADLS Gen2 objects via Databricks SQL ``LIST``.

    The active DatabricksDestination's SQL connection is reused.  The workspace
    must have a Unity Catalog external location (or storage credential) granting
    LIST + READ_FILES on the configured ``abfss://`` URI.

    ``start_offset`` is ignored — ADLS has no lexicographic listing equivalent.
    Use ``partition_prefix_pattern`` on the pipeline config for efficient
    date-partitioned source traversal instead.
    """

    def __init__(self, destination: "DatabricksDestination") -> None:
        self._destination = destination

    def list_files(
        self,
        uri: str,
        pattern: Union[str, List[str]],
        start_offset: Optional[str] = None,
    ) -> Iterator[StorageObject]:
        """List ADLS objects matching pattern(s) under a URI prefix.

        Args:
            uri: abfss:// URI prefix to list from.
            pattern: Glob pattern(s) matched against each path relative to
                     ``uri`` (e.g. "*.parquet" for the top level only,
                     "**/*.parquet" to recurse).
            start_offset: Not used for ADLS; silently ignored.
        """
        if not uri.startswith("abfss://"):
            raise ValueError(
                f"AdlsStorageClient requires an abfss:// URI, got: {uri!r}"
            )

        matcher = PatternMatcher(pattern)
        root = uri.rstrip("/") + "/"

        # Optional SQL LIKE prefilter — OR-joined across all patterns. Only a
        # prefilter: the bodies are suffix-anchored and their wildcards cross
        # "/", so they never exclude a path that matcher.matches() would accept.
        like_bodies = matcher.sql_like_bodies()
        where_clause = (
            " OR ".join(f"path LIKE '{body}'" for body in like_bodies)
            if like_bodies
            else None
        )

        sql = (
            "SELECT path, size, modification_time "
            f"FROM LIST('{self._destination.escape_string_literal(uri)}', RECURSIVE => TRUE) "
            + (f"WHERE ({where_clause}) " if where_clause else "")
            + "ORDER BY path"
        )

        logger.debug(
            "ADLS LIST via Databricks SQL: uri=%r patterns=%r prefilter=%r",
            uri,
            matcher.patterns,
            where_clause,
        )

        try:
            rows = self._destination.execute_sql(sql)
        except Exception as exc:
            raise RuntimeError(
                f"Databricks SQL LIST failed for {uri!r}. "
                "Ensure the workspace has a Unity Catalog external location "
                f"granting LIST + READ_FILES on this URI. Error: {exc}"
            ) from exc

        for row in rows:
            path = str(row[0]) if row[0] is not None else ""
            if not path:
                continue

            # Derive full_uri: the path returned by LIST is either relative to the
            # listed URI or the full abfss:// URI depending on the Databricks
            # runtime version.  Normalise to full_uri plus the path relative to
            # the listed URI, which is what the pattern matches against.
            if path.startswith("abfss://"):
                full_uri = path
                rel_path = relative_path(path, root)
            else:
                rel_path = path.lstrip("/")
                full_uri = root + rel_path

            if not rel_path:
                continue

            # Python-side precise glob filter (SQL LIKE is a prefilter only)
            if not matcher.matches(rel_path):
                continue

            size = int(row[1]) if row[1] is not None else 0
            mtime = row[2]  # datetime or milliseconds int depending on runtime

            # Convert mtime to epoch-millis for a stable generation surrogate.
            generation = _mtime_to_generation(mtime)

            yield StorageObject(
                name=path,
                full_uri=full_uri,
                size=size,
                generation=generation,
                updated=mtime,
            )


def _mtime_to_generation(mtime: object) -> int:
    """Convert a Databricks LIST modification_time to an int generation surrogate.

    Databricks returns modification_time as either a datetime object or an integer
    (epoch milliseconds).  We normalise to epoch-millis so that re-uploading a
    file produces a different generation, matching the GCS generation contract.
    """
    if mtime is None:
        return 0
    if isinstance(mtime, int):
        return mtime
    # datetime-like object
    try:
        import datetime

        if isinstance(mtime, datetime.datetime):
            return int(mtime.timestamp() * 1000)
    except Exception as exc:
        import logging as _logging

        _logging.getLogger(__name__).debug(
            "Could not convert mtime %r to milliseconds: %s", mtime, exc
        )
    return 0
