"""ADLS Gen2 storage client for the native_load adapter.

Uses Databricks SQL ``LIST`` to enumerate files via the workspace's Unity Catalog
external location — no Azure SDK or separate auth surface required.  The active
SQL warehouse that saga already holds for COPY INTO is reused for listing.
"""

import fnmatch
import logging
from typing import TYPE_CHECKING, Iterator, List, Optional, Union

from dlt_saga.pipelines.native_load.storage.base import StorageClient, StorageObject

if TYPE_CHECKING:
    from dlt_saga.destinations.databricks.destination import DatabricksDestination

logger = logging.getLogger(__name__)


def _pattern_to_sql_like(pattern: str) -> str:
    """Convert a fnmatch glob pattern to a SQL LIKE expression fragment.

    Only the common case (leading/trailing ``*``) is handled.  Python's fnmatch
    is applied afterwards for precise filtering; the SQL clause is a prefilter
    only.
    """
    # Replace fnmatch wildcards: * → %, ? → _
    # Escape existing SQL metacharacters first
    result = pattern.replace("%", r"\%").replace("_", r"\_")
    result = result.replace("*", "%").replace("?", "_")
    return result


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
            pattern: Glob pattern(s) matched against the full path.
            start_offset: Not used for ADLS; silently ignored.
        """
        if not uri.startswith("abfss://"):
            raise ValueError(
                f"AdlsStorageClient requires an abfss:// URI, got: {uri!r}"
            )

        patterns = [pattern] if isinstance(pattern, str) else list(pattern)

        # Build SQL LIKE prefilter — OR-joined across all patterns
        like_clauses = " OR ".join(
            f"path LIKE '{_pattern_to_sql_like(p)}'" for p in patterns
        )
        where_clause = f"({like_clauses})"

        from dlt_saga.pipelines.native_load._sql import esc_sql_literal

        sql = (
            f"SELECT path, size, modification_time "
            f"FROM LIST('{esc_sql_literal(uri)}', RECURSIVE => TRUE) "
            f"WHERE {where_clause} "
            f"ORDER BY path"
        )

        logger.debug("ADLS LIST via Databricks SQL: uri=%r patterns=%r", uri, patterns)

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
            # container root or the full abfss:// URI depending on the Databricks
            # runtime version.  Normalise to full_uri.
            if path.startswith("abfss://"):
                full_uri = path
            else:
                full_uri = uri.rstrip("/") + "/" + path.lstrip("/")

            basename = full_uri.rsplit("/", 1)[-1]
            if not basename:
                continue

            # Python-side precise fnmatch filter (SQL LIKE is a prefilter only)
            if not any(fnmatch.fnmatch(basename, p) for p in patterns):
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
    except Exception:
        pass
    return 0
