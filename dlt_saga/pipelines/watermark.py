"""Shared high-water-mark lookup for incremental pipelines.

Both incremental mechanisms in the API layer read the same thing from the
warehouse — ``MAX(incremental_column)`` for the pipeline's destination table:

- the placeholder-cursor path (``BaseApiPipeline._resolve_incremental_value``),
  which substitutes the resolved value into a ``{incremental_value}`` query
  param, and
- the date-window resolver (``DateWindowResolver._get_watermark``), which turns
  the watermark into a ``[start, end]`` window.

They differ only in how they *apply* the watermark; reading it is identical.
This is the single source of truth for that read, so the table-id construction
and destination call live in one place.
"""

from typing import Any, Optional


def read_destination_watermark(
    destination: Any,
    database: str,
    dataset: str,
    table: str,
    column: Optional[str],
) -> Optional[Any]:
    """Return ``MAX(column)`` for ``{database}.{dataset}.{table}``, or ``None``.

    Returns ``None`` when no ``column`` is given. A missing or empty table is
    handled by the destination (it returns ``None``) — i.e. the first run. Any
    other error propagates; the caller decides whether to treat it as a first
    run or surface it.
    """
    if not column:
        return None
    table_id = f"{database}.{dataset}.{table}"
    return destination.get_max_column_value(table_id, column)
