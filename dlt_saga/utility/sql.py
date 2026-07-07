"""SQL script utilities shared across destinations."""

from typing import List


def looks_like_missing_table(exc: Exception) -> bool:
    """Best-effort detection of "relation/table does not exist" across destinations.

    BigQuery emits ``Not found: Table ...``, Databricks ``TABLE_OR_VIEW_NOT_FOUND``,
    and DuckDB ``Catalog Error: Table with name ... does not exist``. We match on
    the substrings the three share so anything else — permission denials, network
    errors, SQL errors — is *not* mistaken for a missing table and can propagate
    instead of being silently swallowed as "no data" (which would trigger a full
    re-extract or duplicate loads on incremental pipelines).
    """
    msg = str(exc).lower()
    return any(
        marker in msg
        for marker in (
            "not found",
            "does not exist",
            "no such table",
            "table_or_view_not_found",
        )
    )


def _scan_quoted(sql: str, start: int) -> int:
    """Return the index just past the string/identifier opened at ``start``.

    A quote is escaped by doubling it (``''``) or with a preceding backslash
    (``\\'``), so both the doubling dialect (DuckDB) and the backslash dialect
    (BigQuery/Databricks) are handled. Unterminated quotes run to end-of-input.
    """
    quote = sql[start]
    n = len(sql)
    i = start + 1
    while i < n:
        c = sql[i]
        if c == "\\" and i + 1 < n:
            i += 2
            continue
        if c == quote:
            if i + 1 < n and sql[i + 1] == quote:
                i += 2
                continue
            return i + 1
        i += 1
    return n


def _scan_line_comment(sql: str, start: int) -> int:
    """Return the index just past a ``-- ...`` comment (through the newline)."""
    end = sql.find("\n", start)
    return len(sql) if end == -1 else end + 1


def _scan_block_comment(sql: str, start: int) -> int:
    """Return the index just past a ``/* ... */`` comment."""
    end = sql.find("*/", start + 2)
    return len(sql) if end == -1 else end + 2


def split_sql_statements(sql: str) -> List[str]:
    """Split a SQL script into individual statements on top-level ``;``.

    A plain ``sql.split(";")`` breaks any statement that contains a semicolon
    inside a string literal (e.g. a table/column ``COMMENT`` or a historize-log
    message) or a comment, producing invalid fragments. This splitter treats a
    ``;`` as a statement separator only when it is not inside a single-quoted
    string, a double-/back-quoted identifier, or a line/block comment. Comment
    text is preserved (engines ignore it); only empty statements are dropped.

    Args:
        sql: One or more SQL statements separated by ``;``.

    Returns:
        Non-empty, stripped statements in order.
    """
    statements: List[str] = []
    buf: List[str] = []
    i = 0
    n = len(sql)

    while i < n:
        c = sql[i]
        if c in ("'", '"', "`"):
            end = _scan_quoted(sql, i)
        elif c == "-" and i + 1 < n and sql[i + 1] == "-":
            end = _scan_line_comment(sql, i)
        elif c == "/" and i + 1 < n and sql[i + 1] == "*":
            end = _scan_block_comment(sql, i)
        elif c == ";":
            stmt = "".join(buf).strip()
            if stmt:
                statements.append(stmt)
            buf = []
            i += 1
            continue
        else:
            buf.append(c)
            i += 1
            continue

        buf.append(sql[i:end])
        i = end

    tail = "".join(buf).strip()
    if tail:
        statements.append(tail)
    return statements
