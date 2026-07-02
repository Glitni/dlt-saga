"""Helpers for hand-built SQL.

Centralizes SQL string-literal escaping so the several call sites that
interpolate values into SQL (orchestration run-recording, historize state)
share one implementation and can't drift.
"""


def escape_sql_literal(value: str) -> str:
    """Escape a string for safe interpolation into a SQL single-quoted literal.

    Tolerates arbitrary input — notably multi-line error messages and
    tracebacks. Backslash is escaped first so the escape sequences inserted
    afterward are not themselves doubled; single quotes are doubled; the control
    characters that would otherwise break a single-quoted literal (newline,
    carriage return, tab) become escape sequences; and NUL is dropped (it is
    unstorable / string-terminating in several drivers).

    Assumes the target dialect honors C-style backslash escapes in string
    literals (BigQuery, Databricks). On dialects that treat backslash literally
    (e.g. DuckDB's default strings) the value stays syntactically valid — the
    escape sequences render as literal text rather than as control characters,
    which is acceptable for the metadata/diagnostics these literals carry.
    """
    return (
        value.replace("\\", "\\\\")
        .replace("'", "''")
        .replace("\n", "\\n")
        .replace("\r", "\\r")
        .replace("\t", "\\t")
        .replace("\x00", "")
    )
