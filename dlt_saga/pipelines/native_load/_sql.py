"""Shared SQL string-literal escaping for the native_load adapter."""


def esc_sql_literal(value: str) -> str:
    """Escape a string for a single-quoted (non-raw) SQL string literal.

    Backslash escaping runs first so its output is not re-escaped by the
    subsequent rules. Works for both BigQuery and Databricks dialects.
    """
    return (
        value.replace("\\", "\\\\")
        .replace("'", "''")
        .replace("\n", "\\n")
        .replace("\r", "\\r")
        .replace("\t", "\\t")
    )
