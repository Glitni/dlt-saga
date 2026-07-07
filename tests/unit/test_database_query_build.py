"""DatabaseClient._build_query: incremental WHERE literal rendering + custom-query validation.

Covers:
- The auto-built incremental WHERE renders the watermark by inferred type
  (numerics unquoted; strings escaped/quoted; tz-aware datetimes normalised),
  instead of always emitting an unescaped single-quoted string.
- A custom incremental query must contain the {incremental_value} placeholder
  and have a watermark available, instead of silently reloading everything or
  sending a raw placeholder to the database.
"""

from datetime import datetime, timezone

import pytest

from dlt_saga.pipelines.database.client import DatabaseClient
from dlt_saga.pipelines.database.config import DatabaseConfig


def _client(**cfg) -> DatabaseClient:
    return DatabaseClient(DatabaseConfig(**cfg))


@pytest.mark.unit
class TestIncrementalWhereLiteral:
    def test_numeric_watermark_unquoted(self):
        client = _client(
            connection_string="postgresql://u:p@h/d",
            database_type="postgres",
            source_table="orders",
        )
        query = client._build_query(incremental_value=42, incremental_column="id")
        assert "WHERE id > 42" in query
        assert "'42'" not in query

    def test_string_watermark_escaped_and_quoted(self):
        client = _client(
            connection_string="postgresql://u:p@h/d",
            database_type="postgres",
            source_table="orders",
        )
        query = client._build_query(
            incremental_value="o'brien", incremental_column="name"
        )
        # ANSI dialects double the quote.
        assert "WHERE name > 'o''brien'" in query

    def test_bigquery_escapes_with_backslash(self):
        client = _client(database_type="bigquery", source_table="orders")
        query = client._build_query(
            incremental_value="o'brien", incremental_column="name"
        )
        assert "WHERE name > 'o\\'brien'" in query

    def test_tz_aware_datetime_normalised_to_naive_utc(self):
        client = _client(
            connection_string="mssql://u:p@h/d",
            database_type="mssql",
            source_table="orders",
        )
        value = datetime(2026, 1, 1, 12, 0, tzinfo=timezone.utc)
        query = client._build_query(
            incremental_value=value, incremental_column="updated_at"
        )
        # No trailing timezone offset (SQL Server DATETIME rejects it).
        assert "WHERE updated_at > '2026-01-01 12:00:00'" in query
        assert "+00:00" not in query

    def test_no_watermark_is_full_load(self):
        client = _client(
            connection_string="postgresql://u:p@h/d",
            database_type="postgres",
            source_table="orders",
        )
        query = client._build_query(incremental_value=None, incremental_column="id")
        assert query == "SELECT * FROM orders"


@pytest.mark.unit
class TestCustomQueryIncremental:
    _QUERY = "SELECT * FROM t WHERE updated > '{incremental_value}'"

    def test_missing_placeholder_raises(self):
        client = _client(
            connection_string="postgresql://u:p@h/d",
            query="SELECT * FROM t",
        )
        with pytest.raises(ValueError, match="placeholder"):
            client._build_query(
                incremental_value="2024-01-01", incremental_column="updated"
            )

    def test_first_run_without_watermark_raises(self):
        client = _client(
            connection_string="postgresql://u:p@h/d",
            query=self._QUERY,
        )
        with pytest.raises(ValueError, match="no watermark is available"):
            client._build_query(incremental_value=None, incremental_column="updated")

    def test_placeholder_substituted_when_watermark_present(self):
        client = _client(
            connection_string="postgresql://u:p@h/d",
            query=self._QUERY,
        )
        query = client._build_query(
            incremental_value="2024-06-15", incremental_column="updated"
        )
        assert query == "SELECT * FROM t WHERE updated > '2024-06-15'"

    def test_non_incremental_custom_query_unchanged(self):
        client = _client(
            connection_string="postgresql://u:p@h/d",
            query="SELECT * FROM t",
        )
        query = client._build_query(incremental_value=None, incremental_column=None)
        assert query == "SELECT * FROM t"
