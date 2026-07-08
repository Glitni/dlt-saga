"""Unit tests for BigQuery destination dialect quoting + partition/cluster DDL.

Focus: quote_identifier escapes embedded backticks, and partition/cluster DDL
route column names through quote_identifier so reserved-word / dash / quoted
names can't break the generated CTAS.
"""

import pytest

from dlt_saga.destinations.bigquery.destination import BigQueryDestination


def _dest() -> BigQueryDestination:
    # These dialect methods are pure (partition_ddl only calls quote_identifier),
    # so bypass __init__ to avoid needing config/credentials.
    return object.__new__(BigQueryDestination)


@pytest.mark.unit
class TestBigQueryQuoteIdentifier:
    def test_plain_identifier_backtick_quoted(self):
        assert _dest().quote_identifier("my_col") == "`my_col`"

    def test_embedded_backtick_is_backslash_escaped(self):
        # GoogleSQL uses C-style escapes inside backtick identifiers.
        assert _dest().quote_identifier("a`b") == "`a\\`b`"

    def test_embedded_backslash_is_escaped_first(self):
        assert _dest().quote_identifier("a\\b") == "`a\\\\b`"


@pytest.mark.unit
class TestBigQueryPartitionClusterDdl:
    def test_partition_ddl_quotes_column(self):
        assert _dest().partition_ddl("event_ts") == "PARTITION BY DATE(`event_ts`)"

    def test_partition_ddl_date_column_not_wrapped_in_date(self):
        assert _dest().partition_ddl("dt", col_type="DATE") == "PARTITION BY `dt`"

    def test_cluster_ddl_quotes_each_column(self):
        assert _dest().cluster_ddl(["id", "region"]) == "CLUSTER BY `id`, `region`"

    def test_cluster_ddl_empty_is_blank(self):
        assert _dest().cluster_ddl([]) == ""

    def test_reserved_word_column_is_quoted(self):
        # A column named after a reserved word would break unquoted DDL.
        assert _dest().partition_ddl("order", col_type="DATE") == "PARTITION BY `order`"


@pytest.mark.unit
class TestBigQueryStringAndJson:
    def test_escape_string_literal_uses_backslash(self):
        # GoogleSQL rejects '' inside a single-quoted literal; it wants \'.
        assert _dest().escape_string_literal("it's") == "it\\'s"

    def test_parse_json_expression_uses_parse_json(self):
        assert _dest().parse_json_expression("'{}'") == "PARSE_JSON('{}')"


@pytest.mark.unit
class TestBigQueryListTablesByPattern:
    def _dest_with_capture(self):
        from types import SimpleNamespace

        dest = _dest()
        dest.config = SimpleNamespace(project_id="proj")
        captured = {}
        dest.execute_sql = lambda sql: captured.setdefault("sql", sql) or []
        return dest, captured

    def test_no_age_filter_by_default(self):
        dest, captured = self._dest_with_capture()
        dest.list_tables_by_pattern("ds", "t__ext_%")
        assert "creation_time" not in captured["sql"]

    def test_age_filter_applied_when_set(self):
        dest, captured = self._dest_with_capture()
        dest.list_tables_by_pattern("ds", "t__ext_%", min_age_hours=6)
        assert "creation_time <" in captured["sql"]
        assert "INTERVAL 6 HOUR" in captured["sql"]
