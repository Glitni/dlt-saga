"""Unit tests for DuckDB destination dialect methods."""

import pytest


@pytest.mark.unit
class TestDuckDBTimestampNDaysAgo:
    def test_returns_duckdb_interval_syntax(self):
        from dlt_saga.testing import make_destination

        dest = make_destination()
        try:
            assert dest.timestamp_n_days_ago(7) == "now() - INTERVAL '7' DAY"
            assert dest.timestamp_n_days_ago(0) == "now() - INTERVAL '0' DAY"
            assert dest.timestamp_n_days_ago(365) == "now() - INTERVAL '365' DAY"
        finally:
            dest.close()

    def test_expression_is_valid_duckdb_sql(self):
        """Verify the expression executes without error in DuckDB."""
        from dlt_saga.testing import make_destination

        dest = make_destination()
        try:
            expr = dest.timestamp_n_days_ago(30)
            rows = dest.execute_sql(f"SELECT ({expr})::DATE AS d")
            assert len(rows) == 1
            assert rows[0][0] is not None
        finally:
            dest.close()

    def test_expression_does_not_use_bigquery_syntax(self):
        """CURRENT_TIMESTAMP() is BigQuery syntax and must not appear."""
        from dlt_saga.testing import make_destination

        dest = make_destination()
        try:
            expr = dest.timestamp_n_days_ago(7)
            assert "CURRENT_TIMESTAMP" not in expr
        finally:
            dest.close()


@pytest.mark.unit
class TestDuckDBFullTableId:
    def test_two_part_by_default(self):
        from dlt_saga.testing import make_destination

        dest = make_destination()
        try:
            assert dest.get_full_table_id("my_schema", "my_table") == (
                '"my_schema"."my_table"'
            )
        finally:
            dest.close()

    def test_database_override_prepends_catalog(self):
        from dlt_saga.testing import make_destination

        dest = make_destination()
        try:
            assert dest.get_full_table_id(
                "my_schema", "my_table", database="other_cat"
            ) == ('"other_cat"."my_schema"."my_table"')
        finally:
            dest.close()


@pytest.mark.unit
class TestDuckDBHashExpression:
    """Change-detection hash must not conflate NULL with '' and must keep column
    boundaries unambiguous (the old md5(concat(COALESCE(...,''))) did both,
    silently missing SCD2 changes)."""

    def test_shape_uses_json_not_naive_concat(self):
        from dlt_saga.testing import make_destination

        dest = make_destination()
        try:
            expr = dest.hash_expression(["a", "b"])
            assert "json_object" in expr
            assert "md5(" in expr
            assert "COALESCE" not in expr
            assert "concat(" not in expr
        finally:
            dest.close()

    def test_distinguishes_null_empty_and_separator_cases(self):
        """(NULL,'x') vs ('','x') and ('a|b','c') vs ('a','b|c') must all hash
        differently — each collided under the old expression."""
        from dlt_saga.testing import make_destination

        dest = make_destination()
        try:
            dest.execute_sql("CREATE TABLE t (a VARCHAR, b VARCHAR)")
            dest.execute_sql(
                "INSERT INTO t VALUES (NULL,'x'), ('','x'), ('a|b','c'), ('a','b|c')"
            )
            expr = dest.hash_expression(["a", "b"])
            rows = dest.execute_sql(f"SELECT {expr} AS h FROM t")
            hashes = [r[0] for r in rows]
            assert len(set(hashes)) == len(hashes)
        finally:
            dest.close()


@pytest.mark.unit
class TestDuckDBQuoteIdentifier:
    def test_plain_identifier_double_quoted(self):
        from dlt_saga.testing import make_destination

        dest = make_destination()
        try:
            assert dest.quote_identifier("my_col") == '"my_col"'
        finally:
            dest.close()

    def test_embedded_double_quote_is_doubled(self):
        from dlt_saga.testing import make_destination

        dest = make_destination()
        try:
            # A " in the name must be doubled so it can't terminate the
            # identifier early.
            assert dest.quote_identifier('a"b') == '"a""b"'
        finally:
            dest.close()

    def test_reserved_word_column_round_trips(self):
        """A reserved-word column name survives a real CREATE/INSERT/SELECT once
        quoted — the point of routing DDL through quote_identifier."""
        from dlt_saga.testing import make_destination

        dest = make_destination()
        try:
            col = dest.quote_identifier("select")
            dest.execute_sql(f"CREATE TABLE t ({col} INTEGER)")
            dest.execute_sql(f"INSERT INTO t ({col}) VALUES (1)")
            rows = dest.execute_sql(f"SELECT {col} FROM t")
            assert rows[0][0] == 1
        finally:
            dest.close()
