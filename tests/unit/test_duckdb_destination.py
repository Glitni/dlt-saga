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
