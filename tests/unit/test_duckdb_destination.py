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
