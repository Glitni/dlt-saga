"""Unit tests for the literal/comment-aware SQL statement splitter."""

import pytest

from dlt_saga.utility.sql import split_sql_statements


@pytest.mark.unit
class TestSplitSqlStatements:
    def test_simple_two_statements(self):
        assert split_sql_statements("SELECT 1; SELECT 2") == ["SELECT 1", "SELECT 2"]

    def test_single_statement_no_semicolon(self):
        assert split_sql_statements("SELECT 1") == ["SELECT 1"]

    def test_trailing_semicolon_and_blanks(self):
        assert split_sql_statements("SELECT 1;;  ; SELECT 2;") == [
            "SELECT 1",
            "SELECT 2",
        ]

    def test_empty_input(self):
        assert split_sql_statements("") == []
        assert split_sql_statements("   ;  ; ") == []

    def test_semicolon_inside_single_quotes_not_split(self):
        sql = "COMMENT ON TABLE t IS 'has ; a semicolon'"
        assert split_sql_statements(sql) == [sql]

    def test_semicolon_in_literal_then_next_statement(self):
        sql = "COMMENT ON TABLE t IS 'a; b'; SELECT 1"
        assert split_sql_statements(sql) == [
            "COMMENT ON TABLE t IS 'a; b'",
            "SELECT 1",
        ]

    def test_doubled_quote_escape(self):
        sql = "SELECT 'it''s; fine' AS x"
        assert split_sql_statements(sql) == [sql]

    def test_backslash_quote_escape(self):
        # Backslash-escaped quote (BigQuery/Databricks dialect): the string does
        # not terminate at \', so the ; stays inside the literal.
        sql = "SELECT 'a\\'; still in string' AS x"
        assert split_sql_statements(sql) == [sql]

    def test_semicolon_in_line_comment_not_split(self):
        sql = "SELECT 1 -- trailing ; comment\nFROM t"
        assert split_sql_statements(sql) == [sql]

    def test_line_comment_before_second_statement(self):
        sql = "SELECT 1; -- note ; here\nSELECT 2"
        assert split_sql_statements(sql) == ["SELECT 1", "-- note ; here\nSELECT 2"]

    def test_semicolon_in_block_comment_not_split(self):
        sql = "SELECT 1 /* a ; b */ FROM t"
        assert split_sql_statements(sql) == [sql]

    def test_semicolon_in_backtick_identifier_not_split(self):
        sql = "SELECT `weird;col` FROM t"
        assert split_sql_statements(sql) == [sql]

    def test_semicolon_in_double_quoted_not_split(self):
        sql = 'SELECT "weird;ident" FROM t'
        assert split_sql_statements(sql) == [sql]

    def test_multiple_statements_with_literals(self):
        sql = (
            "CREATE TABLE t (id INT); "
            "COMMENT ON TABLE t IS 'first; second'; "
            "INSERT INTO t VALUES (1)"
        )
        assert split_sql_statements(sql) == [
            "CREATE TABLE t (id INT)",
            "COMMENT ON TABLE t IS 'first; second'",
            "INSERT INTO t VALUES (1)",
        ]

    def test_unterminated_block_comment_does_not_hang(self):
        sql = "SELECT 1 /* never closed"
        assert split_sql_statements(sql) == [sql]

    def test_unterminated_string_does_not_hang(self):
        sql = "SELECT 'never closed"
        assert split_sql_statements(sql) == [sql]
