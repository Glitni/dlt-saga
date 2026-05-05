"""Unit tests for native_load._sql."""

import pytest

from dlt_saga.pipelines.native_load._sql import esc_sql_literal


@pytest.mark.unit
class TestEscSqlLiteral:
    def test_plain_string(self):
        assert esc_sql_literal("hello world") == "hello world"

    def test_single_quote(self):
        assert esc_sql_literal("it's") == "it''s"

    def test_multiple_single_quotes(self):
        assert esc_sql_literal("a'b'c") == "a''b''c"

    def test_backslash(self):
        assert esc_sql_literal("a\\b") == "a\\\\b"

    def test_newline(self):
        assert esc_sql_literal("a\nb") == "a\\nb"

    def test_carriage_return(self):
        assert esc_sql_literal("a\rb") == "a\\rb"

    def test_tab(self):
        assert esc_sql_literal("a\tb") == "a\\tb"

    def test_backslash_then_newline(self):
        # Backslash must be escaped before newline so they don't compose wrong
        result = esc_sql_literal("a\\\nb")
        assert result == "a\\\\\\nb"

    def test_empty_string(self):
        assert esc_sql_literal("") == ""

    def test_uri(self):
        uri = "gs://my-bucket/prefix/2024/01/data.parquet"
        assert esc_sql_literal(uri) == uri  # no special chars

    def test_error_message_with_all_special(self):
        value = "error: 'foo'\nbar\ttab\r\n"
        result = esc_sql_literal(value)
        assert "''" in result  # single quote escaped
        assert "\\n" in result
        assert "\\t" in result
        assert "\\r" in result
