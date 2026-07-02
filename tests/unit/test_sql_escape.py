"""Unit tests for SQL single-quoted literal escaping."""

import pytest

from dlt_saga.utility.sql import escape_sql_literal


@pytest.mark.unit
class TestEscapeSqlLiteral:
    def test_single_quotes_doubled(self):
        assert escape_sql_literal("O'Brien") == "O''Brien"

    def test_backslash_doubled(self):
        assert escape_sql_literal("a\\b") == "a\\\\b"

    def test_newline_escaped(self):
        assert escape_sql_literal("line1\nline2") == "line1\\nline2"

    def test_no_raw_control_chars_remain(self):
        """The whole point: arbitrary input can't leave a literal-breaking char."""
        result = escape_sql_literal("a\nb\rc\td\x00e")
        assert not any(c in result for c in "\n\r\t\x00")

    def test_backslash_escaped_before_control_chars_not_doubled(self):
        """A real newline becomes \\n (one backslash), not \\\\n."""
        assert escape_sql_literal("\n") == "\\n"
        # A literal backslash followed by 'n' stays distinct from an escaped newline.
        assert escape_sql_literal("\\n") == "\\\\n"

    def test_nul_dropped(self):
        assert escape_sql_literal("a\x00b") == "ab"

    def test_multiline_traceback_stays_single_line(self):
        traceback = (
            "Traceback (most recent call last):\n"
            "  File 'x.py', line 1\n"
            "DatabaseError: 400 Syntax error: Unclosed string literal\n"
        )
        result = escape_sql_literal(traceback)
        assert "\n" not in result
        assert "''x.py''" in result  # embedded quotes still doubled
