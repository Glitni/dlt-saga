"""Unit tests for native_load's path-relative glob matching.

Includes a parity check against fsspec's own glob, which is what the
``filesystem`` adapter's ``file_glob`` goes through — the two adapters must
agree on what a pattern means.
"""

import posixpath
import re

import pytest

from dlt_saga.pipelines.native_load.storage.matching import (
    PatternMatcher,
    relative_path,
    supports_delimiter_listing,
)

# Layout used by both the direct and the fsspec-parity tests.
_LAYOUT = [
    "report.csv",
    "report.csv.ingested",
    "notes.txt",
    "legacy/report.csv",
    "legacy/deep/report.csv",
    "2026/01/part.parquet",
]

_PATTERNS = [
    "*.csv",
    "*.csv*",
    "report-*.csv",
    "*",
    "**",
    "**/*.csv",
    "legacy/*.csv",
    "legacy/**/*.csv",
    "*/*/part.parquet",
    "**/part.parquet",
    "report.csv",
    "notes.???",
    "[rn]*.csv",
    "[!r]*.txt",
]


def _like_to_regex(body):
    """Compile a SQL LIKE body (backslash escapes, % and _) into a regex."""
    out = []
    i = 0
    while i < len(body):
        char = body[i]
        i += 1
        if char == "\\" and i < len(body):
            out.append(re.escape(body[i]))
            i += 1
        elif char == "%":
            out.append(".*")
        elif char == "_":
            out.append(".")
        else:
            out.append(re.escape(char))
    return re.compile("".join(out))


@pytest.mark.unit
class TestPatternMatcherSemantics:
    def test_star_does_not_cross_directories(self):
        matcher = PatternMatcher("*.csv")
        assert matcher.matches("report.csv")
        assert not matcher.matches("legacy/report.csv")

    def test_double_star_recurses(self):
        matcher = PatternMatcher("**/*.csv")
        assert matcher.matches("report.csv")
        assert matcher.matches("legacy/report.csv")
        assert matcher.matches("legacy/deep/report.csv")
        assert not matcher.matches("notes.txt")

    def test_folder_segment_is_matched(self):
        matcher = PatternMatcher("legacy/*.csv")
        assert matcher.matches("legacy/report.csv")
        assert not matcher.matches("report.csv")
        assert not matcher.matches("legacy/deep/report.csv")

    def test_pattern_list_matches_any(self):
        matcher = PatternMatcher(["*.csv", "*.csv.ingested"])
        assert matcher.matches("report.csv")
        assert matcher.matches("report.csv.ingested")
        assert not matcher.matches("notes.txt")

    def test_question_mark_and_character_class(self):
        matcher = PatternMatcher("file?.[pc]sv")
        assert matcher.matches("file1.csv")
        assert matcher.matches("file2.psv")
        assert not matcher.matches("file10.csv")

    def test_negated_character_class(self):
        matcher = PatternMatcher("[!r]*.csv")
        assert matcher.matches("export.csv")
        assert not matcher.matches("report.csv")

    def test_leading_slash_and_dot_slash_normalized(self):
        assert PatternMatcher("/*.csv").matches("a.csv")
        assert PatternMatcher("./*.csv").matches("a.csv")

    def test_string_and_single_item_list_are_equivalent(self):
        assert PatternMatcher("*.csv").matches("a.csv")
        assert PatternMatcher(["*.csv"]).matches("a.csv")

    def test_partial_double_star_rejected(self):
        with pytest.raises(ValueError, match="whole path segment"):
            PatternMatcher("data**.parquet")

    def test_empty_pattern_rejected(self):
        with pytest.raises(ValueError, match="non-empty"):
            PatternMatcher("")

    def test_empty_pattern_list_rejected(self):
        with pytest.raises(ValueError, match="at least one pattern"):
            PatternMatcher([])


@pytest.mark.unit
class TestPatternMatcherProperties:
    def test_is_recursive(self):
        assert PatternMatcher("**/*.csv").is_recursive
        assert PatternMatcher(["*.csv", "**/*.txt"]).is_recursive
        assert not PatternMatcher("*.csv").is_recursive
        assert not PatternMatcher("legacy/*.csv").is_recursive

    def test_widened_prefixes_non_recursive_patterns(self):
        assert PatternMatcher("*.csv").widened() == ["**/*.csv"]
        assert PatternMatcher(["*.csv", "**/*.txt"]).widened() == [
            "**/*.csv",
            "**/*.txt",
        ]

    def test_widened_patterns_reach_subfolders(self):
        matcher = PatternMatcher(PatternMatcher("*.csv").widened())
        assert matcher.matches("legacy/deep/report.csv")

    def test_sql_like_bodies_are_suffix_anchored(self):
        assert PatternMatcher("report-*.csv").sql_like_bodies() == ["%report-%.csv"]

    def test_sql_like_bodies_escape_sql_wildcards(self):
        assert PatternMatcher("data_*.csv").sql_like_bodies() == [r"%data\_%.csv"]

    def test_sql_like_bodies_none_for_pure_wildcard(self):
        assert PatternMatcher("**").sql_like_bodies() is None
        assert PatternMatcher(["*.csv", "*"]).sql_like_bodies() is None

    def test_sql_like_bodies_widen_character_classes(self):
        # SQL LIKE has no character classes; "_" is the any-single-char widening
        assert PatternMatcher("[rn]*.csv").sql_like_bodies() == ["%_%.csv"]

    def test_sql_like_bodies_collapse_recursive_separator(self):
        # "%/report-%.csv" would demand a subfolder and drop the top-level files
        # that "**/" is meant to include.
        assert PatternMatcher("**/report-*.csv").sql_like_bodies() == ["%report-%.csv"]
        assert PatternMatcher("a/**/b.csv").sql_like_bodies() == ["%a/%b.csv"]

    @pytest.mark.parametrize("pattern", _PATTERNS)
    def test_sql_like_bodies_never_exclude_a_match(self, pattern):
        # The prefilter must be over-inclusive: every path matches() accepts has
        # to survive the LIKE, whose bodies are suffix-anchored.
        matcher = PatternMatcher(pattern)
        bodies = matcher.sql_like_bodies()
        if bodies is None:
            return
        regexes = [_like_to_regex(body) for body in bodies]
        for path in _LAYOUT:
            if matcher.matches(path):
                assert any(regex.fullmatch(path) for regex in regexes), (
                    f"pattern {pattern!r} prefilter would drop {path!r}"
                )


@pytest.mark.unit
class TestListingHelpers:
    def test_relative_path_strips_listing_prefix(self):
        assert relative_path("root/2026/f.csv", "root/") == "2026/f.csv"

    def test_relative_path_without_prefix(self):
        assert relative_path("2026/f.csv", "") == "2026/f.csv"

    def test_relative_path_tolerates_partial_prefix(self):
        assert relative_path("root/f.csv", "root") == "f.csv"

    def test_delimiter_listing_for_top_level_pattern(self):
        assert supports_delimiter_listing(PatternMatcher("*.csv"), "root/")
        assert supports_delimiter_listing(PatternMatcher("*.csv"), "")

    def test_no_delimiter_listing_for_recursive_pattern(self):
        assert not supports_delimiter_listing(PatternMatcher("**/*.csv"), "root/")

    def test_no_delimiter_listing_for_multi_segment_pattern(self):
        assert not supports_delimiter_listing(PatternMatcher("legacy/*.csv"), "root/")

    def test_no_delimiter_listing_for_partial_prefix(self):
        assert not supports_delimiter_listing(PatternMatcher("*.csv"), "root")

    def test_no_delimiter_listing_when_any_pattern_recurses(self):
        matcher = PatternMatcher(["*.csv", "**/*.txt"])
        assert not supports_delimiter_listing(matcher, "root/")


@pytest.mark.unit
class TestFsspecParity:
    """The same pattern must select the same files as the filesystem adapter.

    dlt's filesystem source globs ``posixpath.join(root, file_glob)`` through
    fsspec, so fsspec's own glob is the reference implementation.
    """

    @pytest.fixture
    def memory_fs(self):
        from fsspec.implementations.memory import MemoryFileSystem

        fs = MemoryFileSystem()
        fs.store.clear()
        fs.pseudo_dirs.clear()
        for path in _LAYOUT:
            with fs.open(f"memory://root/{path}", "wb") as handle:
                handle.write(b"x")
        return fs

    @pytest.mark.parametrize("pattern", _PATTERNS)
    def test_matches_fsspec_glob(self, pattern, memory_fs):
        globbed = memory_fs.glob(posixpath.join("/root", pattern), detail=True)
        expected = {
            path[len("/root/") :]
            for path, info in globbed.items()
            if info["type"] == "file"
        }

        matcher = PatternMatcher(pattern)
        actual = {path for path in _LAYOUT if matcher.matches(path)}

        assert actual == expected, f"pattern {pattern!r} diverges from fsspec"
