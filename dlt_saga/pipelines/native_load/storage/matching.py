"""Path-relative glob matching for native_load discovery.

``file_pattern`` is matched against each object's path *relative to the URI
being listed*, using the same glob rules fsspec applies to the ``filesystem``
adapter's ``file_glob``:

* ``*`` matches any run of characters within a single path segment
* ``**`` matches any number of segments, and must be a whole segment
* ``?`` matches exactly one character within a segment
* ``[abc]`` matches one character from the set (``[!abc]`` negates)

So ``"*.csv"`` selects only the files directly under the listed URI, while
``"**/*.csv"`` recurses into subfolders. Matching the relative path rather than
the basename keeps patterns interchangeable between the two adapters, and lets
discovery prune the listing server-side when a pattern cannot reach below the
root (see :func:`supports_delimiter_listing`).
"""

import re
from typing import List, Optional, Sequence, Tuple, Union

# A recursive segment expands to "zero or more segments" when followed by more
# pattern, and to "one or more segments" when it ends the pattern.
_RECURSIVE_INNER = "(?:[^/]+/)*"
_RECURSIVE_TAIL = "(?:[^/]+/)*[^/]+"


def _class_end(pattern: str, start: int) -> Optional[int]:
    """Index of the ``]`` closing the class that opens at ``start``, or None.

    ``start`` is the index just past the ``[``. None means the class is
    unterminated, which fnmatch treats as a literal bracket.
    """
    end = start
    if end < len(pattern) and pattern[end] == "!":
        end += 1
    if end < len(pattern) and pattern[end] == "]":
        end += 1
    while end < len(pattern) and pattern[end] != "]":
        end += 1
    return None if end >= len(pattern) else end


def _translate_class(segment: str, start: int) -> Tuple[str, int]:
    """Translate the character class opening at ``start`` (just past the ``[``).

    Returns the regex fragment and the index to resume translation from. An
    unterminated class is treated as a literal bracket, matching fnmatch.
    """
    end = _class_end(segment, start)
    if end is None:
        return re.escape("["), start

    inner = segment[start:end].replace("\\", r"\\")
    if inner.startswith("!"):
        inner = "^" + inner[1:]
    elif inner.startswith("^"):
        inner = "\\" + inner
    return f"[{inner}]", end + 1


def _translate_segment(segment: str) -> str:
    """Translate a single path segment of a glob pattern into a regex."""
    out: List[str] = []
    i, n = 0, len(segment)
    while i < n:
        char = segment[i]
        i += 1
        if char == "*":
            while i < n and segment[i] == "*":
                i += 1
            out.append("[^/]*")
        elif char == "?":
            out.append("[^/]")
        elif char == "[":
            fragment, i = _translate_class(segment, i)
            out.append(fragment)
        else:
            out.append(re.escape(char))
    return "".join(out)


def _normalize(pattern: str) -> str:
    """Strip root/relative prefixes so a pattern always reads from the listed URI."""
    normalized = pattern.strip()
    while normalized.startswith("./"):
        normalized = normalized[2:]
    return normalized.lstrip("/")


def _is_recursive_pattern(pattern: str) -> bool:
    """True when the pattern has a ``**`` segment and can match below the root."""
    return "**" in _normalize(pattern).split("/")


def _translate(pattern: str) -> str:
    """Translate a glob pattern into a regex matched against a relative path."""
    normalized = _normalize(pattern)
    if not normalized:
        raise ValueError("file_pattern entries must be non-empty")

    segments = normalized.split("/")
    parts: List[str] = []
    for index, segment in enumerate(segments):
        is_last = index == len(segments) - 1
        if segment == "**":
            parts.append(_RECURSIVE_TAIL if is_last else _RECURSIVE_INNER)
            continue
        if "**" in segment:
            raise ValueError(
                f"Invalid file_pattern {pattern!r}: '**' must be a whole path "
                "segment (e.g. '**/*.parquet'), not part of one."
            )
        parts.append(_translate_segment(segment))
        if not is_last:
            parts.append("/")
    return "".join(parts)


def _to_sql_like(pattern: str) -> str:
    """Translate a glob pattern into a SQL LIKE body (backslash-escaped).

    Every construct widens to something LIKE can express without ever narrowing
    the match:

    * ``**/`` collapses to ``%`` *including* its separator - ``%`` can match
      nothing, while ``%/`` would demand a subfolder and drop the top-level
      files ``**/`` is meant to include
    * ``*`` becomes ``%``, which also crosses ``/``
    * ``?`` and character classes become ``_`` (any single character)
    """
    normalized = _normalize(pattern).replace("**/", "*").replace("**", "*")

    out: List[str] = []
    i, n = 0, len(normalized)
    while i < n:
        char = normalized[i]
        i += 1
        if char == "*":
            out.append("%")
        elif char == "?":
            out.append("_")
        elif char == "[":
            end = _class_end(normalized, i)
            if end is None:
                out.append("[")
            else:
                out.append("_")
                i = end + 1
        elif char in ("%", "_", "\\"):
            out.append("\\" + char)
        else:
            out.append(char)

    # Collapse runs of % (from consecutive stars) into one.
    return re.sub("%+", "%", "".join(out))


class PatternMatcher:
    """Matches object paths against one or more relative-path glob patterns."""

    def __init__(self, patterns: Union[str, Sequence[str]]) -> None:
        raw = [patterns] if isinstance(patterns, str) else list(patterns)
        if not raw:
            raise ValueError("file_pattern must contain at least one pattern")
        self.patterns = tuple(raw)
        self._regexes = tuple(re.compile(_translate(p)) for p in raw)

    def matches(self, relative_path: str) -> bool:
        """True when the relative path matches any configured pattern."""
        return any(regex.fullmatch(relative_path) for regex in self._regexes)

    @property
    def is_recursive(self) -> bool:
        """True when any pattern can match below the listed URI."""
        return any(_is_recursive_pattern(p) for p in self.patterns)

    def widened(self) -> List[str]:
        """Return the patterns rewritten to also match files in subfolders.

        Used only for diagnostics: when a non-recursive pattern matches nothing,
        the widened form tells us whether the wanted files live deeper.
        """
        return [
            p if _is_recursive_pattern(p) else f"**/{_normalize(p)}"
            for p in self.patterns
        ]

    def sql_like_bodies(self) -> Optional[List[str]]:
        """LIKE bodies for a server-side prefilter, or None when not worth it.

        Each body is prefixed with ``%`` because the listed path may be absolute
        or container-relative while the pattern is relative to the listed URI -
        the relative path is always a *suffix* of the returned path. ``%`` also
        crosses ``/``, so the prefilter is deliberately over-inclusive; the
        precise check is :meth:`matches`.
        """
        bodies = [re.sub("%+", "%", f"%{_to_sql_like(p)}") for p in self.patterns]
        if any(set(body) <= {"%"} for body in bodies):
            # A pure-wildcard pattern makes the whole OR-joined clause useless.
            return None
        return bodies


def relative_path(object_name: str, listing_prefix: str) -> str:
    """Return ``object_name`` relative to the prefix it was listed under."""
    if listing_prefix and object_name.startswith(listing_prefix):
        object_name = object_name[len(listing_prefix) :]
    return object_name.lstrip("/")


def supports_delimiter_listing(matcher: PatternMatcher, listing_prefix: str) -> bool:
    """True when the listing can be pruned to one level server-side.

    A non-recursive pattern can never match anything below the listed URI, so a
    delimited listing (GCS ``delimiter``, S3 ``Delimiter``) returns exactly the
    candidates - and skips enumerating subtrees entirely. Only safe when the
    prefix is a directory boundary; a partial prefix such as ``data`` selects
    objects by string prefix, where one level has no meaning.
    """
    if matcher.is_recursive:
        return False
    if any("/" in _normalize(p) for p in matcher.patterns):
        return False
    return not listing_prefix or listing_prefix.endswith("/")
