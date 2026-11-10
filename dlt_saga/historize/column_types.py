"""Nested column types: parsing a catalog spelling, and reconciling two of them.

The historize layer compares a source column's type with the historized
table's by the catalog's own spelling. For a scalar that is the whole story:
equal spellings are the same type, anything else is a change that can't be
taken in place (see ``HistorizeRunner._guard_column_types``). A struct, or an
array of structs, is different: a field added to or removed from it is the
nested form of a column added to or dropped from the table, and the layer
already handles those without a rebuild — a new column starts NULL, a dropped
one is read as NULL. The same holds one level down, because the row hash is
computed at run time from both sides and serialises a struct field by field,
so a widened struct whose new field is NULL on the stored row hashes the same
as a source row that has no value there.

This module gives that comparison a structure to work with: :func:`parse_type`
turns a spelling (BigQuery ``STRUCT<a STRING, b INT64>``, DuckDB
``STRUCT(a VARCHAR, b INTEGER)[]``, Databricks ``struct<a:string>``) into a
:class:`ColumnType` tree, and :func:`reconcile_types` says whether a source
type can be taken in place of the target's — fields added, fields removed —
or not, naming the nested path that changed. Spelling a type back and
reshaping a value from one type to another are SQL dialect, so they live on
the destination (``spell_column_type``, ``reshape_expression``).
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Callable, List, Optional, Tuple

SCALAR = "scalar"
STRUCT = "struct"
ARRAY = "array"


@dataclass(frozen=True)
class ColumnType:
    """A column type as a tree.

    Attributes:
        kind: ``scalar``, ``struct`` or ``array``.
        name: The scalar's spelling, parameters included (``DECIMAL(12, 2)``).
        fields: A struct's ``(name, type)`` pairs, in order.
        element: An array's element type.
    """

    kind: str
    name: str = ""
    fields: Tuple[Tuple[str, "ColumnType"], ...] = ()
    element: Optional["ColumnType"] = None

    @property
    def is_nested(self) -> bool:
        """Whether the type holds named fields somewhere inside it."""
        if self.kind == STRUCT:
            return True
        if self.kind == ARRAY and self.element is not None:
            return self.element.is_nested
        return False

    def field(self, name: str) -> Optional["ColumnType"]:
        """A struct field's type by name, case-insensitively."""
        for field_name, field_type in self.fields:
            if field_name.lower() == name.lower():
                return field_type
        return None


def scalar(name: str) -> ColumnType:
    return ColumnType(SCALAR, name=name)


def struct(*fields: Tuple[str, ColumnType]) -> ColumnType:
    return ColumnType(STRUCT, fields=tuple(fields))


def array(element: ColumnType) -> ColumnType:
    return ColumnType(ARRAY, element=element)


# ---------------------------------------------------------------------------
# Parsing
# ---------------------------------------------------------------------------

_TOKEN = re.compile(
    r"""
    \s*(?:
        (?P<quoted>"(?:[^"]|"")*"|`[^`]*`)   # a quoted name
      | (?P<punct>[<>()\[\],:])
      | (?P<word>[^\s<>()\[\],:"`]+)
    )
    """,
    re.VERBOSE,
)


class TypeSpellingError(ValueError):
    """A catalog type spelling this parser does not understand."""


def _tokenize(spelling: str) -> List[Tuple[str, str]]:
    tokens: List[Tuple[str, str]] = []
    pos = 0
    text = spelling.strip()
    while pos < len(text):
        match = _TOKEN.match(text, pos)
        if match is None or match.end() == pos:
            raise TypeSpellingError(f"Cannot parse type spelling {spelling!r}")
        pos = match.end()
        kind = match.lastgroup or "word"
        tokens.append((kind, match.group(kind)))
    return tokens


class _Parser:
    def __init__(self, spelling: str) -> None:
        self.spelling = spelling
        self.tokens = _tokenize(spelling)
        self.pos = 0

    def peek(self, offset: int = 0) -> Optional[Tuple[str, str]]:
        index = self.pos + offset
        return self.tokens[index] if index < len(self.tokens) else None

    def take(self) -> Tuple[str, str]:
        token = self.peek()
        if token is None:
            raise TypeSpellingError(
                f"Unexpected end of type spelling {self.spelling!r}"
            )
        self.pos += 1
        return token

    def expect(self, value: str) -> None:
        token = self.take()
        if token[1] != value:
            raise TypeSpellingError(
                f"Expected {value!r} in type spelling {self.spelling!r}, got {token[1]!r}"
            )

    def _is_punct(self, value: str, offset: int = 0) -> bool:
        token = self.peek(offset)
        return token is not None and token[0] == "punct" and token[1] == value

    def parse_type(self) -> ColumnType:
        token = self.peek()
        if token is None:
            raise TypeSpellingError(f"Empty type spelling {self.spelling!r}")
        word = token[1].upper() if token[0] == "word" else ""
        if word == "STRUCT" and (self._is_punct("<", 1) or self._is_punct("(", 1)):
            self.take()
            opener = self.take()[1]
            closer = ">" if opener == "<" else ")"
            parsed = struct(*self._parse_fields(closer))
        elif word == "ARRAY" and self._is_punct("<", 1):
            self.take()
            self.take()
            parsed = array(self.parse_type())
            self.expect(">")
        else:
            parsed = self._parse_scalar()
        # DuckDB spells an array as a suffix: ``STRUCT(...)[]``, ``VARCHAR[]``.
        while self._is_punct("[") and self._is_punct("]", 1):
            self.take()
            self.take()
            parsed = array(parsed)
        return parsed

    def _parse_fields(self, closer: str) -> List[Tuple[str, ColumnType]]:
        fields: List[Tuple[str, ColumnType]] = []
        while True:
            kind, name = self.take()
            if kind == "punct":
                raise TypeSpellingError(
                    f"Expected a field name in type spelling {self.spelling!r}, "
                    f"got {name!r}"
                )
            if kind == "quoted":
                name = _unquote(name)
            if self._is_punct(":"):  # Databricks: ``struct<a:string>``
                self.take()
            fields.append((name, self.parse_type()))
            token = self.take()
            if token[1] == closer:
                return fields
            if token[1] != ",":
                raise TypeSpellingError(
                    f"Expected ',' or {closer!r} in type spelling {self.spelling!r}, "
                    f"got {token[1]!r}"
                )

    def _parse_scalar(self) -> ColumnType:
        # A scalar is one or more words (``TIMESTAMP WITH TIME ZONE``), then an
        # optional balanced parameter group (``DECIMAL(12, 2)``, ``MAP(K, V)``).
        words: List[str] = []
        while True:
            token = self.peek()
            if token is None or token[0] != "word":
                break
            words.append(self.take()[1])
        if not words:
            token = self.peek()
            raise TypeSpellingError(
                f"Expected a type name in type spelling {self.spelling!r}, got "
                f"{token[1] if token else 'end'!r}"
            )
        name = " ".join(words)
        if self._is_punct("("):
            name += self._take_balanced()
        return scalar(name)

    def _take_balanced(self) -> str:
        """Consume a ``(...)`` group verbatim, with canonical spacing."""
        depth = 0
        parts: List[str] = []
        while True:
            kind, value = self.take()
            if value == "(":
                depth += 1
                parts.append("(")
            elif value == ")":
                depth -= 1
                parts.append(")")
                if depth == 0:
                    return "".join(parts)
            elif value == ",":
                parts.append(", ")
            else:
                if parts and parts[-1] not in ("(", ", "):
                    parts.append(" ")
                parts.append(value)


def _unquote(name: str) -> str:
    if name.startswith('"'):
        return name[1:-1].replace('""', '"')
    return name[1:-1]


def parse_type(spelling: str) -> ColumnType:
    """Parse a catalog type spelling into a :class:`ColumnType`.

    Raises:
        TypeSpellingError: when the spelling is not understood. Callers treat
            that as "not comparable", never as "equal".
    """
    parser = _Parser(spelling)
    parsed = parser.parse_type()
    if parser.peek() is not None:
        raise TypeSpellingError(
            f"Trailing text in type spelling {spelling!r}: {parser.peek()[1]!r}"
        )
    return parsed


# ---------------------------------------------------------------------------
# Reconciliation
# ---------------------------------------------------------------------------

Normalize = Callable[[str], str]


@dataclass
class TypeReconciliation:
    """How a source type relates to the target's.

    Attributes:
        merged: The type the target should have to hold the source's values:
            the target's own fields in their order, then the fields the
            source added, at every level. Equal to the target's type when
            nothing was added.
        added: Dotted paths of fields the source has and the target lacks.
        removed: Dotted paths of fields the target has and the source lacks.
        conflict: Why the two can't be reconciled, or ``None``: a scalar whose
            type changed (``b.x: INT64 → STRING``), a struct where a scalar
            was, an array where a struct was.
    """

    merged: ColumnType
    added: List[str] = field(default_factory=list)
    removed: List[str] = field(default_factory=list)
    conflict: Optional[str] = None

    @property
    def compatible(self) -> bool:
        return self.conflict is None

    @property
    def unchanged(self) -> bool:
        return self.compatible and not self.added and not self.removed


def reconcile_types(
    target: ColumnType, source: ColumnType, normalize: Normalize
) -> TypeReconciliation:
    """Say whether *source* can be taken in place of *target*, and how.

    ``normalize`` is the destination's canonical spelling of a scalar name,
    so a catalog alias does not read as a type change.
    """
    result = TypeReconciliation(merged=target)
    result.merged = _reconcile(target, source, normalize, "", result)
    return result


def _reconcile(
    target: ColumnType,
    source: ColumnType,
    normalize: Normalize,
    path: str,
    result: TypeReconciliation,
) -> ColumnType:
    if result.conflict is not None:
        return target
    where = path or "<column>"
    if target.kind != source.kind:
        result.conflict = (
            f"{where}: {_describe(target)} in the historized table, "
            f"{_describe(source)} in the source"
        )
        return target
    if target.kind == SCALAR:
        if normalize(target.name) != normalize(source.name):
            result.conflict = (
                f"{where}: {target.name} in the historized table, "
                f"{source.name} in the source"
            )
        return target
    if target.kind == ARRAY:
        assert target.element is not None and source.element is not None
        return array(
            _reconcile(target.element, source.element, normalize, path, result)
        )

    merged: List[Tuple[str, ColumnType]] = []
    for name, target_field in target.fields:
        child = f"{path}.{name}" if path else name
        source_field = source.field(name)
        if source_field is None:
            result.removed.append(child)
            merged.append((name, target_field))
            continue
        merged.append(
            (name, _reconcile(target_field, source_field, normalize, child, result))
        )
    for name, source_field in source.fields:
        if target.field(name) is None:
            child = f"{path}.{name}" if path else name
            result.added.append(child)
            merged.append((name, source_field))
    return struct(*merged)


def _describe(column_type: ColumnType) -> str:
    if column_type.kind == SCALAR:
        return column_type.name
    if column_type.kind == ARRAY:
        return "an array"
    return "a struct"


def types_equal(a: ColumnType, b: ColumnType, normalize: Normalize) -> bool:
    """Structural equality: same shape, same field names and order, same scalars."""
    if a.kind != b.kind:
        return False
    if a.kind == SCALAR:
        return normalize(a.name) == normalize(b.name)
    if a.kind == ARRAY:
        assert a.element is not None and b.element is not None
        return types_equal(a.element, b.element, normalize)
    if len(a.fields) != len(b.fields):
        return False
    return all(
        name_a.lower() == name_b.lower() and types_equal(type_a, type_b, normalize)
        for (name_a, type_a), (name_b, type_b) in zip(a.fields, b.fields)
    )


def leaf_paths(column_type: ColumnType, prefix: str) -> List[str]:
    """Dotted paths of every named field under *prefix*, nested ones included.

    Recorded per run next to the column names, so a field the source drops
    can be warned about once rather than on every run (see
    ``HistorizeRunner._recorded_source_columns``).
    """
    if column_type.kind == ARRAY and column_type.element is not None:
        return leaf_paths(column_type.element, prefix)
    paths: List[str] = []
    for name, field_type in column_type.fields:
        child = f"{prefix}.{name}"
        paths.append(child)
        paths.extend(leaf_paths(field_type, child))
    return paths
