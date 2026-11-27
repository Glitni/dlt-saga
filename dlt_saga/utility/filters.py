"""Declarative row filters applied during ingest.

The YAML ``filters:`` block on a pipeline config compiles to a list of
``FilterSpec`` objects.  Two consumers:

1. ``BasePipeline`` evaluates filters per-row in Python (works for every
   source — CSV, Sheets, API, database — even when the source can't push
   down the predicate to the upstream query).
2. ``NativeLoadPipeline`` asks the destination to render the same specs as
   a SQL ``WHERE`` clause for pushdown into ``INSERT … SELECT`` or
   ``COPY INTO`` so the warehouse never materialises filtered-out rows.

Multiple entries are AND-joined.  No support for OR/grouping in v1 —
keep it simple and add later if needed.

YAML schema::

    filters:
      - column: config             # required: top-level column name
        path: aid.legal_entity     # optional: dotted JSON path inside column
        op: eq                     # default: eq
        value: bm                  # required for eq/ne/in/not_in/matches

A path in list form may hold a selector in place of a key: a mapping
(``{path, op, value}``) that picks the first element of the array at that
position whose value at ``path`` satisfies the comparison.
"""

from __future__ import annotations

import json
import logging
import re
from dataclasses import dataclass
from typing import Any, Callable, Dict, List, Optional, Tuple, Union

logger = logging.getLogger(__name__)

# Operators we recognise.  Kept deliberately small for v1.
ALLOWED_OPS = {"eq", "ne", "in", "not_in", "is_null", "is_not_null", "matches"}
OPS_REQUIRING_VALUE = {"eq", "ne", "in", "not_in", "matches"}
OPS_REQUIRING_LIST = {"in", "not_in"}

_VALUE_SCHEMA: Dict[str, Any] = {
    "description": "Comparison value. Required for eq/ne/in/not_in/matches; omit for is_null/is_not_null. List required for in/not_in.",
    "oneOf": [
        {"type": "string"},
        {"type": "number"},
        {"type": "boolean"},
        {"type": "null"},
        {
            "type": "array",
            "items": {
                "oneOf": [
                    {"type": "string"},
                    {"type": "number"},
                    {"type": "boolean"},
                ]
            },
        },
    ],
}

_OP_SCHEMA: Dict[str, Any] = {
    "type": "string",
    "enum": list(sorted(ALLOWED_OPS)),
    "default": "eq",
    "description": "Comparison operator.",
}

# One item of a path in list form that is not a key: picks an array element.
JSON_PATH_SELECTOR_SCHEMA: Dict[str, Any] = {
    "type": "object",
    "description": "Selects the first element of the array at this position whose value at 'path' satisfies the comparison.",
    "properties": {
        "path": {
            "description": "Keys from the element to the value compared, as a dotted string or a list of keys.",
            "oneOf": [
                {"type": "string"},
                {"type": "array", "items": {"type": "string"}},
            ],
        },
        "op": _OP_SCHEMA,
        "value": _VALUE_SCHEMA,
    },
    "required": ["path"],
    "additionalProperties": False,
}

# The ``path`` key of a filter or a ``historize.json_columns`` entry.
JSON_PATH_SCHEMA: List[Dict[str, Any]] = [
    {"type": "string"},
    {
        "type": "array",
        "items": {"oneOf": [{"type": "string"}, JSON_PATH_SELECTOR_SCHEMA]},
    },
]

# Shared JSON-schema fragment describing one ``filters:`` entry.  Both the
# top-level ``filters:`` (ingest stage) and ``historize.filters:`` reference
# this so the editor-side validation rules can't drift between the two.
FILTER_ENTRY_SCHEMA: Dict[str, Any] = {
    "type": "object",
    "properties": {
        "column": {
            "type": "string",
            "description": "Source column name (case-sensitive for most paths).",
        },
        "path": {
            "description": "Optional JSON path inside the column's value, as a dotted string ('aid.legal_entity') or a list of keys. Keys that aren't plain identifiers ('content-type') are quoted for the destination; a key containing '.' needs the list form. In the list form an item may be a selector ({path, op, value}) picking the first matching element of the array at that position; a path that ends at a selector takes only is_null/is_not_null.",
            "oneOf": JSON_PATH_SCHEMA,
        },
        "op": _OP_SCHEMA,
        "value": _VALUE_SCHEMA,
    },
    "required": ["column"],
    "additionalProperties": False,
}


def filter_field_metadata(description: str) -> Dict[str, Any]:
    """Build the dataclass field metadata for a ``filters:`` field.

    Used by both ``BaseConfig.filters`` (ingest) and
    ``HistorizeConfig.filters`` (historize).  Only the description differs
    between the two — everything else is structural and stays in
    ``FILTER_ENTRY_SCHEMA``.
    """
    return {"description": description, "items": FILTER_ENTRY_SCHEMA}


def filter_where_clause(filter_sql: Optional[str]) -> str:
    """Return ``" WHERE <filter>"`` (with leading space) or empty string.

    For source-read sites that have no other WHERE clause to AND into.
    """
    return f" WHERE {filter_sql}" if filter_sql else ""


def and_filter(filter_sql: Optional[str], where_body: str) -> str:
    """AND a rendered filter into an existing WHERE body, or pass through.

    Wraps the filter in parentheses so operator precedence is deterministic
    regardless of what's already in ``where_body``.
    """
    if not filter_sql:
        return where_body
    return f"{where_body} AND ({filter_sql})"


@dataclass(frozen=True)
class ElementSelector:
    """A path segment that picks one element of the array at its position.

    The first element, in array order, whose value at ``path`` satisfies
    ``op``/``value`` under the rules of a path-based filter (compared as text).
    """

    path: Tuple[str, ...]
    op: str = "eq"
    value: Any = None

    def definition(self) -> Dict[str, Any]:
        """The selector as its config mapping, with defaults spelled out."""
        value = list(self.value) if isinstance(self.value, tuple) else self.value
        return {"path": list(self.path), "op": self.op, "value": value}

    def __str__(self) -> str:
        condition = f"{'.'.join(self.path)} {self.op}"
        if self.op in OPS_REQUIRING_VALUE:
            condition += f" {self.definition()['value']!r}"
        return f"[{condition}]"


JsonPathSegment = Union[str, ElementSelector]
JsonPath = Tuple[JsonPathSegment, ...]


def format_json_path(path: JsonPath) -> str:
    """A path as text for messages: keys dotted, a selector in brackets."""
    text = ""
    for segment in path:
        if isinstance(segment, ElementSelector):
            text += str(segment)
        else:
            text += f".{segment}" if text else segment
    return text


def json_path_definition(path: JsonPath) -> List[Any]:
    """A path as JSON-serialisable config: keys as written, selectors as mappings."""
    return [
        segment.definition() if isinstance(segment, ElementSelector) else segment
        for segment in path
    ]


@dataclass(frozen=True)
class FilterSpec:
    """One declarative row filter.

    ``path`` is a tuple of segments (``("aid", "legal_entity")``) rather
    than a dotted string so the dataclass stays hashable and so JSON path
    rendering doesn't have to re-parse on every call.
    """

    column: str
    path: Optional[JsonPath] = None
    op: str = "eq"
    value: Any = None

    @property
    def path_str(self) -> Optional[str]:
        return format_json_path(self.path) if self.path else None


def parse_filters(raw: Any, context: Optional[str] = None) -> List[FilterSpec]:
    """Parse the YAML ``filters:`` block.

    Returns an empty list when ``raw`` is None.  Raises ValueError with a
    user-facing message on malformed input — these are configuration
    errors, so the message should be enough for the user to fix the YAML
    without a traceback.

    Args:
        raw: Raw value from YAML (expected to be a list or None).
        context: Optional caller label (e.g. pipeline name, "historize")
            prefixed to the error message so the user can locate the bad
            config quickly when the same exception bubbles up through
            multiple layers.
    """
    try:
        return _parse_filters_impl(raw)
    except ValueError as exc:
        if context:
            raise ValueError(f"Invalid filters for {context}: {exc}") from exc
        raise


def _parse_filters_impl(raw: Any) -> List[FilterSpec]:
    if raw is None:
        return []
    if not isinstance(raw, list):
        raise ValueError(
            f"filters: must be a list of mappings, got {type(raw).__name__}"
        )
    return [_parse_one(entry, i) for i, entry in enumerate(raw)]


def _parse_one(entry: Any, i: int) -> FilterSpec:
    if not isinstance(entry, dict):
        raise ValueError(f"filters[{i}]: must be a mapping, got {type(entry).__name__}")
    column = entry.get("column")
    if not isinstance(column, str) or not column:
        raise ValueError(
            f"filters[{i}]: 'column' is required and must be a non-empty string"
        )
    label = f"filters[{i}]"
    op = _parse_op(entry, label)
    path = _parse_path(entry.get("path"), i)
    value = _parse_value(entry, op, label)
    if path and isinstance(path[-1], ElementSelector) and op in OPS_REQUIRING_VALUE:
        raise ValueError(
            f"{label}: 'path' ends at a selector, which yields the matched "
            f"element rather than a value to compare. Use is_null/is_not_null "
            f"to test whether an element matches, or continue the path to a "
            f"key inside the element"
        )
    return FilterSpec(column=column, path=path, op=op, value=value)


def _parse_op(entry: dict, label: str) -> str:
    op = entry.get("op", "eq")
    if op not in ALLOWED_OPS:
        raise ValueError(f"{label}: unknown op {op!r}. Valid: {sorted(ALLOWED_OPS)}")
    return op


def _parse_value(entry: dict, op: str, label: str) -> Any:
    if op not in OPS_REQUIRING_VALUE:
        if "value" in entry:
            raise ValueError(f"{label}: op {op!r} does not take 'value'")
        return None
    if "value" not in entry:
        raise ValueError(f"{label}: op {op!r} requires 'value'")
    value = entry["value"]
    if op in OPS_REQUIRING_LIST and (not isinstance(value, list) or not value):
        raise ValueError(f"{label}: op {op!r} requires 'value' to be a non-empty list")
    return value


def _parse_path(raw: Any, index: int) -> Optional[JsonPath]:
    if raw is None:
        return None
    return parse_json_path(raw, f"filters[{index}]")


def parse_json_path(raw: Any, label: str) -> JsonPath:
    """Parse a JSON path given as a dotted string or a list of segments.

    The one parser for every config key that addresses a value inside a JSON
    column (``filters`` and ``historize.json_columns``), so a path means the
    same thing everywhere.

    Args:
        raw: ``"a.b"``, or a list whose items are keys or selector mappings
            (``{path, op, value}``, see :class:`ElementSelector`).
        label: Config location prefixed to error messages.

    Raises:
        ValueError: The path is empty or holds anything but keys and selectors.
    """
    if isinstance(raw, str):
        return _parse_key_path(raw, label)
    if isinstance(raw, list) and raw:
        return tuple(
            _parse_segment(segment, f"{label}: path[{position}]")
            for position, segment in enumerate(raw)
        )
    raise ValueError(
        f"{label}: 'path' must be a dotted string or a non-empty list of keys "
        f"and selectors"
    )


def _parse_key_path(raw: Any, label: str) -> Tuple[str, ...]:
    """Parse a path of plain keys: a dotted string or a list of strings."""
    if isinstance(raw, str):
        segments = [seg for seg in raw.split(".") if seg]
        if not segments:
            raise ValueError(f"{label}: 'path' is empty")
        return tuple(segments)
    if isinstance(raw, list) and raw and all(isinstance(s, str) and s for s in raw):
        return tuple(raw)
    raise ValueError(
        f"{label}: 'path' must be a dotted string or a non-empty list of "
        f"non-empty strings"
    )


def _parse_segment(raw: Any, label: str) -> JsonPathSegment:
    if isinstance(raw, str) and raw:
        return raw
    if isinstance(raw, dict):
        return _parse_selector(raw, label)
    raise ValueError(
        f"{label}: must be a key (non-empty string) or a selector mapping "
        f"({{path, op, value}})"
    )


_SELECTOR_KEYS = {"path", "op", "value"}


def _parse_selector(entry: dict, label: str) -> ElementSelector:
    unknown = sorted(set(entry) - _SELECTOR_KEYS)
    if unknown:
        raise ValueError(
            f"{label}: unknown key(s) {unknown} in selector. "
            f"Valid: {sorted(_SELECTOR_KEYS)}"
        )
    if entry.get("path") is None:
        raise ValueError(
            f"{label}: a selector needs 'path', the key(s) inside each array "
            f"element holding the value to compare"
        )
    op = _parse_op(entry, label)
    value = _parse_value(entry, op, label)
    return ElementSelector(
        path=_parse_key_path(entry["path"], label),
        op=op,
        value=tuple(value) if isinstance(value, list) else value,
    )


# ---------------------------------------------------------------------------
# Row-level evaluation (used by BasePipeline)
# ---------------------------------------------------------------------------


def _extract_value(row: Any, spec: FilterSpec) -> Any:
    """Pull the value referenced by ``spec`` out of a dict row.

    Returns ``None`` if the column is absent, the value is None, or the
    JSON path can't be walked.  Path access auto-parses JSON-encoded
    strings so users can filter into a stringified JSON column without
    extra config.  A selector segment takes the first matching element of
    the list at its position.
    """
    if not isinstance(row, dict):
        return None
    if spec.column not in row:
        return None
    cur = row[spec.column]
    if spec.path is None:
        return cur
    if isinstance(cur, str):
        try:
            cur = json.loads(cur)
        except (json.JSONDecodeError, TypeError):
            return None
    for seg in spec.path:
        if isinstance(seg, ElementSelector):
            cur = _first_matching_element(cur, seg)
        elif isinstance(cur, dict):
            cur = cur.get(seg)
        else:
            return None
        if cur is None:
            return None
    return cur


def _first_matching_element(value: Any, selector: ElementSelector) -> Any:
    if not isinstance(value, list):
        return None
    for element in value:
        candidate = element
        for key in selector.path:
            candidate = candidate.get(key) if isinstance(candidate, dict) else None
        # A comparison with a missing value is never true in SQL.
        if candidate is None and selector.op != "is_null":
            continue
        if _eval(candidate, selector):
            return element
    return None


def _coerce_for_path(value: Any) -> Optional[str]:
    """Coerce a path-extracted value to a STRING for comparison.

    Path-based filters always compare as strings.  Every destination's
    JSON-path access returns a string scalar (the JSON wire form of the
    leaf), so the Python side coerces the same way to keep semantics
    identical whether the filter evaluates in Python (BasePipeline) or
    pushes down to SQL (native_load).  Booleans use lowercase
    ``"true"``/``"false"`` to match the JSON wire form rather than
    Python's ``str(True) == "True"``.
    """
    if value is None:
        return None
    if isinstance(value, bool):
        return "true" if value else "false"
    return str(value)


def _coerce_compare_value(spec: Union[FilterSpec, ElementSelector]) -> Any:
    """Coerce ``spec.value`` when path is set; pass through otherwise."""
    if spec.path is None:
        return spec.value
    if spec.op in ("eq", "ne"):
        return _coerce_for_path(spec.value)
    if spec.op in ("in", "not_in"):
        return [_coerce_for_path(v) for v in spec.value]
    return spec.value  # 'matches' regex pattern, 'is_null'/'is_not_null' no value


def _eval(value: Any, spec: Union[FilterSpec, ElementSelector]) -> bool:
    op = spec.op
    if spec.path is not None:
        value = _coerce_for_path(value)
    cmp_value = _coerce_compare_value(spec)

    if op == "is_null":
        return value is None
    if op == "is_not_null":
        return value is not None
    if op == "eq":
        return value == cmp_value
    if op == "ne":
        return value != cmp_value
    if op == "in":
        return value in cmp_value
    if op == "not_in":
        return value not in cmp_value
    if op == "matches":
        if value is None:
            return False
        return re.search(cmp_value, str(value)) is not None
    raise ValueError(f"Unsupported op {op!r}")


def _spec_applies(row_keys: Any, spec: FilterSpec) -> bool:
    """Skip filters whose column isn't in the row's schema.

    Pipelines often yield multiple resources with different shapes.  A
    filter on ``config`` shouldn't drop every row from a resource that
    has no ``config`` column — it should silently not apply.  Filters
    that don't match any column are caught at pipeline-config validation
    time (out of scope here).
    """
    return spec.column in row_keys


def build_row_predicate(specs: List[FilterSpec]) -> Callable[[dict], bool]:
    """Return a callable that returns True if a dict row passes all specs."""

    def predicate(row: dict) -> bool:
        if not isinstance(row, dict):
            return True
        for spec in specs:
            if not _spec_applies(row, spec):
                continue
            value = _extract_value(row, spec)
            if not _eval(value, spec):
                return False
        return True

    return predicate


def apply_filters_to_arrow(table: Any, specs: List[FilterSpec]) -> Any:
    """Return ``table`` filtered to rows passing every spec.

    Implemented as a Python-side row iteration via ``to_pylist``.  Slower
    than ``pyarrow.compute`` for plain column comparisons but correct
    for JSON-path filters (which compute doesn't natively support).
    Optimise later if profiling shows it matters — most use cases are
    small reference tables where the cost is irrelevant.
    """
    import pyarrow as pa

    if isinstance(table, pa.RecordBatch):
        table = pa.Table.from_batches([table])

    schema_names = set(table.schema.names)
    applicable = [s for s in specs if s.column in schema_names]
    if not applicable:
        return table

    pred = build_row_predicate(applicable)
    rows = table.to_pylist()
    mask = pa.array([pred(r) for r in rows])
    return table.filter(mask)
