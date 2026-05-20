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
"""

from __future__ import annotations

import json
import logging
import re
from dataclasses import dataclass
from typing import Any, Callable, Dict, List, Optional, Tuple

logger = logging.getLogger(__name__)

# Operators we recognise.  Kept deliberately small for v1.
ALLOWED_OPS = {"eq", "ne", "in", "not_in", "is_null", "is_not_null", "matches"}
OPS_REQUIRING_VALUE = {"eq", "ne", "in", "not_in", "matches"}
OPS_REQUIRING_LIST = {"in", "not_in"}

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
            "description": "Optional dotted JSON path inside the column's value (e.g., 'aid.legal_entity').",
            "oneOf": [
                {"type": "string"},
                {"type": "array", "items": {"type": "string"}},
            ],
        },
        "op": {
            "type": "string",
            "enum": list(sorted(ALLOWED_OPS)),
            "default": "eq",
            "description": "Comparison operator.",
        },
        "value": {
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
        },
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
class FilterSpec:
    """One declarative row filter.

    ``path`` is a tuple of segments (``("aid", "legal_entity")``) rather
    than a dotted string so the dataclass stays hashable and so JSON path
    rendering doesn't have to re-parse on every call.
    """

    column: str
    path: Optional[Tuple[str, ...]] = None
    op: str = "eq"
    value: Any = None

    @property
    def path_str(self) -> Optional[str]:
        return ".".join(self.path) if self.path else None


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
    op = entry.get("op", "eq")
    if op not in ALLOWED_OPS:
        raise ValueError(
            f"filters[{i}]: unknown op {op!r}. Valid: {sorted(ALLOWED_OPS)}"
        )
    path = _parse_path(entry.get("path"), i)
    value = _parse_value(entry, op, i)
    return FilterSpec(column=column, path=path, op=op, value=value)


def _parse_value(entry: dict, op: str, i: int) -> Any:
    if op not in OPS_REQUIRING_VALUE:
        if "value" in entry:
            raise ValueError(f"filters[{i}]: op {op!r} does not take 'value'")
        return None
    if "value" not in entry:
        raise ValueError(f"filters[{i}]: op {op!r} requires 'value'")
    value = entry["value"]
    if op in OPS_REQUIRING_LIST and (not isinstance(value, list) or not value):
        raise ValueError(
            f"filters[{i}]: op {op!r} requires 'value' to be a non-empty list"
        )
    return value


def _parse_path(raw: Any, index: int) -> Optional[Tuple[str, ...]]:
    if raw is None:
        return None
    if isinstance(raw, str):
        segments = [seg for seg in raw.split(".") if seg]
        if not segments:
            raise ValueError(f"filters[{index}]: 'path' is empty")
        return tuple(segments)
    if isinstance(raw, list):
        if not raw or not all(isinstance(s, str) and s for s in raw):
            raise ValueError(
                f"filters[{index}]: 'path' must be a non-empty list of non-empty strings"
            )
        return tuple(raw)
    raise ValueError(
        f"filters[{index}]: 'path' must be a dotted string or list of strings"
    )


# ---------------------------------------------------------------------------
# Row-level evaluation (used by BasePipeline)
# ---------------------------------------------------------------------------


def _extract_value(row: Any, spec: FilterSpec) -> Any:
    """Pull the value referenced by ``spec`` out of a dict row.

    Returns ``None`` if the column is absent, the value is None, or the
    JSON path can't be walked.  Path access auto-parses JSON-encoded
    strings so users can filter into a stringified JSON column without
    extra config.
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
        if isinstance(cur, dict):
            cur = cur.get(seg)
        else:
            return None
        if cur is None:
            return None
    return cur


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


def _coerce_compare_value(spec: FilterSpec) -> Any:
    """Coerce ``spec.value`` when path is set; pass through otherwise."""
    if spec.path is None:
        return spec.value
    if spec.op in ("eq", "ne"):
        return _coerce_for_path(spec.value)
    if spec.op in ("in", "not_in"):
        return [_coerce_for_path(v) for v in spec.value]
    return spec.value  # 'matches' regex pattern, 'is_null'/'is_not_null' no value


def _eval(value: Any, spec: FilterSpec) -> bool:
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
