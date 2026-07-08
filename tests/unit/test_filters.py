"""Unit tests for declarative ingest filters."""

from unittest.mock import MagicMock

import pytest

from dlt_saga.destinations.base import Destination
from dlt_saga.utility.filters import (
    FilterSpec,
    apply_filters_to_arrow,
    build_row_predicate,
    parse_filters,
)

# ---------------------------------------------------------------------------
# parse_filters
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestParseFilters:
    def test_none_returns_empty(self):
        assert parse_filters(None) == []

    def test_empty_list_returns_empty(self):
        assert parse_filters([]) == []

    def test_simple_eq(self):
        specs = parse_filters([{"column": "x", "op": "eq", "value": 1}])
        assert specs == [FilterSpec(column="x", op="eq", value=1)]

    def test_eq_is_default_op(self):
        specs = parse_filters([{"column": "x", "value": 1}])
        assert specs[0].op == "eq"

    def test_dotted_path_string(self):
        specs = parse_filters(
            [{"column": "config", "path": "aid.legal_entity", "value": "bm"}]
        )
        assert specs[0].path == ("aid", "legal_entity")

    def test_path_list(self):
        specs = parse_filters(
            [{"column": "config", "path": ["aid", "legal_entity"], "value": "bm"}]
        )
        assert specs[0].path == ("aid", "legal_entity")

    def test_in_requires_list(self):
        with pytest.raises(ValueError, match="non-empty list"):
            parse_filters([{"column": "x", "op": "in", "value": 1}])

    def test_in_rejects_empty_list(self):
        with pytest.raises(ValueError, match="non-empty list"):
            parse_filters([{"column": "x", "op": "in", "value": []}])

    def test_is_null_no_value(self):
        specs = parse_filters([{"column": "x", "op": "is_null"}])
        assert specs[0].op == "is_null"
        assert specs[0].value is None

    def test_is_null_rejects_value(self):
        with pytest.raises(ValueError, match="does not take 'value'"):
            parse_filters([{"column": "x", "op": "is_null", "value": 1}])

    def test_eq_requires_value(self):
        with pytest.raises(ValueError, match="requires 'value'"):
            parse_filters([{"column": "x", "op": "eq"}])

    def test_unknown_op(self):
        with pytest.raises(ValueError, match="unknown op"):
            parse_filters([{"column": "x", "op": "foo", "value": 1}])

    def test_missing_column(self):
        with pytest.raises(ValueError, match="'column' is required"):
            parse_filters([{"op": "eq", "value": 1}])

    def test_not_a_list(self):
        with pytest.raises(ValueError, match="must be a list"):
            parse_filters("not a list")

    def test_entry_not_mapping(self):
        with pytest.raises(ValueError, match="must be a mapping"):
            parse_filters(["not a dict"])

    def test_empty_path_string(self):
        with pytest.raises(ValueError, match="'path' is empty"):
            parse_filters([{"column": "x", "path": "...", "value": 1}])


# ---------------------------------------------------------------------------
# build_row_predicate
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestRowPredicate:
    def test_eq_pass(self):
        specs = parse_filters([{"column": "x", "op": "eq", "value": 1}])
        pred = build_row_predicate(specs)
        assert pred({"x": 1}) is True

    def test_eq_fail(self):
        specs = parse_filters([{"column": "x", "op": "eq", "value": 1}])
        pred = build_row_predicate(specs)
        assert pred({"x": 2}) is False

    def test_multiple_filters_and(self):
        specs = parse_filters(
            [
                {"column": "x", "op": "eq", "value": 1},
                {"column": "y", "op": "eq", "value": 2},
            ]
        )
        pred = build_row_predicate(specs)
        assert pred({"x": 1, "y": 2}) is True
        assert pred({"x": 1, "y": 3}) is False
        assert pred({"x": 2, "y": 2}) is False

    def test_in_op(self):
        specs = parse_filters([{"column": "x", "op": "in", "value": [1, 2, 3]}])
        pred = build_row_predicate(specs)
        assert pred({"x": 2}) is True
        assert pred({"x": 4}) is False

    def test_not_in_op(self):
        specs = parse_filters([{"column": "x", "op": "not_in", "value": [1, 2]}])
        pred = build_row_predicate(specs)
        assert pred({"x": 3}) is True
        assert pred({"x": 1}) is False

    def test_is_null(self):
        specs = parse_filters([{"column": "x", "op": "is_null"}])
        pred = build_row_predicate(specs)
        assert pred({"x": None}) is True
        assert pred({"x": 1}) is False

    def test_is_not_null(self):
        specs = parse_filters([{"column": "x", "op": "is_not_null"}])
        pred = build_row_predicate(specs)
        assert pred({"x": 1}) is True
        assert pred({"x": None}) is False

    def test_matches_op(self):
        specs = parse_filters([{"column": "x", "op": "matches", "value": "^foo"}])
        pred = build_row_predicate(specs)
        assert pred({"x": "foobar"}) is True
        assert pred({"x": "barfoo"}) is False
        assert pred({"x": None}) is False

    def test_path_into_dict(self):
        specs = parse_filters(
            [{"column": "config", "path": "aid.legal_entity", "value": "bm"}]
        )
        pred = build_row_predicate(specs)
        assert pred({"config": {"aid": {"legal_entity": "bm"}}}) is True
        assert pred({"config": {"aid": {"legal_entity": "bt"}}}) is False

    def test_path_auto_parses_json_string(self):
        specs = parse_filters(
            [{"column": "config", "path": "aid.legal_entity", "value": "bm"}]
        )
        pred = build_row_predicate(specs)
        assert pred({"config": '{"aid": {"legal_entity": "bm"}}'}) is True
        assert pred({"config": '{"aid": {"legal_entity": "bt"}}'}) is False

    def test_path_missing_intermediate_drops_row(self):
        specs = parse_filters(
            [{"column": "config", "path": "aid.legal_entity", "value": "bm"}]
        )
        pred = build_row_predicate(specs)
        assert pred({"config": {"other": "x"}}) is False  # path can't be walked

    def test_path_coerces_int_value_to_string(self):
        """Path-extracted values compare as STRING — same in Python and SQL."""
        specs = parse_filters(
            [{"column": "config", "path": "settings.version", "op": "eq", "value": 5}]
        )
        pred = build_row_predicate(specs)
        # int leaf, int value: both coerce to "5" → match
        assert pred({"config": {"settings": {"version": 5}}}) is True
        # int leaf, mismatched int value
        specs2 = parse_filters(
            [{"column": "config", "path": "settings.version", "op": "eq", "value": 5}]
        )
        pred2 = build_row_predicate(specs2)
        assert pred2({"config": {"settings": {"version": 6}}}) is False

    def test_path_coerces_bool_to_lowercase_json_form(self):
        """Booleans on path use 'true'/'false' (JSON form), not Python's 'True'/'False'."""
        specs = parse_filters(
            [{"column": "config", "path": "enabled", "op": "eq", "value": True}]
        )
        pred = build_row_predicate(specs)
        assert pred({"config": {"enabled": True}}) is True
        assert pred({"config": {"enabled": False}}) is False
        # Cross-form: user wrote string "true" — should also match the bool leaf
        specs2 = parse_filters(
            [{"column": "config", "path": "enabled", "op": "eq", "value": "true"}]
        )
        pred2 = build_row_predicate(specs2)
        assert pred2({"config": {"enabled": True}}) is True

    def test_path_in_coerces_list_values(self):
        specs = parse_filters(
            [
                {
                    "column": "config",
                    "path": "version",
                    "op": "in",
                    "value": [1, 2, 3],
                }
            ]
        )
        pred = build_row_predicate(specs)
        assert pred({"config": {"version": 2}}) is True
        assert pred({"config": {"version": 4}}) is False

    def test_no_path_preserves_native_types(self):
        """Without path, eq behaves with native Python equality (no string coercion)."""
        specs = parse_filters([{"column": "x", "op": "eq", "value": 1}])
        pred = build_row_predicate(specs)
        assert pred({"x": 1}) is True
        # str "1" should NOT match int 1 — no coercion when path is absent.
        assert pred({"x": "1"}) is False

    def test_missing_column_skips_filter(self):
        """If the column isn't in the row, the filter should not apply.

        Multi-resource pipelines have heterogeneous shapes — a filter on
        ``config`` shouldn't wipe out resources that have no ``config``.
        """
        specs = parse_filters([{"column": "config", "op": "eq", "value": "bm"}])
        pred = build_row_predicate(specs)
        assert pred({"other_col": "x"}) is True


# ---------------------------------------------------------------------------
# apply_filters_to_arrow
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestArrowFilter:
    def test_filters_arrow_table(self):
        pa = pytest.importorskip("pyarrow")
        table = pa.table({"x": [1, 2, 3, 4], "y": ["a", "b", "c", "d"]})
        specs = parse_filters([{"column": "x", "op": "in", "value": [2, 4]}])
        result = apply_filters_to_arrow(table, specs)
        assert result.column("x").to_pylist() == [2, 4]
        assert result.column("y").to_pylist() == ["b", "d"]

    def test_missing_column_passes_through(self):
        pa = pytest.importorskip("pyarrow")
        table = pa.table({"other": [1, 2, 3]})
        specs = parse_filters([{"column": "config", "op": "eq", "value": "x"}])
        result = apply_filters_to_arrow(table, specs)
        # No filter applies → table unchanged
        assert result.num_rows == 3

    def test_json_path_on_string_column(self):
        pa = pytest.importorskip("pyarrow")
        rows = [
            '{"aid": {"legal_entity": "bm"}}',
            '{"aid": {"legal_entity": "bt"}}',
            '{"aid": {"legal_entity": "bm"}}',
        ]
        table = pa.table({"config": rows, "id": [1, 2, 3]})
        specs = parse_filters(
            [{"column": "config", "path": "aid.legal_entity", "value": "bm"}]
        )
        result = apply_filters_to_arrow(table, specs)
        assert result.column("id").to_pylist() == [1, 3]


# ---------------------------------------------------------------------------
# Destination.render_filter — default dialect
# ---------------------------------------------------------------------------


class _BaseDestination(Destination):
    """Minimal concrete Destination using only the default render_filter."""

    def __init__(self):
        self.config = MagicMock()

    def create_dlt_destination(self):
        return None

    def apply_hints(self, resource, **hints):
        return resource

    def get_access_manager(self):
        return None

    def supports_access_management(self):
        return False

    def supports_partitioning(self):
        return False

    def supports_clustering(self):
        return False


@pytest.mark.unit
class TestRenderFilterDefault:
    def test_empty_returns_empty_string(self):
        dest = _BaseDestination()
        assert dest.render_filter([]) == ""

    def test_eq_string(self):
        dest = _BaseDestination()
        specs = parse_filters([{"column": "name", "op": "eq", "value": "alice"}])
        assert dest.render_filter(specs) == "\"name\" = 'alice'"

    def test_eq_int(self):
        dest = _BaseDestination()
        specs = parse_filters([{"column": "id", "op": "eq", "value": 42}])
        assert dest.render_filter(specs) == '"id" = 42'

    def test_eq_bool(self):
        dest = _BaseDestination()
        specs = parse_filters([{"column": "active", "op": "eq", "value": True}])
        assert dest.render_filter(specs) == '"active" = TRUE'

    def test_in(self):
        dest = _BaseDestination()
        specs = parse_filters(
            [{"column": "name", "op": "in", "value": ["a", "b", "c"]}]
        )
        assert dest.render_filter(specs) == "\"name\" IN ('a', 'b', 'c')"

    def test_not_in(self):
        dest = _BaseDestination()
        specs = parse_filters([{"column": "id", "op": "not_in", "value": [1, 2]}])
        assert dest.render_filter(specs) == '"id" NOT IN (1, 2)'

    def test_is_null(self):
        dest = _BaseDestination()
        specs = parse_filters([{"column": "x", "op": "is_null"}])
        assert dest.render_filter(specs) == '"x" IS NULL'

    def test_is_not_null(self):
        dest = _BaseDestination()
        specs = parse_filters([{"column": "x", "op": "is_not_null"}])
        assert dest.render_filter(specs) == '"x" IS NOT NULL'

    def test_ne(self):
        dest = _BaseDestination()
        specs = parse_filters([{"column": "x", "op": "ne", "value": "y"}])
        assert dest.render_filter(specs) == "\"x\" <> 'y'"

    def test_and_join(self):
        dest = _BaseDestination()
        specs = parse_filters(
            [
                {"column": "x", "op": "eq", "value": 1},
                {"column": "y", "op": "eq", "value": 2},
            ]
        )
        assert dest.render_filter(specs) == '"x" = 1 AND "y" = 2'

    def test_default_json_path(self):
        """Default dialect uses JSON_VALUE — works on BigQuery/DuckDB/Postgres."""
        dest = _BaseDestination()
        specs = parse_filters(
            [{"column": "config", "path": "aid.legal_entity", "value": "bm"}]
        )
        assert (
            dest.render_filter(specs)
            == "JSON_VALUE(\"config\", '$.aid.legal_entity') = 'bm'"
        )

    def test_path_renders_int_value_as_string(self):
        """Path-based JSON access returns STRING — comparisons must be STRING-STRING."""
        dest = _BaseDestination()
        specs = parse_filters(
            [{"column": "config", "path": "settings.version", "value": 5}]
        )
        # 5 (int) renders as '5' (quoted string), not bare 5.
        assert (
            dest.render_filter(specs)
            == "JSON_VALUE(\"config\", '$.settings.version') = '5'"
        )

    def test_path_renders_bool_lowercase(self):
        dest = _BaseDestination()
        specs = parse_filters([{"column": "config", "path": "enabled", "value": True}])
        # True renders as 'true' to match JSON wire form.
        assert (
            dest.render_filter(specs) == "JSON_VALUE(\"config\", '$.enabled') = 'true'"
        )

    def test_path_in_renders_values_as_strings(self):
        dest = _BaseDestination()
        specs = parse_filters(
            [{"column": "config", "path": "version", "op": "in", "value": [1, 2]}]
        )
        assert (
            dest.render_filter(specs)
            == "JSON_VALUE(\"config\", '$.version') IN ('1', '2')"
        )

    def test_no_path_preserves_value_type(self):
        """Top-level column without path keeps native SQL literal types."""
        dest = _BaseDestination()
        specs = parse_filters([{"column": "id", "value": 5}])
        assert dest.render_filter(specs) == '"id" = 5'

    def test_string_escaping(self):
        # Base dialect is standard SQL: the single quote is doubled ('').
        dest = _BaseDestination()
        specs = parse_filters([{"column": "x", "op": "eq", "value": "o'brien"}])
        assert dest.render_filter(specs) == "\"x\" = 'o''brien'"

    def test_custom_column_resolver(self):
        """When a resolver is passed, column refs go through it."""
        dest = _BaseDestination()
        specs = parse_filters([{"column": "config", "op": "eq", "value": "x"}])

        def upper_resolver(name):
            return f"`{name.upper()}`"

        assert (
            dest.render_filter(specs, column_resolver=upper_resolver)
            == "`CONFIG` = 'x'"
        )


# ---------------------------------------------------------------------------
# Databricks colon-notation override
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestRenderFilterDatabricks:
    """Exercises the Databricks dialect override without needing a live connection."""

    def _make_dest(self):
        pytest.importorskip("databricks.sql")
        from dlt_saga.destinations.databricks.destination import DatabricksDestination

        dest = DatabricksDestination.__new__(DatabricksDestination)
        dest.config = MagicMock()
        return dest

    def test_path_uses_colon_notation(self):
        dest = self._make_dest()
        specs = parse_filters(
            [{"column": "config", "path": "aid.legal_entity", "value": "bm"}]
        )
        assert dest.render_filter(specs) == "`config`:aid.legal_entity = 'bm'"

    def test_no_path_plain_column(self):
        dest = self._make_dest()
        specs = parse_filters([{"column": "x", "op": "eq", "value": 1}])
        assert dest.render_filter(specs) == "`x` = 1"

    def test_regex_uses_rlike(self):
        dest = self._make_dest()
        specs = parse_filters([{"column": "x", "op": "matches", "value": "^foo"}])
        assert dest.render_filter(specs) == "`x` RLIKE '^foo'"


# ---------------------------------------------------------------------------
# BigQuery dialect — regex
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestRenderFilterBigQuery:
    def _make_dest(self):
        pytest.importorskip("google.cloud.bigquery")
        from dlt_saga.destinations.bigquery.destination import BigQueryDestination

        dest = BigQueryDestination.__new__(BigQueryDestination)
        dest.config = MagicMock()
        return dest

    def test_regex_uses_regexp_contains(self):
        dest = self._make_dest()
        specs = parse_filters([{"column": "x", "op": "matches", "value": "^foo"}])
        # Normal (non-raw) literal + backslash escaping — a raw r'' literal can't
        # carry a quote; the parser unescapes \\d back to \d for the regex engine.
        assert dest.render_filter(specs) == "REGEXP_CONTAINS(`x`, '^foo')"

    def test_native_load_where_resolves_case_insensitive(self):
        """Filter on logical name `config` should bind to ext column `Config`."""
        dest = self._make_dest()
        spec = MagicMock()
        spec.filters = parse_filters(
            [{"column": "config", "path": "aid.legal_entity", "value": "bm"}]
        )
        ext_cols = [("Config", "STRING"), ("id", "INT64")]
        where = dest._render_native_load_where(spec, ext_cols)
        # Raw external column name preserved, JSON_VALUE used for path access.
        assert where == "JSON_VALUE(`Config`, '$.aid.legal_entity') = 'bm'"

    def test_native_load_where_unknown_column_raises(self):
        dest = self._make_dest()
        spec = MagicMock()
        spec.filters = parse_filters([{"column": "missing", "op": "eq", "value": "x"}])
        ext_cols = [("config", "STRING")]
        with pytest.raises(ValueError, match="not found in external table"):
            dest._render_native_load_where(spec, ext_cols)

    def test_native_load_where_no_filters_returns_empty(self):
        dest = self._make_dest()
        spec = MagicMock()
        spec.filters = []
        assert dest._render_native_load_where(spec, [("a", "STRING")]) == ""
