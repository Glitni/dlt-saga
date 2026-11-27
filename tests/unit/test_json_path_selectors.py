"""Unit tests for element selectors in JSON paths.

A selector is a path segment picking the first matching element of an array.
Covers the parser, the Python evaluator, the fingerprint form, and the SQL
each destination renders (run on DuckDB, compared as text for the others).
"""

from unittest.mock import MagicMock

import pytest

from dlt_saga.historize.config import HistorizeConfig
from dlt_saga.historize.state import HistorizeStateManager
from dlt_saga.utility.filters import (
    ElementSelector,
    build_row_predicate,
    format_json_path,
    parse_filters,
    parse_json_path,
)

ARTICLE = {"path": "schema", "op": "matches", "value": "^vendor/article/"}


# ---------------------------------------------------------------------------
# Parsing
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestParse:
    def test_selector_between_keys(self):
        path = parse_json_path(["data", ARTICLE, "data", "id"], "x")
        assert path == (
            "data",
            ElementSelector(path=("schema",), op="matches", value="^vendor/article/"),
            "data",
            "id",
        )

    def test_op_defaults_to_eq(self):
        (selector,) = parse_json_path([{"path": "key", "value": "title"}], "x")
        assert selector == ElementSelector(path=("key",), op="eq", value="title")

    def test_selector_path_is_dotted_or_list(self):
        (dotted,) = parse_json_path([{"path": "a.b", "value": 1}], "x")
        (listed,) = parse_json_path([{"path": ["a", "b"], "value": 1}], "x")
        assert dotted == listed

    def test_list_value_keeps_the_path_hashable(self):
        path = parse_json_path([{"path": "k", "op": "in", "value": ["a", "b"]}], "x")
        assert path[0].value == ("a", "b")
        hash(path)

    def test_plain_paths_parse_as_before(self):
        assert parse_json_path("a.b", "x") == ("a", "b")
        assert parse_json_path(["a", "b.c"], "x") == ("a", "b.c")

    @pytest.mark.parametrize(
        "selector, message",
        [
            ({"op": "eq", "value": 1}, "needs 'path'"),
            ({"path": "k"}, "requires 'value'"),
            ({"path": "k", "op": "like", "value": 1}, "unknown op"),
            ({"path": "k", "op": "is_null", "value": 1}, "does not take 'value'"),
            ({"path": "k", "value": 1, "column": "c"}, "unknown key"),
            ({"path": [{"path": "k", "value": 1}], "value": 1}, "non-empty strings"),
        ],
    )
    def test_malformed_selector(self, selector, message):
        with pytest.raises(ValueError, match=message):
            parse_json_path(["data", selector], "x")

    def test_error_names_the_segment(self):
        with pytest.raises(ValueError, match=r"x: path\[1\]"):
            parse_json_path(["data", 5], "x")

    def test_filter_ending_at_a_selector_only_tests_for_null(self):
        base = {"column": "contexts", "path": ["data", ARTICLE]}
        (spec,) = parse_filters([{**base, "op": "is_not_null"}])
        assert spec.path[-1].op == "matches"
        with pytest.raises(ValueError, match="ends at a selector"):
            parse_filters([{**base, "value": "x"}])

    def test_format(self):
        path = parse_json_path(["data", {"path": "k", "value": "t"}, "v"], "x")
        assert format_json_path(path) == "data[k eq 't'].v"
        assert format_json_path(("a", "b")) == "a.b"


# ---------------------------------------------------------------------------
# Fingerprint
# ---------------------------------------------------------------------------


def _fingerprint(path):
    config = HistorizeConfig(
        primary_key=["id"], json_columns={"c": {"column": "p", "path": path}}
    )
    return HistorizeStateManager.decode_fingerprint(
        HistorizeStateManager.compute_fingerprint(config)
    )["json_columns"]["c"]


@pytest.mark.unit
class TestFingerprint:
    def test_plain_path_is_stored_as_before(self):
        assert _fingerprint("meta.tenant") == ["p", ["meta", "tenant"], "text"]

    def test_selector_defaults_are_spelled_out(self):
        short = _fingerprint(["data", {"path": "key", "value": "t"}, "value"])
        explicit = _fingerprint(
            ["data", {"path": ["key"], "op": "eq", "value": "t"}, "value"]
        )
        assert short == explicit
        assert short[1][1] == {"path": ["key"], "op": "eq", "value": "t"}

    def test_another_selector_value_is_another_definition(self):
        assert _fingerprint([{"path": "key", "value": "a"}]) != _fingerprint(
            [{"path": "key", "value": "b"}]
        )


# ---------------------------------------------------------------------------
# Python evaluation
# ---------------------------------------------------------------------------


def _passes(row, **entry):
    return build_row_predicate(parse_filters([{"column": "c", **entry}]))(row)


@pytest.mark.unit
class TestRowPredicate:
    ROW = {
        "c": {
            "data": [
                {"schema": "vendor/page/1", "data": {"id": "p1"}},
                {"schema": "vendor/article/1", "data": {"id": "a1"}},
                {"schema": "vendor/article/2", "data": {"id": "a2"}},
            ]
        }
    }

    def test_first_match_in_array_order(self):
        path = ["data", ARTICLE, "data", "id"]
        assert _passes(self.ROW, path=path, value="a1")
        assert not _passes(self.ROW, path=path, value="a2")

    def test_json_string_column(self):
        import json

        row = {"c": json.dumps(self.ROW["c"])}
        assert _passes(row, path=["data", ARTICLE, "data", "id"], value="a1")

    def test_no_match_is_null(self):
        missing = {"path": "schema", "value": "none"}
        assert _passes(self.ROW, path=["data", missing, "data", "id"], op="is_null")
        assert _passes(self.ROW, path=["data", missing], op="is_null")
        assert _passes(self.ROW, path=["data", ARTICLE], op="is_not_null")

    def test_not_an_array_is_null(self):
        row = {"c": {"data": {"schema": "vendor/article/1"}}}
        assert _passes(row, path=["data", ARTICLE], op="is_null")

    def test_element_without_the_key_never_matches_a_comparison(self):
        row = {"c": {"data": [5, {"other": 1}, {"schema": "b", "v": 1}]}}
        not_a = {"path": "schema", "op": "ne", "value": "a"}
        assert _passes(row, path=["data", not_a, "v"], value=1)

    def test_is_null_selects_an_element_missing_the_key(self):
        row = {"c": {"data": [{"schema": "a"}, {"v": 2}]}}
        unset = {"path": "schema", "op": "is_null"}
        assert _passes(row, path=["data", unset, "v"], value=2)


# ---------------------------------------------------------------------------
# SQL
# ---------------------------------------------------------------------------


def _text(dest, column_type):
    return lambda value_sql, keys: dest.json_extract_text(value_sql, keys, column_type)


@pytest.mark.unit
class TestRendered:
    PATH = parse_json_path(["data", {"path": "key", "value": "t"}, "value"], "x")

    def test_destination_without_the_primitive_refuses(self):
        from tests.unit.test_filters import _BaseDestination

        dest = _BaseDestination()
        with pytest.raises(NotImplementedError, match="selecting an array element"):
            dest.json_path_expression('"c"', self.PATH, _text(dest, "JSON"))

    def test_path_of_keys_is_the_extract_alone(self):
        from tests.unit.test_filters import _BaseDestination

        dest = _BaseDestination()
        assert dest.json_path_expression(
            '"c"', ("a", "b"), dest.json_extract_scalar
        ) == dest.json_extract_scalar('"c"', ("a", "b"))

    def test_bigquery(self):
        pytest.importorskip("google.cloud.bigquery")
        from dlt_saga.destinations.bigquery.destination import BigQueryDestination

        dest = BigQueryDestination.__new__(BigQueryDestination)
        dest.config = MagicMock()
        assert dest.json_path_expression("`c`", self.PATH, _text(dest, "STRING")) == (
            "(SELECT COALESCE(JSON_VALUE(_saga_el0, '$.value'), "
            "NULLIF(JSON_QUERY(_saga_el0, '$.value'), 'null')) "
            "FROM UNNEST(JSON_QUERY_ARRAY(`c`, '$.data')) AS _saga_el0 "
            "WITH OFFSET AS _saga_el0_pos "
            "WHERE JSON_VALUE(_saga_el0, '$.key') = 't' "
            "ORDER BY _saga_el0_pos LIMIT 1)"
        )

    def test_bigquery_filter_on_an_element_existing(self):
        pytest.importorskip("google.cloud.bigquery")
        from dlt_saga.destinations.bigquery.destination import BigQueryDestination

        dest = BigQueryDestination.__new__(BigQueryDestination)
        dest.config = MagicMock()
        specs = parse_filters([{"column": "c", "path": [ARTICLE], "op": "is_not_null"}])
        assert dest.render_filter(specs) == (
            "(SELECT _saga_el0 FROM UNNEST(JSON_QUERY_ARRAY(`c`, '$')) AS _saga_el0 "
            "WITH OFFSET AS _saga_el0_pos "
            "WHERE REGEXP_CONTAINS(JSON_VALUE(_saga_el0, '$.schema'), "
            "'^vendor/article/') ORDER BY _saga_el0_pos LIMIT 1) IS NOT NULL"
        )

    def test_databricks(self):
        pytest.importorskip("databricks.sql")
        from dlt_saga.destinations.databricks.destination import DatabricksDestination

        dest = DatabricksDestination.__new__(DatabricksDestination)
        dest.config = MagicMock()
        assert dest.json_path_expression("`c`", self.PATH, _text(dest, "STRING")) == (
            "get(transform(filter("
            "from_json(TRY_CAST(`c`:data AS STRING), 'array<string>'), "
            "_saga_el0 -> _saga_el0:key = 't'), "
            "_saga_el0 -> _saga_el0:value), 0)"
        )
        # A path that ends at the selector reads the element itself.
        ends = parse_json_path([{"path": "key", "value": "t"}], "x")
        assert dest.json_path_expression("`c`", ends, _text(dest, "STRING")) == (
            "get(transform(filter("
            "from_json(TRY_CAST(`c` AS STRING), 'array<string>'), "
            "_saga_el0 -> _saga_el0:key = 't'), _saga_el0 -> _saga_el0), 0)"
        )


@pytest.mark.unit
class TestDuckDB:
    """Runs the rendered SQL, on a string column and a JSON-typed one."""

    ROWS = [
        (
            1,
            '{"data": [{"schema": "vendor/page/1", "data": {"id": "p1"}}, '
            '{"schema": "vendor/article/1", "data": {"id": "a1", "tags": '
            '[{"key": "x", "value": 1}, {"key": "y", "value": 2}]}}, '
            '{"schema": "vendor/article/2", "data": {"id": "a2"}}]}',
        ),
        (2, '{"data": [{"schema": "vendor/page/1", "data": {"id": "p2"}}]}'),
        (3, '{"data": {"schema": "vendor/article/1"}}'),
        (4, '{"data": [1, "text", null]}'),
        (5, "not json"),
        (6, None),
    ]

    @pytest.fixture(params=["VARCHAR", "JSON"])
    def dest(self, request):
        from dlt_saga.destinations.duckdb.config import DuckDBDestinationConfig
        from dlt_saga.destinations.duckdb.destination import DuckDBDestination

        destination = DuckDBDestination(
            DuckDBDestinationConfig(
                project_id="test",
                location="local",
                schema_name="main",
                database_path=":memory:",
            )
        )
        destination.column_type = request.param
        destination.connection.execute(
            f"CREATE TABLE t (id INTEGER, c {request.param})"
        )
        for row_id, text in self.ROWS:
            destination.connection.execute(
                f"INSERT INTO t VALUES (?, TRY_CAST(? AS {request.param}))",
                [row_id, text],
            )
        yield destination
        destination.close()

    def _values(self, dest, path, extract=None):
        extract = extract or _text(dest, dest.column_type)
        expression = dest.json_path_expression(
            '"c"', parse_json_path(path, "x"), extract
        )
        rows = dest.connection.execute(
            f"SELECT id, {expression} FROM t ORDER BY id"
        ).fetchall()
        return {row_id: value for row_id, value in rows if value is not None}

    def test_first_match_in_array_order(self, dest):
        assert self._values(dest, ["data", ARTICLE, "data", "id"]) == {1: "a1"}

    def test_selector_inside_a_selected_element(self, dest):
        path = ["data", ARTICLE, "data", "tags", {"path": "key", "value": "y"}, "value"]
        assert self._values(dest, path) == {1: "2"}

    def test_path_ending_at_the_selector_is_the_element(self, dest):
        import json

        page = {"path": "schema", "value": "vendor/page/1"}
        values = self._values(dest, ["data", page])
        assert {k: json.loads(v) for k, v in values.items()} == {
            1: {"schema": "vendor/page/1", "data": {"id": "p1"}},
            2: {"schema": "vendor/page/1", "data": {"id": "p2"}},
        }

    def test_document_extract(self, dest):
        import json

        values = self._values(
            dest,
            ["data", ARTICLE, "data"],
            lambda value_sql, keys: dest.json_extract_document(
                value_sql, keys, dest.column_type
            ),
        )
        assert json.loads(values[1])["id"] == "a1"
        assert set(values) == {1}

    def _ids(self, dest, filters):
        where = dest.render_filter(parse_filters(filters))
        rows = dest.connection.execute(
            f"SELECT id FROM t WHERE {where} ORDER BY id"
        ).fetchall()
        return [row[0] for row in rows]

    def test_filter_on_a_value_inside_the_element(self, dest):
        page = {"path": "schema", "value": "vendor/page/1"}
        assert self._ids(
            dest, [{"column": "c", "path": ["data", page, "data", "id"], "value": "p2"}]
        ) == [2]

    def test_filter_on_an_element_existing(self, dest):
        entry = {"column": "c", "path": ["data", ARTICLE]}
        assert self._ids(dest, [{**entry, "op": "is_not_null"}]) == [1]
        assert self._ids(dest, [{**entry, "op": "is_null"}]) == [2, 3, 4, 5, 6]
