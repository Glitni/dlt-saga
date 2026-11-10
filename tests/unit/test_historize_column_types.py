"""Nested column types: parsing, reconciling, spelling and reshaping.

A struct column that gained or lost a field is the nested form of a table
that gained or lost a column. These pin the pieces that make it reconcilable
in place: a catalog spelling parses into a type tree on every destination's
dialect, two trees reconcile into "added / removed / conflict", and each
destination can spell a tree back and reshape a value from one tree to
another with the fields in the target's order.
"""

import pytest

from dlt_saga.historize.column_types import (
    TypeSpellingError,
    array,
    leaf_paths,
    parse_type,
    reconcile_types,
    scalar,
    struct,
    types_equal,
)

pytestmark = pytest.mark.unit


def _norm(name: str) -> str:
    return " ".join(name.upper().split())


class TestParse:
    def test_bigquery_struct(self):
        assert parse_type("STRUCT<a STRING, b INT64>") == struct(
            ("a", scalar("STRING")), ("b", scalar("INT64"))
        )

    def test_bigquery_array_of_struct_with_parameters(self):
        parsed = parse_type("ARRAY<STRUCT<customerAccountId STRING, n NUMERIC(10, 2)>>")
        assert parsed == array(
            struct(
                ("customerAccountId", scalar("STRING")), ("n", scalar("NUMERIC(10, 2)"))
            )
        )

    def test_duckdb_struct_list_and_multi_word_scalar(self):
        parsed = parse_type('STRUCT(a VARCHAR, "b c" TIMESTAMP WITH TIME ZONE)[]')
        assert parsed == array(
            struct(
                ("a", scalar("VARCHAR")), ("b c", scalar("TIMESTAMP WITH TIME ZONE"))
            )
        )

    def test_databricks_colon_form(self):
        assert parse_type("struct<a:string,b:array<int>>") == struct(
            ("a", scalar("string")), ("b", array(scalar("int")))
        )

    def test_backtick_quoted_field_name(self):
        assert parse_type("STRUCT<`end` STRING>") == struct(("end", scalar("STRING")))

    @pytest.mark.parametrize(
        "spelling, expected",
        [
            ("INT64", "INT64"),
            ("DECIMAL(12,2)", "DECIMAL(12, 2)"),
            ("MAP(VARCHAR, INTEGER)", "MAP(VARCHAR, INTEGER)"),
            ("DOUBLE PRECISION", "DOUBLE PRECISION"),
        ],
    )
    def test_scalars_keep_their_spelling_with_canonical_parameters(
        self, spelling, expected
    ):
        parsed = parse_type(spelling)
        assert parsed == scalar(expected)
        assert not parsed.is_nested

    def test_nested_in_nested(self):
        parsed = parse_type("STRUCT<a STRUCT<x BOOL>, b ARRAY<STRUCT<y STRING>>>")
        assert parsed.field("a") == struct(("x", scalar("BOOL")))
        assert parsed.field("B") == array(struct(("y", scalar("STRING"))))
        assert parsed.is_nested

    @pytest.mark.parametrize("bad", ["", "STRUCT<a>", "STRUCT<a STRING", "STRUCT<, >"])
    def test_malformed_spelling_raises(self, bad):
        with pytest.raises(TypeSpellingError):
            parse_type(bad)


class TestReconcile:
    def test_added_fields_are_appended_in_source_order(self):
        target = parse_type("STRUCT<a STRING, b INT64>")
        source = parse_type("STRUCT<c BOOL, b INT64, a STRING, d STRING>")

        outcome = reconcile_types(target, source, _norm)

        assert outcome.compatible
        assert outcome.added == ["c", "d"]
        assert outcome.removed == []
        assert [name for name, _ in outcome.merged.fields] == ["a", "b", "c", "d"]

    def test_removed_fields_stay_in_the_merged_type(self):
        target = parse_type("STRUCT<a STRING, b INT64>")
        source = parse_type("STRUCT<a STRING>")

        outcome = reconcile_types(target, source, _norm)

        assert outcome.compatible
        assert outcome.removed == ["b"]
        assert outcome.merged == target

    def test_reorder_alone_is_unchanged(self):
        target = parse_type("STRUCT<a STRING, b INT64>")
        source = parse_type("STRUCT<b INT64, a STRING>")

        outcome = reconcile_types(target, source, _norm)

        assert outcome.unchanged
        assert outcome.merged == target

    def test_inside_an_array(self):
        target = parse_type("ARRAY<STRUCT<a STRING>>")
        source = parse_type("ARRAY<STRUCT<a STRING, b INT64>>")

        outcome = reconcile_types(target, source, _norm)

        assert outcome.added == ["b"]
        assert outcome.merged == parse_type("ARRAY<STRUCT<a STRING, b INT64>>")

    def test_nested_paths_are_dotted(self):
        target = parse_type("STRUCT<a STRUCT<x BOOL>>")
        source = parse_type("STRUCT<a STRUCT<x BOOL, y STRING>>")

        assert reconcile_types(target, source, _norm).added == ["a.y"]

    def test_retyped_nested_scalar_is_a_conflict_with_its_path(self):
        target = parse_type("STRUCT<a STRUCT<x INT64>>")
        source = parse_type("STRUCT<a STRUCT<x STRING>>")

        outcome = reconcile_types(target, source, _norm)

        assert not outcome.compatible
        assert (
            outcome.conflict
            == "a.x: INT64 in the historized table, STRING in the source"
        )

    def test_struct_where_a_scalar_was_is_a_conflict(self):
        target = parse_type("STRUCT<a STRING>")
        source = parse_type("STRUCT<a STRUCT<x STRING>>")

        assert "a: STRING in the historized table, a struct" in (
            reconcile_types(target, source, _norm).conflict
        )

    def test_array_where_a_struct_was_is_a_conflict(self):
        target = parse_type("STRUCT<a STRING>")
        source = parse_type("ARRAY<STRUCT<a STRING>>")

        assert "a struct in the historized table, an array" in (
            reconcile_types(target, source, _norm).conflict
        )

    def test_field_names_match_case_insensitively(self):
        target = parse_type("STRUCT<CustomerId STRING>")
        source = parse_type("STRUCT<customerid STRING>")

        assert reconcile_types(target, source, _norm).unchanged

    def test_scalar_aliases_go_through_normalize(self):
        target = parse_type("STRUCT(a TIMESTAMP WITH TIME ZONE)")
        source = parse_type("STRUCT(a TIMESTAMPTZ)")

        def alias(n):
            return {"TIMESTAMP WITH TIME ZONE": "TIMESTAMPTZ"}.get(_norm(n), _norm(n))

        assert reconcile_types(target, source, alias).unchanged
        assert not reconcile_types(target, source, _norm).compatible


class TestLeafPaths:
    def test_every_named_field_under_the_column(self):
        parsed = parse_type("ARRAY<STRUCT<a STRING, b STRUCT<x BOOL>>>")
        assert leaf_paths(parsed, "col") == ["col.a", "col.b", "col.b.x"]

    def test_equality_is_structural(self):
        a = parse_type("STRUCT<a STRING, b INT64>")
        assert types_equal(a, parse_type("STRUCT<A STRING, B INT64>"), _norm)
        assert not types_equal(a, parse_type("STRUCT<b INT64, a STRING>"), _norm)


class TestBigQueryDialect:
    def _dest(self):
        from dlt_saga.destinations.bigquery.destination import BigQueryDestination

        return object.__new__(BigQueryDestination)

    def test_spelling(self):
        dest = self._dest()
        parsed = parse_type("ARRAY<STRUCT<a STRING, `end` NUMERIC(10, 2)>>")
        assert dest.spell_column_type(parsed) == (
            "ARRAY<STRUCT<`a` STRING, `end` NUMERIC(10, 2)>>"
        )

    def test_identical_types_read_the_column_as_is(self):
        dest = self._dest()
        t = parse_type("STRUCT<a STRING>")
        assert dest.reshape_expression("`m`", t, t) == "`m`"

    def test_struct_is_rebuilt_in_target_order_with_null_for_missing(self):
        dest = self._dest()
        target = parse_type("STRUCT<a STRING, b INT64, c BOOL>")
        source = parse_type("STRUCT<c BOOL, a STRING>")

        assert dest.reshape_expression("`m`", source, target) == (
            "STRUCT(`m`.`a` AS `a`, CAST(NULL AS INT64) AS `b`, `m`.`c` AS `c`)"
        )

    def test_array_of_struct_keeps_element_order(self):
        dest = self._dest()
        target = parse_type("ARRAY<STRUCT<a STRING, b INT64>>")
        source = parse_type("ARRAY<STRUCT<a STRING>>")

        assert dest.reshape_expression("`l`", source, target) == (
            "IF(`l` IS NULL, NULL, ARRAY(SELECT STRUCT(_e0.`a` AS `a`, CAST(NULL AS INT64) "
            "AS `b`) FROM UNNEST(`l`) AS _e0 WITH OFFSET AS _e0_o ORDER BY _e0_o))"
        )

    def test_missing_nested_struct_is_a_typed_null(self):
        dest = self._dest()
        target = parse_type("STRUCT<a STRING, d STRUCT<x BOOL>>")
        source = parse_type("STRUCT<a STRING>")

        assert dest.reshape_expression("`m`", source, target) == (
            "STRUCT(`m`.`a` AS `a`, CAST(NULL AS STRUCT<`x` BOOL>) AS `d`)"
        )

    def test_schema_field_widening_keeps_existing_fields_and_appends_new(self):
        from google.cloud import bigquery

        from dlt_saga.destinations.bigquery.destination import BigQueryDestination

        existing = bigquery.SchemaField(
            "accounts",
            "RECORD",
            mode="REPEATED",
            description="kept",
            fields=[bigquery.SchemaField("id", "STRING", description="also kept")],
        )
        widened = parse_type(
            "ARRAY<STRUCT<id STRING, amount NUMERIC(10, 2), tags ARRAY<STRING>, "
            "owner STRUCT<name STRING(20)>>>"
        )

        result = BigQueryDestination._widen_schema_field(existing, widened)

        assert result.mode == "REPEATED" and result.description == "kept"
        names = [f.name for f in result.fields]
        assert names == ["id", "amount", "tags", "owner"]
        assert result.fields[0].description == "also kept"
        amount = result.fields[1]
        assert (amount.field_type, amount.precision, amount.scale) == ("NUMERIC", 10, 2)
        assert result.fields[2].mode == "REPEATED"
        owner = result.fields[3]
        assert owner.field_type == "RECORD"
        assert owner.fields[0].max_length == 20

    def test_schema_field_widening_maps_standard_sql_names(self):
        from google.cloud import bigquery

        from dlt_saga.destinations.bigquery.destination import BigQueryDestination

        existing = bigquery.SchemaField("m", "RECORD", fields=[])
        widened = parse_type("STRUCT<i INT64, f FLOAT64, b BOOL>")

        result = BigQueryDestination._widen_schema_field(existing, widened)

        assert [f.field_type for f in result.fields] == ["INTEGER", "FLOAT", "BOOLEAN"]

    def test_widen_column_type_patches_the_table_schema(self):
        from unittest.mock import MagicMock

        from google.cloud import bigquery

        dest = self._dest()
        client = MagicMock()
        table = MagicMock()
        table.schema = [
            bigquery.SchemaField("id", "INTEGER"),
            bigquery.SchemaField(
                "m", "RECORD", fields=[bigquery.SchemaField("a", "STRING")]
            ),
        ]
        client.get_table.return_value = table
        dest._description_client_and_ref = lambda d, t: (client, f"p.{d}.{t}")

        dest.widen_column_type(
            "ds",
            "tbl",
            "m",
            parse_type("STRUCT<a STRING>"),
            parse_type("STRUCT<a STRING, b INT64>"),
        )

        client.update_table.assert_called_once_with(table, ["schema"])
        assert [f.name for f in table.schema[1].fields] == ["a", "b"]


class TestDuckDBDialect:
    def _dest(self):
        from dlt_saga.destinations.duckdb.destination import DuckDBDestination

        return object.__new__(DuckDBDestination)

    def test_spelling(self):
        dest = self._dest()
        parsed = parse_type('STRUCT(a VARCHAR, "b c" TIMESTAMP WITH TIME ZONE)[]')
        assert dest.spell_column_type(parsed) == (
            'STRUCT(a VARCHAR, "b c" TIMESTAMP WITH TIME ZONE)[]'
        )

    def test_struct_is_rebuilt_in_target_order_with_null_for_missing(self):
        dest = self._dest()
        target = parse_type("STRUCT(a VARCHAR, b INTEGER)")
        source = parse_type("STRUCT(b INTEGER)")

        assert dest.reshape_expression('"m"', source, target) == (
            'struct_pack(a := CAST(NULL AS VARCHAR), b := struct_extract("m", '
            + "'b'))"
        )

    def test_array_of_struct_uses_list_transform(self):
        dest = self._dest()
        target = parse_type("STRUCT(a VARCHAR, b INTEGER)[]")
        source = parse_type("STRUCT(a VARCHAR)[]")

        body = "struct_pack(a := struct_extract(_e0, 'a'), b := CAST(NULL AS INTEGER))"
        assert dest.reshape_expression('"l"', source, target) == (
            f'list_transform("l", {dest.lambda_sql("_e0", body)})'
        )

    def test_lambda_syntax_follows_the_installed_version(self, monkeypatch):
        import duckdb

        from dlt_saga.destinations.duckdb.destination import DuckDBDestination

        monkeypatch.setattr(duckdb, "__version__", "1.2.2")
        assert DuckDBDestination.lambda_sql("x", "x + 1") == "x -> x + 1"
        monkeypatch.setattr(duckdb, "__version__", "1.3.0")
        assert DuckDBDestination.lambda_sql("x", "x + 1") == "lambda x: x + 1"

    def test_widen_column_type_alters_with_a_using_clause(self):
        from unittest.mock import MagicMock

        dest = self._dest()
        dest.execute_sql = MagicMock()
        dest.config = MagicMock()

        dest.widen_column_type(
            "ds",
            "tbl",
            "m",
            parse_type("STRUCT(a VARCHAR)"),
            parse_type("STRUCT(a VARCHAR, b INTEGER)"),
        )

        sql = dest.execute_sql.call_args[0][0]
        assert (
            'ALTER COLUMN "m" SET DATA TYPE STRUCT(a VARCHAR, b INTEGER) USING' in sql
        )
        assert (
            'struct_pack(a := struct_extract("m", '
            + "'a'), b := CAST(NULL AS INTEGER))"
            in sql
        )


class TestBaseDestination:
    def test_defaults_to_no_nested_evolution(self):
        from unittest.mock import MagicMock

        from dlt_saga.destinations.base import Destination

        bare = MagicMock()
        assert Destination.supports_nested_evolution(bare) is False
        with pytest.raises(NotImplementedError):
            Destination.reshape_expression(
                bare, "x", None, parse_type("STRUCT<a STRING>")
            )
