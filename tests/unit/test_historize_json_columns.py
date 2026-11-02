"""Unit tests for ``historize.json_columns``.

Covers config parsing and validation, the config fingerprint, the derived
source relation, and the destination methods it is built from. End-to-end
behaviour on DuckDB lives in ``tests/integration/test_historize_json_columns.py``.
"""

from unittest.mock import MagicMock

import pytest

from dlt_saga.historize.config import HistorizeConfig, JsonColumn
from dlt_saga.historize.sql import HistorizeSqlBuilder, build_source_relation
from dlt_saga.historize.state import HistorizeStateManager


def _config(**overrides):
    return HistorizeConfig(**{"primary_key": ["id"], **overrides})


# ---------------------------------------------------------------------------
# Parsing and validation
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestParse:
    def test_unset_is_empty(self):
        assert _config().json_column_specs == ()

    def test_data_type_defaults_to_text(self):
        specs = _config(
            json_columns={"user_id": {"column": "payload", "path": "user_id"}}
        ).json_column_specs
        assert specs == (
            JsonColumn(name="user_id", column="payload", path=("user_id",)),
        )
        assert specs[0].data_type == "text"

    def test_dotted_and_list_paths_parse_alike(self):
        dotted = _config(
            json_columns={"tenant": {"column": "payload", "path": "meta.tenant"}}
        )
        listed = _config(
            json_columns={"tenant": {"column": "payload", "path": ["meta", "tenant"]}}
        )
        assert dotted.json_column_specs == listed.json_column_specs
        assert dotted.json_column_specs[0].path == ("meta", "tenant")

    def test_config_order_kept(self):
        specs = _config(
            json_columns={
                "b": {"column": "payload", "path": "b"},
                "a": {"column": "payload", "path": "a", "data_type": "bigint"},
            }
        ).json_column_specs
        assert [s.name for s in specs] == ["b", "a"]

    def test_from_dict(self):
        config = HistorizeConfig.from_dict(
            {
                "json_columns": {
                    "t": {"column": "p", "path": "t", "data_type": "timestamp"}
                }
            },
            top_level_primary_key=["id"],
        )
        assert config.json_column_specs[0].data_type == "timestamp"


@pytest.mark.unit
class TestValidation:
    @pytest.mark.parametrize(
        "json_columns, match",
        [
            (["payload"], "must be a mapping"),
            ({"x": "payload"}, "must be a mapping with 'column'"),
            ({"x": {}}, "'column' is required"),
            ({"x": {"column": "p"}}, "'path' is required"),
            ({"x": {"column": "p", "path": None}}, "'path' is required"),
            ({"x": {"column": "pay load", "path": "x"}}, "'column' is required"),
            ({"x": {"column": "p", "type": "text"}}, r"unknown key\(s\) \['type'\]"),
            (
                {"x": {"column": "p", "path": "x", "data_type": "json"}},
                "data_type must be one of",
            ),
            (
                {"x": {"column": "p", "path": "x", "data_type": "decimal"}},
                "data_type must be one of",
            ),
            ({"x": {"column": "p", "path": ""}}, "'path' is empty"),
            ({"x": {"column": "p", "path": []}}, "non-empty list"),
            ({"bad-name": {"column": "p"}}, "valid SQL identifier"),
            (
                {"x": {"column": "p", "path": "x"}, "X": {"column": "p", "path": "X"}},
                "case-insensitive",
            ),
            (
                {"_dlt_ingested_at": {"column": "p", "path": "_dlt_ingested_at"}},
                "reserved for historize",
            ),
            (
                {"_saga_snapshot": {"column": "p", "path": "_saga_snapshot"}},
                "reserved for historize",
            ),
            (
                {"_DLT_VALID_FROM": {"column": "p", "path": "_DLT_VALID_FROM"}},
                "reserved for historize",
            ),
        ],
    )
    def test_rejected(self, json_columns, match):
        with pytest.raises(ValueError, match=match):
            _config(json_columns=json_columns)

    def test_configured_scd2_column_names_reserved(self):
        with pytest.raises(ValueError, match="reserved for historize"):
            _config(
                valid_from_column="valid_from",
                json_columns={"valid_from": {"column": "p", "path": "valid_from"}},
            )

    def test_name_may_be_the_snapshot_column(self):
        """A derived column can serve as the snapshot column."""
        config = _config(
            snapshot_column="event_time",
            json_columns={
                "event_time": {
                    "column": "p",
                    "path": "event_time",
                    "data_type": "timestamp",
                }
            },
        )
        assert config.json_column_specs[0].name == "event_time"


# ---------------------------------------------------------------------------
# Fingerprint
# ---------------------------------------------------------------------------


def _decoded(config):
    return HistorizeStateManager.decode_fingerprint(
        HistorizeStateManager.compute_fingerprint(config)
    )


def _state_for(config):
    state = HistorizeStateManager.PipelineState(
        has_successful_run=True,
        config_fingerprint=HistorizeStateManager.compute_fingerprint(config),
    )
    return state


@pytest.mark.unit
class TestFingerprint:
    def test_spelled_out_default_is_not_a_change(self):
        short = _config(
            json_columns={"user_id": {"column": "payload", "path": "user_id"}}
        )
        explicit = _config(
            json_columns={
                "user_id": {"column": "payload", "path": "user_id", "data_type": "text"}
            }
        )
        assert _decoded(short) == _decoded(explicit)

    def test_entry_order_is_not_a_change(self):
        first = _config(
            json_columns={
                "a": {"column": "p", "path": "a"},
                "b": {"column": "p", "path": "b"},
            }
        )
        second = _config(
            json_columns={
                "b": {"column": "p", "path": "b"},
                "a": {"column": "p", "path": "a"},
            }
        )
        assert _decoded(first) == _decoded(second)

    @pytest.mark.parametrize(
        "changed",
        [
            {"uid": {"column": "payload", "path": "user_id"}},
            {
                "user_id": {"column": "payload", "path": "user_id"},
                "tenant": {"column": "payload", "path": "tenant"},
            },
            {},
        ],
    )
    def test_adding_or_removing_an_entry_is_schema_evolution(self, changed):
        """An entry on one side only is reconciled on the target, not rebuilt."""
        manager = HistorizeStateManager.__new__(HistorizeStateManager)
        before = _config(
            json_columns={"user_id": {"column": "payload", "path": "user_id"}}
        )
        assert not manager.config_changed(
            _state_for(before), _config(json_columns=changed)
        )

    @pytest.mark.parametrize(
        "redefined",
        [
            {"column": "payload", "path": "user.id"},
            {"column": "envelope", "path": "user_id"},
            {"column": "payload", "path": "user_id", "data_type": "bigint"},
        ],
    )
    def test_redefining_an_entry_blocks(self, redefined):
        """An entry kept under its name with another definition needs a rebuild."""
        manager = HistorizeStateManager.__new__(HistorizeStateManager)
        before = _config(
            json_columns={"user_id": {"column": "payload", "path": "user_id"}}
        )
        changes = manager.fingerprint_changes(
            _state_for(before), _config(json_columns={"user_id": redefined})
        )
        assert changes.blocking == ["json_columns"]
        # Described per entry, naming only the redefined one.
        (line,) = changes.describe(changes.blocking)
        assert line.startswith(
            "  json_columns.user_id: ['payload', ['user_id'], 'text'] → "
        )
        assert redefined["column"] in line

    def test_adding_to_a_table_historized_before_the_key_existed(self):
        """A stored fingerprint without the key reads as 'no json_columns'.

        Such a table has no derived column that could be redefined, so adding
        entries to it is schema evolution like on any other table.
        """
        import base64
        import json

        legacy = _decoded(_config())
        del legacy["json_columns"]
        state = HistorizeStateManager.PipelineState(
            has_successful_run=True,
            config_fingerprint=base64.b64encode(json.dumps(legacy).encode()).decode(),
        )
        manager = HistorizeStateManager.__new__(HistorizeStateManager)
        assert not manager.config_changed(state, _config())
        assert not manager.config_changed(
            state,
            _config(json_columns={"tenant": {"column": "payload", "path": "tenant"}}),
        )


# ---------------------------------------------------------------------------
# Source relation and SQL
# ---------------------------------------------------------------------------


def _stub_destination():
    dest = MagicMock()
    dest.quote_identifier.side_effect = lambda s: f"`{s}`"
    dest.escape_string_literal.side_effect = lambda s: s
    dest.is_pseudo_column.return_value = False
    dest.json_extract_scalar.side_effect = lambda col, path: (
        f"EXTRACT({col}, {'.'.join(path)})"
    )
    dest.safe_cast.side_effect = lambda expr, t: f"SAFE({expr} AS {t})"
    dest.dlt_type_to_native.side_effect = lambda t: t.upper()
    dest.type_name.side_effect = lambda t: t.upper()
    dest.build_historize_create_table_sql.side_effect = lambda **kw: kw
    return dest


def _builder(config, dest=None):
    dest = dest or _stub_destination()
    return HistorizeSqlBuilder(
        config=config,
        destination=dest,
        source_table_id="proj.ds.src",
        target_table_id="proj.ds.tgt",
        primary_key=["id"],
        source_relation=build_source_relation(
            dest, "proj.ds.src", config.json_column_specs
        ),
    )


_SPECS = {
    "event_time": {"column": "payload", "path": "eventTime", "data_type": "timestamp"},
    "user_id": {"column": "payload", "path": "user_id"},
}


@pytest.mark.unit
class TestSourceRelation:
    def test_without_json_columns_reads_the_table(self):
        assert build_source_relation(_stub_destination(), "proj.ds.src", ()) == (
            "proj.ds.src"
        )

    def test_without_json_columns_sql_is_unchanged(self):
        dest = _stub_destination()
        plain = HistorizeSqlBuilder(
            config=_config(),
            destination=dest,
            source_table_id="proj.ds.src",
            target_table_id="proj.ds.tgt",
            primary_key=["id"],
        )
        relation = _builder(_config(json_columns={}), dest)
        assert relation.build_full_reprocess_sql(["v"]) == (
            plain.build_full_reprocess_sql(["v"])
        )

    def test_relation_adds_converted_columns(self):
        relation = build_source_relation(
            _stub_destination(),
            "proj.ds.src",
            _config(json_columns=_SPECS).json_column_specs,
        )
        assert relation == (
            "(\n  SELECT *,\n"
            "    SAFE(EXTRACT(`payload`, eventTime) AS timestamp) AS `event_time`,\n"
            "    SAFE(EXTRACT(`payload`, user_id) AS text) AS `user_id`\n"
            "  FROM proj.ds.src\n)"
        )

    def test_every_source_read_uses_the_relation(self):
        builder = _builder(_config(json_columns=_SPECS))
        statements = [
            builder.build_full_reprocess_sql(["payload", "event_time"]),
            builder.build_incremental_sql(
                ["payload", "event_time"], ["2026-01-01"], "2025-12-31"
            ),
            builder.build_null_pk_probe_sql(),
            *builder.build_rollback_sql("2026-01-01", "r1", "ds"),
        ]
        for sql in statements:
            assert "FROM proj.ds.src\n)" in sql or "proj.ds.src" not in sql
        # The bare table is never read directly.
        joined = "\n".join(statements)
        assert joined.count("FROM proj.ds.src") == joined.count("FROM proj.ds.src\n)")

    def test_create_table_hints_carry_derived_columns(self):
        hints = _builder(_config(json_columns=_SPECS)).build_create_target_table_sql(
            ["payload", "event_time", "user_id"]
        )["hints"]
        assert hints.derived_columns == [
            ("event_time", "TIMESTAMP"),
            ("user_id", "TEXT"),
        ]

    def test_failed_cast_checks_skip_text(self):
        checks = _builder(_config(json_columns=_SPECS)).failed_cast_checks()
        assert checks == [
            (
                "event_time",
                "(EXTRACT(`payload`, eventTime) IS NOT NULL AND `event_time` IS NULL)",
            )
        ]

    def test_failed_cast_probe(self):
        builder = _builder(_config(json_columns=_SPECS))
        sql = builder.build_failed_cast_probe_sql()
        assert "MAX(CASE WHEN (EXTRACT(`payload`, eventTime) IS NOT NULL" in sql
        assert "AS _cast_fail_0" in sql
        assert "FROM (\n  SELECT *," in sql

    def test_failed_cast_probe_bounded_by_snapshot_from(self):
        builder = _builder(_config(json_columns=_SPECS))
        sql = builder.build_failed_cast_probe_sql(snapshot_from="2026-01-02 00:00:00")
        assert "WHERE `_dlt_ingested_at` >= TIMESTAMP '2026-01-02 00:00:00'" in sql

    def test_no_failed_cast_probe_for_text_only(self):
        builder = _builder(
            _config(json_columns={"user_id": {"column": "payload", "path": "user_id"}})
        )
        assert builder.build_failed_cast_probe_sql() is None


# ---------------------------------------------------------------------------
# Destination dialects
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestDialects:
    def test_bigquery(self):
        pytest.importorskip("google.cloud.bigquery")
        from dlt_saga.destinations.bigquery.destination import BigQueryDestination

        dest = BigQueryDestination.__new__(BigQueryDestination)
        dest.config = MagicMock()
        assert dest.json_extract_scalar("`p`", ("a", "b")) == "JSON_VALUE(`p`, '$.a.b')"
        assert dest.safe_cast("x", "timestamp") == "SAFE_CAST(x AS TIMESTAMP)"
        assert dest.safe_cast("x", "text") == "SAFE_CAST(x AS STRING)"

    def test_databricks(self):
        pytest.importorskip("databricks.sql")
        from dlt_saga.destinations.databricks.destination import DatabricksDestination

        dest = DatabricksDestination.__new__(DatabricksDestination)
        dest.config = MagicMock()
        assert dest.json_extract_scalar("`p`", ("a", "b")) == "`p`:a.b"
        assert dest.safe_cast("x", "text") == "TRY_CAST(x AS STRING)"
        assert dest.safe_cast("x", "bool") == "TRY_CAST(x AS BOOLEAN)"

    def test_duckdb(self):
        from dlt_saga.destinations.duckdb.destination import DuckDBDestination

        dest = DuckDBDestination.__new__(DuckDBDestination)
        assert dest.safe_cast("x", "timestamp") == "TRY_CAST(x AS TIMESTAMPTZ)"
        assert "json_valid" in dest.json_extract_scalar('"p"', ("a",))


# ---------------------------------------------------------------------------
# Runner checks that need the destination but not the source
# ---------------------------------------------------------------------------


def _bare_runner(config, pseudo):
    from dlt_saga.historize.runner import HistorizeRunner

    runner = object.__new__(HistorizeRunner)
    runner.config = config
    runner.pipeline_name = "test__events"
    runner.destination = MagicMock()
    runner.destination.is_pseudo_column.side_effect = lambda name: name in pseudo
    return runner


@pytest.mark.unit
class TestPseudoColumns:
    def test_pseudo_snapshot_column_rejected(self):
        runner = _bare_runner(
            _config(
                snapshot_column="_PARTITIONTIME",
                json_columns={"user_id": {"column": "payload", "path": "user_id"}},
            ),
            pseudo={"_PARTITIONTIME"},
        )
        with pytest.raises(ValueError, match="pseudo-column snapshot_column"):
            runner._validate_json_columns_on_destination()

    def test_pseudo_arrival_column_rejected(self):
        runner = _bare_runner(
            _config(
                snapshot_column="ingested_at",
                arrival_column="_PARTITIONTIME",
                json_columns={"user_id": {"column": "payload", "path": "user_id"}},
            ),
            pseudo={"_PARTITIONTIME"},
        )
        with pytest.raises(ValueError, match="pseudo-column arrival_column"):
            runner._validate_json_columns_on_destination()

    def test_reserved_output_name_rejected(self):
        runner = _bare_runner(
            _config(
                json_columns={
                    "_partition_x": {"column": "payload", "path": "_partition_x"}
                }
            ),
            pseudo={"_partition_x"},
        )
        with pytest.raises(ValueError, match="reserves this name"):
            runner._validate_json_columns_on_destination()

    def test_pseudo_snapshot_without_json_columns_unaffected(self):
        runner = _bare_runner(
            _config(snapshot_column="_PARTITIONTIME"), pseudo={"_PARTITIONTIME"}
        )
        runner._validate_json_columns_on_destination()
