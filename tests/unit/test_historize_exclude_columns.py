"""Unit tests for ``historize.exclude_columns``.

An excluded column is left out of the historized table and of the
change-detection hash. It can't name anything the target has to carry, it is
fingerprinted like the other detection keys (adding narrows, removing blocks),
and on an existing target it is handled like a source-dropped column — kept
with its history, read as NULL — without the drop warning.
"""

import logging
from unittest.mock import MagicMock

import pytest

from dlt_saga.historize.config import HistorizeConfig
from dlt_saga.historize.runner import HistorizeRunner
from dlt_saga.historize.sql import HistorizeSqlBuilder
from dlt_saga.historize.state import HistorizeStateManager

pytestmark = pytest.mark.unit


def _config(**kwargs):
    kwargs.setdefault("primary_key", ["id"])
    kwargs.setdefault("snapshot_column", "snap")
    return HistorizeConfig(**kwargs)


class TestConfigValidation:
    def test_single_string_is_coerced_to_a_list(self):
        assert _config(exclude_columns="payload").exclude_columns == ["payload"]

    def test_invalid_identifier_is_refused(self):
        with pytest.raises(ValueError, match="exclude_columns"):
            _config(exclude_columns=["a`b"])

    @pytest.mark.parametrize(
        "kwargs",
        [
            {"snapshot_column": "snap", "exclude_columns": ["snap"]},
            {"partition_column": "day", "exclude_columns": ["day"]},
            {"cluster_columns": ["region"], "exclude_columns": ["region"]},
            {"track_columns": ["a"], "exclude_columns": ["a"]},
            {"ignore_columns": ["a"], "exclude_columns": ["a"]},
            {"strategy": "scd1", "order_by": ["snap", "v"], "exclude_columns": ["v"]},
            {
                "json_columns": {"amount": {"column": "payload", "path": "amount"}},
                "exclude_columns": ["amount"],
            },
        ],
    )
    def test_columns_the_target_must_carry_are_refused(self, kwargs):
        with pytest.raises(ValueError, match="exclude_columns"):
            _config(**kwargs)

    def test_conflict_is_case_insensitive(self):
        with pytest.raises(ValueError, match="'Region' \\(cluster_columns\\)"):
            _config(cluster_columns=["Region"], exclude_columns=["region"])

    def test_json_source_column_may_be_excluded(self):
        config = _config(
            json_columns={"amount": {"column": "payload", "path": "amount"}},
            exclude_columns=["payload"],
        )
        assert config.exclude_columns == ["payload"]

    def test_allowed_under_scd1(self):
        config = _config(
            strategy="scd1", track_deletions=False, exclude_columns=["payload"]
        )
        assert config.exclude_columns == ["payload"]

    def test_primary_key_is_refused_in_validate(self):
        config = HistorizeConfig(
            primary_key=None, snapshot_column="snap", exclude_columns=["id"]
        )
        config.primary_key = ["id"]  # as inherited by from_dict
        with pytest.raises(ValueError, match="primary_key column"):
            config.validate()

    def test_from_dict_inherits_key_then_refuses_it(self):
        config = HistorizeConfig.from_dict(
            {"exclude_columns": ["Id"]}, top_level_primary_key=["id"]
        )
        with pytest.raises(ValueError, match="primary_key"):
            config.validate()


def _builder(**config_kwargs):
    dest = MagicMock()
    dest.quote_identifier.side_effect = lambda n: f'"{n}"'
    dest.is_pseudo_column.return_value = False
    dest.hash_expression.side_effect = lambda cols: f"HASH({', '.join(cols)})"
    dest.escape_string_literal.side_effect = lambda s: s
    dest.type_name.side_effect = lambda t: t.upper()
    config = _config(**config_kwargs)
    return HistorizeSqlBuilder(
        config=config,
        destination=dest,
        source_table_id="src",
        target_table_id="tgt",
        primary_key=["id"],
    )


SOURCE = ["id", "snap", "a", "payload", "_dlt_ingested_at"]


class TestColumnSelection:
    def test_excluded_column_is_not_a_value_column(self):
        builder = _builder(exclude_columns=["payload"])
        assert builder.select_value_columns(SOURCE) == ["a"]

    def test_unset_leaves_selection_unchanged(self):
        assert _builder().select_value_columns(SOURCE) == ["a", "payload"]

    def test_excluded_column_still_on_the_target_is_not_hashed(self):
        builder = _builder(exclude_columns=["payload"])
        # After reconcile the still-present column is appended to value_columns.
        assert builder._get_hash_columns(["a", "payload"]) == ["a"]

    def test_excluded_is_removed_even_with_track_columns(self):
        builder = _builder(track_columns=["a", "b"], exclude_columns=["payload"])
        assert builder._get_hash_columns(["a", "b", "payload"]) == ["a", "b"]

    @pytest.mark.parametrize("strategy", ["scd2", "scd1"])
    def test_full_sql_never_mentions_the_excluded_column(self, strategy):
        kwargs = {"strategy": strategy, "exclude_columns": ["payload"]}
        if strategy == "scd1":
            kwargs["track_deletions"] = False
        builder = _builder(**kwargs)
        value_columns = builder.select_value_columns(SOURCE)
        sql = builder.build_create_target_table_sql(value_columns)
        assert '"payload"' not in sql
        if strategy == "scd1":
            sql = builder.build_scd1_sql(
                value_columns=value_columns, snapshots=["2026-01-02 00:00:00"]
            )
        else:
            sql = builder.build_incremental_sql(
                value_columns=value_columns, new_snapshots=["2026-01-02 00:00:00"]
            )
        assert '"payload"' not in sql


def _manager():
    dest = MagicMock()
    dest.get_full_table_id.return_value = "cat.schema._saga_historize_log"
    return HistorizeStateManager(dest, "db", "schema")


def _state(manager, **previous):
    return HistorizeStateManager.PipelineState(
        config_fingerprint=manager.compute_fingerprint(_config(**previous)),
        has_successful_run=True,
    )


class TestFingerprint:
    @pytest.mark.parametrize(
        "previous, current",
        [
            ({}, {"exclude_columns": ["payload"]}),
            ({"exclude_columns": ["a"]}, {"exclude_columns": ["a", "b"]}),
            # Moving a column between the two lists leaves the hash unchanged.
            ({"ignore_columns": ["a"]}, {"exclude_columns": ["a"]}),
            ({"exclude_columns": ["a"]}, {"ignore_columns": ["a"]}),
            (
                {"track_columns": ["a", "b"]},
                {"track_columns": ["a"], "exclude_columns": ["b"]},
            ),
        ],
    )
    def test_adding_narrows(self, previous, current):
        manager = _manager()
        changes = manager.fingerprint_changes(
            _state(manager, **previous), _config(**current)
        )
        assert changes.blocking == []
        assert changes.narrowed

    @pytest.mark.parametrize(
        "previous, current",
        [
            ({"exclude_columns": ["payload"]}, {}),
            ({"exclude_columns": ["a", "b"]}, {"exclude_columns": ["a"]}),
            # A previously excluded column enters the hash through track_columns.
            ({"exclude_columns": ["b"]}, {"track_columns": ["a", "b"]}),
        ],
    )
    def test_removing_blocks(self, previous, current):
        manager = _manager()
        changes = manager.fingerprint_changes(
            _state(manager, **previous), _config(**current)
        )
        assert changes.blocking
        assert changes.narrowed == []

    def test_older_fingerprint_means_nothing_excluded(self):
        manager = _manager()
        import base64
        import json

        decoded = json.loads(
            base64.b64decode(manager.compute_fingerprint(_config())).decode()
        )
        del decoded["exclude_columns"]
        old = base64.b64encode(json.dumps(decoded).encode()).decode()
        state = HistorizeStateManager.PipelineState(
            config_fingerprint=old, has_successful_run=True
        )
        assert manager.fingerprint_changes(state, _config()).narrowed == []
        changes = manager.fingerprint_changes(
            state, _config(exclude_columns=["payload"])
        )
        assert changes.narrowed == ["exclude_columns"]


def _runner(source, target, previous=None, **config_kwargs) -> HistorizeRunner:
    dest = MagicMock()
    dest.list_table_columns.return_value = list(target)
    dest.quote_identifier.side_effect = lambda n: f'"{n}"'
    dest.is_pseudo_column.return_value = False
    dest.normalize_type_name.side_effect = lambda t: " ".join(t.upper().split())
    dest.dlt_type_to_native.side_effect = lambda t: t.upper()

    runner = object.__new__(HistorizeRunner)
    runner.logger = logging.getLogger("dlt_saga.historize.runner")
    runner.pipeline_name = "grp__tbl"
    runner.destination = dest
    runner.database = "proj"
    runner.schema = "ds"
    runner.target_schema = "ds"
    runner.target_table_name = "tbl_historized"
    runner.source_table_id = "proj.ds.src"
    runner.target_table_id = "proj.ds.tbl_historized"
    runner.full_refresh = False
    runner.partial_refresh = False
    runner.historize_from = None
    runner.config = _config(**config_kwargs)
    runner._source_columns_cache = [name for name, _ in source]
    runner._source_column_types = {name.lower(): t for name, t in source}
    runner.state_manager = MagicMock()
    runner.state_manager.get_last_source_columns.return_value = previous
    runner.sql_builder = HistorizeSqlBuilder(
        config=runner.config,
        destination=dest,
        source_table_id=runner.source_table_id,
        target_table_id=runner.target_table_id,
        primary_key=["id"],
    )
    return runner


SYSTEM = [
    ("_dlt_valid_from", "TIMESTAMP"),
    ("_dlt_valid_to", "TIMESTAMP"),
    ("_dlt_is_deleted", "BOOLEAN"),
]
SRC = [
    ("id", "INTEGER"),
    ("snap", "TIMESTAMP"),
    ("a", "VARCHAR"),
    ("payload", "VARCHAR"),
]


class TestReconcile:
    def test_excluded_column_on_target_is_read_as_null_without_warning(self, caplog):
        runner = _runner(
            source=SRC,
            target=[
                ("id", "INTEGER"),
                ("a", "VARCHAR"),
                ("payload", "VARCHAR"),
                *SYSTEM,
            ],
            previous=["id", "snap", "a", "payload"],
            exclude_columns=["payload"],
        )
        value_columns = runner.sql_builder.select_value_columns(
            runner._source_columns()
        )
        with caplog.at_level(logging.DEBUG, logger="dlt_saga.historize.runner"):
            carried = runner._reconcile_target_columns(value_columns)

        assert carried == ["a", "payload"]
        assert runner.sql_builder._absent_source_columns == {"payload": "VARCHAR"}
        assert not [r for r in caplog.records if r.levelno == logging.WARNING]
        assert any(
            r.levelno == logging.DEBUG and "exclude_columns" in r.getMessage()
            for r in caplog.records
        )
        # Not hashed, so the exclusion itself opens no versions.
        assert runner.sql_builder._get_hash_columns(carried) == ["a"]

    def test_source_drop_still_warns_alongside_an_exclusion(self, caplog):
        runner = _runner(
            source=SRC,
            target=[
                ("id", "INTEGER"),
                ("a", "VARCHAR"),
                ("payload", "VARCHAR"),
                ("gone", "DOUBLE"),
                *SYSTEM,
            ],
            previous=["id", "snap", "a", "payload", "gone"],
            exclude_columns=["payload"],
        )
        value_columns = runner.sql_builder.select_value_columns(
            runner._source_columns()
        )
        with caplog.at_level(logging.WARNING):
            runner._reconcile_target_columns(value_columns)
        warnings = [
            r.getMessage() for r in caplog.records if r.levelno == logging.WARNING
        ]
        assert len(warnings) == 1
        assert "gone" in warnings[0] and "payload" not in warnings[0]

    def test_excluded_column_absent_from_target_is_not_added(self):
        runner = _runner(
            source=SRC,
            target=[("id", "INTEGER"), ("a", "VARCHAR"), *SYSTEM],
            exclude_columns=["payload"],
        )
        value_columns = runner.sql_builder.select_value_columns(
            runner._source_columns()
        )
        assert runner._reconcile_target_columns(value_columns) == ["a"]
        runner.destination.add_column.assert_not_called()

    def test_excluding_every_value_column_is_refused_with_a_hint(self):
        runner = _runner(source=SRC, target=[], exclude_columns=["a", "payload"])
        with pytest.raises(ValueError, match="exclude_columns"):
            runner._discover_value_columns()


class TestNarrowedLog:
    def test_exclusion_note_names_the_ways_to_remove_the_column(self, caplog):
        runner = _runner(source=SRC, target=[], exclude_columns=["payload"])
        manager = _manager()
        runner.state_manager = manager
        state = _state(manager)
        with caplog.at_level(logging.INFO, logger="dlt_saga.historize.runner"):
            runner._check_config_changes(state)
        message = next(
            r.getMessage() for r in caplog.records if "narrowed" in r.getMessage()
        )
        assert "exclude_columns: [] → ['payload']" in message
        assert "--full-refresh" in message and "by hand" in message
