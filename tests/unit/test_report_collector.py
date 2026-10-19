"""Unit tests for report-collector error classification.

A missing table is an expected empty section (debug); any other error must warn
loudly so a permission/network failure doesn't render a silently-empty section.
"""

import logging
from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from dlt_saga.report.collector import (
    _as_str_list,
    _query_load_runs,
    _query_orchestration_runs,
    _sortable_ts,
    collect_pipeline_metadata,
    collect_report_data,
)


def _fake_config(**config_dict):
    """A minimal stand-in for PipelineConfig for collect_pipeline_metadata."""
    return SimpleNamespace(
        pipeline_name="api__orders",
        pipeline_group="api",
        get_tag_names=lambda: ["daily"],
        raw_write_disposition="append",
        ingest_enabled=True,
        historize_enabled=False,
        enabled=True,
        table_name="orders",
        schema_name="dlt_api",
        adapter=None,
        config_dict=config_dict,
    )


def _dest_raising(exc):
    dest = MagicMock()
    dest.get_full_table_id.return_value = "cat.schema.tbl"
    dest.timestamp_n_days_ago.return_value = "TS"
    dest.execute_sql.side_effect = exc
    return dest


@pytest.mark.unit
class TestReportQueryErrorClassification:
    def test_missing_table_is_debug_not_warning(self, caplog):
        dest = _dest_raising(RuntimeError("Table cat.schema.tbl does not exist"))
        with caplog.at_level(logging.DEBUG, logger="dlt_saga.report.collector"):
            result = _query_load_runs(dest, "schema", 30)
        assert result == []
        assert not any(r.levelno == logging.WARNING for r in caplog.records)

    def test_real_error_warns_but_does_not_raise(self, caplog):
        dest = _dest_raising(RuntimeError("PERMISSION_DENIED"))
        with caplog.at_level(logging.WARNING, logger="dlt_saga.report.collector"):
            result = _query_load_runs(dest, "schema", 30)
        assert result == []
        assert any(
            r.levelno == logging.WARNING and "incomplete" in r.getMessage()
            for r in caplog.records
        )


@pytest.mark.unit
class TestCollectPipelineMetadata:
    def test_reads_doc_metadata_from_config_dict(self):
        cfg = _fake_config(
            description="Orders",
            classification=["pii:false"],
            meta={"data_owner": "team@example.com"},
        )
        (info,) = collect_pipeline_metadata({"api": [cfg]})
        assert info.description == "Orders"
        assert info.classification == ["pii:false"]
        assert info.meta == {"data_owner": "team@example.com"}

    def test_absent_doc_metadata_defaults(self):
        (info,) = collect_pipeline_metadata({"api": [_fake_config()]})
        assert info.description is None
        assert info.classification == []
        assert info.meta is None


@pytest.mark.unit
class TestAsStrList:
    def test_none_is_empty(self):
        assert _as_str_list(None) == []

    def test_scalar_string_wrapped(self):
        assert _as_str_list("pii") == ["pii"]

    def test_list_coerced_to_strings(self):
        assert _as_str_list([1, "b"]) == ["1", "b"]


@pytest.mark.unit
class TestSortableTs:
    """The report sort must not crash comparing a NULL/naive started_at against
    tz-aware warehouse timestamps."""

    def test_none_sorts_before_real_timestamp(self):
        aware = datetime(2026, 1, 1, tzinfo=timezone.utc)
        assert _sortable_ts(None) < _sortable_ts(aware)

    def test_naive_is_treated_as_utc_and_comparable(self):
        naive = datetime(2026, 1, 1)
        aware = datetime(2026, 1, 1, tzinfo=timezone.utc)
        # No TypeError, and equal instants compare equal.
        assert _sortable_ts(naive) == _sortable_ts(aware)

    def test_mixed_list_sorts_without_typeerror(self):
        items = [
            datetime(2026, 3, 1, tzinfo=timezone.utc),
            None,
            datetime(2026, 1, 1),  # naive
        ]
        ordered = sorted(items, key=_sortable_ts, reverse=True)
        assert ordered[0] == datetime(2026, 3, 1, tzinfo=timezone.utc)
        assert ordered[-1] is None


@pytest.mark.unit
class TestOrchestrationCompleteness:
    """The plans table is the only record of failed attempts, so an unreadable
    one must be distinguishable from an empty history."""

    def test_missing_plans_table_is_an_empty_history(self):
        dest = _dest_raising(RuntimeError("Table cat.schema.tbl does not exist"))
        assert _query_orchestration_runs(dest, "schema", 30) == []

    def test_unreadable_plans_table_is_none(self):
        dest = _dest_raising(RuntimeError("PERMISSION_DENIED"))
        assert _query_orchestration_runs(dest, "schema", 30) is None

    def test_collect_flags_incomplete_history(self):
        dest = _dest_raising(RuntimeError("PERMISSION_DENIED"))
        data = collect_report_data(
            {"api": [_fake_config()]}, dest, "dlt_api", orchestration_schema="orch"
        )
        assert data.orchestration_complete is False
        assert data.orchestration_runs == []

    def test_collect_missing_tables_is_complete(self):
        dest = _dest_raising(RuntimeError("Table cat.schema.tbl does not exist"))
        data = collect_report_data(
            {"api": [_fake_config()]}, dest, "dlt_api", orchestration_schema="orch"
        )
        assert data.orchestration_complete is True

    def test_collect_carries_disabled_pipelines(self):
        dest = _dest_raising(RuntimeError("Table cat.schema.tbl does not exist"))
        data = collect_report_data(
            {"api": [_fake_config()]},
            dest,
            "dlt_api",
            orchestration_schema="orch",
            disabled_configs={"api": [_fake_config()]},
        )
        assert [p.pipeline_name for p in data.disabled_pipelines] == ["api__orders"]
        assert len(data.pipelines) == 1


def _named_config(pipeline_name, pipeline_group, table_name, schema_name):
    config = _fake_config()
    config.pipeline_name = pipeline_name
    config.pipeline_group = pipeline_group
    config.table_name = table_name
    config.schema_name = schema_name
    return config


def _plan_row(pipeline_type, table_name):
    return SimpleNamespace(
        execution_id="exec-1",
        task_index=0,
        pipeline_type=pipeline_type,
        table_name=table_name,
        status="failed",
        log_timestamp=None,
        started_at=None,
        completed_at=None,
        error_message="boom",
        is_orchestrated=False,
    )


def _dest_with_plans(*rows):
    """Serves ``rows`` from the plans query; every other table is missing."""
    dest = MagicMock()
    dest.get_full_table_id.return_value = "cat.schema.tbl"
    dest.timestamp_n_days_ago.return_value = "TS"

    def _execute(sql, schema):
        if "QUALIFY" in sql:
            return list(rows)
        raise RuntimeError("Table cat.schema.tbl does not exist")

    dest.execute_sql.side_effect = _execute
    return dest


def _collect(dest, configs, **kwargs):
    return collect_report_data(
        configs, dest, "dlt_dev", orchestration_schema="orch", **kwargs
    )


@pytest.mark.unit
class TestOrchestrationRunNaming:
    """Plan rows store no pipeline name; it is resolved from the configs by
    (pipeline_type, table_name), never rebuilt from naming conventions."""

    def test_dev_table_prefix_is_not_doubled(self):
        config = _named_config("sales__orders", "sales", "sales__orders", "dlt_dev")
        dest = _dest_with_plans(_plan_row("sales", "sales__orders"))
        (run,) = _collect(dest, {"sales": [config]}).orchestration_runs
        assert run.pipeline_name == "sales__orders"
        assert run.schema_name == "dlt_dev"

    def test_custom_table_name_resolves_to_the_pipeline(self):
        config = _named_config("sales__orders", "sales", "orders_v2", "analytics")
        dest = _dest_with_plans(_plan_row("sales", "orders_v2"))
        (run,) = _collect(dest, {"sales": [config]}).orchestration_runs
        assert run.pipeline_name == "sales__orders"
        assert run.schema_name == "analytics"

    def test_pipeline_outside_the_selection_resolves_from_known_configs(self):
        selected = _named_config("sales__orders", "sales", "sales__orders", "dlt_dev")
        other = _named_config("crm__accounts", "crm", "crm__accounts", "dlt_dev")
        dest = _dest_with_plans(_plan_row("crm", "crm__accounts"))
        data = _collect(
            dest,
            {"sales": [selected]},
            known_configs=[{"sales": [selected], "crm": [other]}, {}],
        )
        (run,) = data.orchestration_runs
        assert run.pipeline_name == "crm__accounts"

    def test_disabled_pipeline_resolves_by_default(self):
        enabled = _named_config("sales__orders", "sales", "sales__orders", "dlt_dev")
        disabled = _named_config("crm__accounts", "crm", "crm__accounts", "dlt_dev")
        dest = _dest_with_plans(_plan_row("crm", "crm__accounts"))
        data = _collect(
            dest, {"sales": [enabled]}, disabled_configs={"crm": [disabled]}
        )
        (run,) = data.orchestration_runs
        assert run.pipeline_name == "crm__accounts"

    def test_unconfigured_pipeline_keeps_its_table_name(self):
        config = _named_config("sales__orders", "sales", "sales__orders", "dlt_dev")
        dest = _dest_with_plans(_plan_row("sales", "sales__removed"))
        (run,) = _collect(dest, {"sales": [config]}).orchestration_runs
        assert run.pipeline_name == "sales__removed"
        assert run.schema_name == ""
