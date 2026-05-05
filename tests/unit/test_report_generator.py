"""Unit tests for the HTML report generator.

These cover the serialization layer (ReportData → JSON) and the file-write
happy path. The CSS/JS asset reading is exercised by writing a real report
to a tmp_path so we catch packaging mistakes (missing asset files).
"""

import json
from datetime import datetime, timezone

import pytest

from dlt_saga.report.collector import (
    ExecutionInfo,
    HistorizeRun,
    LoadRun,
    OrchestrationRun,
    PipelineInfo,
    ReportData,
)
from dlt_saga.report.generator import _serialize_report_data, generate_report


def _empty_report() -> ReportData:
    return ReportData(
        generated_at=datetime(2026, 5, 5, 12, 0, 0, tzinfo=timezone.utc),
        environment="dev",
        project="my-project",
        days=14,
    )


def _populated_report() -> ReportData:
    ts = datetime(2026, 5, 5, 12, 0, 0, tzinfo=timezone.utc)
    end = datetime(2026, 5, 5, 12, 0, 30, tzinfo=timezone.utc)
    return ReportData(
        generated_at=ts,
        environment="prod",
        project="my-project",
        days=7,
        pipelines=[
            PipelineInfo(
                pipeline_name="filesystem__sample",
                pipeline_group="filesystem",
                tags=["daily"],
                write_disposition="append",
                ingest_enabled=True,
                historize_enabled=False,
                enabled=True,
                table_name="sample",
                schema_name="dlt_filesystem",
                adapter=None,
            )
        ],
        load_runs=[
            LoadRun(
                pipeline_name="filesystem__sample",
                table_name="sample",
                dataset_name="dlt_filesystem",
                destination_name="duckdb",
                destination_type="duckdb",
                row_count=42,
                started_at=ts,
                finished_at=end,
                first_run=True,
            )
        ],
        historize_runs=[
            HistorizeRun(
                pipeline_name="filesystem__snap",
                source_table="snap",
                target_table="snap_h",
                snapshot_value="2026-05-04",
                new_or_changed_rows=3,
                deleted_rows=1,
                is_full_reprocess=False,
                started_at=ts,
                finished_at=end,
                status="completed",
            )
        ],
        orchestration_runs=[
            OrchestrationRun(
                execution_id="exec-1",
                task_index=0,
                pipeline_group="filesystem",
                pipeline_name="filesystem__sample",
                table_name="sample",
                status="success",
                log_timestamp=ts,
                started_at=ts,
                completed_at=end,
                error_message=None,
            )
        ],
        executions=[
            ExecutionInfo(
                execution_id="exec-1",
                created_at=ts,
                command="ingest",
                pipeline_count=1,
                task_count=1,
                select_criteria="tag:daily",
                environment="prod",
                profile="default",
                target="prod",
            )
        ],
    )


@pytest.mark.unit
class TestSerializeReportData:
    def test_empty_report_round_trips_through_json(self):
        out = _serialize_report_data(_empty_report())
        parsed = json.loads(out)
        assert parsed["environment"] == "dev"
        assert parsed["project"] == "my-project"
        assert parsed["days"] == 14
        for key in (
            "pipelines",
            "load_runs",
            "historize_runs",
            "orchestration_runs",
            "executions",
        ):
            assert parsed[key] == [], f"{key} should be empty list"

    def test_populated_report_serializes_all_collections(self):
        out = _serialize_report_data(_populated_report())
        parsed = json.loads(out)
        assert len(parsed["pipelines"]) == 1
        assert parsed["pipelines"][0]["pipeline_name"] == "filesystem__sample"
        assert len(parsed["load_runs"]) == 1
        assert parsed["load_runs"][0]["row_count"] == 42
        assert parsed["load_runs"][0]["duration_seconds"] == 30.0
        assert len(parsed["historize_runs"]) == 1
        assert parsed["historize_runs"][0]["new_or_changed_rows"] == 3
        assert len(parsed["orchestration_runs"]) == 1
        assert parsed["orchestration_runs"][0]["execution_id"] == "exec-1"
        assert len(parsed["executions"]) == 1
        assert parsed["executions"][0]["target"] == "prod"

    def test_none_datetimes_serialize_as_null(self):
        data = _empty_report()
        data.load_runs.append(
            LoadRun(
                pipeline_name="a",
                table_name="b",
                dataset_name="c",
                destination_name="d",
                destination_type="duckdb",
                row_count=0,
                started_at=None,
                finished_at=None,
                first_run=True,
            )
        )
        parsed = json.loads(_serialize_report_data(data))
        assert parsed["load_runs"][0]["started_at"] is None
        assert parsed["load_runs"][0]["finished_at"] is None
        assert parsed["load_runs"][0]["duration_seconds"] is None


@pytest.mark.unit
class TestGenerateReport:
    def test_generate_report_writes_html_file(self, tmp_path):
        out = tmp_path / "report.html"
        result_path = generate_report(_populated_report(), str(out))
        assert out.exists(), "Report file was not created"
        assert result_path == str(out.resolve())

    def test_generated_html_has_expected_structure(self, tmp_path):
        out = tmp_path / "report.html"
        generate_report(_populated_report(), str(out))
        html = out.read_text(encoding="utf-8")
        assert html.startswith("<!DOCTYPE html>")
        assert "<title>Saga Pipeline Report</title>" in html
        assert "const REPORT_DATA = " in html
        assert "</html>" in html.rstrip()

    def test_generated_html_embeds_report_data_as_json(self, tmp_path):
        out = tmp_path / "report.html"
        generate_report(_populated_report(), str(out))
        html = out.read_text(encoding="utf-8")
        # Pull the JSON literal out of the script tag and parse it
        marker = "const REPORT_DATA = "
        start = html.index(marker) + len(marker)
        end = html.index(";\n", start)
        embedded = json.loads(html[start:end])
        assert embedded["project"] == "my-project"
        assert embedded["pipelines"][0]["pipeline_name"] == "filesystem__sample"
