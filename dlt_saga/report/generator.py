"""HTML report generator for pipeline observability.

Produces a standalone HTML file with embedded CSS and JavaScript,
containing a dashboard, pipeline catalog, and run history.

The CSS and JS are maintained as separate files (report.css, report.js)
for easier editing and are inlined at report generation time.
"""

import json
import logging
from datetime import datetime
from pathlib import Path
from typing import Optional

from dlt_saga.report.collector import ReportData

logger = logging.getLogger(__name__)

_ASSETS_DIR = Path(__file__).parent


def _read_asset(filename: str) -> str:
    """Read a static asset file from the report package directory."""
    return (_ASSETS_DIR / filename).read_text(encoding="utf-8")


def _serialize_report_data(data: ReportData) -> str:
    """Serialize ReportData to JSON for embedding in HTML."""

    def _dt(v: Optional[datetime]) -> Optional[str]:
        return v.isoformat() if v else None

    pipelines = []
    for p in data.pipelines:
        pipelines.append(
            {
                "pipeline_name": p.pipeline_name,
                "pipeline_group": p.pipeline_group,
                "tags": p.tags,
                "write_disposition": p.write_disposition,
                "ingest_enabled": p.ingest_enabled,
                "historize_enabled": p.historize_enabled,
                "enabled": p.enabled,
                "table_name": p.table_name,
                "schema_name": p.schema_name,
                "adapter": p.adapter,
            }
        )

    load_runs = []
    for r in data.load_runs:
        load_runs.append(
            {
                "pipeline_name": r.pipeline_name,
                "table_name": r.table_name,
                "dataset_name": r.dataset_name,
                "destination_name": r.destination_name,
                "destination_type": r.destination_type,
                "row_count": r.row_count,
                "started_at": _dt(r.started_at),
                "finished_at": _dt(r.finished_at),
                "first_run": r.first_run,
                "duration_seconds": r.duration_seconds,
                "status": r.status,
            }
        )

    historize_runs = []
    for hr in data.historize_runs:
        historize_runs.append(
            {
                "pipeline_name": hr.pipeline_name,
                "source_table": hr.source_table,
                "target_table": hr.target_table,
                "snapshot_value": hr.snapshot_value,
                "new_or_changed_rows": hr.new_or_changed_rows,
                "deleted_rows": hr.deleted_rows,
                "is_full_reprocess": hr.is_full_reprocess,
                "started_at": _dt(hr.started_at),
                "finished_at": _dt(hr.finished_at),
                "status": hr.status,
                "duration_seconds": hr.duration_seconds,
            }
        )

    orchestration_runs = []
    for o in data.orchestration_runs:
        orchestration_runs.append(
            {
                "execution_id": o.execution_id,
                "task_index": o.task_index,
                "pipeline_group": o.pipeline_group,
                "pipeline_name": o.pipeline_name,
                "table_name": o.table_name,
                "status": o.status,
                "log_timestamp": _dt(o.log_timestamp),
                "started_at": _dt(o.started_at),
                "completed_at": _dt(o.completed_at),
                "error_message": o.error_message,
                "duration_seconds": o.duration_seconds,
            }
        )

    executions = []
    for e in data.executions:
        executions.append(
            {
                "execution_id": e.execution_id,
                "created_at": _dt(e.created_at),
                "command": e.command,
                "pipeline_count": e.pipeline_count,
                "task_count": e.task_count,
                "select_criteria": e.select_criteria,
                "environment": e.environment,
                "profile": e.profile,
                "target": e.target,
            }
        )

    return json.dumps(
        {
            "generated_at": _dt(data.generated_at),
            "environment": data.environment,
            "project": data.project,
            "days": data.days,
            "pipelines": pipelines,
            "load_runs": load_runs,
            "historize_runs": historize_runs,
            "orchestration_runs": orchestration_runs,
            "executions": executions,
        },
        indent=None,
    )


def generate_report(data: ReportData, output_path: str) -> str:
    """Generate a standalone HTML report file.

    Args:
        data: Collected report data
        output_path: Path to write the HTML file

    Returns:
        Absolute path of the generated file
    """
    json_data = _serialize_report_data(data)
    css = _read_asset("report.css")
    js = _read_asset("report.js")
    favicon_svg = _read_asset("favicon.svg")
    html = _build_html(json_data, css, js, favicon_svg)

    path = Path(output_path)
    path.write_text(html, encoding="utf-8")

    abs_path = str(path.resolve())
    logger.debug(f"Report written to {abs_path}")
    return abs_path


def _build_html(json_data: str, css: str, js: str, favicon_svg: str) -> str:
    """Build the complete HTML document with inlined CSS, JS, and favicon."""
    import base64

    favicon_b64 = base64.b64encode(favicon_svg.encode("utf-8")).decode("ascii")
    return f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Saga Pipeline Report</title>
<link rel="icon" type="image/svg+xml" href="data:image/svg+xml;base64,{favicon_b64}">
<style>
{css}
</style>
</head>
<body>
<div id="app">
  <nav class="sidebar">
    <div class="sidebar-header">
      <h1>Saga</h1>
      <span class="subtitle">Pipeline Report</span>
    </div>
    <ul class="nav-links">
      <li><a href="#" class="nav-link active" data-tab="dashboard">Dashboard</a></li>
      <li><a href="#" class="nav-link" data-tab="orchestration">Orchestration</a></li>
      <li><a href="#" class="nav-link" data-tab="ingest-runs">Ingest Runs</a></li>
      <li><a href="#" class="nav-link" data-tab="historize-runs">Historize Runs</a></li>
      <li><a href="#" class="nav-link" data-tab="pipelines">Pipelines</a></li>
    </ul>
    <div class="sidebar-footer">
      <div id="meta-info" class="sidebar-meta"></div>
      <button id="theme-toggle" class="theme-toggle"></button>
    </div>
  </nav>
  <main class="content">
    <div id="tab-dashboard" class="tab-panel active"></div>
    <div id="tab-orchestration" class="tab-panel"></div>
    <div id="tab-ingest-runs" class="tab-panel"></div>
    <div id="tab-historize-runs" class="tab-panel"></div>
    <div id="tab-pipelines" class="tab-panel"></div>
    <div id="tab-pipeline-detail" class="tab-panel"></div>
  </main>
</div>

<script>
const REPORT_DATA = {json_data};
{js}
</script>
</body>
</html>"""
