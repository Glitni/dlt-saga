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
from dlt_saga.report.status import StatusSummary, compute_status, summary_to_dict

logger = logging.getLogger(__name__)

_ASSETS_DIR = Path(__file__).parent


def _read_asset(filename: str) -> str:
    """Read a static asset file from the report package directory."""
    return (_ASSETS_DIR / filename).read_text(encoding="utf-8")


def _serialize_report_data(
    data: ReportData, status: Optional[StatusSummary] = None
) -> str:
    """Serialize ReportData to JSON for embedding in HTML.

    ``status`` is the verdict set the dashboard's health headline reads, so the
    page and the status summary judge pipelines identically. Computed from
    ``data`` when not given.
    """
    status = status or compute_status(data)

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
                "description": p.description,
                "classification": p.classification,
                "meta": p.meta,
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
                "is_orchestrated": o.is_orchestrated,
                "schema_name": o.schema_name,
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
                "start_value_override": e.start_value_override,
                "end_value_override": e.end_value_override,
                "is_orchestrated": e.is_orchestrated,
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
            "status": {
                "complete": status.complete,
                "pipelines": [
                    {
                        "pipeline_name": p.pipeline_name,
                        "state": p.state,
                        "is_overdue": p.is_overdue,
                    }
                    for p in status.pipelines
                ],
            },
        },
        indent=None,
    )


def generate_report(
    data: ReportData, output_path: str, status: Optional[StatusSummary] = None
) -> str:
    """Generate a standalone HTML report file.

    Args:
        data: Collected report data
        output_path: Path to write the HTML file
        status: Precomputed verdicts, so a summary written alongside judges
            from the same instant. Computed from ``data`` when not given.

    Returns:
        Absolute path of the generated file
    """
    json_data = _serialize_report_data(data, status)
    css = _read_asset("report.css")
    js = _read_asset("report.js")
    favicon_svg = _read_asset("favicon.svg")
    html = _build_html(json_data, css, js, favicon_svg)

    path = Path(output_path)
    path.write_text(html, encoding="utf-8")

    abs_path = str(path.resolve())
    logger.debug(f"Report written to {abs_path}")
    return abs_path


def summary_location(report_output: str) -> str:
    """Where the status summary goes for a given report output.

    Beside the report, with ``.json`` for its extension, so it inherits the
    report's location and access rules. A ``gs://`` URI naming only a bucket
    gets the summary beside the default report object.

    Raises:
        ValueError: If the report itself is a ``.json`` path, which the summary
            would overwrite.
    """
    from dlt_saga.report.uploader import DEFAULT_REPORT_OBJECT, is_remote_uri

    if is_remote_uri(report_output):
        scheme, _, remainder = report_output.partition("://")
        bucket, _, object_name = remainder.partition("/")
        prefix, _, name = (object_name or DEFAULT_REPORT_OBJECT).rpartition("/")
        directory = f"{bucket}/{prefix}/" if prefix else f"{bucket}/"
        location = f"{scheme}://{directory}{_json_name(name)}"
    else:
        path = Path(report_output)
        location = str(path.with_name(_json_name(path.name)))
    if location == report_output:
        raise ValueError(
            f"The report output '{report_output}' is already a .json path, so the "
            "status summary would overwrite it. Give the report an .html name."
        )
    return location


def _json_name(name: str) -> str:
    stem = name.rsplit(".", 1)[0] if "." in name else name
    return f"{stem}.json"


def generate_status_summary(
    status: StatusSummary,
    output_path: str,
    *,
    stale_after_minutes: int,
    report_file: Optional[str],
) -> str:
    """Write the machine-readable status summary as JSON.

    Args:
        status: The verdicts to publish.
        output_path: Path to write the JSON file.
        stale_after_minutes: How long the summary stays current.
        report_file: File name of the HTML report it sits beside.

    Returns:
        Absolute path of the generated file.
    """
    payload = summary_to_dict(
        status, stale_after_minutes=stale_after_minutes, report_file=report_file
    )
    path = Path(output_path)
    # Compact: the file is polled by machines, and indentation alone is a
    # quarter of its size. Pipe it through `jq` to read it.
    path.write_text(json.dumps(payload, separators=(",", ":")), encoding="utf-8")

    abs_path = str(path.resolve())
    logger.debug(f"Status summary written to {abs_path}")
    return abs_path


def _build_html(json_data: str, css: str, js: str, favicon_svg: str) -> str:
    """Build the complete HTML document with inlined CSS, JS, and favicon."""
    import base64

    favicon_b64 = base64.b64encode(favicon_svg.encode("utf-8")).decode("ascii")

    # Escape characters that would let a string value in the embedded JSON break
    # out of the <script> block (e.g. an error message containing "</script>").
    # These only occur inside JSON string literals, and \uXXXX is valid there, so
    # the payload stays parseable while "</script>" can no longer close the tag.
    json_data = (
        json_data.replace("<", "\\u003c")
        .replace(">", "\\u003e")
        .replace("&", "\\u0026")
    )

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
      <span class="brand-logo">{favicon_svg}</span>
      <div class="brand-text">
        <h1>Saga</h1>
        <span class="subtitle">Pipeline report</span>
      </div>
    </div>
    <ul class="nav-links">
      <li class="nav-group-label">Overview</li>
      <li><a href="#" class="nav-link active" data-tab="dashboard">Dashboard</a></li>
      <li class="nav-group-label">Runs</li>
      <li><a href="#" class="nav-link" data-tab="orchestration">Executions</a></li>
      <li><a href="#" class="nav-link" data-tab="ingest-runs">Ingest runs</a></li>
      <li><a href="#" class="nav-link" data-tab="historize-runs">Historize runs</a></li>
      <li class="nav-group-label">Catalog</li>
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
