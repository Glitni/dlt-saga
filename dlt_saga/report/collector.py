"""Data collector for pipeline observability reports.

Queries _saga_load_info, _saga_historize_log, and _saga_execution_plans tables
from the destination, and combines with pipeline config metadata to produce
report data.
"""

import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

from dlt_saga.pipeline_config.base_config import PipelineConfig

logger = logging.getLogger(__name__)


@dataclass
class LoadRun:
    """A single ingest run record from _saga_load_info."""

    pipeline_name: str
    table_name: str
    dataset_name: str
    destination_name: str
    destination_type: str
    row_count: int
    started_at: Optional[datetime]
    finished_at: Optional[datetime]
    first_run: bool

    @property
    def duration_seconds(self) -> Optional[float]:
        if self.started_at and self.finished_at:
            return (self.finished_at - self.started_at).total_seconds()
        return None

    @property
    def status(self) -> str:
        return "success"  # Only successful loads are recorded


@dataclass
class HistorizeRun:
    """A single historize run record from _saga_historize_log."""

    pipeline_name: str
    source_table: str
    target_table: str
    snapshot_value: str
    new_or_changed_rows: int
    deleted_rows: int
    is_full_reprocess: bool
    started_at: Optional[datetime]
    finished_at: Optional[datetime]
    status: str

    @property
    def duration_seconds(self) -> Optional[float]:
        if self.started_at and self.finished_at:
            return (self.finished_at - self.started_at).total_seconds()
        return None


@dataclass
class OrchestrationRun:
    """A pipeline task from an orchestration execution plan."""

    execution_id: str
    task_index: int
    pipeline_group: str
    pipeline_name: str
    table_name: str
    status: str
    log_timestamp: Optional[datetime]
    started_at: Optional[datetime]
    completed_at: Optional[datetime]
    error_message: Optional[str]

    @property
    def duration_seconds(self) -> Optional[float]:
        if self.started_at and self.completed_at:
            return (self.completed_at - self.started_at).total_seconds()
        return None


@dataclass
class ExecutionInfo:
    """Execution-level metadata from _saga_executions."""

    execution_id: str
    created_at: Optional[datetime]
    command: str
    pipeline_count: int
    task_count: int
    select_criteria: Optional[str]
    environment: Optional[str]
    profile: Optional[str]
    target: Optional[str]


@dataclass
class PipelineInfo:
    """Metadata about a pipeline, combining config and run history."""

    pipeline_name: str
    pipeline_group: str
    tags: List[str]
    write_disposition: str
    ingest_enabled: bool
    historize_enabled: bool
    enabled: bool
    table_name: str
    schema_name: str
    adapter: Optional[str] = None


@dataclass
class ReportData:
    """All data needed to render the report."""

    generated_at: datetime = field(default_factory=datetime.now)
    environment: str = ""
    project: str = ""
    days: int = 14
    pipelines: List[PipelineInfo] = field(default_factory=list)
    load_runs: List[LoadRun] = field(default_factory=list)
    historize_runs: List[HistorizeRun] = field(default_factory=list)
    orchestration_runs: List[OrchestrationRun] = field(default_factory=list)
    executions: List[ExecutionInfo] = field(default_factory=list)


def collect_pipeline_metadata(
    configs: Dict[str, List[PipelineConfig]],
) -> List[PipelineInfo]:
    """Extract pipeline metadata from discovered configs."""
    pipelines = []
    for group, config_list in configs.items():
        for config in config_list:
            pipelines.append(
                PipelineInfo(
                    pipeline_name=config.pipeline_name,
                    pipeline_group=config.pipeline_group,
                    tags=config.get_tag_names(),
                    write_disposition=config.raw_write_disposition,
                    ingest_enabled=config.ingest_enabled,
                    historize_enabled=config.historize_enabled,
                    enabled=config.enabled,
                    table_name=config.table_name,
                    schema_name=config.schema_name,
                    adapter=config.adapter,
                )
            )
    return pipelines


def _query_load_runs(destination: Any, schema: str, days: int) -> List[LoadRun]:
    """Query the load-info table for recent ingest runs."""
    from dlt_saga.project_config import get_load_info_table_name

    table_id = destination.get_full_table_id(schema, get_load_info_table_name())
    sql = f"""
        SELECT
            pipeline_name,
            table_name,
            dataset_name,
            destination_name,
            destination_type,
            row_count,
            started_at,
            finished_at,
            first_run
        FROM {table_id}
        WHERE started_at >= {destination.timestamp_n_days_ago(days)}
    """
    try:
        rows = list(destination.execute_sql(sql, schema))
        runs = []
        for row in rows:
            runs.append(
                LoadRun(
                    pipeline_name=row.pipeline_name,
                    table_name=row.table_name,
                    dataset_name=row.dataset_name,
                    destination_name=getattr(row, "destination_name", ""),
                    destination_type=getattr(row, "destination_type", ""),
                    row_count=row.row_count or 0,
                    started_at=row.started_at,
                    finished_at=row.finished_at,
                    first_run=getattr(row, "first_run", False),
                )
            )
        logger.debug(
            f"Fetched {len(runs)} load run(s) from {get_load_info_table_name()}"
        )
        return runs
    except Exception as e:
        logger.debug(f"No load info table in {schema} (skipping): {e}")
        return []


def _query_historize_runs(
    destination: Any, schema: str, days: int
) -> List[HistorizeRun]:
    """Query the historize-log table for recent historize runs."""
    from dlt_saga.project_config import get_historize_log_table_name

    table_id = destination.get_full_table_id(schema, get_historize_log_table_name())
    sql = f"""
        SELECT
            pipeline_name,
            source_table,
            target_table,
            snapshot_value,
            new_or_changed_rows,
            deleted_rows,
            is_full_reprocess,
            started_at,
            finished_at,
            status
        FROM {table_id}
        WHERE started_at >= {destination.timestamp_n_days_ago(days)}
    """
    try:
        rows = list(destination.execute_sql(sql, schema))
        runs = []
        for row in rows:
            runs.append(
                HistorizeRun(
                    pipeline_name=row.pipeline_name,
                    source_table=row.source_table,
                    target_table=row.target_table,
                    snapshot_value=row.snapshot_value or "",
                    new_or_changed_rows=row.new_or_changed_rows or 0,
                    deleted_rows=row.deleted_rows or 0,
                    is_full_reprocess=getattr(row, "is_full_reprocess", False),
                    started_at=row.started_at,
                    finished_at=row.finished_at,
                    status=row.status or "unknown",
                )
            )
        logger.debug(
            f"Fetched {len(runs)} historize run(s) from {get_historize_log_table_name()}"
        )
        return runs
    except Exception as e:
        logger.debug(f"No historize log table in {schema} (skipping): {e}")
        return []


def _query_orchestration_runs(
    destination: Any, schema: str, days: int
) -> List[OrchestrationRun]:
    """Query the execution plans table for orchestration run data.

    Uses QUALIFY to get the latest status per pipeline per execution
    (the table is append-only with status updates as new rows).
    """
    from dlt_saga.project_config import get_execution_plans_table_name

    table_id = destination.get_full_table_id(schema, get_execution_plans_table_name())
    sql = f"""
        SELECT
            execution_id,
            task_index,
            pipeline_type,
            pipeline_identifier,
            table_name,
            status,
            log_timestamp,
            started_at,
            completed_at,
            error_message
        FROM {table_id}
        WHERE log_timestamp >= {destination.timestamp_n_days_ago(days)}
        QUALIFY ROW_NUMBER() OVER (
            PARTITION BY execution_id, task_index, pipeline_identifier
            ORDER BY log_timestamp DESC
        ) = 1
        ORDER BY log_timestamp DESC
    """
    try:
        rows = list(destination.execute_sql(sql, schema))
        runs = []
        for row in rows:
            # Derive pipeline_name from group + table to match load_run naming
            pipeline_name = f"{row.pipeline_type}__{row.table_name}"
            runs.append(
                OrchestrationRun(
                    execution_id=row.execution_id,
                    task_index=row.task_index,
                    pipeline_group=row.pipeline_type,
                    pipeline_name=pipeline_name,
                    table_name=row.table_name,
                    status=row.status or "unknown",
                    log_timestamp=row.log_timestamp,
                    started_at=row.started_at,
                    completed_at=row.completed_at,
                    error_message=getattr(row, "error_message", None) or None,
                )
            )
        logger.debug(
            f"Fetched {len(runs)} orchestration run(s) from {get_execution_plans_table_name()}"
        )
        return runs
    except Exception as e:
        logger.debug(f"No execution plans table in {schema} (skipping): {e}")
        return []


def _query_executions(destination: Any, schema: str, days: int) -> List[ExecutionInfo]:
    """Query the executions table for execution-level metadata."""
    from dlt_saga.project_config import get_executions_table_name

    table_id = destination.get_full_table_id(schema, get_executions_table_name())
    sql = f"""
        SELECT
            execution_id,
            created_at,
            command,
            pipeline_count,
            task_count,
            select_criteria,
            environment,
            profile,
            target
        FROM {table_id}
        WHERE created_at >= {destination.timestamp_n_days_ago(days)}
        ORDER BY created_at DESC
    """
    try:
        rows = list(destination.execute_sql(sql, schema))
        return [
            ExecutionInfo(
                execution_id=row.execution_id,
                created_at=row.created_at,
                command=getattr(row, "command", "ingest"),
                pipeline_count=getattr(row, "pipeline_count", 0),
                task_count=getattr(row, "task_count", 0),
                select_criteria=getattr(row, "select_criteria", None),
                environment=getattr(row, "environment", None),
                profile=getattr(row, "profile", None),
                target=getattr(row, "target", None),
            )
            for row in rows
        ]
    except Exception as e:
        logger.debug(f"No executions table in {schema} (skipping): {e}")
        return []


def _run_queries_parallel(
    tasks: List[Tuple[str, Any]],
) -> Tuple[
    List[LoadRun], List[HistorizeRun], List[OrchestrationRun], List[ExecutionInfo]
]:
    """Execute tagged query tasks in parallel and route results by tag.

    Tags: "load", "historize", "orch", "exec".
    Returns merged lists sorted by started_at/created_at descending.
    """
    all_load_runs: List[LoadRun] = []
    all_historize_runs: List[HistorizeRun] = []
    orchestration_runs: List[OrchestrationRun] = []
    executions: List[ExecutionInfo] = []

    with ThreadPoolExecutor(max_workers=min(len(tasks), 8)) as executor:
        future_to_tag = {executor.submit(fn): tag for tag, fn in tasks}
        for future in as_completed(future_to_tag):
            tag = future_to_tag[future]
            try:
                result = future.result()
            except Exception as e:
                logger.debug(f"Report query task '{tag}' failed: {e}")
                continue
            if tag == "load":
                all_load_runs.extend(result)
            elif tag == "historize":
                all_historize_runs.extend(result)
            elif tag == "orch":
                orchestration_runs = result
            elif tag == "exec":
                executions = result

    all_load_runs.sort(key=lambda r: r.started_at or datetime.min, reverse=True)
    all_historize_runs.sort(key=lambda r: r.started_at or datetime.min, reverse=True)

    return all_load_runs, all_historize_runs, orchestration_runs, executions


def collect_report_data(
    configs: Dict[str, List[PipelineConfig]],
    destination: Any,
    schema: str,
    days: int = 30,
    environment: str = "",
    project: str = "",
    orchestration_schema: Optional[str] = None,
) -> ReportData:
    """Collect all data needed for the report.

    Args:
        configs: Discovered pipeline configs (enabled)
        destination: Destination instance for querying run history
        schema: Dataset/schema name to query for run data
        days: Number of days of history to include
        environment: Environment name (dev/prod)
        project: Project identifier
        orchestration_schema: Dataset containing _saga_execution_plans table

    Returns:
        ReportData with all collected information
    """
    pipelines = collect_pipeline_metadata(configs)

    # Collect unique schemas from pipeline configs, plus the provided fallback schema
    schemas_seen: set = set()
    for p in pipelines:
        if p.schema_name:
            schemas_seen.add(p.schema_name)
    if schema and schema not in schemas_seen:
        schemas_seen.add(schema)

    orch_schema = orchestration_schema or schema

    # Build the full set of (query_fn, schema) tasks to run in parallel.
    # Each task is a (tag, callable) pair so results can be routed correctly.
    tasks: List[Tuple[str, Any]] = []
    for s in schemas_seen:
        tasks.append(("load", lambda s=s: _query_load_runs(destination, s, days)))
        tasks.append(
            ("historize", lambda s=s: _query_historize_runs(destination, s, days))
        )
    if orch_schema:
        tasks.append(
            (
                "orch",
                lambda: _query_orchestration_runs(destination, orch_schema, days),
            )
        )
        tasks.append(
            ("exec", lambda: _query_executions(destination, orch_schema, days))
        )

    all_load_runs, all_historize_runs, orchestration_runs, executions = (
        _run_queries_parallel(tasks)
    )

    return ReportData(
        generated_at=datetime.now(),
        environment=environment,
        project=project,
        days=days,
        pipelines=pipelines,
        load_runs=all_load_runs,
        historize_runs=all_historize_runs,
        orchestration_runs=orchestration_runs,
        executions=executions,
    )
