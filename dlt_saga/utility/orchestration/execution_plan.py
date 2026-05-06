"""Execution plan management for orchestrator/worker pattern.

This module manages the execution plan used for coordinating distributed
pipeline execution.  It stores plans via the configured Destination
abstraction (BigQuery, Snowflake, etc.) — no direct database imports.
"""

import json
import logging
import uuid
from dataclasses import dataclass
from typing import Any, Dict, List, Optional

from dlt_saga.pipeline_config import PipelineConfig
from dlt_saga.utility.cli.logging import YELLOW, colorize

logger = logging.getLogger(__name__)


def _escape(value: str) -> str:
    """Escape a string value for safe use in SQL single-quoted literals."""
    return value.replace("'", "''").replace("\\", "\\\\")


@dataclass
class ExecutionMetadata:
    """Execution-level metadata for an orchestration run.

    Captures what was requested (selection criteria, command) and the context
    in which it ran (environment, profile, target). This is stored once per
    execution in the _saga_executions table.

    ``start_value_override`` and ``end_value_override`` are runtime overrides
    that the worker would otherwise miss (Cloud Run only forwards a fixed set
    of env vars). They are baked into each pipeline's stored config_json so
    workers pick them up when reconstructing the PipelineConfig.
    """

    select_criteria: Optional[str] = None
    command: str = "ingest"
    environment: Optional[str] = None
    profile: Optional[str] = None
    target: Optional[str] = None
    start_value_override: Optional[str] = None
    end_value_override: Optional[str] = None


class ExecutionPlanManager:
    """Manages execution plans for orchestrator/worker coordination.

    Uses the Destination abstraction for all SQL execution, DDL, and type
    mapping — following the same pattern as HistorizeStateManager.

    The execution plan table stores which pipelines to run and tracks their status.
    Each row represents one pipeline to be executed by a specific task.

    The executions table stores execution-level metadata (one row per execution):
    what was requested, by whom, and in what context.
    """

    def __init__(self, destination: Any, schema: str = "dlt_orchestration"):
        """Initialize the execution plan manager.

        Args:
            destination: Destination instance for SQL execution and dialect.
            schema: Schema/dataset for orchestration tables.
        """
        from dlt_saga.project_config import (
            get_execution_plans_table_name,
            get_execution_plans_view_name,
            get_executions_table_name,
        )

        self.destination = destination
        self.schema = schema
        plans_view = get_execution_plans_view_name()
        self.plans_table_id = destination.get_full_table_id(
            schema, get_execution_plans_table_name()
        )
        self.plans_view_name = plans_view
        self.plans_view_id = destination.get_full_table_id(schema, plans_view)
        self.executions_table_id = destination.get_full_table_id(
            schema, get_executions_table_name()
        )

    def _t(self, logical_type: str) -> str:
        """Shorthand for destination type name."""
        return self.destination.type_name(logical_type)

    def ensure_table_exists(self):
        """Ensure the orchestration tables and view exist.

        Uses CREATE TABLE IF NOT EXISTS, so safe to call repeatedly.

        Table schema:
        - log_timestamp: When this log entry was created (for partitioning)
        - execution_id: Unique ID for this execution run
        - task_index: Which worker task should execute this pipeline
        - pipeline_type: Pipeline group (google_sheets, filesystem, etc.)
        - pipeline_identifier: Unique identifier (file path, etc.)
        - table_name: Derived table name
        - config_json: Full configuration as JSON
        - status: pending/running/completed/failed
        - started_at: When execution started
        - completed_at: When execution completed
        - error_message: Error details if failed
        """
        d = self.destination
        d.ensure_schema_exists(self.schema)

        # Plans table (append-only log pattern)
        plans_ddl = f"""
            CREATE TABLE IF NOT EXISTS {self.plans_table_id} (
                log_timestamp {self._t("timestamp")} NOT NULL,
                execution_id {self._t("string")} NOT NULL,
                task_index {self._t("int64")} NOT NULL,
                pipeline_type {self._t("string")} NOT NULL,
                pipeline_identifier {self._t("string")} NOT NULL,
                table_name {self._t("string")} NOT NULL,
                config_json {d.json_type_name()} NOT NULL,
                status {self._t("string")} NOT NULL,
                started_at {self._t("timestamp")},
                completed_at {self._t("timestamp")},
                error_message {self._t("string")}
            )
            {d.partition_ddl("log_timestamp")}
            {d.cluster_ddl(["execution_id", "task_index"])}
        """
        d.execute_sql(plans_ddl, self.schema)

        # Executions metadata table
        exec_ddl = f"""
            CREATE TABLE IF NOT EXISTS {self.executions_table_id} (
                execution_id {self._t("string")} NOT NULL,
                created_at {self._t("timestamp")} NOT NULL,
                command {self._t("string")} NOT NULL,
                pipeline_count {self._t("int64")} NOT NULL,
                task_count {self._t("int64")} NOT NULL,
                select_criteria {self._t("string")},
                environment {self._t("string")},
                profile {self._t("string")},
                target {self._t("string")}
            )
        """
        d.execute_sql(exec_ddl, self.schema)

        # View for latest status per pipeline per execution
        self._ensure_view_exists()

    def _ensure_view_exists(self):
        """Create or replace the current-status view.

        Uses a window function to select the most recent log entry per
        (execution_id, task_index, pipeline_identifier) combination.
        """
        view_sql = f"""
            SELECT *
            FROM {self.plans_table_id}
            QUALIFY ROW_NUMBER() OVER (
                PARTITION BY execution_id, task_index, pipeline_identifier
                ORDER BY log_timestamp DESC
            ) = 1
        """
        try:
            self.destination.create_or_replace_view(
                self.schema, self.plans_view_name, view_sql
            )
        except Exception as e:
            logger.warning(f"Could not create view {self.plans_view_name}: {e}")

    def _log_task_mapping(self, rows_to_insert: List[Dict]):
        """Log mapping for task index to pipeline(s)."""
        sorted_rows = sorted(rows_to_insert, key=lambda x: x["task_index"])

        from itertools import groupby

        logger.info("Task index to pipeline mapping:")
        for task_idx, group in groupby(sorted_rows, key=lambda x: x["task_index"]):
            pipelines = [
                f"{row['table_name']} ({row['pipeline_type']})" for row in group
            ]
            if len(pipelines) == 1:
                logger.info(f"  Task {task_idx}: {pipelines[0]}")
            else:
                logger.info(
                    f"  Task {task_idx}: {len(pipelines)} pipelines - {', '.join(pipelines)}"
                )

    def _format_sql_value(self, value: Any) -> str:
        """Format a Python value as a SQL literal."""
        if value is None:
            return "NULL"
        if isinstance(value, bool):
            return "TRUE" if value else "FALSE"
        if isinstance(value, int):
            return str(value)
        return f"'{_escape(str(value))}'"

    @staticmethod
    def _config_overrides_from_metadata(
        metadata: "ExecutionMetadata",
    ) -> Dict[str, Any]:
        """Project runtime overrides off ExecutionMetadata into a dict.

        These get merged into each pipeline's stored config_json so the worker
        side picks them up — Cloud Run only forwards a fixed set of env vars,
        so the orchestrator's CLI flags don't otherwise reach workers.
        """
        overrides: Dict[str, Any] = {}
        if metadata.start_value_override:
            overrides["start_value_override"] = metadata.start_value_override
        if metadata.end_value_override:
            overrides["end_value_override"] = metadata.end_value_override
        return overrides

    @staticmethod
    def _interleave_by_dataset(
        configs: List[PipelineConfig],
    ) -> List[PipelineConfig]:
        """Interleave pipelines by target dataset (schema_name).

        Round-robins across datasets so that consecutive task indices write
        to different BigQuery datasets, avoiding bursts of concurrent writes
        to the same ``_dlt_pipeline_state`` table.
        """
        by_schema: Dict[str, List[PipelineConfig]] = {}
        for config in configs:
            by_schema.setdefault(config.schema_name, []).append(config)

        interleaved: List[PipelineConfig] = []
        iters = [iter(cfgs) for cfgs in by_schema.values()]
        while iters:
            remaining = []
            for it in iters:
                cfg = next(it, None)
                if cfg is not None:
                    interleaved.append(cfg)
                    remaining.append(it)
            iters = remaining
        return interleaved

    def create_execution_plan(
        self,
        pipeline_configs: List[PipelineConfig],
        metadata: Optional[ExecutionMetadata] = None,
    ) -> str:
        """Create a new execution plan for the given pipeline configs.

        Pipelines with the same task_group are assigned to the same task index
        and will run in parallel in the same worker task.

        Args:
            pipeline_configs: List of PipelineConfig objects to execute
            metadata: Optional execution-level metadata (select criteria, command, etc.)

        Returns:
            execution_id: Unique ID for this execution
        """
        execution_id = str(uuid.uuid4())
        logger.debug(
            f"Creating execution plan {execution_id} for {len(pipeline_configs)} pipelines"
        )

        self.ensure_table_exists()

        # Bake runtime overrides from metadata into each pipeline's stored
        # config_json so workers pick them up on the other side of the
        # orchestrator → Cloud Run hop. Workers don't see the orchestrator's
        # CLI flags; the only state they receive is the plan row.
        meta = metadata or ExecutionMetadata()
        config_overrides = self._config_overrides_from_metadata(meta)

        def _build_stored_config(config: PipelineConfig) -> Dict[str, Any]:
            return {
                **config.config_dict,
                **config_overrides,
                "pipeline_name": config.pipeline_name,
                "schema_name": config.schema_name,
            }

        # Group pipelines by task_group
        grouped_pipelines: Dict[str, List[PipelineConfig]] = {}
        ungrouped_pipelines = []

        for config in pipeline_configs:
            task_group = config.config_dict.get("task_group")
            if task_group:
                if task_group not in grouped_pipelines:
                    grouped_pipelines[task_group] = []
                grouped_pipelines[task_group].append(config)
            else:
                ungrouped_pipelines.append(config)

        # Assign task indices: groups first, then ungrouped pipelines
        rows_to_insert = []
        task_index = 0

        for task_group, group_configs in grouped_pipelines.items():
            logger.info(
                f"Task group '{task_group}': {len(group_configs)} pipelines "
                f"will run in parallel in task {task_index}"
            )
            for config in group_configs:
                rows_to_insert.append(
                    {
                        "execution_id": execution_id,
                        "task_index": task_index,
                        "pipeline_type": config.pipeline_group,
                        "pipeline_identifier": config.identifier,
                        "table_name": config.table_name,
                        "config_json": json.dumps(_build_stored_config(config)),
                        "status": "pending",
                    }
                )
            task_index += 1

        for config in self._interleave_by_dataset(ungrouped_pipelines):
            rows_to_insert.append(
                {
                    "execution_id": execution_id,
                    "task_index": task_index,
                    "pipeline_type": config.pipeline_group,
                    "pipeline_identifier": config.identifier,
                    "table_name": config.table_name,
                    "config_json": json.dumps(_build_stored_config(config)),
                    "status": "pending",
                }
            )
            task_index += 1

        if config_overrides:
            override_summary = ", ".join(
                f"{k}={v}" for k, v in config_overrides.items()
            )
            logger.info(f"Baked runtime overrides into plan: {override_summary}")

        # Build INSERT using SQL literals (same approach as HistorizeStateManager)
        d = self.destination
        now = d.current_timestamp_expression()
        value_clauses = []
        for row in rows_to_insert:
            parse_json = d.parse_json_expression(
                self._format_sql_value(row["config_json"])
            )
            value_clauses.append(
                f"({now}, "
                f"{self._format_sql_value(row['execution_id'])}, "
                f"{self._format_sql_value(row['task_index'])}, "
                f"{self._format_sql_value(row['pipeline_type'])}, "
                f"{self._format_sql_value(row['pipeline_identifier'])}, "
                f"{self._format_sql_value(row['table_name'])}, "
                f"{parse_json}, "
                f"{self._format_sql_value(row['status'])}, "
                f"NULL, NULL, NULL)"
            )

        insert_sql = f"""
            INSERT INTO {self.plans_table_id}
            (log_timestamp, execution_id, task_index, pipeline_type,
             pipeline_identifier, table_name, config_json, status,
             started_at, completed_at, error_message)
            VALUES
            {",".join(value_clauses)}
        """
        d.execute_sql(insert_sql, self.schema)

        # Insert execution-level metadata (meta was already initialized above)
        meta_sql = f"""
            INSERT INTO {self.executions_table_id}
            (execution_id, created_at, command, pipeline_count, task_count,
             select_criteria, environment, profile, target)
            VALUES (
                {self._format_sql_value(execution_id)},
                {now},
                {self._format_sql_value(meta.command)},
                {self._format_sql_value(len(rows_to_insert))},
                {self._format_sql_value(task_index)},
                {self._format_sql_value(meta.select_criteria)},
                {self._format_sql_value(meta.environment)},
                {self._format_sql_value(meta.profile)},
                {self._format_sql_value(meta.target)}
            )
        """
        d.execute_sql(meta_sql, self.schema)

        logger.info(
            f"Created execution plan {colorize(execution_id, YELLOW)} "
            f"with {len(rows_to_insert)} tasks"
        )
        self._log_task_mapping(rows_to_insert)

        return execution_id

    def get_task_assignment(
        self, execution_id: str, task_index: int
    ) -> List[PipelineConfig]:
        """Get the pipeline configs assigned to a specific task.

        A task may have multiple pipelines if they share a task_group.

        Args:
            execution_id: Execution plan ID
            task_index: Task index (from CLOUD_RUN_TASK_INDEX / SAGA_TASK_INDEX)

        Returns:
            List of PipelineConfig objects assigned to this task (empty if none found)
        """
        safe_eid = _escape(execution_id)
        query = f"""
            SELECT
                pipeline_type,
                pipeline_identifier,
                table_name,
                config_json
            FROM {self.plans_view_id}
            WHERE execution_id = '{safe_eid}'
              AND task_index = {int(task_index)}
            ORDER BY table_name
        """

        try:
            results = self.destination.execute_sql(query, self.schema)

            pipeline_configs = []
            for row in results:
                # config_json may come back as dict (BigQuery JSON) or string
                config_dict = row.config_json
                if isinstance(config_dict, str):
                    config_dict = json.loads(config_dict)

                pipeline_group = row.pipeline_type
                pipeline_configs.append(
                    PipelineConfig(
                        pipeline_group=pipeline_group,
                        pipeline_name=config_dict.get(
                            "pipeline_name",
                            f"{pipeline_group}__{row.table_name}",
                        ),
                        table_name=row.table_name,
                        identifier=row.pipeline_identifier,
                        config_dict=config_dict,
                        enabled=config_dict.get("enabled", True),
                        tags=config_dict.get("tags", []),
                        schema_name=config_dict.get("schema_name", ""),
                        adapter=config_dict.get("adapter"),
                        source_type="execution_plan",
                    )
                )

            if not pipeline_configs:
                logger.warning(
                    f"No task assignments found for execution {execution_id}, "
                    f"task {task_index}"
                )

            return pipeline_configs

        except Exception as e:
            logger.error(f"Failed to get task assignment: {e}")
            raise

    def update_task_status(
        self,
        execution_id: str,
        task_index: int,
        status: str,
        error_message: Optional[str] = None,
    ):
        """Log a status change for a task (append-only pattern).

        Updates status for ALL pipelines assigned to this task (supports task groups).
        Inserts a new log entry for each pipeline with the updated status.

        Args:
            execution_id: Execution plan ID
            task_index: Task index
            status: New status (pending/running/completed/failed)
            error_message: Optional error message if failed
        """
        d = self.destination
        safe_eid = _escape(execution_id)
        now = d.current_timestamp_expression()

        # Get current task data from view
        select_query = f"""
            SELECT
                pipeline_type,
                pipeline_identifier,
                table_name,
                config_json,
                started_at,
                completed_at
            FROM {self.plans_view_id}
            WHERE execution_id = '{safe_eid}'
              AND task_index = {int(task_index)}
            ORDER BY pipeline_identifier
        """

        try:
            result = d.execute_sql(select_query, self.schema)
            rows = list(result)

            if not rows:
                logger.warning(
                    f"No existing data found for execution {execution_id}, "
                    f"task {task_index}"
                )
                return

            for current_data in rows:
                # Resolve timestamps
                started_at = current_data.started_at
                completed_at = current_data.completed_at

                if status == "running" and not started_at:
                    started_at_sql = now
                elif started_at:
                    started_at_sql = self._format_sql_value(started_at)
                else:
                    started_at_sql = "NULL"

                if status in ("completed", "failed"):
                    completed_at_sql = now
                elif completed_at:
                    completed_at_sql = self._format_sql_value(completed_at)
                else:
                    completed_at_sql = "NULL"

                # config_json may be dict or string depending on destination
                config_json_raw = current_data.config_json
                if not isinstance(config_json_raw, str):
                    config_json_raw = json.dumps(config_json_raw)

                parse_json = d.parse_json_expression(
                    self._format_sql_value(config_json_raw)
                )

                insert_sql = f"""
                    INSERT INTO {self.plans_table_id}
                    (log_timestamp, execution_id, task_index, pipeline_type,
                     pipeline_identifier, table_name, config_json, status,
                     started_at, completed_at, error_message)
                    VALUES (
                        {now},
                        '{safe_eid}',
                        {int(task_index)},
                        {self._format_sql_value(current_data.pipeline_type)},
                        {self._format_sql_value(current_data.pipeline_identifier)},
                        {self._format_sql_value(current_data.table_name)},
                        {parse_json},
                        {self._format_sql_value(status)},
                        {started_at_sql},
                        {completed_at_sql},
                        {self._format_sql_value(error_message or "")}
                    )
                """
                d.execute_sql(insert_sql, self.schema)

            pipeline_count = len(rows)
            pipeline_word = "pipeline" if pipeline_count == 1 else "pipelines"
            logger.debug(
                f"Logged status change for task {task_index} "
                f"({pipeline_count} {pipeline_word}): {status}"
            )

        except Exception as e:
            logger.error(f"Failed to log status change: {e}")
            # Don't raise - status updates are best-effort

    def get_execution_summary(self, execution_id: str) -> Dict:
        """Get summary statistics for an execution.

        Args:
            execution_id: Execution plan ID

        Returns:
            Dict with counts by status
        """
        safe_eid = _escape(execution_id)
        query = f"""
            SELECT
                status,
                COUNT(*) as count
            FROM {self.plans_view_id}
            WHERE execution_id = '{safe_eid}'
            GROUP BY status
        """

        results = self.destination.execute_sql(query, self.schema)

        summary = {
            "pending": 0,
            "running": 0,
            "completed": 0,
            "failed": 0,
        }

        for row in results:
            summary[row.status] = row.count

        return summary
