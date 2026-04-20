"""Cloud Run Jobs triggering utility for orchestrator pattern.

This module handles triggering Cloud Run Jobs with specific task counts
for distributed pipeline execution.
"""

import logging
import os
from typing import Optional

from dlt_saga.utility.cli.logging import YELLOW, colorize
from dlt_saga.utility.env import get_env

logger = logging.getLogger(__name__)


class CloudRunJobTrigger:
    """Triggers Cloud Run Jobs for distributed execution.

    Used by the orchestrator to spawn worker tasks that execute pipelines.
    """

    def __init__(
        self,
        project_id: str,
        region: str = "europe-north1",
        job_name: str = "dlt-saga-ingest",
    ):
        """Initialize Cloud Run job trigger.

        Args:
            project_id: GCP project ID
            region: Cloud Run region (default: europe-north1)
            job_name: Name of the Cloud Run Job to trigger
        """
        from dlt_saga.utility.optional_deps import require_optional

        require_optional("google.cloud.run_v2", "Cloud Run orchestration")
        from google.cloud import run_v2  # type: ignore[attr-defined]

        self._run_v2 = run_v2
        self.project_id = project_id
        self.region = region
        self.job_name = job_name
        self.client = run_v2.JobsClient()

    def trigger_execution(
        self,
        execution_id: str,
        task_count: int,
        debug_logging: bool = False,
        worker_command: str = "ingest",
    ) -> str:
        """Trigger a Cloud Run Job execution with specified task count.

        Args:
            execution_id: Execution plan ID (passed to workers via env var)
            task_count: Number of parallel tasks to spawn
            debug_logging: Enable debug logging in worker tasks

        Returns:
            execution_name: Full resource name of the created execution
        """
        run_v2 = self._run_v2

        logger.info(
            f"Triggering Cloud Run Job {self.job_name} with {task_count} tasks for execution {execution_id}"
        )

        # Build job resource name
        job_path = (
            f"projects/{self.project_id}/locations/{self.region}/jobs/{self.job_name}"
        )

        # Prepare environment variable overrides
        env_vars = [
            run_v2.EnvVar(name="SAGA_WORKER_MODE", value="true"),
            run_v2.EnvVar(name="SAGA_EXECUTION_ID", value=execution_id),
            run_v2.EnvVar(name="SAGA_WORKER_COMMAND", value=worker_command),
        ]

        # Add debug logging flag if enabled
        if debug_logging:
            env_vars.append(run_v2.EnvVar(name="SAGA_DEBUG_LOGGING", value="true"))

        # Create container overrides with environment variables
        container_overrides = [
            run_v2.RunJobRequest.Overrides.ContainerOverride(env=env_vars)
        ]

        # Create overrides with task count and environment variables
        overrides = run_v2.RunJobRequest.Overrides(
            container_overrides=container_overrides,
            task_count=task_count,
        )

        # Prepare execution request with overrides
        request = run_v2.RunJobRequest(
            name=job_path,
            overrides=overrides,
        )

        # Trigger the execution (don't wait for completion)
        operation = self.client.run_job(request=request)

        # Get the execution metadata without waiting for completion
        # The operation metadata contains the execution name
        execution_name = operation.metadata.name

        logger.info(f"Triggered execution: {execution_name}")
        execution_url = f"https://console.cloud.google.com/run/jobs/details/{self.region}/{self.job_name}/executions?project={self.project_id}"
        logger.info(f"Monitor execution status at: {colorize(execution_url, YELLOW)}")

        return execution_name


def trigger_distributed_execution(
    execution_id: str,
    task_count: int,
    project_id: Optional[str] = None,
    region: Optional[str] = None,
    job_name: Optional[str] = None,
    debug_logging: bool = False,
    worker_command: str = "ingest",
) -> str:
    """Convenience function to trigger a distributed execution.

    Args:
        execution_id: Execution plan ID
        task_count: Number of tasks to spawn
        project_id: GCP project (defaults to SAGA_DESTINATION_DATABASE env var)
        region: Cloud Run region (defaults to CLOUD_RUN_REGION env var or europe-north1)
        job_name: Job name (defaults to CLOUD_RUN_JOB env var, then fallback to dlt-saga-ingest-daily)
        debug_logging: Enable debug logging in worker tasks
        worker_command: Command for workers to execute (ingest, historize, or run)

    Returns:
        execution_name: Full resource name of the created execution
    """
    # Get defaults from environment
    project_id = project_id or get_env("SAGA_DESTINATION_DATABASE")
    region = os.getenv("CLOUD_RUN_REGION") or region or "europe-north1"
    # CLOUD_RUN_JOB takes precedence — inside Cloud Run each job must
    # re-trigger itself.  The passed value is the fallback for local runs.
    job_name = os.getenv("CLOUD_RUN_JOB") or job_name

    if not project_id:
        raise ValueError(
            "project_id must be provided or SAGA_DESTINATION_DATABASE must be set"
        )

    if not job_name:
        raise ValueError(
            "job_name must be provided, set CLOUD_RUN_JOB, "
            "or configure orchestration.job_name in saga_project.yml"
        )

    trigger = CloudRunJobTrigger(
        project_id=project_id,
        region=region,
        job_name=job_name,
    )

    return trigger.trigger_execution(
        execution_id, task_count, debug_logging, worker_command
    )
