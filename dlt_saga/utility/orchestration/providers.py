"""Orchestration provider interface and implementations.

Providers handle the mechanism of triggering worker execution after an
execution plan has been created and persisted. Plan creation and status
tracking remain in ExecutionPlanManager -- providers only handle the
fan-out to workers.
"""

import json
import logging
import os
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import TYPE_CHECKING, Optional

from dlt_saga.utility.env import get_env

if TYPE_CHECKING:
    from dlt_saga.project_config import OrchestrationConfig

logger = logging.getLogger(__name__)


@dataclass
class TriggerResult:
    """Result of triggering worker execution."""

    execution_reference: str


class OrchestrationProvider(ABC):
    """Interface for triggering distributed worker execution.

    Providers are responsible for spawning workers that execute tasks from
    an execution plan. The plan itself is already persisted before
    ``trigger()`` is called.
    """

    @abstractmethod
    def trigger(
        self,
        execution_id: str,
        task_count: int,
        command: str = "ingest",
        debug: bool = False,
    ) -> TriggerResult:
        """Trigger workers to execute an execution plan.

        Args:
            execution_id: ID of the persisted execution plan.
            task_count: Number of parallel tasks to spawn.
            command: Worker command (ingest, historize, run).
            debug: Enable debug logging in workers.

        Returns:
            TriggerResult with a reference to the triggered execution.
        """
        ...


class CloudRunProvider(OrchestrationProvider):
    """Triggers Cloud Run Jobs for distributed execution.

    Wraps :class:`CloudRunJobTrigger` to conform to the
    :class:`OrchestrationProvider` interface.
    """

    def __init__(
        self,
        project_id: Optional[str] = None,
        region: Optional[str] = None,
        job_name: Optional[str] = None,
    ):
        self.project_id = project_id or get_env("SAGA_DESTINATION_DATABASE")
        self.region = os.getenv("CLOUD_RUN_REGION") or region or "europe-north1"
        # CLOUD_RUN_JOB takes precedence — inside Cloud Run each job must
        # re-trigger itself.  The config value is the fallback for local
        # ad-hoc runs where the env var isn't set.
        self.job_name = os.getenv("CLOUD_RUN_JOB") or job_name

        if not self.project_id:
            raise ValueError(
                "project_id must be provided or SAGA_DESTINATION_DATABASE must be set"
            )

        if not self.job_name:
            raise ValueError(
                "job_name must be provided, set CLOUD_RUN_JOB, "
                "or configure orchestration.job_name in saga_project.yml"
            )

    def trigger(
        self,
        execution_id: str,
        task_count: int,
        command: str = "ingest",
        debug: bool = False,
    ) -> TriggerResult:
        from dlt_saga.utility.orchestration.cloud_run_trigger import CloudRunJobTrigger

        trigger = CloudRunJobTrigger(
            project_id=self.project_id,
            region=self.region,
            job_name=self.job_name,
        )
        execution_name = trigger.trigger_execution(
            execution_id=execution_id,
            task_count=task_count,
            debug_logging=debug,
            worker_command=command,
        )
        return TriggerResult(execution_reference=execution_name)


_KNOWN_PROVIDERS = {"cloud_run", "stdout"}


def resolve_provider(
    config: Optional["OrchestrationConfig"] = None,
    project_id: Optional[str] = None,
) -> Optional[OrchestrationProvider]:
    """Resolve an orchestration provider from configuration.

    Returns the provider specified in ``orchestration.provider`` from
    ``saga_project.yml``, or ``None`` when the section is absent or
    ``provider`` is not set.

    Args:
        config: ``orchestration:`` section from ``saga_project.yml``.
        project_id: GCP project ID (passed to CloudRunProvider).

    Returns:
        An :class:`OrchestrationProvider` instance, or ``None`` when
        no provider is configured.

    Raises:
        ValueError: If the configured provider name is unknown.
    """
    provider_name = config.provider if config else None

    if not provider_name:
        return None

    if provider_name not in _KNOWN_PROVIDERS:
        raise ValueError(
            f"Unknown orchestration provider: '{provider_name}'. "
            f"Available: {', '.join(sorted(_KNOWN_PROVIDERS))}"
        )
    if provider_name == "cloud_run":
        return CloudRunProvider(
            project_id=project_id,
            region=config.region if config else None,
            job_name=config.job_name if config else None,
        )
    if provider_name == "stdout":
        return StdoutProvider()

    return None  # unreachable, but satisfies type checker


class StdoutProvider(OrchestrationProvider):
    """Outputs execution plan metadata to stdout for external orchestrators.

    Use this when an external system (Airflow, Cloud Workflows, etc.)
    handles the worker fan-out.  The plan is already persisted; this
    provider reports what was created so the external system can trigger
    ``saga worker`` tasks.
    """

    def trigger(
        self,
        execution_id: str,
        task_count: int,
        command: str = "ingest",
        debug: bool = False,
    ) -> TriggerResult:
        output = {
            "execution_id": execution_id,
            "task_count": task_count,
            "command": command,
        }
        # JSON goes to stdout; status messages go to stderr via logger
        print(json.dumps(output))
        logger.info(
            "Execution plan ready for external orchestrator: "
            f"{task_count} task(s), command={command}"
        )
        return TriggerResult(execution_reference=f"stdout:{execution_id}")
