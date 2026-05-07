"""BigQuery destination configuration."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Dict, List, Optional

from dlt_saga.destinations.config import DestinationConfig

if TYPE_CHECKING:
    from dlt_saga.utility.cli.context import ExecutionContext


@dataclass
class BigQueryDestinationConfig(DestinationConfig):
    """BigQuery-specific destination configuration.

    Supports both native BigQuery tables and BigLake Iceberg tables.
    Table format is controlled by the table_format field.
    """

    project_id: str = ""  # Project where datasets/tables live
    billing_project_id: Optional[str] = (
        None  # Project for job execution (defaults to project_id)
    )
    destination_type: str = "bigquery"
    location: str = "EU"
    dataset_name: Optional[str] = None
    dataset_access: Optional[List[str]] = None  # Dataset access control entries
    table_format: str = "native"  # "native" or "iceberg"
    storage_path: Optional[str] = None  # Required for Iceberg tables (gs://bucket/path)

    @property
    def database(self) -> str:
        return self.project_id

    @property
    def job_project_id(self) -> str:
        """Project where BigQuery jobs are executed and billed."""
        return self.billing_project_id or self.project_id

    def __post_init__(self):
        """Validate required fields after initialization."""
        if not self.project_id:
            raise ValueError("project_id is required for BigQuery destination")

        if self.table_format not in ("native", "iceberg"):
            raise ValueError(
                f"table_format must be 'native' or 'iceberg', got '{self.table_format}'"
            )

        if self.table_format == "iceberg" and not self.storage_path:
            raise ValueError(
                "storage_path is required for BigLake Iceberg tables (table_format='iceberg')"
            )

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> BigQueryDestinationConfig:
        """Create BigQuery config from dictionary.

        Args:
            data: Configuration dictionary with keys: project_id, location, dataset_name,
                  dataset_access, table_format, storage_path

        Returns:
            BigQueryDestinationConfig instance
        """
        return cls(
            destination_type="bigquery",
            project_id=data["project_id"],
            billing_project_id=data.get("billing_project_id"),
            location=data.get("location", "EU"),
            dataset_name=data.get("dataset_name"),
            dataset_access=data.get("dataset_access"),
            table_format=data.get("table_format", "native"),
            storage_path=data.get("storage_path"),
        )

    @classmethod
    def from_context(
        cls,
        context: ExecutionContext,
        config_dict: Dict[str, Any],
    ) -> BigQueryDestinationConfig:
        """Resolve BigQuery config from execution context and pipeline config.

        Resolution priority for each field:
            project_id: context (profile) > config_dict > SAGA_DESTINATION_DATABASE env var
            location:   context (profile) > config_dict > "EU"
            dataset_name: already resolved by ConfigSource (e.g., FilePipelineConfig)
            table_format: config_dict > context (profile) > "native"
            storage_path: context (profile) — required for iceberg
            dataset_access: config_dict (from dlt_project.yml)
        """
        from dlt_saga.utility.env import get_env

        project_id = (
            context.get_database()
            or config_dict.get("gcp_project_id")
            or get_env("SAGA_DESTINATION_DATABASE")
        )
        if not project_id:
            raise ValueError(
                "BigQuery project_id must be set via profile (database), "
                "pipeline config (gcp_project_id), or SAGA_DESTINATION_DATABASE env var"
            )

        location = context.get_location() or config_dict.get("location", "EU")
        table_format = config_dict.get("table_format") or context.get_table_format()

        storage_path = None
        if table_format == "iceberg":
            # config_dict["storage_path"] lets the factory override the profile value
            # (used when historize-layer needs a different storage_path than ingest)
            storage_path = config_dict.get("storage_path") or context.get_storage_path()
            if not storage_path:
                raise ValueError(
                    "storage_path must be configured in profile for Iceberg tables"
                )

        billing_project_id = None
        if context.profile_target:
            billing_project_id = context.profile_target.billing_project

        return cls(
            destination_type="bigquery",
            project_id=project_id,
            billing_project_id=billing_project_id,
            location=location,
            dataset_name=config_dict.get("schema_name"),
            dataset_access=config_dict.get("dataset_access"),
            table_format=table_format,
            storage_path=storage_path,
        )
