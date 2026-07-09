"""BigQuery destination configuration."""

from __future__ import annotations

from dataclasses import dataclass, field
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

    # BigQuery's idiomatic aliases for generic profile keys: `project_id`/
    # `project` for `database`, and `dataset` (BigQuery's term) for `schema`.
    PROFILE_KEY_ALIASES = {
        "database": ("project_id", "project"),
        "schema": ("dataset",),
    }

    project_id: str = ""  # Project where datasets/tables live (profile key: database)
    # Project for job execution (defaults to project_id). profile_key maps the
    # runtime field name to the profile YAML key, which differs here.
    billing_project_id: Optional[str] = field(
        default=None,
        metadata={
            "profile_field": True,
            "profile_key": "billing_project",
            "description": (
                "GCP project used for job execution / billing on saga-issued queries "
                "(load-info inserts, historize SQL, native-load SQL, IAM sync). "
                "Does NOT apply to dlt-internal jobs (extract/normalize/load), which "
                "always bill to 'database'. BigQuery-specific, defaults to 'database'."
            ),
        },
    )
    destination_type: str = "bigquery"
    location: str = "EU"
    dataset_name: Optional[str] = None  # profile key: schema
    schema_access: Optional[List[str]] = None  # pipeline-level, not a profile key
    table_format: str = "native"  # "native" or "iceberg" (generic profile key)
    storage_path: Optional[str] = field(
        default=None,
        metadata={
            "profile_field": True,
            "description": "Cloud storage path for Iceberg tables (e.g., 'gs://bucket/path')",
        },
    )
    # Partition expiration in days. Maps to time_partitioning.expiration_ms on
    # native BigQuery tables. Resolution: pipeline config > profile
    # destination_config > None (no expiration). Has no effect on Iceberg
    # tables, which don't expose a time_partitioning option.
    partition_expiration_days: Optional[int] = field(
        default=None,
        metadata={
            "profile_field": True,
            "minimum": 1,
            "description": (
                "BigQuery only. Default partition expiration (in days) for partitioned "
                "tables created by pipelines targeting this profile. Pipeline-level "
                "value overrides this. Honored on CREATE TABLE and reconciled (ALTER) "
                "on every subsequent run. No effect on Iceberg or non-BigQuery targets."
            ),
        },
    )

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

        if (
            self.partition_expiration_days is not None
            and self.partition_expiration_days < 1
        ):
            raise ValueError(
                f"partition_expiration_days must be >= 1, got {self.partition_expiration_days}"
            )

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> BigQueryDestinationConfig:
        """Create BigQuery config from dictionary.

        Args:
            data: Configuration dictionary with keys: project_id, location, dataset_name,
                  schema_access (alias: dataset_access), table_format, storage_path

        Returns:
            BigQueryDestinationConfig instance
        """
        return cls(
            destination_type="bigquery",
            project_id=data.get("project_id"),
            billing_project_id=data.get("billing_project_id"),
            location=data.get("location", "EU"),
            dataset_name=data.get("dataset_name"),
            schema_access=data.get("schema_access") or data.get("dataset_access"),
            table_format=data.get("table_format", "native"),
            storage_path=data.get("storage_path"),
            partition_expiration_days=data.get("partition_expiration_days"),
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
            schema_access: config_dict (from dlt_project.yml)
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

        # Resolution: pipeline config overrides profile destination_config, which
        # acts as the destination-level default.
        partition_expiration_days = config_dict.get("partition_expiration_days")
        if partition_expiration_days is None and context.profile_target:
            partition_expiration_days = context.profile_target.destination_config.get(
                "partition_expiration_days"
            )

        return cls(
            destination_type="bigquery",
            project_id=project_id,
            billing_project_id=billing_project_id,
            location=location,
            dataset_name=config_dict.get("schema_name"),
            schema_access=config_dict.get("schema_access")
            or config_dict.get("dataset_access"),
            table_format=table_format,
            storage_path=storage_path,
            partition_expiration_days=partition_expiration_days,
        )
