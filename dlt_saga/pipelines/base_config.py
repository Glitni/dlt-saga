"""Base configuration class for all pipeline sources.

This class defines generic fields that are common to all pipeline types
and control pipeline execution and source extraction behavior.
"""

from dataclasses import dataclass, field
from typing import List, Optional


@dataclass
class BaseConfig:
    """Base configuration for all pipeline sources.

    All source-specific config classes (GSheetsConfig, FilesystemConfig, etc.)
    should inherit from this class. This ensures type safety and validation
    for generic pipeline configuration fields.

    These fields control:
    1. Pipeline execution control (enabled, tags, task_group)
    2. Source extraction behavior (incremental loading)

    Destination-related fields (destination_type, gcp_project_id, dataset_name, etc.)
    belong in TargetConfig since they control how/where data is loaded to the destination.
    """

    # =========================================================================
    # Pipeline Execution Control
    # =========================================================================

    enabled: bool = field(
        default=True, metadata={"description": "Whether this pipeline is enabled"}
    )

    tags: List[str] = field(
        default_factory=list,
        metadata={
            "description": "Tags for filtering and scheduling pipelines. Simple tags (e.g., 'daily') run at all times. Schedule tags with values (e.g., 'hourly: [1, 10]', 'daily: [monday]') run only at specified times.",
            "$ref": "dlt_common.json#/$defs/tags",
        },
    )

    task_group: Optional[str] = field(
        default=None,
        metadata={
            "description": "Optional group name for pipelines that should run together in the same Cloud Run task"
        },
    )

    adapter: Optional[str] = field(
        default=None,
        metadata={
            "description": "Pipeline adapter to use. Format: <namespace>.<path> (e.g., 'dlt_saga.api.genesyscloud', 'local.api.my_custom'). The namespace maps to a package, and the path resolves to a pipeline.py module within it. If omitted, falls back to folder-structure-based resolution.",
        },
    )

    # =========================================================================
    # Source Extraction Behavior (Incremental Loading)
    # =========================================================================

    incremental: bool = field(
        default=False,
        metadata={
            "description": "Enable incremental loading (track state and load only new/changed data)"
        },
    )

    incremental_column: Optional[str] = field(
        default=None,
        metadata={
            "description": "Column name to use for incremental loading (e.g., 'updated_at', 'partitiondate', 'id')"
        },
    )

    initial_value: Optional[str] = field(
        default=None,
        metadata={
            "description": "Initial value for incremental loading on first run (e.g., start date in YYYY-MM-DD format, or numeric ID)"
        },
    )

    start_value_override: Optional[str] = field(
        default=None,
        metadata={
            "description": "Override start value for incremental pipelines (format depends on pipeline type, default: YYYY-MM-DD)",
        },
    )

    end_value_override: Optional[str] = field(
        default=None,
        metadata={
            "description": "Override end value for incremental pipelines (format depends on pipeline type, default: YYYY-MM-DD)",
        },
    )

    def __post_init__(self):
        """Validate configuration after initialization."""
        # Convert single tag string to list
        if isinstance(self.tags, str):
            self.tags = [self.tags]
        elif self.tags is None:
            self.tags = []

        # Validate incremental configuration
        if self.incremental and not self.incremental_column:
            raise ValueError("incremental_column is required when incremental=True")
