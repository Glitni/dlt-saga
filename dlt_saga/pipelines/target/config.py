import re
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Dict, List, Optional, Union

from dlt_saga.historize.config import HistorizeConfig


class ReplaceStrategy(str, Enum):
    TRUNCATE_AND_INSERT = "truncate-and-insert"
    INSERT_FROM_STAGING = "insert-from-staging"
    STAGING_OPTIMIZED = "staging-optimized"


class MergeStrategy(str, Enum):
    DELETE_INSERT = "delete-insert"
    SCD2 = "scd2"
    UPSERT = "upsert"


@dataclass
class TargetConfig:
    """Pipeline destination configuration and loading behavior.

    This class defines:
    1. Destination connection overrides (destination_type, gcp_project_id, dataset_name, location)
    2. IAM/Access control (access)
    3. Loading behavior (write disposition, merge strategy, deduplication, etc.)
    4. Destination hints (partitioning, clustering, column specifications)

    Destination connection fields are per-pipeline overrides to profile defaults.
    The actual connection credentials come from profiles.yml and destination configs.
    """

    # =========================================================================
    # Destination Connection Overrides
    # =========================================================================

    destination_type: str = field(
        default="bigquery",
        metadata={
            "description": "Destination type (currently only BigQuery is supported)",
            "enum": ["bigquery"],
        },
    )

    gcp_project_id: Optional[str] = field(
        default=None,
        metadata={
            "description": "GCP project ID override for BigQuery destination (defaults to profile config)"
        },
    )

    dataset_name: Optional[str] = field(
        default=None,
        metadata={
            "description": "BigQuery dataset name override (defaults to environment-aware naming)",
            "pattern": "^[a-zA-Z0-9_]+$",
        },
    )

    location: str = field(
        default="EU",
        metadata={"description": "BigQuery location/region (defaults to EU)"},
    )

    table_format: str = field(
        default="native",
        metadata={
            "description": "Table format for BigQuery: 'native' for standard BigQuery tables, 'iceberg' for BigLake Iceberg tables",
            "enum": ["native", "iceberg"],
        },
    )

    # =========================================================================
    # IAM / Access Control
    # =========================================================================

    access: Optional[List[str]] = field(
        default=None,
        metadata={
            "description": "BigQuery access control list (only applied in production)"
        },
    )

    # =========================================================================
    # Loading Behavior
    # =========================================================================

    # Write disposition
    write_disposition: str = field(
        default="replace",
        metadata={
            "description": "How to write data to the destination table. Use 'append+historize' to enable historization of appended snapshots. Use 'historize' alone for external data delivery (no ingest).",
            "enum": ["replace", "append", "merge", "append+historize", "historize"],
        },
    )

    # Replace strategy
    replace_strategy: Optional[ReplaceStrategy] = field(
        default=None,
        metadata={
            "description": "Strategy when using write_disposition: replace",
            "enum": ["truncate-and-insert", "insert-from-staging", "staging-optimized"],
        },
    )

    # Merge config
    merge_key: Optional[Union[str, List[str]]] = field(
        default=None,
        metadata={
            "description": "Merge key column(s) (alternative to primary_key)",
            "$ref": "dlt_common.json#/$defs/string_or_array_of_strings",
        },
    )
    merge_strategy: Optional[MergeStrategy] = field(
        default=None,
        metadata={
            "description": "Strategy when using write_disposition: merge",
            "enum": ["delete-insert", "scd2", "upsert"],
        },
    )
    primary_key: Optional[Union[str, List[str]]] = field(
        default=None,
        metadata={
            "description": "Primary key column(s) for merge operations",
            "$ref": "dlt_common.json#/$defs/string_or_array_of_strings",
        },
    )

    # SCD2 specific configuration
    valid_from_column: str = field(
        default="_dlt_valid_from",
        metadata={"description": "Column name for SCD2 valid_from timestamp"},
    )
    valid_to_column: str = field(
        default="_dlt_valid_to",
        metadata={"description": "Column name for SCD2 valid_to timestamp"},
    )
    active_record_timestamp: Optional[str] = field(
        default=None,
        metadata={"description": "Timestamp to use for active records in SCD2"},
    )
    boundary_timestamp: Optional[str] = field(
        default=None,
        metadata={"description": "Boundary timestamp for SCD2 merge operations"},
    )
    row_version_column_name: Optional[str] = field(
        default=None, metadata={"description": "Column name for row version tracking"}
    )

    # Deduplication configuration
    deduplicate: bool = field(
        default=True,
        metadata={"description": "Whether to deduplicate rows based on primary_key"},
    )
    dedup_sort: Optional[str] = field(
        default=None,
        metadata={
            "description": "Sort order for deduplication (asc = oldest wins, desc = newest wins)",
            "enum": ["asc", "desc"],
        },
    )
    dedup_sort_column: Optional[str] = field(
        default=None,
        metadata={"description": "Column to use for deduplication sorting"},
    )

    # Hard delete config
    hard_delete_column: Optional[str] = field(
        default=None,
        metadata={"description": "Column indicating rows should be hard deleted"},
    )

    # BigQuery partitioning and clustering
    partition_column: Optional[str] = field(
        default=None,
        metadata={
            "description": "Column to partition BigQuery table by (must be DATE or TIMESTAMP)"
        },
    )
    cluster_columns: Optional[List[str]] = field(
        default=None,
        metadata={
            "description": "Columns to cluster BigQuery table by (max 4)",
            "maxItems": 4,
        },
    )

    # Additional hints
    columns: Optional[Dict[str, Dict[str, Any]]] = field(
        default=None,
        metadata={
            "description": (
                "Column type hints for explicit schema definition. "
                "Keys can be either the original source column name (e.g., 'Oppty_TargetAmount') "
                "or the dlt-normalized snake_case name (e.g., 'oppty_target_amount'). "
                "dlt normalizes identifiers by splitting CamelCase on word boundaries: "
                "'TargetAmount' becomes 'target_amount', not 'targetamount'. "
                "Using a key that matches neither form (e.g., 'oppty_targetamount') silently creates "
                "a ghost column in the destination with all NULL values."
            ),
            "$ref": "dlt_common.json#/$defs/column_hint",
        },
    )

    # Historization config (only used when write_disposition contains "historize")
    historize: Optional[HistorizeConfig] = field(
        default=None,
        metadata={
            "description": "Historization configuration for SCD2 processing of snapshot data. Only used when write_disposition is 'append+historize' or 'historize'.",
        },
    )

    def __post_init__(self):
        self._validate_dataset_name()
        self._validate_destination_type()
        self._validate_column_identifiers()
        self._normalize_keys()
        self._normalize_enums()
        self._initialize_columns()

    def _validate_dataset_name(self):
        """Validate dataset_name pattern if provided."""
        if self.dataset_name:
            import re

            pattern = r"^[a-zA-Z0-9_]+$"
            if not re.match(pattern, self.dataset_name):
                raise ValueError(
                    f"dataset_name '{self.dataset_name}' must match pattern {pattern}"
                )

    def _validate_column_identifiers(self):
        """Validate SQL identifier format for column name fields."""
        _SQL_IDENT = re.compile(r"^[a-zA-Z_][a-zA-Z0-9_]*$")

        if self.partition_column and not _SQL_IDENT.match(self.partition_column):
            raise ValueError(
                f"partition_column must be a valid SQL identifier, got '{self.partition_column}'"
            )
        if self.cluster_columns:
            if len(self.cluster_columns) > 4:
                raise ValueError(
                    f"cluster_columns supports at most 4 columns, got {len(self.cluster_columns)}"
                )
            for col in self.cluster_columns:
                if not _SQL_IDENT.match(col):
                    raise ValueError(
                        f"cluster_columns contains invalid SQL identifier: '{col}'"
                    )

    def _validate_destination_type(self):
        """Validate destination_type is supported."""
        valid_destinations = ["bigquery"]
        if self.destination_type not in valid_destinations:
            raise ValueError(
                f"destination_type must be one of {valid_destinations}, got '{self.destination_type}'"
            )

    def _normalize_keys(self):
        """Normalize primary_key and merge_key to lists."""
        if isinstance(self.primary_key, str):
            self.primary_key = [self.primary_key]
        if isinstance(self.merge_key, str):
            self.merge_key = [self.merge_key]

    def _normalize_enums(self):
        """Convert string values to enums if needed."""
        if isinstance(self.merge_strategy, str):
            self.merge_strategy = MergeStrategy(self.merge_strategy)
        if isinstance(self.replace_strategy, str):
            self.replace_strategy = ReplaceStrategy(self.replace_strategy)

    def _initialize_columns(self):
        """Initialize columns dict and add dedup/hard_delete hints."""
        if self.columns is None:
            self.columns = {}
        if self.dedup_sort and self.dedup_sort_column:
            self.columns[self.dedup_sort_column] = {"dedup_sort": self.dedup_sort}
        if self.hard_delete_column:
            self.columns[self.hard_delete_column] = {"hard_delete": True}

    def get_dlt_column_hints(self) -> Dict[str, Dict[str, Any]]:
        """Get column hints for dlt, filtering out custom fields like 'default'.

        Returns:
            Column hints dict with only dlt-supported fields
        """
        if not self.columns:
            return {}

        # Custom fields that are NOT part of dlt's column hint schema
        # These are our extensions and should be filtered out before passing to dlt
        black_list_fields = {"default"}

        # Filter out custom fields that dlt doesn't recognize
        dlt_columns = {}
        for col_name, col_config in self.columns.items():
            dlt_columns[col_name] = {
                k: v for k, v in col_config.items() if k not in black_list_fields
            }

        return dlt_columns
