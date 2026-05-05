"""Configuration for the historize command.

Defines HistorizeConfig which controls how snapshot data is historized
into SCD2 tables with _dlt_valid_from/_dlt_valid_to periods.
"""

import re
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional


@dataclass
class HistorizeConfig:
    """Configuration for historizing a pipeline's raw snapshot data into SCD2.

    This config is nested under the `historize:` key in pipeline YAML files.
    For pipelines that also run ingest, primary_key is inherited from the
    top-level config if not specified here.

    For external-delivery pipelines (write_disposition: "historize"), source
    location is specified via top-level source_database, source_schema,
    source_table fields (shared with ingest config).
    """

    snapshot_column: str = field(
        default="_dlt_ingested_at",
        metadata={
            "description": (
                "Column that identifies each snapshot in the source table. "
                "Rows sharing the same value are treated as one point-in-time view. "
                "Defaults to '_dlt_ingested_at', which is injected automatically for append pipelines."
            )
        },
    )

    primary_key: Optional[List[str]] = field(
        default=None,
        metadata={
            "description": (
                "Column(s) that uniquely identify a business entity across snapshots. "
                "Inherited from the top-level pipeline primary_key if not set here."
            )
        },
    )

    track_columns: Optional[List[str]] = field(
        default=None,
        metadata={
            "description": (
                "Columns to include in change detection. When set, only these columns are hashed; "
                "all other non-PK columns are ignored for change detection but still appear in the output. "
                "Applied before ignore_columns."
            )
        },
    )

    ignore_columns: List[str] = field(
        default_factory=list,
        metadata={
            "description": (
                "Columns to exclude from change detection. These columns are still present in the "
                "output table but do not trigger a new SCD2 record when their value changes. "
                "Applied after track_columns when both are set."
            )
        },
    )

    output_table_suffix: str = field(
        default="_historized",
        metadata={
            "description": (
                "Suffix appended to the source table name to derive the historized table name. "
                "Defaults to '_historized'."
            )
        },
    )

    output_dataset: Optional[str] = field(
        default=None,
        metadata={
            "description": "Dataset to write the historized table to. Defaults to the same dataset as the source."
        },
    )

    output_table: Optional[str] = field(
        default=None,
        metadata={
            "description": "Explicit name for the historized output table. Overrides output_table_suffix when set."
        },
    )

    partition_column: str = field(
        default="_dlt_valid_from",
        metadata={
            "description": "Column to partition the historized table by. Defaults to '_dlt_valid_from'."
        },
    )

    cluster_columns: Optional[List[str]] = field(
        default=None,
        metadata={"description": "Columns to cluster the historized table by (max 4)."},
    )

    track_deletions: bool = field(
        default=True,
        metadata={
            "description": (
                "When True, rows that disappear from the source produce a deletion marker row "
                "(_dlt_is_deleted=True) in the historized table. "
                "When False, only value changes are tracked."
            )
        },
    )

    def __post_init__(self):
        if isinstance(self.primary_key, str):
            self.primary_key = [self.primary_key]
        if isinstance(self.track_columns, str):
            self.track_columns = [self.track_columns]
        if isinstance(self.ignore_columns, str):
            self.ignore_columns = [self.ignore_columns]
        if isinstance(self.cluster_columns, str):
            self.cluster_columns = [self.cluster_columns]
        self._validate_column_identifiers()

    def _validate_column_identifiers(self):
        """Validate SQL identifier format for column name fields."""
        _SQL_IDENT = re.compile(r"^[a-zA-Z_][a-zA-Z0-9_]*$")

        if not _SQL_IDENT.match(self.partition_column):
            raise ValueError(
                f"partition_column must be a valid SQL identifier, got '{self.partition_column}'"
            )
        if self.cluster_columns:
            for col in self.cluster_columns:
                if not _SQL_IDENT.match(col):
                    raise ValueError(
                        f"cluster_columns contains invalid SQL identifier: '{col}'"
                    )

    @classmethod
    def from_dict(
        cls,
        historize_dict: Dict[str, Any],
        top_level_primary_key: Optional[List[str]] = None,
    ) -> "HistorizeConfig":
        """Create HistorizeConfig from a config dictionary.

        Args:
            historize_dict: The `historize:` section from pipeline YAML
            top_level_primary_key: Primary key from top-level config, used as fallback

        Returns:
            HistorizeConfig instance

        Raises:
            ValueError: If primary_key is not set and cannot be inherited
        """
        # Filter to only known fields to avoid passing unexpected keys
        known_fields = {f.name for f in cls.__dataclass_fields__.values()}
        filtered = {k: v for k, v in historize_dict.items() if k in known_fields}

        config = cls(**filtered)

        # Inherit primary_key from top-level if not set in historize section
        if config.primary_key is None and top_level_primary_key is not None:
            pk = top_level_primary_key
            if isinstance(pk, str):
                pk = [pk]
            config.primary_key = pk

        return config

    def validate(self, config_dict: Optional[Dict[str, Any]] = None) -> None:
        """Validate the configuration.

        Args:
            config_dict: Top-level pipeline config dict for checking source fields

        Raises:
            ValueError: If configuration is invalid
        """
        if not self.primary_key:
            raise ValueError(
                "historize requires a primary_key. Set it in the historize section "
                "or as a top-level primary_key in the pipeline config."
            )

        if config_dict:
            source_table = config_dict.get("source_table")
            source_schema = config_dict.get("source_schema")
            if source_table and not source_schema:
                raise ValueError(
                    "External source requires both source_schema and source_table. "
                    "source_schema is missing."
                )
