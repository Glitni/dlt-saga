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

    # Snapshot identification
    snapshot_column: str = "_dlt_ingested_at"

    # Primary key for change detection (inherited from top-level if None)
    primary_key: Optional[List[str]] = None

    # Columns to exclude from change detection (beyond system columns)
    exclude_columns: List[str] = field(default_factory=list)

    # Output table naming
    output_table_suffix: str = "_historized"
    output_dataset: Optional[str] = None
    output_table: Optional[str] = None

    # Historized table hints
    partition_column: str = "_dlt_valid_from"
    cluster_columns: Optional[List[str]] = None

    # Deletion tracking
    track_deletions: bool = True

    def __post_init__(self):
        if isinstance(self.primary_key, str):
            self.primary_key = [self.primary_key]
        if isinstance(self.exclude_columns, str):
            self.exclude_columns = [self.exclude_columns]
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
