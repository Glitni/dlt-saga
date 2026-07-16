"""Configuration for the historize command.

Defines HistorizeConfig which controls how snapshot data is historized
into SCD2 tables with _dlt_valid_from/_dlt_valid_to periods.
"""

import re
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

from dlt_saga.utility.filters import filter_field_metadata as _filter_field_metadata

# Bare SQL identifier: letters/underscore start, then letters/digits/underscore.
# Column names interpolated into historize SQL are checked against this so a
# config value can't inject SQL or silently break unquoted interpolation.
_SQL_IDENT = re.compile(r"^[a-zA-Z_][a-zA-Z0-9_]*$")


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
                "Defaults to '_dlt_ingested_at', which is injected automatically for append and replace pipelines."
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

    table_suffix: str = field(
        default="_historized",
        metadata={
            "description": (
                "Suffix appended to the source table name to derive the historized table name. "
                "Defaults to '_historized'. (Legacy alias: output_table_suffix.)"
            )
        },
    )

    schema_name: Optional[str] = field(
        default=None,
        metadata={
            "description": (
                "Schema to write the historized table to. Used directly in prod; "
                "in dev it is composed with the developer sandbox "
                "(<sandbox>_<schema_name>) so a shared config stays isolated per "
                "developer. Defaults to the same schema as the source. "
                "(Legacy alias: output_schema.)"
            )
        },
    )

    table_name: Optional[str] = field(
        default=None,
        metadata={
            "description": (
                "Explicit name for the historized output table. Overrides "
                "table_suffix when set; group-prefixed in dev like the ingest "
                "table. (Legacy alias: output_table.)"
            )
        },
    )

    valid_from_column: str = field(
        default="_dlt_valid_from",
        metadata={
            "description": (
                "Name of the SCD2 valid-from column in the historized table. "
                "Defaults to '_dlt_valid_from'."
            )
        },
    )

    valid_to_column: str = field(
        default="_dlt_valid_to",
        metadata={
            "description": (
                "Name of the SCD2 valid-to column in the historized table. "
                "Defaults to '_dlt_valid_to'."
            )
        },
    )

    is_deleted_column: str = field(
        default="_dlt_is_deleted",
        metadata={
            "description": (
                "Name of the soft-delete marker column in the historized table. "
                "Defaults to '_dlt_is_deleted'."
            )
        },
    )

    partition_column: Optional[str] = field(
        default=None,
        metadata={
            "description": (
                "Column to partition the historized table by. "
                "Defaults to the valid_from_column."
            )
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

    merge_key: Optional[List[str]] = field(
        default=None,
        metadata={
            "description": (
                "Columns that scope deletion detection and gap-driven reappearance "
                "logic — the historize analogue of dlt's SCD2 merge_key. Must be a "
                "subset of primary_key. When set, a key is considered deleted only "
                "if it disappears from a snapshot containing other rows that share "
                "the same merge_key value; snapshots from sibling groups don't "
                "drive deletions or reappearances for this group. Use when the "
                "source unions independently-delivered partitions sharing one "
                "snapshot column (per-instance / per-tenant feeds). Defaults to "
                "None (global scope, current behavior)."
            )
        },
    )

    table_format: Optional[str] = field(
        default=None,
        metadata={
            "description": (
                "Table format for the historized table. Overrides the profile-level "
                "table_format for this pipeline specifically. "
                "Supported values depend on the destination: 'native', 'iceberg' (BigQuery, Databricks), "
                "'delta', 'delta_uniform' (Databricks only). "
                "When omitted, the resolution chain is: pipeline.historize.table_format → "
                "pipeline.table_format → profile.historize.table_format → profile.table_format → 'native'."
            )
        },
    )

    filters: Optional[List[Dict[str, Any]]] = field(
        default=None,
        metadata=_filter_field_metadata(
            "Declarative row filters applied during historize (AND-joined). "
            "Same schema as the top-level filters: block but applied only to "
            "the source read in the historize SQL — independent of any ingest "
            "filter. Useful for partitioning one source table into multiple "
            "tenant-scoped histories (combine with historize.table_name). "
            "Operators: eq (default), ne, in, not_in, is_null, is_not_null, "
            "matches. Path-based filters drill into JSON columns and compare "
            "as strings."
        ),
    )

    description: Optional[str] = field(
        default=None,
        metadata={
            "description": (
                "Table-level description for the historized table. Overrides the "
                "top-level 'description' for the historized table only; inherits "
                "it when omitted. Honored when persist_docs.table is true."
            )
        },
    )

    classification: Optional[List[str]] = field(
        default=None,
        metadata={
            "description": (
                "Table-level classification for the historized table (e.g. "
                "['pii']). Overrides the top-level 'classification' for the "
                "historized table only; inherits it when omitted."
            )
        },
    )

    columns: Optional[Dict[str, Dict[str, Any]]] = field(
        default=None,
        metadata={
            "description": (
                "Per-column description/classification overrides for the "
                "historized table, merged over the top-level 'columns' (override "
                "only the keys you specify; inherit the rest). Use to document or "
                "classify a column differently on the historized table."
            ),
            "$ref": "dlt_common.json#/$defs/column_hint",
        },
    )

    persist_docs: Optional[Dict[str, bool]] = field(
        default=None,
        metadata={
            "description": (
                "Overrides the top-level persist_docs for the historized table "
                "only ({table, columns} — whether to write the table/column "
                "descriptions). Inherits the top-level value when omitted."
            )
        },
    )

    def __post_init__(self):
        if isinstance(self.primary_key, str):
            self.primary_key = [self.primary_key]
        if isinstance(self.classification, str):
            self.classification = [self.classification]
        if isinstance(self.track_columns, str):
            self.track_columns = [self.track_columns]
        if isinstance(self.ignore_columns, str):
            self.ignore_columns = [self.ignore_columns]
        if isinstance(self.cluster_columns, str):
            self.cluster_columns = [self.cluster_columns]
        if isinstance(self.merge_key, str):
            self.merge_key = [self.merge_key]
        # partition_column defaults to the SCD2 valid-from column when not set.
        if not self.partition_column:
            self.partition_column = self.valid_from_column
        self._validate_column_identifiers()

    def _validate_column_identifiers(self):
        """Validate SQL identifier format for column name fields.

        Covers every config-supplied column name interpolated into historize
        SQL. ``primary_key`` is validated separately in ``validate()`` because
        it may still be inherited from the top-level config after construction.
        """
        for attr in (
            "snapshot_column",
            "valid_from_column",
            "valid_to_column",
            "is_deleted_column",
            "partition_column",
        ):
            value = getattr(self, attr)
            if not _SQL_IDENT.match(value):
                raise ValueError(
                    f"{attr} must be a valid SQL identifier, got '{value}'"
                )
        for attr in ("cluster_columns", "merge_key", "track_columns", "ignore_columns"):
            for col in getattr(self, attr) or []:
                if not _SQL_IDENT.match(col):
                    raise ValueError(f"{attr} contains invalid SQL identifier: '{col}'")

    @classmethod
    def from_dict(
        cls,
        historize_dict: Optional[Dict[str, Any]],
        top_level_primary_key: Optional[List[str]] = None,
    ) -> "HistorizeConfig":
        """Create HistorizeConfig from a config dictionary.

        Args:
            historize_dict: The `historize:` section from pipeline YAML. May be
                ``None`` when the key is present with an empty/null value
                (an empty ``historize:`` block in YAML parses to ``None``).
            top_level_primary_key: Primary key from top-level config, used as fallback

        Returns:
            HistorizeConfig instance

        Raises:
            ValueError: If primary_key is not set and cannot be inherited
        """
        # An empty ``historize:`` block parses to None; treat it as no overrides.
        historize_dict = historize_dict or {}
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

        for pk in self.primary_key:
            if not _SQL_IDENT.match(pk):
                raise ValueError(f"primary_key contains invalid SQL identifier: '{pk}'")

        if self.track_columns is not None and len(self.track_columns) == 0:
            raise ValueError(
                "track_columns must be either omitted (track all columns) or a "
                "non-empty list. To track specific columns, list them explicitly."
            )

        if self.merge_key:
            pk_set = set(self.primary_key or [])
            extras = [c for c in self.merge_key if c not in pk_set]
            if extras:
                raise ValueError(
                    f"merge_key columns {extras!r} must be a subset of "
                    f"primary_key {list(self.primary_key or [])!r}. "
                    "Scoping deletion detection by a non-PK column would let two "
                    "distinct keys collide on the scope group and produce "
                    "inconsistent deletion timing."
                )

        if config_dict:
            source_table = config_dict.get("source_table")
            source_schema = config_dict.get("source_schema")
            if source_table and not source_schema:
                raise ValueError(
                    "External source requires both source_schema and source_table. "
                    "source_schema is missing."
                )
