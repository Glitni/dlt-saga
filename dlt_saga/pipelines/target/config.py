import re
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Dict, List, Optional, Union

from dlt_saga.historize.config import HistorizeConfig
from dlt_saga.utility.column_docs import compose_description
from dlt_saga.utility.secrets.redaction import redact


@dataclass
class PersistDocs:
    """Controls whether declared docs are written to the destination.

    Mirrors dbt's ``persist_docs``, with two differences: the sub-key is
    ``table`` rather than dbt's ``relation`` (saga has no table/view
    polymorphism — every output is a table), and the defaults differ per key.

    ``table`` defaults **on**: a table description is a single, O(1) write that
    every destination handles cheaply. ``columns`` defaults **off**: per-column
    reconciliation is O(columns) and, on destinations without a batch
    column-comment mechanism (e.g. Databricks), a per-run cost — so it's opt-in.
    Enable it per project via ``+persist_docs: {columns: true}`` in
    ``saga_project.yml`` (or per pipeline). Table-level classification still
    applies by default; only the per-column layer is opt-in.
    """

    table: bool = field(
        default=True,
        metadata={
            "description": (
                "Persist the table-level description (and classification) to the "
                "destination. Defaults to true — a single cheap write."
            )
        },
    )
    columns: bool = field(
        default=False,
        metadata={
            "description": (
                "Persist column descriptions (including encoded classification) "
                "to the destination. Defaults to false — per-column writes are "
                "O(columns) and a per-run cost on destinations without batch "
                "column-comment DDL; enable per project/pipeline to opt in."
            )
        },
    )

    @classmethod
    def from_value(cls, value: Any) -> "PersistDocs":
        """Normalize a config value (None / bool / dict / instance) to PersistDocs."""
        if value is None:
            return cls()
        if isinstance(value, PersistDocs):
            return value
        if isinstance(value, bool):
            return cls(table=value, columns=value)
        if isinstance(value, dict):
            unknown = set(value) - {"table", "columns"}
            if unknown:
                raise ValueError(
                    f"persist_docs has unknown key(s) {sorted(unknown)}; "
                    "allowed keys are 'table' and 'columns'"
                )
            return cls(
                table=bool(value.get("table", True)),
                columns=bool(value.get("columns", False)),
            )
        raise ValueError(
            "persist_docs must be a bool or a mapping of {table, columns}, "
            f"got {type(value).__name__}"
        )


class ReplaceStrategy(str, Enum):
    TRUNCATE_AND_INSERT = "truncate-and-insert"
    INSERT_FROM_STAGING = "insert-from-staging"
    STAGING_OPTIMIZED = "staging-optimized"


class MergeStrategy(str, Enum):
    DELETE_INSERT = "delete-insert"
    SCD2 = "scd2"
    UPSERT = "upsert"
    INSERT_ONLY = "insert-only"


@dataclass
class TargetConfig:
    """Pipeline destination configuration and loading behavior.

    This class defines:
    1. Destination connection overrides (destination_type, gcp_project_id, schema_name, location)
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
            "description": (
                "Destination type. Usually inherited from the active profile; "
                "override per pipeline only when it differs."
            ),
            "enum": ["bigquery", "databricks", "duckdb"],
        },
    )

    gcp_project_id: Optional[str] = field(
        default=None,
        metadata={
            "description": "GCP project ID override for BigQuery destination (defaults to profile config)"
        },
    )

    schema_name: Optional[str] = field(
        default=None,
        metadata={
            "description": (
                "Schema/dataset name override for this pipeline. Used directly "
                "in prod; in dev it is composed with the developer sandbox "
                "(<sandbox>_<schema_name>) so a shared config stays isolated "
                "per developer. Defaults to environment-aware naming "
                "(dlt_<group> in prod, the dev schema otherwise)."
            ),
            "pattern": "^[a-zA-Z0-9_]+$",
        },
    )

    table_name: Optional[str] = field(
        default=None,
        metadata={
            "description": (
                "Table name override for this pipeline. Used directly in prod; "
                "in dev it is group-prefixed (<group>__<table_name>) to stay "
                "disambiguated within the shared dev schema. Defaults to "
                "environment-aware naming derived from the config path."
            ),
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
            "description": (
                "Table format. BigQuery: 'native' (standard) or 'iceberg' "
                "(BigLake, requires storage_path). Databricks: 'native'/'delta' "
                "(equivalent), 'iceberg', or 'delta_uniform'. Valid values are "
                "destination-specific."
            ),
            "enum": ["native", "iceberg", "delta", "delta_uniform"],
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
            "description": "How to write data to the destination table. Use 'append+historize' to enable historization of appended snapshots. Use 'historize' alone for external data delivery (no ingest). 'replace+historize' is native_load-only (bulk full-replace + SCD2 layer).",
            "enum": [
                "replace",
                "append",
                "merge",
                "append+historize",
                "historize",
                "replace+historize",
            ],
        },
    )

    # Databricks-only loader selection
    insert_api: Optional[str] = field(
        default=None,
        metadata={
            "description": (
                "Databricks-only: alternative insert API. 'zerobus' uses the "
                "Databricks Zerobus SDK for low-latency append loading into Delta "
                "tables; 'copy_into' uses the default COPY INTO loader. "
                "'zerobus' requires write_disposition 'append' or 'append+historize'. "
                "Ignored on non-Databricks destinations."
            ),
            "enum": ["zerobus", "copy_into"],
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
            "description": (
                "Strategy when using write_disposition: merge. "
                "'insert-only' performs idempotent, key-based appending: rows whose "
                "primary_key already exists in the target are skipped; existing rows are "
                "never updated or deleted. Requires primary_key; merge_key is not supported."
            ),
            "enum": ["delete-insert", "scd2", "upsert", "insert-only"],
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
    partition_expiration_days: Optional[int] = field(
        default=None,
        metadata={
            "description": (
                "BigQuery only. Partition expiration in days; maps to "
                "time_partitioning.expiration_ms on the created table. Honored "
                "on first CREATE TABLE (via bigquery_adapter for dlt-managed "
                "pipelines and an OPTIONS clause for native_load CTAS) and "
                "reconciled on every subsequent run — changing or unsetting "
                "the value emits ALTER TABLE ... SET OPTIONS against the "
                "existing table. Per-pipeline value overrides the profile "
                "default. Has no effect on Iceberg tables or non-BigQuery "
                "destinations (silently ignored)."
            ),
            "minimum": 1,
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
                "a ghost column in the destination with all NULL values. "
                "Each column also accepts a 'description' (str) written to the "
                "destination column, and 'classification' (list of strings) for "
                "lightweight data classification (e.g. ['pii']) encoded into the "
                "column description. Both are honored when persist_docs.columns is "
                "true. Note: 'classification' is data governance, distinct from the "
                "top-level 'tags' used for pipeline selection."
            ),
            "$ref": "dlt_common.json#/$defs/column_hint",
        },
    )

    # Documentation
    description: Optional[str] = field(
        default=None,
        metadata={
            "description": (
                "Table-level description written to the destination table. "
                "Overrides any pipeline-generated description. Honored when "
                "persist_docs.table is true."
            )
        },
    )
    classification: Optional[List[str]] = field(
        default=None,
        metadata={
            "description": (
                "Table-level data-classification labels (e.g. ['pii', "
                "'confidential']) encoded into the table description. Honored "
                "when persist_docs.table is true. Data governance, distinct from "
                "the top-level 'tags' used for pipeline selection."
            )
        },
    )
    persist_docs: PersistDocs = field(
        default_factory=PersistDocs,
        metadata={
            "description": (
                "dbt-style toggle controlling whether declared table/column docs "
                "are written to the destination. 'table' defaults to true; "
                "'columns' defaults to false (per-column writes are O(columns))."
            )
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
        self.persist_docs = PersistDocs.from_value(self.persist_docs)
        self._validate_schema_name()
        self._validate_table_name()
        self._validate_destination_type()
        self._validate_column_identifiers()
        self._validate_insert_api()
        self._normalize_keys()
        self._normalize_enums()
        self._validate_merge_strategy()
        self._initialize_columns()
        self._validate_classification()

    def _validate_schema_name(self):
        """Validate the schema_name pattern if provided."""
        import re

        pattern = r"^[a-zA-Z0-9_]+$"
        if self.schema_name and not re.match(pattern, self.schema_name):
            raise ValueError(
                f"schema_name '{self.schema_name}' must match pattern {pattern}"
            )

    def _validate_table_name(self):
        """Validate the table_name pattern if provided."""
        import re

        pattern = r"^[a-zA-Z0-9_]+$"
        if self.table_name and not re.match(pattern, self.table_name):
            raise ValueError(
                f"table_name '{self.table_name}' must match pattern {pattern}"
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
        valid_destinations = ["bigquery", "databricks", "duckdb"]
        if self.destination_type not in valid_destinations:
            raise ValueError(
                f"destination_type must be one of {valid_destinations}, got '{self.destination_type}'"
            )

    def _validate_merge_strategy(self):
        """Validate constraints for specific merge strategies.

        The ``insert-only`` strategy requires ``primary_key`` and explicitly
        does not support ``merge_key`` (dlt limitation).
        """
        if self.merge_strategy != MergeStrategy.INSERT_ONLY:
            return

        if not self.primary_key:
            raise ValueError(
                "merge_strategy='insert-only' requires primary_key. "
                "Insert-only inserts rows whose primary_key does not already "
                "exist in the target; without a primary_key dlt cannot detect duplicates."
            )

        if self.merge_key:
            raise ValueError(
                "merge_strategy='insert-only' does not support merge_key "
                "(dlt limitation). Use primary_key only."
            )

    def _validate_insert_api(self):
        """Validate insert_api value and write-disposition compatibility.

        Zerobus is an append-only Databricks loader and requires the
        ingest path to be in append mode. Both ``append`` and
        ``append+historize`` qualify (the +historize suffix only affects
        our SCD2 layer, not the underlying dlt write disposition).
        """
        if self.insert_api is None:
            return

        valid = {"zerobus", "copy_into"}
        if self.insert_api not in valid:
            raise ValueError(
                f"insert_api must be one of {sorted(valid)} or unset, "
                f"got '{self.insert_api}'"
            )

        if self.insert_api == "zerobus":
            base = (self.write_disposition or "").split("+", 1)[0]
            if base != "append":
                raise ValueError(
                    "insert_api='zerobus' requires write_disposition 'append' "
                    f"or 'append+historize' (Zerobus is append-only); "
                    f"got write_disposition='{self.write_disposition}'."
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

    # Classification labels are encoded into the description, so they must not
    # contain the block's delimiters (``, ; = ]``) or the encoding would be
    # ambiguous to parse back.
    _CLASSIFICATION_RE = re.compile(r"^[A-Za-z0-9_.:\-]+$")

    def _validate_classification_values(self, values: Any, context: str) -> None:
        """Validate a classification list is strings using the allowed charset."""
        if values is None:
            return
        if not isinstance(values, list) or not all(isinstance(v, str) for v in values):
            raise ValueError(f"{context} must be a list of strings, got {values!r}")
        for value in values:
            if not self._CLASSIFICATION_RE.match(value):
                raise ValueError(
                    f"{context} entry '{value}' contains invalid characters; "
                    "allowed: letters, digits, and _ . : -"
                )

    def _validate_classification(self):
        """Validate table-level and per-column classification labels."""
        self._validate_classification_values(self.classification, "classification")
        for col_name, col_config in self.columns.items():
            self._validate_classification_values(
                col_config.get("classification"),
                f"columns.{col_name}.classification",
            )

    def resolve_table_description(self, generated: Optional[str]) -> Optional[str]:
        """Resolve the table description written to the destination.

        A configured ``description`` overrides the pipeline-generated one; any
        table-level ``classification`` is encoded into the result as a
        ``[saga:...]`` block. Returns ``None`` when ``persist_docs.table`` is off
        or there is nothing to write.
        """
        if not self.persist_docs.table:
            return None
        human = self.description if self.description is not None else generated
        composed = compose_description(
            human,
            {"classification": self.classification} if self.classification else None,
        )
        # Safety net: if a resolved secret was folded into the generated
        # description (e.g. an adapter composing a credential into base_url /
        # endpoint), mask it so a mistake degrades to a redacted description
        # rather than a secret persisted permanently in warehouse metadata.
        # Applied to both create-time hint and reconcile, so idempotency holds.
        return redact(composed) or None

    def get_column_description_map(self) -> Dict[str, str]:
        """Return {column: composed description} for columns that carry docs.

        Reuses ``get_dlt_column_hints`` so it honors ``persist_docs.columns`` and
        composes classification into the description exactly as dlt writes it at
        create time — a reconcile against this map is therefore a no-op when
        nothing changed. Columns without a description are omitted.
        """
        return {
            col: hints["description"]
            for col, hints in self.get_dlt_column_hints().items()
            if hints.get("description")
        }

    def get_dlt_column_hints(self) -> Dict[str, Dict[str, Any]]:
        """Get column hints for dlt, filtering out custom fields like 'default'.

        Column ``classification`` is saga-only (dlt has no column-tag concept) and
        is folded into the column ``description`` as a ``[saga:...]`` block, so
        both it and the human description reach the destination through dlt's
        native column-description support. When ``persist_docs.columns`` is off,
        the description is dropped (other hints, e.g. type overrides, still flow).

        Returns:
            Column hints dict with only dlt-supported fields.
        """
        if not self.columns:
            return {}

        # Custom fields that are NOT part of dlt's column hint schema — our
        # extensions, filtered out before passing to dlt. ``classification`` is
        # folded into ``description`` below, which is (re)composed there too.
        black_list_fields = {"default", "classification", "description"}

        dlt_columns: Dict[str, Dict[str, Any]] = {}
        for col_name, col_config in self.columns.items():
            hints = {k: v for k, v in col_config.items() if k not in black_list_fields}

            if self.persist_docs.columns:
                classification = col_config.get("classification")
                description = compose_description(
                    col_config.get("description"),
                    {"classification": classification} if classification else None,
                )
                if description:
                    hints["description"] = description

            if hints:
                dlt_columns[col_name] = hints

        return dlt_columns
