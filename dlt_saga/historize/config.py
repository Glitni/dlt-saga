"""Configuration for the historize command.

Defines HistorizeConfig which controls how snapshot data is historized:
into SCD2 tables with _dlt_valid_from/_dlt_valid_to periods, or into SCD1
tables holding only the latest version per key.
"""

import re
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

from dlt_saga.utility.filters import filter_field_metadata as _filter_field_metadata

# Bare SQL identifier: letters/underscore start, then letters/digits/underscore.
# Column names interpolated into historize SQL are checked against this so a
# config value can't inject SQL or silently break unquoted interpolation.
_SQL_IDENT = re.compile(r"^[a-zA-Z_][a-zA-Z0-9_]*$")

STRATEGIES = ("scd2", "scd1")
DELIVERIES = ("full", "incremental")


@dataclass
class HistorizeConfig:
    """Configuration for historizing a pipeline's raw snapshot data.

    This config is nested under the `historize:` key in pipeline YAML files.
    For pipelines that also run ingest, primary_key is inherited from the
    top-level config if not specified here.

    For external-delivery pipelines (write_disposition: "historize"), source
    location is specified via top-level source_database, source_schema,
    source_table fields (shared with ingest config).
    """

    strategy: str = field(
        default="scd2",
        metadata={
            "description": (
                "How changes are kept. 'scd2' (default): every version of a key "
                "is kept, with _dlt_valid_from/_dlt_valid_to validity periods. "
                "'scd1': only the latest version per key is kept — a newer "
                "version overwrites the stored row, so superseded values do not "
                "survive in the historized table. Changing it requires a full "
                "refresh, and moving from scd1 to scd2 can't recover the "
                "versions scd1 already overwrote."
            ),
            "enum": list(STRATEGIES),
        },
    )

    delivery: str = field(
        default="full",
        metadata={
            "description": (
                "What each snapshot_column value holds. 'full' (default): every "
                "record that exists at that time, so a record missing from a "
                "snapshot has been deleted and one returning after missing "
                "snapshots has reappeared. 'incremental': only the records sent "
                "in that delivery (new, changed or re-sent), so a missing record "
                "means nothing — deletion detection, gap-driven reappearance and "
                "merge_key don't apply, and scd2 opens a version only when a "
                "record's values change. Changing it requires a full refresh."
            ),
            "enum": list(DELIVERIES),
        },
    )

    order_by: Optional[List[str]] = field(
        default=None,
        metadata={
            "description": (
                "strategy: scd1 only. Columns that rank the versions of a key, "
                "highest wins (NULL ranks lowest). Defaults to [snapshot_column]; "
                "when snapshot_column isn't listed, it is appended as the final "
                "tiebreaker. "
                "Each column must be carried into the historized table (a value "
                "column, or snapshot_column itself), since later runs compare "
                "against the stored version. A version tying with the stored "
                "one on every column replaces it, so re-processing a corrected "
                "source row takes effect."
            )
        },
    )

    snapshot_column: str = field(
        default="_dlt_ingested_at",
        metadata={
            "description": (
                "Column that identifies each delivery in the source table: rows "
                "sharing a value form one delivery, and the value becomes "
                "valid_from. Under delivery: full each delivery is a complete "
                "snapshot of the source; under incremental it holds only the "
                "records sent in it. "
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
                "Column to partition the historized table by. Unset by default: "
                "the SCD2 validity columns don't make good partition keys "
                "(current-state reads touch every partition), so the layout "
                "default is clustering instead. Partitioning can't be changed "
                "in place; a mismatch against an existing table is reported "
                "and requires a full refresh."
            )
        },
    )

    cluster_columns: Optional[List[str]] = field(
        default=None,
        metadata={
            "description": (
                "Columns to cluster the historized table by (max 4). Unset "
                "(the default) clusters a newly created table by "
                "[valid_to_column, valid_from_column] and leaves an existing "
                "table's clustering alone. Set explicitly to enforce clustering "
                "on every run — an empty list removes clustering."
            )
        },
    )

    track_deletions: Optional[bool] = field(
        default=None,
        metadata={
            "description": (
                "When True, rows that disappear from the source produce a deletion marker row "
                "(_dlt_is_deleted=True) in the historized table. "
                "When False, only value changes are tracked. Defaults to True for "
                "strategy: scd2 with delivery: full. Opt-in for strategy: scd1, "
                "where a deleted key keeps its row with _dlt_is_deleted=True "
                "(order_by must then start with snapshot_column). Not "
                "supported with delivery: incremental."
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

    detect_late_arrivals: Optional[bool] = field(
        default=None,
        metadata={
            "description": (
                "Handling of snapshots that land in the source after later "
                "snapshots have already been historized (below the incremental "
                "watermark). Unset (default): detect and warn, suggesting this "
                "key — nothing is replayed. true: automatically rewind and "
                "replay history from the earliest late snapshot (refused with a "
                "warning if raw no longer contains every historized snapshot in "
                "the rewind window, e.g. after partition expiration). false: "
                "skip detection entirely (silences the warning). Detection "
                "compares arrival_column against the previous run's start time, "
                "so it only engages when snapshot_column differs from "
                "arrival_column and the source retains snapshot history (append "
                "or historize write dispositions)."
            )
        },
    )

    late_arrival_window_days: Optional[int] = field(
        default=None,
        metadata={
            "description": (
                "How late a snapshot may arrive and still be replayed "
                "automatically, in days behind the incremental watermark. Late "
                "snapshots older than this are ignored with a warning (handle "
                "them manually with --historize-from). Bounds the worst-case "
                "rewind: without it, a stray very old file triggers a "
                "correspondingly large replay. Only meaningful with "
                "detect_late_arrivals: true; omit for no bound."
            )
        },
    )

    arrival_column: str = field(
        default="_dlt_ingested_at",
        metadata={
            "description": (
                "Column holding each row's warehouse arrival time, used to detect "
                "late-arriving snapshots. Defaults to '_dlt_ingested_at' (injected "
                "automatically by saga ingest); external deliveries can point it at "
                "their own load-time column. Detection is skipped when the column "
                "is absent from the source or equal to snapshot_column. A custom "
                "arrival column is still a normal value column — add it to "
                "ignore_columns so a redelivery of unchanged data doesn't register "
                "as a change."
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
        for attr in (
            "primary_key",
            "classification",
            "track_columns",
            "ignore_columns",
            "cluster_columns",
            "merge_key",
            "order_by",
        ):
            if isinstance(getattr(self, attr), str):
                setattr(self, attr, [getattr(self, attr)])
        self._validate_choices()
        # Delivery first: an option it rules out shouldn't get a strategy
        # error suggesting a fix that delivery then rejects.
        self._validate_delivery_options()
        self._validate_strategy_options()
        if self.track_deletions is None:
            self.track_deletions = self.strategy == "scd2" and self.is_full_delivery
        if (
            self.late_arrival_window_days is not None
            and self.late_arrival_window_days < 1
        ):
            raise ValueError(
                f"late_arrival_window_days must be a positive number of days, "
                f"got {self.late_arrival_window_days}"
            )
        self._validate_column_identifiers()

    @property
    def is_full_delivery(self) -> bool:
        """Whether each snapshot holds every record that exists at that time."""
        return self.delivery == "full"

    @property
    def is_scd1(self) -> bool:
        """Whether only the latest version per key is kept."""
        return self.strategy == "scd1"

    @property
    def effective_order_by(self) -> List[str]:
        """Version ranking for scd1: ``order_by`` plus the snapshot tiebreaker.

        Empty for scd2, which orders versions by ``snapshot_column`` alone.
        """
        if not self.is_scd1:
            return []
        columns = list(self.order_by or [])
        # Case-insensitive, like column resolution on every destination.
        if self.snapshot_column.lower() not in {c.lower() for c in columns}:
            columns.append(self.snapshot_column)
        return columns

    def _validate_choices(self) -> None:
        """Reject values outside a closed set of choices."""
        for key, allowed in (("strategy", STRATEGIES), ("delivery", DELIVERIES)):
            value = getattr(self, key)
            if value not in allowed:
                raise ValueError(
                    f"historize.{key} must be one of {', '.join(allowed)}, "
                    f"got '{value}'"
                )

    def _validate_delivery_options(self) -> None:
        """Reject options that need full snapshots under ``delivery: incremental``.

        Both read a record's absence from a snapshot, which only means something
        when the snapshot holds every record.
        """
        if self.is_full_delivery:
            return
        unsupported = {
            "track_deletions": self.track_deletions is True,
            "merge_key": bool(self.merge_key),
        }
        for key, is_set in unsupported.items():
            if is_set:
                raise ValueError(
                    f"historize.{key} is not supported with delivery: incremental "
                    f"— each snapshot holds only the records sent in it, so a "
                    f"record's absence says nothing. Remove it, or use delivery: "
                    f"full if every snapshot is a complete copy of the source."
                )

    def _validate_strategy_options(self) -> None:
        """Reject options that have no meaning under the chosen strategy.

        Refused rather than ignored, so a config never reads as doing something
        it doesn't.
        """
        if not self.is_scd1:
            if self.order_by:
                raise ValueError(
                    "historize.order_by only applies to strategy: scd1 — scd2 "
                    "orders versions by snapshot_column. Remove order_by, or "
                    "set strategy: scd1."
                )
            return

        if self.order_by is not None and len(self.order_by) == 0:
            raise ValueError(
                "historize.order_by must be either omitted (rank versions by "
                "snapshot_column) or a non-empty list."
            )
        tracks_deletions = self.track_deletions is True
        if tracks_deletions and self.order_by:
            snapshot = self.snapshot_column.lower()
            if self.order_by[0].lower() != snapshot:
                suggested = [self.snapshot_column] + [
                    col for col in self.order_by if col.lower() != snapshot
                ]
                raise ValueError(
                    "historize.order_by must start with snapshot_column when "
                    "strategy: scd1 tracks deletions — a deletion happens at a "
                    "snapshot and has no values for other ranking columns, so "
                    "they can only break ties within one snapshot. Use "
                    f"order_by: [{', '.join(suggested)}], or set "
                    "track_deletions: false."
                )
        unsupported = {
            "merge_key": (
                bool(self.merge_key) and not tracks_deletions,
                "it only scopes deletion detection. Set track_deletions: true, "
                "or remove it.",
            ),
            "track_columns": (
                self.track_columns is not None,
                "every newer version replaces the stored one, so no column is "
                "excluded from change detection. Remove it, or use strategy: scd2.",
            ),
            "ignore_columns": (
                bool(self.ignore_columns),
                "every newer version replaces the stored one, so no column is "
                "excluded from change detection. Remove it, or use strategy: scd2.",
            ),
            "valid_to_column": (
                self.valid_to_column
                != self.__dataclass_fields__["valid_to_column"].default,
                "an scd1 table has no such column. Remove it, or use strategy: scd2.",
            ),
            "is_deleted_column": (
                self.is_deleted_column
                != self.__dataclass_fields__["is_deleted_column"].default
                and not tracks_deletions,
                "an scd1 table only has it with track_deletions: true. Set "
                "that, or remove it.",
            ),
        }
        for key, (is_set, reason) in unsupported.items():
            if is_set:
                raise ValueError(
                    f"historize.{key} is not supported with strategy: scd1 — {reason}"
                )

    def _validate_column_identifiers(self):
        """Validate SQL identifier format for column name fields.

        Covers every config-supplied column name interpolated into historize
        SQL. ``primary_key`` is validated separately in ``validate()`` because
        it may still be inherited from the top-level config after construction.
        """
        for attr in (
            "snapshot_column",
            "arrival_column",
            "valid_from_column",
            "valid_to_column",
            "is_deleted_column",
            "partition_column",
        ):
            value = getattr(self, attr)
            if value is not None and not _SQL_IDENT.match(value):
                raise ValueError(
                    f"{attr} must be a valid SQL identifier, got '{value}'"
                )
        for attr in (
            "cluster_columns",
            "merge_key",
            "track_columns",
            "ignore_columns",
            "order_by",
        ):
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
