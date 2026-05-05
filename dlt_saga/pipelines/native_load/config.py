"""Configuration for the native_load pipeline adapter."""

import dataclasses
import logging
import re
import warnings
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Union

from dlt_saga.pipelines.base_config import BaseConfig

logger = logging.getLogger(__name__)

_VALID_FILE_TYPES = ("parquet", "csv", "jsonl")
_VALID_URI_SCHEMES = ("gs://", "s3://", "abfss://")
_DEFAULT_PATTERNS = {"parquet": "*.parquet", "csv": "*.csv", "jsonl": "*.jsonl"}


@dataclass
class NativeLoadConfig(BaseConfig):
    """Configuration for the native_load pipeline adapter.

    This adapter ingests cloud-storage datasets directly into the warehouse,
    bypassing dlt's extract/normalize machinery. Designed for large sources
    (tens of thousands of files, GB+ per run) where dlt would be too slow.

    Supported write_disposition values: ``append``, ``replace``,
    ``append+historize``, ``replace+historize``.

    File-level state tracking (skip already-loaded files across runs) is a
    separate opt-in: ``incremental: true``. Mirrors dlt's split between
    ``write_disposition`` and ``dlt.sources.incremental``.
    """

    write_disposition: str = field(
        default="append",
        metadata={
            "description": (
                "Table-level write semantics. Use `incremental: true` for "
                "file-level state tracking (separate concern, mirroring dlt's "
                "split between write_disposition and dlt.sources.incremental)."
            ),
            "enum": ["append", "replace", "append+historize", "replace+historize"],
        },
    )

    # -------------------------------------------------------------------------
    # Source
    # -------------------------------------------------------------------------

    source_uri: str = field(
        default="",
        metadata={
            "required": True,
            "description": (
                "Root URI to load from. Must start with gs://, s3://, or abfss:// "
                "and end with /. v1 only exercises gs://; s3:// and abfss:// are "
                "accepted by validation but raise NotImplementedError at runtime."
            ),
        },
    )

    file_pattern: Optional[Union[str, List[str]]] = field(
        default=None,
        metadata={
            "description": (
                "Glob pattern matched against file basenames. "
                "Can be a single pattern (e.g. '*.parquet') or a list of patterns. "
                "Defaults to *.parquet / *.csv / *.jsonl based on file_type."
            )
        },
    )

    file_type: str = field(
        default="parquet",
        metadata={
            "description": "File format: parquet, csv, or jsonl.",
            "enum": list(_VALID_FILE_TYPES),
        },
    )

    # -------------------------------------------------------------------------
    # Discovery
    # -------------------------------------------------------------------------

    filename_date_regex: Optional[str] = field(
        default=None,
        metadata={
            "description": (
                "Regex with exactly one capture group that extracts the date "
                "string from each filename. When set (together with "
                "`filename_date_format`), files are grouped by extracted date "
                "and loaded chronologically. `_dlt_source_file_date` is "
                "derived from the captured value."
            ),
        },
    )

    filename_date_format: Optional[str] = field(
        default=None,
        metadata={
            "description": (
                "strftime format string for the value captured by "
                "`filename_date_regex`. Example: '%Y%m%d' for '20260115'."
            ),
        },
    )

    date_lookback_days: int = field(
        default=2,
        metadata={
            "description": (
                "When `incremental: true` and date fields are set, extend the "
                "file-scan window this many days before `last_cursor` to catch "
                "late-arriving files. Ignored when `incremental: false`."
            ),
        },
    )

    date_filename_prefix: Optional[str] = field(
        default=None,
        metadata={
            "description": (
                "Common filename prefix used to compute the `start_offset` "
                "for GCS lexicographic listing. Auto-detected from the first "
                "50 blobs when absent. Only meaningful with `incremental: true` "
                "and `filename_date_regex`; ignored otherwise."
            ),
        },
    )

    # -------------------------------------------------------------------------
    # Load behaviour
    # -------------------------------------------------------------------------

    load_batch_size: int = field(
        default=5000,
        metadata={"description": "Maximum number of source URIs per load chunk."},
    )

    incremental: bool = field(
        default=False,
        metadata={
            "description": (
                "Track loaded (uri, generation) pairs in `_saga_native_load_log` "
                "so re-runs only ingest new files. Defaults to false — matches "
                "dlt, where incremental loading is opt-in. Required for "
                "`start_offset` pruning in date mode and for "
                "`partition_prefix_pattern` partition-walk. "
                "Not compatible with write_disposition='replace'."
            ),
        },
    )

    staging_dataset: Optional[str] = field(
        default=None,
        metadata={
            "description": (
                "BigQuery-only: staging dataset for transient external tables. "
                "Defaults to <target_dataset>_staging."
            )
        },
    )

    max_bad_records: int = field(
        default=0,
        metadata={
            "description": "Maximum tolerated bad records per chunk (BigQuery only)."
        },
    )

    ignore_unknown_values: bool = field(
        default=False,
        metadata={
            "description": "Silently skip extra values in source (BigQuery only)."
        },
    )

    # -------------------------------------------------------------------------
    # Schema
    # -------------------------------------------------------------------------

    autodetect_schema: bool = field(
        default=True,
        metadata={
            "description": (
                "Infer schema from source files. "
                "Setting to False requires the target table to already exist."
            )
        },
    )

    include_file_metadata: bool = field(
        default=True,
        metadata={
            "description": (
                "Inject _dlt_source_file_name into each row for provenance tracking "
                "and per-file row count reporting. Recommended."
            )
        },
    )

    # -------------------------------------------------------------------------
    # Column schema (BigQuery only)
    # -------------------------------------------------------------------------

    columns: Dict[str, Any] = field(
        default_factory=dict,
        metadata={
            "description": (
                "Explicit column type hints keyed by raw or normalized column name. "
                "Each value: {data_type: <type>}. "
                "Supported dlt logical types: text, bigint, double, bool, timestamp, date, time, decimal. "
                "Native BigQuery types (STRING, INT64, …) also accepted. "
                "Emits SAFE_CAST(col AS type) in the SELECT, overriding autodetect. "
                "Example: {order_date: {data_type: timestamp}, amount: {data_type: decimal}}. "
                "BigQuery only; ignored by Databricks."
            ),
        },
    )

    # -------------------------------------------------------------------------
    # Databricks-specific — external tables and table format
    # -------------------------------------------------------------------------

    target_location: Optional[str] = field(
        default=None,
        metadata={
            "description": (
                "Explicit external-table LOCATION (full URI, e.g. "
                "abfss://lake@account.dfs.core.windows.net/raw/group/table/). "
                "Databricks only; raises on BigQuery. "
                "Overrides the profile's storage_root + auto-derived path."
            ),
        },
    )

    table_format: str = field(
        default="delta",
        metadata={
            "description": (
                "Target table format on Databricks. "
                "'delta' (default) — Delta Lake managed/external table with Liquid Clustering. "
                "'iceberg' — native managed Iceberg in Unity Catalog. "
                "'delta_uniform' — Delta storage + Iceberg metadata sidecar (UniForm). "
                "Ignored on BigQuery."
            ),
            "enum": ["delta", "iceberg", "delta_uniform"],
        },
    )

    # -------------------------------------------------------------------------
    # ADLS-specific — partition-pruned listing
    # -------------------------------------------------------------------------

    partition_prefix_pattern: Optional[str] = field(
        default=None,
        metadata={
            "description": (
                "Hive-style partition layout under source_uri. "
                "Tokens: {year}, {month}, {day}, {hour}. "
                "When set with filename_date_format, discovery lists only the partitions "
                "in [last_cursor - lookback_days, today] instead of the full source root. "
                "Example: 'year={year}/month={month}/day={day}/'. "
                "ADLS only; ignored for GCS sources."
            ),
        },
    )

    # -------------------------------------------------------------------------
    # CSV options (ignored for non-CSV file_type)
    # -------------------------------------------------------------------------

    csv_separator: Optional[str] = field(
        default=None,
        metadata={"description": "Field delimiter for CSV files (e.g. ';')."},
    )

    encoding: Optional[str] = field(
        default=None,
        metadata={"description": "File encoding for CSV files (e.g. 'ISO-8859-1')."},
    )

    csv_skip_leading_rows: int = field(
        default=0,
        metadata={"description": "Number of leading rows to skip in CSV files."},
    )

    csv_quote_character: Optional[str] = field(
        default=None,
        metadata={"description": "Quote character for CSV files."},
    )

    csv_null_marker: Optional[str] = field(
        default=None,
        metadata={"description": "String that represents NULL values in CSV files."},
    )

    # -------------------------------------------------------------------------
    # Validation
    # -------------------------------------------------------------------------

    def __post_init__(self) -> None:
        # Inline BaseConfig tags normalisation; skip incremental_column validation
        # because NativeLoadConfig redefines `incremental` to mean file-level state
        # tracking (not source cursor), so incremental_column is not required here.
        if self.tags is not None and not isinstance(self.tags, list):
            self.tags = [self.tags]
        elif self.tags is None:
            self.tags = []

        self._validate_write_disposition()
        self._validate_source_uri()
        self._validate_file_type()
        self._validate_date_fields()
        self._validate_numeric_floors()
        self._apply_format_defaults()
        self._validate_csv_fields()
        self._validate_table_format()
        self._validate_inert_date_settings()

    def _validate_write_disposition(self) -> None:
        base = self.write_disposition.replace("+historize", "")
        if base not in ("append", "replace"):
            raise ValueError(
                f"native_load only supports write_disposition 'append', 'replace', "
                f"'append+historize', or 'replace+historize', got: {self.write_disposition!r}"
            )
        if base == "replace" and self.incremental:
            raise ValueError(
                "incremental=True is not supported with write_disposition='replace': "
                "replace truncates the target each run, so per-run file dedup would lose "
                "data. Use write_disposition='append' if you want incremental loading."
            )

    def _validate_source_uri(self) -> None:
        if not self.source_uri:
            raise ValueError("source_uri is required for native_load adapter")
        if not any(self.source_uri.startswith(s) for s in _VALID_URI_SCHEMES):
            raise ValueError(
                f"source_uri must start with one of {_VALID_URI_SCHEMES}, "
                f"got: {self.source_uri!r}"
            )
        if not self.source_uri.endswith("/"):
            raise ValueError(f"source_uri must end with '/', got: {self.source_uri!r}")

    def _validate_file_type(self) -> None:
        if self.file_type not in _VALID_FILE_TYPES:
            raise ValueError(
                f"file_type must be one of {_VALID_FILE_TYPES}, got: {self.file_type!r}"
            )

    def _validate_date_fields(self) -> None:
        has_regex = bool(self.filename_date_regex)
        has_format = bool(self.filename_date_format)

        if has_regex != has_format:
            raise ValueError(
                "`filename_date_regex` and `filename_date_format` must both be set or "
                "both absent."
            )

        if self.filename_date_regex:
            try:
                pattern = re.compile(self.filename_date_regex)
            except re.error as exc:
                raise ValueError(
                    f"filename_date_regex is not a valid regex: {exc}"
                ) from exc
            if pattern.groups != 1:
                raise ValueError(
                    f"filename_date_regex must contain exactly one capture group, "
                    f"got {pattern.groups}: {self.filename_date_regex!r}"
                )

        if self.filename_date_format:
            import datetime

            try:
                datetime.datetime(2024, 1, 1).strftime(self.filename_date_format)
            except Exception as exc:
                raise ValueError(
                    f"filename_date_format is not a valid strftime string: {exc}"
                ) from exc

    def _validate_numeric_floors(self) -> None:
        if self.load_batch_size < 1:
            raise ValueError("load_batch_size must be >= 1")
        if self.date_lookback_days < 0:
            raise ValueError("date_lookback_days must be >= 0")
        if self.max_bad_records < 0:
            raise ValueError("max_bad_records must be >= 0")
        if self.csv_skip_leading_rows < 0:
            raise ValueError("csv_skip_leading_rows must be >= 0")

    def _apply_format_defaults(self) -> None:
        if self.file_pattern is None:
            self.file_pattern = _DEFAULT_PATTERNS[self.file_type]

    def _validate_csv_fields(self) -> None:
        csv_only = {
            "csv_separator": self.csv_separator,
            "encoding": self.encoding,
            "csv_skip_leading_rows": self.csv_skip_leading_rows
            if self.csv_skip_leading_rows
            else None,
            "csv_quote_character": self.csv_quote_character,
            "csv_null_marker": self.csv_null_marker,
        }
        if self.file_type != "csv":
            for name, val in csv_only.items():
                if val is not None:
                    warnings.warn(
                        f"Config field {name!r} is set but file_type={self.file_type!r}; "
                        "it will be ignored.",
                        stacklevel=4,
                    )

    def _validate_table_format(self) -> None:
        valid = ("delta", "iceberg", "delta_uniform")
        if self.table_format not in valid:
            raise ValueError(
                f"table_format must be one of {valid}, got: {self.table_format!r}"
            )

    def _validate_inert_date_settings(self) -> None:
        if not self.incremental:
            if self.date_filename_prefix is not None:
                logger.warning(
                    "date_filename_prefix is set but incremental=false; ignored."
                )
            if self.date_lookback_days != 2:
                logger.warning(
                    "date_lookback_days is set but incremental=false; ignored."
                )
            if self.partition_prefix_pattern is not None:
                logger.warning(
                    "partition_prefix_pattern is set but incremental=false; ignored."
                )
        if self.partition_prefix_pattern is not None and not self.filename_date_regex:
            logger.warning(
                "partition_prefix_pattern is set but `filename_date_regex` is absent; "
                "partition-prefix walk requires date fields and will be skipped."
            )

    @classmethod
    def from_dict(cls, data: dict) -> "NativeLoadConfig":
        """Create a NativeLoadConfig from a pipeline config dict.

        Unknown keys are silently ignored so YAML configs that include
        destination-shared keys (schema_name, pipeline_name, etc.) don't raise.
        """
        known = {f.name for f in dataclasses.fields(cls)}
        filtered = {k: v for k, v in data.items() if k in known}
        return cls(**filtered)
