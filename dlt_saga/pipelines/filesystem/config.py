from dataclasses import dataclass, field
from typing import Optional

from dlt_saga.pipelines.base_config import BaseConfig
from dlt_saga.utility.secrets.secret_str import SecretStr, coerce_secret


@dataclass
class FilesystemConfig(BaseConfig):
    """Configuration for filesystem sources"""

    # Filesystem configuration (required, validated in __post_init__)
    filesystem_type: str = field(
        default="",
        metadata={
            "description": "Filesystem type (gs=Google Cloud Storage, s3=AWS S3, azure=Azure Blob, file=local, sftp=SFTP)",
            "enum": ["gs", "s3", "azure", "file", "sftp"],
            "required": True,
        },
    )
    bucket_name: str = field(
        default="",
        metadata={
            "description": "Bucket or container name (for cloud storage) or base path (for local files)",
            "required": True,
        },
    )
    file_type: str = field(
        default="",
        metadata={
            "description": "File format to read",
            "enum": ["csv", "parquet", "json", "jsonl", "geojson"],
            "required": True,
        },
    )
    project_id: Optional[str] = field(
        default=None,
        metadata={"description": "GCP project ID (for GCS access)"},
    )
    file_glob: Optional[str] = field(
        default=None,
        metadata={
            "description": "Glob pattern for file selection (e.g., 'data/*.csv', 'exports/**/*.parquet')"
        },
    )
    credentials: Optional[str] = field(
        default=None,
        metadata={"description": "Path to credentials file"},
    )

    # CSV parsing configuration
    csv_separator: Optional[str] = field(
        default=None,
        metadata={"description": "CSV separator character (default: comma)"},
    )
    encoding: Optional[str] = field(
        default=None,
        metadata={
            "description": "File encoding (default: utf-8, common: iso-8859-1, windows-1252)"
        },
    )

    # Optional fields with defaults
    endpoint_url: Optional[str] = field(
        default=None,
        metadata={"description": "Custom endpoint URL (for S3-compatible storage)"},
    )

    # SFTP configuration
    hostname: Optional[str] = field(
        default=None,
        metadata={
            "description": "SFTP server hostname (required for sftp filesystem_type)"
        },
    )
    port: Optional[int] = field(
        default=22,
        metadata={"description": "SFTP port (default: 22)"},
    )

    # SFTP Credentials
    # Supports both plain values and secret URIs:
    # - Plain: username: "myuser"
    # - Secret: username: "googlesecretmanager::project-id::secret-name"
    username: Optional[SecretStr] = field(
        default=None,
        metadata={
            "description": "SFTP username (plain value or secret URI: googlesecretmanager::project::secret)"
        },
    )
    password: Optional[SecretStr] = field(
        default=None,
        metadata={
            "description": "SFTP password (plain value or secret URI: googlesecretmanager::project::secret)"
        },
    )
    key_filename: Optional[SecretStr] = field(
        default=None,
        metadata={
            "description": "SSH private key (file path, key content, or secret URI: googlesecretmanager::project::secret)"
        },
    )
    key_passphrase: Optional[SecretStr] = field(
        default=None,
        metadata={
            "description": "Passphrase for encrypted SSH key (plain value or secret URI)"
        },
    )
    known_hosts: Optional[str] = field(
        default=None,
        metadata={
            "description": "Known hosts (file path, content, or secret URI: googlesecretmanager::project::secret)"
        },
    )
    end_date: Optional[str] = field(
        default=None,
        metadata={
            "description": "End date for backfill scenarios (YYYY-MM-DD format)",
            "pattern": "^\\d{4}-\\d{2}-\\d{2}$",
        },
    )

    # Snapshot date extraction from file path (for historization)
    snapshot_date_regex: Optional[str] = field(
        default=None,
        metadata={
            "description": "Regex to extract snapshot date from file path. "
            "Must contain exactly one capture group for the date string. "
            "Example: 'snapshots/(\\d{4}-\\d{2}-\\d{2})/' or 'orders_(\\d{8})\\.csv'"
        },
    )
    snapshot_date_format: Optional[str] = field(
        default=None,
        metadata={
            "description": "strptime format for parsing the regex-extracted date. "
            "Required when snapshot_date_regex is set. Example: '%Y-%m-%d' or '%Y%m%d'"
        },
    )

    # Post-load cleanup
    delete_after_load: Optional[bool] = field(
        default=False,
        metadata={
            "description": "Delete source files after successful load (default: False, opt-in only)"
        },
    )

    # CSV file metadata injection
    csv_include_file_metadata: Optional[bool] = field(
        default=False,
        metadata={
            "description": "Include file metadata columns (_dlt_source_file_name, _dlt_source_modification_date) in each CSV row"
        },
    )

    # JSON key-value mapping configuration
    json_key_column: Optional[str] = field(
        default=None,
        metadata={
            "description": "Column name for keys when reading key-value JSON objects (e.g., 'municipality_code')"
        },
    )
    json_value_column: Optional[str] = field(
        default=None,
        metadata={
            "description": "Column name for values when reading key-value JSON objects (e.g., 'municipality_name')"
        },
    )

    def __post_init__(self):
        """Validate configuration after initialization."""
        # Call parent __post_init__ first
        super().__post_init__()

        # Coerce credential fields to SecretStr
        self.username = coerce_secret(self.username)
        self.password = coerce_secret(self.password)
        self.key_filename = coerce_secret(self.key_filename)
        self.key_passphrase = coerce_secret(self.key_passphrase)

        # Validate numeric bounds
        if self.port is not None and not (1 <= self.port <= 65535):
            raise ValueError(f"port must be 1-65535, got {self.port}")

        # Validate required fields
        if not self.filesystem_type:
            raise ValueError("filesystem_type is required for filesystem sources")
        if not self.bucket_name:
            raise ValueError("bucket_name is required for filesystem sources")
        if not self.file_type:
            raise ValueError("file_type is required for filesystem sources")
        if not self.file_glob:
            raise ValueError("file_glob is required for filesystem sources")

        self._validate_snapshot_config()

    def _validate_snapshot_config(self) -> None:
        """Validate snapshot date format and regex configuration."""
        if self.snapshot_date_format:
            from datetime import datetime

            try:
                datetime.now().strftime(self.snapshot_date_format)
            except ValueError as e:
                raise ValueError(
                    f"Invalid snapshot_date_format '{self.snapshot_date_format}': {e}"
                ) from e

        if self.snapshot_date_regex and not self.snapshot_date_format:
            raise ValueError(
                "snapshot_date_format is required when snapshot_date_regex is set"
            )
        if self.snapshot_date_regex:
            import re

            pattern = re.compile(self.snapshot_date_regex)
            if pattern.groups != 1:
                raise ValueError(
                    f"snapshot_date_regex must contain exactly one capture group, "
                    f"got {pattern.groups}: '{self.snapshot_date_regex}'"
                )
