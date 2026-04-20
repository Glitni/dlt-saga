"""Configuration for database pipelines using ConnectorX.

ConnectorX supports multiple database types:
- Postgres, MySQL, MariaDB, SQLite, Redshift, ClickHouse
- SQL Server, Azure SQL, Oracle, BigQuery, Trino
"""

from dataclasses import dataclass, field
from typing import Any, Dict, Optional

from dlt_saga.pipelines.base_config import BaseConfig
from dlt_saga.utility.secrets.secret_str import SecretStr, coerce_secret


@dataclass
class DatabaseConfig(BaseConfig):
    """Database source configuration for ConnectorX-based extraction."""

    # Connection details (option 1: connection string)
    connection_string: Optional[SecretStr] = field(
        default=None,
        metadata={
            "description": "Database connection string (supports env vars and secret URIs). "
            "Example: 'postgresql://user:pass@host:5432/db'"
        },
    )

    # Connection details (option 2: individual components)
    database_type: Optional[str] = field(
        default=None,
        metadata={
            "description": "Database type: postgres, mysql, mssql, sqlite, oracle, bigquery, trino, etc.",
            "enum": [
                "postgres",
                "postgresql",
                "mysql",
                "mariadb",
                "mssql",
                "sqlite",
                "redshift",
                "clickhouse",
                "oracle",
                "bigquery",
                "trino",
                "duckdb",
            ],
        },
    )
    host: Optional[str] = field(
        default=None,
        metadata={"description": "Database host (e.g., localhost, db.example.com)"},
    )
    port: Optional[int] = field(
        default=None,
        metadata={"description": "Database port (e.g., 5432 for Postgres)"},
    )
    source_database: Optional[str] = field(
        default=None,
        metadata={"description": "Source database name (project for BigQuery)"},
    )
    username: Optional[SecretStr] = field(
        default=None,
        metadata={
            "description": "Database username (supports env vars and secret URIs)"
        },
    )
    password: Optional[SecretStr] = field(
        default=None,
        metadata={
            "description": "Database password (supports env vars and secret URIs)"
        },
    )
    source_schema: Optional[str] = field(
        default=None,
        metadata={
            "description": "Source schema (optional, for databases that support schemas)"
        },
    )

    # Query or table selection
    query: Optional[str] = field(
        default=None,
        metadata={
            "description": "SQL query to extract data. Use {incremental_column} and {incremental_value} placeholders for incremental queries."
        },
    )
    source_table: Optional[str] = field(
        default=None,
        metadata={"description": "Source table name to extract (alternative to query)"},
    )

    # ConnectorX-specific options
    partition_on: Optional[str] = field(
        default=None,
        metadata={
            "description": "Column to partition on for parallel reading (improves performance for large tables)"
        },
    )
    partition_num: Optional[int] = field(
        default=None,
        metadata={
            "description": "Number of partitions for parallel reading (default: number of CPU cores)"
        },
    )

    # Additional connection options
    connection_options: Optional[Dict[str, Any]] = field(
        default=None,
        metadata={
            "description": "Additional connection parameters (e.g., sslmode, connect_timeout)"
        },
    )

    # Retry options
    max_retries: int = field(
        default=2,
        metadata={
            "description": "Maximum number of retry attempts for transient errors (e.g., connection failures)"
        },
    )
    retry_backoff_base: int = field(
        default=2,
        metadata={
            "description": "Base for exponential backoff between retries (seconds = base^attempt: 2s, 4s)"
        },
    )

    def __post_init__(self):
        """Validate configuration after initialization."""
        # Call parent __post_init__ first
        super().__post_init__()

        # Coerce credential fields to SecretStr
        self.connection_string = coerce_secret(self.connection_string)
        self.username = coerce_secret(self.username)
        self.password = coerce_secret(self.password)

        self._validate_connection()
        self._validate_numeric_bounds()

    def _validate_connection(self) -> None:
        """Validate connection string or individual component requirements."""
        # Must have either connection_string or individual components
        # BigQuery and SQLite don't require 'host'
        if not self.connection_string:
            if not self.database_type:
                raise ValueError(
                    "Must provide either 'connection_string' or 'database_type'"
                )

            # BigQuery and SQLite have different requirements
            if self.database_type.lower() in ("bigquery",):
                # BigQuery: database is optional - if not provided, uses profile project
                pass
            elif self.database_type.lower() in ("duckdb",):
                if not self.source_database:
                    raise ValueError(
                        "For DuckDB, must provide 'source_database' (file path)"
                    )
            elif self.database_type.lower() in ("sqlite",):
                if not self.source_database:
                    raise ValueError("For SQLite, must provide 'database' (file path)")
            else:
                # All other databases require host and database
                if not all([self.host, self.source_database]):
                    raise ValueError(
                        f"For {self.database_type}, must provide 'host' and 'database'"
                    )

        # Must have either query or source_table
        if not self.query and not self.source_table:
            raise ValueError("Must provide either 'query' or 'source_table'")

    def _validate_numeric_bounds(self) -> None:
        """Validate numeric field ranges."""
        if self.port is not None and not (1 <= self.port <= 65535):
            raise ValueError(f"port must be 1-65535, got {self.port}")
        if self.max_retries < 0:
            raise ValueError(f"max_retries must be >= 0, got {self.max_retries}")
        if self.retry_backoff_base < 1:
            raise ValueError(
                f"retry_backoff_base must be >= 1, got {self.retry_backoff_base}"
            )
        if self.partition_num is not None and self.partition_num < 1:
            raise ValueError(f"partition_num must be >= 1, got {self.partition_num}")
