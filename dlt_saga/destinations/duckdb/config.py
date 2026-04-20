"""DuckDB destination configuration."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Dict, Optional

from dlt_saga.destinations.config import DestinationConfig

if TYPE_CHECKING:
    from dlt_saga.utility.cli.context import ExecutionContext


@dataclass
class DuckDBDestinationConfig(DestinationConfig):
    """DuckDB-specific destination configuration.

    Supports both in-memory databases (for testing) and file-based databases
    (for local development).
    """

    destination_type: str = "duckdb"
    database_path: str = ":memory:"
    schema_name: Optional[str] = None
    project_id: str = "local"  # Used for table ID construction

    @property
    def database(self) -> str:
        return self.project_id

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> DuckDBDestinationConfig:
        """Create DuckDB config from dictionary.

        Args:
            data: Configuration dictionary with keys: database_path, dataset_name

        Returns:
            DuckDBDestinationConfig instance
        """
        return cls(
            destination_type="duckdb",
            database_path=data.get("database_path", ":memory:"),
            schema_name=data.get("schema_name"),
            location=data.get("location"),
            project_id=data.get("project_id", data.get("project", "local")),
        )

    @classmethod
    def from_context(
        cls,
        context: ExecutionContext,
        config_dict: Dict[str, Any],
    ) -> DuckDBDestinationConfig:
        """Resolve DuckDB config from execution context and pipeline config.

        Resolution priority:
            database_path: context (profile) > ":memory:"
            schema_name:  already resolved by ConfigSource (e.g., FilePipelineConfig)
        """
        return cls(
            destination_type="duckdb",
            database_path=context.get_database_path() or ":memory:",
            schema_name=config_dict.get("schema_name"),
            project_id="local",
        )
