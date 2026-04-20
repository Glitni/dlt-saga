"""Database pipeline using ConnectorX for high-performance extraction."""

import logging
from dataclasses import fields
from typing import Any, Dict, List, Tuple

import dlt

from dlt_saga.pipelines.base_pipeline import BasePipeline
from dlt_saga.pipelines.database.client import DatabaseClient
from dlt_saga.pipelines.database.config import DatabaseConfig

logger = logging.getLogger(__name__)


class DatabasePipeline(BasePipeline):
    """Pipeline for extracting data from databases using ConnectorX.

    Supports multiple database types: Postgres, MySQL, SQL Server, Oracle,
    BigQuery, Trino, and more. Uses ConnectorX with Arrow format for
    high-performance data extraction.
    """

    def __init__(self, config: Dict[str, Any], log_prefix: str = None):
        """Initialize database pipeline.

        Args:
            config: Pipeline configuration dictionary
            log_prefix: Optional prefix for log messages
        """
        # Extract database-specific config
        # Use fields() to get all fields including inherited ones from BaseConfig
        config_field_names = {f.name for f in fields(DatabaseConfig)}
        self.source_config = DatabaseConfig(
            **{k: v for k, v in config.items() if k in config_field_names}
        )

        # Initialize client
        self.client = DatabaseClient(self.source_config)

        # Initialize base pipeline (handles destination, target config, etc.)
        super().__init__(config, log_prefix)

    def extract_data(self) -> List[Tuple[Any, str]]:
        """Extract data from database.

        Returns:
            List of (dlt.resource, description) tuples
        """
        resources = []

        # Determine incremental value if applicable
        incremental_value = None
        incremental = self.config_dict.get("incremental", False)
        if incremental:
            incremental_column = self.config_dict.get("incremental_column")
            initial_value = self.config_dict.get("initial_value")

            # Query target table for max value, fall back to initial_value
            table_id = f"{self.destination_database}.{self.pipeline.dataset_name}.{self.table_name}"
            incremental_value = (
                self.destination.get_max_column_value(table_id, incremental_column)
                or initial_value
            )

            if incremental_value:
                self.logger.info(
                    f"Incremental loading from {incremental_column} > {incremental_value}"
                )
        else:
            incremental_column = None

        # Fetch data using ConnectorX (returns Arrow table by default)
        arrow_table = self.client.fetch_data(
            incremental_value=incremental_value,
            incremental_column=incremental_column,
            return_type="arrow",
        )

        # Convert Arrow table to dlt resource
        # dlt.resource can handle Arrow tables directly
        resource = dlt.resource(
            arrow_table, name=self.table_name, write_disposition="auto"
        )

        # Build description
        source_info = self._get_source_description()
        description = f"Data from {source_info}"

        resources.append((resource, description))
        return resources

    def _get_source_description(self) -> str:
        """Build a human-readable source description.

        Returns:
            Description of the data source
        """
        if self.source_config.connection_string:
            # Parse database type from connection string
            db_type = self.source_config.connection_string.split("://")[0]
            if self.source_config.source_table:
                return f"{db_type} table: {self.source_config.source_table}"
            return f"{db_type} query"

        # Use configured database type and name
        db_type = self.source_config.database_type
        db_name = (
            self.source_config.source_database
            if isinstance(self.source_config.source_database, str)
            else None
        )

        if self.source_config.source_table:
            table = self.source_config.source_table
            if self.source_config.source_schema:
                table = f"{self.source_config.source_schema}.{table}"
            return f"{db_type} database {db_name}, table: {table}"

        return f"{db_type} database {db_name}, custom query"
