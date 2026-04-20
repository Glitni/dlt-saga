"""Database pipeline for extracting data using ConnectorX with Apache Arrow.

Supports: PostgreSQL, MySQL, MariaDB, SQL Server, Oracle, SQLite,
         Redshift, ClickHouse, BigQuery, Trino

High-performance extraction with parallel reading and incremental loading.
See README.md or example configs in configs/database/examples/ for usage.
"""

from dlt_saga.pipelines.database.client import DatabaseClient
from dlt_saga.pipelines.database.config import DatabaseConfig
from dlt_saga.pipelines.database.pipeline import DatabasePipeline

__all__ = ["DatabaseClient", "DatabaseConfig", "DatabasePipeline"]
