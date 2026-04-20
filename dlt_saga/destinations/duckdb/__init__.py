"""DuckDB destination implementation."""

from dlt_saga.destinations.duckdb.config import DuckDBDestinationConfig
from dlt_saga.destinations.duckdb.destination import DuckDBDestination

__all__ = ["DuckDBDestination", "DuckDBDestinationConfig"]
