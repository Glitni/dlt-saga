"""Destination implementations for different data warehouses.

This package provides a modular architecture for supporting multiple destinations
(BigQuery, Snowflake, Postgres, etc.) with a consistent interface.
"""

from dlt_saga.destinations.base import AccessManager, Destination
from dlt_saga.destinations.factory import DestinationFactory

__all__ = ["DestinationFactory", "Destination", "AccessManager"]
