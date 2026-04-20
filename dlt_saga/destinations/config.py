"""Base configuration classes for destinations.

This module contains only the generic base configuration class.
Destination-specific configurations are in their respective modules
(e.g., destinations/bigquery/config.py).
"""

from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Dict, Optional

if TYPE_CHECKING:
    from dlt_saga.utility.cli.context import ExecutionContext


@dataclass
class DestinationConfig:
    """Base configuration for all destinations.

    Destination-specific subclasses should be defined in their
    respective modules (e.g., BigQueryDestinationConfig in
    destinations/bigquery/config.py).
    """

    destination_type: str = "bigquery"
    location: Optional[str] = None

    @property
    def database(self) -> str:
        """The top-level database/project/catalog identifier for this destination.

        Subclasses must override this to return the destination-specific field
        (e.g. ``project_id`` for BigQuery, ``catalog`` for Databricks).
        Used for logging and pipeline state isolation.
        """
        raise NotImplementedError(f"{type(self).__name__} must implement database")

    @classmethod
    def from_context(
        cls,
        context: "ExecutionContext",
        config_dict: Dict[str, Any],
    ) -> "DestinationConfig":
        """Resolve destination config from execution context and pipeline config.

        Each destination subclass overrides this to pull the values it needs
        from the context (profile) and config_dict (pipeline YAML).

        Args:
            context: Active execution context (profile target, env vars, etc.)
            config_dict: Pipeline-level configuration dictionary

        Returns:
            Fully resolved DestinationConfig subclass instance
        """
        raise NotImplementedError(f"{cls.__name__} must implement from_context()")

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "DestinationConfig":
        """Create config from a pre-built dictionary.

        Each destination subclass overrides this.
        """
        raise NotImplementedError(f"{cls.__name__} must implement from_dict()")
