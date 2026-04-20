"""Factory for creating destination instances based on type."""

import logging
from typing import TYPE_CHECKING, Any, Dict, Type

from dlt_saga.destinations.base import Destination
from dlt_saga.destinations.bigquery.config import BigQueryDestinationConfig
from dlt_saga.destinations.bigquery.destination import BigQueryDestination
from dlt_saga.destinations.config import DestinationConfig
from dlt_saga.destinations.databricks.config import DatabricksDestinationConfig
from dlt_saga.destinations.databricks.destination import DatabricksDestination
from dlt_saga.destinations.duckdb.config import DuckDBDestinationConfig
from dlt_saga.destinations.duckdb.destination import DuckDBDestination

if TYPE_CHECKING:
    from dlt_saga.utility.cli.context import ExecutionContext

logger = logging.getLogger(__name__)


class DestinationFactory:
    """Factory for creating destination instances.

    Uses a registry pattern to map destination types to their implementations.
    New destinations can be registered using the register() class method.
    """

    _registry: Dict[str, Type[Destination]] = {}
    _config_registry: Dict[str, Type[DestinationConfig]] = {}

    @classmethod
    def register(
        cls,
        destination_type: str,
        destination_class: Type[Destination],
        config_class: Type[DestinationConfig],
    ) -> None:
        """Register a destination implementation.

        Args:
            destination_type: Destination type identifier (e.g., "bigquery", "snowflake")
            destination_class: Destination class to instantiate for this type
            config_class: Config class with from_dict() and from_context() methods
        """
        cls._registry[destination_type] = destination_class
        cls._config_registry[destination_type] = config_class
        logger.debug(f"Registered destination type: {destination_type}")

    @classmethod
    def _check_registered(cls, destination_type: str) -> None:
        """Raise ValueError if destination_type is not registered."""
        if destination_type not in cls._registry:
            available = ", ".join(cls._registry.keys()) or "none"
            raise ValueError(
                f"Unknown destination type: '{destination_type}'. "
                f"Available types: {available}"
            )

    @classmethod
    def create(cls, destination_type: str, config_dict: Dict[str, Any]) -> Destination:
        """Create a destination instance from a pre-built config dict.

        Args:
            destination_type: Type of destination to create (e.g., "bigquery")
            config_dict: Configuration dictionary with destination-specific settings

        Returns:
            Destination instance

        Raises:
            ValueError: If destination_type is not registered
        """
        cls._check_registered(destination_type)

        config_class = cls._config_registry[destination_type]
        config = config_class.from_dict(config_dict)

        destination_class = cls._registry[destination_type]
        logger.debug(f"Creating {destination_type} destination")
        return destination_class(config)

    @classmethod
    def create_from_context(
        cls,
        destination_type: str,
        context: "ExecutionContext",
        config_dict: Dict[str, Any],
    ) -> Destination:
        """Create a destination by resolving config from execution context.

        Each destination config class knows how to pull the values it needs
        from the context (profile target) and config_dict (pipeline YAML).

        Args:
            destination_type: Type of destination to create
            context: Active execution context
            config_dict: Pipeline-level configuration dictionary

        Returns:
            Destination instance
        """
        cls._check_registered(destination_type)

        config_class = cls._config_registry[destination_type]
        config = config_class.from_context(context, config_dict)

        destination_class = cls._registry[destination_type]
        schema = getattr(config, "schema_name", None) or getattr(
            config, "dataset_name", None
        )
        logger.debug(
            f"Creating {destination_type} destination from context (schema={schema})"
        )
        return destination_class(config)

    @classmethod
    def get_destination_class(cls, destination_type: str) -> Type[Destination]:
        """Get the destination class for a given type.

        Args:
            destination_type: Type of destination (e.g., "bigquery")

        Returns:
            Destination class

        Raises:
            ValueError: If destination_type is not registered
        """
        cls._check_registered(destination_type)
        return cls._registry[destination_type]

    @classmethod
    def get_available_types(cls) -> list[str]:
        """Get list of registered destination types.

        Returns:
            List of destination type identifiers
        """
        return list(cls._registry.keys())


DestinationFactory.register("bigquery", BigQueryDestination, BigQueryDestinationConfig)
DestinationFactory.register(
    "databricks", DatabricksDestination, DatabricksDestinationConfig
)
DestinationFactory.register("duckdb", DuckDBDestination, DuckDBDestinationConfig)
