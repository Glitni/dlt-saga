"""Destination implementations for different data warehouses.

This package provides a modular architecture for supporting multiple destinations
(BigQuery, Snowflake, Postgres, etc.) with a consistent interface.

The re-exports resolve on first attribute access (PEP 562) rather than at
package-import time; see ``dlt_saga.utility.secrets`` for the deadlock this
avoids. ``DestinationFactory`` still registers every built-in destination the
moment it is touched, since touching it imports
``dlt_saga.destinations.factory``.
"""

from typing import TYPE_CHECKING

from dlt_saga.utility.lazy import lazy_exports

if TYPE_CHECKING:  # pragma: no cover - for type checkers only, never at runtime
    from dlt_saga.destinations.base import AccessManager, Destination
    from dlt_saga.destinations.factory import DestinationFactory

# Public name -> submodule that defines it. Eager imports here would put this
# package in a module-lock cycle with the modules that import
# ``dlt_saga.destinations.base`` / ``.factory`` directly.
_EXPORTS = {
    "AccessManager": "base",
    "Destination": "base",
    "DestinationFactory": "factory",
}

__all__ = ["DestinationFactory", "Destination", "AccessManager"]

__getattr__, __dir__ = lazy_exports(__name__, _EXPORTS, globals())
