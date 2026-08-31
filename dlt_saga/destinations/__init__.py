"""Destination implementations for different data warehouses.

This package provides a modular architecture for supporting multiple destinations
(BigQuery, Snowflake, Postgres, etc.) with a consistent interface.

The re-exports resolve on first attribute access (PEP 562) rather than at
package-import time; see ``dlt_saga.utility.secrets`` for the deadlock this
avoids. ``DestinationFactory`` still registers every built-in destination the
moment it is touched, since touching it imports
``dlt_saga.destinations.factory``.
"""

from importlib import import_module
from typing import TYPE_CHECKING, Any

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


# The eager __init__ also published these submodules as package attributes
# (a side effect of importing them), so keep answering for them by name.
_SUBMODULES = frozenset(_EXPORTS.values())


def __getattr__(name: str) -> Any:
    """Import the defining submodule on first access, then cache the result."""
    if name in _SUBMODULES:
        value: Any = import_module(f"{__name__}.{name}")
    else:
        submodule = _EXPORTS.get(name)
        if submodule is None:
            raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
        value = getattr(import_module(f"{__name__}.{submodule}"), name)
    globals()[name] = value  # __getattr__ is only consulted on a miss
    return value


def __dir__() -> list[str]:
    return sorted(set(globals()) | set(_EXPORTS) | set(_SUBMODULES))
