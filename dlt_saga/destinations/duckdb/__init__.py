"""DuckDB destination implementation.

The re-exports resolve on first attribute access (PEP 562) rather than at
package-import time; see ``dlt_saga.utility.secrets`` for the deadlock this
avoids.
"""

from importlib import import_module
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:  # pragma: no cover - for type checkers only, never at runtime
    from dlt_saga.destinations.duckdb.config import DuckDBDestinationConfig
    from dlt_saga.destinations.duckdb.destination import DuckDBDestination

# Public name -> submodule that defines it. Eager imports here would put this
# package in a module-lock cycle with ``dlt_saga.destinations.factory`` and
# ``dlt_saga.testing``, which import these submodules directly.
_EXPORTS = {
    "DuckDBDestinationConfig": "config",
    "DuckDBDestination": "destination",
}

__all__ = ["DuckDBDestination", "DuckDBDestinationConfig"]


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
