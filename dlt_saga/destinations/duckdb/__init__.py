"""DuckDB destination implementation.

The re-exports resolve on first attribute access (PEP 562) rather than at
package-import time; see ``dlt_saga.utility.secrets`` for the deadlock this
avoids.
"""

from typing import TYPE_CHECKING

from dlt_saga.utility.lazy import lazy_exports

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

__getattr__, __dir__ = lazy_exports(__name__, _EXPORTS, globals())
