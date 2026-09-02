"""BigQuery destination implementation.

The re-exports resolve on first attribute access (PEP 562) rather than at
package-import time; see ``dlt_saga.utility.secrets`` for the deadlock this
avoids.
"""

from typing import TYPE_CHECKING

from dlt_saga.utility.lazy import lazy_exports

if TYPE_CHECKING:  # pragma: no cover - for type checkers only, never at runtime
    from dlt_saga.destinations.bigquery.access import BigQueryAccessManager
    from dlt_saga.destinations.bigquery.config import BigQueryDestinationConfig
    from dlt_saga.destinations.bigquery.destination import BigQueryDestination

# Public name -> submodule that defines it. Eager imports here would put this
# package in a module-lock cycle with ``dlt_saga.destinations.factory``, which
# imports these submodules directly.
_EXPORTS = {
    "BigQueryAccessManager": "access",
    "BigQueryDestinationConfig": "config",
    "BigQueryDestination": "destination",
}

__all__ = [
    "BigQueryDestination",
    "BigQueryDestinationConfig",
    "BigQueryAccessManager",
]

__getattr__, __dir__ = lazy_exports(__name__, _EXPORTS, globals())
