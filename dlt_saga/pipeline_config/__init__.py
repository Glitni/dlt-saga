"""Pipeline configuration management.

Provides abstractions for discovering and managing pipeline configurations
from different sources (files, SharePoint, databases, etc.).

The ``default_generate_*`` functions implement the framework's default rules
for deriving schema names, table names, and external-table locations from a
pipeline config identifier. They live in
:mod:`dlt_saga.pipeline_config.naming` and are re-exported here so users
writing a custom ``naming_module`` (configured via ``naming_module:`` in
``saga_project.yml``) can import, copy, or wrap them as a starting point.

The re-exports resolve on first attribute access (PEP 562) rather than at
package-import time; see ``dlt_saga.utility.secrets`` for the deadlock this
avoids.
"""

from importlib import import_module
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:  # pragma: no cover - for type checkers only, never at runtime
    from dlt_saga.pipeline_config.base_config import ConfigSource, PipelineConfig
    from dlt_saga.pipeline_config.file_config import FilePipelineConfig
    from dlt_saga.pipeline_config.naming import (
        default_generate_schema_name,
        default_generate_table_name,
        default_generate_target_location,
    )

# Public name -> submodule that defines it. Eager imports here would put this
# package in a module-lock cycle with the many modules that import
# ``dlt_saga.pipeline_config.base_config`` / ``.naming`` directly.
_EXPORTS = {
    "ConfigSource": "base_config",
    "PipelineConfig": "base_config",
    "FilePipelineConfig": "file_config",
    "default_generate_schema_name": "naming",
    "default_generate_table_name": "naming",
    "default_generate_target_location": "naming",
}

__all__ = [
    "ConfigSource",
    "PipelineConfig",
    "FilePipelineConfig",
    "default_generate_schema_name",
    "default_generate_table_name",
    "default_generate_target_location",
]


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
