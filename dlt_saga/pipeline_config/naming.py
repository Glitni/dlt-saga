"""Default name and location generation for pipeline configs.

These functions implement the framework's default rules for deriving
schema names, table names, and external-table locations from a pipeline
config identifier. They are the canonical defaults used when no custom
``naming_module`` is configured in ``saga_project.yml`` — and the same
implementations users typically copy or wrap when they do define one.

Identifier contract — segments
------------------------------

The naming functions operate on an ordered list of strings — *segments* —
that uniquely identify a pipeline config. The contract is:

- ``segments[0]`` is the **pipeline group** (top-level partition of the
  config space, e.g. ``"google_sheets"``).
- ``segments[-1]`` is the **base name** (leaf identifier of the config).
- Anything in between adds organisational depth.

For the file-based config source, segments are the relative path parts with
the file extension stripped — e.g. ``["google_sheets", "asm", "salgsmal"]``
for ``configs/google_sheets/asm/salgsmal.yml``. Other config sources
(database-backed, SharePoint-backed, …) produce their own segments from
whatever identifier shape they use; the naming functions don't care.

Custom naming module
--------------------

Users override any subset of these defaults by pointing
``naming_module: <importable.module>`` in ``saga_project.yml`` at a Python
module that defines one or more of:

- ``generate_schema_name(segments, environment, default_schema) -> str``
- ``generate_table_name(segments, environment) -> str``
- ``generate_target_location(segments, environment, default_storage_root) -> Optional[str]``

Missing functions fall back to the defaults below. Users typically import
these defaults inside their custom module and wrap them with their own
logic — see the docstrings on each function for examples.
"""

import importlib
import logging
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)

# Cached custom naming module (None = not loaded, False = no custom module)
_naming_module: Any = None


def load_naming_module(project_config: Dict[str, Any]) -> Any:
    """Load the custom naming module declared in ``saga_project.yml``.

    Result is cached on first call. Missing optional functions are logged at
    debug level only — all three hooks are genuinely optional and fall back
    to the framework defaults in this module.

    Args:
        project_config: Parsed ``saga_project.yml`` contents.

    Returns:
        The imported module, or ``False`` if no custom module is configured.
    """
    global _naming_module
    if _naming_module is not None:
        return _naming_module

    try:
        module_name = project_config.get("naming_module")
        if not module_name:
            _naming_module = False
            return _naming_module

        _naming_module = importlib.import_module(module_name)
        logger.debug(f"Loaded custom naming module: {module_name}")

        for fn_name in (
            "generate_schema_name",
            "generate_table_name",
            "generate_target_location",
        ):
            if not hasattr(_naming_module, fn_name):
                logger.debug(
                    f"Custom naming module '{module_name}' does not define "
                    f"{fn_name}(); using framework default for that function"
                )

        return _naming_module

    except Exception as e:
        logger.warning(f"Failed to load custom naming module: {e}, using defaults")
        _naming_module = False
        return _naming_module


def _sanitize_segment(s: str) -> str:
    """Lowercase and replace common SQL-unsafe separators with underscores."""
    return s.lower().replace("-", "_").replace(" ", "_")


def default_generate_schema_name(
    segments: List[str], environment: str, default_schema: str
) -> str:
    """Default schema name generation from config identifier segments.

    Prod: ``dlt_{segments[0]}`` (e.g. ``dlt_google_sheets``).
    Dev: ``default_schema`` (from profile or ``SAGA_SCHEMA_NAME``).

    Args:
        segments: Ordered identifier segments. ``segments[0]`` is the group.
            Empty list falls back to ``"default"``.
        environment: Current environment (``"prod"`` or ``"dev"``).
        default_schema: Dev schema name from profile/env var.
    """
    if environment == "prod":
        first_segment = segments[0] if segments else "default"
        return f"dlt_{first_segment}"
    return default_schema


def default_generate_table_name(segments: List[str], environment: str) -> str:
    """Default table name generation from config identifier segments.

    Prod: ``segments[1:]`` joined with ``__`` (or ``segments[0]`` when there's
    only one segment), e.g. ``"asm__salgsmal"``.
    Dev: ``"{segments[0]}__{prod_name}"``, e.g. ``"google_sheets__asm__salgsmal"``.

    Inner segments are lowercased and have ``-`` / spaces replaced with ``_``
    so they're safe to use as SQL identifiers.

    Args:
        segments: Ordered identifier segments. Empty list returns
            ``"default_data"``.
        environment: Current environment (``"prod"`` or ``"dev"``).
    """
    if not segments:
        return "default_data"

    first_segment = segments[0]

    if len(segments) == 1:
        base_name = _sanitize_segment(segments[0])
    elif len(segments) == 2:
        base_name = _sanitize_segment(segments[1])
    else:
        base_name = "__".join(_sanitize_segment(s) for s in segments[1:])

    if environment == "prod":
        return base_name
    return f"{first_segment}__{base_name}"


def default_generate_target_location(
    segments: List[str],
    environment: str,
    default_storage_root: Optional[str],
    pipeline_group: Optional[str] = None,
    table_name: Optional[str] = None,
) -> Optional[str]:
    """Default external-table LOCATION generation for native_load on Databricks.

    Returns ``<default_storage_root>/<pipeline_group>/<table_name>/`` or
    ``None`` when no ``default_storage_root`` is configured (caller should
    then fall back to a managed table).

    The kwargs ``pipeline_group`` and ``table_name`` exist so callers that
    have already resolved them (e.g. through a custom ``generate_table_name``)
    can pass them in; when omitted, both are derived from ``segments`` using
    framework defaults.

    Args:
        segments: Ordered identifier segments. ``segments[0]`` is the group.
        environment: Current environment (``"prod"`` or ``"dev"``).
        default_storage_root: Profile's ``storage_root`` (e.g.
            ``"abfss://lake@<account>.dfs.core.windows.net/raw/"``) or ``None``.
        pipeline_group: Optional pre-resolved pipeline group; defaults to
            ``segments[0]``.
        table_name: Optional pre-resolved table name; defaults to
            ``default_generate_table_name(segments, environment)``.

    Returns:
        Full external-table URI, or ``None`` to indicate "use a managed table".
    """
    if not default_storage_root:
        return None
    if pipeline_group is None:
        pipeline_group = segments[0] if segments else "default"
    if table_name is None:
        table_name = default_generate_table_name(segments, environment)
    return f"{default_storage_root.rstrip('/')}/{pipeline_group}/{table_name}/"
