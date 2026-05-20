"""Schema and table naming utilities with environment-aware logic.

This module provides environment detection and generic naming helpers.
Schema and table name resolution is the responsibility of each ConfigSource
implementation (e.g., FilePipelineConfig derives names from config file paths).

Environment-based conventions:
- Dev (SAGA_ENVIRONMENT != 'prod'):
  - Single schema for all pipelines (e.g., 'dlt_dev')
  - Execution plans in same dev schema

- Prod (SAGA_ENVIRONMENT == 'prod'):
  - Separate schemas per config grouping
  - Execution plans in dedicated 'dlt_orchestration' schema
"""

import logging
from typing import TYPE_CHECKING, Tuple

from dlt_saga.utility.env import get_env

if TYPE_CHECKING:
    from dlt_saga.historize.config import HistorizeConfig

logger = logging.getLogger(__name__)


def get_environment() -> str:
    """Get current environment from profile or SAGA_ENVIRONMENT env var.

    Priority:
    1. Active profile target's environment setting
    2. SAGA_ENVIRONMENT env var (falls back to DLT_ENVIRONMENT with warning)

    Returns:
        Environment string ('prod' or 'dev')
    """
    # Check if we have a profile target with environment setting
    try:
        from dlt_saga.utility.cli.context import get_execution_context

        context = get_execution_context()
        env = context.get_environment()
        if env:
            return env
    except ImportError:
        pass  # context module not available yet

    # Fall back to environment variable
    return get_env("SAGA_ENVIRONMENT", "dev")


def is_production() -> bool:
    """Check if running in production environment.

    Returns:
        True if environment == 'prod', False otherwise
    """
    return get_environment() == "prod"


def get_dev_schema() -> str:
    """Get dev schema name from profile or environment variable.

    Returns:
        Dev schema name
    """
    try:
        from dlt_saga.utility.cli.context import get_execution_context

        context = get_execution_context()
        schema = context.get_schema()
        if schema:
            return schema
    except ImportError:
        pass

    return get_env("SAGA_SCHEMA_NAME", "dlt_dev")


def resolve_historized_target(
    source_schema: str,
    source_table: str,
    historize_config: "HistorizeConfig",
) -> Tuple[str, str]:
    """Return (historize_schema, historize_table) applying the configured placement strategy.

    Resolution order:
    - historize_schema:
        1. historize_config.output_schema (explicit per-pipeline override)
        2. if placement == schema_suffix: ``{source_schema}{schema_suffix}``
        3. source_schema (same schema as source)
    - historize_table:
        1. historize_config.output_table (explicit per-pipeline override)
        2. if placement == schema_suffix: source_table (no table-level suffix)
        3. ``{source_table}{historize_config.output_table_suffix}``

    Args:
        source_schema: Schema where the source ingested table lives.
        source_table: Table name of the source ingested table.
        historize_config: HistorizeConfig for this pipeline (may carry output_schema /
            output_table overrides).

    Returns:
        Tuple of (historize_schema, historize_table).
    """
    from dlt_saga.project_config import get_historize_project_config

    proj = get_historize_project_config()

    # --- Schema resolution ---
    if historize_config.output_schema:
        historize_schema = historize_config.output_schema
    elif proj.placement == "schema_suffix":
        historize_schema = f"{source_schema}{proj.schema_suffix}"
    else:
        historize_schema = source_schema

    # --- Table resolution ---
    if historize_config.output_table:
        historize_table = historize_config.output_table
    elif proj.placement == "schema_suffix":
        # No table-level suffix when using schema separation
        historize_table = source_table
    else:
        historize_table = get_historized_table_name(
            source_table, historize_config.output_table_suffix
        )

    return historize_schema, historize_table


def get_historized_table_name(base_table_name: str, suffix: str = "_historized") -> str:
    """Get the historized table name by appending a suffix.

    Args:
        base_table_name: Base table name (e.g., 'proffdata__bedriftsdata')
        suffix: Suffix to append (default: '_historized')

    Returns:
        Historized table name (e.g., 'proffdata__bedriftsdata_historized')
    """
    return f"{base_table_name}{suffix}"


def get_execution_plan_schema() -> str:
    """Get schema name for execution plans.

    Resolution order:
    1. ``orchestration.schema`` in ``saga_project.yml`` (explicit override)
    2. ``dlt_orchestration`` when environment is prod
    3. Developer-specific schema (``SAGA_SCHEMA_NAME`` or ``dlt_dev``) in dev

    Returns:
        Schema name for execution plans
    """
    from dlt_saga.project_config import get_orchestration_config

    config = get_orchestration_config()
    if config.schema:
        return config.schema

    if is_production():
        return "dlt_orchestration"
    else:
        return get_dev_schema()
