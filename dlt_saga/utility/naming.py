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
from typing import TYPE_CHECKING, Optional, Tuple

from dlt_saga.utility.env import get_env


def normalize_identifier(name: str, max_length: Optional[int] = None) -> str:
    """Normalize a SQL identifier using dlt's snake_case naming convention.

    dlt normalizes identifiers when it creates schemas/tables/columns (e.g.
    ``OrderItem_ID`` → ``order_item_id``), so any code that matches
    config-declared names against what dlt actually created — or that builds
    identifiers to hand back to dlt — must apply the same normalization.
    Destination-agnostic; dlt normalizes identically regardless of destination.

    ``max_length`` enables dlt's length-capping (with hashing), e.g. BigQuery's
    64-character schema-name limit.
    """
    from dlt.common.normalizers.naming.snake_case import NamingConvention

    return NamingConvention(max_length=max_length).normalize_identifier(name)


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
    """Resolve the developer's dev schema.

    Resolution: profile ``dataset``/``schema`` (from the active target) →
    ``SAGA_SCHEMA_NAME``. There is deliberately **no** shared default: an
    unresolved dev schema is a configuration error, not a silent fallback to a
    shared ``dlt_dev`` dataset. A shared default silently collides every
    unconfigured developer's data, historize logs, and execution-plan tables in
    one dataset — a misconfiguration that reads as success.

    Returns:
        The developer's dev schema name.

    Raises:
        ValueError: when neither the profile nor ``SAGA_SCHEMA_NAME`` provides a
            schema.
    """
    try:
        from dlt_saga.utility.cli.context import get_execution_context

        context = get_execution_context()
        schema = context.get_schema()
        if schema:
            return schema
    except ImportError:
        pass

    env_schema = get_env("SAGA_SCHEMA_NAME")
    if env_schema:
        return env_schema

    raise ValueError(
        "No dev schema configured. Set `dataset` (or `schema`) on your "
        "profile's dev target in profiles.yml, or export "
        "SAGA_SCHEMA_NAME=<your-schema>."
    )


def resolve_historized_target(
    source_schema: str,
    source_table: str,
    historize_config: "HistorizeConfig",
) -> Tuple[str, str]:
    """Return (historize_schema, historize_table) applying the configured placement strategy.

    Resolution order:
    - historize_schema:
        1. historize_config.schema_name (explicit per-pipeline override) —
           assumed already env-composed (caller runs the override through the
           schema generator before passing it in).
        2. if placement == schema_suffix: ``{source_schema}{schema_suffix}``
        3. source_schema (same schema as source)
    - historize_table:
        1. historize_config.table_name (explicit per-pipeline override) —
           assumed already env-aware (caller is responsible for running the
           override through the project's table-name generator before
           passing it in).
        2. if placement == schema_suffix: source_table (no table-level suffix)
        3. ``{source_table}{historize_config.table_suffix}``

    Args:
        source_schema: Schema where the source ingested table lives.
        source_table: Table name of the source ingested table (env-prefixed in dev).
        historize_config: HistorizeConfig for this pipeline (may carry schema_name /
            table_name overrides).

    Returns:
        Tuple of (historize_schema, historize_table).
    """
    from dlt_saga.project_config import get_historize_project_config

    proj = get_historize_project_config()

    # --- Schema resolution ---
    if historize_config.schema_name:
        historize_schema = historize_config.schema_name
    elif proj.placement == "schema_suffix":
        historize_schema = f"{source_schema}{proj.schema_suffix}"
    else:
        historize_schema = source_schema

    # --- Table resolution ---
    if historize_config.table_name:
        historize_table = historize_config.table_name
    elif proj.placement == "schema_suffix":
        # No table-level suffix when using schema separation
        historize_table = source_table
    else:
        historize_table = get_historized_table_name(
            source_table, historize_config.table_suffix
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

    The ``orchestration.*`` config is prod-only infrastructure (how remote
    workers coordinate). Dev runs are always local — they touch the
    execution-plan / executions tables only to record local run telemetry — so
    they must stay in the developer's own schema and never read
    ``orchestration.schema`` (which would collide every developer's runs in one
    shared dataset).

    Resolution:
    - prod: ``orchestration.schema`` (explicit override) else ``dlt_orchestration``
    - dev: the developer's schema (:func:`get_dev_schema`)

    Returns:
        Schema name for execution plans
    """
    if is_production():
        from dlt_saga.project_config import get_orchestration_config

        return get_orchestration_config().schema or "dlt_orchestration"

    return get_dev_schema()
