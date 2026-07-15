"""Factory for building HistorizeRunner instances.

Centralizes the HistorizeRunner construction logic that was previously
duplicated between cli.py and session.py.
"""

import logging
from typing import Any, Dict, Optional, Tuple

from dlt_saga.historize.config import HistorizeConfig
from dlt_saga.historize.runner import HistorizeRunner
from dlt_saga.pipeline_config import PipelineConfig
from dlt_saga.utility.naming import resolve_historized_target

logger = logging.getLogger(__name__)


def _resolve_historize_table_format(
    historize_config: HistorizeConfig,
    config_dict: Dict[str, Any],
    context: Any,
) -> str:
    """Resolve the effective table_format for the historize layer.

    Resolution chain (first non-None wins):
    1. pipeline.historize.table_format  (historize_config.table_format)
    2. pipeline.table_format            (config_dict["table_format"])
    3. profile.historize.table_format   (context.get_historize_table_format())
    4. profile.table_format             (context.get_table_format())
    5. "native"
    """
    return (
        historize_config.table_format
        or config_dict.get("table_format")
        or context.get_historize_table_format()
        or context.get_table_format()
        or "native"
    )


def _segments_from_config_path(config_path: str) -> list:
    """Derive naming segments (``[group, sub_1, ..., name]``) from a config path.

    Returns an empty list when the path can't be resolved into segments
    (no ``configs/`` ancestor, missing path, etc.) — callers fall back to
    behaviour that doesn't depend on segments.
    """
    from pathlib import Path

    if not config_path:
        return []
    parts = Path(config_path).parts
    if "configs" in parts:
        idx = parts.index("configs")
        segments = list(parts[idx + 1 :])
    else:
        segments = list(parts)
    if not segments:
        return []
    last = segments[-1]
    for ext in (".yml", ".yaml"):
        if last.endswith(ext):
            segments[-1] = last[: -len(ext)]
            break
    return segments


def _env_aware_override(
    config_dict: Dict[str, Any],
    override: str,
    environment: Optional[str] = None,
) -> str:
    """Run ``output_table`` through the project's table-name generator so it
    gets the same env-aware prefix as the source table.

    ``environment`` pins the resolution (the collision guard passes ``"prod"``);
    when omitted it reads the active environment. Falls back to the literal
    override when no ``config_path`` is available.
    """
    from dlt_saga.pipeline_config.naming import resolve_table_name_with_leaf
    from dlt_saga.project_config import get_project_config
    from dlt_saga.utility.naming import get_environment

    segments = _segments_from_config_path(config_dict.get("config_path") or "")
    if not segments:
        return override

    project_config = get_project_config()
    return resolve_table_name_with_leaf(
        segments,
        override,
        environment or get_environment(),
        {"naming_module": project_config.naming_module},
    )


def _apply_naming_module_historize_overrides(
    historize_config: HistorizeConfig,
    config_dict: Dict[str, Any],
    source_schema: str,
    source_table: str,
    environment: Optional[str] = None,
) -> None:
    """Populate ``historize_config.output_schema`` / ``output_table`` from a
    custom naming module's ``layer="historize"`` hooks, when present.

    The naming module owns layer-aware naming: if it returns a value
    distinct from the ingest layer, the historize layer adopts that name.
    Per-pipeline overrides already set on ``historize_config`` win — they're
    the highest-priority signal. The framework default (no naming module
    or layer-agnostic hook) leaves the config untouched and the existing
    placement-suffix logic continues to apply.
    """
    from dlt_saga.pipeline_config.naming import (
        call_hook,
        load_naming_module,
    )
    from dlt_saga.project_config import get_project_config
    from dlt_saga.utility.cli.context import get_execution_context
    from dlt_saga.utility.naming import get_dev_schema

    segments = _segments_from_config_path(config_dict.get("config_path") or "")
    if not segments:
        return

    project_config = get_project_config()
    module = load_naming_module({"naming_module": project_config.naming_module})
    if not module:
        return

    context = get_execution_context()
    environment = environment or context.get_environment() or "dev"
    # Only resolve the dev schema in dev; in prod the schema is derived from
    # segments and default_schema is unused, and a dev schema is legitimately
    # unset there (get_dev_schema raises). Pinning environment="prod" (the
    # collision guard) must not read the live dev schema.
    if environment == "prod":
        default_schema = ""
    else:
        default_schema = context.get_schema() or get_dev_schema()

    if historize_config.output_schema is None and hasattr(
        module, "generate_schema_name"
    ):
        try:
            hist_schema = call_hook(
                module.generate_schema_name,
                segments,
                environment,
                default_schema,
                layer="historize",
            )
        except Exception as exc:
            logger.debug(
                "naming_module.generate_schema_name(layer='historize') raised: %s",
                exc,
            )
            hist_schema = None
        if hist_schema and hist_schema != source_schema:
            historize_config.output_schema = hist_schema

    if historize_config.output_table is None and hasattr(module, "generate_table_name"):
        try:
            hist_table = call_hook(
                module.generate_table_name,
                segments,
                environment,
                layer="historize",
            )
        except Exception as exc:
            logger.debug(
                "naming_module.generate_table_name(layer='historize') raised: %s",
                exc,
            )
            hist_table = None
        if hist_table and hist_table != source_table:
            historize_config.output_table = hist_table


def _resolve_historize_storage_path(context: Any) -> Optional[str]:
    """Resolve the effective storage_path for the historize layer.

    Resolution chain (first non-None wins):
    1. profile.historize.storage_path  (context.get_historize_storage_path())
    2. profile.storage_path            (context.get_storage_path())
    """
    return context.get_historize_storage_path() or context.get_storage_path()


def _resolve_historize_schema_access(
    config_dict: Dict[str, Any],
) -> Optional[list]:
    """Resolve the access list that applies to the historize schema.

    ``historize_schema_access`` is an overlay over ``schema_access``: the
    final list is the deduplicated union of the two. When
    ``historize_schema_access`` is unset, returns ``schema_access``
    unchanged (the default — historize gets the same grants as ingest,
    which is correct when raw and historize share one schema under
    ``placement: table_suffix``).

    Order is preserved: ingest entries come first, then any new historize
    entries. Returns ``None`` when neither list is set.
    """
    ingest_access = config_dict.get("schema_access")
    historize_access = config_dict.get("historize_schema_access")

    if not historize_access:
        return ingest_access

    base = list(ingest_access) if ingest_access else []
    seen = set(base)
    overlay = []
    for entry in historize_access:
        if entry not in seen:
            seen.add(entry)
            overlay.append(entry)
    return base + overlay


def resolve_historize_target(
    pipeline_config: PipelineConfig,
    *,
    environment: Optional[str] = None,
    source_schema: Optional[str] = None,
    source_table: Optional[str] = None,
) -> Tuple[HistorizeConfig, str, str]:
    """Resolve the historize layer's ``(schema, table)`` target for a pipeline.

    Runs the historize name-resolution chain: ``HistorizeConfig`` construction,
    the env-aware ``output_table`` override, custom naming-module layer
    overrides, and the placement-strategy resolution. Stops short of building a
    destination — :func:`build_historize_runner` calls this and then builds one;
    collision detection (``utility/collisions.py``) calls it pinned to prod.

    Requires an active execution context (naming-module overrides and placement
    resolution read it).

    Args:
        pipeline_config: The pipeline whose historize target to resolve.
        environment: Pins resolution to a specific environment (``"prod"`` /
            ``"dev"``); when omitted it reads the active one. The collision
            guard passes ``"prod"`` for an environment-invariant verdict.
        source_schema: Override for the source (ingest) schema the historize
            target derives from; defaults to ``pipeline_config.schema_name``.
            The guard passes the config's *prod* ingest schema.
        source_table: Override for the source (ingest) table; defaults to
            ``pipeline_config.table_name``.

    Returns:
        ``(historize_config, target_schema, target_table)``. The returned
        ``historize_config`` carries the name-related mutations
        (``output_table`` / ``output_schema``) but not the ``table_format`` /
        ``storage_path`` resolution, which is destination-facing and left to
        :func:`build_historize_runner`.
    """
    config_dict = pipeline_config.config_dict

    historize_dict = config_dict.get("historize") or {}
    top_level_pk = config_dict.get("primary_key")
    if isinstance(top_level_pk, str):
        top_level_pk = [top_level_pk]

    historize_config = HistorizeConfig.from_dict(historize_dict, top_level_pk)
    schema_name = (
        source_schema if source_schema is not None else pipeline_config.schema_name
    )
    table_name = (
        source_table if source_table is not None else pipeline_config.table_name
    )

    # When output_table is set, run it through the same table-name generator
    # as the source so it picks up the env prefix and any custom naming rules
    # — i.e. treat the override as a peer of the source, not a literal name.
    if historize_config.output_table:
        historize_config.output_table = _env_aware_override(
            config_dict, historize_config.output_table, environment=environment
        )

    # Apply layer-aware naming-module overrides for the historize layer
    # (sets output_schema / output_table from a custom naming module when
    # it returns a value distinct from the ingest layer). Runs before
    # resolve_historized_target so per-pipeline output_* overrides win, while
    # naming-module overrides supersede the placement suffix default.
    _apply_naming_module_historize_overrides(
        historize_config,
        config_dict,
        source_schema=schema_name,
        source_table=table_name,
        environment=environment,
    )

    # Resolve historize schema and table using the configured placement strategy.
    target_schema, target_table = resolve_historized_target(
        source_schema=schema_name,
        source_table=table_name,
        historize_config=historize_config,
    )
    return historize_config, target_schema, target_table


def build_historize_runner(
    pipeline_config: PipelineConfig,
    full_refresh: bool,
    partial_refresh: bool = False,
    historize_from: Optional[str] = None,
    log_prefix: Optional[str] = None,
) -> HistorizeRunner:
    """Build a HistorizeRunner from a pipeline config and execution context.

    Args:
        pipeline_config: The pipeline configuration to historize.
        full_refresh: Whether to rebuild from scratch.
        partial_refresh: Whether to do a partial rebuild from earliest
            available raw snapshot.
        historize_from: ISO date/datetime to reprocess from.

    Returns:
        A configured HistorizeRunner ready to call ``.run()``.
    """
    from dlt_saga.destinations.factory import DestinationFactory
    from dlt_saga.utility.cli.context import get_execution_context

    config_dict = pipeline_config.config_dict
    context = get_execution_context()

    # Resolve the historize (schema, table) target — the shared name-resolution
    # chain used by collision detection too. Returns the HistorizeConfig with
    # its name-related mutations already applied.
    historize_config, target_schema, target_table_name = resolve_historize_target(
        pipeline_config
    )

    destination_type = context.get_destination_type()
    schema_name = pipeline_config.schema_name

    # Resolve the effective table_format and storage_path for the historize layer
    table_format = _resolve_historize_table_format(
        historize_config, config_dict, context
    )
    storage_path = _resolve_historize_storage_path(context)

    # Set resolved table_format on the config so HistorizeSqlBuilder reads it
    historize_config.table_format = table_format

    # Pin resolved target_schema on historize_config so HistorizeRunner uses it
    # everywhere (state manager, partial refresh, etc.)
    if target_schema != schema_name and historize_config.output_schema is None:
        historize_config.output_schema = target_schema

    # Resolve the historize-layer schema_access overlay and warn loudly if
    # the overlay was declared but the historize schema isn't distinct from
    # the ingest schema — the overlay would silently leak into the ingest
    # schema's grants, mixing concerns.
    historize_access = _resolve_historize_schema_access(config_dict)
    if config_dict.get("historize_schema_access") and target_schema == schema_name:
        logger.warning(
            "historize_schema_access is set on %r but the historize schema "
            "(%s) is the same as the ingest schema. The overlay would apply "
            "to the shared schema and merge with schema_access — either "
            "consolidate into schema_access or configure placement: "
            "schema_suffix / a custom naming_module to make the historize "
            "schema distinct.",
            pipeline_config.pipeline_name,
            target_schema,
        )

    # Build dest_config_dict with historize-resolved table_format (and storage_path
    # for BigQuery Iceberg).  Both override what from_context() would read from the
    # context/profile so the destination instance reflects the historize-layer settings.
    # ``schema_access`` is set to the historize overlay so HistorizeRunner can
    # apply the right grants when syncing the historize target schema.
    dest_config_dict: Dict[str, Any] = {
        **config_dict,
        "schema_name": schema_name,
        "table_format": table_format,
    }
    if table_format == "iceberg" and storage_path:
        dest_config_dict["storage_path"] = storage_path
    if historize_access is not None:
        dest_config_dict["schema_access"] = historize_access

    destination = DestinationFactory.create_from_context(
        destination_type, context, dest_config_dict
    )
    database = getattr(destination.config, "project_id", None) or getattr(
        destination.config, "catalog", "local"
    )

    return HistorizeRunner(
        pipeline_name=pipeline_config.pipeline_name,
        historize_config=historize_config,
        destination=destination,
        database=database,
        schema=schema_name,
        source_table_name=pipeline_config.table_name,
        target_table_name=target_table_name,
        config_dict=pipeline_config.config_dict,
        full_refresh=full_refresh,
        partial_refresh=partial_refresh,
        historize_from=historize_from,
        log_prefix=log_prefix,
    )
