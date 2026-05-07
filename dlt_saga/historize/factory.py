"""Factory for building HistorizeRunner instances.

Centralizes the HistorizeRunner construction logic that was previously
duplicated between cli.py and session.py.
"""

from typing import Any, Dict, Optional

from dlt_saga.historize.config import HistorizeConfig
from dlt_saga.historize.runner import HistorizeRunner
from dlt_saga.pipeline_config import PipelineConfig
from dlt_saga.utility.naming import resolve_historized_target


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


def _resolve_historize_storage_path(context: Any) -> Optional[str]:
    """Resolve the effective storage_path for the historize layer.

    Resolution chain (first non-None wins):
    1. profile.historize.storage_path  (context.get_historize_storage_path())
    2. profile.storage_path            (context.get_storage_path())
    """
    return context.get_historize_storage_path() or context.get_storage_path()


def build_historize_runner(
    pipeline_config: PipelineConfig,
    full_refresh: bool,
    partial_refresh: bool = False,
    historize_from: Optional[str] = None,
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

    historize_dict = config_dict.get("historize", {})
    top_level_pk = config_dict.get("primary_key")
    if isinstance(top_level_pk, str):
        top_level_pk = [top_level_pk]

    historize_config = HistorizeConfig.from_dict(historize_dict, top_level_pk)

    destination_type = context.get_destination_type()
    schema_name = pipeline_config.schema_name

    # Resolve the effective table_format and storage_path for the historize layer
    table_format = _resolve_historize_table_format(
        historize_config, config_dict, context
    )
    storage_path = _resolve_historize_storage_path(context)

    # Set resolved table_format on the config so HistorizeSqlBuilder reads it
    historize_config.table_format = table_format

    # Build dest_config_dict with historize-resolved table_format (and storage_path
    # for BigQuery Iceberg).  Both override what from_context() would read from the
    # context/profile so the destination instance reflects the historize-layer settings.
    dest_config_dict: Dict[str, Any] = {
        **config_dict,
        "schema_name": schema_name,
        "table_format": table_format,
    }
    if table_format == "iceberg" and storage_path:
        dest_config_dict["storage_path"] = storage_path

    destination = DestinationFactory.create_from_context(
        destination_type, context, dest_config_dict
    )
    database = getattr(destination.config, "project_id", None) or getattr(
        destination.config, "catalog", "local"
    )

    # Resolve historize dataset and table using the configured placement strategy
    target_schema, target_table_name = resolve_historized_target(
        source_dataset=schema_name,
        source_table=pipeline_config.table_name,
        historize_config=historize_config,
    )

    # Pin resolved target_schema on historize_config so HistorizeRunner uses it
    # everywhere (state manager, partial refresh, etc.)
    if target_schema != schema_name and historize_config.output_dataset is None:
        historize_config.output_dataset = target_schema

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
    )
