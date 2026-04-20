"""Factory for building HistorizeRunner instances.

Centralizes the HistorizeRunner construction logic that was previously
duplicated between cli.py and session.py.
"""

from typing import Any, Dict, Optional

from dlt_saga.historize.config import HistorizeConfig
from dlt_saga.historize.runner import HistorizeRunner
from dlt_saga.pipeline_config import PipelineConfig
from dlt_saga.utility.naming import get_historized_table_name


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

    dest_config_dict: Dict[str, Any] = {**config_dict, "schema_name": schema_name}
    destination = DestinationFactory.create_from_context(
        destination_type, context, dest_config_dict
    )
    database = getattr(destination.config, "project_id", "local")

    source_table_name = pipeline_config.table_name
    base_historized = get_historized_table_name(
        pipeline_config.table_name, historize_config.output_table_suffix
    )
    target_table_name = historize_config.output_table or base_historized

    return HistorizeRunner(
        pipeline_name=pipeline_config.pipeline_name,
        historize_config=historize_config,
        destination=destination,
        database=database,
        schema=schema_name,
        source_table_name=source_table_name,
        target_table_name=target_table_name,
        config_dict=pipeline_config.config_dict,
        full_refresh=full_refresh,
        partial_refresh=partial_refresh,
        historize_from=historize_from,
    )
