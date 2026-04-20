"""Pipeline execution utilities.

This module provides functions for executing pipelines with comprehensive
timing, error handling, and result enrichment.
"""

import logging
import time
from datetime import datetime
from typing import List, Optional

from dlt_saga.pipeline_config import PipelineConfig
from dlt_saga.pipeline_config.base_config import ConfigSource
from dlt_saga.pipelines.registry import get_pipeline_class

logger = logging.getLogger(__name__)


def execute_pipeline(
    pipeline_config: PipelineConfig,
    log_prefix: Optional[str] = None,
) -> List[dict]:
    """Execute a pipeline with comprehensive error handling and timing.

    Args:
        pipeline_config: PipelineConfig object containing all configuration
        log_prefix: Optional prefix for log messages (e.g., "[1/2]")

    Returns:
        List of load info dictionaries with timing metadata

    Raises:
        Exception: If pipeline execution fails
    """
    pipeline_class = get_pipeline_class(
        pipeline_config.pipeline_group,
        config_path=pipeline_config.identifier,
        adapter=pipeline_config.adapter,
    )

    # Add log prefix manually since pipeline hasn't been initialized yet
    if log_prefix:
        logger.debug(
            f"{log_prefix} Initializing {pipeline_config.pipeline_group} pipeline"
        )

    # Start comprehensive timing
    pipeline_start_time = time.time()
    pipeline_start_datetime = datetime.now()

    # Prepare config dict with environment-aware fields
    config_dict = ConfigSource.prepare_for_execution(pipeline_config)

    # Initialize pipeline with optional log prefix
    pipeline = pipeline_class(config_dict, log_prefix=log_prefix)

    # Record initialization time
    init_time = time.time()
    init_duration = init_time - pipeline_start_time
    pipeline.logger.debug(f"Pipeline initialization completed in {init_duration:.1f}s")

    # Run the pipeline
    result = pipeline.run()

    # Record total execution time
    pipeline_end_time = time.time()
    total_duration = pipeline_end_time - pipeline_start_time

    pipeline.logger.debug("Pipeline executed successfully")

    # Add comprehensive timing to result
    for load_info in result:
        if isinstance(load_info, dict):
            load_info["pipeline_start_time"] = pipeline_start_datetime
            load_info["pipeline_end_time"] = datetime.now()
            load_info["total_pipeline_duration"] = total_duration
            load_info["initialization_duration"] = init_duration
            load_info["processing_duration"] = total_duration - init_duration

    return result
