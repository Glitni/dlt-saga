"""Pipeline configuration management.

Provides abstractions for discovering and managing pipeline configurations
from different sources (files, SharePoint, databases, etc.).

The ``default_generate_*`` functions implement the framework's default rules
for deriving schema names, table names, and external-table locations from a
pipeline config identifier. They live in
:mod:`dlt_saga.pipeline_config.naming` and are re-exported here so users
writing a custom ``naming_module`` (configured via ``naming_module:`` in
``saga_project.yml``) can import, copy, or wrap them as a starting point.
"""

from dlt_saga.pipeline_config.base_config import ConfigSource, PipelineConfig
from dlt_saga.pipeline_config.file_config import FilePipelineConfig
from dlt_saga.pipeline_config.naming import (
    default_generate_schema_name,
    default_generate_table_name,
    default_generate_target_location,
)

__all__ = [
    "ConfigSource",
    "PipelineConfig",
    "FilePipelineConfig",
    "default_generate_schema_name",
    "default_generate_table_name",
    "default_generate_target_location",
]
