"""Pipeline configuration management.

This module provides abstractions for discovering and managing pipeline
configurations from different sources (files, SharePoint, databases, etc.).
"""

from dlt_saga.pipeline_config.base_config import ConfigSource, PipelineConfig
from dlt_saga.pipeline_config.file_config import FilePipelineConfig

__all__ = ["ConfigSource", "PipelineConfig", "FilePipelineConfig"]
