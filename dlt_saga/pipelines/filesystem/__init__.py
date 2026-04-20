"""Filesystem pipeline for extracting files from GCS, local storage, or SFTP.

This pipeline supports incremental loading of files based on modification time,
with support for various file formats (CSV, Parquet, JSON, etc.) through dlt's
filesystem source.
"""

from dlt_saga.pipelines.filesystem.client import FilesystemClient
from dlt_saga.pipelines.filesystem.config import FilesystemConfig
from dlt_saga.pipelines.filesystem.pipeline import FilesystemPipeline

__all__ = ["FilesystemClient", "FilesystemConfig", "FilesystemPipeline"]
