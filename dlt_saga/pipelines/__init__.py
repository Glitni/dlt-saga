"""Pipeline source implementations for data ingestion.

This package contains source-specific pipeline implementations that extract data
from various sources (Google Sheets, filesystems, databases, REST APIs, etc.) and
load them into data warehouses using the dlt framework.

Each source type follows a consistent pattern:
- config.py: Source-specific configuration (inherits from BaseConfig)
- client.py: API/data source interaction logic (inherits from BaseClient)
- pipeline.py: Pipeline orchestration (inherits from BasePipeline)

All pipeline implementations are registered in the registry for automatic discovery.
"""
