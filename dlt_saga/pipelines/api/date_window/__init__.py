"""Reusable date-window incremental loading for REST API sources."""

from dlt_saga.pipelines.api.date_window.config import DateWindowApiConfig
from dlt_saga.pipelines.api.date_window.pipeline import DateWindowApiPipeline

__all__ = ["DateWindowApiConfig", "DateWindowApiPipeline"]
