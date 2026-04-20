"""Google Sheets pipeline for extracting data from Google Spreadsheets.

This pipeline uses the Google Sheets API to extract data with change detection
based on last modified timestamps. Supports incremental loading to avoid
re-processing unchanged sheets.
"""

from dlt_saga.pipelines.google_sheets.client import GSheetsClient
from dlt_saga.pipelines.google_sheets.config import GSheetsConfig
from dlt_saga.pipelines.google_sheets.pipeline import GoogleSheetsPipeline

__all__ = ["GSheetsClient", "GSheetsConfig", "GoogleSheetsPipeline"]
