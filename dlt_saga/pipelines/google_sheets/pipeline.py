import logging
from dataclasses import fields
from typing import Any, Dict, List, Tuple

import dlt

from dlt_saga.pipelines.base_pipeline import BasePipeline
from dlt_saga.pipelines.google_sheets.client import GSheetsClient
from dlt_saga.pipelines.google_sheets.config import GSheetsConfig
from dlt_saga.utility.cli.context import get_execution_context
from dlt_saga.utility.cli.logging import YELLOW, colorize

logger = logging.getLogger(__name__)


class GoogleSheetsPipeline(BasePipeline):
    def __init__(self, config: Dict[str, Any], log_prefix: str = None):
        # Process source config
        # Use fields() to get all fields including inherited ones from BaseConfig
        config_field_names = {f.name for f in fields(GSheetsConfig)}
        self.source_config = GSheetsConfig(
            **{k: v for k, v in config.items() if k in config_field_names}
        )

        # Initialize client
        self.client = GSheetsClient(self.source_config)

        # Initialize pipeline with optional log prefix
        super().__init__(config, log_prefix)

    def _table_description(self, spreadsheet_title: str, sheet_name: str) -> str:
        """Table description with optional sheet name parameter."""
        description_parts = [
            f"Data extracted from Google Sheet: {spreadsheet_title}",
            f"Sheet: {sheet_name}",
            f"Range: {self.source_config.range or 'full sheet'}",
        ]

        return " | ".join(description_parts)

    def _should_skip_extraction(self) -> bool:
        """Check if extraction should be skipped based on modification time.

        Compares the spreadsheet's last modified time with the last time we
        successfully loaded data. Skips if unchanged unless --force flag is set.

        Returns:
            True if extraction should be skipped, False otherwise
        """
        # Check if force or full_refresh flag is set
        context = get_execution_context()
        if context.force or context.full_refresh:
            self.logger.debug(
                f"{'Full refresh' if context.full_refresh else 'Force'} mode "
                "- skipping modification check for "
                f"{colorize(self.base_table_name, YELLOW)}"
            )
            return False

        try:
            # Get the last modified time from Google Drive
            sheet_modified_time = self.client.get_last_modified_time(
                self.source_config.spreadsheet_id
            )

            # Get the last time we loaded data from _saga_load_info table
            # Use table_name (environment-aware) to match what's stored in _saga_load_info
            last_load_time = self._get_last_load_with_data(self.table_name)

            if last_load_time:
                # Compare timestamps
                if sheet_modified_time <= last_load_time:
                    self.logger.info(
                        f"Skipping extraction for "
                        f"{colorize(self.base_table_name, YELLOW)} "
                        f"Sheet not modified since last load "
                        f"(sheet: {sheet_modified_time.isoformat()}, "
                        f"last load: {last_load_time.isoformat()})"
                    )
                    return True

            self.logger.info(
                f"Starting extraction for {colorize(self.base_table_name, YELLOW)}"
            )

            return False

        except Exception as e:
            # If anything goes wrong with change detection, log and proceed with extraction
            self.logger.warning(
                f"Change detection failed: {str(e)}, proceeding with extraction anyway",
                exc_info=True,
            )
            return False

    def extract_data(self) -> List[Tuple[Any, str]]:
        # Check if we should skip extraction
        if self._should_skip_extraction():
            return []  # Return empty list to skip processing

        resources = []
        spreadsheet_title = self.client.get_spreadsheet_title(
            self.source_config.spreadsheet_id
        )

        sheet_names = (
            [self.source_config.sheet_name]
            if self.source_config.sheet_name
            else self.client.get_sheet_names(self.source_config.spreadsheet_id)
        )

        for sheet_name in sheet_names:
            table_name = self.table_name + (
                ""
                if self.source_config.sheet_name
                else f"_{sheet_name.lower().replace(' ', '_')}"
            )
            data = self.client.get_sheet_data(
                self.source_config.spreadsheet_id,
                sheet_name,
                self.source_config.range,
            )
            resource = dlt.resource(data, name=table_name)
            description = self._table_description(
                sheet_name=sheet_name, spreadsheet_title=spreadsheet_title
            )
            resources.append((resource, description))

        return resources
