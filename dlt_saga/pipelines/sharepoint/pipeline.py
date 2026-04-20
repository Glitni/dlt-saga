from dataclasses import fields
from typing import Any, Dict, List, Tuple

import dlt

from dlt_saga.pipelines.base_pipeline import BasePipeline
from dlt_saga.utility.cli.context import get_execution_context
from dlt_saga.utility.cli.logging import YELLOW, colorize

from .client import SharePointClient
from .config import SharePointConfig


class SharePointPipeline(BasePipeline):
    """Downloads a file from SharePoint and loads it as a dlt resource.

    Supported file types: xlsx, csv, json, jsonl.

    Authentication uses the SharePoint app-only OAuth 2.0 flow: the OAuth2
    form body is stored in a secrets provider (e.g. Azure Key Vault) and
    fetched at runtime via the ``auth_secret`` URI.
    """

    def __init__(self, config: Dict[str, Any], log_prefix: str = None):
        config_field_names = {f.name for f in fields(SharePointConfig)}
        self.source_config = SharePointConfig(
            **{k: v for k, v in config.items() if k in config_field_names}
        )
        self.client = SharePointClient(self.source_config)
        super().__init__(config, log_prefix)

    def _should_skip_extraction(self) -> bool:
        """Check if extraction should be skipped based on file modification time.

        Compares the SharePoint file's last-modified time with the last time we
        successfully loaded data. Skips if unchanged unless --force or --full-refresh
        is set.

        Returns:
            True if extraction should be skipped, False otherwise.
        """
        context = get_execution_context()
        if context.force or context.full_refresh:
            self.logger.debug(
                "%s mode - skipping modification check for %s",
                "Full refresh" if context.full_refresh else "Force",
                colorize(self.base_table_name, YELLOW),
            )
            return False

        try:
            file_modified_time = self.client.get_last_modified_time()
            last_load_time = self._get_last_load_with_data(self.table_name)

            if last_load_time and file_modified_time <= last_load_time:
                self.logger.info(
                    "Skipping extraction for %s "
                    "- file not modified since last load "
                    "(file: %s, last load: %s)",
                    colorize(self.base_table_name, YELLOW),
                    file_modified_time.isoformat(),
                    last_load_time.isoformat(),
                )
                return True

            self.logger.info(
                "Starting extraction for %s",
                colorize(self.base_table_name, YELLOW),
            )
            return False

        except Exception as e:
            self.logger.warning(
                "Change detection failed: %s, proceeding with extraction anyway",
                e,
                exc_info=True,
            )
            return False

    def extract_data(self) -> List[Tuple[Any, str]]:
        if self._should_skip_extraction():
            return []

        self.logger.info(
            "Downloading from SharePoint: %s", self.source_config.file_path
        )
        file_bytes = self.client.download_file()

        records = self.client.read_file(file_bytes)
        self.logger.info(
            "Parsed %s rows from %s",
            len(records),
            self.source_config.file_path,
        )

        resource = dlt.resource(records, name=self.table_name)
        description = (
            f"SharePoint file: {self.source_config.file_path}"
            f" ({self.source_config.file_type})"
        )
        if (
            self.source_config.file_type.lower() == "xlsx"
            and self.source_config.sheet_name
        ):
            description += f" | Sheet: {self.source_config.sheet_name}"
        return [(resource, description)]
