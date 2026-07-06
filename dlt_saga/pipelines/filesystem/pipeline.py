import logging
from dataclasses import fields
from typing import Any, Dict, Iterator, List, Optional, Tuple

import dlt

from dlt_saga.pipelines.base_pipeline import BasePipeline
from dlt_saga.pipelines.filesystem.client import FilesystemClient
from dlt_saga.pipelines.filesystem.config import FilesystemConfig
from dlt_saga.utility.cli.context import get_execution_context
from dlt_saga.utility.cli.logging import YELLOW, colorize

logger = logging.getLogger(__name__)


class FilesystemPipeline(BasePipeline):
    def __init__(self, config: Dict[str, Any], log_prefix: str = None):
        # Process source config
        # Use fields() to get all fields including inherited ones from BaseConfig
        config_field_names = {f.name for f in fields(FilesystemConfig)}
        self.source_config = FilesystemConfig(
            **{k: v for k, v in config.items() if k in config_field_names}
        )

        # Initialize client
        self.client = FilesystemClient(self.source_config)

        # Initialize pipeline with optional log prefix
        super().__init__(config, log_prefix)

    def _table_description(self) -> str:
        """Table description"""
        description_parts = [
            f"Data extracted from: {self.source_config.filesystem_type}",
            f"File type: {self.source_config.file_type}",
        ]

        # Add file glob information
        if self.source_config.file_glob:
            description_parts.append(f"File glob: {self.source_config.file_glob}")

        # Add incremental information if enabled
        incremental = self.config_dict.get("incremental")
        if incremental:
            description_parts.append("Incremental mode: enabled")
            incremental_column = self.config_dict.get("incremental_column")
            if incremental_column:
                description_parts.append(f"Incremental column: {incremental_column}")

        return " | ".join(description_parts)

    def _extract_config_defaults(self) -> Dict[str, Any]:
        """Extract columns with default values from config.

        Returns:
            Dict mapping column names (original or normalized) to their default values
        """
        config_defaults = {}
        if self.target_writer.config.columns:
            for col_name, col_config in self.target_writer.config.columns.items():
                if "default" in col_config:
                    config_defaults[col_name] = col_config["default"]
        return config_defaults

    def _build_original_defaults_map(
        self, config_defaults: Dict[str, Any]
    ) -> Optional[Dict[str, Any]]:
        """Build map from original column names to default values.

        Args:
            config_defaults: Dict of column names (original or normalized) to defaults

        Returns:
            Dict of original column names to defaults, or None if mapping fails
        """
        header_map = self.client._get_column_name_mapping()
        if not header_map:
            logger.warning(
                "Could not read CSV headers or build column mapping, "
                "default values will not be applied"
            )
            return None

        original_defaults = {}
        for original, normalized in header_map.items():
            # Check original name first (standard), then normalized (backward compat)
            if original in config_defaults:
                original_defaults[original] = config_defaults[original]
            elif normalized in config_defaults:
                original_defaults[original] = config_defaults[normalized]

        if not original_defaults:
            logger.warning("No matching columns found for default values")
            return None

        return original_defaults

    @staticmethod
    def _is_null_or_empty(value: Any) -> bool:
        """Check if a value is null, empty, or NaN.

        Handles all missing-value representations from pandas CSV readers:
        - None (Python null)
        - "" and " " (empty/blank strings)
        - float('nan') (pandas default for missing values with dtype=object/str)
        - pd.NA (pandas nullable dtypes, raises TypeError on bool())
        """
        if value is None or value in ("", " "):
            return True
        if isinstance(value, float):
            import math

            return math.isnan(value)
        # pd.NA raises TypeError when bool() is called on it
        try:
            bool(value)
            return False
        except (TypeError, ValueError):
            return True

    def _create_defaults_transformer(self, original_defaults: Dict[str, Any]) -> Any:
        """Create the dlt transformer that applies default values.

        Args:
            original_defaults: Dict of original column names to default values

        Returns:
            dlt transformer function
        """
        is_null_or_empty = self._is_null_or_empty

        @dlt.transformer
        def apply_defaults(items: Iterator[Dict[str, Any]]) -> Iterator[Dict[str, Any]]:
            """Apply default values to NULL/empty columns based on config."""
            replaced_count = {col: 0 for col in original_defaults}

            for item in items:
                for col_name, default_value in original_defaults.items():
                    if col_name not in item or is_null_or_empty(item[col_name]):
                        item[col_name] = default_value
                        replaced_count[col_name] += 1
                yield item

        return apply_defaults

    def _create_apply_defaults_transformer(self) -> Any:
        """Create a dlt transformer that applies default values from column config.

        Returns a transformer that replaces NULL/empty values with configured defaults.
        This is useful for SCD2 merge keys that cannot be NULL.

        The transformer runs in the extract phase where data still has original CSV
        column names, so we need to map normalized config names back to original names.

        Returns:
            dlt transformer function that applies defaults, or None if no defaults configured
        """
        config_defaults = self._extract_config_defaults()
        if not config_defaults:
            return None

        original_defaults = self._build_original_defaults_map(config_defaults)
        if not original_defaults:
            return None

        return self._create_defaults_transformer(original_defaults)

    def _should_skip_extraction(self) -> bool:
        """Check if extraction should be skipped based on file modification time.

        Applies to non-incremental loads. For single-file globs, compares the file's
        modification time against the last load time. For wildcard globs, checks if
        any file has been modified since the last load (stops checking as soon as
        a modified file is found).

        Skips extraction if no files have been modified since the last successful load,
        unless --force flag is set.

        Returns:
            True if extraction should be skipped, False otherwise
        """
        # Only check for non-incremental loads
        if self.config_dict.get("incremental"):
            return False

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
            # Get the last time we loaded data
            # Use table_name (environment-aware) to match what's stored in _saga_load_info
            last_load_time = self._get_last_load_with_data(self.table_name)

            if not last_load_time:
                # No previous load, proceed with extraction
                self.logger.info(
                    f"Starting extraction for "
                    f"{colorize(self.base_table_name, YELLOW)} "
                    f"(no previous load found)"
                )
                return False

            # Check if any files have been modified since the last load
            has_modified = self.client.has_files_modified_since(last_load_time)

            if has_modified is None:
                # Couldn't determine modification status, proceed with extraction
                self.logger.debug("Could not check file modifications, proceeding")
                return False

            if has_modified:
                # At least one file has been modified
                self.logger.info(
                    f"Starting extraction for "
                    f"{colorize(self.base_table_name, YELLOW)} "
                    f"(files modified since last load: {last_load_time.isoformat()})"
                )
                return False

            # No files modified since last load - skip extraction
            self.logger.info(
                f"Skipping extraction for "
                f"{colorize(self.base_table_name, YELLOW)} "
                f"- No files modified since last load ({last_load_time.isoformat()})"
            )
            return True

        except Exception as e:
            # If anything goes wrong with change detection, log and proceed with extraction
            self.logger.warning(
                f"Change detection failed: {str(e)}, proceeding with extraction anyway",
                exc_info=True,
            )
            return False

    def _needs_file_metadata(self) -> bool:
        """Check if file metadata injection is needed for this pipeline.

        Returns True for append-mode pipelines where _dlt_source_file_name and
        _dlt_source_modification_date are needed for _dlt_ingested_at resolution.
        """
        wd = self.config_dict.get("write_disposition", "append")
        # Strip +historize suffix to get base disposition
        base_wd = wd.split("+")[0] if "+" in wd else wd
        return base_wd == "append"

    def _read_incremental(self, use_metadata: bool) -> Any:
        """Read files incrementally, optionally with metadata injection."""
        end_date = self.source_config.end_date
        initial_value = self.config_dict.get("initial_value")
        if end_date:
            logger.info(f"Backfill run detected: {initial_value} to {end_date}")

        incremental_column = self.config_dict.get(
            "incremental_column", "modification_date"
        )

        if use_metadata:
            return self.client.read_incremental_with_metadata(
                incremental_column=incremental_column,
                initial_value=initial_value,
            )
        return self.client.read_incremental(
            incremental_column=incremental_column,
            initial_value=initial_value,
        )

    def _read_standard(self, use_metadata: bool) -> Any:
        """Read files without incremental logic, optionally with metadata."""
        if use_metadata:
            return self.client.read_with_metadata()

        reader_method = getattr(
            self.client, f"read_{self.source_config.file_type}", None
        )
        if reader_method:
            return reader_method()
        raise ValueError(f"Unsupported file type: {self.source_config.file_type}")

    def extract_data(self) -> List[Tuple[Any, str]]:
        # Check if we should skip extraction (for non-incremental loads with unchanged files)
        if self._should_skip_extraction():
            return []  # Return empty list to skip processing

        # Pass column hints to the client if available
        if self.target_writer.config.columns:
            self.client.column_hints = self.target_writer.config.columns

        # Determine if we need file metadata for append-mode pipelines
        use_metadata = self._needs_file_metadata()
        if use_metadata:
            logger.debug("Append mode: using metadata-injecting readers")

        # Read files
        incremental = self.config_dict.get("incremental", False)
        if incremental:
            logger.debug("Using incremental ingestion mode")
            resource = self._read_incremental(use_metadata)
        else:
            logger.debug("Using standard filesystem ingestion mode")
            resource = self._read_standard(use_metadata)

        # Apply default values transformer if configured (before naming)
        apply_defaults_transformer = self._create_apply_defaults_transformer()
        if apply_defaults_transformer:
            resource = resource | apply_defaults_transformer

        # Apply table name AFTER transformer to ensure it's not overwritten
        if self.table_name:
            resource = resource.with_name(self.table_name)

        description = self._table_description()
        return [(resource, description)]
