import logging
import os
from datetime import datetime, timezone
from typing import Any, Dict, Iterator, Optional

import dlt
from dlt.sources.filesystem import FileItemDict, filesystem, readers

from dlt_saga.pipelines.filesystem.config import FilesystemConfig

logger = logging.getLogger(__name__)


class FilesystemClient:
    def __init__(self, config: FilesystemConfig):
        self.config = config
        self.fs_url = self._build_filesystem_url()
        self.column_hints: Optional[Dict[str, Any]] = None  # Set by pipeline if needed
        self._column_name_mapping_cache: Optional[Dict[str, str]] = (
            None  # Lazy-loaded cache
        )

        # For GCS without explicit credentials, route through ADC so that the
        # active profile (incl. `run_as` impersonation) controls identity rather
        # than a stray GOOGLE_APPLICATION_CREDENTIALS pointing at an unrelated
        # service-account key. Profile-driven auth must be deterministic.
        if config.filesystem_type == "gs" and not config.credentials:
            previous_gac = os.environ.pop("GOOGLE_APPLICATION_CREDENTIALS", None)
            if previous_gac:
                logger.debug(
                    "Cleared GOOGLE_APPLICATION_CREDENTIALS=%s for GCS read; "
                    "ADC (gcloud / metadata server / profile impersonation) "
                    "will be used. Pass `credentials:` in the pipeline config "
                    "to override.",
                    previous_gac,
                )

    def _build_filesystem_url(self) -> str:
        """Construct filesystem URL from configuration."""
        if self.config.filesystem_type == "gs":
            return f"gs://{self.config.bucket_name}"
        elif self.config.filesystem_type == "s3":
            return f"s3://{self.config.bucket_name}"
        elif self.config.filesystem_type == "azure":
            return f"azure://{self.config.bucket_name}"
        elif self.config.filesystem_type == "file":
            return self.config.bucket_name
        elif self.config.filesystem_type == "sftp":
            if not self.config.hostname:
                raise ValueError("hostname is required for SFTP filesystem")
            # Build SFTP URL: sftp://hostname/path
            return f"sftp://{self.config.hostname}/{self.config.bucket_name}"
        raise ValueError(f"Unsupported filesystem type: {self.config.filesystem_type}")

    def _get_sftp_credentials(self) -> Optional[Dict[str, Any]]:
        """Get SFTP credentials, resolving secrets if needed.

        Credentials can be:
        - Plain values: username: "myuser"
        - Secret URIs: username: "googlesecretmanager::project::secret-name"

        Returns:
            Dictionary of SFTP credentials for fsspec, or None if not SFTP
        """
        from dlt_saga.utility.secrets import resolve_secret

        if self.config.filesystem_type != "sftp":
            return None

        credentials = {}

        # Resolve username (supports secret URI or plain value)
        if self.config.username:
            credentials["sftp_username"] = resolve_secret(self.config.username)

        # Resolve password (supports secret URI or plain value)
        if self.config.password:
            credentials["sftp_password"] = resolve_secret(self.config.password)

        # Resolve SSH key (if secret URI, write to temp file)
        if self.config.key_filename:
            key_value = resolve_secret(self.config.key_filename)

            # If it looks like key content (multi-line), write to temp file
            if "\n" in key_value or key_value.startswith("-----BEGIN"):
                import tempfile

                temp_key_file = tempfile.NamedTemporaryFile(
                    mode="w", delete=False, suffix=".pem"
                )
                temp_key_file.write(key_value)
                temp_key_file.close()
                credentials["sftp_key_filename"] = temp_key_file.name
                logger.debug("Wrote SSH key to temporary file")
            else:
                # Assume it's a file path
                credentials["sftp_key_filename"] = key_value

        # Resolve key passphrase
        if self.config.key_passphrase:
            credentials["sftp_key_passphrase"] = resolve_secret(
                self.config.key_passphrase
            )

        # Resolve known_hosts (if secret URI, write to temp file)
        if self.config.known_hosts:
            known_hosts_value = resolve_secret(self.config.known_hosts)

            # If it looks like known_hosts content, write to temp file
            if "\n" in known_hosts_value or " ssh-" in known_hosts_value:
                import tempfile

                temp_known_hosts = tempfile.NamedTemporaryFile(
                    mode="w", delete=False, suffix="_known_hosts"
                )
                temp_known_hosts.write(known_hosts_value)
                temp_known_hosts.close()
                credentials["sftp_known_hosts"] = temp_known_hosts.name
                logger.debug("Wrote known_hosts to temporary file")
            else:
                # Assume it's a file path
                credentials["sftp_known_hosts"] = known_hosts_value

        # Add port if non-standard
        if self.config.port and self.config.port != 22:
            credentials["sftp_port"] = self.config.port

        return credentials if credentials else None

    def _read_csv_headers(self) -> Optional[list]:
        """Read CSV headers from the file without loading data.

        Returns:
            List of column names from CSV, or None if unable to read
        """
        try:
            import csv
            import io

            from dlt.sources.filesystem import filesystem

            fs_kwargs = self.get_filesystem_kwargs()
            fs = filesystem(**fs_kwargs)

            # Get first file from filesystem
            file_items = list(fs)
            if not file_items:
                logger.debug("No files found for header reading")
                return None

            # Read first row to get headers
            first_file = file_items[0]

            # dlt filesystem returns binary stream, need to wrap it for text mode
            with first_file.open() as binary_stream:
                # Wrap binary stream with TextIOWrapper for text mode
                text_stream = io.TextIOWrapper(
                    binary_stream,
                    encoding=self.config.encoding or "utf-8",
                    newline="",  # Let csv module handle newlines
                )
                reader = csv.reader(
                    text_stream, delimiter=self.config.csv_separator or ","
                )
                headers = next(reader)
                logger.debug(f"Read {len(headers)} CSV headers")
                return headers

        except Exception as e:
            logger.warning(f"Could not read CSV headers: {e}", exc_info=True)
            return None

    def _get_column_name_mapping(self) -> Optional[Dict[str, str]]:
        """Build a map of original CSV column names to normalized names.

        This is used by both pandas dtype hints and default value transformers
        to match original CSV column names to normalized config column names.

        Results are cached to avoid reading CSV headers multiple times.

        Returns:
            Dictionary mapping original names to normalized names, or None if unable to read
        """
        # Check cache first (None means not computed yet, use sentinel for "failed")
        if self._column_name_mapping_cache is not None:
            return self._column_name_mapping_cache

        # Read CSV headers
        csv_headers = self._read_csv_headers()
        if not csv_headers:
            self._column_name_mapping_cache = None
            return None

        # Import dlt's normalization function
        try:
            from dlt.common.normalizers.naming.snake_case import NamingConvention

            naming = NamingConvention()
        except ImportError:
            logger.warning("Could not import dlt normalization")
            self._column_name_mapping_cache = None
            return None

        # Build map: original CSV column name -> normalized name
        header_map = {}
        for original in csv_headers:
            normalized = naming.normalize_identifier(original)
            header_map[original] = normalized

        # Cache the result
        self._column_name_mapping_cache = header_map
        return header_map

    # Map dlt types to pandas dtypes (timestamp, date, time handled automatically)
    _DLT_TO_PANDAS_DTYPE = {
        "text": "str",
        "bigint": "Int64",
        "double": "float64",
        "bool": "boolean",
    }

    def _get_pandas_dtypes(self) -> Optional[Dict[str, str]]:
        """Build pandas dtype map by reading CSV headers and matching to config.

        Returns:
            Dictionary mapping original CSV column names to pandas dtype strings
        """
        columns = getattr(self, "column_hints", None) or getattr(
            self.config, "columns", None
        )
        if not columns:
            return None

        header_map = self._get_column_name_mapping()
        if not header_map:
            return None

        dtype_map = {}
        for original, normalized in header_map.items():
            # Priority: original name (standard), then normalized (backward compat)
            col_def = columns.get(original) or columns.get(normalized)
            if not col_def:
                continue

            dlt_type = col_def.get("data_type", "")
            if dlt_type in self._DLT_TO_PANDAS_DTYPE:
                dtype_map[original] = self._DLT_TO_PANDAS_DTYPE[dlt_type]

        if dtype_map:
            logger.debug(f"Built dtype map for {len(dtype_map)} columns")
        return dtype_map or None

    def get_filesystem_kwargs(self) -> Dict[str, Any]:
        """Generate ONLY the arguments that dlt's filesystem() accepts."""
        kwargs: Dict[str, Any] = {
            "bucket_url": self.fs_url,
        }

        # Add project ID if specified
        if self.config.project_id:
            kwargs["client_kwargs"] = {"project": self.config.project_id}

        # Add file_glob if provided
        if self.config.file_glob is not None:
            kwargs["file_glob"] = self.config.file_glob

        # Only add credentials if explicitly provided
        if self.config.credentials is not None:
            kwargs["credentials"] = self.config.credentials

        # Special handling for S3 endpoints
        if self.config.filesystem_type == "s3" and self.config.endpoint_url:
            if "client_kwargs" not in kwargs:
                kwargs["client_kwargs"] = {}
            kwargs["client_kwargs"]["endpoint_url"] = self.config.endpoint_url

        # SFTP credentials
        if self.config.filesystem_type == "sftp":
            sftp_creds = self._get_sftp_credentials()
            if sftp_creds:
                kwargs["credentials"] = sftp_creds
            else:
                logger.warning("SFTP filesystem_type but no credentials retrieved!")

        return kwargs

    def _parse_incremental_dates(
        self, initial_value: Optional[Any]
    ) -> tuple[Optional[Any], Optional[Any]]:
        """Parse initial_value and end_date into timezone-aware datetimes.

        Returns:
            Tuple of (initial_datetime, end_datetime)
        """
        initial_datetime = None
        end_datetime = None

        if initial_value:
            try:
                initial_date = datetime.strptime(initial_value, "%Y-%m-%d")
                initial_datetime = datetime.combine(
                    initial_date, datetime.min.time(), tzinfo=timezone.utc
                )
                logger.info(f"Initial value converted to datetime: {initial_datetime}")
            except ValueError:
                logger.warning(
                    f"Could not parse initial_value '{initial_value}' as date, using as-is"
                )
                initial_datetime = initial_value

        if self.config.end_date:
            try:
                end_date = datetime.strptime(self.config.end_date, "%Y-%m-%d")
                end_datetime = datetime.combine(
                    end_date, datetime.max.time(), tzinfo=timezone.utc
                )
                logger.info(f"Backfill end_date: {end_datetime}")
            except ValueError:
                logger.warning(
                    f"Could not parse end_date '{self.config.end_date}' as date, ignoring"
                )

        return initial_datetime, end_datetime

    def _apply_incremental_hints(
        self, fs, incremental_column: str, initial_datetime: Any, end_datetime: Any
    ):
        """Apply incremental hints to filesystem resource based on date range."""
        logger.info(f"Using filesystem-level incremental ({incremental_column})")

        if initial_datetime and end_datetime:
            logger.info(f"Starting backfill from {initial_datetime} to {end_datetime}")
            return fs.apply_hints(
                incremental=dlt.sources.incremental(
                    incremental_column,
                    initial_value=initial_datetime,
                    end_value=end_datetime,
                )
            )
        elif initial_datetime:
            logger.info(f"Starting incremental ingestion from: {initial_datetime}")
            return fs.apply_hints(
                incremental=dlt.sources.incremental(
                    incremental_column,
                    initial_value=initial_datetime,
                )
            )
        else:
            logger.info("Starting incremental ingestion without initial value")
            return fs.apply_hints(
                incremental=dlt.sources.incremental(incremental_column)
            )

    def _create_csv_reader(self, fs, fs_kwargs: dict, **kwargs):
        """Create CSV reader (DuckDB or pandas based on encoding).

        When include_file_metadata is enabled, uses a custom transformer that
        injects _dlt_source_file_name and _dlt_source_modification_date columns.
        """
        # Use metadata-injecting transformer if enabled
        if self.config.csv_include_file_metadata:
            logger.debug("Using CSV reader with file metadata injection")
            return fs | self._get_csv_with_metadata_transformer()

        csv_kwargs: Dict[str, Any] = {}
        if self.config.csv_separator:
            csv_kwargs["sep"] = self.config.csv_separator
        if self.config.encoding:
            csv_kwargs["encoding"] = self.config.encoding
        csv_kwargs.update(kwargs)

        encoding = self.config.encoding or "utf-8"
        if encoding.lower() in ("utf-8", "utf8"):
            return fs | readers(**fs_kwargs).read_csv_duckdb(
                **csv_kwargs, parallel=True
            )
        else:
            logger.debug(
                f"Using pandas reader for non-UTF-8 encoding: {encoding} "
                "(DuckDB only supports UTF-8)"
            )
            dtype_map = self._get_pandas_dtypes()
            if dtype_map:
                csv_kwargs["dtype"] = dtype_map
                logger.debug(
                    f"Using dtype hints for {len(dtype_map)} columns to skip type inference"
                )
            return fs | readers(**fs_kwargs).read_csv(**csv_kwargs)

    def _create_file_reader(self, fs, fs_kwargs: dict, **kwargs):
        """Create appropriate file reader based on file type."""
        file_type = self.config.file_type.lower() if self.config.file_type else "csv"

        if file_type == "csv":
            return self._create_csv_reader(fs, fs_kwargs, **kwargs)
        elif file_type == "parquet":
            return fs | readers(**fs_kwargs).read_parquet(**kwargs)
        elif file_type == "jsonl":
            return fs | readers(**fs_kwargs).read_jsonl(**kwargs)
        elif file_type == "json":
            return fs | self._get_json_array_transformer()
        else:
            raise ValueError(
                f"Unsupported file_type: {file_type}. "
                f"Supported types: csv, parquet, jsonl, json"
            )

    def read_incremental(
        self,
        incremental_column: str = "modification_date",
        initial_value: Optional[Any] = None,
        **kwargs,
    ):
        """Read files incrementally using DLT's native filesystem approach with simplified configuration."""
        # Use incremental_column from config, fallback to default
        actual_incremental_column = getattr(
            self.config, "incremental_column", incremental_column
        )
        logger.info(f"Using DLT-compatible glob pattern: {self.config.file_glob}")
        logger.info(f"Using incremental column: {actual_incremental_column}")

        # Parse dates and create filesystem resource
        initial_datetime, end_datetime = self._parse_incremental_dates(initial_value)

        fs_kwargs = self.get_filesystem_kwargs()
        fs_kwargs["file_glob"] = self.config.file_glob
        fs = filesystem(**fs_kwargs)

        # Apply incremental hints
        fs = self._apply_incremental_hints(
            fs, actual_incremental_column, initial_datetime, end_datetime
        )

        # Create and return appropriate reader
        return self._create_file_reader(fs, fs_kwargs, **kwargs)

    def read_csv(self, **kwargs) -> Any:
        """Read CSV files, optionally with file metadata injection."""
        # Use metadata-injecting transformer if enabled
        if self.config.csv_include_file_metadata:
            logger.debug("Using CSV reader with file metadata injection")
            fs = filesystem(**self.get_filesystem_kwargs())
            return fs | self._get_csv_with_metadata_transformer()

        csv_kwargs: Dict[str, Any] = {}
        if self.config.csv_separator:
            csv_kwargs["sep"] = self.config.csv_separator
        if self.config.encoding:
            csv_kwargs["encoding"] = self.config.encoding

        csv_kwargs.update(kwargs)

        # DuckDB only supports UTF-8 encoding
        # Fall back to pandas for non-UTF-8 files (without chunking to avoid encoding issues)
        encoding = self.config.encoding or "utf-8"
        if encoding.lower() in ("utf-8", "utf8"):
            # Use DuckDB reader for better performance with UTF-8 files
            # parallel=True enables parallel CSV parsing for faster reads
            return readers(**self.get_filesystem_kwargs()).read_csv_duckdb(
                **csv_kwargs, parallel=True
            )
        else:
            # Use pandas for non-UTF-8 encoded files (no chunking)
            logger.debug(
                f"Using pandas reader for non-UTF-8 encoding: {encoding} "
                "(DuckDB only supports UTF-8)"
            )

            # Add dtype hints from config to skip type inference
            dtype_map = self._get_pandas_dtypes()
            if dtype_map:
                csv_kwargs["dtype"] = dtype_map
                logger.debug(
                    f"Using dtype hints for {len(dtype_map)} columns to skip type inference"
                )

            return readers(**self.get_filesystem_kwargs()).read_csv(**csv_kwargs)

    def read_parquet(self, **kwargs) -> Any:
        return readers(**self.get_filesystem_kwargs()).read_parquet(**kwargs)

    def read_jsonl(self, **kwargs) -> Any:
        return readers(**self.get_filesystem_kwargs()).read_jsonl(**kwargs)

    def _get_json_array_transformer(self):
        """Get transformer for parsing JSON array files.

        Returns a dlt transformer that can be piped after a filesystem resource.
        Handles:
        - JSON arrays: yields each item individually
        - JSON key-value objects with json_key_column/json_value_column config:
          transforms {"key1": "val1", "key2": "val2"} into rows
          Complex values (lists/dicts) are serialized to JSON strings to prevent
          dlt from normalizing them into separate tables.
        - Other JSON objects: yields the object as-is
        """
        from typing import Iterator

        import dlt
        from dlt.common import json
        from dlt.common.storages.fsspec_filesystem import FileItemDict
        from dlt.common.typing import TDataItems

        # Capture config for use in transformer
        key_column = self.config.json_key_column
        value_column = self.config.json_value_column

        @dlt.transformer(name="read_json")
        def parse_json_array(items: Iterator[FileItemDict]) -> Iterator[TDataItems]:
            """Parse JSON files and yield data."""
            for file_obj in items:
                with file_obj.open() as f:
                    data = json.load(f)
                    # If data is a list, yield each item individually
                    if isinstance(data, list):
                        yield from data
                    # If data is a dict and key-value mapping is configured,
                    # transform each key-value pair into a row
                    elif isinstance(data, dict) and key_column and value_column:
                        for key, value in data.items():
                            # Serialize complex values (lists/dicts) to JSON strings
                            # to prevent dlt from normalizing them into separate tables
                            if isinstance(value, (list, dict)):
                                value = json.dumps(value)
                            yield {key_column: key, value_column: value}
                    # Otherwise yield the dict itself
                    else:
                        yield data

        return parse_json_array

    def read_json(self) -> Any:
        """Read JSON files (array format) using a custom transformer."""
        return (
            filesystem(**self.get_filesystem_kwargs())
            | self._get_json_array_transformer()
        )

    def _get_parquet_with_metadata_transformer(self):
        """Get transformer for reading Parquet files with file metadata injection."""
        from typing import Iterator

        import dlt
        from dlt.common.storages.fsspec_filesystem import FileItemDict
        from dlt.common.typing import TDataItems

        @dlt.transformer(name="read_parquet_with_metadata")
        def read_parquet_with_metadata(
            items: Iterator[FileItemDict],
        ) -> Iterator[TDataItems]:
            """Read Parquet files and inject file metadata into each row."""
            import pyarrow.parquet as pq

            for file_obj in items:
                file_name = file_obj.get("file_name", "")
                modification_date = file_obj.get("modification_date")
                logger.debug(f"Processing parquet file: {file_name}")

                with file_obj.open() as f:
                    table = pq.read_table(f)
                    for row in table.to_pylist():
                        row["_dlt_source_file_name"] = file_name
                        row["_dlt_source_modification_date"] = modification_date
                        yield row

        return read_parquet_with_metadata

    def _get_jsonl_with_metadata_transformer(self):
        """Get transformer for reading JSONL files with file metadata injection."""
        from typing import Iterator

        import dlt
        from dlt.common import json
        from dlt.common.storages.fsspec_filesystem import FileItemDict
        from dlt.common.typing import TDataItems

        @dlt.transformer(name="read_jsonl_with_metadata")
        def read_jsonl_with_metadata(
            items: Iterator[FileItemDict],
        ) -> Iterator[TDataItems]:
            """Read JSONL files and inject file metadata into each row."""
            for file_obj in items:
                file_name = file_obj.get("file_name", "")
                modification_date = file_obj.get("modification_date")
                logger.debug(f"Processing JSONL file: {file_name}")

                with file_obj.open() as f:
                    content = f.read()
                    if isinstance(content, bytes):
                        content = content.decode("utf-8")

                    for line in content.splitlines():
                        line = line.strip()
                        if line:
                            row = json.loads(line)
                            row["_dlt_source_file_name"] = file_name
                            row["_dlt_source_modification_date"] = modification_date
                            yield row

        return read_jsonl_with_metadata

    def _get_json_with_metadata_transformer(self):
        """Get transformer for reading JSON files with file metadata injection."""
        from typing import Iterator

        import dlt
        from dlt.common import json
        from dlt.common.storages.fsspec_filesystem import FileItemDict
        from dlt.common.typing import TDataItems

        key_column = self.config.json_key_column
        value_column = self.config.json_value_column

        @dlt.transformer(name="read_json_with_metadata")
        def read_json_with_metadata(
            items: Iterator[FileItemDict],
        ) -> Iterator[TDataItems]:
            """Read JSON files and inject file metadata into each row."""
            for file_obj in items:
                file_name = file_obj.get("file_name", "")
                modification_date = file_obj.get("modification_date")
                logger.debug(f"Processing JSON file: {file_name}")

                with file_obj.open() as f:
                    data = json.load(f)

                    def _inject(row):
                        row["_dlt_source_file_name"] = file_name
                        row["_dlt_source_modification_date"] = modification_date
                        return row

                    if isinstance(data, list):
                        for item in data:
                            yield _inject(item)
                    elif isinstance(data, dict) and key_column and value_column:
                        for key, val in data.items():
                            if isinstance(val, (list, dict)):
                                val = json.dumps(val)
                            yield _inject({key_column: key, value_column: val})
                    else:
                        yield _inject(data)

        return read_json_with_metadata

    def _create_file_reader_with_metadata(self, fs, fs_kwargs: dict, **kwargs):
        """Create a file reader that always injects file metadata columns.

        For append-mode pipelines, this ensures _dlt_source_file_name and
        _dlt_source_modification_date are available for _dlt_ingested_at resolution.
        """
        file_type = self.config.file_type.lower() if self.config.file_type else "csv"

        if file_type == "csv":
            return fs | self._get_csv_with_metadata_transformer()
        elif file_type == "parquet":
            return fs | self._get_parquet_with_metadata_transformer()
        elif file_type == "jsonl":
            return fs | self._get_jsonl_with_metadata_transformer()
        elif file_type == "json":
            return fs | self._get_json_with_metadata_transformer()
        elif file_type == "geojson":
            # GeoJSON uses the standard geojson transformer — metadata not supported
            logger.debug(
                "GeoJSON metadata injection not yet supported, using standard reader"
            )
            return fs | self._get_geojson_transformer()
        else:
            raise ValueError(
                f"Unsupported file_type: {file_type}. "
                f"Supported types: csv, parquet, json, jsonl"
            )

    def read_with_metadata(self, **kwargs) -> Any:
        """Read files with metadata injection for all file types.

        Always injects _dlt_source_file_name and _dlt_source_modification_date
        into each output row, regardless of file type.
        """
        fs = filesystem(**self.get_filesystem_kwargs())
        fs_kwargs = self.get_filesystem_kwargs()
        return self._create_file_reader_with_metadata(fs, fs_kwargs, **kwargs)

    def read_incremental_with_metadata(
        self,
        incremental_column: str = "modification_date",
        initial_value: Optional[Any] = None,
        **kwargs,
    ) -> Any:
        """Read files incrementally with metadata injection."""
        initial_datetime, end_datetime = self._parse_incremental_dates(initial_value)

        fs_kwargs = self.get_filesystem_kwargs()
        fs_kwargs["file_glob"] = self.config.file_glob
        fs = filesystem(**fs_kwargs)

        fs = self._apply_incremental_hints(
            fs, incremental_column, initial_datetime, end_datetime
        )

        return self._create_file_reader_with_metadata(fs, fs_kwargs, **kwargs)

    def _get_csv_with_metadata_transformer(self):
        """Get transformer for reading CSV files with file metadata injection.

        Returns a dlt transformer that reads CSV files and injects file metadata
        (_dlt_source_file_name, _dlt_source_modification_date) into each row.
        Uses pandas for encoding support.
        """
        import io
        from typing import Iterator

        import dlt
        import pandas as pd
        from dlt.common.storages.fsspec_filesystem import FileItemDict
        from dlt.common.typing import TDataItems

        # Capture config values for use in transformer
        csv_separator = self.config.csv_separator or ","
        encoding = self.config.encoding or "utf-8"

        @dlt.transformer(name="read_csv_with_metadata")
        def read_csv_with_metadata(
            items: Iterator[FileItemDict],
        ) -> Iterator[TDataItems]:
            """Read CSV files and inject file metadata into each row."""
            for file_obj in items:
                file_name = file_obj.get("file_name", "")
                modification_date = file_obj.get("modification_date")
                logger.debug(f"Processing file: {file_name}")

                with file_obj.open() as f:
                    # Read binary content and decode
                    content = f.read()
                    logger.debug(
                        f"Read {len(content)} bytes, type: {type(content).__name__}"
                    )
                    if isinstance(content, bytes):
                        content = content.decode(encoding)

                    # Skip empty files (just whitespace/newlines)
                    if not content.strip():
                        logger.debug(f"Skipping empty file: {file_name}")
                        continue

                    # Use StringIO for pandas to read from string
                    # Read all columns as strings to preserve leading zeros and special chars
                    # dlt will handle type conversion based on schema hints
                    df = pd.read_csv(io.StringIO(content), sep=csv_separator, dtype=str)
                    logger.debug(f"Parsed {len(df)} rows from {file_name}")

                    # Inject file metadata columns
                    df["_dlt_source_file_name"] = file_name
                    df["_dlt_source_modification_date"] = modification_date

                    yield from df.to_dict(orient="records")

        return read_csv_with_metadata

    def _get_geojson_transformer(self):
        """Get transformer for reading GeoJSON files as STRING type.

        Returns a dlt transformer that reads each GeoJSON file and yields
        a record with the filename and the file content as a string.
        Stored as STRING to allow direct use with ST_GEOGFROMGEOJSON().
        """
        from typing import Iterator

        import dlt
        from dlt.common.storages.fsspec_filesystem import FileItemDict
        from dlt.common.typing import TDataItems

        @dlt.transformer(name="read_geojson")
        def parse_geojson(items: Iterator[FileItemDict]) -> Iterator[TDataItems]:
            """Parse GeoJSON files and yield filename + JSON string content."""
            for file_obj in items:
                # Get filename without path and extension
                file_name = file_obj.get("file_name", "")
                base_name = (
                    file_name.split("/")[-1]
                    .replace(".geojson", "")
                    .replace(".json", "")
                )

                with file_obj.open() as f:
                    content = f.read()
                    if isinstance(content, bytes):
                        content = content.decode("utf-8")

                    yield {
                        "name": base_name,
                        "geojson": content,
                    }

        return parse_geojson

    def read_geojson(self) -> Any:
        """Read GeoJSON files, storing each file's content as a raw JSON string.

        Unlike read_json which expands the JSON structure, this method preserves
        the entire GeoJSON content as a string to avoid schema inference issues
        with complex geospatial data.

        Returns:
            dlt resource yielding records with 'name' and 'geojson' columns
        """
        return (
            filesystem(**self.get_filesystem_kwargs()) | self._get_geojson_transformer()
        )

    def get_files(self, limit: Optional[int] = None) -> Iterator[FileItemDict]:
        """Get file items from filesystem with optional limit."""
        fs = filesystem(**self.get_filesystem_kwargs())
        count = 0
        for file_item in fs:
            if limit and count >= limit:
                break
            yield file_item
            count += 1

    def has_files_modified_since(self, since: datetime) -> Optional[bool]:
        """Check if any files matching the glob have been modified since the given time.

        Efficiently checks files by stopping as soon as a modified file is found.
        Works with both single files and wildcard patterns.

        Args:
            since: Datetime threshold - check if any file is newer than this

        Returns:
            True if any file has been modified since the threshold
            False if no files have been modified
            None if modification check is not possible (e.g., no files, error)
        """
        try:
            fs_kwargs = self.get_filesystem_kwargs()
            fs = filesystem(**fs_kwargs)

            files_checked = 0
            for file_item in fs:
                files_checked += 1
                if "modification_date" in file_item:
                    mod_time = file_item["modification_date"]
                    if mod_time > since:
                        logger.debug(
                            f"Found modified file: {file_item.get('file_name', 'unknown')} "
                            f"(modified: {mod_time}, threshold: {since})"
                        )
                        return True
                else:
                    # If any file lacks modification_date, we can't reliably check
                    logger.debug("File modification_date not available, cannot check")
                    return None

            if files_checked == 0:
                logger.debug("No files found matching glob pattern")
                return None

            logger.debug(
                f"Checked {files_checked} file(s), none modified since {since}"
            )
            return False

        except Exception as e:
            logger.debug(f"Could not check file modifications: {e}")
            return None

    def test_connection(self) -> bool:
        """Test connection to the filesystem."""
        try:
            list(self.get_files(limit=1))
            logger.info(
                f"Connection successful: {self.config.filesystem_type} {self.fs_url}"
            )
            return True
        except Exception as e:
            logger.error(f"Connection failed: {e}")
            return False
