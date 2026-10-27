import logging
import os
from datetime import datetime, timezone
from typing import Any, Dict, Iterator, List, Optional

import dlt
from dlt.common.storages.fsspec_filesystem import FileItem
from dlt.sources.filesystem import (
    FileItemDict,
    filesystem,
    read_csv,
    read_csv_duckdb,
    read_jsonl,
    read_parquet,
)

from dlt_saga.pipelines.filesystem.config import FilesystemConfig
from dlt_saga.utility.naming import normalize_identifier

logger = logging.getLogger(__name__)

# Rows per pandas chunk when streaming CSV files with metadata injection. Bounds
# peak memory to one chunk instead of materializing the whole file (+ a dict copy)
# regardless of file size.
_CSV_METADATA_CHUNK_SIZE = 50_000

# Read-only Drive scope for gdrive sources. The service account must also be
# shared into the source folder (or file, with file_id): the scope alone grants
# no access to files.
_GDRIVE_SCOPES = ("https://www.googleapis.com/auth/drive.readonly",)
_GDRIVE_FILE_FIELDS = "id, name, mimeType, modifiedTime, size"
# Native Google files (Sheets, Docs, ...) have no binary content to download
_GDRIVE_NATIVE_MIME_PREFIX = "application/vnd.google-apps."

# gcsfs sends every request with no timeout unless given one, so a connection
# that stops delivering bytes hangs the pipeline indefinitely. Bounding the
# connect and the gap between reads (not the total) turns a stall into an error
# gcsfs retries, without capping how long a large file may take to download.
_GCS_CONNECT_TIMEOUT_SECONDS = 30
_GCS_READ_STALL_TIMEOUT_SECONDS = 60

# paramiko bounds only the SSH handshake by default: reads on an open SFTP
# channel wait forever for a server that stops sending. The read timeout is set
# on the channel of a filesystem this client opens for itself (see
# _get_sftp_filesystem), so it reaches no other fsspec user in the process.
_SFTP_CONNECT_TIMEOUT_SECONDS = 30
_SFTP_READ_STALL_TIMEOUT_SECONDS = 60


class _GdriveFileItem(FileItemDict):
    """A Drive file addressed by ID, downloaded on first open().

    dlt's ``FileItemDict.open()`` serves ``file_content`` from memory when
    present, so every reader and transformer works unchanged; listing-only
    callers (change detection, test_connection) never download the file.
    """

    def __init__(self, item: FileItem, file_id: str, download: Any):
        super().__init__(item)
        self._file_id = file_id
        self._download = download

    def open(self, *args: Any, **kwargs: Any) -> Any:  # noqa: A003
        if "file_content" not in self:
            self["file_content"] = self._download(self._file_id)
        return super().open(*args, **kwargs)


def _gcs_requests_timeout() -> Any:
    """The per-request timeout handed to gcsfs (an ``aiohttp.ClientTimeout``)."""
    import aiohttp

    return aiohttp.ClientTimeout(
        total=None,
        sock_connect=_GCS_CONNECT_TIMEOUT_SECONDS,
        sock_read=_GCS_READ_STALL_TIMEOUT_SECONDS,
    )


def _resolve_secret_uris(credentials: Any) -> Any:
    """Resolve secret URIs in a ``credentials`` value (string or object)."""
    from dlt_saga.utility.secrets.resolver import SecretResolver

    if isinstance(credentials, dict):
        return SecretResolver.resolve_dict(credentials)
    if isinstance(credentials, str):
        return SecretResolver.resolve(credentials)
    return credentials


def _iter_csv_records_with_metadata(
    file_obj: FileItemDict,
    separator: str,
    encoding: str,
    chunk_size: int = _CSV_METADATA_CHUNK_SIZE,
) -> Iterator[dict]:
    """Stream a CSV file as dict rows, injecting file-metadata columns.

    Reads the file object directly in chunks (``pd.read_csv(chunksize=...)``)
    rather than loading the whole file into memory and re-parsing from a string.
    Every column is read as ``str`` (``dtype=str``) to preserve leading zeros and
    let dlt apply schema hints downstream — unchanged from the previous behaviour.
    An empty/whitespace-only file yields no rows (pandas raises
    ``EmptyDataError``), matching the prior explicit skip.
    """
    import pandas as pd

    file_name = file_obj.get("file_name", "")
    modification_date = file_obj.get("modification_date")
    logger.debug(f"Processing file: {file_name}")

    with file_obj.open() as f:
        try:
            reader = pd.read_csv(
                f,
                sep=separator,
                dtype=str,
                encoding=encoding,
                chunksize=chunk_size,
            )
            for chunk in reader:
                chunk["_dlt_source_file_name"] = file_name
                chunk["_dlt_source_modification_date"] = modification_date
                yield from chunk.to_dict(orient="records")
        except pd.errors.EmptyDataError:
            logger.debug(f"Skipping empty file: {file_name}")
            return


class FilesystemClient:
    def __init__(self, config: FilesystemConfig):
        self.config = config
        self.fs_url = self._build_filesystem_url()
        self.column_hints: Optional[Dict[str, Any]] = None  # Set by pipeline if needed
        self._column_name_mapping_cache: Optional[Dict[str, str]] = (
            None  # Lazy-loaded cache
        )
        self._gdrive_credentials: Any = None  # Lazy-loaded, see _get_gdrive_credentials
        self._gdrive_service: Any = None  # Lazy-loaded, see _get_gdrive_service
        # file_id content by ID, so the header read and the data read share one
        # download (each _file_source() call builds a fresh, empty file item)
        self._gdrive_content: Dict[str, bytes] = {}
        self._sftp_filesystem: Any = None  # Lazy-loaded, see _get_sftp_filesystem
        # GCS without explicit credentials uses Application Default Credentials
        # (gcloud / metadata server / profile `run_as` impersonation) — the same
        # resolution as the BigQuery destination. Credential selection is left to
        # google.auth (which the auth provider patches for impersonation); this
        # client never mutates process-global GOOGLE_APPLICATION_CREDENTIALS,
        # which under parallel execution would strip it for other concurrently
        # running pipelines.

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
        elif self.config.filesystem_type == "gdrive":
            # gdrive://<folder_id>; file_glob is matched against names below it.
            # With file_id the file is fetched by ID (see _gdrive_file_resource)
            return f"gdrive://{self.config.file_id or self.config.bucket_name}"
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

        # Add port if non-standard
        if self.config.port and self.config.port != 22:
            credentials["sftp_port"] = self.config.port

        return credentials if credentials else None

    def _get_sftp_known_hosts_file(self) -> Optional[str]:
        """Path to the configured ``known_hosts``, writing content to a temp file.

        The value (plain or a secret URI) is either a path to an existing file
        or the entries themselves.
        """
        from dlt_saga.utility.secrets import resolve_secret

        if not self.config.known_hosts:
            return None
        value = resolve_secret(self.config.known_hosts)
        if os.path.isfile(value):
            return value
        # An entry is at least "<host> <key type> <key>"
        if len(value.split()) < 3:
            raise ValueError(
                f"known_hosts is neither an existing file nor known_hosts entries: "
                f"{value!r}. Give a path to a known_hosts file, or its content "
                f"(e.g. the output of `ssh-keyscan <hostname>`)."
            )
        import tempfile

        with tempfile.NamedTemporaryFile(
            mode="w", delete=False, suffix="_known_hosts"
        ) as temp_known_hosts:
            temp_known_hosts.write(value)
        logger.debug("Wrote known_hosts to temporary file")
        return temp_known_hosts.name

    def _get_sftp_filesystem(self) -> Any:
        """Open the SFTP filesystem for this source, cached per client.

        Opened outside fsspec's instance cache, so the channel timeout set here
        stays on this client's connection, and a connection a stall has broken
        is never handed to a later run in the same process. With
        ``known_hosts`` set, a server whose host key it doesn't list is refused
        before any credential is sent.
        """
        if self._sftp_filesystem is None:
            from dlt.common.configuration.specs import SFTPCredentials
            from dlt.common.utils import without_none

            from dlt_saga.pipelines.filesystem.sftp import KnownHostsSFTPFileSystem

            sftp_creds = self._get_sftp_credentials()
            if not sftp_creds:
                logger.warning("SFTP filesystem_type but no credentials retrieved!")
            credentials = SFTPCredentials()
            credentials.update(
                {"sftp_timeout": _SFTP_CONNECT_TIMEOUT_SECONDS, **(sftp_creds or {})}
            )
            fs = KnownHostsSFTPFileSystem(
                self.config.hostname,
                known_hosts=self._get_sftp_known_hosts_file(),
                skip_instance_cache=True,
                **without_none(credentials.to_fsspec_credentials()),
            )
            fs.ftp.get_channel().settimeout(_SFTP_READ_STALL_TIMEOUT_SECONDS)
            self._sftp_filesystem = fs
        return self._sftp_filesystem

    def _get_gdrive_credentials(self) -> Any:
        """Build Drive credentials for dlt's gdrive filesystem, cached per client.

        ``credentials`` when set (see ``_gdrive_credential_args``), otherwise
        Application Default Credentials — always scoped to ``drive.readonly``.
        dlt's ``GcpServiceAccountCredentials`` accepts the resolved native
        credentials as-is, so the scope survives into the Drive client.
        """
        if self._gdrive_credentials is None:
            from dlt.common.configuration.specs import GcpServiceAccountCredentials
            from dlt.common.configuration.specs.exceptions import (
                InvalidGoogleNativeCredentialsType,
            )

            from dlt_saga.utility.gcp.client_pool import google_api_pool
            from dlt_saga.utility.optional_deps import require_optional

            require_optional("googleapiclient", "Google Drive filesystem pipelines")

            native = google_api_pool.get_credentials(**self._gdrive_credential_args())
            dlt_creds = GcpServiceAccountCredentials()
            try:
                dlt_creds.parse_native_representation(native)
            except InvalidGoogleNativeCredentialsType:
                # Non service-account credentials (ADC user credentials,
                # metadata server, impersonation) are taken as default
                # credentials before the service-account parse rejects them
                if not dlt_creds.has_default_credentials():
                    raise
            self._gdrive_credentials = dlt_creds
        return self._gdrive_credentials

    def _gdrive_credential_args(self) -> Dict[str, Any]:
        """google_api_pool arguments for the configured ``credentials``.

        No credentials falls through to Application Default Credentials.
        """
        from dlt_saga.utility.gcp.credentials import service_account_credential_args

        return {
            **service_account_credential_args(self.config.credentials),
            "scopes": _GDRIVE_SCOPES,
        }

    def _get_gdrive_service(self) -> Any:
        """Drive v3 ``files()`` service for file_id reads, reused per client."""
        if self._gdrive_service is None:
            from dlt_saga.utility.gcp.client_pool import google_api_pool
            from dlt_saga.utility.optional_deps import require_optional

            require_optional("googleapiclient", "Google Drive filesystem pipelines")
            self._gdrive_service = google_api_pool.get_client(
                "drive", "v3", cache_client=False, **self._gdrive_credential_args()
            ).files()
        return self._gdrive_service

    def _download_gdrive_file(self, file_id: str) -> bytes:
        """Download a Drive file's binary content, once per client."""
        if file_id not in self._gdrive_content:
            self._gdrive_content[file_id] = self._fetch_gdrive_file(file_id)
        return self._gdrive_content[file_id]

    def _fetch_gdrive_file(self, file_id: str) -> bytes:
        """Fetch a Drive file's binary content from the Drive API."""
        import io

        from googleapiclient.http import MediaIoBaseDownload

        buffer = io.BytesIO()
        downloader = MediaIoBaseDownload(
            buffer, self._get_gdrive_service().get_media(fileId=file_id)
        )
        done = False
        while not done:
            _, done = downloader.next_chunk()
        return buffer.getvalue()

    def _gdrive_file_resource(self) -> Any:
        """Resource yielding the single Drive file named by ``file_id``.

        Fetching by ID needs access to the file only (not its folder) and is
        immune to Drive's eventually consistent listings and duplicate names.
        """
        from dlt.common.storages.fsspec_filesystem import MTIME_DISPATCH

        file_id = self.config.file_id

        @dlt.resource(name="filesystem", standalone=True)
        def gdrive_file() -> Iterator[List[FileItemDict]]:
            meta = (
                self._get_gdrive_service()
                .get(fileId=file_id, fields=_GDRIVE_FILE_FIELDS, supportsAllDrives=True)
                .execute()
            )
            mime_type = meta.get("mimeType", "")
            if mime_type.startswith(_GDRIVE_NATIVE_MIME_PREFIX):
                raise ValueError(
                    f"Drive file {file_id} ('{meta.get('name')}') is a native "
                    f"Google file ({mime_type}) with no downloadable content; "
                    "load Google Sheets with the google_sheets adapter"
                )
            item = FileItem(
                file_url=self.fs_url,
                file_name=meta["name"],
                relative_path=meta["name"],
                mime_type=mime_type,
                modification_date=MTIME_DISPATCH["gdrive"](meta),
                size_in_bytes=int(meta.get("size", 0)),
            )
            # a one-item page, matching the pages dlt's filesystem resource yields
            yield [_GdriveFileItem(item, file_id, self._download_gdrive_file)]

        return gdrive_file()

    def _file_source(self) -> Any:
        """Resource listing the files to read, as ``FileItemDict`` items.

        dlt's filesystem resource, except for a gdrive ``file_id`` source,
        which reads the one file by ID.
        """
        if self.config.file_id:
            return self._gdrive_file_resource()
        return filesystem(**self.get_filesystem_kwargs())

    def _read_csv_headers(self) -> Optional[list]:
        """Read CSV headers from the file without loading data.

        Returns:
            List of column names from CSV, or None if unable to read
        """
        try:
            import csv
            import io

            fs = self._file_source()

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
                    # utf-8-sig strips a leading BOM (Excel-exported CSVs) so the
                    # first header isn't read as "﻿col"; reads plain UTF-8
                    # unchanged.
                    encoding=self.config.encoding or "utf-8-sig",
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

        # Build map: original CSV column name -> normalized name
        header_map = {}
        for original in csv_headers:
            header_map[original] = normalize_identifier(original)

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

        # Only add credentials if explicitly provided; secret URIs are resolved
        # by value, whole or inside an object
        if self.config.credentials is not None:
            kwargs["credentials"] = _resolve_secret_uris(self.config.credentials)

        if self.config.filesystem_type == "gs":
            kwargs["kwargs"] = {"requests_timeout": _gcs_requests_timeout()}

        # Special handling for S3 endpoints
        if self.config.filesystem_type == "s3" and self.config.endpoint_url:
            if "client_kwargs" not in kwargs:
                kwargs["client_kwargs"] = {}
            kwargs["client_kwargs"]["endpoint_url"] = self.config.endpoint_url

        # SFTP: dlt reads through an fsspec instance passed as credentials, which
        # lets the connection carry a read timeout
        if self.config.filesystem_type == "sftp":
            kwargs["credentials"] = self._get_sftp_filesystem()

        # Google Drive: resolved credentials replace the raw config value, and
        # the filesystem is opened read-only
        if self.config.filesystem_type == "gdrive":
            kwargs["credentials"] = self._get_gdrive_credentials()
            kwargs["kwargs"] = {"access": "read_only"}

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

        if end_datetime:
            # An upper bound is honored whether or not initial_value is set — a
            # bare end_date caps the load at that date (initial_value=None means
            # "from the beginning"), rather than being silently dropped.
            if initial_datetime:
                logger.info(
                    f"Starting backfill from {initial_datetime} to {end_datetime}"
                )
            else:
                logger.info(f"Loading up to end_date {end_datetime}")
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

    def _create_csv_reader(self, fs, **kwargs):
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
            return fs | read_csv_duckdb(**csv_kwargs, parallel=True).with_name(
                "read_csv_duckdb"
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
            return fs | read_csv(**csv_kwargs).with_name("read_csv")

    def _create_file_reader(self, fs, **kwargs):
        """Create appropriate file reader based on file type."""
        file_type = self.config.file_type.lower() if self.config.file_type else "csv"

        if file_type == "csv":
            return self._create_csv_reader(fs, **kwargs)
        elif file_type == "parquet":
            return fs | read_parquet(**kwargs).with_name("read_parquet")
        elif file_type == "jsonl":
            return fs | read_jsonl(**kwargs).with_name("read_jsonl")
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

        fs = self._file_source()

        # Apply incremental hints
        fs = self._apply_incremental_hints(
            fs, actual_incremental_column, initial_datetime, end_datetime
        )

        # Create and return appropriate reader
        return self._create_file_reader(fs, **kwargs)

    def read_csv(self, **kwargs) -> Any:
        """Read CSV files, optionally with file metadata injection."""
        # Use metadata-injecting transformer if enabled
        if self.config.csv_include_file_metadata:
            logger.debug("Using CSV reader with file metadata injection")
            fs = self._file_source()
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
            return self._file_source() | read_csv_duckdb(
                **csv_kwargs, parallel=True
            ).with_name("read_csv_duckdb")
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

            return self._file_source() | read_csv(**csv_kwargs).with_name("read_csv")

    def read_parquet(self, **kwargs) -> Any:
        return self._file_source() | read_parquet(**kwargs).with_name("read_parquet")

    def read_jsonl(self, **kwargs) -> Any:
        return self._file_source() | read_jsonl(**kwargs).with_name("read_jsonl")

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
        return self._file_source() | self._get_json_array_transformer()

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

        encoding = self.config.encoding or "utf-8-sig"

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
                        content = content.decode(encoding)

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

    def _create_file_reader_with_metadata(self, fs, **kwargs):
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
        return self._create_file_reader_with_metadata(self._file_source(), **kwargs)

    def read_incremental_with_metadata(
        self,
        incremental_column: str = "modification_date",
        initial_value: Optional[Any] = None,
        **kwargs,
    ) -> Any:
        """Read files incrementally with metadata injection."""
        initial_datetime, end_datetime = self._parse_incremental_dates(initial_value)

        fs = self._apply_incremental_hints(
            self._file_source(), incremental_column, initial_datetime, end_datetime
        )

        return self._create_file_reader_with_metadata(fs, **kwargs)

    def _get_csv_with_metadata_transformer(self):
        """Get transformer for reading CSV files with file metadata injection.

        Returns a dlt transformer that streams CSV files and injects file metadata
        (_dlt_source_file_name, _dlt_source_modification_date) into each row.
        Uses pandas for encoding support.
        """
        from dlt.common.typing import TDataItems

        # Capture config values for use in transformer
        csv_separator = self.config.csv_separator or ","
        # utf-8-sig strips a leading BOM (Excel-exported CSVs); plain UTF-8 reads
        # unchanged.
        encoding = self.config.encoding or "utf-8-sig"

        @dlt.transformer(name="read_csv_with_metadata")
        def read_csv_with_metadata(
            items: Iterator[FileItemDict],
        ) -> Iterator[TDataItems]:
            """Read CSV files and inject file metadata into each row."""
            for file_obj in items:
                yield from _iter_csv_records_with_metadata(
                    file_obj, csv_separator, encoding
                )

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

        encoding = self.config.encoding or "utf-8-sig"

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
                        content = content.decode(encoding)

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
        return self._file_source() | self._get_geojson_transformer()

    def get_files(self, limit: Optional[int] = None) -> Iterator[FileItemDict]:
        """Get file items from filesystem with optional limit."""
        fs = self._file_source()
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
            fs = self._file_source()

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
