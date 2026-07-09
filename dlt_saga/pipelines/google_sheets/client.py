from datetime import datetime
from itertools import zip_longest
from typing import Any, Dict, Iterator, Optional

from dlt_saga.pipelines.google_sheets.config import GSheetsConfig
from dlt_saga.utility.tabular import dedupe_headers

# OAuth scopes needed for Sheets and Drive API access
# Must be tuple to match client_pool type hints
SCOPES = (
    "https://www.googleapis.com/auth/spreadsheets.readonly",
    "https://www.googleapis.com/auth/drive.metadata.readonly",
)


class GSheetsClient:
    def __init__(self, config: GSheetsConfig):
        from dlt_saga.utility.optional_deps import require_optional

        require_optional("googleapiclient", "Google Sheets pipelines")
        self.config = config
        # Per-instance (per-run) service cache. Building a googleapiclient service
        # fetches the API discovery document over HTTP, so without this a single
        # run built the sheets service twice (title + values) plus the drive
        # service — three discovery round-trips. The instance lives for one
        # pipeline run, so this reuse is short-lived and doesn't reintroduce the
        # stale-SSL problem that motivated the pool-level cache_client=False.
        self._sheets_service: Any = None
        self._drive_service: Any = None

    def _prepare_credentials_args(self) -> Dict:
        """Prepare credentials arguments for the connection pool.

        Returns:
            Dict with keys: secret_id, credentials_path, credentials_dict, scopes
        """
        # Determine which credential source to use
        secret_id = None
        credentials_path = None
        credentials_dict = None

        # 1. Check for direct credentials in source config
        if isinstance(self.config.credentials, dict):
            credentials_dict = self.config.credentials
        elif isinstance(self.config.credentials, str):
            credentials_path = self.config.credentials
        else:
            # 2. Check for secret manager config
            from dlt_saga.utility.gcp.client_pool import _get_google_secrets_value

            secret_id = self.config.secret_id or _get_google_secrets_value(
                "sheets_secret_name"
            )

        return {
            "secret_id": secret_id,
            "credentials_path": credentials_path,
            "credentials_dict": credentials_dict,
            "scopes": SCOPES,
        }

    def get_sheets_service(self):
        """Get Google Sheets API service, reused for this client's lifetime."""
        if self._sheets_service is None:
            from dlt_saga.utility.gcp.client_pool import google_api_pool

            cred_args = self._prepare_credentials_args()
            # Pool-level caching stays disabled (cache_client=False) to avoid
            # stale-SSL issues in Cloud Run; the instance holds the service for
            # the run so repeated calls don't re-fetch the discovery document.
            self._sheets_service = google_api_pool.get_client(
                "sheets", "v4", cache_client=False, **cred_args
            )
        return self._sheets_service

    def get_drive_service(self):
        """Get Google Drive API service for metadata access, reused per run."""
        if self._drive_service is None:
            from dlt_saga.utility.gcp.client_pool import google_api_pool

            cred_args = self._prepare_credentials_args()
            self._drive_service = google_api_pool.get_client(
                "drive", "v3", cache_client=False, **cred_args
            )
        return self._drive_service

    def get_service(self):
        """Backward compatibility - returns sheets service."""
        return self.get_sheets_service()

    def get_spreadsheet_title(self, spreadsheet_id: str) -> str:
        service = self.get_service()
        result = service.spreadsheets().get(spreadsheetId=spreadsheet_id).execute()
        return result["properties"]["title"]

    def get_last_modified_time(self, spreadsheet_id: str) -> datetime:
        """Get the last modified timestamp of the spreadsheet from Drive API.

        Args:
            spreadsheet_id: The ID of the Google Spreadsheet

        Returns:
            datetime object representing when the file was last modified
        """
        drive_service = self.get_drive_service()

        # Try with supportsAllDrives=True to handle Shared Drive files
        try:
            file_metadata = (
                drive_service.files()
                .get(
                    fileId=spreadsheet_id, fields="modifiedTime", supportsAllDrives=True
                )
                .execute()
            )
        except Exception:
            # Fallback to regular request without Shared Drive support
            file_metadata = (
                drive_service.files()
                .get(fileId=spreadsheet_id, fields="modifiedTime")
                .execute()
            )

        # Google Drive returns RFC 3339 formatted timestamp
        modified_time_str = file_metadata.get("modifiedTime")
        return datetime.fromisoformat(modified_time_str.replace("Z", "+00:00"))

    def get_sheet_data(
        self, spreadsheet_id: str, sheet_name: str, range: Optional[str] = None
    ) -> Iterator[Dict[str, Any]]:
        service = self.get_service()
        # No range → request the bare sheet name, which returns the entire grid
        # (all columns/rows). A fixed range like 'A:Z' truncates wider sheets.
        range_name = f"'{sheet_name}'!{range}" if range else f"'{sheet_name}'"
        result = (
            service.spreadsheets()
            .values()
            .get(spreadsheetId=spreadsheet_id, range=range_name)
            .execute()
        )
        values = result.get("values", [])

        if not values:
            return

        headers = dedupe_headers(values[0], source=f"sheet '{sheet_name}'")
        for row in values[1:]:
            yield dict(zip_longest(headers, row, fillvalue=""))
