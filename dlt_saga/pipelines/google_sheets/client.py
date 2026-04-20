from datetime import datetime
from itertools import zip_longest
from typing import Any, Dict, Iterator, List

from dlt_saga.pipelines.google_sheets.config import GSheetsConfig

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
        """Get Google Sheets API service from connection pool."""
        from dlt_saga.utility.gcp.client_pool import google_api_pool

        cred_args = self._prepare_credentials_args()
        # Disable client caching to avoid SSL connection issues in Cloud Run
        return google_api_pool.get_client(
            "sheets", "v4", cache_client=False, **cred_args
        )

    def get_drive_service(self):
        """Get Google Drive API service for metadata access from connection pool."""
        from dlt_saga.utility.gcp.client_pool import google_api_pool

        cred_args = self._prepare_credentials_args()
        # Disable client caching to avoid SSL connection issues in Cloud Run
        return google_api_pool.get_client(
            "drive", "v3", cache_client=False, **cred_args
        )

    def get_service(self):
        """Backward compatibility - returns sheets service."""
        return self.get_sheets_service()

    def get_sheet_names(self, spreadsheet_id: str) -> List[str]:
        service = self.get_service()
        result = service.spreadsheets().get(spreadsheetId=spreadsheet_id).execute()
        return [sheet["properties"]["title"] for sheet in result["sheets"]]

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
        self, spreadsheet_id: str, sheet_name: str, range: str = "A:Z"
    ) -> Iterator[Dict[str, Any]]:
        service = self.get_service()
        range_name = f"'{sheet_name}'!{range}"
        result = (
            service.spreadsheets()
            .values()
            .get(spreadsheetId=spreadsheet_id, range=range_name)
            .execute()
        )
        values = result.get("values", [])

        if not values:
            return

        headers = values[0]
        for row in values[1:]:
            yield dict(zip_longest(headers, row, fillvalue=""))
