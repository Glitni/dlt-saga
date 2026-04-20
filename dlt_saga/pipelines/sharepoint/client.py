"""SharePoint REST API client.

Downloads files from SharePoint using the legacy SharePoint app-only
OAuth 2.0 flow (same as ADF LS_REST_SP_AZURE / LS_HTTP_AZURE_BINARY).

Authentication flow
-------------------
1. Fetch the OAuth2 form body from the configured secrets provider.
2. POST the body to the SharePoint token endpoint to obtain a Bearer token.
3. GET the file via the SharePoint REST API using the token.
"""

import csv
import io
import json
import logging
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional
from urllib.parse import quote

import requests

from dlt_saga.pipelines.base_client import BaseClient
from dlt_saga.utility.optional_deps import require_optional
from dlt_saga.utility.secrets.resolver import resolve_secret

from .config import SharePointConfig

logger = logging.getLogger(__name__)

_TOKEN_ENDPOINT = (
    "https://accounts.accesscontrol.windows.net/{tenant_id}/tokens/OAuth/2"
)


class SharePointClient(BaseClient):
    """Client for downloading and parsing SharePoint files."""

    def __init__(self, config: SharePointConfig):
        self.config = config
        self._token: Optional[str] = None

    # ------------------------------------------------------------------
    # BaseClient interface
    # ------------------------------------------------------------------

    def connect(self, credentials=None):
        self._token = self._acquire_token()

    def test_connection(self):
        if not self._token:
            self.connect()
        logger.info("SharePoint token acquired — connection OK")

    # ------------------------------------------------------------------
    # Public helpers
    # ------------------------------------------------------------------

    def get_last_modified_time(self) -> datetime:
        """Return the last-modified timestamp of the SharePoint file (UTC)."""
        if not self._token:
            self.connect()

        encoded_path = quote(self.config.file_path, safe="/-_.")
        url = (
            f"{self.config.site_url.rstrip('/')}/"
            f"_api/web/GetFileByServerRelativeUrl('{encoded_path}')"
            f"?$select=TimeLastModified"
        )
        response = requests.get(
            url,
            headers={
                "Authorization": f"Bearer {self._token}",
                "Accept": "application/json;odata=nometadata",
            },
            timeout=30,
        )
        response.raise_for_status()
        raw = response.json().get("TimeLastModified")
        if not raw:
            raise ValueError(
                "SharePoint file metadata response did not contain 'TimeLastModified'"
            )
        dt = datetime.fromisoformat(raw.rstrip("Z")).replace(tzinfo=timezone.utc)
        logger.debug("SharePoint file last modified: %s", dt.isoformat())
        return dt

    def download_file(self) -> bytes:
        """Download a file from SharePoint and return raw bytes."""
        if not self._token:
            self.connect()

        encoded_path = quote(self.config.file_path, safe="/-_.")
        url = (
            f"{self.config.site_url.rstrip('/')}/"
            f"_api/web/GetFileByServerRelativeUrl('{encoded_path}')/$value"
        )
        logger.debug("Downloading SharePoint file: %s", url)
        response = requests.get(
            url,
            headers={"Authorization": f"Bearer {self._token}"},
            timeout=120,
        )
        response.raise_for_status()
        logger.info("Downloaded %s bytes from SharePoint", f"{len(response.content):,}")
        return response.content

    def read_file(self, file_bytes: bytes) -> List[Dict[str, Any]]:
        """Dispatch to the correct reader based on config.file_type."""
        file_type = self.config.file_type.lower()
        if file_type == "xlsx":
            return self.read_excel(file_bytes)
        elif file_type == "csv":
            return self.read_csv(file_bytes)
        elif file_type == "json":
            return self.read_json(file_bytes)
        elif file_type == "jsonl":
            return self.read_jsonl(file_bytes)
        else:
            raise ValueError(f"Unsupported file_type: {file_type}")

    def read_excel(self, file_bytes: bytes) -> List[Dict[str, Any]]:
        """Parse an Excel workbook and return rows as a list of dicts."""
        require_optional("openpyxl", "SharePoint Excel files")
        import openpyxl

        wb = openpyxl.load_workbook(
            io.BytesIO(file_bytes), read_only=True, data_only=True
        )
        try:
            if self.config.sheet_name:
                if self.config.sheet_name not in wb.sheetnames:
                    raise ValueError(
                        f"Sheet '{self.config.sheet_name}' not found. "
                        f"Available sheets: {wb.sheetnames}"
                    )
                ws = wb[self.config.sheet_name]
            else:
                ws = wb.active

            rows = list(ws.iter_rows(values_only=True))
        finally:
            wb.close()

        if not rows:
            return []

        header_idx = self.config.header_row - 1
        headers = [
            str(cell) if cell is not None else f"col_{i}"
            for i, cell in enumerate(rows[header_idx])
        ]

        records: List[Dict[str, Any]] = []
        for row in rows[header_idx + 1 :]:
            if all(v is None for v in row):
                continue
            records.append(dict(zip(headers, row)))

        return records

    def read_csv(self, file_bytes: bytes) -> List[Dict[str, Any]]:
        """Parse a CSV file and return rows as a list of dicts."""
        text = file_bytes.decode(self.config.encoding)
        reader = csv.DictReader(io.StringIO(text), delimiter=self.config.csv_separator)
        return [dict(row) for row in reader]

    def read_json(self, file_bytes: bytes) -> List[Dict[str, Any]]:
        """Parse a JSON file (array at root) and return rows as a list of dicts."""
        data = json.loads(file_bytes.decode(self.config.encoding))
        if isinstance(data, list):
            return data
        raise ValueError(
            "JSON file must contain a root-level array. "
            f"Got {type(data).__name__} instead."
        )

    def read_jsonl(self, file_bytes: bytes) -> List[Dict[str, Any]]:
        """Parse a JSONL (newline-delimited JSON) file and return rows as a list of dicts."""
        text = file_bytes.decode(self.config.encoding)
        return [json.loads(line) for line in text.splitlines() if line.strip()]

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    def _acquire_token(self) -> str:
        oauth_body = resolve_secret(self.config.auth_secret)
        url = _TOKEN_ENDPOINT.format(tenant_id=self.config.tenant_id)
        response = requests.post(
            url,
            data=oauth_body,
            headers={"Content-Type": "application/x-www-form-urlencoded"},
            timeout=30,
        )
        response.raise_for_status()
        data = response.json()
        token = data.get("access_token")
        if not token:
            raise ValueError(
                "Token response did not contain 'access_token'. "
                f"Response keys: {list(data.keys())}"
            )
        logger.debug(
            "SharePoint Bearer token acquired (expires_in=%s)", data.get("expires_in")
        )
        return token
