from dataclasses import dataclass, field
from typing import Any, Dict, Optional, Union

from dlt_saga.pipelines.base_config import BaseConfig


@dataclass
class GSheetsConfig(BaseConfig):
    """Google Sheets source configuration"""

    # Required (has default for dataclass inheritance, validated in __post_init__)
    spreadsheet_id: str = field(
        default="",
        metadata={
            "description": "Google Sheets spreadsheet ID (found in the URL)",
            "pattern": "^[a-zA-Z0-9-_]+$",
            "required": True,  # Mark as required in metadata
        },
    )

    def __post_init__(self):
        """Validate configuration after initialization."""
        # Call parent __post_init__ first
        super().__post_init__()

        # Validate required fields
        if not self.spreadsheet_id:
            raise ValueError("spreadsheet_id is required for Google Sheets pipelines")

    # Optional
    sheet_name: Optional[str] = field(
        default=None,
        metadata={
            "description": "Name of the specific sheet/tab to read (optional, defaults to first sheet)"
        },
    )
    range: str = field(
        default="A:Z",
        metadata={
            "description": "Cell range to read in A1 notation (e.g., 'A:Z', 'Sheet1!A1:D10')"
        },
    )

    # Credentials
    secret_id: Optional[str] = field(
        default=None,
        metadata={"description": "Google Secret Manager secret ID for credentials"},
    )
    credentials: Optional[Union[Dict[str, Any], str]] = field(
        default=None,
        metadata={
            "description": "Google service account credentials (alternative to secret_id)",
            "oneOf": [
                {
                    "type": "object",
                    "description": "Service account credentials as JSON object",
                },
                {
                    "type": "string",
                    "description": "Path to credentials file or credentials JSON string",
                },
            ],
        },
    )
