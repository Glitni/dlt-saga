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
        if not self.sheet_name:
            raise ValueError(
                "sheet_name is required for Google Sheets pipelines. Each config maps "
                "to exactly one sheet/tab (one destination table); for a spreadsheet "
                "with several sheets, create one config per sheet."
            )

    # Required (has default for dataclass inheritance, validated in __post_init__)
    sheet_name: Optional[str] = field(
        default=None,
        metadata={
            "description": (
                "Name of the sheet/tab to read. Required — each config maps to "
                "exactly one sheet (one destination table)."
            ),
            "required": True,
        },
    )
    range: Optional[str] = field(
        default=None,
        metadata={
            "description": (
                "Optional cell range within the sheet in A1 notation (e.g. "
                "'A1:D10'). Omit to read the entire sheet — the default. A fixed "
                "range like 'A:Z' silently truncates sheets wider than that range."
            )
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
