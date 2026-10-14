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
        # sheet_id 0 is valid (the first tab's gid), so compare against None
        if not self.sheet_name and self.sheet_id is None:
            raise ValueError(
                "sheet_name or sheet_id is required for Google Sheets pipelines. Each "
                "config maps to exactly one sheet/tab (one destination table); for a "
                "spreadsheet with several sheets, create one config per sheet."
            )
        if self.sheet_name and self.sheet_id is not None:
            raise ValueError(
                "Set either sheet_name or sheet_id for Google Sheets pipelines, not both"
            )
        if self.sheet_id is not None and (
            isinstance(self.sheet_id, bool)
            or not isinstance(self.sheet_id, int)
            or self.sheet_id < 0
        ):
            raise ValueError(
                f"sheet_id must be a non-negative integer (the gid in the sheet URL), "
                f"got {self.sheet_id!r}"
            )

    # One of sheet_name / sheet_id is required (validated in __post_init__)
    sheet_name: Optional[str] = field(
        default=None,
        metadata={
            "description": (
                "Name of the sheet/tab to read. Each config maps to exactly one "
                "sheet (one destination table). Set this or sheet_id."
            ),
        },
    )
    sheet_id: Optional[int] = field(
        default=None,
        metadata={
            "description": (
                "Numeric ID of the sheet/tab to read: the gid in the sheet URL "
                "(#gid=<ID>). Unlike sheet_name it survives renaming the tab. "
                "Set this or sheet_name."
            ),
            "minimum": 0,
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
    credentials: Optional[Union[Dict[str, Any], str]] = field(
        default=None,
        metadata={
            "description": (
                "Google service account credentials. Without credentials (or the "
                "deprecated secret_id), Application Default Credentials are used"
            ),
            "secret": True,
            "oneOf": [
                {
                    "type": "object",
                    "description": "Service account credentials as JSON object",
                },
                {
                    "type": "string",
                    "description": (
                        "Service account JSON (plain or a secret URI), or a path "
                        "to a credentials file"
                    ),
                },
            ],
        },
    )
    secret_id: Optional[str] = field(
        default=None,
        metadata={
            "description": (
                "Deprecated: use credentials: "
                "'googlesecretmanager::<project>::<secret>'. Google Secret Manager "
                "secret name for credentials"
            ),
        },
    )
