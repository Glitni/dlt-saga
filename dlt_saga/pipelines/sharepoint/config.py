import logging
from dataclasses import dataclass, field
from typing import Any, Dict, Optional

from dlt_saga.pipelines.base_config import BaseConfig
from dlt_saga.utility.secrets.secret_str import SecretStr, coerce_secret

logger = logging.getLogger(__name__)

# Deprecated config keys → their current names. Honoured at load time with a
# warning; remove in a future major version.
_DEPRECATED_KEY_ALIASES = {"auth_secret": "token_request_body"}


@dataclass
class SharePointConfig(BaseConfig):
    """SharePoint file source configuration.

    Authenticates via an OAuth2 client-credentials body stored in a secrets
    provider (e.g. Azure Key Vault).  The secret value must be a URL-encoded
    form body suitable for POSTing directly to the SharePoint token endpoint:

        grant_type=client_credentials
        &client_id=<app-id>@<tenant-id>
        &client_secret=<secret>
        &resource=00000003-0000-0ff1ce00-000000000000/<host>@<tenant-id>
    """

    # -------------------------------------------------------------------------
    # Authentication
    # -------------------------------------------------------------------------

    token_request_body: Optional[SecretStr] = field(
        default=None,
        metadata={
            "description": (
                "The OAuth2 token-request form body, supplied as a plain value, "
                "${ENV_VAR}, or secret URI (e.g. "
                "'azurekeyvault::https://my-vault.vault.azure.net::MY-SECRET-NAME'). "
                "The resolved value is POSTed directly to the SharePoint token endpoint."
            ),
            "required": True,
        },
    )
    tenant_id: str = field(
        default="",
        metadata={
            "description": "Azure AD tenant ID (GUID) — used to build the token endpoint URL",
            "required": True,
        },
    )

    # -------------------------------------------------------------------------
    # SharePoint location
    # -------------------------------------------------------------------------

    site_url: str = field(
        default="",
        metadata={
            "description": "SharePoint base URL (e.g. https://contoso.sharepoint.com)",
            "required": True,
        },
    )
    file_path: str = field(
        default="",
        metadata={
            "description": (
                "Server-relative path to the file "
                "(e.g. /sites/MySite/Shared Documents/report.xlsx)"
            ),
            "required": True,
        },
    )

    # -------------------------------------------------------------------------
    # File type
    # -------------------------------------------------------------------------

    file_type: str = field(
        default="",
        metadata={
            "description": "File format to read",
            "enum": ["xlsx", "csv", "json", "jsonl"],
            "required": True,
        },
    )

    # -------------------------------------------------------------------------
    # Excel options
    # -------------------------------------------------------------------------

    sheet_name: Optional[str] = field(
        default=None,
        metadata={"description": "Sheet name to read (default: active/first sheet)"},
    )
    header_row: int = field(
        default=1,
        metadata={"description": "1-indexed row number that contains column headers"},
    )

    # -------------------------------------------------------------------------
    # CSV options
    # -------------------------------------------------------------------------

    csv_separator: str = field(
        default=",",
        metadata={"description": "CSV separator character (default: comma)"},
    )
    encoding: str = field(
        default="utf-8",
        metadata={"description": "File encoding (default: utf-8)"},
    )

    def __post_init__(self):
        super().__post_init__()

        self.token_request_body = coerce_secret(self.token_request_body)

        required = ("tenant_id", "site_url", "file_path", "file_type")
        for name in required:
            if not getattr(self, name):
                raise ValueError(f"{name} is required for SharePoint pipelines")
        if not self.token_request_body:
            raise ValueError("token_request_body is required for SharePoint pipelines")

        valid_types = ("xlsx", "csv", "json", "jsonl")
        if self.file_type.lower() not in valid_types:
            raise ValueError(
                f"file_type '{self.file_type}' is not supported. "
                f"Supported types: {', '.join(valid_types)}"
            )

        if not isinstance(self.header_row, int) or self.header_row < 1:
            raise ValueError(
                f"header_row must be a positive integer, got {self.header_row!r}"
            )


def apply_deprecated_aliases(config: Dict[str, Any]) -> Dict[str, Any]:
    """Map deprecated SharePoint config keys to their current names.

    Honours the old key with a warning so existing configs keep working; the
    current key wins if both are present. Returns a new dict (input untouched).
    """
    out = dict(config)
    for old, new in _DEPRECATED_KEY_ALIASES.items():
        if old not in out:
            continue
        logger.warning(
            "SharePoint config key '%s' is deprecated; rename it to '%s'.", old, new
        )
        if new not in out:
            out[new] = out[old]
        out.pop(old, None)
    return out
