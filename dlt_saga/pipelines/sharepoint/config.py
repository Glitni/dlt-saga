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

    Two authentication methods are supported:

    * **Entra ID app-only with a certificate (recommended).** Set ``client_id``
      and ``certificate`` (a PEM containing the private key). This is
      Microsoft's replacement for the retired Azure ACS flow.
    * **Legacy Azure ACS app-only (deprecated).** Set ``token_request_body`` to
      a URL-encoded client-credentials form body. Azure ACS for SharePoint
      Online was retired by Microsoft and stopped working on 2 April 2026, so
      this path may fail; it remains only as a migration bridge.
    """

    # -------------------------------------------------------------------------
    # Authentication — Entra ID app-only with a certificate (recommended)
    # -------------------------------------------------------------------------

    client_id: str = field(
        default="",
        metadata={
            "description": (
                "Entra ID application (client) ID for app-only authentication. "
                "Required together with 'certificate'. Plain value, ${ENV_VAR}, "
                "or secret URI (resolved at runtime)."
            ),
        },
    )
    certificate: Optional[SecretStr] = field(
        default=None,
        metadata={
            "description": (
                "PEM-encoded certificate including the private key, supplied as a "
                "plain value, ${ENV_VAR}, or secret URI (e.g. "
                "'azurekeyvault::https://my-vault.vault.azure.net::MY-CERT'). "
                "Enables Entra ID app-only authentication."
            ),
        },
    )
    certificate_password: Optional[SecretStr] = field(
        default=None,
        metadata={
            "description": (
                "Password for the certificate's private key, if encrypted. "
                "Plain value, ${ENV_VAR}, or secret URI."
            ),
        },
    )

    # -------------------------------------------------------------------------
    # Authentication — legacy Azure ACS app-only (DEPRECATED)
    # -------------------------------------------------------------------------

    token_request_body: Optional[SecretStr] = field(
        default=None,
        metadata={
            "description": (
                "DEPRECATED — legacy Azure ACS app-only flow. Azure ACS for "
                "SharePoint Online was retired by Microsoft and stopped working on "
                "2 April 2026; use 'client_id' + 'certificate' instead. The OAuth2 "
                "token-request form body, supplied as a plain value, ${ENV_VAR}, or "
                "secret URI (e.g. "
                "'azurekeyvault::https://my-vault.vault.azure.net::MY-SECRET-NAME'). "
                "The resolved value is POSTed directly to the Azure ACS token endpoint."
            ),
        },
    )
    tenant_id: str = field(
        default="",
        metadata={
            "description": (
                "Azure AD tenant ID (GUID). Plain value, ${ENV_VAR}, or secret URI "
                "(resolved at runtime)."
            ),
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

        self.certificate = coerce_secret(self.certificate)
        self.certificate_password = coerce_secret(self.certificate_password)
        self.token_request_body = coerce_secret(self.token_request_body)

        required = ("tenant_id", "site_url", "file_path", "file_type")
        for name in required:
            if not getattr(self, name):
                raise ValueError(f"{name} is required for SharePoint pipelines")

        self._validate_auth()

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

    def _validate_auth(self) -> None:
        """Ensure a supported authentication method is configured."""
        if self.certificate:
            if not self.client_id:
                raise ValueError(
                    "client_id is required when using certificate authentication "
                    "for SharePoint pipelines"
                )
            return
        if self.token_request_body:
            # Legacy Azure ACS path; a deprecation warning is emitted at connect time.
            return
        raise ValueError(
            "SharePoint authentication is not configured. Provide 'client_id' and "
            "'certificate' for Entra ID app-only authentication (recommended), or "
            "the legacy 'token_request_body' for Azure ACS (deprecated, retired "
            "2 April 2026)."
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
