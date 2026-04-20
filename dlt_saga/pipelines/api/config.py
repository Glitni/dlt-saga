"""Configuration for API pipelines."""

from dataclasses import dataclass, field
from typing import Any, Dict, Optional

from dlt_saga.pipelines.base_config import BaseConfig
from dlt_saga.utility.secrets.secret_str import SecretStr, coerce_secret


@dataclass
class ApiConfig(BaseConfig):
    """Configuration for API source.

    Authentication credentials can be:
    - Plain values: auth_token: "my_api_key"
    - Environment variables: auth_token: "${API_KEY}"
    - Secret URIs: auth_token: "googlesecretmanager::project::secret-name"
    """

    # API endpoint details (required, validated in __post_init__)
    base_url: str = field(
        default="",
        metadata={
            "description": "Base URL of the API (e.g., 'https://api.example.com')",
            "format": "uri",
            "required": True,
        },
    )
    endpoint: str = field(
        default="",
        metadata={
            "description": "API endpoint path (e.g., '/v1/users', '/data')",
            "required": True,
        },
    )

    # HTTP method
    method: str = field(
        default="GET",
        metadata={
            "description": "HTTP method to use",
            "enum": ["GET", "POST", "PUT", "PATCH", "DELETE"],
        },
    )

    # Query parameters
    query_params: Optional[Dict[str, Any]] = field(
        default=None,
        metadata={"description": "Query parameters to send with the request"},
    )

    # Authentication (flat structure)
    auth_type: Optional[str] = field(
        default="none",
        metadata={
            "description": "Authentication type",
            "enum": ["none", "api_key", "bearer"],
        },
    )
    auth_token: Optional[SecretStr] = field(
        default=None,
        metadata={
            "description": "API token or bearer token (plain value, ${ENV_VAR}, or secret URI: googlesecretmanager::project::secret)"
        },
    )
    auth_header_name: Optional[str] = field(
        default=None,
        metadata={
            "description": "Custom header name for authentication (default: Authorization for bearer, X-API-Key for api_key)"
        },
    )

    # Response processing
    response_path: Optional[str] = field(
        default=None,
        metadata={
            "description": "JSON path to extract data from response (e.g., 'data.users', 'results')"
        },
    )

    # Request headers (additional custom headers)
    headers: Optional[Dict[str, str]] = field(
        default=None,
        metadata={"description": "Additional custom headers to send with the request"},
    )

    # Timeout for requests
    timeout: int = field(
        default=30, metadata={"description": "Request timeout in seconds"}
    )

    # dlt normalization settings
    max_table_nesting: int = field(
        default=0,
        metadata={
            "description": "dlt normalization setting (0=store nested data as JSON, >0=create child tables)"
        },
    )

    # Rate limiting and retry configuration
    max_retries: int = field(
        default=3,
        metadata={
            "description": "Maximum number of retry attempts for transient errors (429, 503)"
        },
    )
    retry_backoff_base: int = field(
        default=2,
        metadata={
            "description": "Base for exponential backoff (seconds = base^attempt: 1s, 2s, 4s)"
        },
    )
    page_delay: float = field(
        default=0.0,
        metadata={
            "description": "Delay in seconds between paginated requests (0 = no delay). "
            "Use this to prevent rate limiting when fetching many pages."
        },
    )

    # Pagination configuration (optional — omit for single-request endpoints)
    pagination: Optional[Dict[str, Any]] = field(
        default=None,
        metadata={
            "description": (
                "Pagination configuration. Supported types: "
                "offset, page, cursor, next_url. "
                "Example: {type: offset, limit: 100, offset_param: offset, limit_param: limit}"
            ),
        },
    )

    def __post_init__(self):
        """Validate configuration after initialization."""
        # Call parent __post_init__ first
        super().__post_init__()

        # Coerce credential fields to SecretStr
        self.auth_token = coerce_secret(self.auth_token)

        # Validate required fields
        if not self.base_url:
            raise ValueError("base_url is required for API pipelines")
        if self.base_url and not self.base_url.startswith(("http://", "https://")):
            raise ValueError(
                f"base_url must start with http:// or https://, got '{self.base_url}'"
            )
        if not self.endpoint:
            raise ValueError("endpoint is required for API pipelines")

        # Validate numeric bounds
        if self.timeout < 1:
            raise ValueError(f"timeout must be >= 1, got {self.timeout}")
        if self.max_table_nesting < 0:
            raise ValueError(
                f"max_table_nesting must be >= 0, got {self.max_table_nesting}"
            )
        if self.max_retries < 0:
            raise ValueError(f"max_retries must be >= 0, got {self.max_retries}")
        if self.retry_backoff_base < 1:
            raise ValueError(
                f"retry_backoff_base must be >= 1, got {self.retry_backoff_base}"
            )
        if self.page_delay < 0:
            raise ValueError(f"page_delay must be >= 0, got {self.page_delay}")

        if self.pagination:
            _validate_pagination(self.pagination)


_VALID_PAGINATION_TYPES = ("offset", "page", "cursor", "next_url")


def _validate_pagination(pagination: Dict[str, Any]) -> None:
    """Validate pagination configuration dict."""
    ptype = pagination.get("type")
    if not ptype:
        raise ValueError("pagination.type is required (offset, page, cursor, next_url)")
    if ptype not in _VALID_PAGINATION_TYPES:
        raise ValueError(
            f"pagination.type must be one of {_VALID_PAGINATION_TYPES}, got '{ptype}'"
        )
    if ptype in ("offset", "page") and not pagination.get("limit"):
        raise ValueError(f"pagination.limit is required for type '{ptype}'")
    if ptype == "cursor" and not pagination.get("cursor_path"):
        raise ValueError("pagination.cursor_path is required for type 'cursor'")
    if ptype == "next_url" and not pagination.get("next_url_path"):
        raise ValueError("pagination.next_url_path is required for type 'next_url'")
