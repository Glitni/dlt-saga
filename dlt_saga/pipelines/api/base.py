"""Base pipeline for REST API sources."""

import logging
import os
import time
from dataclasses import fields
from typing import TYPE_CHECKING, Any, Dict, Iterator, List, Optional, Tuple, Union
from urllib.parse import urljoin

if TYPE_CHECKING:
    from dlt_saga.utility.secrets.secret_str import SecretStr

import dlt
import requests

from dlt_saga.pipelines.api.config import ApiConfig
from dlt_saga.pipelines.base_pipeline import BasePipeline

logger = logging.getLogger(__name__)


class BaseApiPipeline(BasePipeline):
    """Base pipeline for ingesting data from REST APIs.

    This pipeline provides generic functionality for calling REST APIs,
    handling authentication, and extracting data from JSON responses.

    Specific APIs can inherit from this class and override methods as needed.
    """

    def __init__(self, config: Dict[str, Any], log_prefix: str = None):
        """Initialize API pipeline.

        Args:
            config: Configuration dictionary
            log_prefix: Optional prefix for log messages
        """
        super().__init__(config, log_prefix)

        # Parse API-specific config via factory method (subclasses override)
        self.api_config = self._create_api_config(config)

    def _create_api_config(self, config_dict: Dict[str, Any]) -> ApiConfig:
        """Create the API config object for this pipeline.

        Subclasses should override this to return their specific config type
        (e.g. MblMedietallConfig, LivewrappedConfig). This eliminates double
        config construction — only one config object is created per pipeline.

        Args:
            config_dict: Configuration dictionary

        Returns:
            ApiConfig (or subclass) object
        """
        config_field_names = {f.name for f in fields(ApiConfig)}
        return ApiConfig(
            **{k: v for k, v in config_dict.items() if k in config_field_names}
        )

    def _get_auth_headers(self) -> Dict[str, str]:
        """Get authentication headers based on auth config.

        Returns:
            Dictionary of headers to add to request
        """
        if not self.api_config.auth_type or self.api_config.auth_type == "none":
            return {}

        headers = {}

        if self.api_config.auth_type == "api_key":
            # API key in custom header
            header_name = self.api_config.auth_header_name or "X-API-Key"
            token = self._resolve_token(self.api_config.auth_token)
            headers[header_name] = token

        elif self.api_config.auth_type == "bearer":
            # Bearer token in Authorization header
            token = self._resolve_token(self.api_config.auth_token)
            headers["Authorization"] = f"Bearer {token}"

        return headers

    def _resolve_token(self, token: Union[str, "SecretStr", None]) -> str:
        """Resolve token value (supports env vars and secret URIs).

        Args:
            token: Token value, ${ENV_VAR} reference, secret URI, or SecretStr

        Returns:
            Resolved token value
        """
        from dlt_saga.utility.secrets import resolve_secret
        from dlt_saga.utility.secrets.secret_str import SecretStr

        if not token:
            raise ValueError("Token value is required")

        # Unwrap SecretStr for env var check below
        raw = token.get_secret_value() if isinstance(token, SecretStr) else token

        # Support ${ENV_VAR} syntax
        if raw.startswith("${") and raw.endswith("}"):
            env_var = raw[2:-1]
            value = os.getenv(env_var)
            if not value:
                raise ValueError(
                    f"Environment variable '{env_var}' is not set for API authentication"
                )
            return value

        # Support secret URI format (googlesecretmanager::project::secret)
        # Also handles plain values (returns as-is)
        return resolve_secret(token)

    def _extract_data_from_response(self, response_data: Any) -> list:
        """Extract data array from response using response_path.

        Args:
            response_data: Parsed JSON response

        Returns:
            List of records to load
        """
        if not self.api_config.response_path:
            # No path specified - response should already be a list
            if isinstance(response_data, list):
                return response_data
            else:
                raise ValueError(
                    f"Response is not a list and no response_path specified. "
                    f"Got: {type(response_data)}"
                )

        # Navigate to data using dot notation (e.g., "data.users" or "sites")
        current = response_data
        for key in self.api_config.response_path.split("."):
            if not isinstance(current, dict):
                raise ValueError(
                    f"Cannot navigate path '{self.api_config.response_path}' "
                    f"- expected dict at '{key}', got {type(current)}"
                )

            if key not in current:
                raise ValueError(
                    f"Key '{key}' not found in response at path '{self.api_config.response_path}'"
                )

            current = current[key]

        if not isinstance(current, list):
            raise ValueError(
                f"Expected list at path '{self.api_config.response_path}', "
                f"got {type(current)}"
            )

        return current

    def _make_request(  # noqa: C901
        self,
        url: Optional[str] = None,
        query_params: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """Make HTTP request to API with retry logic for transient errors.

        Automatically retries transient errors (429, 5xx) with exponential backoff.
        The retry behavior is controlled by config parameters:
        - max_retries: Number of retry attempts (default: 3)
        - retry_backoff_base: Base for exponential backoff (default: 2, gives 1s, 2s, 4s)

        Args:
            url: Override URL (used by next_url pagination). Defaults to
                base_url + endpoint.
            query_params: Override query parameters (used by pagination).
                Defaults to self.api_config.query_params.

        Returns:
            Full JSON response dictionary

        Raises:
            ValueError: If API request fails after all retries
        """
        if url is None:
            url = urljoin(self.api_config.base_url, self.api_config.endpoint)
        if query_params is None:
            query_params = self.api_config.query_params

        # Build headers
        headers = {"Content-Type": "application/json"}

        # Add custom headers if specified
        if self.api_config.headers:
            headers.update(self.api_config.headers)

        # Add authentication headers
        auth_headers = self._get_auth_headers()
        headers.update(auth_headers)

        # Retry loop with exponential backoff
        max_retries = self.api_config.max_retries
        for attempt in range(max_retries + 1):
            self.logger.debug(
                f"Making API request: {self.api_config.method} {url} "
                f"(attempt {attempt + 1}/{max_retries + 1})"
            )

            try:
                response = requests.request(
                    method=self.api_config.method,
                    url=url,
                    params=query_params,
                    headers=headers,
                    timeout=self.api_config.timeout,
                )

                # Success case
                if 200 <= response.status_code <= 299:
                    if attempt > 0:
                        self.logger.debug(f"Request succeeded after {attempt} retries")
                    return response.json()

                # Transient errors - retry with exponential backoff
                # 429: rate limited; 5xx: server-side errors (transient blips, overload, etc.)
                if response.status_code == 429 or 500 <= response.status_code < 600:
                    if attempt < max_retries:
                        wait_seconds = self.api_config.retry_backoff_base**attempt
                        self.logger.debug(
                            f"HTTP {response.status_code} ({response.reason}): Transient error. "
                            f"Retrying in {wait_seconds}s (attempt {attempt + 1}/{max_retries})..."
                        )
                        time.sleep(wait_seconds)
                        continue
                    else:
                        # Max retries exceeded
                        raise ValueError(
                            f"API request failed after {max_retries} retries "
                            f"(HTTP {response.status_code}): {response.text}"
                        )

                # Non-retryable error (4xx except 429)
                raise ValueError(
                    f"API request failed with status {response.status_code}: {response.text}"
                )

            except requests.Timeout as e:
                # Timeout - always retry if we have attempts left
                if attempt < max_retries:
                    wait_seconds = self.api_config.retry_backoff_base**attempt
                    self.logger.info(
                        f"Request timeout after {self.api_config.timeout}s: {e}. "
                        f"Retrying in {wait_seconds}s (attempt {attempt + 1}/{max_retries})..."
                    )
                    time.sleep(wait_seconds)
                    continue
                raise ValueError(
                    f"API request timed out after {max_retries} retries "
                    f"(timeout: {self.api_config.timeout}s): {e}"
                )
            except requests.RequestException as e:
                # Check if it's a transient network error
                if attempt < max_retries and ("503" in str(e) or "429" in str(e)):
                    wait_seconds = self.api_config.retry_backoff_base**attempt
                    self.logger.warning(
                        f"Network error: {e}. "
                        f"Retrying in {wait_seconds}s (attempt {attempt + 1}/{max_retries})..."
                    )
                    time.sleep(wait_seconds)
                    continue
                raise ValueError(f"API request failed: {e}")

        # Should never reach here
        raise ValueError("Unexpected: _make_request loop completed without return")

    # ------------------------------------------------------------------
    # Pagination
    # ------------------------------------------------------------------

    @staticmethod
    def _resolve_json_path(data: Any, path: str) -> Any:
        """Navigate a JSON object using dot-notation path.

        Args:
            data: Parsed JSON (dict/list).
            path: Dot-separated path, e.g. ``"meta.next_cursor"``.

        Returns:
            The value at the path, or None if any segment is missing.
        """
        current = data
        for key in path.split("."):
            if not isinstance(current, dict) or key not in current:
                return None
            current = current[key]
        return current

    def _fetch_all_pages(self) -> Iterator[dict]:
        """Yield records across all pages.

        Reads ``self.api_config.pagination`` to determine the strategy:

        - **offset**: Sends ``offset_param`` and ``limit_param`` query params.
          Increments offset by ``limit`` each page. Stops when a page returns
          fewer items than ``limit``, or ``total_path`` is exhausted.
        - **page**: Sends ``page_param`` and ``page_size_param``. Increments
          page number each iteration. Same stop conditions as offset.
        - **cursor**: Reads the next cursor from ``cursor_path`` in the response
          and sends it as ``cursor_param``. Stops when cursor is null/empty.
        - **next_url**: Reads the next URL from ``next_url_path``. Stops when
          the URL is null/empty.
        """
        cfg = self.api_config.pagination
        ptype = cfg["type"]
        max_pages = cfg.get("max_pages", 10_000)
        page_delay = self.api_config.page_delay

        if ptype == "offset":
            yield from self._paginate_offset(cfg, max_pages, page_delay)
        elif ptype == "page":
            yield from self._paginate_page(cfg, max_pages, page_delay)
        elif ptype == "cursor":
            yield from self._paginate_cursor(cfg, max_pages, page_delay)
        elif ptype == "next_url":
            yield from self._paginate_next_url(cfg, max_pages, page_delay)

    def _paginate_offset(
        self, cfg: dict, max_pages: int, page_delay: float
    ) -> Iterator[dict]:
        limit = int(cfg["limit"])
        offset_param = cfg.get("offset_param", "offset")
        limit_param = cfg.get("limit_param", "limit")
        total_path = cfg.get("total_path")
        offset = int(cfg.get("start_offset", 0))

        base_params = dict(self.api_config.query_params or {})

        for page_num in range(max_pages):
            params = {**base_params, offset_param: offset, limit_param: limit}
            response = self._make_request(query_params=params)
            records = self._extract_data_from_response(response)

            if not records:
                break
            yield from records
            self.logger.debug(
                f"Page {page_num + 1}: {len(records)} records (offset={offset})"
            )

            # Check total-based stop condition
            if total_path:
                total = self._resolve_json_path(response, total_path)
                if total is not None and offset + limit >= int(total):
                    break

            # Fewer items than limit means last page
            if len(records) < limit:
                break

            offset += limit
            if page_delay:
                time.sleep(page_delay)

    def _paginate_page(
        self, cfg: dict, max_pages: int, page_delay: float
    ) -> Iterator[dict]:
        limit = int(cfg["limit"])
        page_param = cfg.get("page_param", "page")
        page_size_param = cfg.get("page_size_param", "page_size")
        total_path = cfg.get("total_path")
        page = int(cfg.get("start_page", 1))

        base_params = dict(self.api_config.query_params or {})

        for page_num in range(max_pages):
            params = {**base_params, page_param: page, page_size_param: limit}
            response = self._make_request(query_params=params)
            records = self._extract_data_from_response(response)

            if not records:
                break
            yield from records
            self.logger.debug(
                f"Page {page_num + 1}: {len(records)} records (page={page})"
            )

            if total_path:
                total = self._resolve_json_path(response, total_path)
                if total is not None and page * limit >= int(total):
                    break

            if len(records) < limit:
                break

            page += 1
            if page_delay:
                time.sleep(page_delay)

    def _paginate_cursor(
        self, cfg: dict, max_pages: int, page_delay: float
    ) -> Iterator[dict]:
        cursor_param = cfg.get("cursor_param", "cursor")
        cursor_path = cfg["cursor_path"]
        limit = cfg.get("limit")
        limit_param = cfg.get("limit_param", "limit")

        base_params = dict(self.api_config.query_params or {})
        cursor: Optional[str] = None

        for page_num in range(max_pages):
            params = {**base_params}
            if limit:
                params[limit_param] = int(limit)
            if cursor:
                params[cursor_param] = cursor

            response = self._make_request(query_params=params)
            records = self._extract_data_from_response(response)

            if records:
                yield from records
            self.logger.debug(f"Page {page_num + 1}: {len(records)} records")

            cursor = self._resolve_json_path(response, cursor_path)
            if not cursor:
                break
            if page_delay:
                time.sleep(page_delay)

    def _paginate_next_url(
        self, cfg: dict, max_pages: int, page_delay: float
    ) -> Iterator[dict]:
        next_url_path = cfg["next_url_path"]
        next_url: Optional[str] = None

        for page_num in range(max_pages):
            if next_url:
                response = self._make_request(url=next_url, query_params={})
            else:
                response = self._make_request()

            records = self._extract_data_from_response(response)
            if records:
                yield from records
            self.logger.debug(f"Page {page_num + 1}: {len(records)} records")

            next_url = self._resolve_json_path(response, next_url_path)
            if not next_url:
                break
            if page_delay:
                time.sleep(page_delay)

    def fetch_data(self) -> list:
        """Fetch data from API endpoint.

        Returns:
            List of records from API

        Raises:
            ValueError: If API request fails
        """
        # Make request and get full response
        response_data = self._make_request()

        # Extract data from response
        data = self._extract_data_from_response(response_data)

        self.logger.debug(f"Successfully fetched {len(data)} records from API")
        return data

    def extract_data(self) -> List[Tuple[Any, str]]:
        """Extract data from API.

        This is the main method called by BasePipeline.run().
        When pagination is configured, yields records lazily across pages
        so dlt can process them incrementally without loading all pages
        into memory.

        Returns:
            List of tuples containing (dlt.resource, description)
        """
        if self.api_config.pagination:
            data = self._fetch_all_pages()
        else:
            data = self.fetch_data()

        # Create a dlt resource with the fetched data
        # max_table_nesting=0 prevents normalization into subtables, stores nested data as JSON
        # max_table_nesting>0 creates child tables for nested structures
        resource = dlt.resource(
            data,
            name=self.table_name,
            max_table_nesting=self.api_config.max_table_nesting,
        )

        # Create a description for the table
        description = f"Data from {self.api_config.base_url}{self.api_config.endpoint}"

        return [(resource, description)]
