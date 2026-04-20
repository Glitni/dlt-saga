"""Connection pooling for Google API and BigQuery clients.

This module provides thread-safe singleton pools for reusing API client connections
across multiple pipeline executions, reducing initialization overhead and memory usage.

All Google Cloud imports are lazy — the module can be imported without any
``google.*`` packages installed.  Pools are instantiated on first access
via ``__getattr__``.
"""

import json
import logging
import threading
from typing import Any, Dict, Optional, Tuple

logger = logging.getLogger(__name__)


def _get_google_secrets_value(key: str) -> Optional[str]:
    """Get a value from the google_secrets provider config in saga_project.yml.

    Args:
        key: The config key (e.g. "project_id", "sheets_secret_name")

    Returns:
        The config value, or None if not found.
    """
    from dlt_saga.project_config import get_providers_config

    providers = get_providers_config()
    google_secrets = providers.google_secrets
    return getattr(google_secrets, key, None) if google_secrets else None


class GoogleAPIClientPool:
    """Thread-safe pool for Google API clients (one per thread per service).

    Each thread gets its own client instances to avoid potential connection
    issues in multi-threaded environments. Credentials are cached and shared
    across threads since they're just data, not network connections.
    """

    _instance = None
    _lock = threading.Lock()

    def __new__(cls):
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = super().__new__(cls)
                    cls._instance._initialized = False
        return cls._instance

    def __init__(self):
        if not self._initialized:
            # Store clients keyed by (thread_id, service_name, version, cred_key)
            self._clients: Dict[Tuple[int, str, str, str], Any] = {}
            self._credentials_cache: Dict[str, Any] = {}
            self._client_lock = threading.Lock()
            self._cred_lock = threading.Lock()
            self._initialized = True

    def _get_credentials_key(
        self,
        secret_id: Optional[str] = None,
        credentials_path: Optional[str] = None,
        credentials_dict: Optional[Dict] = None,
        scopes: Optional[Tuple[str, ...]] = None,
    ) -> str:
        """Generate a unique cache key for credentials."""
        # Create a hashable representation
        if credentials_dict:
            # Use a sorted JSON dump for consistent hashing
            cred_repr = json.dumps(credentials_dict, sort_keys=True)
        elif credentials_path:
            cred_repr = f"file:{credentials_path}"
        elif secret_id:
            cred_repr = f"secret:{secret_id}"
        else:
            cred_repr = "default"

        scopes_repr = ":".join(sorted(scopes)) if scopes else "no_scopes"
        return f"{cred_repr}::{scopes_repr}"

    def get_credentials(
        self,
        secret_id: Optional[str] = None,
        credentials_path: Optional[str] = None,
        credentials_dict: Optional[Dict] = None,
        scopes: Optional[Tuple[str, ...]] = None,
    ) -> Any:
        """Get or create cached credentials.

        Args:
            secret_id: Google Secret Manager secret ID
            credentials_path: Path to service account JSON file
            credentials_dict: Service account credentials as dict
            scopes: OAuth scopes for the credentials

        Returns:
            Cached or newly created Credentials object
        """
        from google.oauth2.service_account import Credentials

        cache_key = self._get_credentials_key(
            secret_id, credentials_path, credentials_dict, scopes
        )

        # Check cache first (fast path without lock)
        if cache_key in self._credentials_cache:
            return self._credentials_cache[cache_key]

        # Slow path: create credentials with lock
        with self._cred_lock:
            # Double-check after acquiring lock
            if cache_key in self._credentials_cache:
                return self._credentials_cache[cache_key]

            # Create credentials based on source
            if credentials_dict:
                creds = Credentials.from_service_account_info(
                    credentials_dict, scopes=scopes
                )
            elif credentials_path:
                creds = Credentials.from_service_account_file(
                    credentials_path, scopes=scopes
                )
            elif secret_id:
                # Fetch from Secret Manager
                from google.cloud import secretmanager

                secrets_project = _get_google_secrets_value("project_id")
                if not secrets_project:
                    raise ValueError(
                        "Google Secrets project_id must be configured in "
                        "saga_project.yml (providers.google_secrets.project_id)"
                    )

                client = secretmanager.SecretManagerServiceClient()
                secret_name = (
                    f"projects/{secrets_project}/secrets/{secret_id}/versions/latest"
                )
                response = client.access_secret_version(request={"name": secret_name})
                sa_info = json.loads(response.payload.data.decode("UTF-8"))

                # Ensure scopes are properly set - if None, credentials won't work for Drive API
                if scopes is None:
                    raise ValueError(
                        "Scopes must be specified when creating credentials from Secret Manager"
                    )

                creds = Credentials.from_service_account_info(sa_info, scopes=scopes)
            else:
                # Application Default Credentials
                from google.auth import default

                creds, _ = default(scopes=scopes)

            # Cache and return
            self._credentials_cache[cache_key] = creds
            return creds

    def get_client(
        self,
        service_name: str,
        version: str,
        secret_id: Optional[str] = None,
        credentials_path: Optional[str] = None,
        credentials_dict: Optional[Dict] = None,
        scopes: Optional[Tuple[str, ...]] = None,
        cache_client: bool = True,
    ) -> Any:
        """Get or create a Google API client for the current thread.

        Each thread gets its own client to avoid potential connection issues
        in multi-threaded environments.

        Args:
            service_name: API service name (e.g., 'sheets', 'drive')
            version: API version (e.g., 'v4', 'v3')
            secret_id: Google Secret Manager secret ID
            credentials_path: Path to service account JSON file
            credentials_dict: Service account credentials as dict
            scopes: OAuth scopes for the client
            cache_client: Whether to cache the client (default True). Set to False
                         to create a new client for every request.

        Returns:
            Cached or newly created API client Resource
        """
        from googleapiclient.discovery import build

        # Get or create credentials (always cached, thread-safe)
        creds = self.get_credentials(
            secret_id, credentials_path, credentials_dict, scopes
        )

        # If client caching is disabled, always create a new client
        if not cache_client:
            return build(service_name, version, credentials=creds)

        # Create cache key for the client (include thread_id for per-thread clients)
        thread_id = threading.get_ident()
        cred_key = self._get_credentials_key(
            secret_id, credentials_path, credentials_dict, scopes
        )
        client_key = (thread_id, service_name, version, cred_key)

        # Check cache first (fast path without lock)
        if client_key in self._clients:
            return self._clients[client_key]

        # Slow path: create client with lock
        with self._client_lock:
            # Double-check after acquiring lock
            if client_key in self._clients:
                return self._clients[client_key]

            # Build the API client for this thread
            client = build(service_name, version, credentials=creds)
            self._clients[client_key] = client
            logger.debug(
                f"Created {service_name} {version} client for thread {thread_id}"
            )
            return client

    def clear_cache(self):
        """Clear all cached clients and credentials. Useful for testing."""
        with self._client_lock, self._cred_lock:
            self._clients.clear()
            self._credentials_cache.clear()
            logger.info("Cleared Google API client pool cache")


class BigQueryClientPool:
    """Thread-safe pool for BigQuery clients (one per thread per project).

    gRPC clients are NOT thread-safe for concurrent requests, so we maintain
    a separate client instance per thread per project to avoid stream corruption.
    """

    _instance = None
    _lock = threading.Lock()

    def __new__(cls):
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = super().__new__(cls)
                    cls._instance._initialized = False
        return cls._instance

    def __init__(self):
        if not self._initialized:
            self._clients: Dict[tuple[int, str], Any] = {}
            self._client_lock = threading.Lock()
            self._initialized = True

    def get_client(self, project_id: str) -> Any:
        """Get or create a BigQuery client for the current thread and project.

        Each thread gets its own client per project to avoid gRPC stream corruption
        when making concurrent requests.

        Args:
            project_id: GCP project ID

        Returns:
            BigQuery client for current thread and project
        """
        from google.cloud import bigquery

        thread_id = threading.get_ident()
        client_key = (thread_id, project_id)

        # Check if current thread has a client for this project (fast path without lock)
        if client_key in self._clients:
            return self._clients[client_key]

        # Slow path: create client for this thread and project with lock
        with self._client_lock:
            # Double-check after acquiring lock
            if client_key in self._clients:
                return self._clients[client_key]

            # Create new client for this thread and project
            client = bigquery.Client(project=project_id)
            self._clients[client_key] = client
            logger.debug(
                f"Created BigQuery client for thread {thread_id}, project {project_id}"
            )
            return client

    def clear_cache(self):
        """Clear all cached clients. Useful for testing."""
        with self._client_lock:
            # Close all clients
            for client in self._clients.values():
                client.close()
            self._clients.clear()
            logger.info("Cleared BigQuery client pool cache")


class SecretManagerClientPool:
    """Thread-safe pool for Secret Manager clients (one per thread).

    gRPC clients are NOT thread-safe for concurrent requests, so we maintain
    a separate client instance per thread to avoid stream corruption.
    """

    _instance = None
    _lock = threading.Lock()

    def __new__(cls):
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = super().__new__(cls)
                    cls._instance._initialized = False
        return cls._instance

    def __init__(self):
        if not self._initialized:
            self._clients: Dict[int, Any] = {}
            self._client_lock = threading.Lock()
            self._initialized = True

    def get_client(self) -> Any:
        """Get or create a Secret Manager client for the current thread.

        Each thread gets its own client to avoid gRPC stream corruption
        when making concurrent requests.

        Returns:
            Secret Manager client for current thread
        """
        from google.cloud import secretmanager

        thread_id = threading.get_ident()

        # Check if current thread has a client (fast path without lock)
        if thread_id in self._clients:
            return self._clients[thread_id]

        # Slow path: create client for this thread with lock
        with self._client_lock:
            # Double-check after acquiring lock
            if thread_id in self._clients:
                return self._clients[thread_id]

            # Create new client for this thread
            client = secretmanager.SecretManagerServiceClient()
            self._clients[thread_id] = client
            logger.debug(f"Created Secret Manager client for thread {thread_id}")
            return client

    def clear_cache(self):
        """Clear cached clients. Useful for testing."""
        with self._client_lock:
            self._clients.clear()
            logger.info("Cleared Secret Manager client pool cache")


# ---------------------------------------------------------------------------
# Lazy singleton instances — instantiated on first access so that importing
# this module does not require any google.* packages to be installed.
# ---------------------------------------------------------------------------

_google_api_pool: Optional[GoogleAPIClientPool] = None
_bigquery_pool: Optional[BigQueryClientPool] = None
_secret_manager_pool: Optional[SecretManagerClientPool] = None


def __getattr__(name: str) -> Any:
    global _google_api_pool, _bigquery_pool, _secret_manager_pool

    if name == "google_api_pool":
        if _google_api_pool is None:
            _google_api_pool = GoogleAPIClientPool()
        return _google_api_pool
    if name == "bigquery_pool":
        if _bigquery_pool is None:
            _bigquery_pool = BigQueryClientPool()
        return _bigquery_pool
    if name == "secret_manager_pool":
        if _secret_manager_pool is None:
            _secret_manager_pool = SecretManagerClientPool()
        return _secret_manager_pool

    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
