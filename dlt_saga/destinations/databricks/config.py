"""Databricks destination configuration."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Dict, List, Optional

from dlt_saga.destinations.config import DestinationConfig

if TYPE_CHECKING:
    from dlt_saga.utility.cli.context import ExecutionContext


@dataclass
class DatabricksDestinationConfig(DestinationConfig):
    """Databricks Unity Catalog destination configuration.

    Supports PAT, OAuth M2M, and OAuth U2M auth modes via the Databricks SDK.
    ``catalog`` is the Unity Catalog catalog name; ``schema_name`` is the
    target schema within that catalog.
    """

    # Databricks' idiomatic alias for the generic `database` profile key.
    PROFILE_KEY_ALIASES = {"database": ("catalog",)}

    server_hostname: str = field(
        default="",
        metadata={
            "profile_field": True,
            "description": (
                "Databricks workspace hostname "
                "(e.g., adb-1234567890.12.azuredatabricks.net). "
                "Find it in Settings → Developer → SQL Warehouse → Connection Details."
            ),
        },
    )
    http_path: str = field(
        default="",
        metadata={
            "profile_field": True,
            "description": (
                "Databricks SQL Warehouse HTTP path "
                "(e.g., /sql/1.0/warehouses/abc123). "
                "Find it in the warehouse's Connection Details tab."
            ),
        },
    )
    # Unity Catalog name. Exposed in the profile schema as a generic alias of
    # 'database' (works on any target), so it's not flagged as Databricks-only.
    catalog: str = ""
    schema_name: Optional[str] = None  # profile key: schema
    schema_access: Optional[List[str]] = None  # pipeline-level, not a profile key
    auth_mode: Optional[str] = field(  # "pat", "m2m", "u2m", or None (SDK auto-detect)
        default=None,
        metadata={
            "profile_field": True,
            "enum": ["u2m", "m2m", "pat"],
            "description": (
                "Databricks authentication mode. "
                "'u2m': browser OAuth (default). "
                "'m2m': service principal OAuth. "
                "'pat': personal access token."
            ),
        },
    )
    access_token: Optional[str] = field(  # PAT only; resolved via SecretResolver
        default=None,
        metadata={
            "profile_field": True,
            "description": (
                "Databricks personal access token (auth_mode: pat). "
                "Prefer a secret URI: "
                "'azurekeyvault::https://my-vault.vault.azure.net::secret-name'"
            ),
        },
    )
    client_id: Optional[str] = field(  # M2M only
        default=None,
        metadata={
            "profile_field": True,
            "description": "Databricks service principal client ID (auth_mode: m2m)",
        },
    )
    client_secret: Optional[str] = field(  # M2M only; resolved via SecretResolver
        default=None,
        metadata={
            "profile_field": True,
            "description": (
                "Databricks service principal client secret (auth_mode: m2m). "
                "Prefer a secret URI: "
                "'azurekeyvault::https://my-vault.vault.azure.net::secret-name'"
            ),
        },
    )
    destination_type: str = "databricks"

    # Unity Catalog volume staging.
    # dlt defaults to creating '_dlt_staging_load_volume' in the target schema;
    # set this to use an existing shared volume instead.
    staging_volume_name: Optional[str] = field(
        default=None,
        metadata={
            "profile_field": True,
            "description": (
                "Fully-qualified Unity Catalog volume used to stage Parquet files "
                "before COPY INTO (e.g., 'my_catalog.my_schema.ingest_volume'). "
                "Recommended: point to a shared team volume. "
                "When omitted, dlt auto-creates '_dlt_staging_load_volume' in the target schema."
            ),
        },
    )
    staging_credentials_name: Optional[str] = field(
        default=None,
        metadata={
            "profile_field": True,
            "description": (
                "Named Unity Catalog storage credential used in COPY INTO (optional)"
            ),
        },
    )
    storage_root: Optional[str] = field(
        default=None,
        metadata={
            "profile_field": True,
            "description": (
                "Base cloud storage location for external Delta/Iceberg tables "
                "(e.g., 'abfss://container@account.dfs.core.windows.net/raw/'). "
                "When set, native_load creates external tables at "
                "'<storage_root>/<pipeline_group>/<table_name>/' unless overridden "
                "per pipeline. Databricks-specific."
            ),
        },
    )

    @property
    def database(self) -> str:
        return self.catalog

    def __post_init__(self) -> None:
        """Validate required fields after initialisation."""
        if not self.server_hostname:
            raise ValueError(
                "server_hostname is required for Databricks destination. "
                "Set it in profiles.yml: server_hostname: adb-1234.azuredatabricks.net"
            )
        if not self.http_path:
            raise ValueError(
                "http_path is required for Databricks destination. "
                "Set it in profiles.yml: http_path: /sql/1.0/warehouses/abc123"
            )
        if not self.catalog:
            raise ValueError(
                "catalog is required for Databricks destination. "
                "Set it in profiles.yml: catalog: my_catalog"
            )

    @property
    def host(self) -> str:
        """Workspace URL with https:// prefix, as expected by the SDK."""
        if self.server_hostname.startswith("https://"):
            return self.server_hostname
        return f"https://{self.server_hostname}"

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "DatabricksDestinationConfig":
        """Create Databricks config from a dictionary.

        Args:
            data: Configuration dictionary.

        Returns:
            DatabricksDestinationConfig instance.
        """
        return cls(
            destination_type="databricks",
            server_hostname=data.get("server_hostname", ""),
            http_path=data.get("http_path", ""),
            catalog=data.get("catalog") or data.get("database", ""),
            schema_name=data.get("schema_name"),
            schema_access=data.get("schema_access") or data.get("dataset_access"),
            auth_mode=data.get("auth_mode"),
            access_token=data.get("access_token"),
            client_id=data.get("client_id"),
            client_secret=data.get("client_secret"),
            staging_volume_name=data.get("staging_volume_name"),
            staging_credentials_name=data.get("staging_credentials_name"),
            storage_root=data.get("storage_root"),
        )

    @classmethod
    def from_context(
        cls,
        context: "ExecutionContext",
        config_dict: Dict[str, Any],
    ) -> "DatabricksDestinationConfig":
        """Resolve Databricks config from execution context and pipeline config.

        Field resolution priority:
            server_hostname: profile (destination_config) only
            http_path:       profile (destination_config) only
            catalog:         profile (destination_config) — "catalog" key,
                             falls back to "database" key (generic alias)
            schema_name:     already resolved by ConfigSource
            auth_mode:       profile (destination_config)
            access_token:    profile (destination_config), resolved via SecretResolver
            client_id:       profile (destination_config)
            client_secret:   profile (destination_config), resolved via SecretResolver
        """
        from dlt_saga.utility.secrets.resolver import SecretResolver

        pt = context.profile_target
        dc = pt.destination_config if pt else {}

        server_hostname = dc.get("server_hostname", "")
        http_path = dc.get("http_path", "")
        catalog = dc.get("catalog") or dc.get("database", "")

        raw_token = dc.get("access_token")
        access_token: Optional[str] = (
            SecretResolver.resolve(raw_token) if raw_token else None
        )

        raw_secret = dc.get("client_secret")
        client_secret: Optional[str] = (
            SecretResolver.resolve(raw_secret) if raw_secret else None
        )

        return cls(
            destination_type="databricks",
            server_hostname=server_hostname,
            http_path=http_path,
            catalog=catalog,
            schema_name=config_dict.get("schema_name"),
            schema_access=config_dict.get("schema_access")
            or config_dict.get("dataset_access"),
            auth_mode=dc.get("auth_mode"),
            access_token=access_token,
            client_id=dc.get("client_id"),
            client_secret=client_secret,
            staging_volume_name=dc.get("staging_volume_name"),
            staging_credentials_name=dc.get("staging_credentials_name"),
            storage_root=dc.get("storage_root"),
        )
