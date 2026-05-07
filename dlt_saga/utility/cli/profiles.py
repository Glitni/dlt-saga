"""dbt-style profiles management for dlt.

This module provides a profiles-based configuration system similar to dbt,
allowing users to define multiple targets (dev, prod, etc.) with different
credentials, projects, and settings.
"""

import logging
import os
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, Optional

import yaml

from dlt_saga.utility.env import get_env

logger = logging.getLogger(__name__)


def interpolate_env_vars(value: str) -> str:
    """Interpolate environment variables in a string.

    Supports dbt-style syntax: {{ env_var('VAR_NAME') }} or {{ env_var('VAR_NAME', 'default') }}

    Args:
        value: String that may contain env var references

    Returns:
        String with environment variables interpolated
    """
    if not isinstance(value, str):
        return value

    # Pattern: {{ env_var('VAR_NAME') }} or {{ env_var('VAR_NAME', 'default') }}
    pattern = r"{{\s*env_var\s*\(\s*['\"]([^'\"]+)['\"]\s*(?:,\s*['\"]([^'\"]*)['\"]\s*)?\)\s*}}"

    def replace_env_var(match):
        var_name = match.group(1)
        default_value = match.group(2) if match.group(2) is not None else ""
        return os.getenv(var_name, default_value)

    return re.sub(pattern, replace_env_var, value)


# Fields that are core (destination-agnostic) on ProfileTarget.
# Everything else in the YAML target goes into destination_config.
_CORE_FIELDS = {
    "environment",
    "type",
    "destination_type",  # alias for "type"
    "schema",
    "dataset",  # alias for "schema"
    "dev_row_limit",
    "table_format",
    "auth_provider",
    "historize",
}


@dataclass
class ProfileTarget:
    """Represents a single target configuration within a profile.

    A target defines the connection and execution parameters for a specific
    environment (dev, prod, etc.).

    Core fields are destination-agnostic (environment, schema, destination_type).
    Destination-specific fields (database, location, run_as, database_path,
    storage_path, etc.) live in destination_config.
    """

    name: str = field(
        metadata={
            "description": "Target name (derived from the YAML key)",
        }
    )
    environment: str = field(
        metadata={
            "description": (
                "Environment name — controls naming strategy. "
                "Defaults to the target name if not specified."
            ),
            "enum": ["dev", "prod"],
        }
    )
    destination_type: str = field(
        metadata={
            "description": "Destination type (required in profiles.yml as 'type:')",
            "enum": [
                "bigquery",
                "databricks",
                "duckdb",
                "snowflake",
                "redshift",
                "postgres",
            ],
        },
    )
    schema: Optional[str] = field(
        default=None,
        metadata={
            "description": (
                "Schema/dataset name. Supports env var interpolation: "
                "{{ env_var('VAR', 'default') }}"
            ),
        },
    )
    dev_row_limit: Optional[int] = field(
        default=None,
        metadata={
            "description": "Row limit for dev testing (limits rows extracted per resource)",
        },
    )
    table_format: str = field(
        default="native",
        metadata={
            "description": "Table format for the destination",
            "enum": ["native", "iceberg"],
        },
    )
    auth_provider: Optional[str] = field(
        default=None,
        metadata={
            "description": "Explicit auth provider name",
            "enum": ["gcp", "azure", "databricks", "aws"],
        },
    )
    historize: Dict[str, Any] = field(
        default_factory=dict,
        metadata={
            "description": (
                "Historize-layer overrides. Supports table_format and storage_path keys "
                "to select a different table format for historized tables than for ingest tables."
            ),
        },
    )
    destination_config: Dict[str, Any] = field(
        default_factory=dict,
        metadata={
            "description": (
                "Additional destination-specific configuration fields "
                "(database, location, billing_project, run_as, "
                "database_path, storage_path, etc.)"
            ),
        },
    )

    # ---- convenience accessors for common destination_config fields ----

    @property
    def database(self) -> Optional[str]:
        # "database" is the canonical profile key. Destination-specific names
        # are accepted as aliases: "catalog" (Databricks), "project_id" / "project"
        # (BigQuery legacy).
        dc = self.destination_config
        return (
            dc.get("database")
            or dc.get("catalog")
            or dc.get("project_id")
            or dc.get("project")
        )

    @property
    def location(self) -> Optional[str]:
        return self.destination_config.get("location")

    @property
    def run_as(self) -> Optional[str]:
        return self.destination_config.get("run_as")

    @property
    def billing_project(self) -> Optional[str]:
        return self.destination_config.get("billing_project")

    @property
    def database_path(self) -> Optional[str]:
        return self.destination_config.get("database_path")

    @property
    def storage_path(self) -> Optional[str]:
        return self.destination_config.get("storage_path")

    @property
    def historize_table_format(self) -> Optional[str]:
        """Historize-layer table_format override, or None if not set."""
        return self.historize.get("table_format") or None

    @property
    def historize_storage_path(self) -> Optional[str]:
        """Historize-layer storage_path override, or None if not set."""
        return self.historize.get("storage_path") or None

    @property
    def server_hostname(self) -> Optional[str]:
        return self.destination_config.get("server_hostname")

    @property
    def http_path(self) -> Optional[str]:
        return self.destination_config.get("http_path")

    @property
    def catalog(self) -> Optional[str]:
        return self.database

    @classmethod
    def from_dict(cls, name: str, config: Dict[str, Any]) -> "ProfileTarget":
        """Create a ProfileTarget from a dictionary configuration.

        Supports environment variable interpolation using {{ env_var('VAR') }} syntax.

        The 'environment' field controls naming strategy:
        - If not specified, defaults to the target name (e.g., 'dev' -> 'dev', 'prod' -> 'prod')
        - 'dev': Single dataset with prefixed table names
        - 'prod': Separate datasets per pipeline type with non-prefixed tables

        Args:
            name: Name of the target
            config: Dictionary containing target configuration

        Returns:
            ProfileTarget instance
        """
        # Environment defaults to target name if not specified
        environment = config.get("environment", name)

        # Destination type (required)
        # Accept both 'type' (canonical, dbt-style) and 'destination_type' (alias)
        destination_type = config.get("type") or config.get("destination_type")
        if not destination_type:
            raise ValueError(
                f"Profile target '{name}' is missing required field 'type' "
                f"(e.g. bigquery, duckdb)"
            )

        # Schema (generic term for dataset/schema)
        # Accept both 'schema' (canonical) and 'dataset' (alias)
        schema_raw = config.get("schema") or config.get("dataset")
        schema = interpolate_env_vars(schema_raw) if schema_raw else None

        # Row limit for dev testing
        dev_row_limit = config.get("dev_row_limit")

        # Table format
        table_format = config.get("table_format", "native")

        # Auth provider (explicit override, e.g., "gcp", "azure", "aws")
        auth_provider = config.get("auth_provider")

        # Historize-layer overrides (table_format, storage_path)
        historize_raw = config.get("historize") or {}
        historize: Dict[str, Any] = {
            k: interpolate_env_vars(v) if isinstance(v, str) else v
            for k, v in historize_raw.items()
        }

        # Everything not in _CORE_FIELDS goes into destination_config
        dest_config: Dict[str, Any] = {}
        for key, value in config.items():
            if key not in _CORE_FIELDS:
                if isinstance(value, str):
                    dest_config[key] = interpolate_env_vars(value)
                else:
                    dest_config[key] = value

        return cls(
            name=name,
            environment=environment,
            destination_type=destination_type,
            schema=schema,
            dev_row_limit=dev_row_limit,
            table_format=table_format,
            auth_provider=auth_provider,
            historize=historize,
            destination_config=dest_config,
        )


@dataclass
class Profile:
    """Represents a dlt profile with multiple targets.

    A profile contains a default target and multiple output targets that can
    be selected via the --target CLI parameter.
    """

    name: str = field(
        metadata={
            "description": "Profile name (derived from the YAML key)",
        }
    )
    default_target: str = field(
        metadata={
            "description": "Default target name to use when --target is not specified",
        }
    )
    targets: Dict[str, ProfileTarget] = field(
        metadata={
            "description": "Named output targets",
        }
    )

    def get_target(self, target_name: Optional[str] = None) -> ProfileTarget:
        """Get a target by name, or the default target if not specified.

        Args:
            target_name: Name of the target to retrieve, or None for default

        Returns:
            ProfileTarget instance

        Raises:
            ValueError: If the target doesn't exist
        """
        name = target_name or self.default_target

        if name not in self.targets:
            available = ", ".join(self.targets.keys())
            raise ValueError(
                f"Target '{name}' not found in profile '{self.name}'. "
                f"Available targets: {available}"
            )

        return self.targets[name]

    @classmethod
    def from_dict(cls, name: str, config: Dict[str, Any]) -> "Profile":
        """Create a Profile from a dictionary configuration.

        Args:
            name: Name of the profile
            config: Dictionary containing profile configuration

        Returns:
            Profile instance
        """
        outputs = config.get("outputs", {})
        if not outputs:
            raise ValueError(f"Profile '{name}' has no outputs defined")

        targets = {
            target_name: ProfileTarget.from_dict(target_name, target_config)
            for target_name, target_config in outputs.items()
        }

        default_target = config.get("target")
        if not default_target:
            # Use first target as default if not specified
            default_target = next(iter(targets.keys()))
            logger.warning(
                f"No default target specified for profile '{name}', using '{default_target}'"
            )

        if default_target not in targets:
            raise ValueError(
                f"Default target '{default_target}' not found in profile '{name}' outputs"
            )

        return cls(
            name=name,
            default_target=default_target,
            targets=targets,
        )


class ProfilesConfig:
    """Manages dlt profiles configuration from profiles.yml file."""

    def __init__(self, profiles_path: Optional[Path] = None):
        """Initialize profiles configuration.

        Args:
            profiles_path: Explicit path to profiles.yml. If None, uses
                the search path: SAGA_PROFILES_DIR env var → repo root → .dlt/
        """
        self.profiles_path = profiles_path or _find_profiles_file()
        self._profiles: Optional[Dict[str, Profile]] = None

    def load_profiles(self) -> Dict[str, Profile]:
        """Load profiles from profiles.yml file.

        Returns:
            Dictionary mapping profile names to Profile objects

        Raises:
            FileNotFoundError: If profiles.yml doesn't exist
            ValueError: If profiles.yml is invalid
        """
        if self._profiles is not None:
            return self._profiles

        if self.profiles_path is None or not self.profiles_path.exists():
            raise FileNotFoundError(
                "Profiles file not found. Searched: SAGA_PROFILES_DIR env var, "
                "./profiles.yml, .dlt/profiles.yml. "
                "Create a profiles.yml file to use profiles."
            )

        try:
            with open(self.profiles_path, "r") as f:
                profiles_data = yaml.safe_load(f) or {}
        except Exception as e:
            raise ValueError(f"Failed to parse profiles.yml: {e}") from e

        if not profiles_data:
            raise ValueError("profiles.yml is empty")

        profiles = {}
        for profile_name, profile_config in profiles_data.items():
            # Skip YAML anchors (entries starting with underscore)
            if profile_name.startswith("_"):
                logger.debug(f"Skipping YAML anchor definition: {profile_name}")
                continue

            try:
                profiles[profile_name] = Profile.from_dict(profile_name, profile_config)
            except Exception as e:
                logger.error(f"Failed to load profile '{profile_name}': {e}")
                continue

        if not profiles:
            raise ValueError("No valid profiles found in profiles.yml")

        self._profiles = profiles
        logger.debug(f"Loaded {len(profiles)} profile(s) from {self.profiles_path}")
        return profiles

    def get_profile(self, profile_name: str = "default") -> Profile:
        """Get a profile by name.

        Args:
            profile_name: Name of the profile (default: 'default')

        Returns:
            Profile instance

        Raises:
            ValueError: If the profile doesn't exist
        """
        profiles = self.load_profiles()

        if profile_name not in profiles:
            available = ", ".join(profiles.keys())
            raise ValueError(
                f"Profile '{profile_name}' not found. Available profiles: {available}. "
                f"Set 'profile: <name>' in saga_project.yml, use --profile <name>, "
                f"or set SAGA_PROFILE=<name>."
            )

        return profiles[profile_name]

    def get_target(
        self, profile_name: str = "default", target_name: Optional[str] = None
    ) -> ProfileTarget:
        """Get a target from a profile.

        Args:
            profile_name: Name of the profile (default: 'default')
            target_name: Name of the target, or None for profile's default

        Returns:
            ProfileTarget instance
        """
        profile = self.get_profile(profile_name)
        target = profile.get_target(target_name)

        logger.info(
            f"Using profile '{profile_name}' with target '{target.name}' "
            f"(database: {target.database}, environment: {target.environment})"
        )

        return target

    def profiles_exist(self) -> bool:
        """Check if profiles.yml file exists.

        Returns:
            True if profiles.yml exists, False otherwise
        """
        return self.profiles_path is not None and self.profiles_path.exists()


def _find_profiles_file() -> Optional[Path]:
    """Find profiles.yml using a search path.

    Search order:
        1. SAGA_PROFILES_DIR env var (directory containing profiles.yml)
        2. ./profiles.yml (repo root)

    Returns:
        Path to profiles.yml, or None if not found.
    """
    env_dir = get_env("SAGA_PROFILES_DIR")
    if env_dir:
        candidate = Path(env_dir) / "profiles.yml"
        if candidate.exists():
            return candidate
        logger.debug(
            f"SAGA_PROFILES_DIR set to '{env_dir}' but profiles.yml not found there"
        )

    candidate = Path("profiles.yml")
    if candidate.exists():
        return candidate

    return None


# Global instance
_profiles_config: Optional[ProfilesConfig] = None


def get_profiles_config() -> ProfilesConfig:
    """Get or create the global ProfilesConfig instance.

    Returns:
        ProfilesConfig instance
    """
    global _profiles_config
    if _profiles_config is None:
        _profiles_config = ProfilesConfig()
    return _profiles_config
