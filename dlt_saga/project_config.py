"""Cached loader for saga_project.yml.

Provides access to project-level configuration including config_source settings,
providers configuration, and pipeline settings.
"""

import logging
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional

import yaml

logger = logging.getLogger(__name__)


@dataclass
class ConfigSourceConfig:
    """The config_source section of saga_project.yml."""

    type: str = field(
        default="file",
        metadata={
            "description": "Config source type. Only 'file' is supported.",
            "enum": ["file"],
        },
    )
    paths: List[str] = field(
        default_factory=lambda: ["configs"],
        metadata={
            "description": (
                "One or more config directories (relative to project root). "
                "Use 'paths: [...]' for multiple directories, or 'path: ...' for one. "
                "Duplicate pipeline names across directories raise an error."
            ),
        },
    )

    @property
    def path(self) -> str:
        """Primary config path. Backward-compatible single-path accessor."""
        return self.paths[0] if self.paths else "configs"

    @classmethod
    def from_dict(cls, data: dict) -> "ConfigSourceConfig":
        """Create from parsed YAML dict.

        Accepts either ``paths: [...]`` (list) or ``path: ...`` (single string).
        If both are present, ``paths`` takes precedence.
        """
        raw_paths = data.get("paths")
        raw_path = data.get("path")

        if raw_paths is not None:
            paths = [raw_paths] if isinstance(raw_paths, str) else list(raw_paths)
        elif raw_path is not None:
            paths = [raw_path]
        else:
            paths = ["configs"]

        return cls(
            type=data.get("type", "file"),
            paths=paths,
        )


@dataclass
class GoogleSecretsConfig:
    """The providers.google_secrets section of saga_project.yml."""

    project_id: Optional[str] = field(
        default=None,
        metadata={"description": "GCP project ID containing secrets"},
    )
    sheets_secret_name: Optional[str] = field(
        default=None,
        metadata={"description": "Secret name for Google Sheets credentials"},
    )

    @classmethod
    def from_dict(cls, data: dict) -> "GoogleSecretsConfig":
        """Create from parsed YAML dict."""
        return cls(
            project_id=data.get("project_id"),
            sheets_secret_name=data.get("sheets_secret_name"),
        )


@dataclass
class ProvidersConfig:
    """The providers section of saga_project.yml."""

    google_secrets: Optional[GoogleSecretsConfig] = field(
        default=None,
        metadata={"description": "Google Secret Manager provider settings"},
    )

    @classmethod
    def from_dict(cls, data: dict) -> "ProvidersConfig":
        """Create from parsed YAML dict."""
        gs_data = data.get("google_secrets")
        return cls(
            google_secrets=GoogleSecretsConfig.from_dict(gs_data) if gs_data else None,
        )


@dataclass
class OrchestrationConfig:
    """The orchestration section of saga_project.yml.

    Controls how ``--orchestrate`` triggers distributed execution.

    Supported providers:
    - ``cloud_run``: Trigger Cloud Run Jobs (default in prod when not configured).
    - ``stdout``: Output JSON to stdout for external orchestrators
      (Airflow, Cloud Workflows, etc.).
    """

    provider: Optional[str] = field(
        default=None,
        metadata={
            "description": (
                "Orchestration provider name. "
                "When set, orchestration works in any environment."
            ),
            "enum": ["cloud_run", "stdout"],
        },
    )
    region: Optional[str] = field(
        default=None,
        metadata={"description": "Cloud Run region (cloud_run provider)"},
    )
    job_name: Optional[str] = field(
        default=None,
        metadata={"description": "Cloud Run Job name (cloud_run provider)"},
    )
    schema: Optional[str] = field(
        default=None,
        metadata={
            "description": (
                "Schema/dataset for execution plan tables. "
                "Defaults to 'dlt_orchestration' in prod, "
                "developer schema in dev."
            ),
        },
    )

    @classmethod
    def from_dict(cls, data: dict) -> "OrchestrationConfig":
        """Create from parsed YAML dict."""
        return cls(
            provider=data.get("provider"),
            region=data.get("region"),
            job_name=data.get("job_name"),
            schema=data.get("schema"),
        )


@dataclass
class LogTablesConfig:
    """The log_tables section of saga_project.yml.

    Controls the names of internal tracking tables created by the framework.
    All fields default to the framework's original names and are effectively
    write-once: changing them after first run orphans existing tables and breaks
    incremental detection and historization state (no migration is performed).
    """

    load_info: str = field(
        default="_saga_load_info",
        metadata={"description": "Name of the load-info tracking table."},
    )
    historize_log: str = field(
        default="_saga_historize_log",
        metadata={"description": "Name of the historize-log tracking table."},
    )
    execution_plans: str = field(
        default="_saga_execution_plans",
        metadata={"description": "Name of the execution plans table."},
    )
    executions: str = field(
        default="_saga_executions",
        metadata={"description": "Name of the executions metadata table."},
    )

    @property
    def execution_plans_current(self) -> str:
        """View name derived from the execution_plans table name."""
        return f"{self.execution_plans}_current"

    @classmethod
    def from_dict(cls, data: dict) -> "LogTablesConfig":
        """Create from parsed YAML dict."""
        return cls(
            load_info=data.get("load_info", "_saga_load_info"),
            historize_log=data.get("historize_log", "_saga_historize_log"),
            execution_plans=data.get("execution_plans", "_saga_execution_plans"),
            executions=data.get("executions", "_saga_executions"),
        )


@dataclass
class SagaProjectConfig:
    """Top-level structure of saga_project.yml."""

    config_source: Optional[ConfigSourceConfig] = field(
        default=None,
        metadata={"description": "Where pipeline configs are discovered from"},
    )
    providers: Optional[ProvidersConfig] = field(
        default=None,
        metadata={"description": "Provider credentials and secrets configuration"},
    )
    orchestration: Optional[OrchestrationConfig] = field(
        default=None,
        metadata={
            "description": (
                "Orchestration settings for distributed execution. "
                "When provider is set, --orchestrate works in any environment."
            ),
        },
    )
    naming_module: Optional[str] = field(
        default=None,
        metadata={
            "description": "Custom naming module for schema/table name generation"
        },
    )
    pipelines: Optional[Dict[str, Any]] = field(
        default=None,
        metadata={
            "description": (
                "Pipeline-level settings, organized by pipeline group. "
                "Supports dbt-style hierarchical config with +key for merge."
            ),
        },
    )
    hooks: Optional[Dict[str, Any]] = field(
        default=None,
        metadata={
            "description": (
                "Lifecycle hook callables, keyed by event name. "
                "Each value is a list of 'module:callable' strings."
            ),
        },
    )
    profile: Optional[str] = field(
        default=None,
        metadata={
            "description": (
                "Default profile name to use from profiles.yml. "
                "Overridden by the SAGA_PROFILE env var or --profile CLI flag. "
                "Falls back to 'default' if not set."
            ),
        },
    )
    log_tables: LogTablesConfig = field(
        default_factory=lambda: LogTablesConfig(),
        metadata={
            "description": (
                "Names of internal tracking tables. "
                "All fields are effectively write-once: changing them after first run "
                "orphans existing tables and breaks incremental detection and "
                "historization state — no migration is performed automatically."
            ),
        },
    )

    @classmethod
    def from_dict(cls, data: dict) -> "SagaProjectConfig":
        """Create from parsed YAML dict."""
        cs_data = data.get("config_source")
        prov_data = data.get("providers")
        orch_data = data.get("orchestration")
        return cls(
            config_source=(ConfigSourceConfig.from_dict(cs_data) if cs_data else None),
            providers=ProvidersConfig.from_dict(prov_data) if prov_data else None,
            orchestration=(
                OrchestrationConfig.from_dict(orch_data) if orch_data else None
            ),
            naming_module=data.get("naming_module"),
            pipelines=data.get("pipelines"),
            hooks=data.get("hooks"),
            profile=data.get("profile"),
            log_tables=LogTablesConfig.from_dict(data.get("log_tables") or {}),
        )


_project_config: Optional[SagaProjectConfig] = None


def get_project_config() -> SagaProjectConfig:
    """Load and cache saga_project.yml.

    Returns:
        SagaProjectConfig instance (empty defaults if file not found).
    """
    global _project_config
    if _project_config is not None:
        return _project_config

    project_path = Path("saga_project.yml")
    if not project_path.exists():
        _project_config = SagaProjectConfig()
        return _project_config

    try:
        with open(project_path) as f:
            data = yaml.safe_load(f) or {}
    except Exception as e:
        logger.warning(f"Failed to read saga_project.yml: {e}")
        _project_config = SagaProjectConfig()
        return _project_config

    _project_config = SagaProjectConfig.from_dict(data)
    return _project_config


def get_providers_config() -> ProvidersConfig:
    """Get the providers: section from saga_project.yml.

    Returns:
        ProvidersConfig instance.
    """
    config = get_project_config()
    return config.providers or ProvidersConfig()


def get_orchestration_config() -> OrchestrationConfig:
    """Get the orchestration: section from saga_project.yml.

    Returns:
        OrchestrationConfig instance (empty defaults if section absent).
    """
    config = get_project_config()
    return config.orchestration or OrchestrationConfig()


def get_config_source_settings() -> ConfigSourceConfig:
    """Get the config_source: section from saga_project.yml.

    Returns:
        ConfigSourceConfig instance.
    """
    config = get_project_config()
    return config.config_source or ConfigSourceConfig()


def get_load_info_table_name() -> str:
    """Return the configured name for the load-info tracking table.

    Configured via ``log_tables.load_info`` in saga_project.yml.
    Default: ``_saga_load_info``.
    """
    return get_project_config().log_tables.load_info


def get_historize_log_table_name() -> str:
    """Return the configured name for the historize-log tracking table.

    Configured via ``log_tables.historize_log`` in saga_project.yml.
    Default: ``_saga_historize_log``.
    """
    return get_project_config().log_tables.historize_log


def get_execution_plans_table_name() -> str:
    """Return the configured name for the execution plans table.

    Configured via ``log_tables.execution_plans`` in saga_project.yml.
    Default: ``_saga_execution_plans``.
    """
    return get_project_config().log_tables.execution_plans


def get_executions_table_name() -> str:
    """Return the configured name for the executions metadata table.

    Configured via ``log_tables.executions`` in saga_project.yml.
    Default: ``_saga_executions``.
    """
    return get_project_config().log_tables.executions


def get_execution_plans_view_name() -> str:
    """Return the derived name for the execution plans current-status view.

    Derived as ``{execution_plans}_current``.
    Default: ``_saga_execution_plans_current``.
    """
    return get_project_config().log_tables.execution_plans_current


def _reset_cache() -> None:
    """Reset the cached config. For testing only."""
    global _project_config
    _project_config = None
