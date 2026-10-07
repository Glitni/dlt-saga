"""Cached loader for saga_project.yml.

Provides access to project-level configuration including config_source settings,
providers configuration, and pipeline settings.
"""

import logging
import re
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

from dlt_saga.utility.project_root import find_project_root
from dlt_saga.utility.yaml_io import load_yaml

logger = logging.getLogger(__name__)


_VALID_SUFFIX_RE = re.compile(r"^_?[a-zA-Z][a-zA-Z0-9_]*$")


@dataclass
class HistorizeProjectConfig:
    """The ``historize:`` section of saga_project.yml.

    Controls where historized tables live relative to their source tables.

    ``placement`` is effectively write-once: changing it after the first run
    orphans existing historize tables in the old location — no migration is
    performed automatically.
    """

    placement: str = field(
        default="table_suffix",
        metadata={
            "description": (
                "Table placement strategy. "
                "'table_suffix' (default): historize table lives in the source schema "
                "with a suffix appended to the table name. "
                "'schema_suffix': historize table lives in a parallel schema whose name "
                "is the source schema name plus schema_suffix, and the table name is unchanged."
            ),
            "enum": ["table_suffix", "schema_suffix"],
        },
    )
    table_suffix: str = field(
        default="_historized",
        metadata={
            "description": (
                "Suffix appended to the source table name when placement=table_suffix. "
                "Ignored when placement=schema_suffix."
            ),
        },
    )
    schema_suffix: str = field(
        default="_historized",
        metadata={
            "description": (
                "Suffix appended to the source schema name when placement=schema_suffix. "
                "Ignored when placement=table_suffix."
            ),
        },
    )

    def __post_init__(self) -> None:
        if self.placement not in ("table_suffix", "schema_suffix"):
            raise ValueError(
                f"historize.placement must be 'table_suffix' or 'schema_suffix', "
                f"got '{self.placement}'"
            )
        for field_name, val in [
            ("table_suffix", self.table_suffix),
            ("schema_suffix", self.schema_suffix),
        ]:
            if val and not _VALID_SUFFIX_RE.match(val.lstrip("_")):
                raise ValueError(
                    f"historize.{field_name} must start with a letter or underscore and "
                    f"contain only alphanumeric characters and underscores, got '{val}'"
                )

    @classmethod
    def from_dict(cls, data: dict) -> "HistorizeProjectConfig":
        """Create from parsed YAML dict."""
        return cls(
            placement=data.get("placement", "table_suffix"),
            table_suffix=data.get("table_suffix", "_historized"),
            schema_suffix=data.get("schema_suffix", "_historized"),
        )


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
    schema_access: Optional[List[str]] = field(
        default=None,
        metadata={
            "description": (
                "Access entries applied to the orchestration schema by "
                "``saga update-access``. Same string format as "
                "``pipelines.schema_access`` (e.g. "
                "``READER:serviceAccount:<email>``). Use this to grant "
                "external orchestrators (Airflow, Dagster, Prefect) read "
                "access on the execution plan tables so they can wait on "
                "and inspect runs they triggered. Applied in prod only — "
                "matches the per-pipeline access behaviour. The legacy "
                "``dataset_access`` key is accepted as a read-time alias."
            ),
        },
    )
    worker_concurrency: Optional[int] = field(
        default=None,
        metadata={
            "description": (
                "Maximum number of pipelines a worker runs in parallel "
                "within a single task. Caps the thread pool used when a "
                "``task_group`` assigns multiple pipelines to one worker. "
                "Tune to fit the worker container's memory budget. "
                "Precedence: CLI ``--workers`` > ``SAGA_WORKER_CONCURRENCY`` "
                "env var > this field > default 4."
            ),
            "minimum": 1,
        },
    )

    def __post_init__(self) -> None:
        if self.worker_concurrency is not None and self.worker_concurrency < 1:
            raise ValueError(
                f"orchestration.worker_concurrency must be >= 1, "
                f"got {self.worker_concurrency}"
            )

    @classmethod
    def from_dict(cls, data: dict) -> "OrchestrationConfig":
        """Create from parsed YAML dict."""
        return cls(
            provider=data.get("provider"),
            region=data.get("region"),
            job_name=data.get("job_name"),
            schema=data.get("schema"),
            schema_access=data.get("schema_access") or data.get("dataset_access"),
            worker_concurrency=data.get("worker_concurrency"),
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
    native_load_log: str = field(
        default="_saga_native_load_log",
        metadata={"description": "Name of the native-load state-log table."},
    )
    notify_log: str = field(
        default="_saga_notify_log",
        metadata={"description": "Name of the `saga notify` log table."},
    )

    @property
    def execution_plans_current(self) -> str:
        """View name derived from the execution_plans table name."""
        return f"{self.execution_plans}_current"

    @property
    def native_load_log_latest(self) -> str:
        """View name derived from the native_load_log table name."""
        return f"{self.native_load_log}_latest"

    @classmethod
    def from_dict(cls, data: dict) -> "LogTablesConfig":
        """Create from parsed YAML dict."""
        return cls(
            load_info=data.get("load_info", "_saga_load_info"),
            historize_log=data.get("historize_log", "_saga_historize_log"),
            execution_plans=data.get("execution_plans", "_saga_execution_plans"),
            executions=data.get("executions", "_saga_executions"),
            native_load_log=data.get("native_load_log", "_saga_native_load_log"),
            notify_log=data.get("notify_log", "_saga_notify_log"),
        )


def normalize_mentions(value: Any, context: str) -> Optional[List[str]]:
    """Normalise a ``mentions`` value to a list of Slack tokens.

    Shared by the project-level ``notifications.slack.mentions`` and the
    per-pipeline key of the same name, so both accept a bare string and reject
    the same shapes with the same message.

    Args:
        value: Raw value from YAML — a string, a list of strings, or None.
        context: Dotted config path, used in the error message.

    Returns:
        A list of tokens, or ``None`` when unset.

    Raises:
        ValueError: If *value* is neither a string nor a list of strings.
    """
    if value is None:
        return None
    if isinstance(value, str):
        return [value]
    if not isinstance(value, list) or not all(isinstance(m, str) for m in value):
        raise ValueError(
            f"{context} must be a string or list of strings, got {value!r}"
        )
    return list(value)


@dataclass
class SlackNotificationConfig:
    """Slack notification settings.

    Posting is opt-in: with neither ``token`` nor ``webhook_url`` set, no
    notifier is registered and nothing is sent.

    Two transports, mirroring Elementary's: a **bot token** posts through
    ``chat.postMessage`` and needs a ``channel``; an **incoming webhook** posts
    to a URL whose channel is fixed when the webhook is created. As in
    Elementary, ``token`` takes precedence when both are given.
    """

    token: Optional[str] = field(
        default=None,
        metadata={
            "description": (
                "Slack bot token (``xoxb-…``) posting via chat.postMessage. "
                "Requires 'channel'. Use a secret URI "
                "(googlesecretmanager::…, azurekeyvault::…, env_secret::…) "
                "rather than {{ env_var() }}, which renders at config-load "
                "time and would bake the token into the execution plan. The "
                "app needs chat:write, and chat:write.public (or membership) "
                "for the target channel. Takes precedence over webhook_url."
            )
        },
    )
    channel: Optional[str] = field(
        default=None,
        metadata={
            "description": (
                "Channel to post to when using 'token' — '#data-alerts' or a "
                "channel ID. Ignored with webhook_url, where the channel is "
                "fixed by the webhook itself."
            )
        },
    )

    webhook_url: Optional[str] = field(
        default=None,
        metadata={
            "description": (
                "Slack incoming-webhook URL. Use a secret URI "
                "(googlesecretmanager::…, azurekeyvault::…, env_secret::…) "
                "rather than {{ env_var() }}: env_var renders at config-load "
                "time and would bake the webhook into the execution plan. The "
                "webhook fixes the channel; routing to several channels needs "
                "several webhooks."
            )
        },
    )
    username: Optional[str] = field(
        default="dlt-saga",
        metadata={
            "description": (
                "Display name on posted messages. Defaults to 'dlt-saga' so a "
                "digest is attributable even when posted through an app "
                "installed for something else. Requires the chat:write.customize "
                "scope with 'token'; if the app lacks it, saga retries without "
                "the override rather than failing to post. Set to null to "
                "always use the app's own name."
            )
        },
    )
    icon_emoji: Optional[str] = field(
        default=":satellite_antenna:",
        metadata={
            "description": (
                "Emoji shown in place of the app's avatar, e.g. ':card_index_dividers:'. "
                "Same scope requirement and fallback as 'username'."
            )
        },
    )

    report_url: Optional[str] = field(
        default=None,
        metadata={
            "description": (
                "Public URL of the published `saga report`, linked from every "
                "digest. saga cannot derive it: it only ever sees the 'gs://' "
                "URI passed to `saga report --output`, which is not browsable, "
                "and whether that bucket is served from storage.googleapis.com, "
                "a load balancer or a custom domain is a property of the "
                "deployment."
            )
        },
    )

    notify_on: str = field(
        default="failure",
        metadata={
            "description": (
                "When to post the run digest. 'failure' (default) posts only "
                "when at least one pipeline failed; 'always' posts every run."
            ),
            "enum": ["failure", "always"],
        },
    )
    mentions: Optional[List[str]] = field(
        default=None,
        metadata={
            "description": (
                "Slack mention tokens appended to a failure digest, e.g. "
                "'<@U012ABCDEF>' for a user or '<!subteam^S012ABCDEF>' for a "
                "group. An incoming webhook cannot resolve display names, so "
                "raw IDs are required. A pipeline can name its own under the "
                "same key in its config, which is appended to that pipeline's "
                "failure line."
            )
        },
    )
    per_pipeline: bool = field(
        default=False,
        metadata={
            "description": (
                "Also post one message per failed pipeline in addition to the "
                "run digest. Off by default — a systemic failure across a large "
                "selection would otherwise flood the channel."
            )
        },
    )
    timeout_seconds: float = field(
        default=10.0,
        metadata={
            "description": "HTTP timeout for each Slack request, in seconds.",
            "minimum": 1,
        },
    )

    @classmethod
    def from_dict(cls, data: dict) -> "SlackNotificationConfig":
        """Create from the ``notifications.slack`` block."""
        notify_on = str(data.get("notify_on", "failure")).lower()
        if notify_on not in ("failure", "always"):
            raise ValueError(
                f"notifications.slack.notify_on must be 'failure' or 'always', "
                f"got {data.get('notify_on')!r}"
            )
        mentions = normalize_mentions(
            data.get("mentions"), "notifications.slack.mentions"
        )
        report_url = data.get("report_url")
        if report_url is not None and not isinstance(report_url, str):
            raise ValueError(
                f"notifications.slack.report_url must be a URL string, "
                f"got {report_url!r}"
            )
        token = data.get("token")
        channel = data.get("channel")
        if token and not channel:
            raise ValueError(
                "notifications.slack.channel is required with 'token': "
                "chat.postMessage has no channel of its own, unlike an "
                "incoming webhook."
            )
        return cls(
            token=token,
            channel=channel,
            report_url=report_url,
            username=data.get("username", "dlt-saga"),
            icon_emoji=data.get("icon_emoji", ":satellite_antenna:"),
            webhook_url=data.get("webhook_url"),
            notify_on=notify_on,
            mentions=mentions,
            per_pipeline=bool(data.get("per_pipeline", False)),
            timeout_seconds=float(data.get("timeout_seconds", 10.0)),
        )


@dataclass
class NotificationsConfig:
    """Outbound notification settings."""

    slack: Optional[SlackNotificationConfig] = field(
        default=None,
        metadata={"description": "Slack incoming-webhook notifications"},
    )

    @classmethod
    def from_dict(cls, data: dict) -> "NotificationsConfig":
        """Create from the ``notifications:`` block."""
        slack_data = data.get("slack")
        return cls(
            slack=(
                SlackNotificationConfig.from_dict(slack_data) if slack_data else None
            )
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
    notifications: Optional[NotificationsConfig] = field(
        default=None,
        metadata={
            "description": (
                "Outbound notification settings. Configuring a channel here "
                "registers the built-in notifier automatically — no 'hooks:' "
                "entry needed."
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
    historize: HistorizeProjectConfig = field(
        default_factory=HistorizeProjectConfig,
        metadata={
            "description": (
                "Historize layer placement and suffix configuration. "
                "'placement' is effectively write-once: changing it after first run "
                "orphans existing historize tables — no migration is performed."
            ),
        },
    )

    @classmethod
    def from_dict(cls, data: dict) -> "SagaProjectConfig":
        """Create from parsed YAML dict."""
        cs_data = data.get("config_source")
        prov_data = data.get("providers")
        orch_data = data.get("orchestration")
        hist_data = data.get("historize")
        notif_data = data.get("notifications")
        return cls(
            config_source=(ConfigSourceConfig.from_dict(cs_data) if cs_data else None),
            providers=ProvidersConfig.from_dict(prov_data) if prov_data else None,
            orchestration=(
                OrchestrationConfig.from_dict(orch_data) if orch_data else None
            ),
            naming_module=data.get("naming_module"),
            pipelines=data.get("pipelines"),
            hooks=data.get("hooks"),
            notifications=(
                NotificationsConfig.from_dict(notif_data) if notif_data else None
            ),
            profile=data.get("profile"),
            log_tables=LogTablesConfig.from_dict(data.get("log_tables") or {}),
            historize=(
                HistorizeProjectConfig.from_dict(hist_data)
                if hist_data
                else HistorizeProjectConfig()
            ),
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

    # Resolve saga_project.yml by walking up from the cwd (same marker search as
    # package loading), so running saga from a subdirectory still finds the
    # project defaults instead of silently loading empty ones.
    project_path = find_project_root() / "saga_project.yml"
    if not project_path.exists():
        _project_config = SagaProjectConfig()
        return _project_config

    try:
        data = load_yaml(project_path)
    except Exception as e:
        logger.warning(f"Failed to read saga_project.yml: {e}")
        _project_config = SagaProjectConfig()
        return _project_config

    # Render {{ env_var(...) }} templates (and Jinja filters) before parsing.
    from dlt_saga.utility.templating import render_templates

    data = render_templates(data)

    # Rewrite legacy config keys (e.g. dataset_access → schema_access) so
    # downstream consumers only see canonical names. The same normalisation
    # runs in FilePipelineConfig for per-pipeline configs; doing it here too
    # covers callers that read project config without going through the file
    # config source.
    from dlt_saga.pipeline_config.compat import normalize_config_aliases

    normalize_config_aliases(data)

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


def get_native_load_log_table_name() -> str:
    """Return the configured name for the native-load state-log table.

    Configured via ``log_tables.native_load_log`` in saga_project.yml.
    Default: ``_saga_native_load_log``.
    """
    return get_project_config().log_tables.native_load_log


def get_notify_log_table_name() -> str:
    """Return the configured name for the ``saga notify`` log table.

    Configured via ``log_tables.notify_log`` in saga_project.yml.
    Default: ``_saga_notify_log``.
    """
    return get_project_config().log_tables.notify_log


def get_native_load_log_view_name() -> str:
    """Return the derived name for the native-load state-log latest view.

    Derived as ``{native_load_log}_latest``.
    Default: ``_saga_native_load_log_latest``.
    """
    return get_project_config().log_tables.native_load_log_latest


def get_historize_project_config() -> HistorizeProjectConfig:
    """Get the historize: section from saga_project.yml.

    Returns:
        HistorizeProjectConfig instance (defaults if section absent).
    """
    return get_project_config().historize


def _reset_cache() -> None:
    """Reset the cached config. For testing only."""
    global _project_config
    _project_config = None
