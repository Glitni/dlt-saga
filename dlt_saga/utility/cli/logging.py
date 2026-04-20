import logging
import os
import warnings
from typing import Optional


class PrefixedLoggerAdapter(logging.LoggerAdapter):
    """Logger adapter that automatically prepends a prefix to all messages."""

    def __init__(self, logger, prefix: Optional[str] = None):
        super().__init__(logger, {})
        self.prefix = prefix

    def process(self, msg, kwargs):
        """Add prefix to message if it exists."""
        if self.prefix:
            return f"{self.prefix} {msg}", kwargs
        return msg, kwargs


# ---------------------------------------------------------------------------
# ANSI color codes
# ---------------------------------------------------------------------------
GREEN = "\033[32m"
RED = "\033[31m"
YELLOW = "\033[33m"
BLUE = "\033[34m"
CYAN = "\033[36m"
MAGENTA = "\033[35m"
RESET = "\033[0m"


def _is_cloud_run() -> bool:
    """Detect if running inside a Cloud Run service or job."""
    return os.getenv("K_SERVICE") is not None or os.getenv("CLOUD_RUN_JOB") is not None


# ---------------------------------------------------------------------------
# CLI logging configuration — call once from cli.py at startup
# ---------------------------------------------------------------------------


class _ColoredFormatter(logging.Formatter):
    """Formatter that colorizes WARNING and ERROR level names."""

    LEVEL_COLORS = {
        logging.WARNING: YELLOW,
        logging.ERROR: RED,
    }

    def format(self, record):
        color = self.LEVEL_COLORS.get(record.levelno)
        if color:
            record.levelname = f"{color}{record.levelname}{RESET}"
        return super().format(record)


class _OAuth2ClientFilter(logging.Filter):
    """Filter to remove specific oauth2client warnings."""

    def filter(self, record):
        return not record.getMessage().startswith(
            "file_cache is only supported with oauth2client<4.0.0"
        )


class _DatabricksSqlFilter(logging.Filter):
    """Filter out known noisy Databricks SQL connector deprecation notices."""

    _SUPPRESSED = ("[WARN] Parameter '_user_agent_entry' is deprecated",)

    def filter(self, record):
        msg = record.getMessage()
        return not any(msg.startswith(s) for s in self._SUPPRESSED)


def configure_cli_logging() -> None:
    """Configure handlers, formatters, and filters for CLI use.

    This must only be called from the CLI entry point (cli.py).
    Library code must never call this — it should use
    ``logging.getLogger(__name__)`` and nothing else.
    """
    log_format = "%(asctime)s - %(levelname)s - %(message)s"

    noise_filters = [_OAuth2ClientFilter(), _DatabricksSqlFilter()]

    if _is_cloud_run():
        logging.basicConfig(level=logging.INFO, format=log_format)
        for f in noise_filters:
            logging.getLogger().addFilter(f)
    else:
        handler = logging.StreamHandler()
        handler.setFormatter(_ColoredFormatter(log_format))
        # Filters must be on the handler, not the logger, so they apply to
        # records that propagate up from child loggers (callHandlers bypasses
        # parent logger filters).
        for f in noise_filters:
            handler.addFilter(f)
        logging.basicConfig(level=logging.INFO, handlers=[handler])

    warnings.filterwarnings(
        "ignore", message="file_cache is only supported with oauth2client<4.0.0"
    )

    # Disable noisy loggers
    for name in (
        "googleapiclient.discovery_cache",
        "google_auth_oauthlib.flow",
        "oauth2client.client",
        "grpc",
        "absl",
        "urllib3",
        "google.auth.transport",
        # Databricks SQL connector — logs every HTTP request/response at INFO
        "databricks.sdk",
        "databricks.sql",
        # Azure SDK — credential probe messages (MSI, environment, etc.)
        "azure.core",
        "azure.identity",
        "azure.storage",
        "msal",
    ):
        logging.getLogger(name).setLevel(logging.WARNING)


# ---------------------------------------------------------------------------
# Utility functions — safe to use from library and CLI code
# ---------------------------------------------------------------------------


def colorize(text: str, color: str) -> str:
    """Add ANSI color to text (Cloud Run-aware).

    Returns plain text when running in Cloud Run (structured logging),
    colorized text otherwise.
    """
    if _is_cloud_run():
        return text
    return f"{color}{text}{RESET}"
