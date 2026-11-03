import json
import logging
import os
import sys
import warnings
from contextlib import contextmanager
from contextvars import ContextVar
from datetime import datetime, timezone
from enum import Enum
from pathlib import Path
from typing import Any, Dict, Iterator, Mapping, Optional

logger = logging.getLogger(__name__)


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


def _is_worker_mode() -> bool:
    """Detect if running as a distributed worker (orchestrator-spawned)."""
    return (os.getenv("SAGA_WORKER_MODE") or "").lower() == "true"


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
        if not color:
            return super().format(record)
        # The record is shared by every handler: restore levelname so the file
        # handler (and any host handler) doesn't inherit the color codes
        levelname = record.levelname
        record.levelname = f"{color}{levelname}{RESET}"
        try:
            return super().format(record)
        finally:
            record.levelname = levelname


# ---------------------------------------------------------------------------
# Log context — fields stamped on every record emitted while a pipeline runs
# ---------------------------------------------------------------------------

_pipeline_context: ContextVar[Mapping[str, str]] = ContextVar(
    "saga_log_context", default={}
)
_process_context: Dict[str, str] = {}


def bind_log_context(**fields: Optional[Any]) -> None:
    """Attach process-wide fields (e.g. ``execution_id``) to every log record.

    A ``None`` value removes the field. Process-wide because the value holds
    for the whole invocation: a worker executes one plan, an orchestrator
    creates one.
    """
    for key, value in fields.items():
        if value is None:
            _process_context.pop(key, None)
        else:
            _process_context[key] = str(value)


@contextmanager
def log_context(**fields: Optional[Any]) -> Iterator[None]:
    """Stamp ``fields`` on every record emitted while the block runs.

    Bound around each pipeline's execution (``pipeline``, ``command``) in the
    thread that runs it, so interleaved output from a parallel run stays
    attributable to a pipeline without parsing the ``[1/N]`` prefix — and so
    records from dlt or ``requests`` inside the run carry the pipeline too.
    A context variable is per thread, and the reset keeps a pool thread from
    carrying one pipeline's fields into the next task it picks up.
    """
    merged = dict(_pipeline_context.get())
    merged.update({k: str(v) for k, v in fields.items() if v is not None})
    token = _pipeline_context.set(merged)
    try:
        yield
    finally:
        _pipeline_context.reset(token)


def current_log_context() -> Dict[str, str]:
    """The fields the active record context carries (process-wide + thread)."""
    return {**_process_context, **_pipeline_context.get()}


class _LogContextFilter(logging.Filter):
    """Copy the active log context onto the record as ``saga_context``.

    Handler-level, like the redaction filter, so it sees every logger's
    records. The record is shared by every handler; the first stamp wins,
    which is the same context either way.
    """

    def filter(self, record: logging.LogRecord) -> bool:
        if not hasattr(record, "saga_context"):
            record.saga_context = current_log_context()
        return True


# ---------------------------------------------------------------------------
# Console format — text for a terminal, JSON lines for a log collector
# ---------------------------------------------------------------------------


class LogFormat(str, Enum):
    """Console log format: ``text`` (default) or ``json``, one object per line."""

    text = "text"
    json = "json"


_CONSOLE_LOG_FORMAT = "%(asctime)s - %(levelname)s - %(message)s"
_console_format: str = LogFormat.text.value


class _JsonFormatter(logging.Formatter):
    """One JSON object per record, for log collectors.

    ``time``, ``severity`` and ``message`` are the field names collectors parse
    without configuration (Cloud Logging's structured-log parser reads them
    natively; they map one-to-one onto the OpenTelemetry log data model). The
    log context (``pipeline``, ``command``, ``execution_id``) and ``logger``
    land as plain payload fields, so a collector can filter on them.
    """

    def format(self, record: logging.LogRecord) -> str:
        created = datetime.fromtimestamp(record.created, tz=timezone.utc)
        payload: Dict[str, Any] = {
            "time": created.isoformat(timespec="milliseconds").replace("+00:00", "Z"),
            "severity": record.levelname,
            "message": record.getMessage(),
            "logger": record.name,
        }
        context = getattr(record, "saga_context", None)
        payload.update(context if context is not None else current_log_context())
        if record.exc_info and not record.exc_text:
            # Cache like logging.Formatter does, so the file handler's text
            # formatter and the redaction filter see the same rendering.
            record.exc_text = self.formatException(record.exc_info)
        if record.exc_text:
            payload["exception"] = record.exc_text
        if record.stack_info:
            payload["stack"] = self.formatStack(record.stack_info)
        return json.dumps(payload, ensure_ascii=False, default=str)


def _resolve_log_format() -> tuple[str, Optional[str]]:
    """Console format from ``SAGA_LOG_FORMAT``, and the rejected value if any.

    An unknown value falls back to text. The warning about it is the caller's
    to log once the console handler exists; nothing is attached yet here.
    """
    raw = (os.getenv("SAGA_LOG_FORMAT") or "").strip().lower()
    if not raw:
        return LogFormat.text.value, None
    try:
        return LogFormat(raw).value, None
    except ValueError:
        return LogFormat.text.value, raw


def _build_console_formatter(fmt: str) -> logging.Formatter:
    if fmt == LogFormat.json.value:
        return _JsonFormatter()
    if _is_cloud_run():
        return logging.Formatter(_CONSOLE_LOG_FORMAT)
    return _ColoredFormatter(_CONSOLE_LOG_FORMAT)


def set_console_format(fmt: str) -> None:
    """Switch the console handler's format (``--log-format``).

    The file handler keeps its text format: it is the human-readable
    transcript, and a collector never reads it.
    """
    global _console_format
    fmt = LogFormat(fmt).value
    if _console_handler is not None:
        _console_handler.setFormatter(_build_console_formatter(fmt))
    _console_format = fmt


def get_console_format() -> str:
    """The active console format, ``text`` or ``json``."""
    return _console_format


class _DltLogBridge(logging.Handler):
    """Route dlt's own log records through saga's root handlers.

    dlt builds its ``dlt`` logger on first use with ``propagate = False`` and
    its own stderr handler in its own text format, and rebuilds it whenever
    its runtime re-initialises — reusing whichever handler is first on the
    logger and re-setting that handler's formatter. Installed before dlt's
    first initialisation, this is the handler it reuses, so dlt's records go
    through saga's console and file handlers (one format, the redaction and
    noise filters, the log file) rather than forming a second stream in
    another shape — which under the JSON format would be bare text lines in
    the middle of the JSON output.
    """

    def setFormatter(self, fmt: Optional[logging.Formatter]) -> None:
        # dlt sets its own formatter on the handler it reuses; the root
        # handlers format the record, so the one it hands us is irrelevant.
        pass

    def emit(self, record: logging.LogRecord) -> None:
        logging.getLogger().handle(record)


def _install_dlt_log_bridge() -> None:
    dlt_logger = logging.getLogger("dlt")
    for handler in list(dlt_logger.handlers):
        dlt_logger.removeHandler(handler)
    dlt_logger.addHandler(_DltLogBridge())
    # Until dlt initialises, the logger still propagates: the bridge would
    # forward the record and propagation would deliver it a second time.
    dlt_logger.propagate = False


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


# ---------------------------------------------------------------------------
# File logging (dbt-style: full DEBUG to logs/, narrow INFO to terminal)
# ---------------------------------------------------------------------------

_FILE_LOG_FORMAT = "%(asctime)s %(levelname)-8s %(name)s %(message)s"
_DEFAULT_RETENTION = 10

_log_file_path: Optional[Path] = None
_console_handler: Optional[logging.Handler] = None

# ``extra=FILE_ONLY`` marks a record for the log file only, for text that
# already reaches the terminal another way (e.g. the stderr failure summary).
FILE_ONLY = {"file_only": True}


class _FileOnlyFilter(logging.Filter):
    """Keep records marked with :data:`FILE_ONLY` off the console."""

    def filter(self, record):
        return not getattr(record, "file_only", False)


def get_log_file_path() -> Optional[Path]:
    """Return the active debug-log file path, or None if file logging is disabled."""
    return _log_file_path


def _file_logging_enabled() -> bool:
    """Decide whether to attach a file handler at startup.

    Disabled in any of these cases:
      - Cloud Run (ephemeral fs, stdout is already captured by the platform)
      - Worker mode (orchestrator-spawned, same reasoning)
      - Pytest is loaded (avoid creating log files on test imports)
      - SAGA_LOG_FILE env var is set to a falsy value
    """
    override = os.getenv("SAGA_LOG_FILE")
    if override is not None:
        return override.lower() not in ("0", "false", "no", "off", "")
    if _is_cloud_run() or _is_worker_mode():
        return False
    if "pytest" in sys.modules:
        return False
    return True


def _resolve_log_dir() -> Path:
    """Resolve the directory where log files are written."""
    custom = os.getenv("SAGA_LOG_DIR")
    if custom:
        return Path(custom).expanduser()
    return Path.cwd() / "logs"


def _resolve_retention() -> int:
    """Resolve how many recent log files to keep on disk."""
    raw = os.getenv("SAGA_LOG_RETENTION")
    if raw is None:
        return _DEFAULT_RETENTION
    try:
        value = int(raw)
    except ValueError:
        return _DEFAULT_RETENTION
    return max(value, 1)


def _cleanup_old_log_files(
    log_dir: Path, retention: int, current: Optional[Path] = None
) -> None:
    """Keep only the ``retention`` most recent saga-*.log files.

    ``current`` (this run's file) is always kept and counts toward
    ``retention``. It can't be found by mtime alone: filesystem timestamps can
    be coarse enough (a few ms on Linux) to tie it with files written just
    before, and a tie would leave its survival to directory order.
    """
    try:
        existing = sorted(
            (p for p in log_dir.glob("saga-*.log") if p != current),
            key=lambda p: p.stat().st_mtime,
            reverse=True,
        )
    except OSError:
        return
    if current is not None:
        retention -= 1

    for stale in existing[max(retention, 0) :]:
        try:
            stale.unlink()
        except OSError:
            # Best-effort cleanup — never fail startup over an unlinkable log.
            pass


def _create_file_handler(log_dir: Path) -> Optional[logging.FileHandler]:
    """Create a DEBUG-level file handler. Returns None on any I/O error."""
    try:
        log_dir.mkdir(parents=True, exist_ok=True)
        timestamp = datetime.now().strftime("%Y%m%d-%H%M%S")
        pid = os.getpid()
        path = log_dir / f"saga-{timestamp}-{pid}.log"
        handler = logging.FileHandler(path, encoding="utf-8")
        handler.setLevel(logging.DEBUG)
        handler.setFormatter(logging.Formatter(_FILE_LOG_FORMAT))
        global _log_file_path
        _log_file_path = path
        return handler
    except OSError:
        return None


def ensure_utf8_streams() -> None:
    """Force the CLI's stdout/stderr to UTF-8 so Unicode output can't crash it.

    Commands emit non-ASCII text (``saga doctor``'s ✓/✗/→ status glyphs, dlt's
    own progress output). An *interactive* Windows console is already UTF-8
    (Python 3.6+, PEP 528), so it prints fine. The trap is **redirected** output
    — ``saga doctor > out.txt``, a pipe, or a log-capturing orchestrator — where
    Python falls back to ``locale.getpreferredencoding()`` = the ANSI code page
    (e.g. ``cp1252`` on Western-European Windows). That code page has no ``✓``,
    so a bare ``typer.echo("✓")`` raises ``UnicodeEncodeError`` and aborts the
    command. Reconfiguring to UTF-8 matches what the interactive console already
    does; ``errors="replace"`` is a safety net for any target that genuinely
    can't take UTF-8.

    CLI-entry-point only (call from ``main_saga``); never from library code — it
    mutates process-wide streams. Best-effort: a stream without ``reconfigure``
    (some capture shims) is skipped, and a detached/mid-operation stream is left
    as-is rather than failing startup.
    """
    for stream in (sys.stdout, sys.stderr):
        reconfigure = getattr(stream, "reconfigure", None)
        if reconfigure is None:
            continue
        try:
            reconfigure(encoding="utf-8", errors="replace")
        except (ValueError, OSError):
            pass


def configure_cli_logging() -> None:
    """Configure handlers, formatters, and filters for CLI use.

    This must only be called from the CLI entry point (cli.py).
    Library code must never call this — it should use
    ``logging.getLogger(__name__)`` and nothing else.

    When enabled, a DEBUG-level file handler is attached so that re-running
    with ``--verbose`` is not required to diagnose a failure after the fact.
    """
    global _console_handler, _console_format

    from dlt_saga.utility.secrets.redaction import SecretRedactingFilter

    noise_filters = [_OAuth2ClientFilter(), _DatabricksSqlFilter()]
    # Defense-in-depth: mask any provider-resolved secret that reaches a log
    # record (e.g. a secret folded into a request URL, or a re-raised request
    # exception). Attached per handler so it covers every logger, saga's and
    # third-party alike — a logger-level filter would miss propagated records.
    redaction_filter = SecretRedactingFilter()
    # Stamps the pipeline/command/execution context on each record; the JSON
    # formatter emits it, the text formatters ignore it.
    context_filter = _LogContextFilter()

    root = logging.getLogger()
    # Clear any handlers that a prior call (e.g. in tests) installed so this
    # function is idempotent.
    for h in list(root.handlers):
        root.removeHandler(h)

    _console_format, rejected_format = _resolve_log_format()
    console = logging.StreamHandler()
    console.setFormatter(_build_console_formatter(_console_format))

    console.setLevel(logging.INFO)
    for f in noise_filters:
        console.addFilter(f)
    console.addFilter(redaction_filter)
    console.addFilter(context_filter)
    console.addFilter(_FileOnlyFilter())

    root.addHandler(console)
    _console_handler = console
    _install_dlt_log_bridge()
    if rejected_format is not None:
        logger.warning(
            "Unknown SAGA_LOG_FORMAT %r, expected one of %s; using text",
            rejected_format,
            ", ".join(f.value for f in LogFormat),
        )

    file_handler: Optional[logging.FileHandler] = None
    if _file_logging_enabled():
        log_dir = _resolve_log_dir()
        file_handler = _create_file_handler(log_dir)
        if file_handler is not None:
            for f in noise_filters:
                file_handler.addFilter(f)
            file_handler.addFilter(redaction_filter)
            file_handler.addFilter(context_filter)
            root.addHandler(file_handler)
            _cleanup_old_log_files(log_dir, _resolve_retention(), _log_file_path)

    # Root must allow DEBUG records through when a file handler is attached;
    # the console handler stays at INFO so terminal output remains narrow.
    root.setLevel(logging.DEBUG if file_handler is not None else logging.INFO)

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
        # Filesystem layers — feature/cache setup notices on every file source
        "gcsfs",
        "fsspec",
    ):
        logging.getLogger(name).setLevel(logging.WARNING)


def reenable_saga_loggers() -> None:
    """Clear ``.disabled`` on every ``dlt_saga.*`` logger.

    Python's ``logging.config.dictConfig(...)`` defaults to
    ``disable_existing_loggers=True``, which sets ``.disabled = True`` on
    every logger that existed before the call. When something in saga's
    import graph triggers such a config — Airflow's logging init is the
    most common case, but the same can come from any host application —
    saga's own loggers are silenced mid-run. Both console and file output
    vanish, including the operator-facing failure summary.

    Re-enabling our own namespace restores intended behavior. Records
    still propagate to whatever root handlers the foreign config installed
    (Airflow's task-log handlers, etc.), so this does not fight the host —
    it just stops the host from accidentally silencing us.

    Idempotent. Safe to call from any thread; flipping a boolean on a
    ``Logger`` is not a critical section.
    """
    manager = logging.Logger.manager
    for name, lg in list(manager.loggerDict.items()):
        if (name == "dlt_saga" or name.startswith("dlt_saga.")) and isinstance(
            lg, logging.Logger
        ):
            lg.disabled = False


def set_console_verbose(verbose: bool) -> None:
    """Toggle the console handler between INFO and DEBUG.

    The file handler (if attached) always remains at DEBUG, so re-running with
    ``--verbose`` is only needed when the user wants to see DEBUG on stdout.
    """
    if _console_handler is None:
        # No prior configure_cli_logging — fall back to root level.
        logging.getLogger().setLevel(logging.DEBUG if verbose else logging.INFO)
        return
    _console_handler.setLevel(logging.DEBUG if verbose else logging.INFO)
    # Root must permit DEBUG so the console can see it.
    if verbose:
        logging.getLogger().setLevel(logging.DEBUG)


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
