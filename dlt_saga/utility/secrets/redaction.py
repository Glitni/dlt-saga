"""Process-wide secret redaction for log output and persisted metadata.

Secrets are protected by :class:`SecretStr` only while wrapped; the moment a
resolved secret is composed into a plain ``str`` — a request URL, an error
message, a generated table description — it becomes loggable, persistable text
again.

This module is the defense-in-depth backstop. Every value returned by
:meth:`SecretResolver.resolve` from a real secret provider is registered here,
and a single :class:`SecretRedactingFilter` (installed on the CLI log handlers)
masks any registered value that appears in a formatted log record — message,
positional args, and exception traceback alike. :func:`redact` exposes the same
masking for non-log surfaces (e.g. a generated table description that would
otherwise persist a secret into warehouse metadata, which no log filter sees).

Registration is opt-in by construction: only provider-resolved values land in
the set, never plain passthrough config values. Trivially short values are
skipped so redaction never garbles unrelated log output.
"""

from __future__ import annotations

import logging
import threading
from typing import Tuple

# The mask substituted for any registered secret value.
REDACTION_MASK = "***"

# Values shorter than this are never registered: masking a 1-3 character string
# process-wide would garble unrelated output, and real secrets (tokens, keys,
# passwords) comfortably exceed it. SecretStr remains the primary protection.
_MIN_REDACTION_LENGTH = 4

_redaction_values: set[str] = set()
_lock = threading.Lock()


def register_secret(value: object) -> None:
    """Register a resolved secret value for redaction from logs and metadata.

    Called by :meth:`SecretResolver.resolve` for every value fetched from a
    secret provider. Non-string or trivially short values are ignored.
    """
    if not isinstance(value, str) or len(value) < _MIN_REDACTION_LENGTH:
        return
    with _lock:
        _redaction_values.add(value)


def redaction_values() -> Tuple[str, ...]:
    """Return a snapshot of the registered secret values, longest first.

    Longest-first so a secret that is a substring of another is masked after
    its superstring, never leaving a partial secret behind.
    """
    with _lock:
        return tuple(sorted(_redaction_values, key=len, reverse=True))


def redact(text: str) -> str:
    """Replace every registered secret value in ``text`` with the mask.

    A no-op (returns ``text`` unchanged) when nothing is registered, the input
    is not a non-empty string, or no registered value occurs in the text.
    """
    if not isinstance(text, str) or not text:
        return text
    return _redact_with(text, redaction_values())


def _redact_with(text: str, values: Tuple[str, ...]) -> str:
    for secret in values:
        if secret in text:
            text = text.replace(secret, REDACTION_MASK)
    return text


class SecretRedactingFilter(logging.Filter):
    """Logging filter that masks registered secrets in every emitted record.

    Installed on the CLI log handlers (console + file) so it sees records from
    *all* loggers — saga's own, dlt's, ``requests``' — not just one namespace.
    A logger-level filter would miss records propagated up from child loggers;
    a handler-level filter runs on every record the handler emits.

    Redacts the rendered message, positional args, and any exception traceback
    or stack info. Idempotent: re-running on an already-masked record is a
    no-op, so it is safe to attach to several handlers that share a record.
    """

    def filter(self, record: logging.LogRecord) -> bool:
        values = redaction_values()
        if not values:
            return True

        message = record.getMessage()
        redacted = _redact_with(message, values)
        if redacted != message:
            record.msg = redacted
            record.args = ()

        # Exception tracebacks embed the re-raised message (e.g. a request URL
        # folded into a ValueError), which record.getMessage() never sees.
        if record.exc_info and not record.exc_text:
            record.exc_text = logging.Formatter().formatException(record.exc_info)
        if record.exc_text:
            record.exc_text = _redact_with(record.exc_text, values)
        if record.stack_info:
            record.stack_info = _redact_with(record.stack_info, values)

        return True


def _reset_for_testing() -> None:
    """Clear the redaction registry. For testing only."""
    with _lock:
        _redaction_values.clear()
