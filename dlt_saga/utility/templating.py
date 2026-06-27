"""Jinja2 templating for config files (profiles.yml, saga_project.yml, configs).

Renders dbt-style ``{{ env_var('VAR') }}`` references — plus Jinja filters and
nested calls, e.g. ``{{ env_var('GCP_DATASET') | replace('-', '_') }}`` — in
config values. Uses a sandboxed environment and is applied at config-load time,
before any hierarchical merge, so every layer resolves to concrete strings
independently.

The standard-library ``datetime``, ``timedelta`` and ``timezone`` are also in
scope, so values can be computed relative to run time without any bespoke
helpers — the expression stays self-evident at the call site, e.g. a rolling
seed for a dev override::

    {{ (datetime.now(timezone.utc) - timedelta(days=7)).strftime('%Y-%m-%d') }}

Public API:
    render_template_str(value) -> str
    render_templates(obj) -> obj   # recursive over dict/list/str
"""

import logging
import os
from datetime import datetime, timedelta, timezone
from typing import Any, Optional

from jinja2 import TemplateError
from jinja2.sandbox import SandboxedEnvironment

logger = logging.getLogger(__name__)

# Only strings containing one of these are rendered; everything else is a
# fast-path no-op (avoids touching template-free config values).
_TEMPLATE_MARKERS = ("{{", "{%")

_env: Optional[SandboxedEnvironment] = None


def _env_var(name: str, default: str = "") -> str:
    """``env_var('NAME')`` / ``env_var('NAME', 'default')`` for templates.

    Returns the environment variable, or *default* (empty string when omitted)
    when unset — matching the historical profiles.yml behavior.
    """
    return os.getenv(name, default)


def _get_env() -> SandboxedEnvironment:
    """Lazily build the shared sandboxed Jinja environment."""
    global _env
    if _env is None:
        env = SandboxedEnvironment(autoescape=False)
        env.globals["env_var"] = _env_var
        # Standard-library date/time, so configs can compute rolling values
        # relative to run time (e.g. a dev seed) with plain, visible Python
        # rather than bespoke helpers. The sandbox still blocks imports and
        # dunder/underscore attribute access, so exposing these classes adds no
        # escape surface.
        env.globals["datetime"] = datetime
        env.globals["timedelta"] = timedelta
        env.globals["timezone"] = timezone
        _env = env
    return _env


def render_template_str(value: str) -> str:
    """Render a single string through the sandboxed Jinja environment.

    Non-string input and strings without template markers are returned
    unchanged. Template errors are raised as ``ValueError`` (a configuration
    error) with the offending value for context.
    """
    if not isinstance(value, str) or not any(m in value for m in _TEMPLATE_MARKERS):
        return value
    try:
        return _get_env().from_string(value).render()
    except TemplateError as e:
        raise ValueError(
            f"Failed to render template in config value {value!r}: {e}"
        ) from e


def render_templates(obj: Any) -> Any:
    """Recursively render all string leaves in *obj*.

    Renders values only (dict keys are left untouched) so config merge markers
    like ``+key:`` survive. Lists and nested dicts are walked.
    """
    if isinstance(obj, str):
        return render_template_str(obj)
    if isinstance(obj, dict):
        return {key: render_templates(value) for key, value in obj.items()}
    if isinstance(obj, list):
        return [render_templates(item) for item in obj]
    return obj
