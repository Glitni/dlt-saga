"""The context a warehouse job carries, independent of the destination.

Warehouses let a job be tagged with who issued it and why (BigQuery job
labels, Databricks query tags, Snowflake's ``QUERY_TAG``), so a saga job is
traceable in the warehouse's job history alongside the jobs other tools issue.
This module gathers that context in one neutral form:

* :func:`job_context`: what saga knows about the job being issued right now,
  read from the active log context — ``command`` (``ingest``/``historize``
  for a pipeline's own jobs, else the CLI subcommand), ``pipeline``,
  ``environment``, ``execution_id`` and ``task_index`` on orchestrated runs.
* :func:`user_job_labels`: what the project declared — the active profile
  target's ``job_labels`` with those forwarded in ``SAGA_JOB_LABELS`` merged
  over them.

Values here are raw. Each destination applies its own rules (key syntax,
value cleaning, limits) and its own mechanism for attaching them to a job;
BigQuery's lives in :mod:`dlt_saga.utility.gcp.job_labels`. A destination
that supports ``job_labels`` declares the profile field on its own config
class and validates it there.
"""

from __future__ import annotations

import functools
import json
import logging
import os
from typing import Any, Dict, Mapping, Optional

logger = logging.getLogger(__name__)

APP_NAME = "dlt-saga"

# Labels forwarded to a process by whoever started it (the orchestrator sets
# it on worker containers): a JSON object, merged over the profile's.
ENV_JOB_LABELS = "SAGA_JOB_LABELS"


def job_context() -> Dict[str, str]:
    """The context of a job issued right now, on this thread.

    Always carries ``app`` and ``environment``; the rest only when known.
    """
    from dlt_saga.utility.cli.logging import current_log_context
    from dlt_saga.utility.naming import get_environment

    log_fields = current_log_context()
    context = {
        "app": APP_NAME,
        "environment": get_environment(),
        "command": log_fields.get("command"),
        "pipeline": log_fields.get("pipeline"),
        "execution_id": log_fields.get("execution_id"),
        "task_index": log_fields.get("task_index"),
    }
    return {key: value for key, value in context.items() if value}


def user_job_labels() -> Dict[str, str]:
    """The project-declared labels, as declared: profile target first, then
    ``SAGA_JOB_LABELS`` over it.

    Only the shape is checked here (a mapping of scalar values; anything else
    is dropped with a warning), since this runs inside a job call. The
    destination config and ``saga validate`` are where a declaration the
    destination can't use is an error.
    """
    from dlt_saga.utility.cli.context import get_execution_context

    labels: Dict[str, str] = {}
    target = get_execution_context().profile_target
    if target is not None:
        declared = target.destination_config.get("job_labels")
        if declared:
            labels.update(
                scalar_labels(declared, f"profile target '{target.name}' job_labels")
            )
    forwarded = os.environ.get(ENV_JOB_LABELS)
    if forwarded:
        labels.update(_forwarded_labels(forwarded))
    return labels


def scalar_labels(labels: Any, where: str) -> Dict[str, str]:
    """``labels`` as ``str -> str``, dropping what isn't a scalar entry.

    Warns once per distinct declaration, so a job call never raises over
    config shape and the log doesn't repeat per job.
    """
    if not isinstance(labels, Mapping):
        _warn_once(f"{where}: ignoring job_labels, expected a mapping")
        return {}
    return _scalar_labels(
        tuple((str(key), _scalar_or_none(value)) for key, value in labels.items()),
        where,
    )


def _scalar_or_none(value: Any) -> Optional[str]:
    if value is None or isinstance(value, (Mapping, list, tuple, set)):
        return None
    return str(value)


@functools.lru_cache(maxsize=16)
def _scalar_labels(frozen: tuple, where: str) -> Dict[str, str]:
    labels: Dict[str, str] = {}
    for key, value in frozen:
        if value is None:
            logger.warning(
                "%s: skipping job label %r, its value is not a scalar", where, key
            )
            continue
        labels[key] = value
    return labels


@functools.lru_cache(maxsize=16)
def _warn_once(message: str) -> None:
    logger.warning(message)


@functools.lru_cache(maxsize=4)
def _forwarded_labels(raw: str) -> Dict[str, str]:
    """Parse ``SAGA_JOB_LABELS``, a JSON object, warning once when unusable."""
    try:
        parsed = json.loads(raw)
    except ValueError as exc:
        logger.warning("Ignoring %s: not valid JSON (%s)", ENV_JOB_LABELS, exc)
        return {}
    return scalar_labels(parsed, ENV_JOB_LABELS)
