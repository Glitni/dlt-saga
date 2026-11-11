"""BigQuery job labels for every job a saga process issues.

The job context saga gathers destination-neutrally in
:mod:`dlt_saga.utility.job_labels` becomes BigQuery labels here, so saga's
jobs are traceable in ``INFORMATION_SCHEMA.JOBS`` with the same convention
other tools use (``app`` names the tool, the rest say what it was doing):

* ``app``: always ``dlt-saga``
* ``saga-command``, ``saga-pipeline``, ``saga-environment``,
  ``saga-execution-id``, ``saga-task``: the job context, prefixed
* the profile target's ``job_labels``, as declared

Values are cleaned the way BigQuery requires: lowercase, any character
outside ``[a-z0-9_-]`` replaced by ``-``, cut at 63 characters. Keys are
validated, never rewritten — a key BigQuery can't take is a config error.

Two kinds of client issue jobs. Saga's own pooled clients are shared by every
pipeline a thread runs in turn, so :func:`label_bigquery_jobs` wraps their
``query`` to resolve the labels per call from the active context. dlt's
client belongs to one pipeline run but is opened on dlt's own pool threads,
which carry no context, so :func:`attach_job_labels_to_dlt_destination` takes
the labels once, when the destination is built on the pipeline's thread, and
sets them as the native client's default job config — the library merges
those into every job dlt issues without labels of its own.
"""

from __future__ import annotations

import copy
import functools
import logging
import re
from typing import Any, Dict, Mapping

from dlt_saga.utility.job_labels import job_context, user_job_labels

logger = logging.getLogger(__name__)

# Keys saga sets itself; a profile may not redefine them.
RESERVED_LABEL_KEYS = frozenset({"app"})
RESERVED_LABEL_PREFIX = "saga-"

# BigQuery allows 64 labels per job; saga's built-ins take up to six.
MAX_LABELS_PER_JOB = 64
BUILT_IN_LABEL_COUNT = 6
MAX_USER_LABELS = MAX_LABELS_PER_JOB - BUILT_IN_LABEL_COUNT
LABEL_MAX_LENGTH = 63

_KEY_RE = re.compile(r"^[a-z][a-z0-9_-]{0,62}$")
_DISALLOWED_RE = re.compile(r"[^a-z0-9_-]")

# Job-context field -> label key. ``app`` is the convention shared with
# other tools; the rest are namespaced.
_CONTEXT_LABEL_KEYS = {
    "app": "app",
    "command": "saga-command",
    "pipeline": "saga-pipeline",
    "environment": "saga-environment",
    "execution_id": "saga-execution-id",
    "task_index": "saga-task",
}


def clean_label_value(value: Any) -> str:
    """Rewrite *value* into a valid BigQuery label value.

    Lowercased, every character outside lowercase letters, digits, ``_`` and
    ``-`` replaced by ``-``, cut at 63 characters.
    """
    return _DISALLOWED_RE.sub("-", str(value).lower())[:LABEL_MAX_LENGTH]


def validate_job_labels(labels: Any, where: str) -> Dict[str, str]:
    """Check a user-declared ``job_labels`` mapping and return it cleaned.

    Keys must be valid BigQuery label keys and not collide with saga's own;
    values are cleaned with :func:`clean_label_value`, and an entry whose value
    is empty is dropped — a label rendered from an unset variable is left out
    rather than sent blank.

    Args:
        labels: The raw ``job_labels`` value (``None`` for none).
        where: Where it was declared, for the error message.

    Raises:
        ValueError: On a non-mapping value, an invalid or reserved key, a
            non-scalar value, or more labels than a job can carry.
    """
    return validate_labels(
        labels,
        where,
        scope="job",
        limit=MAX_LABELS_PER_JOB,
        built_in=BUILT_IN_LABEL_COUNT,
    )


def validate_labels(
    labels: Any,
    where: str,
    *,
    scope: str,
    limit: int,
    built_in: int,
    reserved_keys: frozenset = RESERVED_LABEL_KEYS,
) -> Dict[str, str]:
    """Check a user-declared ``<scope>_labels`` mapping and return it cleaned.

    The rules BigQuery applies to labels on any resource, plus saga's reserved
    keys; see :func:`validate_job_labels`.

    Args:
        labels: The raw declared value (``None`` for none).
        where: Where it was declared, for the error message.
        scope: The resource the labels go on (``job``, ``dataset``).
        limit: How many labels BigQuery allows on that resource.
        built_in: How many of those saga's own labels take.
        reserved_keys: Keys a declaration may not set, beside the ``saga-``
            prefix.

    Raises:
        ValueError: On a non-mapping value, an invalid or reserved key, a
            non-scalar value, or more labels than the resource can carry.
    """
    if labels is None:
        return {}
    if not isinstance(labels, Mapping):
        raise ValueError(
            f"{where}: {scope}_labels must be a mapping of label key to value, "
            f"got {type(labels).__name__}"
        )
    cleaned: Dict[str, str] = {}
    for key, value in labels.items():
        if not isinstance(key, str) or not _KEY_RE.match(key):
            raise ValueError(
                f"{where}: {scope} label key {key!r} is invalid. Keys are 1-63 "
                "characters of lowercase letters, digits, '_' or '-', starting "
                "with a letter"
            )
        if key in reserved_keys or key.startswith(RESERVED_LABEL_PREFIX):
            names = [*sorted(reserved_keys), f"{RESERVED_LABEL_PREFIX}*"]
            raise ValueError(
                f"{where}: {scope} label key {key!r} is reserved for saga's "
                f"built-in labels ({' and '.join(repr(name) for name in names)})"
            )
        if value is None or isinstance(value, (Mapping, list, tuple, set)):
            raise ValueError(
                f"{where}: {scope} label {key!r} must have a scalar value, got "
                f"{type(value).__name__}"
            )
        text = clean_label_value(value)
        if text:
            cleaned[key] = text
    if len(cleaned) > limit - built_in:
        raise ValueError(
            f"{where}: {len(cleaned)} {scope} labels declared, at most "
            f"{limit - built_in} fit beside saga's own (BigQuery allows {limit} "
            f"per {scope})"
        )
    return cleaned


def current_job_labels() -> Dict[str, str]:
    """The labels for a job issued right now, on this thread.

    The declared labels go in first and the job context over them, so a
    declaration can never shadow saga's own keys (validation refuses them;
    this is the lenient path).
    """
    labels = dict(_lenient_labels(tuple(user_job_labels().items())))
    for field, value in job_context().items():
        labels[_CONTEXT_LABEL_KEYS[field]] = clean_label_value(value)
    return labels


def with_job_labels(job_config: Any, config_cls: Any, labels: Mapping[str, str]) -> Any:
    """Return *job_config* (or a new *config_cls*) carrying *labels*.

    Labels the caller set explicitly win over *labels*. The given config is
    never mutated: callers reuse job configs across calls.
    """
    if not labels:
        return job_config
    if job_config is None:
        return config_cls(labels=dict(labels))
    merged = copy.deepcopy(job_config)
    merged.labels = {**labels, **(job_config.labels or {})}
    return merged


def label_bigquery_jobs(client: Any) -> None:
    """Make every query job *client* issues carry :func:`current_job_labels`.

    For saga's pooled clients, which outlive the pipeline that happens to be
    using them: the labels are resolved per call, on the calling thread, so a
    pool thread moving on to its next pipeline labels that pipeline's jobs
    correctly. Saga's own code issues query jobs only.
    """
    from google.cloud import bigquery

    query_orig = client.query

    def query(query: str, job_config: Any = None, *args: Any, **kwargs: Any) -> Any:
        labelled = with_job_labels(
            job_config, bigquery.QueryJobConfig, current_job_labels()
        )
        return query_orig(query, labelled, *args, **kwargs)

    client.query = query


def apply_default_job_labels(client: Any, labels: Mapping[str, str]) -> None:
    """Set *labels* as *client*'s default query and load job labels.

    The BigQuery library merges a client's default job config into every job
    whose own config leaves those properties unset, which is how a client used
    by code that builds its own job configs (dlt) still labels every job.
    """
    from google.cloud import bigquery

    if not labels:
        return
    client.default_query_job_config = with_job_labels(
        client.default_query_job_config, bigquery.QueryJobConfig, labels
    )
    client.default_load_job_config = with_job_labels(
        client.default_load_job_config, bigquery.LoadJobConfig, labels
    )


def attach_job_labels_to_dlt_destination(
    dlt_destination: Any, labels: Mapping[str, str]
) -> None:
    """Label every job dlt issues through *dlt_destination* with *labels*.

    dlt opens its BigQuery client inside the job client it builds from the
    destination factory, on whichever thread loads the package, so the labels
    are fixed here — on the pipeline's own thread, where they are known — and
    applied as the native client's defaults each time one is opened.
    """
    if not labels:
        return
    client_orig = dlt_destination.client
    fixed = dict(labels)

    def client(schema: Any, initial_config: Any = None) -> Any:
        job_client = client_orig(schema, initial_config)
        sql_client = job_client.sql_client
        open_orig = sql_client.open_connection

        def open_connection() -> Any:
            native = open_orig()
            apply_default_job_labels(native, fixed)
            return native

        sql_client.open_connection = open_connection
        return job_client

    dlt_destination.client = client


@functools.lru_cache(maxsize=16)
def _lenient_labels(frozen: tuple) -> Dict[str, str]:
    """Clean a declared label set for a job call: what BigQuery can't take is
    dropped with a warning, once per distinct declaration, never raised."""
    labels: Dict[str, str] = {}
    for key, value in frozen:
        try:
            labels.update(validate_job_labels({key: value}, "job_labels"))
        except ValueError as exc:
            logger.warning("Skipping BigQuery job label: %s", exc)
    if len(labels) > MAX_USER_LABELS:
        logger.warning(
            "job_labels: keeping the first %d of %d", MAX_USER_LABELS, len(labels)
        )
        labels = dict(list(labels.items())[:MAX_USER_LABELS])
    return labels
