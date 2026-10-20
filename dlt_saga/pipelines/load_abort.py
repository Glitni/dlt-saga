"""Abort a load package whose load failed terminally before any job ran.

dlt's ``load.auto_abort_on_terminal_error`` aborts a package when one of its
*jobs* fails terminally. A terminal error while the load is still being set up
— creating the dataset, migrating the schema, truncating tables before a
replace — is raised before any job runs, so dlt leaves the package pending. The
next ``run()`` then loads that stale package and returns without extracting,
replaying the same failure until it's cleared by hand, and loading stale data
once the cause is fixed.

:func:`abort_after_terminal_load_failure` extends dlt's job-level abort to those
setup failures, under the same setting so one switch governs both. Transient
errors are left pending for dlt to retry on the next run.
"""

import logging
from typing import Any

logger = logging.getLogger(__name__)


def abort_after_terminal_load_failure(pipeline: Any, exc: BaseException) -> bool:
    """Abort *pipeline*'s pending packages if *exc* is a terminal setup failure.

    Aborting discards the pending packages and rolls local pipeline state
    (incremental cursors included) back to before them, so the next run
    re-extracts from the source.

    Args:
        pipeline: dlt Pipeline whose run raised *exc*.
        exc: Exception raised by the dlt run.

    Returns:
        True when the pending packages were aborted.
    """
    from dlt.common.destination.exceptions import DestinationTerminalException
    from dlt.load.exceptions import LoadClientJobException
    from dlt.pipeline.exceptions import PipelineStepFailed

    if not isinstance(exc, PipelineStepFailed) or exc.step != "load":
        return False
    cause = exc.exception
    # Job failures are dlt's own auto-abort to handle (or deliberately not).
    if isinstance(cause, LoadClientJobException) or not isinstance(
        cause, DestinationTerminalException
    ):
        return False
    if not _auto_abort_enabled():
        return False
    # abort_packages arrived in dlt 1.30, together with the pending-on-failure
    # behaviour it undoes; drop_pending_packages can't roll state back.
    abort = getattr(pipeline, "abort_packages", None)
    if abort is None or not pipeline.has_pending_data:
        return False

    try:
        abort()
    except Exception as abort_exc:
        logger.warning(
            "Could not abort the load package for %s after a terminal load "
            "failure: %s. The next run will retry it instead of extracting; "
            "clear it with `dlt pipeline %s abort-packages`.",
            pipeline.pipeline_name,
            abort_exc,
            pipeline.pipeline_name,
        )
        return False

    logger.warning(
        "Aborted the load package for %s after a terminal load failure; the "
        "next run re-extracts from the source.",
        pipeline.pipeline_name,
    )
    return True


def _auto_abort_enabled() -> bool:
    """Whether ``load.auto_abort_on_terminal_error`` is enabled in dlt config."""
    import dlt

    try:
        return bool(dlt.config.get("load.auto_abort_on_terminal_error", bool))
    except Exception:
        logger.debug(
            "Could not resolve load.auto_abort_on_terminal_error", exc_info=True
        )
        return False
