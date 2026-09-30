"""Pre-load row-count guard for ingest runs.

Motivation
----------
dlt reports a run as successful whenever the load itself succeeds, regardless of
how much data arrived.  For most write dispositions that is harmless: a resource
that yields nothing produces no load package at all, so the target table is left
exactly as it was.  Two cases are different, and both silently destroy data that
was previously correct:

``replace``
    dlt still emits a load job for the table so the truncate/swap can run, which
    means a zero-row extraction replaces the target with an empty table.  A
    source path that moved, a glob that stopped matching, or an upstream export
    that skipped a day all look identical to a successful run.

``merge`` with ``strategy: scd2``
    scd2 retires rows absent from the incoming batch, so a *partial* extraction
    closes every key that failed to arrive.  This needs no zero-row load to
    bite: two of three source files disappearing leaves one open row and a green
    run.

Why a single ``min_rows``
-------------------------
After an scd2 merge the open (non-retired) rows are exactly the distinct primary
keys in the incoming batch, and after a replace the target holds exactly the
incoming rows.  For both dispositions the pre-load batch count therefore *is*
the number of rows that will be live in the target once the run finishes, so one
threshold checked in one place covers both.  For ``append`` and the remaining
merge strategies the same number reads as "rows in this load", which is the
natural meaning there.

The count is taken from the normalize step, before the destination is touched,
so tripping the guard leaves the target untouched rather than reporting damage
after the fact.  Duplicate primary keys in a batch make the count overstate the
surviving rows (four rows across two keys leave two open), so the guard errs
permissive and never trips on a batch that would in fact have been large enough.
"""

import logging
from dataclasses import dataclass
from typing import Any, Dict, Optional

logger = logging.getLogger(__name__)


class MinRowsNotMetError(RuntimeError):
    """Raised when a load carries fewer rows than the configured ``min_rows``.

    Deliberately *not* a :class:`ValueError`: the project reads a ``ValueError``
    from a pipeline as a pre-run configuration error and skips recording it as a
    run outcome, which would keep a tripped guard out of ``saga report`` and
    ``--select "state:failed"`` — the very channels that are meant to surface
    it. This is a genuine run failure and is recorded as one; the callers that
    print tracebacks special-case it, since the message is self-explanatory.
    """


def resolve_guarded_row_count(
    row_counts: Optional[Dict[str, int]], table_name: Optional[str]
) -> int:
    """Return the row count the guard should be evaluated against.

    Prefers the root table's own count; falls back to the sum across all
    non-system tables when the root table is absent from *row_counts* (a
    resource whose dlt table name differs from the configured one).  dlt's
    internal ``_dlt_*`` tables never count.

    Args:
        row_counts: ``row_counts`` mapping from dlt's normalize or load info.
        table_name: Configured target table name for this pipeline.

    Returns:
        Number of rows this load writes to the target table.
    """
    if not row_counts:
        return 0
    if table_name and table_name in row_counts:
        return int(row_counts[table_name] or 0)
    return sum(
        int(count or 0)
        for table, count in row_counts.items()
        if not table.startswith("_dlt_")
    )


def _pending_packages(pipeline: Any) -> set:
    """Return the ids of load packages already pending for *pipeline*.

    Captured before extraction so the guard can tell a package left by an
    earlier crashed run apart from the one it is about to reject — dlt drops
    them together, which is worth naming rather than doing silently.

    Best-effort: an introspection failure yields an empty set, which only
    costs the warning, never correctness.
    """
    try:
        return set(pipeline.list_normalized_load_packages()) | set(
            pipeline.list_extracted_load_packages()
        )
    except Exception:  # pragma: no cover - defensive; dlt API shape only
        logger.debug("Could not list pending load packages", exc_info=True)
        return set()


@dataclass
class RowGuard:
    """Row-count threshold enforced between dlt's normalize and load steps.

    Attributes:
        min_rows: Minimum rows the load must carry.  A load below this
            threshold is abandoned before the destination is touched.
        table_name: Target table name, used to pick the right entry out of
            dlt's ``row_counts`` and to write an actionable error message.
        pipeline_name: Fully-qualified pipeline name, for the error message.
        write_disposition: Effective write disposition, for the error message.
    """

    min_rows: int
    table_name: Optional[str] = None
    pipeline_name: Optional[str] = None
    write_disposition: Optional[str] = None
    # Set when the rejected package could not be deleted, so the raised error
    # can tell the operator the next run would otherwise load it.
    _drop_failed: bool = False

    def run(self, pipeline: Any, data: Any) -> Any:
        """Run *pipeline* over *data*, enforcing the threshold before loading.

        Splits dlt's ``run()`` into its extract → normalize → load phases so the
        row count is known while the load package is still local.  If the count
        is below :attr:`min_rows` the pending packages are dropped — otherwise
        dlt would pick the package up on the next run and empty the table then —
        and :class:`MinRowsNotMetError` is raised.

        Args:
            pipeline: dlt Pipeline instance.
            data: Resource or source to load.

        Returns:
            LoadInfo from ``pipeline.load()``.

        Raises:
            MinRowsNotMetError: If the load carries fewer than ``min_rows`` rows.
        """
        preexisting = _pending_packages(pipeline)
        pipeline.extract(data)
        normalize_info = pipeline.normalize()
        row_count = resolve_guarded_row_count(
            getattr(normalize_info, "row_counts", None), self.table_name
        )

        if row_count < self.min_rows:
            self._discard_pending(pipeline, preexisting)
            raise MinRowsNotMetError(self._message(row_count))

        logger.debug(
            "Row guard passed for %s: %d row(s) >= min_rows=%d",
            self.table_name,
            row_count,
            self.min_rows,
        )
        return pipeline.load()

    def _discard_pending(self, pipeline: Any, preexisting: set) -> None:
        """Drop the rejected load package so no later run can pick it up.

        Without this, dlt finds the normalized package on the next run and
        loads it then — turning a refused load into the same damage a day
        later, with nothing connecting the two.

        dlt's ``drop_pending_packages`` is all-or-nothing, so a package left
        behind by an *earlier* crashed run is discarded alongside this one.
        That is still the better of the two outcomes (the alternative is the
        delayed rewrite above), but it is real data loss on an ``append``, so
        it is named rather than swallowed.

        Args:
            pipeline: dlt Pipeline instance.
            preexisting: Package ids pending *before* this run extracted, from
                :func:`_pending_packages`.
        """
        if preexisting:
            logger.warning(
                "Discarding %d pending load package(s) from an earlier run "
                "alongside the rejected one — dlt drops them together: %s",
                len(preexisting),
                ", ".join(sorted(preexisting)),
            )
        # dlt 1.30 renamed drop_pending_packages -> abort_packages (the old name
        # warns until 2.0). Prefer the new one when present so the guard doesn't
        # emit a DeprecationWarning on every rejected load, while still working
        # on the versions that only have the old name.
        discard = getattr(pipeline, "abort_packages", None) or getattr(
            pipeline, "drop_pending_packages"
        )
        try:
            discard()
        except Exception as exc:
            # The package is still on disk and the next run will load it, which
            # is exactly the damage this guard exists to prevent. Say so loudly;
            # the raised error carries the same warning for the run summary.
            logger.error(
                "Could not drop the rejected load package for %s: %s. The next "
                "run will load it unless it is cleared manually "
                "(`saga destroy` or deleting the pipeline working directory).",
                self.pipeline_name or self.table_name,
                exc,
            )
            self._drop_failed = True

    def _message(self, row_count: int) -> str:
        """Build the user-facing abort message."""
        target = self.pipeline_name or self.table_name or "pipeline"
        # "unchanged" rather than "still holds the previous run's data": on a
        # first run there is no previous data, and on a re-run after an earlier
        # failure the table may not exist at all. Unchanged is true in every case
        # and still says the thing that matters — the guard destroyed nothing.
        detail = (
            f"{target} extracted {row_count} row(s), below the configured "
            f"min_rows={self.min_rows}. The load was abandoned before writing, "
            f"so {self.table_name or 'the target table'} is unchanged."
        )
        if row_count == 0:
            detail += (
                " A zero-row extraction usually means the source moved or a "
                "glob stopped matching — check the source location before "
                "re-running."
            )
        if self._drop_failed:
            detail += (
                " WARNING: the rejected load package could not be deleted, so "
                "the next run of this pipeline will load it and apply the very "
                "write this guard refused. Clear the pipeline working directory "
                "before re-running."
            )
        return detail


def build_row_guard(
    min_rows: Optional[int],
    *,
    table_name: Optional[str],
    pipeline_name: Optional[str],
    write_disposition: Optional[str],
) -> Optional[RowGuard]:
    """Build a :class:`RowGuard`, or ``None`` when no threshold is configured.

    Returning ``None`` keeps unguarded pipelines on dlt's own ``run()`` rather
    than the split extract/normalize/load path.

    Args:
        min_rows: Configured ``min_rows``, or ``None``/0 when unset.
        table_name: Target table name.
        pipeline_name: Fully-qualified pipeline name.
        write_disposition: Effective write disposition.

    Returns:
        A configured guard, or ``None`` when guarding is disabled.
    """
    if not min_rows:
        return None
    return RowGuard(
        min_rows=int(min_rows),
        table_name=table_name,
        pipeline_name=pipeline_name,
        write_disposition=write_disposition,
    )


def warn_on_empty_replace(
    row_counts: Optional[Dict[str, int]],
    *,
    write_disposition: Optional[str],
    table_name: Optional[str],
    min_rows: Optional[int],
    run_logger: Optional[Any] = None,
) -> bool:
    """Warn when a ``replace`` load emptied its target table.

    Unlike the guard this is unconditional, because the condition is otherwise
    invisible: dlt reports the run as successful and the only trace is a row
    count of zero.  It fires after the load, so it reports damage rather than
    preventing it — the message points at ``min_rows`` for prevention.

    Args:
        row_counts: ``row_counts`` from the completed load.
        write_disposition: Configured write disposition (a ``+historize``
            suffix is tolerated).
        table_name: Target table name.
        min_rows: Configured threshold; when set the guard already covers this
            case and no warning is emitted.
        run_logger: Logger to warn through.  Defaults to this module's logger.

    Returns:
        ``True`` if a warning was emitted.
    """
    if min_rows:
        return False
    base = (write_disposition or "").split("+", 1)[0]
    if base != "replace":
        return False
    if resolve_guarded_row_count(row_counts, table_name) > 0:
        return False

    (run_logger or logger).warning(
        "%s was replaced with 0 rows — the target table is now empty. "
        "Set min_rows on this pipeline to abandon such a load instead of "
        "writing it.",
        table_name or "Target table",
    )
    return True
