"""Interactive confirmation prompts for destructive ``saga`` operations.

Each ``_confirm_*`` helper shows a boxed warning banner and (unless ``--yes`` or
a Cloud Run environment skips the prompt so CI/cron don't hang on stdin) asks
for confirmation before a full refresh, partial re-historization, or destroy.
Declining exits 0.

Extracted from ``cli.py`` (module-boundary cleanup); behavior unchanged. The
command bodies stay in ``cli.py`` and call these helpers.
"""

import logging
from typing import Optional

import typer

logger = logging.getLogger(__name__)

_CONFIRM_SEP = "=" * 70

# Reused across the full-refresh confirmations — the ingest and historize bodies
# are identical whether prompted standalone or as part of ``saga run``.
_FULL_REFRESH_BODY = (
    "This will permanently delete:\n"
    "  - All destination tables (main + staging)\n"
    "  - Pipeline state metadata\n"
    "  - Load tracking information"
)
_HISTORIZE_REFRESH_BODY = (
    "This will permanently delete and rebuild:\n"
    "  - All historized output tables\n"
    "  - Historization log entries for selected pipelines"
)


def _warn_banner(title: str, body: str) -> None:
    """Log a separator-boxed warning banner (title above, body inside the box)."""
    logger.warning("%s\n%s\n%s\n%s", title, _CONFIRM_SEP, body, _CONFIRM_SEP)


def _confirm_destructive(title: str, body: str, in_cloud_run: bool, yes: bool) -> None:
    """Warn with a boxed banner, then prompt before a destructive operation.

    ``--yes`` / Cloud Run skip the prompt (so CI/cron don't hang on stdin) but
    still show the banner. Declining exits 0.
    """
    _warn_banner(title, body)
    if not (in_cloud_run or yes):
        if not typer.confirm("Are you sure you want to proceed?", default=False):
            logger.info("Operation cancelled by user")
            raise typer.Exit(0)


def _confirm_full_refresh(full_refresh: bool, in_cloud_run: bool, yes: bool = False):
    """Confirm full refresh operation if specified."""
    if not full_refresh:
        return
    _confirm_destructive("FULL REFRESH MODE", _FULL_REFRESH_BODY, in_cloud_run, yes)


def _confirm_historize_full_refresh(
    full_refresh: bool, in_cloud_run: bool, yes: bool = False
):
    """Confirm historize full refresh operation if specified."""
    if not full_refresh:
        return
    _confirm_destructive(
        "HISTORIZE FULL REFRESH MODE", _HISTORIZE_REFRESH_BODY, in_cloud_run, yes
    )


def _confirm_partial_refresh(
    partial_refresh: bool,
    historize_from: Optional[str],
    in_cloud_run: bool,
    yes: bool,
) -> None:
    """Confirm partial re-historization if --partial-refresh or --historize-from is set."""
    if not partial_refresh and historize_from is None:
        return

    if partial_refresh:
        title = "PARTIAL RE-HISTORIZATION"
        body = (
            "This will roll back and rebuild historization starting from the "
            "earliest\navailable raw snapshot. SCD2 records derived from older "
            "snapshots are preserved."
        )
    else:
        title = f"PARTIAL RE-HISTORIZATION FROM {historize_from}"
        body = (
            f"This will roll back and rebuild historization starting from "
            f"{historize_from}.\nSCD2 records derived from older snapshots are "
            "preserved."
        )
    _confirm_destructive(title, body, in_cloud_run, yes)


def _confirm_destroy(
    dry_run: bool, resource_type: str, in_cloud_run: bool, yes: bool
) -> None:
    """Confirm a destroy operation. No prompt in dry-run (nothing is deleted)."""
    if dry_run:
        return

    scope = {
        "all": "ingest tables, historized tables, and all pipeline state",
        "ingest": "ingest tables and dlt/load state",
        "historize": "historized tables and historize log entries",
    }[resource_type]
    body = (
        "This will permanently delete for the selected pipeline(s):\n"
        f"  - {scope}\n"
        "It does NOT reload any data. Intended for decommissioning a pipeline\n"
        "before its config is removed or disabled. Run with --dry-run first to\n"
        "preview exactly what would be dropped."
    )
    _confirm_destructive("DESTROY MODE", body, in_cloud_run, yes)


def _confirm_run_full_refresh(
    full_refresh: bool,
    in_cloud_run: bool,
    has_ingest: bool,
    has_historize: bool,
    yes: bool = False,
) -> tuple:
    """Ask about ingest and historize full refresh independently.

    Returns (refresh_ingest, refresh_historize). When full_refresh is False
    both are False (phases run incrementally). When the user declines both
    prompts the command exits.
    """
    if not full_refresh:
        return False, False

    # --yes (or Cloud Run) refreshes both phases without prompting, so CI/cron
    # doesn't hang on stdin.
    if in_cloud_run or yes:
        return True, True

    refresh_ingest = False
    refresh_historize = False
    if has_ingest:
        _warn_banner("INGEST FULL REFRESH MODE", _FULL_REFRESH_BODY)
        refresh_ingest = typer.confirm("Rebuild ingested (raw) tables?", default=False)
    if has_historize:
        _warn_banner("HISTORIZE FULL REFRESH MODE", _HISTORIZE_REFRESH_BODY)
        refresh_historize = typer.confirm("Rebuild historized tables?", default=False)
    if not refresh_ingest and not refresh_historize:
        logger.info("Operation cancelled by user")
        raise typer.Exit(0)
    return refresh_ingest, refresh_historize
