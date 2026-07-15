"""Maintenance operations for saga's internal bookkeeping tables.

Backs the ``saga maintenance`` command. Three passes, all run by default:

* **Clustering reconcile** — ``CREATE TABLE IF NOT EXISTS`` never re-clusters an
  existing table, so internal log tables created before clustering-at-CREATE stay
  unclustered; this reconciles their clustering across a project's schemas.
* **Log compaction** — the status logs (native_load, execution-plan) accumulate
  superseded rows. This collapses each to the latest row per key by deleting
  strictly-superseded *earlier* non-terminal rows (lossless: reads already
  collapse to the latest event per key), plus an age-gated *deletion* of dangling
  native_load ``started`` rows (orphans).
* **Stale-task reconcile** — a crashed or never-scheduled orchestration task
  leaves a dangling ``running``/``pending`` execution-plan row forever; this
  *relabels* ones older than the stale cutoff to a terminal ``abandoned`` status
  (an in-place UPDATE, not a delete).

The logic here is warehouse-facing but CLI-agnostic (no typer) — the command in
``cli.py`` is a thin wrapper that discovers configs, builds a destination, and
renders the results returned here.
"""

import logging
from typing import Any, Dict, List, Optional

from dlt_saga.utility.internal_tables import (
    CompactableLog,
    InternalLogTable,
    clustering_state,
    resolve_compactable_logs,
    resolve_internal_log_tables,
)

logger = logging.getLogger(__name__)


def resolve_internal_log_tables_for(selected_configs: dict) -> List[InternalLogTable]:
    """Resolve the internal log tables to check from selected pipeline configs.

    Schemas are taken from the selected configs; the execution-plan log is added
    under the resolved orchestration schema.
    """
    from dlt_saga.utility.cli.common import flatten_configs
    from dlt_saga.utility.naming import get_execution_plan_schema

    flat = flatten_configs(selected_configs) if selected_configs else []
    schemas = {c.schema_name for c in flat if c.schema_name}
    try:
        orchestration_schema = get_execution_plan_schema()
    except Exception as exc:
        logger.debug("Could not resolve orchestration schema: %s", exc)
        orchestration_schema = None
    return resolve_internal_log_tables(schemas, orchestration_schema)


def resolve_compactable_logs_for(selected_configs: dict) -> List[CompactableLog]:
    """Resolve the compactable internal logs from selected pipeline configs.

    Mirrors ``resolve_internal_log_tables_for``: schemas come from the selected
    configs, and the execution-plan log is added under the orchestration schema.
    """
    from dlt_saga.utility.cli.common import flatten_configs
    from dlt_saga.utility.naming import get_execution_plan_schema

    flat = flatten_configs(selected_configs) if selected_configs else []
    schemas = {c.schema_name for c in flat if c.schema_name}
    try:
        orchestration_schema = get_execution_plan_schema()
    except Exception as exc:
        logger.debug("Could not resolve orchestration schema: %s", exc)
        orchestration_schema = None
    return resolve_compactable_logs(schemas, orchestration_schema)


def run_maintenance(
    context: Any, selected_configs: dict, dry_run: bool = False
) -> Dict[str, Dict[str, int]]:
    """Run all maintenance passes behind ``saga maintenance`` / ``Session.maintenance()``.

    Builds a destination for the selected configs, then on a single connection
    runs, in order, the clustering reconcile (if supported), the log compaction,
    and the stale-task reconcile — logs a summary and returns the per-pass
    counts::

        {"clustering": {"absent", "unchanged", "reconciled"},
         "compaction": {"absent", "collapsed", "orphaned"},
         "reconcile": {"abandoned"}}

    Compaction runs before the stale-task reconcile so the latter operates on the
    already-collapsed table; the two touch disjoint rows, so the order is a cost
    choice, not a correctness one.

    Assumes the caller has established the execution context and any
    impersonation; the connection lifecycle (connect/close around the sweep) is
    owned here.
    """
    from dlt_saga.utility.cli.common import build_destination_from_configs

    clustering = {"absent": 0, "unchanged": 0, "reconciled": 0}
    compaction = {"absent": 0, "collapsed": 0, "orphaned": 0}
    reconcile = {"abandoned": 0}

    dest_type = context.get_destination_type() or "unknown"
    destination = build_destination_from_configs(dest_type, context, selected_configs)

    clustering_tables = (
        resolve_internal_log_tables_for(selected_configs)
        if destination.supports_clustering_reconcile()
        else []
    )
    compaction_logs = (
        resolve_compactable_logs_for(selected_configs)
        if destination.supports_log_compaction()
        else []
    )

    if not clustering_tables and not compaction_logs:
        logger.info("Maintenance has nothing to do for destination '%s'.", dest_type)
        return {
            "clustering": clustering,
            "compaction": compaction,
            "reconcile": reconcile,
        }

    destination.connect()
    try:
        if clustering_tables:
            clustering = reconcile_internal_clustering(
                destination, clustering_tables, dry_run
            )
        if compaction_logs:
            compaction = run_log_compaction(destination, compaction_logs, dry_run)
            reconcile = run_stale_task_reconcile(destination, compaction_logs, dry_run)
    finally:
        destination.close()

    _log_maintenance_summary(clustering, compaction, reconcile, dry_run)
    return {
        "clustering": clustering,
        "compaction": compaction,
        "reconcile": reconcile,
    }


def _log_maintenance_summary(
    clustering: Dict[str, int],
    compaction: Dict[str, int],
    reconcile: Dict[str, int],
    dry_run: bool,
) -> None:
    """Log a one-line summary per pass, and a re-run hint when in dry-run.

    ``absent`` counts are omitted: an internal log a schema never uses (e.g. no
    native_load log in a google-sheets-only schema) was never expected to exist,
    so reporting it is noise. The counts remain in the returned dict for callers.
    """
    reconciled = clustering.get("reconciled", 0)
    collapsed = compaction.get("collapsed", 0)
    orphaned = compaction.get("orphaned", 0)
    abandoned = reconcile.get("abandoned", 0)

    logger.info(
        "Clustering: %s %d table(s), %d already up to date.",
        "would reconcile" if dry_run else "reconciled",
        reconciled,
        clustering.get("unchanged", 0),
    )
    verb = "would delete" if dry_run else "deleted"
    if orphaned:
        logger.info(
            "Compaction: %s %d superseded + %d orphan row(s).",
            verb,
            collapsed,
            orphaned,
        )
    else:
        logger.info("Compaction: %s %d superseded row(s).", verb, collapsed)
    logger.info(
        "Stale tasks: %s %d task(s) as abandoned.",
        "would mark" if dry_run else "marked",
        abandoned,
    )
    if dry_run and (reconciled or collapsed or orphaned or abandoned):
        logger.info("Re-run without --dry-run to apply.")


def reconcile_internal_clustering(
    destination: Any, tables: List[InternalLogTable], dry_run: bool = False
) -> Dict[str, int]:
    """Reconcile clustering across ``tables``, returning status counts.

    Assumes a connected destination (the caller owns the connection and any
    impersonation). Counts are keyed by ``absent`` / ``unchanged`` /
    ``reconciled``.
    """
    counts = {"absent": 0, "unchanged": 0, "reconciled": 0}
    for table in tables:
        result = _reconcile_table_clustering(destination, table, dry_run)
        counts[result] = counts.get(result, 0) + 1
    return counts


def _reconcile_table_clustering(
    destination: Any, table: InternalLogTable, dry_run: bool
) -> str:
    """Reconcile (or, in dry-run, classify) one table's clustering.

    Returns ``absent`` / ``unchanged`` / ``reconciled``.
    """
    if dry_run:
        current = destination.get_clustering_columns(table.schema, table.table)
        state = clustering_state(current, table.cluster_columns)
        result = {"ok": "unchanged", "drift": "reconciled"}.get(state, state)
        if state == "drift":
            logger.info(
                "Would reconcile %s.%s (%s): %s -> %s",
                table.schema,
                table.table,
                table.label,
                current,
                table.cluster_columns,
            )
        return result

    result = destination.reconcile_clustering(
        table.schema, table.table, table.cluster_columns
    )
    if result == "reconciled":
        logger.info(
            "Reconciled clustering on %s.%s (%s) -> %s",
            table.schema,
            table.table,
            table.label,
            table.cluster_columns,
        )
    return result


# ---------------------------------------------------------------------------
# Log compaction
# ---------------------------------------------------------------------------


def run_log_compaction(
    destination: Any, logs: List[CompactableLog], dry_run: bool = False
) -> Dict[str, int]:
    """Compact each log in ``logs``, returning aggregate row counts.

    Assumes a connected destination (the caller owns the connection and any
    impersonation). Counts are keyed by ``absent`` (logs that do not exist),
    ``collapsed`` (superseded non-terminal rows), and ``orphaned`` (dangling
    ``started`` rows removed by the age-gated cleanup).
    """
    totals = {"absent": 0, "collapsed": 0, "orphaned": 0}
    for log in logs:
        result = compact_log(destination, log, dry_run)
        if result is None:
            totals["absent"] += 1
            continue
        totals["collapsed"] += result["collapsed"]
        totals["orphaned"] += result["orphaned"]
    return totals


def compact_log(
    destination: Any, log: CompactableLog, dry_run: bool
) -> Optional[Dict[str, int]]:
    """Compact one log (or, in dry-run, count what would be removed).

    Returns ``None`` when the table does not exist, otherwise a dict with
    ``collapsed`` and ``orphaned`` row counts. The collapse and orphan
    predicates are disjoint by construction (collapse targets superseded rows;
    orphan targets the latest row of a terminal-less key), so the counts never
    double-report a row and the orphan count is unaffected by the collapse
    running first.
    """
    if not destination.table_exists(log.schema, log.table):
        return None

    table_id = destination.get_full_table_id(log.schema, log.table)

    collapsed = _apply_or_count(
        destination,
        log.schema,
        table_id,
        _collapse_where(destination, log, table_id),
        dry_run,
    )
    orphaned = 0
    if log.orphan is not None:
        orphaned = _apply_or_count(
            destination,
            log.schema,
            table_id,
            _orphan_where(destination, log, table_id),
            dry_run,
        )

    if collapsed or orphaned:
        # Per-table detail is DEBUG; the run summary (INFO) is the headline.
        logger.debug(
            "%s %d superseded + %d orphan row(s) on %s.%s (%s)",
            "Would compact" if dry_run else "Compacted",
            collapsed,
            orphaned,
            log.schema,
            log.table,
            log.label,
        )
    return {"collapsed": collapsed, "orphaned": orphaned}


def _count_where(destination: Any, schema: str, table_id: str, where: str) -> int:
    """Count rows matching ``where`` (aliased ``R``, read-only).

    Counting first — rather than reading a mutation's affected-row count, which is
    destination-specific — keeps reporting portable and identical between dry-run
    and apply. Best-effort: on the maintenance command's single-threaded run there
    is no concurrent writer to race, and a stray concurrent write would only skew
    the reported number, never the correctness of the predicate-scoped statement.
    """
    count_sql = f"SELECT COUNT(*) AS n FROM {table_id} AS R WHERE {where}"
    return _scalar(list(destination.execute_sql(count_sql, schema)))


def _apply_or_count(
    destination: Any, schema: str, table_id: str, where: str, dry_run: bool
) -> int:
    """Count rows matching ``where``; DELETE them unless ``dry_run``."""
    n = _count_where(destination, schema, table_id, where)
    if n and not dry_run:
        destination.execute_sql(f"DELETE FROM {table_id} AS R WHERE {where}", schema)
    return n


def _scalar(rows: Any) -> int:
    """Extract a single integer from a one-row, one-column result set."""
    if not rows:
        return 0
    row = rows[0]
    value = row[0] if hasattr(row, "__getitem__") else getattr(row, "n", 0)
    return int(value or 0)


def _key_join(log: CompactableLog) -> str:
    """NULL-safe equality between the ``S`` and ``R`` aliases on the log's key."""
    return " AND ".join(
        f"(S.{c} = R.{c} OR (S.{c} IS NULL AND R.{c} IS NULL))" for c in log.key_columns
    )


def _terminal_list(destination: Any, log: CompactableLog) -> str:
    """Render the terminal statuses as a SQL ``IN`` list."""
    return ", ".join(
        f"'{destination.escape_string_literal(s)}'" for s in log.terminal_statuses
    )


def _supersede_expr(destination: Any, log: CompactableLog) -> str:
    """Boolean: row ``S`` supersedes the non-terminal row ``R`` for the same key.

    ``S`` supersedes ``R`` when it is strictly later by ``order_expr``, or —
    matching how reads break ties — equal in order but terminal (a terminal row
    wins over a non-terminal one at the same instant). The tie branch is not
    cosmetic: on a low-resolution clock (e.g. Windows) or a fast load, a
    ``started`` row and its ``success`` share an identical timestamp, and a
    strict ``> `` alone would never collapse that common pair.
    """
    later = f"{log.order_expr.format(a='S')} > {log.order_expr.format(a='R')}"
    tie = (
        f"{log.order_expr.format(a='S')} = {log.order_expr.format(a='R')} "
        f"AND S.{log.status_column} IN ({_terminal_list(destination, log)})"
    )
    return f"({later} OR ({tie}))"


def _collapse_where(destination: Any, log: CompactableLog, table_id: str) -> str:
    """Predicate for lossless collapse: superseded non-terminal rows.

    A non-terminal row is superseded when the same key has a row that supersedes
    it (see :func:`_supersede_expr`), so it is never the row a read returns.
    Terminal rows and each key's latest row are left untouched.
    """
    return (
        f"R.{log.status_column} NOT IN ({_terminal_list(destination, log)}) "
        f"AND EXISTS (SELECT 1 FROM {table_id} AS S "
        f"WHERE {_key_join(log)} AND {_supersede_expr(destination, log)})"
    )


def _orphan_where(destination: Any, log: CompactableLog, table_id: str) -> str:
    """Predicate for the age-gated orphan cleanup (native_load only).

    Targets a stale dangling row: the latest row for its key (nothing supersedes
    it), of the orphan status, older than the stale cutoff, whose key never got a
    terminal row. Disjoint from the collapse predicate (which requires a
    superseding row), so the two never delete the same row twice.
    """
    orphan = log.orphan
    assert orphan is not None
    stale_cutoff = destination.timestamp_n_days_ago(max(1, orphan.stale_hours // 24))
    return (
        f"R.{log.status_column} = '{destination.escape_string_literal(orphan.status)}' "
        f"AND R.{orphan.timestamp_column} < {stale_cutoff} "
        f"AND NOT EXISTS (SELECT 1 FROM {table_id} AS S "
        f"WHERE {_key_join(log)} AND {_supersede_expr(destination, log)}) "
        f"AND NOT EXISTS (SELECT 1 FROM {table_id} AS S "
        f"WHERE {_key_join(log)} "
        f"AND S.{log.status_column} IN ({_terminal_list(destination, log)}))"
    )


# ---------------------------------------------------------------------------
# Stale-task reconciliation (execution-plan log)
# ---------------------------------------------------------------------------


def run_stale_task_reconcile(
    destination: Any, logs: List[CompactableLog], dry_run: bool = False
) -> Dict[str, int]:
    """Relabel stale dangling tasks to ``abandoned`` across logs that opt in.

    Only logs carrying a ``reconcile`` spec (currently just the execution-plan
    log) are touched; the rest are skipped. Assumes a connected destination and
    is meant to run *after* compaction, so it operates on the collapsed table —
    though its predicate is self-contained and correct regardless of order.
    Returns ``{"abandoned": <rows relabelled>}``.
    """
    total = 0
    for log in logs:
        if log.reconcile is not None:
            total += reconcile_stale_tasks(destination, log, dry_run)
    return {"abandoned": total}


def reconcile_stale_tasks(destination: Any, log: CompactableLog, dry_run: bool) -> int:
    """Relabel one log's stale dangling rows to ``abandoned`` (or count them).

    Returns the number of rows relabelled (or that would be, in dry-run); ``0``
    when the table does not exist.
    """
    if not destination.table_exists(log.schema, log.table):
        return 0

    table_id = destination.get_full_table_id(log.schema, log.table)
    where = _stale_task_where(destination, log, table_id)
    n = _count_where(destination, log.schema, table_id, where)
    if n and not dry_run:
        set_clause = _abandon_set_clause(destination, log)
        destination.execute_sql(
            f"UPDATE {table_id} AS R SET {set_clause} WHERE {where}", log.schema
        )
    if n:
        logger.debug(
            "%s %d stale task(s) as '%s' on %s.%s (%s)",
            "Would abandon" if dry_run else "Abandoned",
            n,
            log.reconcile.abandoned_status,
            log.schema,
            log.table,
            log.label,
        )
    return n


def _stale_task_where(destination: Any, log: CompactableLog, table_id: str) -> str:
    """Predicate for the latest non-terminal row of a key that is stale.

    Self-contained (does not assume compaction removed superseded rows): it
    requires the row be non-terminal, older than the cutoff, and *not* superseded
    by any peer — i.e. genuinely the latest for its key. Disjoint from the
    collapse predicate, which requires a superseding peer.
    """
    rec = log.reconcile
    assert rec is not None
    nonterminal = ", ".join(
        f"'{destination.escape_string_literal(s)}'" for s in rec.nonterminal_statuses
    )
    stale_cutoff = destination.timestamp_n_days_ago(max(1, rec.stale_hours // 24))
    return (
        f"R.{log.status_column} IN ({nonterminal}) "
        f"AND {log.order_expr.format(a='R')} < {stale_cutoff} "
        f"AND NOT EXISTS (SELECT 1 FROM {table_id} AS S "
        f"WHERE {_key_join(log)} AND {_supersede_expr(destination, log)})"
    )


def _abandon_set_clause(destination: Any, log: CompactableLog) -> str:
    """``SET`` body relabelling a row to ``abandoned`` with a per-status reason.

    ``CASE`` over the *pre-update* status (SQL evaluates all RHS before assigning)
    so each original status gets its own ``error_message``, prefixed to mark it as
    maintenance-set (not a genuine failure). ``error_message`` is the shared
    column name on both log tables. ``completed_at`` is intentionally left
    untouched: a task we never saw finish has no real completion time.
    """
    rec = log.reconcile
    assert rec is not None
    cases = []
    for status, reason in rec.reasons.items():
        text = f"[saga maintenance] {reason.format(hours=rec.stale_hours)}"
        cases.append(
            f"WHEN '{destination.escape_string_literal(status)}' "
            f"THEN '{destination.escape_string_literal(text)}'"
        )
    case_expr = f"CASE R.{log.status_column} {' '.join(cases)} ELSE R.error_message END"
    return (
        f"{log.status_column} = "
        f"'{destination.escape_string_literal(rec.abandoned_status)}', "
        f"error_message = {case_expr}"
    )
