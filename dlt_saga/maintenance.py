"""Maintenance operations for saga's internal bookkeeping tables.

Backs the ``saga maintenance`` command (and ``saga doctor``'s read-only
clustering check). ``CREATE TABLE IF NOT EXISTS`` never re-clusters an existing
table, so internal log tables created before clustering-at-CREATE stay
unclustered; these helpers reconcile their clustering across a project's schemas.

The logic here is warehouse-facing but CLI-agnostic (no typer) — the command in
``cli.py`` is a thin wrapper that discovers configs, builds a destination, and
renders the results returned here.
"""

import logging
from typing import Any, Dict, List, Tuple

from dlt_saga.utility.internal_tables import (
    InternalLogTable,
    clustering_state,
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


def run_clustering_maintenance(
    context: Any, selected_configs: dict, dry_run: bool = False
) -> Dict[str, int]:
    """Reconcile clustering on the internal log tables of the selected schemas.

    The full operation behind ``saga maintenance`` / ``Session.maintenance()``:
    build a destination for the selected configs, reconcile every internal log
    table's clustering, log a summary, and return the status counts.

    Assumes the caller has established the execution context and any
    impersonation; the connection lifecycle (connect/close around the sweep) is
    owned here. Returns zeroed counts when the destination can't reconcile
    clustering (e.g. DuckDB).
    """
    from dlt_saga.utility.cli.common import build_destination_from_configs

    empty = {"absent": 0, "unchanged": 0, "reconciled": 0}
    dest_type = context.get_destination_type() or "unknown"
    destination = build_destination_from_configs(dest_type, context, selected_configs)
    if not destination.supports_clustering_reconcile():
        logger.info(
            "Clustering reconcile is not supported for destination '%s' — "
            "nothing to do.",
            dest_type,
        )
        return empty

    tables = resolve_internal_log_tables_for(selected_configs)
    destination.connect()
    try:
        counts = reconcile_internal_clustering(destination, tables, dry_run)
    finally:
        destination.close()

    verb = "would reconcile" if dry_run else "reconciled"
    logger.info(
        "Maintenance complete: %d %s, %d already up to date, %d absent.",
        counts.get("reconciled", 0),
        verb,
        counts.get("unchanged", 0),
        counts.get("absent", 0),
    )
    if dry_run and counts.get("reconciled", 0):
        logger.info("Re-run without --dry-run to apply.")
    return counts


def scan_clustering(
    destination: Any, tables: List[InternalLogTable]
) -> List[Tuple[InternalLogTable, str]]:
    """Read each table's current clustering and classify it (read-only).

    Assumes a connected destination — the caller owns the connection (and any
    impersonation) so a single session can serve several checks. Returns
    ``[(table, state)]`` where state is ``"absent"`` / ``"ok"`` / ``"drift"``.
    """
    return [
        (
            table,
            clustering_state(
                destination.get_clustering_columns(table.schema, table.table),
                table.cluster_columns,
            ),
        )
        for table in tables
    ]


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
