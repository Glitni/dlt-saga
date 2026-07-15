"""Registry of saga's internal bookkeeping tables and their desired layout.

Single source of truth for which internal tables exist, where they live, and how
they should be clustered — shared between ``saga doctor``'s read-only clustering
check and ``saga maintenance``'s clustering reconcile. The desired cluster
columns are imported from the same constants the create-time DDL uses, so a
reconciled table matches a freshly created one.
"""

from dataclasses import dataclass
from typing import Dict, Iterable, List, Optional


@dataclass(frozen=True)
class InternalLogTable:
    """An internal bookkeeping table and its desired physical clustering."""

    schema: str
    table: str
    cluster_columns: List[str]
    label: str  # human-facing, e.g. "native_load log"


@dataclass(frozen=True)
class OrphanRule:
    """Age-gated cleanup of dangling non-terminal rows for a compactable log.

    A row is an orphan when it is the latest row for its key, its status is
    ``status``, no terminal row was ever written for the key, and its
    ``timestamp_column`` is older than ``stale_hours`` — i.e. the run crashed
    before recording an outcome. Unlike the lossless collapse, this deletes a
    key's only/latest row, so it is gated on age.
    """

    status: str
    timestamp_column: str
    stale_hours: int


@dataclass(frozen=True)
class TaskReconcile:
    """Relabel a stale dangling non-terminal row to a terminal ``abandoned`` state.

    Where :class:`OrphanRule` *deletes* a dangling row, this *UPDATEs* it: the
    latest non-terminal row of a key, older than ``stale_hours``, is relabelled
    to ``abandoned`` in place (used for the execution-plan log, where a crashed
    or never-scheduled task would otherwise sit as phantom ``pending``/``running``
    forever). ``failed`` is deliberately not used — it is reserved for genuine
    worker-reported failures. ``reasons`` maps each original status to the
    ``error_message`` text (with a ``{hours}`` placeholder).
    """

    nonterminal_statuses: List[str]
    abandoned_status: str
    stale_hours: int
    reasons: Dict[str, str]


@dataclass(frozen=True)
class CompactableLog:
    """An internal status log and the shape of its lossless compaction.

    ``saga maintenance`` collapses each log to the latest row per ``key_columns``
    by deleting strictly-superseded *earlier* non-terminal rows — never a
    terminal row, never a key's only/latest row. Lossless because every read of
    these logs already collapses to the latest event per key (via a companion
    view), so a superseded earlier row is never observed.

    ``order_expr`` is a SQL expression template whose ``{a}`` placeholder is
    filled with a row alias; a higher value means a later event. ``orphan`` adds
    the separate age-gated dangling-row *deletion* (native_load only);
    ``reconcile`` adds the age-gated dangling-row *relabelling* (execution-plan
    only). A log uses at most one of the two.
    """

    schema: str
    table: str
    label: str
    key_columns: List[str]
    order_expr: str
    status_column: str
    terminal_statuses: List[str]
    orphan: Optional[OrphanRule] = None
    reconcile: Optional[TaskReconcile] = None


def resolve_internal_log_tables(
    pipeline_schemas: Iterable[str],
    orchestration_schema: Optional[str] = None,
) -> List[InternalLogTable]:
    """Enumerate the internal log tables to check across a project's schemas.

    The native_load and historize logs live in each pipeline's own schema, so
    they are enumerated once per distinct pipeline schema. The execution-plan
    log lives in the orchestration schema (which may coincide with a pipeline
    schema in dev; distinct table names mean no conflict).

    Absent tables are not filtered here — the caller reads live clustering and
    skips tables that do not exist.
    """
    from dlt_saga.historize.state import LOG_CLUSTER_COLUMNS as HISTORIZE_COLS
    from dlt_saga.pipelines.native_load.state import (
        LOG_CLUSTER_COLUMNS as NATIVE_LOAD_COLS,
    )
    from dlt_saga.project_config import (
        get_execution_plans_table_name,
        get_historize_log_table_name,
        get_native_load_log_table_name,
    )
    from dlt_saga.utility.orchestration.execution_plan import PLANS_CLUSTER_COLUMNS

    tables: List[InternalLogTable] = []
    for schema in sorted({s for s in pipeline_schemas if s}):
        tables.append(
            InternalLogTable(
                schema,
                get_native_load_log_table_name(),
                list(NATIVE_LOAD_COLS),
                "native_load log",
            )
        )
        tables.append(
            InternalLogTable(
                schema,
                get_historize_log_table_name(),
                list(HISTORIZE_COLS),
                "historize log",
            )
        )

    if orchestration_schema:
        tables.append(
            InternalLogTable(
                orchestration_schema,
                get_execution_plans_table_name(),
                list(PLANS_CLUSTER_COLUMNS),
                "execution-plan log",
            )
        )

    return tables


def resolve_compactable_logs(
    pipeline_schemas: Iterable[str],
    orchestration_schema: Optional[str] = None,
) -> List[CompactableLog]:
    """Enumerate the internal logs that ``saga maintenance`` can compact.

    The native_load log (with its orphan-``started`` cleanup) lives in each
    pipeline schema; the execution-plan log lives in the orchestration schema.
    The historize log is deliberately absent: it only ever writes terminal rows
    (one per snapshot run), so it has no superseded rows to collapse.

    Absent tables are not filtered here — the caller checks existence and skips
    tables that do not exist.
    """
    from dlt_saga.pipelines.native_load.state import (
        LOG_KEY_COLUMNS as NATIVE_KEY,
    )
    from dlt_saga.pipelines.native_load.state import (
        LOG_ORDER_EXPR as NATIVE_ORDER,
    )
    from dlt_saga.pipelines.native_load.state import (
        LOG_ORPHAN_STALE_HOURS as NATIVE_STALE,
    )
    from dlt_saga.pipelines.native_load.state import (
        LOG_ORPHAN_STATUS as NATIVE_ORPHAN_STATUS,
    )
    from dlt_saga.pipelines.native_load.state import (
        LOG_ORPHAN_TIMESTAMP_COLUMN as NATIVE_ORPHAN_TS,
    )
    from dlt_saga.pipelines.native_load.state import (
        LOG_STATUS_COLUMN as NATIVE_STATUS,
    )
    from dlt_saga.pipelines.native_load.state import (
        LOG_TERMINAL_STATUSES as NATIVE_TERMINAL,
    )
    from dlt_saga.project_config import (
        get_execution_plans_table_name,
        get_native_load_log_table_name,
    )
    from dlt_saga.utility.orchestration.execution_plan import (
        PLANS_ABANDONED_STATUS,
        PLANS_KEY_COLUMNS,
        PLANS_NONTERMINAL_STATUSES,
        PLANS_ORDER_EXPR,
        PLANS_STALE_HOURS,
        PLANS_STALE_REASONS,
        PLANS_STATUS_COLUMN,
        PLANS_TERMINAL_STATUSES,
    )

    logs: List[CompactableLog] = []
    for schema in sorted({s for s in pipeline_schemas if s}):
        logs.append(
            CompactableLog(
                schema=schema,
                table=get_native_load_log_table_name(),
                label="native_load log",
                key_columns=list(NATIVE_KEY),
                order_expr=NATIVE_ORDER,
                status_column=NATIVE_STATUS,
                terminal_statuses=list(NATIVE_TERMINAL),
                orphan=OrphanRule(
                    status=NATIVE_ORPHAN_STATUS,
                    timestamp_column=NATIVE_ORPHAN_TS,
                    stale_hours=NATIVE_STALE,
                ),
            )
        )

    if orchestration_schema:
        logs.append(
            CompactableLog(
                schema=orchestration_schema,
                table=get_execution_plans_table_name(),
                label="execution-plan log",
                key_columns=list(PLANS_KEY_COLUMNS),
                order_expr=PLANS_ORDER_EXPR,
                status_column=PLANS_STATUS_COLUMN,
                terminal_statuses=list(PLANS_TERMINAL_STATUSES),
                reconcile=TaskReconcile(
                    nonterminal_statuses=list(PLANS_NONTERMINAL_STATUSES),
                    abandoned_status=PLANS_ABANDONED_STATUS,
                    stale_hours=PLANS_STALE_HOURS,
                    reasons=dict(PLANS_STALE_REASONS),
                ),
            )
        )

    return logs


def clustering_state(current: Optional[List[str]], desired: List[str]) -> str:
    """Classify a table's clustering against the desired columns.

    Returns ``"absent"`` (table does not exist), ``"ok"`` (clustering matches),
    or ``"drift"`` (clustering differs and should be reconciled). Comparison is
    case-insensitive and order-sensitive (clustering order is significant).
    """
    if current is None:
        return "absent"
    if [c.lower() for c in current] == [c.lower() for c in desired]:
        return "ok"
    return "drift"
