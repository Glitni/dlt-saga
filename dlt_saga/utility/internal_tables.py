"""Registry of saga's internal bookkeeping tables and their desired layout.

Single source of truth for which internal tables exist, where they live, and how
they should be clustered — shared between ``saga doctor``'s read-only clustering
check and ``saga maintenance``'s clustering reconcile. The desired cluster
columns are imported from the same constants the create-time DDL uses, so a
reconciled table matches a freshly created one.
"""

from dataclasses import dataclass
from typing import Iterable, List, Optional


@dataclass(frozen=True)
class InternalLogTable:
    """An internal bookkeeping table and its desired physical clustering."""

    schema: str
    table: str
    cluster_columns: List[str]
    label: str  # human-facing, e.g. "native_load log"


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
