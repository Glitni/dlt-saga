"""Teardown ("destroy") helpers for removing a pipeline's warehouse footprint.

``saga destroy`` removes the tables and state a pipeline created, without a
subsequent reload — the counterpart to ``--full-refresh`` (drop + rebuild) for
decommissioning a config before it is deleted or disabled, so no stale tables
or state are left behind.

Two independently-gated layers, both driven by the same principle: **drop only
what state proves this pipeline created.** A table's existence at a config's
derived name is not proof of ownership (the name is auto-derived and could
coincide with a pre-existing or another pipeline's table), so neither layer
trusts the derived name — each reads the *recorded* target from saga's own
state:

* **Ingest** — the ``table_name``(s) recorded in ``_saga_load_info`` for this
  ``pipeline_name``. Dropped (with dlt state, staging, and load-info) via the
  destination's ``reset_destination_state`` — the same primitive full-refresh
  uses.
* **Historize** — the ``target_table``(s) recorded in ``_saga_historize_log``
  for this ``pipeline_name``, via ``HistorizeRunner.destroy``.

Because ownership comes from state rather than the current ``write_disposition``,
disposition changes are handled correctly: an ``append+historize`` config later
narrowed to ``append`` still has its historize log entry, so its orphaned
historized table is cleaned up; likewise a config narrowed to ``historize``-only
still has its ingest load-info, so its orphaned ingest table is cleaned up.
"""

import logging
from typing import Any, Dict, Optional

from dlt_saga.pipeline_config import PipelineConfig

logger = logging.getLogger(__name__)


def destroy_ingest_footprint(
    pipeline_config: PipelineConfig,
    dry_run: bool = False,
    log_prefix: str = "",
) -> Dict[str, Any]:
    """Drop the ingest tables this pipeline created, plus its dlt/load state.

    Builds only the destination (never the source pipeline — teardown must not
    depend on source credentials or reachability), reads the owned table(s) from
    ``_saga_load_info``, and drops each via ``reset_destination_state``. A
    pipeline with no load-info records (never successfully ingested, or a
    historize-only config) is a no-op.

    Args:
        pipeline_config: The pipeline configuration to tear down.
        dry_run: When True, report what would be dropped without deleting.
        log_prefix: Optional prefix for log messages (e.g. ``"[1/3]"``).

    Returns the facts of the teardown; the caller composes the user-facing
    summary (one line per pipeline).

    Returns:
        Dict with ``schema``, ``tables`` (bare recorded names), and
        ``table_ids`` (qualified, for logging).
    """
    from dlt_saga.destinations.factory import DestinationFactory
    from dlt_saga.utility.cli.context import get_execution_context

    prefix = f"{log_prefix} " if log_prefix else ""
    context = get_execution_context()
    destination_type = context.get_destination_type()
    schema_name = pipeline_config.schema_name
    pipeline_name = pipeline_config.pipeline_name

    destination = DestinationFactory.create_from_context(
        destination_type,
        context,
        {**pipeline_config.config_dict, "schema_name": schema_name},
    )

    tables = destination.get_ingested_targets(schema_name, pipeline_name)
    table_ids = [destination.get_full_table_id(schema_name, t) for t in tables]

    if not tables:
        logger.debug("%sNo ingest footprint recorded for %s", prefix, pipeline_name)
    elif not dry_run:
        for table in tables:
            # reset_destination_state drops the main + staging table and clears
            # _dlt_pipeline_state / _dlt_version / _saga_load_info for this
            # pipeline — the same teardown full-refresh performs before reload.
            destination.reset_destination_state(
                pipeline_name=pipeline_name, table_name=table
            )

    return {"schema": schema_name, "tables": tables, "table_ids": table_ids}


def destroy_historize_footprint(
    pipeline_config: PipelineConfig,
    dry_run: bool = False,
    log_prefix: str = "",
) -> Optional[Dict[str, Any]]:
    """Drop the historized table(s) this pipeline created, plus its log entries.

    Delegates to ``HistorizeRunner.destroy`` (ownership read from
    ``_saga_historize_log``). Returns ``None`` when the config can't produce a
    historize runner (e.g. no resolvable primary key) — there is no historize
    layer to tear down.

    Args:
        pipeline_config: The pipeline configuration to tear down.
        dry_run: When True, report what would be dropped without deleting.
        log_prefix: Optional prefix for log messages (e.g. ``"[1/3]"``).

    Returns:
        Dict with ``schema``, ``targets``, and ``dropped``, or ``None``.
    """
    from dlt_saga.historize.factory import build_historize_runner

    try:
        runner = build_historize_runner(
            pipeline_config, full_refresh=False, log_prefix=log_prefix
        )
    except ValueError as exc:
        logger.debug(
            "No historize layer to tear down for %s: %s",
            pipeline_config.pipeline_name,
            exc,
        )
        return None
    return runner.destroy(dry_run=dry_run)
