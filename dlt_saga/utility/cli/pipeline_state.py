"""Warehouse-state backing for the ``state:`` selectors.

Every other selector (``tag:``, ``group:``, names, globs) answers from the
config files alone. ``state:new`` and ``state:failed`` instead ask the
destination what has already happened, so this module owns the two reads that
answer them and caches both for the lifetime of one selection:

``state:new``
    The pipeline's target table does not exist yet. Resolved per layer — the
    ingest target for the ingest layer, the historized target for the historize
    layer — from one table listing per distinct schema, so the cost is the
    number of schemas in the selection, not the number of pipelines.

``state:failed``
    The most recent outcome recorded for the pipeline in the execution-plan log
    is a failure. Layer-agnostic: a local ``saga run`` merges its ingest and
    historize phases into one row per pipeline, so the log cannot attribute a
    failure to a layer.

Newness is deliberately read from **table existence** rather than the
``_saga_load_info`` log: a load that writes zero rows records nothing there
(``BasePipeline._save_load_info`` iterates the per-table row counts), and a
``historize``-only pipeline never writes to it at all — both would look
permanently new. Both reads are also scoped to the target being run, so a
pipeline can be new in dev and not in prod.

Errors here must never be swallowed into an empty answer. An empty table
listing means "nothing built yet", which selects *every* pipeline in that
schema, so a denied permission read as an empty listing would silently turn
``state:new`` into a full re-ingest. :meth:`Destination.list_tables` raises for
anything but a missing schema, and this module lets that propagate.
"""

import logging
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Sequence, Set, Tuple

from dlt_saga.pipeline_config.base_config import PipelineConfig
from dlt_saga.utility.naming import normalize_identifier

logger = logging.getLogger(__name__)

STATE_PREFIX = "state:"

#: The closed vocabulary of ``state:`` keywords.
STATE_KEYWORDS = ("new", "failed")

#: Layers a resolver can be scoped to. ``any`` asks about every layer a config
#: is enabled for, which is what a command spanning both layers (``saga list``,
#: ``saga report``) wants.
LAYERS = ("ingest", "historize", "any")

#: Execution-plan statuses that count as a failed outcome. ``abandoned`` is a
#: crashed run that ``saga maintenance`` relabelled — no data landed, so it is
#: as much a retry candidate as a reported failure. Non-terminal statuses
#: (``pending``, ``running``) are excluded: a concurrent run is in flight, not
#: failed.
FAILED_STATUSES = frozenset({"failed", "abandoned"})

#: How far back ``state:failed`` reads the execution-plan log. Bounds the scan
#: (the log is partitioned by ``log_timestamp``) and keeps a long-abandoned
#: pipeline from being retried forever.
FAILED_LOOKBACK_DAYS = 30


def layer_for_resource_type(resource_type: str) -> str:
    """Map a ``--resource-type`` to the layer ``state:new`` judges newness in.

    ``all`` spans both layers, so it becomes ``any``; ``ingest`` and
    ``historize`` name their layer directly.
    """
    return "any" if resource_type == "all" else resource_type


class StateSelectorError(ValueError):
    """A ``state:`` selector that cannot be honoured as written.

    A configuration error (subclasses ``ValueError``), so callers render it as
    a message without a traceback.
    """


def selection_needs_state(select: Optional[Sequence[str]]) -> bool:
    """Return True if any selector in ``select`` is a ``state:`` selector.

    Called before any warehouse work so a selection without ``state:`` costs
    exactly what it always did — no connection, no query.
    """
    if not select:
        return False
    for group in select:
        for token in group.replace(",", " ").split():
            if token.strip().startswith(STATE_PREFIX):
                return True
    return False


@dataclass
class _StateCache:
    """The reads backing the state selectors, shared across layer scopes.

    A command spanning both layers (``saga run``) selects once per layer, and
    the answers differ only in which target is looked up — the underlying
    listings are the same. Holding them here lets :meth:`
    PipelineStateResolver.scoped` hand out a differently-scoped resolver
    without re-reading anything.
    """

    #: Physical table names per schema, normalized the way dlt normalizes
    #: identifiers so a config-declared name matches what actually landed.
    tables: Dict[str, Set[str]] = field(default_factory=dict)
    #: ``(pipeline_group, table)`` of pipelines whose last run failed; None
    #: until the execution-plan log has been read.
    failed: Optional[Set[Tuple[str, str]]] = None


class PipelineStateResolver:
    """Answers ``state:`` questions for one selection against one target.

    Args:
        destination: Connected destination used for the metadata reads.
        layer: Layer newness is judged in — ``"ingest"``, ``"historize"``, or
            ``"any"`` (new in any layer the config is enabled for).
        cache: Shared read cache; a fresh one per resolver by default.
    """

    def __init__(
        self, destination, layer: str = "any", cache: Optional[_StateCache] = None
    ) -> None:
        if layer not in LAYERS:
            raise ValueError(f"layer must be one of {LAYERS}, got '{layer}'")
        self.destination = destination
        self.layer = layer
        self._cache = cache if cache is not None else _StateCache()

    def scoped(self, layer: str) -> "PipelineStateResolver":
        """Return a resolver for ``layer`` that shares this one's reads."""
        return PipelineStateResolver(self.destination, layer=layer, cache=self._cache)

    # -- state:new ------------------------------------------------------

    def is_new(self, config: PipelineConfig, layer: Optional[str] = None) -> bool:
        """Return True if this config's target table doesn't exist yet.

        Args:
            config: The config to judge.
            layer: Overrides the resolver's layer for this call.

        Returns:
            True when a checked layer's target is absent. A config whose target
            can't be resolved is reported as *not* new (with a warning) — the
            safe direction, since guessing "new" would add a full load to the
            run.
        """
        for target_layer in self._layers_for(config, layer or self.layer):
            target = self._target_for(config, target_layer)
            if target is None:
                continue
            schema, table = target
            if not self._table_exists(schema, table):
                logger.debug(
                    "%s is new: %s target %s.%s does not exist",
                    config.pipeline_name,
                    target_layer,
                    schema,
                    table,
                )
                return True
        return False

    def _layers_for(self, config: PipelineConfig, layer: str) -> List[str]:
        """Layers to judge newness in for this config."""
        if layer != "any":
            return [layer]
        layers = []
        if config.ingest_enabled:
            layers.append("ingest")
        if config.historize_enabled:
            layers.append("historize")
        return layers

    def _target_for(
        self, config: PipelineConfig, layer: str
    ) -> Optional[Tuple[str, str]]:
        """Resolve a config's ``(schema, table)`` target for one layer."""
        if layer == "ingest":
            schema, table = config.schema_name, config.table_name
        else:
            # Imported lazily: the historize factory pulls in the runner stack,
            # which need not load for an ingest-only selection.
            from dlt_saga.historize.factory import resolve_historize_target

            try:
                _, schema, table = resolve_historize_target(config)
            except Exception as exc:
                logger.warning(
                    "Could not resolve the historized target for %s, so "
                    "'state:new' cannot judge it; excluding it from the "
                    "selection: %s",
                    config.pipeline_name,
                    exc,
                )
                return None
        if not schema or not table:
            logger.warning(
                "%s has no resolved %s target (schema=%r, table=%r), so "
                "'state:new' cannot judge it; excluding it from the selection",
                config.pipeline_name,
                layer,
                schema,
                table,
            )
            return None
        return schema, table

    def _table_exists(self, schema: str, table: str) -> bool:
        """Return True if ``table`` is present in ``schema``.

        Listings are cached per schema: one metadata read answers every
        pipeline landing in that schema.
        """
        tables = self._cache.tables.get(schema)
        if tables is None:
            listed = self.destination.list_tables(schema)
            tables = {normalize_identifier(name) for name in listed}
            self._cache.tables[schema] = tables
            logger.debug("Listed %d table(s) in %s", len(tables), schema)
        return normalize_identifier(table) in tables

    # -- state:failed ---------------------------------------------------

    def last_run_failed(self, config: PipelineConfig) -> bool:
        """Return True if this pipeline's most recent recorded run failed."""
        return self._key(config.pipeline_group, config.table_name) in self._failures()

    def _failures(self) -> Set[Tuple[str, str]]:
        """Pipelines whose latest execution-plan row is a failed status."""
        if self._cache.failed is None:
            self._cache.failed = self._query_failures()
        return self._cache.failed

    def _query_failures(self) -> Set[Tuple[str, str]]:
        from dlt_saga.project_config import get_execution_plans_table_name
        from dlt_saga.utility.naming import get_execution_plan_schema
        from dlt_saga.utility.sql import looks_like_missing_table

        schema = get_execution_plan_schema()
        table_id = self.destination.get_full_table_id(
            schema, get_execution_plans_table_name()
        )
        # The log is append-only: a task's status history is a row per
        # transition, so the latest row per pipeline is its current outcome.
        # Keyed on (pipeline_type, table_name) rather than the stored
        # pipeline_identifier, which is a config path and so differs between a
        # local checkout and a worker container.
        cutoff = self.destination.timestamp_n_days_ago(FAILED_LOOKBACK_DAYS)
        sql = f"""
            SELECT pipeline_type, table_name, status
            FROM {table_id}
            WHERE log_timestamp >= {cutoff}
            QUALIFY ROW_NUMBER() OVER (
                PARTITION BY pipeline_type, table_name
                ORDER BY log_timestamp DESC
            ) = 1
        """
        try:
            rows = list(self.destination.execute_sql(sql, schema))
        except Exception as exc:
            if looks_like_missing_table(exc):
                # No run has ever been recorded for this target — nothing has
                # failed, as opposed to a read we couldn't perform.
                logger.debug(
                    "No execution-plan log in %s; 'state:failed' matches nothing",
                    schema,
                )
                return set()
            raise
        failures = {
            self._key(row.pipeline_type, row.table_name)
            for row in rows
            if (row.status or "").lower() in FAILED_STATUSES
        }
        logger.debug(
            "%d pipeline(s) have a failed last run in the past %d days",
            len(failures),
            FAILED_LOOKBACK_DAYS,
        )
        return failures

    @staticmethod
    def _key(pipeline_group: str, table_name: str) -> Tuple[str, str]:
        return (pipeline_group or "", normalize_identifier(table_name or ""))


def build_state_resolver(
    select: Optional[Sequence[str]], layer: Optional[str]
) -> Optional[PipelineStateResolver]:
    """Build a resolver for ``select``, or None when it needs no warehouse state.

    Args:
        select: The selectors about to be applied.
        layer: Layer to judge newness in, or None for a command that has no
            warehouse access (``saga validate``) — a ``state:`` selector there
            is an error rather than a silent no-match.

    Returns:
        A connected :class:`PipelineStateResolver`, or None when no ``state:``
        selector is present.

    Raises:
        StateSelectorError: A ``state:`` selector was used by a command that
            cannot read warehouse state.
    """
    if not selection_needs_state(select):
        return None
    if layer is None:
        raise StateSelectorError(
            "'state:' selectors read warehouse state, which this command does "
            "not connect to. Use them with a command that runs against a "
            "target (list, ingest, historize, run, report)."
        )

    from dlt_saga.destinations.factory import DestinationFactory
    from dlt_saga.utility.cli.context import get_execution_context

    context = get_execution_context()
    # schema_name is left empty: every read here is fully qualified from the
    # schema it targets, so no pipeline config is needed to seed the client.
    destination = DestinationFactory.create_from_context(
        context.get_destination_type(), context, {"schema_name": ""}
    )
    destination.connect()
    return PipelineStateResolver(destination, layer=layer)
