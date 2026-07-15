"""Detect multiple pipelines resolving to the same destination table.

dlt-saga's default naming scheme *structurally* guarantees a unique
``(schema, table)`` per pipeline: in prod each pipeline group gets its own
schema (``dlt_<group>``) with unprefixed table names; in dev everything shares
one schema but table names are group-prefixed. So two default-named pipelines
cannot collide.

Collisions only arise from **overrides** — an explicit ``schema_name:`` in a
pipeline's YAML, or a custom ``naming_module`` whose ``generate_schema_name`` /
``generate_table_name`` return the same value for several configs. Two enabled
configs resolving to one table is corruption *by construction*, independent of
schedule or which pipelines a given run selects: each is an independent writer
that overwrites or interleaves the other's data. An hourly pipeline pointed at a
monthly pipeline's table clobbers it every hour and the monthly run stomps back
once a month — they never share an execution and the table is still wrong. So
the invariant is one enabled config ↔ one destination table, full stop.

**Scope is project-wide.** Because the harm doesn't depend on co-execution, the
guard resolves *every enabled config in the project* to its target and finds the
groups where more than one lands on the same table — not just the configs a
given run selected. The runtime guard then refuses to run when any *selected*
pipeline falls in such a group (naming its co-claimant even if that one isn't in
this run), so deploying a pipeline onto an existing table's identity is caught
even when you run the new pipeline alone. ``saga doctor`` reports every group in
the project.

**Which environments are checked.** dev and prod render names differently
(per-group ``dlt_<group>`` schemas + unprefixed tables in prod; one shared dev
schema + group-prefixed tables in dev), so a collision can be real in one and
invisible in the other. Detection resolves targets through the naming layer
(dispatching to a custom ``naming_module`` or the defaults — never
reconstructing default-only internals):

- **Runtime guard** resolves in the **environment the run targets**. That's the
  environment whose tables the run is about to write, so it's the one whose
  collisions can actually corrupt data now.
- **``saga doctor``** resolves in the **current environment and prod**, unioned.
  Prod is the deploy floor — a prod collision is flagged even from a dev doctor
  run, so it can't slip through and surface first in production. The current
  environment adds dev-only collisions a custom ``naming_module`` can create
  (default naming can't collide dev-only: dev tables are group-prefixed and
  path-unique).

When multiple environments are checked, a config pair is a collision if it
collides in *any* of them; each distinct collision is reported once, carrying
the environment(s) it manifests in so the message can say where.
"""

import logging
from dataclasses import dataclass, replace
from typing import Dict, List, Optional, Tuple

from dlt_saga.pipeline_config import ConfigSource, PipelineConfig
from dlt_saga.utility.naming import get_environment, normalize_identifier

logger = logging.getLogger(__name__)


class TargetCollisionError(ValueError):
    """Raised when two or more pipelines resolve to the same destination table.

    A configuration error (subclasses ``ValueError``): callers render it as a
    user-facing message without a traceback.
    """


@dataclass(frozen=True)
class TargetCollision:
    """A single destination table that more than one pipeline resolves to."""

    layer: str  # "ingest" or "historize"
    schema: str
    normalized_table: str  # dlt-normalized physical table name (the group key)
    raw_tables: Tuple[str, ...]  # distinct pre-normalization names, sorted
    pipelines: List[str]  # colliding pipeline names, sorted
    environments: Tuple[str, ...]  # environments this collision manifests in, sorted

    @property
    def env_label(self) -> str:
        """Environments this collision manifests in, e.g. ``"prod"`` / ``"dev+prod"``."""
        return "+".join(self.environments)

    @property
    def display_table(self) -> str:
        """Table name to show the user.

        A single raw name → show it verbatim (what the config actually
        produces). When distinct raw names snake-case to one physical table
        (e.g. ``MyBrands`` and ``my_brands``), show the normalized name plus the
        raw names that fold into it, so the user can see *why* they collide.
        """
        if len(self.raw_tables) == 1:
            return self.raw_tables[0]
        return f"{self.normalized_table} (from {', '.join(self.raw_tables)})"


@dataclass
class _TargetGroup:
    """Accumulates every pipeline that resolves to one physical target."""

    raw_tables: List[str]  # distinct raw table names reaching this target
    pipelines: List[str]  # pipeline names reaching this target


def detect_target_collisions(
    configs: List[PipelineConfig],
    source: ConfigSource,
    *,
    environments: List[str],
    check_ingest: bool = True,
    check_historize: bool = True,
) -> List[TargetCollision]:
    """Return the destination tables that more than one config resolves to.

    Resolves each config's target in every environment in ``environments`` (see
    the module docstring) and unions the results: a config pair is a collision
    if it collides in any of them, reported once. Each config contributes to a
    layer only if it's enabled for that layer, so passing both flags is safe for
    a mixed selection: an ingest-only config is never checked for a historize
    collision and vice-versa.

    Resolution is best-effort per config — a config whose target can't be
    resolved (a config source without :meth:`resolve_ingest_target`, a missing
    ``config_path``, an unresolvable environment, or a genuine historize
    misconfiguration) is skipped rather than crashing the guard, and left for
    the run itself to surface.

    Args:
        configs: The configs to check against each other (project-wide for the
            runtime guard and the default doctor run).
        source: The config source used to resolve environment-pinned names.
        environments: Environments to resolve targets in and union over.
        check_ingest: Check the ingest/write target of ingest-enabled configs.
        check_historize: Check the resolved historized target of
            historize-enabled configs.

    Returns:
        Collisions sorted by ``(layer, schema, normalized_table)``; empty when
        none.
    """
    # Group across environments by the colliding identity — same layer, same set
    # of pipelines — so a pair that collides in both dev and prod is reported
    # once, carrying every environment it manifests in. Names shown are from the
    # environment that surfaced it first (iteration order of ``environments``).
    detected: Dict[Tuple[str, Tuple[str, ...]], TargetCollision] = {}
    env_lists: Dict[Tuple[str, Tuple[str, ...]], List[str]] = {}
    order: List[Tuple[str, Tuple[str, ...]]] = []
    for environment in environments:
        ingest_groups, historize_groups = _group_targets(
            configs, source, environment, check_ingest, check_historize
        )
        env_collisions = _collisions_from_groups(
            "ingest", ingest_groups, environment
        ) + _collisions_from_groups("historize", historize_groups, environment)
        for collision in env_collisions:
            key = (collision.layer, tuple(collision.pipelines))
            if key not in detected:
                detected[key] = collision
                env_lists[key] = [environment]
                order.append(key)
            elif environment not in env_lists[key]:
                env_lists[key].append(environment)

    collisions = [
        replace(detected[key], environments=tuple(sorted(env_lists[key])))
        for key in order
    ]
    collisions.sort(key=lambda c: (c.layer, c.schema, c.normalized_table))
    return collisions


def _group_targets(
    configs: List[PipelineConfig],
    source: ConfigSource,
    environment: str,
    check_ingest: bool,
    check_historize: bool,
) -> Tuple[Dict[Tuple[str, str], _TargetGroup], Dict[Tuple[str, str], _TargetGroup]]:
    """Group configs by resolved ingest/historize target for one environment."""
    ingest_groups: Dict[Tuple[str, str], _TargetGroup] = {}
    historize_groups: Dict[Tuple[str, str], _TargetGroup] = {}

    for config in configs:
        ingest = _resolve_ingest_target(config, source, environment)
        if ingest is None:
            continue
        schema, table = ingest

        if check_ingest and config.ingest_enabled and schema and table:
            _record(ingest_groups, schema, table, config.pipeline_name)

        if check_historize and config.historize_enabled:
            hist = _resolve_historize_target_names(config, schema, table, environment)
            if hist is not None and hist[0] and hist[1]:
                _record(historize_groups, hist[0], hist[1], config.pipeline_name)

    return ingest_groups, historize_groups


def _resolve_ingest_target(
    config: PipelineConfig, source: ConfigSource, environment: str
) -> Optional[Tuple[str, str]]:
    """Resolve a config's ingest ``(schema, table)`` target for an environment.

    Reads the config's ``config_path`` and any explicit ``schema_name:``
    override and hands both to the source's environment-parametric resolver.
    Best-effort: returns None (skipping the config) when the source can't
    resolve it.
    """
    resolve = getattr(source, "resolve_ingest_target", None)
    config_path = config.config_dict.get("config_path")
    if resolve is None or not config_path:
        logger.debug(
            "Cannot resolve ingest target for %s (source lacks "
            "resolve_ingest_target or config has no config_path); skipping "
            "collision check for it",
            config.pipeline_name,
        )
        return None

    schema_override = config.config_dict.get("schema_name")
    try:
        schema, table = resolve(
            config_path, schema_override=schema_override, environment=environment
        )
    except Exception as exc:  # best-effort — don't block the guard on a bad config
        logger.debug(
            "Could not resolve ingest target for %s in %s (skipping): %s",
            config.pipeline_name,
            environment,
            exc,
        )
        return None
    return (schema, table)


def _resolve_historize_target_names(
    config: PipelineConfig, ingest_schema: str, ingest_table: str, environment: str
) -> Optional[Tuple[str, str]]:
    """Resolve a config's historized ``(schema, table)`` target, or None.

    The historized target derives from the config's ingest target in the same
    environment, so those are threaded in as the source schema/table.
    """
    # Imported lazily: the historize factory pulls in the runner/destination
    # stack, which need not load for an ingest-only run.
    from dlt_saga.historize.factory import resolve_historize_target

    try:
        _, schema, table = resolve_historize_target(
            config,
            environment=environment,
            source_schema=ingest_schema,
            source_table=ingest_table,
        )
    except Exception as exc:  # best-effort — don't block the guard on a bad config
        logger.debug(
            "Could not resolve historize target for %s in %s (skipping "
            "collision check for it): %s",
            config.pipeline_name,
            environment,
            exc,
        )
        return None
    return (schema, table)


def _record(
    groups: Dict[Tuple[str, str], _TargetGroup],
    schema: str,
    table: str,
    pipeline_name: str,
) -> None:
    """Record that ``pipeline_name`` resolves to ``(schema, table)``.

    Keyed by the identifiers as they actually land in the warehouse: the table
    is normalized the way dlt normalizes table identifiers (``MyTable`` and
    ``my_table`` become one physical table). The raw name is retained so the
    message can show what each config actually produced.
    """
    key = (schema or "", normalize_identifier(table))
    group = groups.get(key)
    if group is None:
        group = _TargetGroup(raw_tables=[], pipelines=[])
        groups[key] = group
    if table not in group.raw_tables:
        group.raw_tables.append(table)
    group.pipelines.append(pipeline_name)


def _collisions_from_groups(
    layer: str, groups: Dict[Tuple[str, str], _TargetGroup], environment: str
) -> List[TargetCollision]:
    """Turn accumulated target groups into collisions (targets with >1 pipeline)."""
    collisions = []
    for (schema, normalized_table), group in groups.items():
        distinct = sorted(set(group.pipelines))
        if len(distinct) > 1:
            collisions.append(
                TargetCollision(
                    layer=layer,
                    schema=schema,
                    normalized_table=normalized_table,
                    raw_tables=tuple(sorted(set(group.raw_tables))),
                    pipelines=distinct,
                    environments=(environment,),
                )
            )
    return collisions


def format_collisions(collisions: List[TargetCollision]) -> str:
    """Render collisions into an actionable configuration-error message."""
    lines = [
        "Multiple pipelines resolve to the same destination table. A pipeline "
        "config must map to exactly one table — two pipelines pointed at one "
        "table are independent writers that overwrite and interleave each "
        "other's data (whether or not they ever run together). Give each "
        "pipeline a distinct target (via schema_name / table_name / a "
        "naming_module), or merge them into a single pipeline that reads from "
        "all sources.",
        "",
    ]
    for collision in collisions:
        target = ".".join(p for p in (collision.schema, collision.display_table) if p)
        lines.append(f"  {target} ({collision.layer} target, {collision.env_label})")
        lines.append(f"    ← {', '.join(collision.pipelines)}")
        lines.append("")
    return "\n".join(lines).rstrip()


def collisions_for_selection(
    selected_configs: List[PipelineConfig],
    source: ConfigSource,
    *,
    environments: List[str],
    check_ingest: bool = True,
    check_historize: bool = True,
) -> List[TargetCollision]:
    """Project-wide collisions that touch ``selected_configs``.

    Detection is project-wide (the invariant is — see the module docstring): it
    resolves every enabled config in the project, then returns only the
    collisions whose pipelines intersect the selection. So a selected pipeline
    colliding with a *non-selected* enabled one is surfaced (naming both), while
    an unrelated latent collision elsewhere isn't. With the whole project
    selected (the default ``saga doctor`` run) every collision is returned.

    Shared by the run-time guard (:func:`check_target_collisions`, one
    environment) and ``saga doctor`` (dev ∪ prod).

    Args:
        selected_configs: The configs in scope (this run's / this command's
            selection).
        source: The config source, used both to resolve names and to discover
            the project-wide set of enabled configs.
        environments: Environments to resolve targets in and union over.
        check_ingest: Check the ingest layer.
        check_historize: Check the historize layer.
    """
    project_configs = _discover_enabled_configs(source)
    if project_configs is None:
        # Discovery failed — fall back to checking the selection against itself
        # rather than skipping the check entirely.
        project_configs = list(selected_configs)

    collisions = detect_target_collisions(
        project_configs,
        source,
        environments=environments,
        check_ingest=check_ingest,
        check_historize=check_historize,
    )

    selected_names = {config.pipeline_name for config in selected_configs}
    return [c for c in collisions if selected_names.intersection(c.pipelines)]


def check_target_collisions(
    configs: List[PipelineConfig],
    source: ConfigSource,
    *,
    check_ingest: bool = True,
    check_historize: bool = True,
) -> None:
    """Raise if any *selected* pipeline shares a target with any enabled config.

    Detects project-wide (:func:`collisions_for_selection`) in the environment
    the run targets, and refuses the run when a collision touches ``configs``.
    A configuration error (raised before any run touches the warehouse), so
    callers surface it as a user-facing message without a traceback.

    Args:
        configs: This run's selected configs.
        source: The config source (resolves names and discovers the project).
        check_ingest: Guard the ingest layer.
        check_historize: Guard the historize layer.
    """
    relevant = collisions_for_selection(
        configs,
        source,
        environments=[get_environment()],
        check_ingest=check_ingest,
        check_historize=check_historize,
    )
    if relevant:
        raise TargetCollisionError(format_collisions(relevant))


def _discover_enabled_configs(
    source: ConfigSource,
) -> Optional[List[PipelineConfig]]:
    """Flatten the source's enabled configs, or None if discovery fails.

    Discovery is memoized on the source, so this reuses the walk the run already
    performed rather than paying for a second one.
    """
    try:
        enabled, _ = source.discover()
    except Exception as exc:  # best-effort — don't block the guard on discovery
        logger.debug("Could not discover project configs for collision check: %s", exc)
        return None
    return [config for group_configs in enabled.values() for config in group_configs]
