"""Detect multiple pipelines resolving to the same destination table.

dlt-saga's default naming scheme *structurally* guarantees a unique
``(schema, table)`` per pipeline: in prod each pipeline group gets its own
schema (``dlt_<group>``) with unprefixed table names; in dev everything shares
one schema but table names are group-prefixed. So two default-named pipelines
cannot collide.

Collisions only arise from **overrides** — an explicit ``schema_name:`` in a
pipeline's YAML, or a custom ``naming_module`` whose ``generate_schema_name`` /
``generate_table_name`` return the same value for several configs. When two
pipelines write to one table they violate the one-config-one-table invariant:
they race on table creation and on delete/merge, and interleave each other's
rows (with N pipelines feeding one table, each run rewrites what the others
just wrote). This module surfaces such collisions from the already-resolved
``PipelineConfig`` targets — before any run touches the warehouse.
"""

import logging
from collections import defaultdict
from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple

from dlt_saga.pipeline_config import PipelineConfig
from dlt_saga.utility.naming import normalize_identifier

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
    table: str
    pipelines: List[str]  # colliding pipeline names, sorted


def _target_key(schema: str, table: str) -> Tuple[str, str]:
    """Key a target by the identifiers as they actually land in the warehouse.

    The table name is normalized the way dlt normalizes table identifiers
    (``MyTable`` and ``my_table`` become one physical table), so configs that
    differ only in casing/style still collide. The schema is compared as-is:
    it's already constrained to ``^[a-zA-Z0-9_]+$`` and an explicit
    ``schema_name:`` is taken literally as the dataset name.
    """
    return (schema or "", normalize_identifier(table))


def detect_target_collisions(
    configs: List[PipelineConfig],
    *,
    check_ingest: bool = True,
    check_historize: bool = True,
) -> List[TargetCollision]:
    """Return the destination tables that more than one config resolves to.

    Each config contributes to a layer only if it's enabled for that layer, so
    passing both flags is safe for a mixed selection: an ingest-only config is
    never checked for a historize collision and vice-versa.

    Historize-target resolution is best-effort per config — a config whose
    historize target can't be resolved (a genuine misconfiguration) is skipped
    here and left for the run itself to surface, rather than masking the
    collision guard behind an unrelated error.

    Args:
        configs: The selected pipeline configs (schema_name/table_name already
            resolved by the config layer, including overrides).
        check_ingest: Check the ingest/write target of ingest-enabled configs.
        check_historize: Check the resolved historized target of
            historize-enabled configs.

    Returns:
        Collisions sorted by ``(layer, schema, table)``; empty when none.
    """
    ingest_map: Dict[Tuple[str, str], List[str]] = defaultdict(list)
    historize_map: Dict[Tuple[str, str], List[str]] = defaultdict(list)

    for config in configs:
        if (
            check_ingest
            and config.ingest_enabled
            and config.schema_name
            and config.table_name
        ):
            key = _target_key(config.schema_name, config.table_name)
            ingest_map[key].append(config.pipeline_name)

        if check_historize and config.historize_enabled:
            target = _resolve_historize_target_key(config)
            if target is not None:
                historize_map[target].append(config.pipeline_name)

    collisions: List[TargetCollision] = []
    collisions.extend(_collisions_from_map("ingest", ingest_map))
    collisions.extend(_collisions_from_map("historize", historize_map))
    collisions.sort(key=lambda c: (c.layer, c.schema, c.table))
    return collisions


def _resolve_historize_target_key(config: PipelineConfig) -> Optional[Tuple[str, str]]:
    """Resolve a config's historized ``(schema, table)`` collision key, or None."""
    # Imported lazily: the historize factory pulls in the runner/destination
    # stack, which need not load for an ingest-only run.
    from dlt_saga.historize.factory import resolve_historize_target

    try:
        _, schema, table = resolve_historize_target(config)
    except Exception as exc:  # best-effort — don't block the guard on a bad config
        logger.debug(
            "Could not resolve historize target for %s (skipping collision check "
            "for it): %s",
            config.pipeline_name,
            exc,
        )
        return None
    return _target_key(schema, table)


def _collisions_from_map(
    layer: str, target_map: Dict[Tuple[str, str], List[str]]
) -> List[TargetCollision]:
    """Turn a target→pipelines map into collisions (targets with >1 pipeline)."""
    collisions = []
    for (schema, table), names in target_map.items():
        distinct = sorted(set(names))
        if len(distinct) > 1:
            collisions.append(
                TargetCollision(
                    layer=layer, schema=schema, table=table, pipelines=distinct
                )
            )
    return collisions


def format_collisions(collisions: List[TargetCollision]) -> str:
    """Render collisions into an actionable configuration-error message."""
    lines = [
        "Multiple pipelines resolve to the same destination table. A pipeline "
        "config must map to exactly one table — two pipelines writing to one "
        "table race on create and delete and duplicate each other's rows. Give "
        "each pipeline a distinct target (via schema_name / table_name / a "
        "naming_module), or merge them into a single pipeline that reads from "
        "all sources.",
        "",
    ]
    for collision in collisions:
        target = ".".join(p for p in (collision.schema, collision.table) if p)
        lines.append(f"  {target} ({collision.layer} target)")
        lines.append(f"    ← {', '.join(collision.pipelines)}")
        lines.append("")
    return "\n".join(lines).rstrip()


def check_target_collisions(
    configs: List[PipelineConfig],
    *,
    check_ingest: bool = True,
    check_historize: bool = True,
) -> None:
    """Raise ``ValueError`` if any two configs resolve to the same target table.

    A configuration error (raised before any run touches the warehouse), so
    callers surface it as a user-facing message without a traceback.
    """
    collisions = detect_target_collisions(
        configs, check_ingest=check_ingest, check_historize=check_historize
    )
    if collisions:
        raise TargetCollisionError(format_collisions(collisions))
