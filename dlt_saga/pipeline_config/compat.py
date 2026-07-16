"""Backwards-compatible aliases for renamed config keys.

This module normalizes legacy user-facing config keys to their current
names as soon as YAML is parsed, so the hierarchical ``+key:`` merge in
:class:`FilePipelineConfig` and every downstream consumer only ever see
the canonical name.

Current aliases
---------------

- ``dataset_access`` → ``schema_access`` (and ``+dataset_access`` →
  ``+schema_access``). The legacy name leaked from BigQuery's REST API
  vocabulary; the framework standardised on ``schema`` for the
  destination-agnostic concept.
- ``historize.output_schema`` → ``historize.schema_name``,
  ``historize.output_table`` → ``historize.table_name``,
  ``historize.output_table_suffix`` → ``historize.table_suffix``. The
  historize placement keys were unified with the ingest layer's
  ``schema_name`` / ``table_name`` vocabulary; the ``output_`` prefix is
  dropped. (These keys only ever appear under ``historize:``, so the
  recursive rewrite is unambiguous.)

All aliases are read-time only — projects may keep the legacy names in their
YAML indefinitely, and the rename is silent (no deprecation warning) so
upgrades don't add log noise to existing setups.
"""

import logging
from typing import Any, Dict, List, Tuple

logger = logging.getLogger(__name__)


# (legacy_key, canonical_key) pairs applied recursively to any dict loaded
# from a saga_project.yml / pipeline config / profile target. The bare key
# and its ``+``-prefixed inherit-form are both rewritten so the
# hierarchical merge composes correctly when a project mixes old and new
# names across levels.
_ALIASES = (
    ("dataset_access", "schema_access"),
    ("+dataset_access", "+schema_access"),
    ("output_schema", "schema_name"),
    ("+output_schema", "+schema_name"),
    ("output_table", "table_name"),
    ("+output_table", "+table_name"),
    ("output_table_suffix", "table_suffix"),
    ("+output_table_suffix", "+table_suffix"),
)


def normalize_config_aliases(node: Any) -> Any:
    """Rewrite legacy keys to their canonical names, in place, recursively.

    Accepts any value but only mutates dicts and walks their values. Lists
    are walked element-wise so nested config blocks under list items
    (uncommon, but supported) get the same treatment.

    Returns the (possibly mutated) input for fluent use.
    """
    if isinstance(node, dict):
        for legacy, canonical in _ALIASES:
            if legacy in node:
                if canonical in node:
                    # Both forms present — canonical wins, legacy dropped.
                    # Surface the conflict so it's not silent.
                    logger.warning(
                        "Config has both %r and %r set; %r takes precedence "
                        "and the legacy %r entry is ignored.",
                        legacy,
                        canonical,
                        canonical,
                        legacy,
                    )
                    node.pop(legacy)
                else:
                    node[canonical] = node.pop(legacy)

        for value in node.values():
            normalize_config_aliases(value)

    elif isinstance(node, list):
        for item in node:
            normalize_config_aliases(item)

    return node


def find_legacy_keys(node: Any) -> List[Tuple[str, str]]:
    """Return ``(legacy_key, canonical_key)`` pairs present in a **raw** config tree.

    The mirror of :func:`normalize_config_aliases` — it walks the same way but
    *reports* instead of rewriting. Because normalization runs at load time,
    a loaded ``PipelineConfig`` no longer carries legacy keys; callers that want
    to advise on deprecated keys (``saga doctor``) must scan the un-normalized
    YAML and hand it here.

    Deduplicated and sorted by the legacy key. The ``+``-prefixed inherit forms
    are reported verbatim (e.g. ``+dataset_access`` → ``+schema_access``).
    """
    alias_map = dict(_ALIASES)
    found: Dict[str, str] = {}

    def _walk(n: Any) -> None:
        if isinstance(n, dict):
            for key, value in n.items():
                if key in alias_map:
                    found[key] = alias_map[key]
                _walk(value)
        elif isinstance(n, list):
            for item in n:
                _walk(item)

    _walk(node)
    return sorted(found.items())
