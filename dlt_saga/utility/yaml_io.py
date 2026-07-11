"""YAML config file loading.

Centralizes reading YAML config files as UTF-8 (the encoding the YAML spec
mandates). Reading with the platform default instead — cp1252 on Windows —
corrupts any non-ASCII content (e.g. ``ø`` becomes ``Ã¸``). Pinning the
encoding in one place means every config reader inherits it and new readers
can't reintroduce the bug.

Loading also rejects two classes of malformed input that PyYAML's ``safe_load``
accepts silently: duplicate mapping keys (last-wins, so a typo'd override
silently shadows the intended value) and a top-level value that isn't a mapping
(a stray list or scalar would otherwise flow into the config merge and crash
deep with an opaque error).
"""

from pathlib import Path
from typing import Any, Dict, Union

import yaml


class _UniqueKeySafeLoader(yaml.SafeLoader):
    """SafeLoader that raises on duplicate keys instead of last-wins.

    Overriding ``construct_mapping`` (rather than replacing the mapping-tag
    constructor) keeps PyYAML's native ``construct_yaml_map`` generator, which
    handles recursive/self-referential anchors, and delegates merge resolution
    to the stock implementation.
    """

    def construct_mapping(
        self, node: yaml.MappingNode, deep: bool = False
    ) -> Dict[Any, Any]:
        # Reject duplicate keys among the explicitly-written entries, before
        # merge resolution. A real typo repeats a key in the source mapping; a
        # `<<:` merge key legitimately collides with an explicit key (the
        # explicit one wins per YAML merge semantics) and must not be flagged,
        # so skip merge nodes here.
        seen: set[Any] = set()
        for key_node, _ in node.value:
            if key_node.tag == "tag:yaml.org,2002:merge":
                continue
            key = self.construct_object(key_node, deep=deep)
            if key in seen:
                raise yaml.constructor.ConstructorError(
                    "while constructing a mapping",
                    node.start_mark,
                    f"found duplicate key {key!r}",
                    key_node.start_mark,
                )
            seen.add(key)
        # Stock construct_mapping resolves `<<:` merge keys via flatten_mapping
        # and lets explicit keys override merged ones. Nested mappings route
        # back through this override, so they get the duplicate check too.
        return super().construct_mapping(node, deep=deep)


def load_yaml(path: Union[str, Path]) -> Dict[str, Any]:
    """Read and parse a YAML config file as UTF-8.

    Returns the parsed mapping, or an empty dict for an empty file, so callers
    don't need to guard against ``None``.

    Raises:
        ValueError: on a YAML syntax error, a duplicate mapping key, or a
            top-level value that isn't a mapping — with the offending path.
    """
    with open(path, "r", encoding="utf-8") as f:
        try:
            data = yaml.load(f, Loader=_UniqueKeySafeLoader)
        except yaml.YAMLError as exc:
            raise ValueError(f"Failed to parse YAML file '{path}': {exc}") from exc

    if data is None:
        return {}
    if not isinstance(data, dict):
        raise ValueError(
            f"YAML file '{path}' must contain a mapping at the top level, "
            f"got {type(data).__name__}."
        )
    return data
