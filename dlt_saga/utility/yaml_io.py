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
    """SafeLoader that raises on duplicate keys instead of last-wins."""


def _construct_mapping_no_duplicates(
    loader: yaml.SafeLoader, node: yaml.MappingNode, deep: bool = False
) -> Dict[Any, Any]:
    mapping: Dict[Any, Any] = {}
    for key_node, value_node in node.value:
        key = loader.construct_object(key_node, deep=deep)
        if key in mapping:
            raise yaml.constructor.ConstructorError(
                "while constructing a mapping",
                node.start_mark,
                f"found duplicate key {key!r}",
                key_node.start_mark,
            )
        mapping[key] = loader.construct_object(value_node, deep=deep)
    return mapping


_UniqueKeySafeLoader.add_constructor(
    yaml.resolver.BaseResolver.DEFAULT_MAPPING_TAG,
    _construct_mapping_no_duplicates,
)


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
