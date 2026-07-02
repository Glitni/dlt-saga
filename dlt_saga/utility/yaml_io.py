"""YAML config file loading.

Centralizes reading YAML config files as UTF-8 (the encoding the YAML spec
mandates). Reading with the platform default instead — cp1252 on Windows —
corrupts any non-ASCII content (e.g. ``ø`` becomes ``Ã¸``). Pinning the
encoding in one place means every config reader inherits it and new readers
can't reintroduce the bug.
"""

from pathlib import Path
from typing import Any, Dict, Union

import yaml


def load_yaml(path: Union[str, Path]) -> Dict[str, Any]:
    """Read and parse a YAML config file as UTF-8.

    Returns the parsed mapping, or an empty dict for an empty file, so callers
    don't need to guard against ``None``.
    """
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f) or {}
