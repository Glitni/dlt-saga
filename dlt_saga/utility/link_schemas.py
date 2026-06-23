"""Link pipeline config files to their JSON schema via a yaml-language-server modeline.

Each config file is matched to a schema based on the ``adapter`` it declares or
inherits — so editors get per-file validation/autocomplete without anyone adding
``# yaml-language-server: $schema=...`` by hand. This is editor-agnostic (any
yaml-language-server client) and idempotent, so it is safe to run repeatedly or
as a pre-commit hook.

Public API:
    link_config_schemas(schema_dir, config_source=None) -> list[LinkResult]
"""

import logging
import os
from dataclasses import dataclass
from typing import List, Optional

from dlt_saga.utility.generate_schemas import schema_filename_for_adapter

logger = logging.getLogger(__name__)

MODELINE_PREFIX = "# yaml-language-server: $schema="


@dataclass
class LinkResult:
    """Outcome of linking a single config file."""

    config_path: str
    schema_filename: Optional[str]  # None when no schema could be resolved
    changed: bool  # True when the file's modeline was written/updated
    skipped_reason: Optional[str] = None  # set when schema_filename is None


def _relative_schema_path(
    config_path: str, schema_dir: str, schema_filename: str
) -> str:
    """POSIX-style relative path from *config_path*'s directory to the schema."""
    schema_path = os.path.join(schema_dir, schema_filename)
    rel = os.path.relpath(schema_path, os.path.dirname(os.path.abspath(config_path)))
    return rel.replace(os.sep, "/")


def _existing_modeline_index(lines: List[str]) -> Optional[int]:
    """Index of the first existing schema modeline, or None.

    Matches a comment line of the form ``# yaml-language-server: $schema=...``,
    tolerating leading/inner whitespace variations.
    """
    for i, line in enumerate(lines):
        stripped = line.strip()
        if stripped.startswith("#") and "yaml-language-server:" in stripped:
            if "$schema=" in stripped:
                return i
    return None


def apply_modeline(config_path: str, schema_dir: str, schema_filename: str) -> bool:
    """Insert or update the schema modeline in *config_path*.

    Returns True when the file content changed, False when it was already correct.
    """
    rel = _relative_schema_path(config_path, schema_dir, schema_filename)
    desired = f"{MODELINE_PREFIX}{rel}"

    with open(config_path, encoding="utf-8") as f:
        content = f.read()

    newline = "\r\n" if "\r\n" in content else "\n"
    lines = content.splitlines()

    idx = _existing_modeline_index(lines)
    if idx is not None:
        if lines[idx] == desired:
            return False
        lines[idx] = desired
    else:
        lines.insert(0, desired)

    new_content = newline.join(lines)
    if content.endswith(("\n", "\r")) or not content:
        new_content += newline
    if new_content == content:
        return False

    with open(config_path, "w", encoding="utf-8", newline="") as f:
        f.write(new_content)
    return True


def link_config_schemas(schema_dir, config_source=None) -> List[LinkResult]:
    """Match every discovered config file to its adapter's schema modeline.

    Args:
        schema_dir: Directory the generated schemas live in (path-like).
        config_source: Optional ConfigSource; defaults to the project's source.

    Returns:
        One LinkResult per processed config file.
    """
    if config_source is None:
        from dlt_saga.utility.cli.common import get_config_source

        config_source = get_config_source()

    schema_dir = str(schema_dir)
    enabled, disabled = config_source.discover()

    results: List[LinkResult] = []
    for group_map in (enabled, disabled):
        for configs in group_map.values():
            for config in configs:
                if config.source_type != "file":
                    continue
                config_path = config.identifier
                pipeline_group = config.pipeline_name.split("__", 1)[0]
                schema_filename = schema_filename_for_adapter(
                    config.adapter,
                    pipeline_group=pipeline_group,
                    config_path=config_path,
                )
                if not schema_filename:
                    results.append(
                        LinkResult(
                            config_path=config_path,
                            schema_filename=None,
                            changed=False,
                            skipped_reason=(
                                f"could not resolve a schema for adapter "
                                f"{config.adapter!r}"
                            ),
                        )
                    )
                    continue
                changed = apply_modeline(config_path, schema_dir, schema_filename)
                results.append(
                    LinkResult(
                        config_path=config_path,
                        schema_filename=schema_filename,
                        changed=changed,
                    )
                )
    return results
