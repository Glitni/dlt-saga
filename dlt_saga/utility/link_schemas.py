"""Link config files to their JSON schema via a yaml-language-server modeline.

Each pipeline config file is matched to a schema based on the ``adapter`` it
declares or inherits; the project-level files (``saga_project.yml``,
``packages.yml``, ``profiles.yml``) map to their fixed schemas. Editors then get
per-file validation/autocomplete without anyone adding
``# yaml-language-server: $schema=...`` by hand. This is editor-agnostic (any
yaml-language-server client) and idempotent, so it is safe to run repeatedly or
as a pre-commit hook.

Public API:
    link_config_schemas(schema_dir, config_source=None, project_root=None)
        -> list[LinkResult]
"""

import logging
import os
from dataclasses import dataclass
from pathlib import Path
from typing import List, Optional

from dlt_saga.utility.generate_schemas import schema_filename_for_adapter

logger = logging.getLogger(__name__)

MODELINE_PREFIX = "# yaml-language-server: $schema="

# Project-level files map to a fixed schema (no adapter resolution needed).
# These live at the project root, except profiles.yml which has its own search
# path and may legitimately live outside the project (see _link_root_files).
_PROJECT_ROOT_FILES = {
    "saga_project.yml": "saga_project_config.json",
    "packages.yml": "packages_config.json",
}
_PROFILES_SCHEMA = "profiles_config.json"


@dataclass
class LinkResult:
    """Outcome of linking a single config file."""

    config_path: str
    schema_filename: Optional[str]  # None when no schema could be resolved
    changed: bool  # True when the file's modeline was written/updated
    skipped_reason: Optional[str] = None  # set when schema_filename is None
    suggestion: Optional[str] = None  # set when the file was left untouched on purpose


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

    # utf-8-sig strips a leading BOM on read. Reading as plain utf-8 kept the
    # BOM in the content, so inserting the modeline at line 0 pushed the BOM
    # mid-stream and the first real key parsed as "﻿tags". The file is
    # rewritten without a BOM (unnecessary for UTF-8 YAML).
    with open(config_path, encoding="utf-8-sig") as f:
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


def _is_within(path: Path, root: Path) -> bool:
    """True when *path* is *root* or lives somewhere beneath it."""
    try:
        path.relative_to(root)
        return True
    except ValueError:
        return False


def _link_root_files(schema_dir: str, project_root: str) -> List[LinkResult]:
    """Link the project-level files (``saga_project.yml`` etc.) to their schemas.

    ``saga_project.yml`` and ``packages.yml`` are resolved at *project_root*.
    ``profiles.yml`` is resolved via its own search path and linked only when it
    lives inside *project_root* — an external profiles.yml (e.g. a shared file
    under ``SAGA_PROFILES_DIR``) is left untouched, with a suggestion returned
    so the user can wire it up by hand instead.
    """
    root = Path(project_root).resolve()
    results: List[LinkResult] = []

    for filename, schema_filename in _PROJECT_ROOT_FILES.items():
        path = root / filename
        if not path.exists():
            continue
        changed = apply_modeline(str(path), schema_dir, schema_filename)
        results.append(
            LinkResult(
                config_path=str(path),
                schema_filename=schema_filename,
                changed=changed,
            )
        )

    from dlt_saga.utility.cli.profiles import _find_profiles_file

    profiles_path = _find_profiles_file()
    if profiles_path is not None:
        profiles_path = profiles_path.resolve()
        if _is_within(profiles_path, root):
            changed = apply_modeline(str(profiles_path), schema_dir, _PROFILES_SCHEMA)
            results.append(
                LinkResult(
                    config_path=str(profiles_path),
                    schema_filename=_PROFILES_SCHEMA,
                    changed=changed,
                )
            )
        else:
            # An external profiles.yml has no stable relative relationship to
            # this project's schema dir (and may be shared across projects), so
            # suggest an absolute path — robust regardless of where it lives.
            schema_abs = os.path.abspath(
                os.path.join(schema_dir, _PROFILES_SCHEMA)
            ).replace(os.sep, "/")
            results.append(
                LinkResult(
                    config_path=str(profiles_path),
                    schema_filename=_PROFILES_SCHEMA,
                    changed=False,
                    suggestion=(
                        "profiles.yml is outside the project and was not modified. "
                        "To enable validation, add this line at the top of the file:\n"
                        f"  {MODELINE_PREFIX}{schema_abs}"
                    ),
                )
            )
    return results


def _link_mode_config_source():
    """Build the config source used for linking: file sources skip schema-name
    resolution so linking needs no profile/dev schema (it only reads adapters).

    Non-file sources fall back to the standard source — they don't share the
    file source's eager dev-schema resolution.
    """
    from dlt_saga.project_config import get_config_source_settings

    settings = get_config_source_settings()
    if settings.type == "file":
        from dlt_saga.pipeline_config import FilePipelineConfig

        return FilePipelineConfig(root_dir=settings.paths, resolve_schema_names=False)

    from dlt_saga.utility.cli.common import get_config_source

    return get_config_source()


def link_config_schemas(
    schema_dir, config_source=None, project_root=None
) -> List[LinkResult]:
    """Match every discovered config file to its adapter's schema modeline.

    Pipeline configs are matched by adapter; the project-level files
    (``saga_project.yml``, ``packages.yml``, ``profiles.yml``) map to their
    fixed schemas.

    Args:
        schema_dir: Directory the generated schemas live in (path-like).
        config_source: Optional ConfigSource; defaults to the project's source.
        project_root: Directory holding the project-level files; defaults to the
            current working directory.

    Returns:
        One LinkResult per processed config file.
    """
    if config_source is None:
        config_source = _link_mode_config_source()

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

    results.extend(_link_root_files(schema_dir, project_root or os.getcwd()))
    return results
