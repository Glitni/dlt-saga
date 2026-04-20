"""File-based pipeline configuration with hierarchical resolution.

This module implements file-based pipeline discovery with dbt-style configuration
inheritance using dlt_project.yml for project-wide defaults.

Naming resolution (schema and table names) is handled here because it depends
on the config file path — the natural identifier for file-based configs.
Custom naming modules can override defaults via naming_module in saga_project.yml.
"""

import importlib
import logging
import os
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple, Union

import yaml

from dlt_saga.pipeline_config.base_config import (
    ConfigSource,
    PipelineConfig,
    parse_tags,
)
from dlt_saga.utility.naming import get_dev_schema, get_environment

logger = logging.getLogger(__name__)

# Cached custom naming module (None = not loaded, False = no custom module)
_naming_module: Any = None


def _load_naming_module(project_config: Dict[str, Any]) -> Any:
    """Load custom naming module from project config if configured.

    The naming_module field in saga_project.yml points to a Python module
    that can override schema and table name generation. The module should
    provide:
    - generate_schema_name(config_path: str, environment: str, default_schema: str) -> str
    - generate_table_name(config_path: str, environment: str) -> str

    Where config_path is the relative path from the configs root
    (e.g., "google_sheets/asm/salgsmal.yml").

    Args:
        project_config: Parsed saga_project.yml contents

    Returns:
        The imported module, or False if no custom module configured.
    """
    global _naming_module
    if _naming_module is not None:
        return _naming_module

    try:
        module_name = project_config.get("naming_module")
        if not module_name:
            _naming_module = False
            return _naming_module

        _naming_module = importlib.import_module(module_name)
        logger.debug(f"Loaded custom naming module: {module_name}")

        # Validate expected functions
        for fn_name in ("generate_schema_name", "generate_table_name"):
            if not hasattr(_naming_module, fn_name):
                logger.warning(
                    f"Custom naming module '{module_name}' missing {fn_name}(), "
                    f"will use default for that function"
                )

        return _naming_module

    except Exception as e:
        logger.warning(f"Failed to load custom naming module: {e}, using defaults")
        _naming_module = False
        return _naming_module


def _default_generate_schema_name(
    config_path: str, environment: str, default_schema: str
) -> str:
    """Default schema name generation from relative config path.

    Prod: dlt_{first_path_segment} (e.g., dlt_google_sheets)
    Dev: default_schema (from profile or SAGA_SCHEMA_NAME)

    Args:
        config_path: Relative path from configs root (e.g., "google_sheets/asm/salgsmal.yml")
        environment: Current environment ('prod' or 'dev')
        default_schema: Dev schema name from profile/env var
    """
    if environment == "prod":
        first_segment = (
            Path(config_path).parts[0] if Path(config_path).parts else "default"
        )
        return f"dlt_{first_segment}"
    else:
        return default_schema


def _default_generate_table_name(config_path: str, environment: str) -> str:
    """Default table name generation from relative config path.

    Prod: base_name (no prefix, e.g., "asm__salgsmal")
    Dev: {first_segment}__{base_name} (e.g., "google_sheets__asm__salgsmal")

    Args:
        config_path: Relative path from configs root (e.g., "google_sheets/asm/salgsmal.yml")
        environment: Current environment ('prod' or 'dev')
    """
    path = Path(config_path)
    parts = list(path.parts)

    # Remove file extension from last part
    if parts:
        last = parts[-1]
        for ext in (".yml", ".yaml"):
            if last.endswith(ext):
                parts[-1] = last[: -len(ext)]
                break

    if not parts:
        return "default_data"

    first_segment = parts[0]

    if len(parts) == 1:
        base_name = parts[0].lower().replace("-", "_").replace(" ", "_")
    elif len(parts) == 2:
        base_name = parts[1].lower().replace("-", "_").replace(" ", "_")
    else:
        name_parts = parts[1:]
        base_name = "__".join(
            p.lower().replace("-", "_").replace(" ", "_") for p in name_parts
        )

    if environment == "prod":
        return base_name
    else:
        return f"{first_segment}__{base_name}"


class FilePipelineConfig(ConfigSource):
    """Configuration source that reads YAML files from a directory structure.

    Expects configs in: configs/<pipeline_group>/<optional_subfolders>/<name>.yml
    Supports hierarchical configuration with dlt_project.yml defaults.

    Resolution order:
    1. Project-level defaults (configs/dlt_project.yml :: project section)
    2. Folder hierarchy defaults (walking down the path)
    3. File-level configuration

    Inheritance vs Override:
    - `+key:` - Merge/inherit from parent (for lists/dicts)
    - `key:` - Replace/override parent value completely
    """

    def __init__(self, root_dir: Union[str, List[str]] = "configs"):
        """Initialize file-based config source.

        Args:
            root_dir: Root directory (or list of directories) containing config
                files. When a list is given, all directories are walked and
                duplicate pipeline names across directories raise a ValueError.
        """
        if isinstance(root_dir, list):
            self._root_dirs: List[str] = root_dir if root_dir else ["configs"]
        else:
            self._root_dirs = [root_dir]
        # Primary dir drives project-config loading and is kept for compat.
        self.root_dir = self._root_dirs[0]
        self.project_config_path = self._find_project_config(self.root_dir)
        self.project_config = self._load_project_config()

    @staticmethod
    def _find_project_config(root_dir: str) -> Path:
        """Find saga_project.yml at the repo root (parent of configs/)."""
        return Path(root_dir).parent / "saga_project.yml"

    def _load_project_config(self) -> Dict[str, Any]:
        """Load the project config file.

        Returns:
            Dict containing project configuration, or empty dict if file doesn't exist
        """
        if not self.project_config_path.exists():
            logger.debug(
                f"No project config found at {self.project_config_path}, using empty config"
            )
            return {}

        try:
            with open(self.project_config_path, "r") as f:
                config = yaml.safe_load(f) or {}
                logger.debug(f"Loaded project config from {self.project_config_path}")
                return config
        except Exception as e:
            logger.warning(
                f"Failed to load project config from {self.project_config_path}: {e}"
            )
            return {}

    # =========================================================================
    # Naming Resolution
    # =========================================================================

    def _get_relative_config_path_str(self, config_path: str) -> str:
        """Get config path relative to config root as a string.

        This is the primary identifier passed to naming functions,
        analogous to dbt's model path for generate_schema_name.

        Args:
            config_path: Absolute or relative path to a config file

        Returns:
            Relative path string (e.g., "google_sheets/asm/salgsmal.yml")
        """
        relative = self._get_relative_config_path(config_path)
        if relative:
            return str(relative)

        # Fallback: return as-is if we can't resolve
        return config_path

    def resolve_schema_name(self, config_path: str) -> str:
        """Resolve schema name for a config file.

        Uses custom naming module if configured in saga_project.yml,
        otherwise uses default logic:
          prod: dlt_{first_path_segment}
          dev: profile schema or dlt_dev

        Args:
            config_path: Path to config file (absolute or relative)

        Returns:
            Schema name
        """
        rel_path = self._get_relative_config_path_str(config_path)
        environment = get_environment()
        default_schema = get_dev_schema()

        module = _load_naming_module(self.project_config)
        if module and hasattr(module, "generate_schema_name"):
            return module.generate_schema_name(rel_path, environment, default_schema)
        return _default_generate_schema_name(rel_path, environment, default_schema)

    def resolve_table_name(self, config_path: str) -> str:
        """Resolve table name for a config file.

        Uses custom naming module if configured in saga_project.yml,
        otherwise uses default logic:
          prod: base_name (no prefix)
          dev: {first_segment}__{base_name}

        Args:
            config_path: Path to config file (absolute or relative)

        Returns:
            Table name
        """
        rel_path = self._get_relative_config_path_str(config_path)
        environment = get_environment()

        module = _load_naming_module(self.project_config)
        if module and hasattr(module, "generate_table_name"):
            return module.generate_table_name(rel_path, environment)
        return _default_generate_table_name(rel_path, environment)

    # =========================================================================
    # Discovery
    # =========================================================================

    def _collect_config_file(
        self,
        config_path: str,
        seen_names: Dict[str, str],
        enabled_configs: Dict[str, List[PipelineConfig]],
        disabled_configs: Dict[str, List[PipelineConfig]],
        duplicates: List[str],
    ) -> None:
        """Load one config file and slot it into the discovery accumulators."""
        try:
            config = self._load_config_file(config_path)
            if not config:
                return
            if config.pipeline_name in seen_names:
                duplicates.append(
                    f"'{config.pipeline_name}': "
                    f"{config_path} conflicts with "
                    f"{seen_names[config.pipeline_name]}"
                )
                return
            seen_names[config.pipeline_name] = config_path
            target = enabled_configs if config.enabled else disabled_configs
            target.setdefault(config.pipeline_group, []).append(config)
        except Exception as e:
            logger.error(f"Error processing {os.path.basename(config_path)}: {e}")

    def discover(
        self,
    ) -> Tuple[Dict[str, List[PipelineConfig]], Dict[str, List[PipelineConfig]]]:
        """Discover all YAML config files across all configured directories.

        When multiple directories are configured, pipeline names must be unique
        across all of them — a duplicate raises ValueError.

        Returns:
            Tuple of (enabled_configs, disabled_configs)
        """
        enabled_configs: Dict[str, List[PipelineConfig]] = {}
        disabled_configs: Dict[str, List[PipelineConfig]] = {}
        seen_names: Dict[str, str] = {}
        duplicates: List[str] = []

        for root_dir in self._root_dirs:
            if not os.path.isdir(root_dir):
                logger.warning(f"Config directory does not exist: {root_dir}")
                continue
            for root, _, files in os.walk(root_dir):
                for file in files:
                    if not file.endswith((".yml", ".yaml")):
                        continue
                    if file == "saga_project.yml":
                        continue
                    self._collect_config_file(
                        os.path.join(root, file),
                        seen_names,
                        enabled_configs,
                        disabled_configs,
                        duplicates,
                    )

        if duplicates:
            raise ValueError(
                "Duplicate pipeline names found across config directories:\n"
                + "\n".join(f"  {d}" for d in duplicates)
            )

        return enabled_configs, disabled_configs

    def get_config(self, identifier: str) -> Optional[PipelineConfig]:
        """Get a specific config by file path or pipeline name.

        Args:
            identifier: File path to config file OR pipeline name (e.g., 'google_sheets__asm__salgsmal')
                       Pipeline name is always {pipeline_group}__{base_table_name}, consistent across environments

        Returns:
            PipelineConfig if found and valid, None otherwise
        """
        # Try as file path first
        if os.path.exists(identifier):
            try:
                return self._load_config_file(identifier)
            except Exception as e:
                logger.error(f"Failed to load config {identifier}: {str(e)}")
                return None

        # If not a file path, search by pipeline name (consistent across dev/prod)
        logger.debug(
            f"'{identifier}' is not a file path, searching by pipeline name..."
        )
        enabled_configs, disabled_configs = self.discover()

        # Search in enabled configs
        for pipeline_configs in enabled_configs.values():
            for config in pipeline_configs:
                if config.pipeline_name == identifier:
                    return config

        # Search in disabled configs
        for pipeline_configs in disabled_configs.values():
            for config in pipeline_configs:
                if config.pipeline_name == identifier:
                    logger.warning(f"Found pipeline '{identifier}' but it is disabled")
                    return config

        logger.error(f"Pipeline not found: {identifier}")
        return None

    def _load_config_file(self, config_path: str) -> PipelineConfig:
        """Load and parse a single YAML config file with hierarchical resolution.

        Args:
            config_path: Path to YAML config file

        Returns:
            PipelineConfig object

        Raises:
            Exception: If config is invalid or cannot be loaded
        """
        # Load raw YAML
        with open(config_path) as f:
            file_config = yaml.safe_load(f) or {}

        # Apply hierarchical resolution (dlt_project.yml defaults)
        resolved_config = self._resolve_config(config_path, file_config)

        # Determine pipeline group from path
        pipeline_group = self._get_pipeline_group_from_path(config_path)

        # Derive base table name (without pipeline group prefix)
        base_table_name = self._derive_base_table_name(config_path)

        # Resolve environment-aware names from config path
        table_name = self.resolve_table_name(config_path)
        schema_name = resolved_config.get("schema_name") or self.resolve_schema_name(
            config_path
        )

        # Normalize tags to list and parse into ScheduleTag objects
        raw_tags = resolved_config.get("tags", [])
        if isinstance(raw_tags, str):
            raw_tags = [raw_tags]
        elif not isinstance(raw_tags, list):
            raw_tags = []

        # Parse tags into ScheduleTag objects
        tags = parse_tags(raw_tags)

        # Add metadata to config dict (keep raw tags for serialization)
        config_dict = {
            **resolved_config,
            "base_table_name": base_table_name,
            "config_path": config_path,
            "tags": raw_tags,  # Keep raw format in config_dict
        }

        pipeline_name = f"{pipeline_group}__{base_table_name}"

        return PipelineConfig(
            pipeline_group=pipeline_group,
            pipeline_name=pipeline_name,
            table_name=table_name,
            schema_name=schema_name,
            identifier=config_path,
            config_dict=config_dict,
            enabled=resolved_config.get("enabled", True),
            tags=tags,
            adapter=resolved_config.get("adapter"),
            source_type="file",
        )

    # =========================================================================
    # Hierarchical Configuration Resolution
    # =========================================================================

    def _is_path_segment_dict(self, value: Any) -> bool:
        """Check if value is a dict representing a path segment (has lowercase keys).

        Strips the '+' inherit prefix before checking, since folder hierarchy
        configs often use '+key:' syntax (e.g., '+dataset_access:').
        """
        return isinstance(value, dict) and any(
            k.lstrip("+")[:1].islower() for k in value.keys() if k.lstrip("+")
        )

    def _get_pipelines_section(self) -> Optional[Dict[str, Any]]:
        """Get the pipelines section from project config."""
        return self.project_config.get("pipelines")

    def _apply_project_defaults(self, resolved: Dict[str, Any]) -> None:
        """Apply project-level defaults to resolved config."""
        project_defaults = self._get_pipelines_section()
        if not project_defaults:
            return
        for key, value in project_defaults.items():
            # Skip keys that are path segments (nested folders)
            if self._is_path_segment_dict(value):
                continue
            # Remove + prefix if present (all project level uses inherit by default)
            clean_key = key.lstrip("+")
            resolved[clean_key] = value

    def _extract_level_config(
        self, current_dict: Dict[str, Any], is_last_segment: bool
    ) -> Dict[str, Any]:
        """Extract config at current hierarchy level, excluding path segments."""
        level_config = {}
        for key, value in current_dict.items():
            # Skip nested path segments (but not if we're at the last segment)
            if not is_last_segment and self._is_path_segment_dict(value):
                continue
            level_config[key] = value
        return level_config

    def _apply_level_config(
        self, resolved: Dict[str, Any], level_config: Dict[str, Any], segment: str
    ) -> None:
        """Apply configuration from one hierarchy level."""
        for key, value in level_config.items():
            inherit = key.startswith("+")
            clean_key = key.lstrip("+")
            resolved[clean_key] = self._resolve_value(
                resolved.get(clean_key), value, inherit
            )

    def _apply_folder_hierarchy(
        self, resolved: Dict[str, Any], segments: List[str]
    ) -> None:
        """Walk down folder hierarchy, applying defaults at each level."""
        current_dict = self._get_pipelines_section() or {}
        for i, segment in enumerate(segments):
            if segment not in current_dict:
                continue

            current_dict = current_dict[segment]
            is_last_segment = i == len(segments) - 1
            level_config = self._extract_level_config(current_dict, is_last_segment)
            self._apply_level_config(resolved, level_config, segment)

    def _apply_file_config(
        self, resolved: Dict[str, Any], file_config: Dict[str, Any]
    ) -> None:
        """Apply file-level configuration."""
        for key, value in file_config.items():
            inherit = key.startswith("+")
            clean_key = key.lstrip("+")
            resolved[clean_key] = self._resolve_value(
                resolved.get(clean_key), value, inherit
            )

    def _resolve_config(
        self, config_path: str, file_config: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Resolve final configuration using hierarchical resolution.

        Applies project and folder defaults to the provided file config.

        Args:
            config_path: Path to config file
            file_config: The pipeline-specific configuration dict

        Returns:
            Fully resolved configuration dictionary
        """
        if not self.project_config:
            return file_config

        resolved: Dict[str, Any] = {}
        segments = self._get_path_segments(config_path)

        # Apply configuration in hierarchy order
        self._apply_project_defaults(resolved)
        self._apply_folder_hierarchy(resolved, segments)
        self._apply_file_config(resolved, file_config)

        return resolved

    def _resolve_value(self, parent_value: Any, child_value: Any, inherit: bool) -> Any:
        """Resolve a single configuration value with inheritance logic.

        Args:
            parent_value: Value from parent level
            child_value: Value from child level
            inherit: True if using +key: (inherit), False if using key: (override)

        Returns:
            Resolved configuration value
        """
        # If no parent value, just use child value
        if parent_value is None:
            return child_value

        # If no child value, use parent value
        if child_value is None:
            return parent_value if inherit else None

        # Override mode: child completely replaces parent
        if not inherit:
            return child_value

        # Inherit mode: merge based on type
        if isinstance(parent_value, list) and isinstance(child_value, list):
            # For lists: combine and deduplicate while preserving order
            combined = parent_value + child_value
            seen = set()
            result_list = []
            for item in combined:
                if item not in seen:
                    seen.add(item)
                    result_list.append(item)
            return result_list

        elif isinstance(parent_value, dict) and isinstance(child_value, dict):
            # For dicts: merge recursively
            result_dict = parent_value.copy()
            result_dict.update(child_value)
            return result_dict

        else:
            # For other types (str, int, bool, etc.): child value wins even in inherit mode
            return child_value

    # =========================================================================
    # Path Utilities
    # =========================================================================

    def _get_relative_config_path(self, config_path: str) -> Optional[Path]:
        """Get the path of a config file relative to its config root directory.

        Tries each configured root directory in order.

        Args:
            config_path: Absolute or relative path to a config file

        Returns:
            Path relative to the matching config root, or None if not found.
        """
        path_obj = Path(config_path).resolve()

        for root_dir in self._root_dirs:
            try:
                return path_obj.relative_to(Path(root_dir).resolve())
            except ValueError:
                continue

        # Legacy fallback: look for "configs" in path parts (handles unresolved relative paths)
        if "configs" in Path(config_path).parts:
            try:
                return Path(config_path).relative_to("configs")
            except ValueError:
                pass

        return None

    def _get_path_segments(self, config_path: str) -> List[str]:
        """Extract path segments from a config file path.

        Args:
            config_path: Path to config file (e.g., "configs/google_sheets/dbt/models.yml")

        Returns:
            List of path segments (e.g., ["google_sheets", "dbt"])
        """
        relative = self._get_relative_config_path(config_path)
        if relative:
            # All parts except the filename
            return [
                part for part in relative.parts if not part.endswith((".yml", ".yaml"))
            ]

        # Fallback: strip known prefixes manually
        path = Path(config_path)
        segments = []
        for part in path.parts:
            if part == "configs" or part == self.root_dir:
                continue
            if part.endswith((".yml", ".yaml")):
                break
            segments.append(part)
        return segments

    def _get_pipeline_group_from_path(self, path: str) -> str:
        """Extract pipeline group from config file path.

        The pipeline group is always the first directory under the config root.

        Args:
            path: File path to config

        Returns:
            Pipeline group (e.g., 'google_sheets', 'filesystem')

        Raises:
            ValueError: If pipeline group cannot be determined
        """
        relative = self._get_relative_config_path(path)
        if relative and len(relative.parts) >= 2:
            # First part is pipeline_group
            return relative.parts[0]

        # Legacy fallback for paths with "configs" in them
        try:
            path_obj = Path(path).resolve()
            if "configs" in path_obj.parts:
                configs_index = path_obj.parts.index("configs")
                if configs_index + 1 < len(path_obj.parts):
                    return path_obj.parts[configs_index + 1]
        except (ValueError, IndexError):
            pass

        raise ValueError(f"Could not determine pipeline group from path: {path}")

    def _derive_base_table_name(self, config_path: str) -> str:
        """Derive base table name from config path (without pipeline group prefix).

        The pipeline group prefix is added separately based on environment.

        Format: <subfolders separated by _ if any>__<filename>
        Example: regional_budget__buskerud_vestfold (NOT google_sheets__regional_budget__...)

        Args:
            config_path: Path to config file

        Returns:
            Base table name without pipeline type prefix
        """
        if not config_path:
            return "default_data"

        relative = self._get_relative_config_path(config_path)
        if relative:
            parts = list(relative.parts)
        else:
            # Fallback: try relative_to("configs")
            try:
                parts = list(Path(config_path).relative_to("configs").parts)
            except ValueError:
                stem = (
                    Path(config_path).stem.lower().replace("-", "_").replace(" ", "_")
                )
                return stem

        # The first part is the pipeline group (skip it)
        # The last part is the filename
        # Middle parts are subfolders
        if len(parts) == 1:
            # Just a filename in the type directory (no subfolders)
            return (
                parts[0]
                .lower()
                .replace("-", "_")
                .replace(" ", "_")
                .replace(".yml", "")
                .replace(".yaml", "")
            )
        elif len(parts) == 2:
            # Type directory + filename (no subfolders)
            filename = parts[-1]
            cleaned = filename.lower().replace("-", "_").replace(" ", "_")
            cleaned = cleaned.replace(".yml", "").replace(".yaml", "")
            return cleaned
        else:
            # Type directory + subfolders + filename
            subfolders = parts[1:-1]  # Skip type (first), include subfolders
            filename = parts[-1]

            # Combine subfolders and filename with double underscores
            name_parts = subfolders + [filename]

            # Clean each part and join with double underscores
            cleaned_parts = []
            for part in name_parts:
                cleaned = part.lower().replace("-", "_").replace(" ", "_")
                cleaned = cleaned.replace(".yml", "").replace(".yaml", "")
                cleaned_parts.append(cleaned)

            return "__".join(cleaned_parts)
