"""File-based pipeline configuration with hierarchical resolution.

Implements file-based pipeline discovery with dbt-style configuration
inheritance via ``dlt_project.yml``. Naming resolution is delegated to
:mod:`dlt_saga.pipeline_config.naming`, which holds the framework's default
``generate_*`` implementations and the custom-module loader; users
overriding naming behaviour should import the defaults from there.
"""

import logging
import os
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple, Union

from dlt_saga.pipeline_config.base_config import (
    ConfigSource,
    PipelineConfig,
    parse_tags,
)
from dlt_saga.pipeline_config.compat import normalize_config_aliases
from dlt_saga.pipeline_config.naming import (
    call_hook,
    default_generate_schema_name,
    default_generate_table_name,
    hook_accepts_kwarg,
    load_naming_module,
)
from dlt_saga.utility.naming import get_dev_schema, get_environment
from dlt_saga.utility.templating import render_templates
from dlt_saga.utility.yaml_io import load_yaml

logger = logging.getLogger(__name__)

# Lowercase-keyed dicts in the `pipelines:` hierarchy are normally folder
# segments. These keys are config blocks instead, so the folder heuristic must
# not mistake them for pipeline-group folders — letting them be set as
# project-wide or folder-level defaults.
_RESERVED_BLOCK_KEYS = {"dev"}


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

    def __init__(
        self,
        root_dir: Union[str, List[str]] = "configs",
        *,
        resolve_schema_names: bool = True,
    ):
        """Initialize file-based config source.

        Args:
            root_dir: Root directory (or list of directories) containing config
                files. When a list is given, all directories are walked and
                duplicate pipeline names across directories raise a ValueError.
            resolve_schema_names: When False, skip environment-aware schema-name
                resolution during load (``schema_name`` is left empty). Used by
                schema linking, which only needs each config's adapter and would
                otherwise require a profile/dev schema just to write modelines.
                Leave True for any path that runs or reports on pipelines.
        """
        self._resolve_schema_names = resolve_schema_names
        if isinstance(root_dir, list):
            self._root_dirs: List[str] = root_dir if root_dir else ["configs"]
        else:
            self._root_dirs = [root_dir]
        # Primary dir drives project-config loading and is kept for compat.
        self.root_dir = self._root_dirs[0]
        self.project_config_path = self._find_project_config(self.root_dir)
        self.project_config = self._load_project_config()
        # Lazily-computed set of actual sub-directory names in the config tree,
        # used to tell a folder-scope block from a config-value dict during
        # hierarchical resolution (see _is_path_segment_dict).
        self._folder_segment_names_cache: Optional[set] = None
        # Memoized discovery result. Discovery walks the config tree, parses+
        # templates every YAML, and runs the hierarchical merge — expensive, and
        # a single `saga run` triggers it several times (ingest + historize
        # selection, plus every get_config-by-name). Configs don't change within
        # a process, so compute once. See discover().
        self._discovery_cache: Optional[
            Tuple[Dict[str, List[PipelineConfig]], Dict[str, List[PipelineConfig]]]
        ] = None

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
            config = load_yaml(self.project_config_path)
            # Render {{ env_var(...) }} templates before the hierarchical
            # merge so project-level defaults resolve to concrete values.
            config = render_templates(config)
            # Rewrite legacy config keys (e.g. dataset_access → schema_access)
            # before the hierarchical merge runs so mixed-name trees compose
            # correctly.
            normalize_config_aliases(config)
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
            Relative path string (e.g., "google_sheets/reports/monthly.yml")
        """
        relative = self._get_relative_config_path(config_path)
        if relative:
            return str(relative)

        # Fallback: return as-is if we can't resolve
        return config_path

    def get_naming_segments(self, config_path: str) -> List[str]:
        """Convert a config file path into the segment list used by naming.

        Returns ``[group, sub_1, ..., name]`` with the file extension stripped
        from the leaf — e.g. ``configs/google_sheets/reports/monthly.yml`` becomes
        ``["google_sheets", "reports", "monthly"]``. Other config sources produce
        their own segments from whatever identifier shape they expose; the
        naming defaults in :mod:`dlt_saga.pipeline_config.naming` take only the
        segment list and don't care where it came from.
        """
        relative = self._get_relative_config_path(config_path)
        if relative is None:
            return []
        parts = list(relative.parts)
        if not parts:
            return []
        last = parts[-1]
        for ext in (".yml", ".yaml"):
            if last.endswith(ext):
                parts[-1] = last[: -len(ext)]
                break
        return parts

    def resolve_schema_name(
        self,
        config_path: str,
        *,
        layer: str = "ingest",
        environment: Optional[str] = None,
        custom_schema_name: Optional[str] = None,
    ) -> str:
        """Resolve schema name for a config file.

        Uses the custom naming module if one is configured; otherwise falls
        back to :func:`default_generate_schema_name`. The ``layer`` keyword
        is forwarded to custom modules so they can produce distinct shapes
        per layer (``"ingest"`` vs ``"historize"``).

        ``environment`` pins the resolution to a specific environment
        (``"prod"`` / ``"dev"``); when omitted it reads the active one via
        :func:`get_environment`. The collision guard passes ``"prod"`` so its
        verdict is the same whether ``saga doctor`` runs in dev or prod.

        ``custom_schema_name`` is an explicit ``schema_name:`` override. It's
        threaded into the generator so placement composes per environment: used
        directly in prod, composed with the developer sandbox in dev (see
        :func:`default_generate_schema_name`). A custom naming module whose
        ``generate_schema_name`` predates this parameter keeps the legacy
        behaviour — the override is used verbatim in both environments — until
        it adopts the new signature, so no existing prod placement moves.
        """
        segments = self.get_naming_segments(config_path)
        environment = environment or get_environment()
        # The dev schema is only used in dev; in prod the schema is derived from
        # the config segments (dlt_<group>) and default_schema is ignored. Don't
        # resolve it in prod, where a dev schema is legitimately unset and
        # get_dev_schema() would (correctly) raise.
        default_schema = get_dev_schema() if environment != "prod" else ""

        module = load_naming_module(self.project_config)
        if module and hasattr(module, "generate_schema_name"):
            if custom_schema_name and not hook_accepts_kwarg(
                module.generate_schema_name, "custom_schema_name"
            ):
                # Legacy custom hook: preserve pre-existing behaviour (an
                # explicit override wins verbatim, in both environments) rather
                # than silently dropping the override via call_hook.
                return custom_schema_name
            return call_hook(
                module.generate_schema_name,
                segments,
                environment,
                default_schema,
                layer=layer,
                custom_schema_name=custom_schema_name,
            )
        return default_generate_schema_name(
            segments,
            environment,
            default_schema,
            layer=layer,
            custom_schema_name=custom_schema_name,
        )

    def resolve_table_name(
        self,
        config_path: str,
        *,
        layer: str = "ingest",
        environment: Optional[str] = None,
        custom_table_name: Optional[str] = None,
    ) -> str:
        """Resolve table name for a config file.

        Uses the custom naming module if one is configured; otherwise falls
        back to :func:`default_generate_table_name`. The ``layer`` keyword
        is forwarded to custom modules so they can produce distinct shapes
        per layer (``"ingest"`` vs ``"historize"``).

        ``environment`` pins the resolution to a specific environment
        (``"prod"`` / ``"dev"``); when omitted it reads the active one via
        :func:`get_environment`.

        ``custom_table_name`` is an explicit ``table_name:`` override, threaded
        into the generator so it composes per environment (used directly in
        prod, group-prefixed in dev). A custom naming module whose
        ``generate_table_name`` predates this parameter keeps the legacy
        behaviour — the override is used verbatim — until it adopts the new
        signature.
        """
        segments = self.get_naming_segments(config_path)
        environment = environment or get_environment()

        module = load_naming_module(self.project_config)
        if module and hasattr(module, "generate_table_name"):
            if custom_table_name and not hook_accepts_kwarg(
                module.generate_table_name, "custom_table_name"
            ):
                return custom_table_name
            return call_hook(
                module.generate_table_name,
                segments,
                environment,
                layer=layer,
                custom_table_name=custom_table_name,
            )
        return default_generate_table_name(
            segments, environment, layer=layer, custom_table_name=custom_table_name
        )

    def resolve_ingest_target(
        self,
        config_path: str,
        *,
        schema_override: Optional[str] = None,
        table_override: Optional[str] = None,
        environment: Optional[str] = None,
    ) -> Tuple[str, str]:
        """Resolve a config's full ingest ``(schema, table)`` target.

        Mirrors the resolution :meth:`_load_config_file` applies during
        discovery: explicit ``schema_name:`` / ``table_name:`` overrides are
        threaded into the generators (used directly in prod, composed with the
        dev sandbox / group prefix in dev), otherwise the names are generated
        from the config path. ``environment`` pins the resolution (the collision
        guard passes ``"prod"`` for an environment-invariant verdict); when
        omitted it reads the active environment.
        """
        schema = self.resolve_schema_name(
            config_path,
            environment=environment,
            custom_schema_name=schema_override or None,
        )
        table = self.resolve_table_name(
            config_path,
            environment=environment,
            custom_table_name=table_override or None,
        )
        return (schema, table)

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
        failures: List[str],
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
            # Record and re-raise later (see _discover_all). Swallowing here
            # dropped the pipeline from discovery on one log line with exit 0 —
            # a malformed config silently disappeared instead of failing loudly.
            failures.append(f"  {config_path}: {e}")

    def discover(
        self,
    ) -> Tuple[Dict[str, List[PipelineConfig]], Dict[str, List[PipelineConfig]]]:
        """Discover all YAML config files across all configured directories.

        The (expensive) walk+parse+merge runs once per instance and is memoized;
        subsequent calls return copies of the cached result. Fresh top-level
        dicts and lists are returned each call so callers can filter or reorganize
        them without corrupting the cache (the pre-cache behaviour returned
        independent containers every time). The ``PipelineConfig`` values are
        shared and treated read-only.

        When multiple directories are configured, pipeline names must be unique
        across all of them — a duplicate raises ValueError.

        Returns:
            Tuple of (enabled_configs, disabled_configs)
        """
        if self._discovery_cache is None:
            self._discovery_cache = self._discover_all()
        enabled, disabled = self._discovery_cache
        return (
            {group: list(configs) for group, configs in enabled.items()},
            {group: list(configs) for group, configs in disabled.items()},
        )

    def _discover_all(
        self,
    ) -> Tuple[Dict[str, List[PipelineConfig]], Dict[str, List[PipelineConfig]]]:
        """Walk every configured directory and build the discovery result."""
        enabled_configs: Dict[str, List[PipelineConfig]] = {}
        disabled_configs: Dict[str, List[PipelineConfig]] = {}
        seen_names: Dict[str, str] = {}
        duplicates: List[str] = []
        failures: List[str] = []

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
                        failures,
                    )

        if failures:
            raise ValueError(
                "Failed to load pipeline config file(s) — fix or remove them "
                "(a broken config must not silently disappear from discovery):\n"
                + "\n".join(failures)
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
            identifier: File path to config file OR pipeline name (e.g., 'google_sheets__reports__monthly')
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
        file_config = load_yaml(config_path)

        # Render {{ env_var(...) }} templates (and Jinja filters) on the loaded
        # values before the hierarchical merge so each layer resolves to
        # concrete values independently.
        file_config = render_templates(file_config)

        # Rewrite legacy config keys (e.g. dataset_access → schema_access)
        # before the hierarchical merge so mixed-name trees compose correctly.
        normalize_config_aliases(file_config)

        # Apply hierarchical resolution (dlt_project.yml defaults)
        resolved_config = self._resolve_config(config_path, file_config)

        # Fold the `dev:` override block into the config (dev only). Applied
        # after the hierarchical resolve so a project-level `dev:` inherits down
        # like any other key.
        resolved_config = self._apply_dev_overrides(resolved_config)

        # Determine pipeline group from path
        pipeline_group = self._get_pipeline_group_from_path(config_path)

        # Derive base table name (without pipeline group prefix)
        base_table_name = self._derive_base_table_name(config_path)

        # Resolve environment-aware names from config path (current env).
        schema_override = resolved_config.get("schema_name") or ""
        table_override = resolved_config.get("table_name") or ""
        if self._resolve_schema_names:
            # Single source of truth shared with the collision guard, which
            # calls the same helper pinned to environment="prod".
            schema_name, table_name = self.resolve_ingest_target(
                config_path,
                schema_override=schema_override,
                table_override=table_override,
            )
        else:
            # Schema resolution needs the active profile's dev schema; skip it
            # when the caller only needs adapter/path metadata (schema linking),
            # so an offline `saga generate-schemas` doesn't require a configured
            # profile. The table name never needs a profile, so still resolve it.
            schema_name = schema_override
            table_name = self.resolve_table_name(
                config_path, custom_table_name=table_override or None
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

    def _apply_dev_overrides(self, config: Dict[str, Any]) -> Dict[str, Any]:
        """Fold a top-level ``dev:`` override block into the config.

        A ``dev:`` block lets a config (or a project/folder default) carry
        dev-only values — most usefully a smaller ``initial_value`` so dev runs
        load a recent slice instead of full history. Its values are ordinary
        config keys and are already Jinja-rendered at load time, so
        ``dev: {initial_value: "{{ (datetime.now(timezone.utc) - timedelta(days=7)).strftime('%Y-%m-%d') }}"}``
        resolves to a concrete, rolling date.

        The block is **always stripped** so it never reaches the pipeline, and it
        only takes effect when the active environment is ``dev``. Override is
        shallow: each key in ``dev:`` replaces the corresponding top-level key.
        """
        dev_overrides = config.pop("dev", None)
        if (get_environment() or "").lower() == "dev" and isinstance(
            dev_overrides, dict
        ):
            config.update(dev_overrides)
        return config

    # =========================================================================
    # Hierarchical Configuration Resolution
    # =========================================================================

    @property
    def _folder_segment_names(self) -> set:
        """Set of every sub-directory name in the config tree.

        Folder-scope blocks in ``saga_project.yml`` are keyed by real directory
        names (pipeline groups and their subfolders); config-value blocks
        (``persist_docs``, ``historize``, ``columns``, …) never are. Matching
        against actual directories is what distinguishes the two — the previous
        "any dict with lowercase keys is a folder" heuristic dropped every
        dict-valued config default (e.g. ``+persist_docs: {columns: true}``).
        """
        if self._folder_segment_names_cache is None:
            names: set = set()
            for root_dir in self._root_dirs:
                for _dirpath, dirnames, _files in os.walk(root_dir):
                    names.update(dirnames)
            self._folder_segment_names_cache = names
        return self._folder_segment_names_cache

    def _is_path_segment_dict(self, key: str, value: Any) -> bool:
        """Whether ``key: value`` is a nested folder scope rather than a config
        value — true only when ``key`` (minus any ``+`` inherit prefix) names an
        actual sub-directory in the config tree and its value is a dict.
        """
        return isinstance(value, dict) and key.lstrip("+") in self._folder_segment_names

    def _get_pipelines_section(self) -> Optional[Dict[str, Any]]:
        """Get the pipelines section from project config."""
        return self.project_config.get("pipelines")

    def _apply_project_defaults(self, resolved: Dict[str, Any]) -> None:
        """Apply project-level defaults to resolved config."""
        project_defaults = self._get_pipelines_section()
        if not project_defaults:
            return
        for key, value in project_defaults.items():
            clean_key = key.lstrip("+")
            # Skip keys that are path segments (nested folders), but not reserved
            # config blocks (e.g. `dev:`) that merely look like one.
            if clean_key not in _RESERVED_BLOCK_KEYS and self._is_path_segment_dict(
                key, value
            ):
                continue
            # Remove + prefix if present (all project level uses inherit by default)
            resolved[clean_key] = value

    def _extract_level_config(
        self, current_dict: Dict[str, Any], is_last_segment: bool
    ) -> Dict[str, Any]:
        """Extract config at current hierarchy level, excluding path segments."""
        level_config = {}
        for key, value in current_dict.items():
            # Skip nested path segments (but not if we're at the last segment),
            # except reserved config blocks (e.g. `dev:`) that look like folders.
            if (
                not is_last_segment
                and key.lstrip("+") not in _RESERVED_BLOCK_KEYS
                and self._is_path_segment_dict(key, value)
            ):
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
            # For lists: combine and deduplicate while preserving order. Use a
            # linear membership check rather than a set — merged lists may hold
            # dicts (e.g. `+filters:` / `+columns:` entries) which are
            # unhashable, and a set would raise TypeError. Config lists are
            # short, so the O(n^2) scan is irrelevant.
            result_list: List[Any] = []
            for item in parent_value + child_value:
                if item not in result_list:
                    result_list.append(item)
            return result_list

        elif isinstance(parent_value, dict) and isinstance(child_value, dict):
            # For dicts: shallow (one-level) merge — child keys override parent
            # keys; a nested dict on an overridden key replaces it wholesale.
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
