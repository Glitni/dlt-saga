"""Pipeline config validation without execution.

Validates pipeline configs early and offline (no warehouse connection). Per
config: write_disposition, adapter resolution, source config instantiation, and
historize config (:func:`validate_pipeline_config`). Across a selection:
target collisions and deprecated config keys (:func:`validate_pipeline_configs`).

This is the config-correctness gate behind ``saga validate`` and
``Session.validate`` — the counterpart to ``saga doctor``, which covers
environment/connectivity health.
"""

import importlib
import logging
import os
import types
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Type

from dlt_saga.pipeline_config import ConfigSource, PipelineConfig

logger = logging.getLogger(__name__)

# All valid write_disposition values.
VALID_WRITE_DISPOSITIONS = {
    "append",
    "merge",
    "replace",
    "append+historize",
    "replace+historize",
    "historize",
}


@dataclass
class ValidationResult:
    """Result of validating a single pipeline config.

    ``kind`` distinguishes a real pipeline config (the default) from the
    synthetic results the project-wide checks emit: ``"project"`` for a
    project/profile file carrying deprecated keys, ``"check"`` for a project-wide
    check that could not run. Consumers that count pipelines filter on
    ``kind == "pipeline"``.
    """

    pipeline_name: str
    errors: List[str] = field(default_factory=list)
    warnings: List[str] = field(default_factory=list)
    kind: str = "pipeline"

    @property
    def is_valid(self) -> bool:
        return len(self.errors) == 0


def _find_config_class(pipeline_module: types.ModuleType) -> Optional[Type]:
    """Find the BaseConfig subclass in the sibling config.py module.

    Mirrors the pattern from registry._find_pipeline_class() — looks for
    a class defined in the module that inherits from BaseConfig.
    """
    from dlt_saga.pipelines.base_config import BaseConfig

    candidates = [
        obj
        for name, obj in vars(pipeline_module).items()
        if isinstance(obj, type)
        and issubclass(obj, BaseConfig)
        and obj is not BaseConfig
        and obj.__module__ == pipeline_module.__name__
    ]
    if len(candidates) == 1:
        return candidates[0]
    if len(candidates) > 1:
        logger.debug(
            f"Multiple BaseConfig subclasses in {pipeline_module.__name__}: "
            f"{[c.__name__ for c in candidates]}"
        )
    return None


def _get_config_module_path(pipeline_module_path: str) -> str:
    """Derive the config module path from a pipeline module path.

    Example: 'dlt_saga.pipelines.google_sheets.pipeline'
          -> 'dlt_saga.pipelines.google_sheets.config'
    """
    parts = pipeline_module_path.rsplit(".", 1)
    return f"{parts[0]}.config"


def _validate_write_disposition(config: PipelineConfig, result: ValidationResult):
    """Check write_disposition is a known value."""
    wd = config.raw_write_disposition
    if wd not in VALID_WRITE_DISPOSITIONS:
        result.errors.append(
            f"Invalid write_disposition '{wd}'. "
            f"Must be one of: {', '.join(sorted(VALID_WRITE_DISPOSITIONS))}"
        )


def _validate_adapter(config: PipelineConfig, result: ValidationResult):
    """Check that adapter resolves to a valid pipeline class."""
    from dlt_saga.pipelines.registry import get_pipeline_class

    try:
        get_pipeline_class(
            pipeline_group=config.pipeline_group,
            config_path=config.identifier,
            adapter=config.adapter,
        )
    except ImportError as e:
        result.errors.append(f"Cannot resolve pipeline implementation: {e}")


def _validate_source_config(config: PipelineConfig, result: ValidationResult):
    """Find the source config class and try to instantiate it."""
    from dlt_saga.pipelines.registry import get_pipeline_class

    try:
        pipeline_cls = get_pipeline_class(
            pipeline_group=config.pipeline_group,
            config_path=config.identifier,
            adapter=config.adapter,
        )
    except ImportError:
        # Already reported by _validate_adapter
        return

    # Find the config module — try sibling first, then walk up parent packages
    # e.g. api.omnystudio.clips.pipeline → try clips.config, then omnystudio.config, then api.config
    pipeline_module_path = pipeline_cls.__module__
    config_class = None

    package_path = pipeline_module_path.rsplit(".", 1)[0]  # strip '.pipeline'
    while package_path:
        candidate = f"{package_path}.config"
        try:
            mod = importlib.import_module(candidate)
            cls = _find_config_class(mod)
            if cls is not None:
                config_class = cls
                break
        except ImportError:
            pass
        # Walk up one level
        if "." not in package_path:
            break
        package_path = package_path.rsplit(".", 1)[0]

    if config_class is None:
        result.warnings.append(
            f"No BaseConfig subclass found for {pipeline_cls.__module__} — "
            f"source config validation skipped"
        )
        return

    # Filter config_dict to only fields the config class knows about
    known_fields = {f.name for f in config_class.__dataclass_fields__.values()}
    filtered = {k: v for k, v in config.config_dict.items() if k in known_fields}

    try:
        config_class(**filtered)
    except (ValueError, TypeError) as e:
        result.errors.append(f"Source config error: {e}")


def _validate_historize_config(config: PipelineConfig, result: ValidationResult):
    """Validate historize config when historize is enabled."""
    if not config.historize_enabled:
        return

    from dlt_saga.historize.config import HistorizeConfig

    historize_dict = config.config_dict.get("historize") or {}
    top_level_pk = config.config_dict.get("primary_key")
    if isinstance(top_level_pk, str):
        top_level_pk = [top_level_pk]

    try:
        hist_config = HistorizeConfig.from_dict(historize_dict, top_level_pk)
        hist_config.validate(config.config_dict)
    except (ValueError, TypeError) as e:
        result.errors.append(f"Historize config error: {e}")


def validate_pipeline_config(config: PipelineConfig) -> ValidationResult:
    """Validate a pipeline config without executing anything.

    Runs all validation layers and collects errors/warnings.

    Args:
        config: Pipeline config to validate

    Returns:
        ValidationResult with collected errors and warnings
    """
    result = ValidationResult(pipeline_name=config.pipeline_name)

    _validate_write_disposition(config, result)
    # Historize-only pipelines (write_disposition: "historize") have no ingest step,
    # so there is no source adapter/config to resolve — skip those checks for them.
    # Compared against the raw string rather than `ingest_enabled`, which would also
    # short-circuit for invalid/unknown values and mask their adapter errors.
    if config.raw_write_disposition != "historize":
        _validate_adapter(config, result)
        _validate_source_config(config, result)
    _validate_historize_config(config, result)

    return result


# ---------------------------------------------------------------------------
# Project-wide checks (across a selection of configs)
# ---------------------------------------------------------------------------

# Stable message prefixes so the CLI can classify findings for its affirmative
# per-check summary without re-deriving them.
COLLISION_FINDING_PREFIX = "Target collision:"
COLLISION_CHECK_FAILED_PREFIX = "Target-collision check could not run:"
DEPRECATED_KEY_FINDING_PREFIX = "Deprecated key "


def is_collision_finding(message: str) -> bool:
    """True if an error message is a target-collision finding."""
    return message.startswith(COLLISION_FINDING_PREFIX)


def is_collision_check_failure(message: str) -> bool:
    """True if a warning message flags that the collision check couldn't run."""
    return message.startswith(COLLISION_CHECK_FAILED_PREFIX)


def is_deprecated_key_finding(message: str) -> bool:
    """True if a warning message is a deprecated-config-key finding."""
    return message.startswith(DEPRECATED_KEY_FINDING_PREFIX)


def collision_findings(results: List["ValidationResult"]) -> List[str]:
    """Unique collision messages across ``results``.

    Each collision is attached identically to every participant, so deduping by
    message yields one entry per shared target (insertion order preserved).
    """
    seen: Dict[str, None] = {}
    for result in results:
        for error in result.errors:
            if is_collision_finding(error):
                seen[error] = None
    return list(seen)


def collision_check_failed(results: List["ValidationResult"]) -> bool:
    """True if the collision check itself could not run for this selection."""
    return any(is_collision_check_failure(w) for r in results for w in r.warnings)


def deprecated_key_count(results: List["ValidationResult"]) -> int:
    """Total deprecated-key findings across ``results``."""
    return sum(1 for r in results for w in r.warnings if is_deprecated_key_finding(w))


def _legacy_key_message(legacy: str, canonical: str) -> str:
    return (
        f"{DEPRECATED_KEY_FINDING_PREFIX}'{legacy}' → rename to '{canonical}' "
        "(still works via alias)"
    )


def _scan_legacy_keys(path: Optional[str]) -> List:
    """Return the ``(legacy, canonical)`` deprecated-key pairs in a raw YAML file.

    Scans the un-normalized YAML because alias normalization happens at load time
    — a loaded config no longer carries the legacy keys. A missing or malformed
    file yields nothing; those are surfaced by the config/profile checks, not here.
    """
    from dlt_saga.pipeline_config.compat import find_legacy_keys
    from dlt_saga.utility.yaml_io import load_yaml

    if not path or not os.path.exists(path):
        return []
    try:
        raw = load_yaml(path)
    except Exception:
        return []
    return find_legacy_keys(raw)


def _project_scan_paths(source: ConfigSource) -> List[str]:
    """Non-pipeline YAML files to scan for deprecated keys: the project config and
    ``profiles.yml``, de-duplicated in stable order."""
    paths: List[str] = []
    project_path = getattr(source, "project_config_path", None)
    if project_path:
        paths.append(str(project_path))
    try:
        from dlt_saga.utility.cli.profiles import get_profiles_config

        profiles_path = get_profiles_config().profiles_path
        if profiles_path:
            paths.append(str(profiles_path))
    except Exception:
        # A missing/broken profiles.yml is the profile check's concern, not this
        # advisory's.
        pass
    return list(dict.fromkeys(paths))


def _validate_collisions(
    configs: List[PipelineConfig],
    source: ConfigSource,
    environments: Optional[List[str]],
    by_name: Dict[str, ValidationResult],
) -> List[ValidationResult]:
    """Attach a target-collision error to each selected config that shares a
    destination table with another enabled config.

    Detection is project-wide (a selected pipeline can collide with a non-selected
    one), so the message names every colliding pipeline even when the partner has
    no result in this selection. Offline — resolves names only, never connects.

    Returns a synthetic ``kind="check"`` result if the check itself could not run
    (so the caller reports "couldn't check" rather than falsely claiming clean);
    an empty list otherwise.
    """
    from dlt_saga.utility.collisions import collisions_for_selection

    if not configs:
        return []

    if environments is None:
        from dlt_saga.utility.naming import get_environment

        # Audit the current environment and prod (deduped), so a dev run also
        # flags a latent prod-only collision before it ships.
        current = get_environment()
        environments = [current] if current == "prod" else [current, "prod"]

    try:
        collisions = collisions_for_selection(
            configs,
            source,
            environments=environments,
            check_ingest=True,
            check_historize=True,
        )
    except Exception as exc:  # best-effort — never let the probe break validation
        logger.warning("Target-collision check could not run: %s", exc)
        return [
            ValidationResult(
                pipeline_name="<target-collision-check>",
                warnings=[f"{COLLISION_CHECK_FAILED_PREFIX} {exc}"],
                kind="check",
            )
        ]

    for collision in collisions:
        target = ".".join(p for p in (collision.schema, collision.display_table) if p)
        # One canonical message naming the whole group, attached identically to
        # every participant. Identical strings let the CLI dedupe and show the
        # collision once (grouped), while each pipeline's result still carries it
        # so `is_valid` and programmatic consumers see it per-config.
        message = (
            f"{COLLISION_FINDING_PREFIX} {target} "
            f"({collision.layer}, {collision.env_label}) "
            f"← {', '.join(collision.pipelines)}"
        )
        for name in collision.pipelines:
            result = by_name.get(name)
            if result is not None:
                result.errors.append(message)
    return []


def _validate_legacy_keys(
    configs: List[PipelineConfig],
    source: ConfigSource,
    by_name: Dict[str, ValidationResult],
) -> List[ValidationResult]:
    """Warn about deprecated alias keys in the raw YAML (never fatal — aliases
    still resolve at load time).

    Each selected config file's findings attach as warnings to that config's
    result; ``saga_project.yml`` / ``profiles.yml`` findings become a synthetic
    result per file. Returns those synthetic results (empty when clean).
    """
    for config in configs:
        result = by_name.get(config.pipeline_name)
        path = config.config_dict.get("config_path")
        if result is None or not path:
            continue
        for legacy, canonical in _scan_legacy_keys(path):
            result.warnings.append(_legacy_key_message(legacy, canonical))

    project_results: List[ValidationResult] = []
    for path in _project_scan_paths(source):
        pairs = _scan_legacy_keys(path)
        if pairs:
            project_results.append(
                ValidationResult(
                    pipeline_name=path,
                    warnings=[_legacy_key_message(le, ca) for le, ca in pairs],
                    kind="project",
                )
            )
    return project_results


def validate_pipeline_configs(
    configs: List[PipelineConfig],
    source: ConfigSource,
    *,
    environments: Optional[List[str]] = None,
) -> List[ValidationResult]:
    """Validate a selection of pipeline configs as a set.

    Runs the per-config offline checks (:func:`validate_pipeline_config`) plus
    two project-wide checks a single config can't see on its own:

    * **Target collisions** — two pipelines resolving to the same destination
      table (errors, attached to each involved config in the selection).
    * **Deprecated config keys** — legacy alias keys in the selected config files
      and in ``saga_project.yml`` / ``profiles.yml`` (warnings; aliases still
      resolve).

    Every check is offline — no warehouse connection or credentials. This is the
    complete correctness gate behind ``saga validate`` and
    :meth:`dlt_saga.Session.validate`.

    Args:
        configs: The selected configs to validate (already flattened).
        source: The config source — resolves collision targets and locates the
            project/profile files for the deprecated-key scan.
        environments: Environments to resolve collision targets in. When None,
            audits the current environment plus prod.

    Returns:
        One ``kind="pipeline"`` :class:`ValidationResult` per selected config,
        followed by any synthetic results (``kind="project"`` per project/profile
        file with deprecated keys, ``kind="check"`` if a project-wide check could
        not run). Empty list when ``configs`` is empty.
    """
    results = [validate_pipeline_config(c) for c in configs]
    if not results:
        return []

    by_name = {r.pipeline_name: r for r in results}
    collision_results = _validate_collisions(configs, source, environments, by_name)
    project_results = _validate_legacy_keys(configs, source, by_name)

    return results + collision_results + project_results
