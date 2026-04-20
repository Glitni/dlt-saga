"""Pipeline config validation without execution.

Validates pipeline configs early — checks write_disposition, adapter
resolution, source config instantiation, and historize config.
"""

import importlib
import logging
from dataclasses import dataclass, field
from typing import List, Optional, Type

from dlt_saga.pipeline_config import PipelineConfig

logger = logging.getLogger(__name__)

# All valid write_disposition values
VALID_WRITE_DISPOSITIONS = {
    "append",
    "merge",
    "replace",
    "append+historize",
    "merge+historize",
    "replace+historize",
    "historize",
}


@dataclass
class ValidationResult:
    """Result of validating a single pipeline config."""

    pipeline_name: str
    errors: List[str] = field(default_factory=list)
    warnings: List[str] = field(default_factory=list)

    @property
    def is_valid(self) -> bool:
        return len(self.errors) == 0


def _find_config_class(pipeline_module) -> Optional[Type]:
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

    historize_dict = config.config_dict.get("historize", {})
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
    _validate_adapter(config, result)
    _validate_source_config(config, result)
    _validate_historize_config(config, result)

    return result
