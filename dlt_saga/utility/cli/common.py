"""Shared CLI helpers used by both ingest and historize commands.

Extracted from cli.py to avoid duplication between entry points.
"""

import logging
import os
from typing import TYPE_CHECKING, Callable, Dict, List, Optional, Tuple

if TYPE_CHECKING:
    from dlt_saga.utility.cli.profiles import ProfileTarget

import typer

from dlt_saga.destinations.factory import DestinationFactory
from dlt_saga.pipeline_config import ConfigSource, FilePipelineConfig, PipelineConfig
from dlt_saga.project_config import get_config_source_settings, get_project_config
from dlt_saga.utility.auth.providers import (
    AuthenticationError,
    AuthProvider,
    get_auth_provider,
)
from dlt_saga.utility.cli.context import get_execution_context, set_execution_context
from dlt_saga.utility.cli.profiles import get_profiles_config
from dlt_saga.utility.cli.selectors import PipelineSelector
from dlt_saga.utility.env import get_env

logger = logging.getLogger(__name__)

# Config source singleton
_config_source: Optional[ConfigSource] = None


def get_config_source() -> ConfigSource:
    """Get or create the config source singleton.

    Reads config_source.type and config_source.path from saga_project.yml.
    Defaults to file-based config source with path="configs".
    """
    global _config_source
    if _config_source is None:
        config_source_settings = get_config_source_settings()
        source_type = config_source_settings.type

        if source_type == "file":
            root_dir = config_source_settings.path
            _config_source = FilePipelineConfig(root_dir=root_dir)
        else:
            logger.error(
                f"Unknown config_source type: '{source_type}'. "
                f"Currently only 'file' is supported."
            )
            raise typer.Exit(1)

    return _config_source


def setup_logging(verbose: bool) -> None:
    """Set up debug logging if verbose flag or env var is set."""
    if verbose or (get_env("SAGA_DEBUG_LOGGING") or "").lower() == "true":
        logging.getLogger().setLevel(logging.DEBUG)
        if (get_env("SAGA_DEBUG_LOGGING") or "").lower() == "true":
            logger.debug(
                "Debug logging enabled via SAGA_DEBUG_LOGGING environment variable"
            )


def check_cloud_run_environment() -> bool:
    """Check if running in Cloud Run.

    Returns:
        True if in Cloud Run, False otherwise
    """
    return os.getenv("K_SERVICE") is not None or os.getenv("CLOUD_RUN_JOB") is not None


def resolve_profile_name(profile: Optional[str]) -> str:
    """Resolve the profile name using the precedence chain.

    Resolution order (highest to lowest priority):
    1. ``--profile`` CLI flag (passed as ``profile`` argument, not None)
    2. ``SAGA_PROFILE`` environment variable
    3. ``profile:`` key in ``saga_project.yml``
    4. ``"default"`` hardcoded fallback

    Args:
        profile: Value from ``--profile`` CLI flag, or None if not provided.

    Returns:
        Resolved profile name.
    """
    if profile is not None:
        return profile
    env_profile = os.getenv("SAGA_PROFILE")
    if env_profile:
        return env_profile
    project_profile = get_project_config().profile
    if project_profile:
        return project_profile
    return "default"


def load_profile_config(profile: Optional[str], target: Optional[str]):
    """Load profile configuration from profiles.yml.

    ``profile`` may be None when the ``--profile`` flag was not passed; the
    full resolution chain is applied via :func:`resolve_profile_name`.

    Returns:
        ProfileTarget or None if not available
    """
    resolved = resolve_profile_name(profile)
    profiles_config = get_profiles_config()
    profile_target = None

    if profiles_config.profiles_exist():
        profile_target = profiles_config.get_target(resolved, target)

    return profile_target


def setup_execution_context(
    profile_target,
    force: bool = False,
    full_refresh: bool = False,
    update_access: bool = False,
    start_value_override: Optional[str] = None,
    end_value_override: Optional[str] = None,
) -> None:
    """Set up execution context and validate destination type."""
    set_execution_context(
        profile_target,
        force=force,
        full_refresh=full_refresh,
        update_access=update_access,
        start_value_override=start_value_override,
        end_value_override=end_value_override,
    )

    context = get_execution_context()
    destination_type = context.get_destination_type()

    if not destination_type:
        logger.error(
            "No destination type configured. "
            "Set 'type:' in profiles.yml (e.g. bigquery, duckdb)"
        )
        raise typer.Exit(1)

    if destination_type not in DestinationFactory._registry:
        available = ", ".join(sorted(DestinationFactory._registry.keys()))
        logger.error(
            f"Configuration error: Unknown destination type: '{destination_type}'. "
            f"Available types: {available}"
        )
        raise typer.Exit(1)


def discover_and_select_configs(
    select: Optional[List[str]],
    filter_fn=None,
) -> Tuple[Dict[str, List[PipelineConfig]], Dict[str, List[PipelineConfig]]]:
    """Discover and select pipeline configs.

    Args:
        select: Selector expressions
        filter_fn: Optional function to filter configs before selection.
                   Takes a PipelineConfig, returns True to keep.

    Returns:
        Tuple of (selected_enabled_configs, selected_disabled_configs)
    """
    config_source = get_config_source()
    all_enabled_configs, all_disabled_configs = config_source.discover()

    # Apply additional filter if provided
    if filter_fn:
        filtered_enabled: Dict[str, List[PipelineConfig]] = {}
        for ptype, configs in all_enabled_configs.items():
            kept = [c for c in configs if filter_fn(c)]
            if kept:
                filtered_enabled[ptype] = kept
        all_enabled_configs = filtered_enabled

    enabled_selector = PipelineSelector(all_enabled_configs)
    selected_configs = enabled_selector.select(select)

    disabled_selector = PipelineSelector(all_disabled_configs)
    selected_disabled_configs = disabled_selector.select(select)

    return selected_configs, selected_disabled_configs


def validate_credentials(
    in_cloud_run: bool, auth_provider: Optional[AuthProvider] = None
) -> None:
    """Validate credentials using the active auth provider.

    Args:
        in_cloud_run: Whether running in Cloud Run (skip validation).
        auth_provider: AuthProvider to use. If None, resolves from execution context.
    """
    if in_cloud_run:
        return

    if auth_provider is None:
        context = get_execution_context()
        profile_target = context.profile_target
        auth_provider = get_auth_provider(
            auth_provider=profile_target.auth_provider if profile_target else None,
            destination_type=context.get_destination_type(),
        )

    try:
        auth_provider.validate()
    except AuthenticationError as e:
        logger.error(str(e))
        raise typer.Exit(1)


def flatten_configs(configs: Dict[str, List[PipelineConfig]]) -> List[PipelineConfig]:
    """Flatten config dictionary into a single list."""
    all_configs = []
    for pipeline_configs in configs.values():
        all_configs.extend(pipeline_configs)
    return all_configs


def execute_with_impersonation(
    profile_target: Optional["ProfileTarget"], callback: Callable[[], None]
) -> None:
    """Execute a callback with optional identity impersonation.

    Uses the AuthProvider resolved from the destination type to handle
    impersonation setup and teardown.

    Args:
        profile_target: Profile target (may have run_as identity)
        callback: Function to call (no args)
    """
    run_as = profile_target.run_as if profile_target else None

    if run_as:
        auth_provider = get_auth_provider(
            auth_provider=profile_target.auth_provider if profile_target else None,
            destination_type=profile_target.destination_type
            if profile_target
            else None,
        )

        try:
            with auth_provider.impersonate(run_as):
                callback()
        except AuthenticationError as e:
            logger.error(str(e))
            raise typer.Exit(1)
    else:
        callback()
