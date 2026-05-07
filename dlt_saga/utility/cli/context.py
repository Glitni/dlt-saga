"""Execution context for passing profile and environment configuration.

This module provides a way to pass profile-based configuration through
the pipeline execution chain without adding parameters to every function.
"""

from contextlib import contextmanager
from typing import Any, Dict, Generator, Optional

from dlt_saga.utility.cli.profiles import ProfileTarget

# Global execution context
_execution_context: Optional["ExecutionContext"] = None


class ExecutionContext:
    """Holds execution context including active profile target."""

    def __init__(
        self,
        profile_target: Optional[ProfileTarget] = None,
        force: bool = False,
        full_refresh: bool = False,
        update_access: bool = False,
        start_value_override: Optional[str] = None,
        end_value_override: Optional[str] = None,
    ):
        """Initialize execution context.

        Args:
            profile_target: Active profile target, or None to use environment variables
            force: Force execution even if source hasn't changed
            full_refresh: Drop existing pipeline state/tables before running
            update_access: Only update access controls without running pipelines
            start_value_override: Override start value for incremental loading (backfill)
            end_value_override: Override end value for incremental loading (backfill)
        """
        self.profile_target = profile_target
        self.force = force
        self.full_refresh = full_refresh
        self.update_access = update_access
        self.start_value_override = start_value_override
        self.end_value_override = end_value_override

    def get_database(self) -> Optional[str]:
        """Get the database (GCP project, Snowflake account, etc.) from profile."""
        if self.profile_target:
            return self.profile_target.database
        return None

    def get_environment(self) -> Optional[str]:
        """Get the environment from profile or None."""
        if self.profile_target:
            return self.profile_target.environment
        return None

    def get_location(self) -> Optional[str]:
        """Get the location from destination config or None."""
        if self.profile_target:
            return self.profile_target.location
        return None

    def get_schema(self) -> Optional[str]:
        """Get the schema/dataset name from profile or None."""
        if self.profile_target:
            return self.profile_target.schema
        return None

    def has_run_as(self) -> bool:
        """Check if identity impersonation (run_as) is configured."""
        return (
            self.profile_target is not None and self.profile_target.run_as is not None
        )

    def get_run_as(self) -> Optional[str]:
        """Get the identity to impersonate (run_as), or None."""
        if self.profile_target:
            return self.profile_target.run_as
        return None

    def get_destination_type(self) -> Optional[str]:
        """Get the destination type from profile, or None if no profile is set."""
        if self.profile_target:
            return self.profile_target.destination_type
        return None

    def get_dev_row_limit(self) -> Optional[int]:
        """Get the dev_row_limit from profile or None.

        When set, pipelines apply this as a best-effort row cap per resource
        during extraction, allowing dev environments to stop early for faster
        testing. Production profiles omit this field entirely.
        """
        if self.profile_target:
            return self.profile_target.dev_row_limit
        return None

    def get_database_path(self) -> Optional[str]:
        """Get the DuckDB database path from destination config or None."""
        if self.profile_target:
            return self.profile_target.database_path
        return None

    def get_storage_path(self) -> Optional[str]:
        """Get the storage path (Iceberg warehouse) from destination config or None."""
        if self.profile_target:
            return self.profile_target.storage_path
        return None

    def get_table_format(self) -> str:
        """Get the default table format from profile.

        Returns:
            Table format string ("native" or "iceberg"), defaults to "native"
        """
        if self.profile_target:
            return self.profile_target.table_format
        return "native"

    def get_historize_table_format(self) -> Optional[str]:
        """Get the historize-layer table_format override from profile.historize, or None."""
        if self.profile_target:
            return self.profile_target.historize_table_format
        return None

    def get_historize_storage_path(self) -> Optional[str]:
        """Get the historize-layer storage_path override from profile.historize, or None."""
        if self.profile_target:
            return self.profile_target.historize_storage_path
        return None

    def get_destination_config(self) -> Dict[str, Any]:
        """Get the full destination-specific config dict."""
        if self.profile_target:
            return self.profile_target.destination_config
        return {}


def set_execution_context(
    profile_target: Optional[ProfileTarget],
    force: bool = False,
    full_refresh: bool = False,
    update_access: bool = False,
    start_value_override: Optional[str] = None,
    end_value_override: Optional[str] = None,
) -> None:
    """Set the global execution context.

    Args:
        profile_target: Profile target to use, or None for environment variables
        force: Force execution even if source hasn't changed
        full_refresh: Drop existing pipeline state/tables before running
        update_access: Only update access controls without running pipelines
        start_value_override: Override start value for incremental loading (backfill)
        end_value_override: Override end value for incremental loading (backfill)
    """
    global _execution_context
    _execution_context = ExecutionContext(
        profile_target,
        force,
        full_refresh,
        update_access,
        start_value_override,
        end_value_override,
    )


def get_execution_context() -> ExecutionContext:
    """Get the current execution context.

    Returns:
        ExecutionContext instance (creates empty one if not set)
    """
    global _execution_context
    if _execution_context is None:
        _execution_context = ExecutionContext()
    return _execution_context


def clear_execution_context() -> None:
    """Clear the global execution context."""
    global _execution_context
    _execution_context = None


@contextmanager
def execution_context_scope(
    profile_target: Optional[ProfileTarget],
    force: bool = False,
    full_refresh: bool = False,
    update_access: bool = False,
    start_value_override: Optional[str] = None,
    end_value_override: Optional[str] = None,
) -> Generator[ExecutionContext, None, None]:
    """Context manager that temporarily sets the global execution context.

    Saves the current context, sets a new one for the duration of the block,
    and restores the previous context on exit. This allows Session to set
    global state without permanently affecting other callers.

    Args:
        profile_target: Profile target to use.
        force: Force execution even if source hasn't changed.
        full_refresh: Drop existing pipeline state/tables before running.
        update_access: Only update access controls without running pipelines.
        start_value_override: Override start value for incremental loading.
        end_value_override: Override end value for incremental loading.

    Yields:
        The newly created ExecutionContext.
    """
    global _execution_context
    previous = _execution_context
    set_execution_context(
        profile_target,
        force=force,
        full_refresh=full_refresh,
        update_access=update_access,
        start_value_override=start_value_override,
        end_value_override=end_value_override,
    )
    try:
        yield _execution_context  # type: ignore[arg-type]
    finally:
        _execution_context = previous
