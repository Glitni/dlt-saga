"""Register dlt performance defaults as lowest-priority configuration.

Uses dlt's DictionaryProvider to provide sensible defaults that can be
overridden by the user's .dlt/config.toml or environment variables.

Priority order (highest to lowest):
1. Environment variables (e.g. NORMALIZE__WORKERS=8)
2. User's .dlt/config.toml
3. Package defaults (this module)
"""

import logging

logger = logging.getLogger(__name__)

_PROVIDER_NAME = "dlt-saga Package Defaults"
_applied = False

# Default dlt configuration values.
# These are performance-tuned defaults for the saga pipeline framework.
DEFAULTS = {
    "destination": {
        "replace_strategy": "insert-from-staging",
        # dlt-saga manages all schema/dataset naming. Prevent dlt from silently
        # rewriting names (e.g. consecutive underscores, casing) regardless of
        # which destination is in use.
        "enable_dataset_name_normalization": False,
        "bigquery": {
            "loader_file_format": "parquet",
        },
    },
    "normalize": {
        "workers": 4,
        "parquet_normalizer": {
            "add_dlt_id": False,
            "add_dlt_load_id": False,
        },
        "json_normalizer": {
            "add_dlt_id": False,
            "add_dlt_load_id": False,
        },
        "data_writer": {
            "file_max_items": 500000,
            "file_max_bytes": 104857600,
        },
    },
    "load": {
        "workers": 2,
        "delete_completed_jobs": True,
        "truncate_staging_dataset": True,
    },
}

# Pool type for normalize workers. Thread pool avoids fork()-after-init issues
# on Linux where grpcio>=1.80 and gcsfs>=2026.3 start background threads/state
# that don't survive fork(), causing BrokenProcessPool. Threading avoids fork
# entirely; normalization is I/O-bound so the GIL is not a bottleneck.
NORMALIZE_POOL_TYPE = "thread"


def _patch_normalize_pool_type() -> None:
    """Patch NormalizeConfiguration.on_resolved to use thread pool.

    dlt's NormalizeConfiguration.on_resolved() unconditionally overwrites
    pool_type to "process" (or "none" for workers=1), ignoring any value
    from config providers. We patch it to use our configured pool type.
    """
    from dlt.normalize.configuration import NormalizeConfiguration

    _original_on_resolved = NormalizeConfiguration.on_resolved

    def _on_resolved(self: "NormalizeConfiguration") -> None:
        _original_on_resolved(self)
        if self.workers != 1:
            self.pool_type = NORMALIZE_POOL_TYPE  # type: ignore[assignment]

    NormalizeConfiguration.on_resolved = _on_resolved  # type: ignore[method-assign]


def apply_dlt_defaults() -> None:
    """Register package defaults as the lowest-priority dlt config provider.

    Safe to call multiple times — only registers once.
    """
    global _applied
    if _applied:
        return

    import dlt
    from dlt.common.configuration.providers import DictionaryProvider

    class _SagaDefaultsProvider(DictionaryProvider):
        NAME = _PROVIDER_NAME

    provider = _SagaDefaultsProvider()
    provider._config_doc = DEFAULTS

    try:
        dlt.config.register_provider(provider)
    except Exception as e:
        # DuplicateConfigProviderException is expected on repeated calls.
        # Log anything else so we know defaults didn't register.
        logger.warning("Failed to register dlt defaults provider: %s", e)

    _patch_normalize_pool_type()

    _applied = True
    logger.debug("Registered dlt-saga package defaults as lowest-priority dlt config")


def _reset() -> None:
    """Reset applied state. For testing only."""
    global _applied
    _applied = False
