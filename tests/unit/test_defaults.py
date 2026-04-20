"""Unit tests for dlt_saga.defaults module."""

import pytest

from dlt_saga.defaults import DEFAULTS, NORMALIZE_POOL_TYPE, _reset, apply_dlt_defaults


@pytest.fixture(autouse=True)
def _reset_defaults():
    """Reset applied state between tests."""
    _reset()
    yield
    _reset()


@pytest.mark.unit
class TestApplyDltDefaults:
    def test_registers_provider(self):
        """Defaults should be accessible via dlt.config after apply."""
        import dlt

        apply_dlt_defaults()
        # The DictionaryProvider is lowest priority, but if nothing else
        # sets these values, they should be retrievable.
        assert dlt.config.get("normalize.workers") == 4
        assert dlt.config.get("load.workers") == 2

    def test_idempotent(self):
        """Calling apply_dlt_defaults twice should not raise."""
        apply_dlt_defaults()
        # Second call should be a no-op (guarded by _applied flag)
        apply_dlt_defaults()

        import dlt

        # Still works after double call
        assert dlt.config.get("normalize.workers") == 4

    def test_defaults_dict_has_expected_keys(self):
        """Sanity check: DEFAULTS dict has the expected structure."""
        assert "normalize" in DEFAULTS
        assert "load" in DEFAULTS
        assert "destination" in DEFAULTS
        assert DEFAULTS["normalize"]["workers"] == 4
        assert DEFAULTS["load"]["delete_completed_jobs"] is True
        assert DEFAULTS["destination"]["replace_strategy"] == "insert-from-staging"

    def test_normalize_pool_type_patched(self):
        """NormalizeConfiguration.on_resolved should use thread pool."""
        from dlt.normalize.configuration import NormalizeConfiguration

        apply_dlt_defaults()

        config = NormalizeConfiguration()
        config.workers = 4
        config.on_resolved()
        assert config.pool_type == NORMALIZE_POOL_TYPE

    def test_normalize_pool_type_single_worker(self):
        """Single worker should still use 'none' pool type."""
        from dlt.normalize.configuration import NormalizeConfiguration

        apply_dlt_defaults()

        config = NormalizeConfiguration()
        config.workers = 1
        config.on_resolved()
        assert config.pool_type == "none"
