"""Unit tests for HistorizeConfig."""

import pytest

from dlt_saga.historize.config import HistorizeConfig


@pytest.mark.unit
class TestHistorizeConfigTableFormat:
    def test_table_format_defaults_to_none(self):
        config = HistorizeConfig.from_dict({}, top_level_primary_key=["id"])
        assert config.table_format is None

    def test_table_format_set_via_dict(self):
        config = HistorizeConfig.from_dict(
            {"table_format": "iceberg"}, top_level_primary_key=["id"]
        )
        assert config.table_format == "iceberg"

    @pytest.mark.parametrize("fmt", ["native", "iceberg", "delta", "delta_uniform"])
    def test_table_format_round_trips(self, fmt):
        config = HistorizeConfig.from_dict(
            {"table_format": fmt}, top_level_primary_key=["id"]
        )
        assert config.table_format == fmt

    def test_unknown_keys_ignored(self):
        """from_dict filters to known fields — extra keys should not raise."""
        config = HistorizeConfig.from_dict(
            {"table_format": "iceberg", "unknown_key": "value"},
            top_level_primary_key=["id"],
        )
        assert config.table_format == "iceberg"
        assert not hasattr(config, "unknown_key")
