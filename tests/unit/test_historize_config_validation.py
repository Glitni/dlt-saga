"""Unit tests for HistorizeConfig and HistorizeRunner validation."""

from unittest.mock import MagicMock

import pytest


@pytest.mark.unit
class TestHistorizeConfigValidation:
    """Tests for HistorizeConfig validation."""

    def test_empty_track_columns_raises(self):
        from dlt_saga.historize.config import HistorizeConfig

        config = HistorizeConfig(primary_key=["id"], track_columns=[])
        with pytest.raises(ValueError, match="track_columns must be either omitted"):
            config.validate()

    def test_none_track_columns_accepted(self):
        from dlt_saga.historize.config import HistorizeConfig

        config = HistorizeConfig(primary_key=["id"], track_columns=None)
        config.validate()  # should not raise

    def test_nonempty_track_columns_accepted(self):
        from dlt_saga.historize.config import HistorizeConfig

        config = HistorizeConfig(primary_key=["id"], track_columns=["col1", "col2"])
        config.validate()  # should not raise

    def test_missing_primary_key_raises(self):
        from dlt_saga.historize.config import HistorizeConfig

        config = HistorizeConfig()
        with pytest.raises(ValueError, match="primary_key"):
            config.validate()


@pytest.mark.unit
class TestHistorizeRunnerCapabilityValidation:
    """Tests for destination capability checks in HistorizeRunner."""

    def _make_runner(self, config_overrides=None, dest_overrides=None):
        from dlt_saga.historize.config import HistorizeConfig
        from dlt_saga.historize.runner import HistorizeRunner

        dest = MagicMock()
        dest.get_full_table_id.side_effect = lambda s, t: f"{s}.{t}"
        dest.supports_partitioning.return_value = False
        dest.supports_clustering.return_value = False
        if dest_overrides:
            for k, v in dest_overrides.items():
                getattr(dest, k).return_value = v

        config = HistorizeConfig(
            primary_key=["id"],
            **(config_overrides or {}),
        )

        runner = object.__new__(HistorizeRunner)
        runner.pipeline_name = "test__pipe"
        runner.config = config
        runner.destination = dest
        return runner

    def test_partition_column_on_unsupported_dest_raises(self):
        runner = self._make_runner(
            config_overrides={"partition_column": "my_partition_col"},
        )
        with pytest.raises(ValueError, match="does not support partitioning"):
            runner._validate_destination_capabilities()

    def test_cluster_columns_on_unsupported_dest_raises(self):
        runner = self._make_runner(
            config_overrides={"cluster_columns": ["col1"]},
        )
        with pytest.raises(ValueError, match="does not support clustering"):
            runner._validate_destination_capabilities()

    def test_default_partition_column_not_checked(self):
        """partition_column='_dlt_valid_from' (default) should NOT raise even when
        destination reports supports_partitioning=False — the default value is a
        no-op sentinel on destinations that don't support partitioning."""
        runner = self._make_runner(
            config_overrides={"partition_column": "_dlt_valid_from"},
        )
        runner._validate_destination_capabilities()  # should not raise

    def test_supported_partition_accepted(self):
        runner = self._make_runner(
            config_overrides={"partition_column": "my_col"},
            dest_overrides={"supports_partitioning": True, "supports_clustering": True},
        )
        runner._validate_destination_capabilities()  # should not raise


@pytest.mark.unit
class TestHistorizeRunnerAllColumnsIgnored:
    """Test that building a hash over zero columns raises early."""

    def _make_runner_with_dest(self, ignore_columns=None, track_columns=None):
        from dlt_saga.historize.config import HistorizeConfig
        from dlt_saga.historize.runner import HistorizeRunner
        from dlt_saga.historize.sql import HistorizeSqlBuilder

        dest = MagicMock()
        dest.get_full_table_id.side_effect = lambda s, t: f"{s}.{t}"
        dest.supports_partitioning.return_value = False
        dest.supports_clustering.return_value = False

        config = HistorizeConfig(
            primary_key=["id"],
            ignore_columns=ignore_columns or [],
            track_columns=track_columns,
        )

        runner = object.__new__(HistorizeRunner)
        runner.pipeline_name = "test__pipe"
        runner.config = config
        runner.destination = dest
        runner.schema = "test"
        runner.source_table_id = "test.source"

        # Stub sql_builder with a real instance so _get_hash_columns works
        sql_builder = object.__new__(HistorizeSqlBuilder)
        sql_builder.config = config
        sql_builder.primary_key = config.primary_key
        sql_builder._output_exclude = set()
        sql_builder.destination = dest
        sql_builder.source_database = ""
        sql_builder.source_schema = "test"
        sql_builder.source_table = "source"
        runner.sql_builder = sql_builder

        # Stub execute_sql to return fake column rows
        col_row = MagicMock()
        col_row.column_name = "city"
        dest.execute_sql.return_value = [col_row]

        return runner

    def test_all_value_columns_ignored_raises(self):
        runner = self._make_runner_with_dest(ignore_columns=["city"])
        with pytest.raises(ValueError, match="zero columns"):
            runner._discover_value_columns()

    def test_track_columns_not_in_table_raises(self):
        runner = self._make_runner_with_dest(track_columns=["nonexistent_col"])
        with pytest.raises(ValueError, match="zero columns"):
            runner._discover_value_columns()

    def test_valid_ignore_subset_accepted(self):
        from dlt_saga.historize.config import HistorizeConfig
        from dlt_saga.historize.runner import HistorizeRunner
        from dlt_saga.historize.sql import HistorizeSqlBuilder

        dest = MagicMock()
        dest.get_full_table_id.side_effect = lambda s, t: f"{s}.{t}"
        dest.supports_partitioning.return_value = False
        dest.supports_clustering.return_value = False

        config = HistorizeConfig(primary_key=["id"], ignore_columns=["city"])

        runner = object.__new__(HistorizeRunner)
        runner.pipeline_name = "test__pipe"
        runner.config = config
        runner.destination = dest
        runner.schema = "test"
        runner.source_table_id = "test.source"

        sql_builder = object.__new__(HistorizeSqlBuilder)
        sql_builder.config = config
        sql_builder.primary_key = config.primary_key
        sql_builder._output_exclude = set()
        sql_builder.destination = dest
        sql_builder.source_database = ""
        sql_builder.source_schema = "test"
        sql_builder.source_table = "source"
        runner.sql_builder = sql_builder

        # Return two value columns — city is ignored, country remains
        city_row = MagicMock()
        city_row.column_name = "city"
        country_row = MagicMock()
        country_row.column_name = "country"
        dest.execute_sql.return_value = [city_row, country_row]

        result = runner._discover_value_columns()
        assert result == [
            "city",
            "country",
        ]  # all value cols returned (filtering is in hash only)
