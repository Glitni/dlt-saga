"""Tests for numeric bounds validation and format validation in config dataclasses."""

import pytest


@pytest.mark.unit
class TestApiConfigBounds:
    """Numeric bounds checks for ApiConfig."""

    def _make(self, **overrides):
        from dlt_saga.pipelines.api.config import ApiConfig

        defaults = {"base_url": "https://api.example.com", "endpoint": "/v1/data"}
        return ApiConfig(**{**defaults, **overrides})

    def test_negative_timeout_rejected(self):
        with pytest.raises(ValueError, match="timeout must be >= 1"):
            self._make(timeout=0)

    def test_negative_max_table_nesting_rejected(self):
        with pytest.raises(ValueError, match="max_table_nesting must be >= 0"):
            self._make(max_table_nesting=-1)

    def test_negative_max_retries_rejected(self):
        with pytest.raises(ValueError, match="max_retries must be >= 0"):
            self._make(max_retries=-1)

    def test_zero_retry_backoff_base_rejected(self):
        with pytest.raises(ValueError, match="retry_backoff_base must be >= 1"):
            self._make(retry_backoff_base=0)

    def test_negative_page_delay_rejected(self):
        with pytest.raises(ValueError, match="page_delay must be >= 0"):
            self._make(page_delay=-0.5)

    def test_valid_defaults_accepted(self):
        config = self._make()
        assert config.timeout == 30
        assert config.max_table_nesting == 0
        assert config.max_retries == 3


@pytest.mark.unit
class TestDatabaseConfigBounds:
    """Numeric bounds checks for DatabaseConfig."""

    def _make(self, **overrides):
        from dlt_saga.pipelines.database.config import DatabaseConfig

        defaults = {
            "database_type": "postgres",
            "host": "localhost",
            "source_database": "mydb",
            "source_table": "mytable",
        }
        return DatabaseConfig(**{**defaults, **overrides})

    def test_port_zero_rejected(self):
        with pytest.raises(ValueError, match="port must be 1-65535"):
            self._make(port=0)

    def test_port_too_high_rejected(self):
        with pytest.raises(ValueError, match="port must be 1-65535"):
            self._make(port=70000)

    def test_negative_max_retries_rejected(self):
        with pytest.raises(ValueError, match="max_retries must be >= 0"):
            self._make(max_retries=-1)

    def test_zero_retry_backoff_base_rejected(self):
        with pytest.raises(ValueError, match="retry_backoff_base must be >= 1"):
            self._make(retry_backoff_base=0)

    def test_zero_partition_num_rejected(self):
        with pytest.raises(ValueError, match="partition_num must be >= 1"):
            self._make(partition_num=0)

    def test_valid_port_accepted(self):
        config = self._make(port=5432)
        assert config.port == 5432

    def test_none_port_accepted(self):
        config = self._make()
        assert config.port is None


@pytest.mark.unit
class TestFilesystemConfigBounds:
    """Numeric bounds checks for FilesystemConfig."""

    def _make(self, **overrides):
        from dlt_saga.pipelines.filesystem.config import FilesystemConfig

        defaults = {
            "filesystem_type": "gs",
            "bucket_name": "my-bucket",
            "file_type": "csv",
            "file_glob": "*.csv",
        }
        return FilesystemConfig(**{**defaults, **overrides})

    def test_port_zero_rejected(self):
        with pytest.raises(ValueError, match="port must be 1-65535"):
            self._make(port=0)

    def test_port_too_high_rejected(self):
        with pytest.raises(ValueError, match="port must be 1-65535"):
            self._make(port=99999)

    def test_valid_port_accepted(self):
        config = self._make(port=22)
        assert config.port == 22


@pytest.mark.unit
class TestApiConfigUrlValidation:
    """URL format validation for ApiConfig."""

    def test_invalid_base_url_scheme_rejected(self):
        from dlt_saga.pipelines.api.config import ApiConfig

        with pytest.raises(ValueError, match="base_url must start with http"):
            ApiConfig(base_url="ftp://example.com", endpoint="/v1/data")

    def test_http_accepted(self):
        from dlt_saga.pipelines.api.config import ApiConfig

        config = ApiConfig(base_url="http://example.com", endpoint="/v1/data")
        assert config.base_url == "http://example.com"

    def test_https_accepted(self):
        from dlt_saga.pipelines.api.config import ApiConfig

        config = ApiConfig(base_url="https://example.com", endpoint="/v1/data")
        assert config.base_url == "https://example.com"


@pytest.mark.unit
class TestFilesystemConfigFormatValidation:
    """Format validation for FilesystemConfig."""

    def _make(self, **overrides):
        from dlt_saga.pipelines.filesystem.config import FilesystemConfig

        defaults = {
            "filesystem_type": "gs",
            "bucket_name": "my-bucket",
            "file_type": "csv",
            "file_glob": "*.csv",
        }
        return FilesystemConfig(**{**defaults, **overrides})

    def test_valid_snapshot_date_format_accepted(self):
        config = self._make(
            snapshot_date_regex=r"(\d{4}-\d{2}-\d{2})",
            snapshot_date_format="%Y-%m-%d",
        )
        assert config.snapshot_date_format == "%Y-%m-%d"

    def test_valid_compact_format_accepted(self):
        config = self._make(
            snapshot_date_regex=r"(\d{8})",
            snapshot_date_format="%Y%m%d",
        )
        assert config.snapshot_date_format == "%Y%m%d"


@pytest.mark.unit
class TestTargetConfigColumnValidation:
    """SQL identifier and constraint checks for TargetConfig."""

    def _make(self, **overrides):
        from dlt_saga.pipelines.target.config import TargetConfig

        return TargetConfig(**overrides)

    def test_valid_partition_column_accepted(self):
        config = self._make(partition_column="_dlt_ingested_at")
        assert config.partition_column == "_dlt_ingested_at"

    def test_invalid_partition_column_rejected(self):
        with pytest.raises(
            ValueError, match="partition_column must be a valid SQL identifier"
        ):
            self._make(partition_column="has space")

    def test_partition_column_with_leading_digit_rejected(self):
        with pytest.raises(
            ValueError, match="partition_column must be a valid SQL identifier"
        ):
            self._make(partition_column="1column")

    def test_partition_column_none_accepted(self):
        config = self._make(partition_column=None)
        assert config.partition_column is None

    def test_valid_cluster_columns_accepted(self):
        config = self._make(cluster_columns=["id", "created_at", "_dlt_ingested_at"])
        assert config.cluster_columns == ["id", "created_at", "_dlt_ingested_at"]

    def test_cluster_columns_max_4_accepted(self):
        config = self._make(cluster_columns=["a", "b", "c", "d"])
        assert len(config.cluster_columns) == 4

    def test_cluster_columns_exceeds_max_rejected(self):
        with pytest.raises(ValueError, match="cluster_columns supports at most 4"):
            self._make(cluster_columns=["a", "b", "c", "d", "e"])

    def test_cluster_columns_invalid_identifier_rejected(self):
        with pytest.raises(
            ValueError, match="cluster_columns contains invalid SQL identifier"
        ):
            self._make(cluster_columns=["valid_col", "has-dash"])

    def test_cluster_columns_none_accepted(self):
        config = self._make(cluster_columns=None)
        assert config.cluster_columns is None


@pytest.mark.unit
class TestTargetConfigInsertApiValidation:
    """Insert-API validation for TargetConfig (Databricks Zerobus)."""

    def _make(self, **overrides):
        from dlt_saga.pipelines.target.config import TargetConfig

        return TargetConfig(**overrides)

    def test_insert_api_none_accepted(self):
        config = self._make()
        assert config.insert_api is None

    def test_zerobus_with_append_accepted(self):
        config = self._make(write_disposition="append", insert_api="zerobus")
        assert config.insert_api == "zerobus"

    def test_zerobus_with_append_historize_accepted(self):
        config = self._make(write_disposition="append+historize", insert_api="zerobus")
        assert config.insert_api == "zerobus"

    def test_zerobus_with_merge_rejected(self):
        with pytest.raises(
            ValueError, match="insert_api='zerobus' requires write_disposition"
        ):
            self._make(write_disposition="merge", insert_api="zerobus")

    def test_zerobus_with_replace_rejected(self):
        with pytest.raises(
            ValueError, match="insert_api='zerobus' requires write_disposition"
        ):
            self._make(write_disposition="replace", insert_api="zerobus")

    def test_zerobus_with_historize_only_rejected(self):
        # 'historize' (external delivery) has no ingest path, so Zerobus
        # is not applicable.
        with pytest.raises(
            ValueError, match="insert_api='zerobus' requires write_disposition"
        ):
            self._make(write_disposition="historize", insert_api="zerobus")

    def test_copy_into_with_any_disposition_accepted(self):
        for disposition in ("append", "merge", "replace", "append+historize"):
            config = self._make(write_disposition=disposition, insert_api="copy_into")
            assert config.insert_api == "copy_into"

    def test_invalid_insert_api_value_rejected(self):
        with pytest.raises(ValueError, match="insert_api must be one of"):
            self._make(write_disposition="append", insert_api="bogus")


@pytest.mark.unit
class TestTargetConfigInsertOnlyMergeStrategy:
    """Validation for merge_strategy='insert-only'."""

    def _make(self, **overrides):
        from dlt_saga.pipelines.target.config import TargetConfig

        return TargetConfig(**overrides)

    def test_insert_only_string_accepted_and_normalized_to_enum(self):
        from dlt_saga.pipelines.target.config import MergeStrategy

        config = self._make(
            write_disposition="merge",
            merge_strategy="insert-only",
            primary_key="id",
        )
        assert config.merge_strategy == MergeStrategy.INSERT_ONLY

    def test_insert_only_without_primary_key_rejected(self):
        with pytest.raises(
            ValueError, match="merge_strategy='insert-only' requires primary_key"
        ):
            self._make(write_disposition="merge", merge_strategy="insert-only")

    def test_insert_only_with_merge_key_rejected(self):
        with pytest.raises(
            ValueError,
            match="merge_strategy='insert-only' does not support merge_key",
        ):
            self._make(
                write_disposition="merge",
                merge_strategy="insert-only",
                primary_key="id",
                merge_key="other_id",
            )

    def test_insert_only_with_composite_primary_key_accepted(self):
        config = self._make(
            write_disposition="merge",
            merge_strategy="insert-only",
            primary_key=["org_id", "event_id"],
        )
        assert config.primary_key == ["org_id", "event_id"]


@pytest.mark.unit
class TestHistorizeConfigColumnValidation:
    """SQL identifier checks for HistorizeConfig."""

    def _make(self, **overrides):
        from dlt_saga.historize.config import HistorizeConfig

        return HistorizeConfig(**overrides)

    def test_default_partition_column_accepted(self):
        config = self._make()
        assert config.partition_column == "_dlt_valid_from"

    def test_invalid_partition_column_rejected(self):
        with pytest.raises(
            ValueError, match="partition_column must be a valid SQL identifier"
        ):
            self._make(partition_column="not valid!")

    def test_valid_cluster_columns_accepted(self):
        config = self._make(cluster_columns=["orgnr", "period"])
        assert config.cluster_columns == ["orgnr", "period"]

    def test_cluster_columns_invalid_identifier_rejected(self):
        with pytest.raises(
            ValueError, match="cluster_columns contains invalid SQL identifier"
        ):
            self._make(cluster_columns=["ok", "not ok"])
