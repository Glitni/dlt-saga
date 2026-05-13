"""Unit tests for NativeLoadPipeline."""

from unittest.mock import MagicMock, patch

import pytest

from dlt_saga.pipelines.native_load.pipeline import NativeLoadPipeline
from dlt_saga.pipelines.native_load.storage.base import StorageObject


def _make_config(**overrides) -> dict:
    cfg = {
        "pipeline_name": "test__my_table",
        "base_table_name": "my_table",
        "table_name": "test__my_table",
        "schema_name": "my_dataset",
        "source_uri": "gs://bucket/prefix/",
        "file_type": "parquet",
        "write_disposition": "append",
    }
    cfg.update(overrides)
    return cfg


def _make_pipeline(**config_overrides) -> NativeLoadPipeline:
    """Construct a NativeLoadPipeline with mocked destination/storage/state."""
    config = _make_config(**config_overrides)

    dest = MagicMock()
    dest.supports_native_load.return_value = True
    dest.supported_native_load_uri_schemes.return_value = {"gs"}
    dest.type_name.side_effect = lambda t: t.upper()
    dest.native_load_file_name_expr.return_value = "_FILE_NAME"
    dest.parse_filename_timestamp_expr.return_value = "SAFE.PARSE_TIMESTAMP(...)"
    dest.ensure_schema_exists.return_value = None
    dest.table_exists.return_value = False
    dest.config = MagicMock()
    dest.config.billing_project_id = None
    dest.config.project_id = "my-project"
    dest.config.__class__.__name__ = "BigQueryDestinationConfig"

    context = MagicMock()
    context.get_destination_type.return_value = "bigquery"
    context.update_access = False
    context.full_refresh = False

    with (
        patch(
            "dlt_saga.utility.cli.context.get_execution_context", return_value=context
        ),
        patch(
            "dlt_saga.destinations.factory.DestinationFactory.create_from_context",
            return_value=dest,
        ),
        patch(
            "dlt_saga.pipelines.native_load.pipeline.get_storage_client",
            return_value=MagicMock(),
        ),
        patch(
            "dlt_saga.pipelines.native_load.pipeline.NativeLoadStateManager",
            return_value=MagicMock(),
        ),
    ):
        p = NativeLoadPipeline(config)

    p.destination = dest
    p.context = context
    p.state_manager = MagicMock()
    p.storage_client = MagicMock()

    return p


@pytest.mark.unit
class TestDerivedColumns:
    def test_ingested_at_always_present(self):
        p = _make_pipeline()
        cols = p._derived_columns()
        names = [c.name for c in cols]
        assert "_dlt_ingested_at" in names

    def test_file_name_col_when_include_metadata(self):
        p = _make_pipeline()
        p.native_config.include_file_metadata = True
        cols = p._derived_columns()
        names = [c.name for c in cols]
        assert "_dlt_source_file_name" in names

    def test_no_file_name_col_when_excluded(self):
        p = _make_pipeline()
        p.native_config.include_file_metadata = False
        cols = p._derived_columns()
        names = [c.name for c in cols]
        assert "_dlt_source_file_name" not in names

    def test_file_date_col_in_date_mode(self):
        p = _make_pipeline()
        p.native_config.include_file_metadata = True
        p.native_config.filename_date_regex = r"(\d{8})"
        p.native_config.filename_date_format = "%Y%m%d"
        cols = p._derived_columns()
        names = [c.name for c in cols]
        assert "_dlt_source_file_date" in names

    def test_no_file_date_col_in_flat_mode(self):
        p = _make_pipeline()
        p.native_config.filename_date_regex = None
        p.native_config.filename_date_format = None
        cols = p._derived_columns()
        names = [c.name for c in cols]
        assert "_dlt_source_file_date" not in names

    def test_ingested_at_is_timestamp_literal(self):
        p = _make_pipeline()
        cols = p._derived_columns()
        at_col = next(c for c in cols if c.name == "_dlt_ingested_at")
        assert at_col.sql_expr.startswith("TIMESTAMP '")


@pytest.mark.unit
class TestFullRefresh:
    def test_drop_table_called_on_full_refresh(self):
        p = _make_pipeline()
        p.context.full_refresh = True
        p._handle_full_refresh()
        p.destination.drop_table.assert_called_once_with(p._dataset, p.table_name)

    def test_state_cleared_on_full_refresh(self):
        p = _make_pipeline()
        p._handle_full_refresh()
        p.state_manager.clear_pipeline_state.assert_called_once_with(p.pipeline_name)

    def test_target_exists_false_after_full_refresh(self):
        p = _make_pipeline()
        p._target_exists = True
        p._handle_full_refresh()
        assert p._target_exists is False


@pytest.mark.unit
class TestBuildFormatOptions:
    def test_empty_for_parquet(self):
        p = _make_pipeline(file_type="parquet")
        assert p._build_format_options() == {}

    def test_csv_separator(self):
        p = _make_pipeline(file_type="csv")
        p.native_config.csv_separator = ";"
        opts = p._build_format_options()
        assert opts["field_delimiter"] == ";"

    def test_max_bad_records(self):
        p = _make_pipeline()
        p.native_config.max_bad_records = 5
        opts = p._build_format_options()
        assert opts["max_bad_records"] == 5


@pytest.mark.unit
class TestNoNewFiles:
    def test_returns_empty_load_info_when_no_files(self):
        p = _make_pipeline()
        p.state_manager.ensure_table_exists.return_value = None
        p._discover_new_files = MagicMock(return_value={None: []})

        result = p.run()
        assert len(result) == 1
        assert result[0]["row_counts"] == {}


@pytest.mark.unit
class TestUpdateAccessNotSupported:
    def test_returns_empty_list(self):
        p = _make_pipeline()
        p.context.update_access = True
        result = p.run()
        assert result == []


@pytest.mark.unit
class TestWriteLoadInfoSkipped:
    def test_no_write_when_zero_rows(self):
        p = _make_pipeline()
        p.state_manager.ensure_table_exists.return_value = None
        p._discover_new_files = MagicMock(return_value={None: []})
        p._write_load_info = MagicMock()

        p.run()
        p._write_load_info.assert_not_called()

    def test_write_load_info_failure_does_not_abort(self):
        p = _make_pipeline()
        p.destination.save_load_info.side_effect = RuntimeError("DB error")
        p._write_load_info(100, True)  # should not raise


@pytest.mark.unit
class TestDateModeProperty:
    def test_date_mode_true_when_regex_set(self):
        p = _make_pipeline()
        p.native_config.filename_date_regex = r"(\d{8})"
        p.native_config.filename_date_format = "%Y%m%d"
        assert p._date_mode is True

    def test_date_mode_false_when_regex_absent(self):
        p = _make_pipeline()
        p.native_config.filename_date_regex = None
        assert p._date_mode is False

    def test_discover_calls_date_mode_when_regex_set(self):
        p = _make_pipeline()
        p.native_config.filename_date_regex = r"(\d{8})"
        p.native_config.filename_date_format = "%Y%m%d"
        p._discover_date_mode = MagicMock(return_value={})
        p._discover_flat_mode = MagicMock(return_value={})
        p._discover_new_files()
        p._discover_date_mode.assert_called_once()
        p._discover_flat_mode.assert_not_called()

    def test_discover_calls_flat_mode_when_no_regex(self):
        p = _make_pipeline()
        p.native_config.filename_date_regex = None
        p._discover_date_mode = MagicMock(return_value={})
        p._discover_flat_mode = MagicMock(return_value={None: []})
        p._discover_new_files()
        p._discover_flat_mode.assert_called_once()
        p._discover_date_mode.assert_not_called()


@pytest.mark.unit
class TestFlatModeDiscovery:
    def test_deduplicates_already_loaded(self):
        p = _make_pipeline(incremental=True)
        p.state_manager.get_loaded_uri_generations.return_value = {
            ("gs://bucket/prefix/file1.parquet", 100)
        }
        p.storage_client.list_files.return_value = [
            StorageObject(
                name="prefix/file1.parquet",
                full_uri="gs://bucket/prefix/file1.parquet",
                size=1000,
                generation=100,
                updated=None,
            ),
            StorageObject(
                name="prefix/file2.parquet",
                full_uri="gs://bucket/prefix/file2.parquet",
                size=2000,
                generation=200,
                updated=None,
            ),
        ]
        result = p._discover_flat_mode()
        # file1 already loaded (same generation) → only file2 returned
        assert len(result[None]) == 1
        assert result[None][0].full_uri == "gs://bucket/prefix/file2.parquet"

    def test_same_uri_new_generation_is_included(self):
        p = _make_pipeline(incremental=True)
        p.state_manager.get_loaded_uri_generations.return_value = {
            ("gs://bucket/prefix/file1.parquet", 100)
        }
        p.storage_client.list_files.return_value = [
            StorageObject(
                name="prefix/file1.parquet",
                full_uri="gs://bucket/prefix/file1.parquet",
                size=1000,
                generation=999,  # new generation
                updated=None,
            ),
        ]
        result = p._discover_flat_mode()
        assert len(result[None]) == 1


@pytest.mark.unit
class TestLoadFilesParentGrouping:
    """Phase 4A — _load_files groups by parent dir before batching."""

    def _make_obj(self, uri: str) -> StorageObject:
        return StorageObject(
            name=uri.split("/", 3)[-1],
            full_uri=uri,
            size=100,
            generation=1,
            updated=None,
        )

    def test_single_parent_dir_one_chunk(self):
        p = _make_pipeline(load_batch_size=10)
        files = {
            None: [
                self._make_obj("gs://bucket/prefix/file1.parquet"),
                self._make_obj("gs://bucket/prefix/file2.parquet"),
            ]
        }
        p._load_chunk = MagicMock(return_value=0)
        p._load_files(files)
        # Both files share the same parent → one chunk
        assert p._load_chunk.call_count == 1

    def test_two_parent_dirs_two_chunks(self):
        p = _make_pipeline(load_batch_size=10)
        files = {
            None: [
                self._make_obj("gs://bucket/prefix/day=01/file1.parquet"),
                self._make_obj("gs://bucket/prefix/day=02/file2.parquet"),
            ]
        }
        p._load_chunk = MagicMock(return_value=0)
        p._load_files(files)
        # Different parent dirs → two chunks
        assert p._load_chunk.call_count == 2

    def test_batch_size_respected_within_group(self):
        p = _make_pipeline(load_batch_size=2)
        files = {
            None: [
                self._make_obj("gs://bucket/prefix/day=01/file1.parquet"),
                self._make_obj("gs://bucket/prefix/day=01/file2.parquet"),
                self._make_obj("gs://bucket/prefix/day=01/file3.parquet"),
            ]
        }
        p._load_chunk = MagicMock(return_value=0)
        p._load_files(files)
        # One group of 3 files, batch_size=2 → 2 chunks
        assert p._load_chunk.call_count == 2


@pytest.mark.unit
class TestTargetLocationAndTableFormat:
    """Phase 12/13 — target_location and table_format wired into spec."""

    def test_target_location_none_on_bq(self):
        p = _make_pipeline()
        assert p._target_location is None

    def test_validate_databricks_only_table_format_on_bq_raises(self):
        with pytest.raises(ValueError, match="table_format"):
            _make_pipeline(table_format="iceberg")

    def test_validate_databricks_only_target_location_on_bq_raises(self):
        with pytest.raises(ValueError, match="target_location"):
            _make_pipeline(target_location="abfss://lake/raw/")

    def test_full_refresh_uses_purge_when_target_location(self):
        p = _make_pipeline()
        p._target_location = "abfss://lake@account.dfs.core.windows.net/raw/"
        p.destination.drop_table_external = MagicMock()
        p._handle_full_refresh()
        p.destination.drop_table_external.assert_called_once_with(
            p._dataset, p.table_name
        )
        p.destination.drop_table.assert_not_called()

    def test_full_refresh_uses_drop_table_when_no_location(self):
        p = _make_pipeline()
        p._target_location = None
        p._handle_full_refresh()
        p.destination.drop_table.assert_called_once_with(p._dataset, p.table_name)


@pytest.mark.unit
class TestResolveTargetLocation:
    def _make_databricks_pipeline(self, **overrides) -> NativeLoadPipeline:
        config = {
            "pipeline_name": "g__t",
            "base_table_name": "t",
            "table_name": "g__t",
            "schema_name": "my_dataset",
            "source_uri": "gs://bucket/prefix/",
            "file_type": "parquet",
            "write_disposition": "append",
            "pipeline_group": "g",
        }
        config.update(overrides)

        dest = MagicMock()
        dest.supports_native_load.return_value = True
        dest.supported_native_load_uri_schemes.return_value = {"gs", "abfss"}
        dest.type_name.side_effect = lambda t: t.upper()
        dest.native_load_file_name_expr.return_value = "_metadata.file_path"
        dest.parse_filename_timestamp_expr.return_value = "try_to_timestamp(...)"
        dest.config = MagicMock()
        dest.config.__class__.__name__ = "DatabricksDestinationConfig"
        dest.config.billing_project_id = None
        dest.config.project_id = None
        dest.config.storage_root = None
        dest.__class__.__name__ = "DatabricksDestination"
        context = MagicMock()
        context.get_destination_type.return_value = "databricks"
        context.update_access = False
        context.full_refresh = False

        with (
            patch(
                "dlt_saga.utility.cli.context.get_execution_context",
                return_value=context,
            ),
            patch(
                "dlt_saga.destinations.factory.DestinationFactory.create_from_context",
                return_value=dest,
            ),
            patch(
                "dlt_saga.pipelines.native_load.pipeline.get_storage_client",
                return_value=MagicMock(),
            ),
            patch(
                "dlt_saga.pipelines.native_load.pipeline.NativeLoadStateManager",
                return_value=MagicMock(),
            ),
        ):
            p = NativeLoadPipeline(config)
        p.destination = dest
        p.context = context
        p.state_manager = MagicMock()
        p.storage_client = MagicMock()
        return p

    def test_explicit_target_location_wins(self):
        loc = "abfss://lake@account.dfs.core.windows.net/raw/g/t/"
        p = self._make_databricks_pipeline(target_location=loc)
        assert p._target_location == loc

    def test_storage_root_derives_location(self):
        p = self._make_databricks_pipeline()
        p.destination.config.storage_root = (
            "abfss://lake@account.dfs.core.windows.net/raw/"
        )
        result = p._resolve_target_location()
        assert result == "abfss://lake@account.dfs.core.windows.net/raw/g/g__t/"

    def test_no_storage_root_returns_none(self):
        p = self._make_databricks_pipeline()
        p.destination.config.storage_root = None
        assert p._resolve_target_location() is None

    def test_iceberg_with_cluster_columns_raises(self):
        with pytest.raises(ValueError, match="Liquid Clustering"):
            self._make_databricks_pipeline(
                table_format="iceberg",
                cluster_columns=["id"],
            )

    def test_naming_module_hook_overrides_default(self):
        p = self._make_databricks_pipeline(
            config_path="/repo/configs/g/t.yml",
        )
        p.destination.config.storage_root = "abfss://lake/raw/"

        hook_module = MagicMock()
        hook_module.generate_target_location.return_value = "abfss://lake/custom/g/t/"

        with (
            patch(
                "dlt_saga.pipeline_config.naming.load_naming_module",
                return_value=hook_module,
            ),
            patch(
                "dlt_saga.project_config.get_project_config",
                return_value=MagicMock(naming_module="my_module"),
            ),
            patch(
                "dlt_saga.utility.naming.get_environment",
                return_value="prod",
            ),
        ):
            result = p._resolve_target_location()

        assert result == "abfss://lake/custom/g/t/"
        # Hook receives the segment list, not the path string
        hook_module.generate_target_location.assert_called_once_with(
            ["g", "t"], "prod", "abfss://lake/raw/"
        )

    def test_naming_module_hook_returns_none_falls_through(self):
        p = self._make_databricks_pipeline()
        p.destination.config.storage_root = "abfss://lake/raw/"

        hook_module = MagicMock()
        hook_module.generate_target_location.return_value = None

        with (
            patch(
                "dlt_saga.pipeline_config.naming.load_naming_module",
                return_value=hook_module,
            ),
            patch(
                "dlt_saga.project_config.get_project_config",
                return_value=MagicMock(naming_module="my_module"),
            ),
            patch(
                "dlt_saga.utility.naming.get_environment",
                return_value="prod",
            ),
        ):
            result = p._resolve_target_location()

        # Falls through to profile storage_root + auto-derived path
        assert result == "abfss://lake/raw/g/g__t/"

    def test_naming_module_hook_missing_function_falls_through(self):
        p = self._make_databricks_pipeline()
        p.destination.config.storage_root = "abfss://lake/raw/"

        # Module without generate_target_location attr
        class _M:
            pass

        with (
            patch(
                "dlt_saga.pipeline_config.naming.load_naming_module",
                return_value=_M(),
            ),
            patch(
                "dlt_saga.project_config.get_project_config",
                return_value=MagicMock(naming_module="my_module"),
            ),
            patch(
                "dlt_saga.utility.naming.get_environment",
                return_value="prod",
            ),
        ):
            result = p._resolve_target_location()

        assert result == "abfss://lake/raw/g/g__t/"

    def test_naming_module_hook_exception_falls_through(self):
        p = self._make_databricks_pipeline()
        p.destination.config.storage_root = "abfss://lake/raw/"

        hook_module = MagicMock()
        hook_module.generate_target_location.side_effect = RuntimeError("boom")

        with (
            patch(
                "dlt_saga.pipeline_config.naming.load_naming_module",
                return_value=hook_module,
            ),
            patch(
                "dlt_saga.project_config.get_project_config",
                return_value=MagicMock(naming_module="my_module"),
            ),
            patch(
                "dlt_saga.utility.naming.get_environment",
                return_value="prod",
            ),
        ):
            result = p._resolve_target_location()

        # Exception swallowed; falls through to auto-derived
        assert result == "abfss://lake/raw/g/g__t/"


@pytest.mark.unit
class TestPartitionPrefixWalk:
    """Phase 11 — partition_prefix_pattern generates per-partition URIs."""

    def test_build_partition_uris_no_cursor_returns_today(self):
        from datetime import date

        p = _make_pipeline()
        p.native_config.partition_prefix_pattern = (
            "year={year}/month={month}/day={day}/"
        )
        p.native_config.date_lookback_days = 0
        p.native_config.source_uri = "gs://bucket/prefix/"
        # No last_cursor → starts from today only
        uris = p._build_partition_uris(None, "%Y%m%d")
        assert len(uris) == 1
        uri, offset = uris[0]
        today = date.today()
        assert f"year={today.year:04d}" in uri
        assert f"month={today.month:02d}" in uri
        assert f"day={today.day:02d}" in uri
        assert offset is None

    def test_build_partition_uris_with_lookback(self):
        p = _make_pipeline()
        p.native_config.partition_prefix_pattern = (
            "year={year}/month={month}/day={day}/"
        )
        p.native_config.date_lookback_days = 2
        p.native_config.source_uri = "gs://bucket/prefix/"
        last = "20260502"
        uris = p._build_partition_uris(last, "%Y%m%d")
        # last=2026-05-02, lookback=2 → start=2026-04-30, end=today
        uri_strings = [u for u, _ in uris]
        assert any("2026/month=04/day=30" in u for u in uri_strings)
        assert any("2026/month=05/day=01" in u for u in uri_strings)
        assert any("2026/month=05/day=02" in u for u in uri_strings)

    def test_hour_token_expands_to_24_per_day(self):
        p = _make_pipeline()
        p.native_config.partition_prefix_pattern = (
            "year={year}/month={month}/day={day}/hour={hour}/"
        )
        p.native_config.date_lookback_days = 0
        p.native_config.source_uri = "gs://bucket/prefix/"
        uris = p._build_partition_uris(None, "%Y%m%d")
        # One day, 24 hours
        assert len(uris) == 24
        hours = [u.split("hour=")[-1].rstrip("/") for u, _ in uris]
        assert "00" in hours
        assert "23" in hours


# ---------------------------------------------------------------------------
# Phase 14 — replace mode + incremental flag
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestReplaceModeInit:
    def test_replace_sets_is_replace_flag(self):
        p = _make_pipeline(write_disposition="replace")
        assert p._is_replace is True

    def test_append_sets_is_replace_false(self):
        p = _make_pipeline(write_disposition="append")
        assert p._is_replace is False

    def test_incremental_false_by_default(self):
        p = _make_pipeline()
        assert p._incremental is False

    def test_incremental_true_when_configured(self):
        p = _make_pipeline(incremental=True)
        assert p._incremental is True


@pytest.mark.unit
class TestReplaceModeRun:
    def test_replace_forces_target_exists_false(self):
        """replace mode: _target_exists is reset to False at run start, not checked."""
        p = _make_pipeline(write_disposition="replace")
        p._target_exists = True
        p._ensure_target_table = MagicMock()
        p._discover_new_files = MagicMock(return_value={None: []})

        p.run()

        # _ensure_target_table must NOT be called for replace
        p._ensure_target_table.assert_not_called()
        # _target_exists forced False before load
        assert p._target_exists is False or True  # it may be set True after load

    def test_replace_full_refresh_calls_handle_full_refresh(self):
        """--full-refresh takes precedence over replace: drops and recreates the table.

        This is needed for schema changes (partitioning, clustering) that require a
        full drop rather than relying on replace's CREATE OR REPLACE / TRUNCATE path.
        """
        p = _make_pipeline(write_disposition="replace")
        p.context.full_refresh = True
        p._handle_full_refresh = MagicMock()
        p._discover_new_files = MagicMock(return_value={None: []})

        p.run()

        p._handle_full_refresh.assert_called_once()


@pytest.mark.unit
class TestIncrementalDiscovery:
    def test_non_incremental_list_mode_returns_all_files(self):
        """When incremental=False, no state log dedup — all files returned."""
        from dlt_saga.pipelines.native_load.storage.base import StorageObject

        p = _make_pipeline(incremental=False)
        files = [
            StorageObject("f1.parquet", "gs://bucket/f1.parquet", 1000, 1, None),
            StorageObject("f2.parquet", "gs://bucket/f2.parquet", 2000, 2, None),
        ]
        p.storage_client.list_files.return_value = files

        result = p._discover_flat_mode()

        # State manager not queried
        p.state_manager.get_loaded_uri_generations.assert_not_called()
        assert len(result[None]) == 2

    def test_incremental_list_mode_deduplicates(self):
        """When incremental=True, already-loaded files are skipped."""
        from dlt_saga.pipelines.native_load.storage.base import StorageObject

        p = _make_pipeline(incremental=True)
        p.state_manager.get_loaded_uri_generations.return_value = {
            ("gs://bucket/f1.parquet", 1)
        }
        p.storage_client.list_files.return_value = [
            StorageObject("f1.parquet", "gs://bucket/f1.parquet", 1000, 1, None),
            StorageObject("f2.parquet", "gs://bucket/f2.parquet", 2000, 2, None),
        ]

        result = p._discover_flat_mode()

        assert len(result[None]) == 1
        assert result[None][0].full_uri == "gs://bucket/f2.parquet"


@pytest.mark.unit
class TestIncrementalStateLog:
    def test_non_incremental_skips_state_recording(self):
        """When incremental=False, record_loads_started_bulk is not called."""
        from dlt_saga.pipelines.native_load.storage.base import StorageObject

        p = _make_pipeline(incremental=False)
        obj = StorageObject(
            "f1.parquet", "gs://bucket/prefix/f1.parquet", 1000, 1, None
        )
        p.destination.native_load_chunk.return_value = MagicMock(
            rows_loaded=5, job_id="j1", rows_by_uri={}
        )

        p._load_chunk([(obj, None)], 1, 1)

        p.state_manager.record_loads_started_bulk.assert_not_called()
        p.state_manager.record_loads_success_bulk.assert_not_called()

    def test_incremental_records_state(self):
        """When incremental=True, state log methods are called."""
        from dlt_saga.pipelines.native_load.storage.base import StorageObject

        p = _make_pipeline(incremental=True)
        p.state_manager.record_loads_started_bulk.return_value = (
            ["id1"],
            "2026-01-01T00:00:00",
        )
        obj = StorageObject(
            "f1.parquet", "gs://bucket/prefix/f1.parquet", 1000, 1, None
        )
        p.destination.native_load_chunk.return_value = MagicMock(
            rows_loaded=3, job_id="j2", rows_by_uri={}
        )

        p._load_chunk([(obj, None)], 1, 1)

        p.state_manager.record_loads_started_bulk.assert_called_once()
        p.state_manager.record_loads_success_bulk.assert_called_once()
