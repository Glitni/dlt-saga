"""Unit tests for native_load config validation."""

import pytest

from dlt_saga.pipelines.native_load.config import NativeLoadConfig


def _make(**kwargs) -> NativeLoadConfig:
    defaults = {"source_uri": "gs://bucket/prefix/", "file_type": "parquet"}
    defaults.update(kwargs)
    return NativeLoadConfig(**defaults)


@pytest.mark.unit
class TestNativeLoadConfigBasic:
    def test_minimal_valid(self):
        cfg = _make()
        assert cfg.source_uri == "gs://bucket/prefix/"
        assert cfg.file_type == "parquet"
        assert cfg.file_pattern == "*.parquet"

    def test_file_pattern_defaults_by_type(self):
        assert _make(file_type="parquet").file_pattern == "*.parquet"
        assert _make(file_type="csv").file_pattern == "*.csv"
        assert _make(file_type="jsonl").file_pattern == "*.jsonl"

    def test_custom_file_pattern(self):
        cfg = _make(file_pattern="data_*.parquet")
        assert cfg.file_pattern == "data_*.parquet"

    def test_write_disposition_append(self):
        cfg = _make(write_disposition="append")
        assert cfg.write_disposition == "append"

    def test_write_disposition_append_historize(self):
        cfg = _make(write_disposition="append+historize")
        assert cfg.write_disposition == "append+historize"

    def test_write_disposition_invalid(self):
        with pytest.raises(ValueError, match="append"):
            _make(write_disposition="merge")


@pytest.mark.unit
class TestSourceUriValidation:
    def test_missing_source_uri(self):
        with pytest.raises(ValueError, match="source_uri is required"):
            NativeLoadConfig(source_uri="")

    def test_unsupported_scheme(self):
        with pytest.raises(ValueError, match="must start with"):
            _make(source_uri="http://bucket/prefix/")

    def test_missing_trailing_slash(self):
        with pytest.raises(ValueError, match="must end with"):
            _make(source_uri="gs://bucket/prefix")

    def test_gs_scheme(self):
        cfg = _make(source_uri="gs://bucket/prefix/")
        assert cfg.source_uri == "gs://bucket/prefix/"

    def test_s3_scheme(self):
        cfg = _make(source_uri="s3://bucket/prefix/")
        assert cfg.source_uri.startswith("s3://")

    def test_abfss_scheme(self):
        cfg = _make(source_uri="abfss://container@account.dfs.core.windows.net/prefix/")
        assert cfg.source_uri.startswith("abfss://")


@pytest.mark.unit
class TestFileTypeValidation:
    def test_invalid_file_type(self):
        with pytest.raises(ValueError, match="file_type"):
            _make(file_type="xlsx")


@pytest.mark.unit
class TestDateModeValidation:
    def test_both_date_fields_set_is_valid(self):
        cfg = _make(filename_date_regex=r"(\d{8})", filename_date_format="%Y%m%d")
        assert cfg.filename_date_regex == r"(\d{8})"
        assert cfg.filename_date_format == "%Y%m%d"

    def test_only_regex_set_raises(self):
        with pytest.raises(ValueError, match="both be set or both absent"):
            _make(filename_date_regex=r"(\d{8})")

    def test_only_format_set_raises(self):
        with pytest.raises(ValueError, match="both be set or both absent"):
            _make(filename_date_format="%Y%m%d")

    def test_neither_set_is_flat_mode(self):
        cfg = _make()
        assert cfg.filename_date_regex is None
        assert cfg.filename_date_format is None

    def test_date_regex_invalid(self):
        with pytest.raises(ValueError, match="valid regex"):
            _make(filename_date_regex="(invalid[", filename_date_format="%Y%m%d")

    def test_date_regex_no_capture_group(self):
        with pytest.raises(ValueError, match="one capture group"):
            _make(filename_date_regex=r"\d{8}", filename_date_format="%Y%m%d")

    def test_date_regex_two_capture_groups(self):
        with pytest.raises(ValueError, match="one capture group"):
            _make(filename_date_regex=r"(\d{4})(\d{2})", filename_date_format="%Y%m%d")


@pytest.mark.unit
class TestNumericFloors:
    def test_load_batch_size_floor(self):
        with pytest.raises(ValueError, match="load_batch_size"):
            _make(load_batch_size=0)

    def test_date_lookback_days_floor(self):
        with pytest.raises(ValueError, match="date_lookback_days"):
            _make(date_lookback_days=-1)

    def test_max_bad_records_floor(self):
        with pytest.raises(ValueError, match="max_bad_records"):
            _make(max_bad_records=-1)

    def test_csv_skip_leading_rows_floor(self):
        with pytest.raises(ValueError, match="csv_skip_leading_rows"):
            _make(csv_skip_leading_rows=-1)


@pytest.mark.unit
class TestFromDict:
    def test_ignores_unknown_keys(self):
        cfg = NativeLoadConfig.from_dict(
            {
                "source_uri": "gs://bucket/prefix/",
                "file_type": "parquet",
                "pipeline_name": "ignored",
                "schema_name": "also_ignored",
                "unknown_key": "value",
            }
        )
        assert cfg.source_uri == "gs://bucket/prefix/"

    def test_picks_up_known_keys(self):
        cfg = NativeLoadConfig.from_dict(
            {
                "source_uri": "gs://bucket/prefix/",
                "file_type": "csv",
                "csv_separator": ";",
                "load_batch_size": 100,
            }
        )
        assert cfg.file_type == "csv"
        assert cfg.csv_separator == ";"
        assert cfg.load_batch_size == 100


@pytest.mark.unit
class TestColumnsField:
    def test_default_empty(self):
        cfg = _make()
        assert cfg.columns == {}

    def test_accepts_dict(self):
        hints = {
            "order_date": {"data_type": "timestamp"},
            "amount": {"data_type": "decimal"},
        }
        cfg = _make(columns=hints)
        assert cfg.columns["order_date"] == {"data_type": "timestamp"}
        assert cfg.columns["amount"] == {"data_type": "decimal"}

    def test_ignores_non_dict_values_in_from_dict(self):
        """columns key in YAML must be a dict; from_dict should not raise on it."""
        cfg = NativeLoadConfig.from_dict(
            {
                "source_uri": "gs://bucket/prefix/",
                "columns": {"col": {"data_type": "text"}},
            }
        )
        assert cfg.columns == {"col": {"data_type": "text"}}

    def test_native_type_passthrough(self):
        """Native BQ types (e.g. FLOAT64) accepted directly in data_type."""
        cfg = _make(columns={"amount": {"data_type": "FLOAT64"}})
        assert cfg.columns["amount"]["data_type"] == "FLOAT64"


@pytest.mark.unit
class TestFilePatternList:
    def test_single_string_pattern(self):
        cfg = _make(file_pattern="data_*.parquet")
        assert cfg.file_pattern == "data_*.parquet"

    def test_list_of_patterns(self):
        cfg = _make(file_pattern=["data_*.parquet", "export_*.parquet"])
        assert cfg.file_pattern == ["data_*.parquet", "export_*.parquet"]

    def test_default_still_single_string(self):
        cfg = _make(file_type="parquet")
        assert cfg.file_pattern == "*.parquet"
        assert isinstance(cfg.file_pattern, str)


@pytest.mark.unit
class TestTableFormat:
    def test_default_is_delta(self):
        cfg = _make()
        assert cfg.table_format == "delta"

    def test_iceberg_accepted(self):
        cfg = _make(table_format="iceberg")
        assert cfg.table_format == "iceberg"

    def test_delta_uniform_accepted(self):
        cfg = _make(table_format="delta_uniform")
        assert cfg.table_format == "delta_uniform"

    def test_invalid_format_raises(self):
        with pytest.raises(ValueError, match="table_format"):
            _make(table_format="hudi")

    def test_target_location_none_by_default(self):
        cfg = _make()
        assert cfg.target_location is None

    def test_target_location_accepted(self):
        loc = "abfss://lake@account.dfs.core.windows.net/raw/group/table/"
        cfg = _make(target_location=loc)
        assert cfg.target_location == loc


@pytest.mark.unit
class TestPartitionPrefixPattern:
    def test_default_is_none(self):
        cfg = _make()
        assert cfg.partition_prefix_pattern is None

    def test_pattern_accepted(self):
        cfg = _make(partition_prefix_pattern="year={year}/month={month}/day={day}/")
        assert cfg.partition_prefix_pattern == "year={year}/month={month}/day={day}/"

    def test_from_dict_picks_up_new_fields(self):
        cfg = NativeLoadConfig.from_dict(
            {
                "source_uri": "abfss://lake@a.dfs.windows.net/raw/",
                "table_format": "delta_uniform",
                "target_location": "abfss://lake@a.dfs.windows.net/raw/g/t/",
                "partition_prefix_pattern": "year={year}/",
            }
        )
        assert cfg.table_format == "delta_uniform"
        assert cfg.target_location == "abfss://lake@a.dfs.windows.net/raw/g/t/"
        assert cfg.partition_prefix_pattern == "year={year}/"


# ---------------------------------------------------------------------------
# Phase 14 — replace write disposition + incremental flag + removed fields
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestWriteDispositionMatrix:
    def test_replace_valid(self):
        cfg = _make(write_disposition="replace")
        assert cfg.write_disposition == "replace"

    def test_replace_historize_valid(self):
        cfg = _make(write_disposition="replace+historize")
        assert cfg.write_disposition == "replace+historize"

    def test_append_historize_still_valid(self):
        cfg = _make(write_disposition="append+historize")
        assert cfg.write_disposition == "append+historize"

    def test_merge_still_invalid(self):
        with pytest.raises(ValueError, match="append.*replace"):
            _make(write_disposition="merge")


@pytest.mark.unit
class TestIncrementalFlag:
    def test_default_is_false(self):
        cfg = _make()
        assert cfg.incremental is False

    def test_explicit_true(self):
        cfg = _make(incremental=True)
        assert cfg.incremental is True

    def test_replace_with_incremental_raises(self):
        with pytest.raises(ValueError, match="incremental=True is not supported"):
            _make(write_disposition="replace", incremental=True)

    def test_append_with_incremental_allowed(self):
        cfg = _make(write_disposition="append", incremental=True)
        assert cfg.incremental is True


@pytest.mark.unit
class TestInertDateWarnings:
    def test_date_filename_prefix_warns_when_not_incremental(self, caplog):
        import logging

        with caplog.at_level(logging.WARNING):
            _make(date_filename_prefix="data_")
        assert any("date_filename_prefix" in m for m in caplog.messages)

    def test_non_default_lookback_warns_when_not_incremental(self, caplog):
        import logging

        with caplog.at_level(logging.WARNING):
            _make(date_lookback_days=5)
        assert any("date_lookback_days" in m for m in caplog.messages)

    def test_partition_prefix_pattern_warns_when_not_incremental(self, caplog):
        import logging

        with caplog.at_level(logging.WARNING):
            _make(partition_prefix_pattern="year={year}/month={month}/day={day}/")
        assert any("partition_prefix_pattern" in m for m in caplog.messages)

    def test_partition_prefix_pattern_without_date_regex_warns(self, caplog):
        import logging

        with caplog.at_level(logging.WARNING):
            _make(
                incremental=True,
                partition_prefix_pattern="year={year}/month={month}/day={day}/",
            )
        assert any(
            "filename_date_regex" in m and "partition_prefix_pattern" in m
            for m in caplog.messages
        )

    def test_no_inert_warnings_when_incremental_true(self, caplog):
        import logging

        with caplog.at_level(logging.WARNING):
            _make(
                incremental=True,
                filename_date_regex=r"(\d{8})",
                filename_date_format="%Y%m%d",
                date_filename_prefix="data_",
                date_lookback_days=5,
                partition_prefix_pattern="year={year}/",
            )
        inert_msgs = [
            m
            for m in caplog.messages
            if "date_filename_prefix" in m or "date_lookback_days" in m
        ]
        assert inert_msgs == []


@pytest.mark.unit
class TestRemovedFields:
    def test_normalize_column_names_not_a_field(self):
        import dataclasses

        field_names = {f.name for f in dataclasses.fields(NativeLoadConfig)}
        assert "normalize_column_names" not in field_names

    def test_schema_evolution_not_a_field(self):
        import dataclasses

        field_names = {f.name for f in dataclasses.fields(NativeLoadConfig)}
        assert "schema_evolution" not in field_names

    def test_discovery_mode_not_a_field(self):
        import dataclasses

        field_names = {f.name for f in dataclasses.fields(NativeLoadConfig)}
        assert "discovery_mode" not in field_names

    def test_old_cursor_fields_not_fields(self):
        import dataclasses

        field_names = {f.name for f in dataclasses.fields(NativeLoadConfig)}
        for old in (
            "cursor_regex",
            "cursor_format",
            "cursor_lookback_days",
            "cursor_filename_prefix",
        ):
            assert old not in field_names

    def test_from_dict_ignores_removed_fields(self):
        cfg = NativeLoadConfig.from_dict(
            {
                "source_uri": "gs://bucket/prefix/",
                "normalize_column_names": False,
                "schema_evolution": False,
            }
        )
        assert cfg.source_uri == "gs://bucket/prefix/"

    def test_from_dict_old_cursor_names_silently_ignored(self):
        cfg = NativeLoadConfig.from_dict(
            {
                "source_uri": "gs://bucket/prefix/",
                "cursor_regex": r"(\d{8})",
                "cursor_format": "%Y%m%d",
                "cursor_lookback_days": 3,
                "cursor_filename_prefix": "data_",
                "discovery_mode": "cursor",
            }
        )
        assert cfg.filename_date_regex is None
        assert cfg.filename_date_format is None


@pytest.mark.unit
class TestCsvFieldWarnings:
    """CSV-specific options emit a warning when used with a non-CSV file_type."""

    def test_csv_separator_warns_on_parquet(self):
        import warnings

        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            _make(file_type="parquet", csv_separator=";")
        assert any("csv_separator" in str(warning.message) for warning in w)

    def test_encoding_warns_on_parquet(self):
        import warnings

        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            _make(file_type="parquet", encoding="ISO-8859-1")
        assert any("encoding" in str(warning.message) for warning in w)

    def test_csv_quote_character_warns_on_jsonl(self):
        import warnings

        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            _make(file_type="jsonl", csv_quote_character='"')
        assert any("csv_quote_character" in str(warning.message) for warning in w)

    def test_csv_null_marker_warns_on_parquet(self):
        import warnings

        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            _make(file_type="parquet", csv_null_marker="NULL")
        assert any("csv_null_marker" in str(warning.message) for warning in w)

    def test_csv_skip_leading_rows_warns_on_parquet(self):
        import warnings

        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            _make(file_type="parquet", csv_skip_leading_rows=1)
        assert any("csv_skip_leading_rows" in str(warning.message) for warning in w)

    def test_no_warnings_when_file_type_is_csv(self):
        import warnings

        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            cfg = _make(
                file_type="csv",
                csv_separator=";",
                encoding="UTF-8",
                csv_skip_leading_rows=1,
                csv_quote_character='"',
                csv_null_marker="\\N",
            )
        csv_field_warnings = [
            x
            for x in w
            if any(
                name in str(x.message)
                for name in (
                    "csv_separator",
                    "encoding",
                    "csv_quote_character",
                    "csv_null_marker",
                    "csv_skip_leading_rows",
                )
            )
        ]
        assert csv_field_warnings == []
        assert cfg.csv_separator == ";"
