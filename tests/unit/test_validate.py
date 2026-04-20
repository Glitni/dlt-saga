"""Unit tests for pipeline config validation."""

import pytest

from dlt_saga.pipeline_config.base_config import PipelineConfig
from dlt_saga.validate import ValidationResult, validate_pipeline_config


def _make_config(
    pipeline_group="google_sheets",
    table_name="google_sheets__test",
    config_dict=None,
    adapter=None,
    tags=None,
):
    """Helper to create a PipelineConfig for testing."""
    base_dict = {
        "base_table_name": "test",
        "write_disposition": "append",
    }
    if config_dict:
        base_dict.update(config_dict)
    return PipelineConfig(
        pipeline_group=pipeline_group,
        pipeline_name=f"{pipeline_group}__test",
        table_name=table_name,
        identifier=f"configs/{pipeline_group}/test.yml",
        config_dict=base_dict,
        enabled=True,
        tags=tags or [],
        adapter=adapter,
    )


@pytest.mark.unit
class TestValidationResult:
    def test_valid_when_no_errors(self):
        result = ValidationResult(pipeline_name="test")
        assert result.is_valid

    def test_invalid_when_errors(self):
        result = ValidationResult(pipeline_name="test", errors=["bad"])
        assert not result.is_valid

    def test_valid_with_warnings_only(self):
        result = ValidationResult(pipeline_name="test", warnings=["warn"])
        assert result.is_valid


@pytest.mark.unit
class TestValidateWriteDisposition:
    def test_valid_append(self):
        config = _make_config(
            config_dict={"write_disposition": "append", "spreadsheet_id": "abc123"}
        )
        result = validate_pipeline_config(config)
        assert result.is_valid

    def test_valid_append_historize(self):
        config = _make_config(
            config_dict={
                "write_disposition": "append+historize",
                "primary_key": ["id"],
                "spreadsheet_id": "abc123",
            }
        )
        result = validate_pipeline_config(config)
        assert result.is_valid

    def test_valid_merge(self):
        config = _make_config(
            config_dict={"write_disposition": "merge", "spreadsheet_id": "abc123"}
        )
        result = validate_pipeline_config(config)
        assert result.is_valid

    def test_valid_historize_only(self):
        config = _make_config(
            config_dict={
                "write_disposition": "historize",
                "primary_key": ["id"],
                "source_schema": "raw",
                "source_table": "orders",
                "spreadsheet_id": "abc123",
            }
        )
        result = validate_pipeline_config(config)
        assert result.is_valid

    def test_invalid_write_disposition(self):
        config = _make_config(config_dict={"write_disposition": "upsert"})
        result = validate_pipeline_config(config)
        assert not result.is_valid
        assert any("write_disposition" in e for e in result.errors)


@pytest.mark.unit
class TestValidatePipelineImpl:
    def test_valid_builtin_type(self):
        """google_sheets resolves via base implementation."""
        config = _make_config(pipeline_group="google_sheets")
        result = validate_pipeline_config(config)
        # Should not have adapter errors
        impl_errors = [e for e in result.errors if "implementation" in e.lower()]
        assert not impl_errors

    def test_invalid_adapter(self):
        config = _make_config(adapter="dlt_saga.nonexistent.source")
        result = validate_pipeline_config(config)
        assert not result.is_valid
        assert any("implementation" in e.lower() for e in result.errors)

    def test_explicit_valid_impl(self):
        config = _make_config(
            pipeline_group="api",
            table_name="api__test",
            adapter="dlt_saga.api",
        )
        result = validate_pipeline_config(config)
        impl_errors = [e for e in result.errors if "implementation" in e.lower()]
        assert not impl_errors


@pytest.mark.unit
class TestValidateSourceConfig:
    def test_missing_required_field(self):
        """Missing spreadsheet_id for google_sheets should error."""
        config = _make_config(
            pipeline_group="google_sheets",
            config_dict={"spreadsheet_id": ""},
        )
        result = validate_pipeline_config(config)
        assert not result.is_valid
        assert any("spreadsheet_id" in e for e in result.errors)

    def test_valid_source_config(self):
        """Valid google_sheets config passes."""
        config = _make_config(
            pipeline_group="google_sheets",
            config_dict={"spreadsheet_id": "abc123"},
        )
        result = validate_pipeline_config(config)
        source_errors = [e for e in result.errors if "source config" in e.lower()]
        assert not source_errors


@pytest.mark.unit
class TestValidateHistorizeConfig:
    def test_historize_without_primary_key(self):
        config = _make_config(
            config_dict={
                "write_disposition": "append+historize",
            }
        )
        result = validate_pipeline_config(config)
        assert not result.is_valid
        assert any("primary_key" in e for e in result.errors)

    def test_historize_with_primary_key(self):
        config = _make_config(
            config_dict={
                "write_disposition": "append+historize",
                "primary_key": ["id"],
            }
        )
        result = validate_pipeline_config(config)
        historize_errors = [e for e in result.errors if "historize" in e.lower()]
        assert not historize_errors

    def test_historize_only_missing_source_schema(self):
        """historize-only with source_table but no source_schema should error."""
        config = _make_config(
            config_dict={
                "write_disposition": "historize",
                "primary_key": ["id"],
                "source_table": "orders",
            }
        )
        result = validate_pipeline_config(config)
        assert not result.is_valid
        assert any("source_schema" in e for e in result.errors)

    def test_no_historize_skips_check(self):
        """Non-historize config doesn't trigger historize validation."""
        config = _make_config(config_dict={"write_disposition": "append"})
        result = validate_pipeline_config(config)
        historize_errors = [e for e in result.errors if "historize" in e.lower()]
        assert not historize_errors


@pytest.mark.unit
class TestMultipleErrors:
    def test_collects_multiple_errors(self):
        """Validation doesn't fail-fast — collects all errors."""
        config = _make_config(
            config_dict={
                "write_disposition": "invalid_disposition",
                "spreadsheet_id": "",
            }
        )
        result = validate_pipeline_config(config)
        assert not result.is_valid
        assert len(result.errors) >= 2
