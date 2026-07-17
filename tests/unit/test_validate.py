"""Unit tests for pipeline config validation."""

from types import SimpleNamespace
from unittest.mock import patch

import pytest

from dlt_saga.pipeline_config.base_config import PipelineConfig
from dlt_saga.validate import (
    ValidationResult,
    is_collision_check_failure,
    is_collision_finding,
    is_deprecated_key_finding,
    validate_pipeline_config,
    validate_pipeline_configs,
)


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
            config_dict={
                "write_disposition": "append",
                "spreadsheet_id": "abc123",
                "sheet_name": "Sheet1",
            }
        )
        result = validate_pipeline_config(config)
        assert result.is_valid

    def test_valid_append_historize(self):
        config = _make_config(
            config_dict={
                "write_disposition": "append+historize",
                "primary_key": ["id"],
                "spreadsheet_id": "abc123",
                "sheet_name": "Sheet1",
            }
        )
        result = validate_pipeline_config(config)
        assert result.is_valid

    def test_valid_merge(self):
        config = _make_config(
            config_dict={
                "write_disposition": "merge",
                "spreadsheet_id": "abc123",
                "sheet_name": "Sheet1",
            }
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

    def test_historize_only_skips_adapter_resolution(self):
        """A historize-only pipeline (write_disposition: historize) has no ingest
        step, so there is no source adapter to resolve — validation must not flag a
        missing implementation, even for a group with no built-in adapter."""
        config = _make_config(
            pipeline_group="streams",
            table_name="streams__orders",
            config_dict={
                "write_disposition": "historize",
                "primary_key": ["id"],
                "source_schema": "raw",
                "source_table": "orders",
            },
        )
        result = validate_pipeline_config(config)
        assert result.is_valid
        assert not any("implementation" in e.lower() for e in result.errors)

    def test_historize_only_still_validates_historize_config(self):
        """Skipping the adapter/source-config checks for historize-only pipelines
        must not also skip _validate_historize_config — a broken historize block
        (e.g. missing primary_key) should still surface."""
        config = _make_config(
            pipeline_group="streams",
            table_name="streams__orders",
            config_dict={
                "write_disposition": "historize",
                # primary_key intentionally omitted — historize config must flag this.
                "source_schema": "raw",
                "source_table": "orders",
            },
        )
        result = validate_pipeline_config(config)
        assert not result.is_valid
        assert any("primary_key" in e.lower() for e in result.errors)


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
            config_dict={"spreadsheet_id": "abc123", "sheet_name": "Sheet1"},
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


def _gs_config(name, table, config_path=None):
    """A valid google_sheets config with a distinct pipeline_name and target."""
    path = config_path or f"configs/{name}.yml"
    return PipelineConfig(
        pipeline_group="google_sheets",
        pipeline_name=name,
        table_name=table,
        identifier=path,
        config_dict={
            "base_table_name": table,
            "write_disposition": "append",
            "spreadsheet_id": "abc123",
            "sheet_name": "Sheet1",
            "config_path": path,
            "_target": ("dlt_dev", table),
        },
        enabled=True,
        tags=[],
        adapter=None,
    )


def _collision_source(configs):
    """Fake ConfigSource that resolves each config to the target carried in its
    ``config_dict['_target']`` and discovers the whole set as the project."""
    targets = {c.config_dict["config_path"]: c.config_dict["_target"] for c in configs}

    class _Source:
        def resolve_ingest_target(
            self,
            config_path,
            *,
            schema_override=None,
            table_override=None,
            environment=None,
        ):
            schema, table = targets[config_path]
            return (schema_override or schema, table_override or table)

        def discover(self):
            return ({"google_sheets": list(configs)}, {})

    return _Source()


def _target_collision_errors(result):
    return [e for e in result.errors if is_collision_finding(e)]


@pytest.mark.unit
class TestValidateConfigsCollisions:
    """Project-wide target-collision detection in validate_pipeline_configs."""

    def test_no_collision_when_distinct_targets(self):
        configs = [
            _gs_config("google_sheets__a", "a"),
            _gs_config("google_sheets__b", "b"),
        ]
        results = validate_pipeline_configs(
            configs, _collision_source(configs), environments=["dev"]
        )
        assert all(not _target_collision_errors(r) for r in results)
        assert all(r.is_valid for r in results)

    def test_collision_flags_every_involved_config(self):
        # Both resolve to dlt_dev.shared → a collision naming each other.
        configs = [
            _gs_config("google_sheets__a", "shared"),
            _gs_config("google_sheets__b", "shared"),
        ]
        results = validate_pipeline_configs(
            configs, _collision_source(configs), environments=["dev"]
        )
        by_name = {r.pipeline_name: r for r in results}
        errs_a = _target_collision_errors(by_name["google_sheets__a"])
        errs_b = _target_collision_errors(by_name["google_sheets__b"])
        # Each participant carries the same canonical message naming the whole
        # group, so both is_valid flags trip and the CLI can dedupe to one line.
        assert errs_a == errs_b
        assert "google_sheets__a" in errs_a[0] and "google_sheets__b" in errs_a[0]
        assert not by_name["google_sheets__a"].is_valid
        assert not by_name["google_sheets__b"].is_valid

    def test_collision_findings_dedupes_to_one_per_target(self):
        from dlt_saga.validate import collision_findings

        configs = [
            _gs_config("google_sheets__a", "shared"),
            _gs_config("google_sheets__b", "shared"),
        ]
        results = validate_pipeline_configs(
            configs, _collision_source(configs), environments=["dev"]
        )
        # Two pipelines, one shared target → exactly one deduped finding.
        assert len(collision_findings(results)) == 1

    def test_empty_selection_returns_empty(self):
        assert validate_pipeline_configs([], _collision_source([])) == []

    def test_clean_selection_is_all_pipeline_kind(self):
        configs = [_gs_config("google_sheets__a", "a")]
        results = validate_pipeline_configs(
            configs, _collision_source(configs), environments=["dev"]
        )
        assert all(r.kind == "pipeline" for r in results)

    def test_collision_check_failure_yields_check_result(self):
        configs = [_gs_config("google_sheets__a", "a")]
        with patch(
            "dlt_saga.utility.collisions.collisions_for_selection",
            side_effect=RuntimeError("boom"),
        ):
            results = validate_pipeline_configs(
                configs, _collision_source(configs), environments=["dev"]
            )
        check = [r for r in results if r.kind == "check"]
        assert len(check) == 1
        assert any(is_collision_check_failure(w) for w in check[0].warnings)
        # A check that couldn't run is a warning, not an error — the pipeline
        # config itself stays valid.
        assert results[0].is_valid


@pytest.mark.unit
class TestValidateConfigsLegacyKeys:
    """Deprecated-key scan — warnings, never errors."""

    def _source(self, project_path=None):
        return SimpleNamespace(
            project_config_path=project_path,
            resolve_ingest_target=lambda *a, **k: ("dlt_dev", "t"),
            discover=lambda: ({"google_sheets": []}, {}),
        )

    def test_legacy_key_in_config_file_warns_but_stays_valid(self, tmp_path):
        cfg = tmp_path / "a.yml"
        cfg.write_text("historize:\n  output_table: orders\n")
        config = _gs_config("google_sheets__a", "a", config_path=str(cfg))
        with patch(
            "dlt_saga.utility.cli.profiles.get_profiles_config",
            side_effect=RuntimeError,
        ):
            results = validate_pipeline_configs(
                [config], self._source(), environments=["dev"]
            )
        result = next(r for r in results if r.pipeline_name == "google_sheets__a")
        assert result.is_valid  # advisory only
        assert any("output_table" in w and "table_name" in w for w in result.warnings)

    def test_legacy_key_in_project_file_becomes_synthetic_result(self, tmp_path):
        cfg = tmp_path / "a.yml"
        cfg.write_text("write_disposition: append\n")
        proj = tmp_path / "saga_project.yml"
        proj.write_text("pipelines:\n  dataset_access:\n    - OWNER:x\n")
        config = _gs_config("google_sheets__a", "a", config_path=str(cfg))
        with patch(
            "dlt_saga.utility.cli.profiles.get_profiles_config",
            side_effect=RuntimeError,
        ):
            results = validate_pipeline_configs(
                [config], self._source(project_path=str(proj)), environments=["dev"]
            )
        synthetic = [r for r in results if r.pipeline_name == str(proj)]
        assert len(synthetic) == 1
        assert synthetic[0].kind == "project"
        assert synthetic[0].is_valid
        assert all(is_deprecated_key_finding(w) for w in synthetic[0].warnings)
        assert any(
            "dataset_access" in w and "schema_access" in w
            for w in synthetic[0].warnings
        )
