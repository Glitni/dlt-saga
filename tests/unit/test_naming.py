"""Unit tests for naming utilities and file-config naming resolution."""

import os
from types import ModuleType
from unittest.mock import MagicMock, patch

import pytest

from dlt_saga.utility.naming import (
    get_dev_schema,
    get_environment,
    get_execution_plan_schema,
    is_production,
)


def _mock_context(**kwargs):
    """Create a mock ExecutionContext with specified return values."""
    ctx = MagicMock()
    for method, value in kwargs.items():
        getattr(ctx, method).return_value = value
    return ctx


@pytest.fixture(autouse=True)
def _reset_naming_module():
    """Reset the cached naming module between tests."""
    import dlt_saga.pipeline_config.naming as naming_mod

    naming_mod._naming_module = None
    yield
    naming_mod._naming_module = None


@pytest.mark.unit
class TestGetEnvironment:
    @pytest.mark.parametrize(
        "env_vars, context_env, expected",
        [
            ({}, None, "dev"),  # default
            ({"SAGA_ENVIRONMENT": "prod"}, None, "prod"),  # from env var
            ({"SAGA_ENVIRONMENT": "dev"}, "prod", "prod"),  # context wins over env var
        ],
    )
    def test_environment_resolution(self, env_vars, context_env, expected):
        if context_env:
            ctx = _mock_context(get_environment=context_env)
            with patch.dict(os.environ, env_vars, clear=True):
                with patch(
                    "dlt_saga.utility.cli.context.get_execution_context",
                    return_value=ctx,
                ):
                    assert get_environment() == expected
        else:
            with patch.dict(os.environ, env_vars, clear=True):
                with patch(
                    "dlt_saga.utility.cli.context.get_execution_context",
                    side_effect=ImportError,
                ):
                    assert get_environment() == expected


@pytest.mark.unit
class TestIsProduction:
    @pytest.mark.parametrize("env, expected", [("prod", True), ("dev", False)])
    def test_is_production(self, env, expected):
        with patch("dlt_saga.utility.naming.get_environment", return_value=env):
            assert is_production() is expected


@pytest.mark.unit
class TestGetDevSchema:
    @pytest.mark.parametrize(
        "context_schema, env_schema, expected",
        [
            ("dlt_john", "dlt_jane", "dlt_john"),  # context wins
            (None, "dlt_jane", "dlt_jane"),  # env var fallback
            (None, None, "dlt_dev"),  # default
        ],
    )
    def test_dev_schema_priority(self, context_schema, env_schema, expected):
        env_vars = {"SAGA_SCHEMA_NAME": env_schema} if env_schema else {}
        if context_schema:
            ctx = _mock_context(get_schema=context_schema)
            with patch(
                "dlt_saga.utility.cli.context.get_execution_context",
                return_value=ctx,
            ):
                assert get_dev_schema() == expected
        else:
            with patch(
                "dlt_saga.utility.cli.context.get_execution_context",
                side_effect=ImportError,
            ):
                with patch.dict(os.environ, env_vars, clear=True):
                    assert get_dev_schema() == expected


@pytest.mark.unit
class TestFileConfigSchemaName:
    """Test schema name resolution via the segment-based naming defaults."""

    @pytest.mark.parametrize(
        "segments, environment, expected",
        [
            # Prod: dlt_{segments[0]}
            (["google_sheets", "salgsmal"], "prod", "dlt_google_sheets"),
            (["filesystem", "data"], "prod", "dlt_filesystem"),
            (["api", "livewrapped", "stats"], "prod", "dlt_api"),
        ],
    )
    def test_prod_schema(self, segments, environment, expected):
        from dlt_saga.pipeline_config.naming import default_generate_schema_name

        # For prod, default_schema is ignored
        assert (
            default_generate_schema_name(segments, environment, "dlt_dev") == expected
        )

    @pytest.mark.parametrize(
        "default_schema, expected",
        [
            ("dlt_john", "dlt_john"),
            ("dlt_dev", "dlt_dev"),
        ],
    )
    def test_dev_schema_uses_default(self, default_schema, expected):
        from dlt_saga.pipeline_config.naming import default_generate_schema_name

        assert (
            default_generate_schema_name(["google_sheets", "x"], "dev", default_schema)
            == expected
        )

    def test_empty_segments_prod_falls_back_to_default_group(self):
        from dlt_saga.pipeline_config.naming import default_generate_schema_name

        assert default_generate_schema_name([], "prod", "dlt_dev") == "dlt_default"

    def test_custom_naming_module(self):
        """Custom naming module is delegated to when configured."""
        from dlt_saga.pipeline_config.file_config import FilePipelineConfig

        custom_module = ModuleType("custom_naming")
        custom_module.generate_schema_name = lambda path, env, default: "custom_schema"

        fpc = FilePipelineConfig()
        with patch(
            "dlt_saga.pipeline_config.file_config.load_naming_module",
            return_value=custom_module,
        ):
            result = fpc.resolve_schema_name("configs/google_sheets/x.yml")
            assert result == "custom_schema"

    def test_missing_function_falls_back(self):
        """Custom module without generate_schema_name falls back to default."""
        from dlt_saga.pipeline_config.file_config import FilePipelineConfig

        custom_module = ModuleType("custom_naming")
        # No generate_schema_name defined

        fpc = FilePipelineConfig()
        with patch(
            "dlt_saga.pipeline_config.file_config.load_naming_module",
            return_value=custom_module,
        ):
            with patch(
                "dlt_saga.pipeline_config.file_config.get_environment",
                return_value="prod",
            ):
                result = fpc.resolve_schema_name("configs/google_sheets/x.yml")
                assert result == "dlt_google_sheets"

    def test_no_custom_module_uses_default(self):
        """When no custom module is configured, default is used."""
        from dlt_saga.pipeline_config.file_config import FilePipelineConfig

        fpc = FilePipelineConfig()
        with patch(
            "dlt_saga.pipeline_config.file_config.load_naming_module",
            return_value=False,
        ):
            with patch(
                "dlt_saga.pipeline_config.file_config.get_environment",
                return_value="prod",
            ):
                result = fpc.resolve_schema_name("configs/filesystem/data.yml")
                assert result == "dlt_filesystem"


@pytest.mark.unit
class TestFileConfigTableName:
    """Test table name resolution via segment-based naming defaults."""

    @pytest.mark.parametrize(
        "segments, environment, expected",
        [
            # Prod: base_name (no prefix)
            (["google_sheets", "asm", "salgsmal"], "prod", "asm__salgsmal"),
            (["filesystem", "di_avvik_hourly"], "prod", "di_avvik_hourly"),
            (["api", "livewrapped", "stats"], "prod", "livewrapped__stats"),
            # Dev: first_segment__base_name
            (
                ["google_sheets", "asm", "salgsmal"],
                "dev",
                "google_sheets__asm__salgsmal",
            ),
            (
                ["filesystem", "di_avvik_hourly"],
                "dev",
                "filesystem__di_avvik_hourly",
            ),
            (["api", "livewrapped", "stats"], "dev", "api__livewrapped__stats"),
        ],
    )
    def test_table_name(self, segments, environment, expected):
        from dlt_saga.pipeline_config.naming import default_generate_table_name

        assert default_generate_table_name(segments, environment) == expected

    def test_empty_segments_returns_default(self):
        from dlt_saga.pipeline_config.naming import default_generate_table_name

        assert default_generate_table_name([], "prod") == "default_data"

    def test_custom_naming_module(self):
        """Custom naming module is delegated to when configured."""
        from dlt_saga.pipeline_config.file_config import FilePipelineConfig

        custom_module = ModuleType("custom_naming")
        custom_module.generate_table_name = lambda path, env: "custom_table"

        fpc = FilePipelineConfig()
        with patch(
            "dlt_saga.pipeline_config.file_config.load_naming_module",
            return_value=custom_module,
        ):
            result = fpc.resolve_table_name("configs/google_sheets/x.yml")
            assert result == "custom_table"


@pytest.mark.unit
class TestDefaultGenerateTargetLocation:
    """Test default_generate_target_location — the public default exposed for
    users to copy or wrap inside a custom naming module."""

    def test_no_storage_root_returns_none(self):
        from dlt_saga.pipeline_config import default_generate_target_location

        assert default_generate_target_location(["g", "t"], "prod", None) is None
        assert default_generate_target_location(["g", "t"], "prod", "") is None

    def test_default_segment_shape_prod(self):
        from dlt_saga.pipeline_config import default_generate_target_location

        result = default_generate_target_location(
            ["google_sheets", "asm", "salgsmal"],
            "prod",
            "abfss://lake@a.dfs.core.windows.net/raw/",
        )
        # group=google_sheets, table=asm__salgsmal (default_generate_table_name prod shape)
        assert (
            result
            == "abfss://lake@a.dfs.core.windows.net/raw/google_sheets/asm__salgsmal/"
        )

    def test_strips_trailing_slash_on_root(self):
        from dlt_saga.pipeline_config import default_generate_target_location

        result = default_generate_target_location(
            ["g", "t"],
            "prod",
            "abfss://lake/raw",  # no trailing slash
        )
        assert result == "abfss://lake/raw/g/t/"

    def test_pipeline_group_kwarg_overrides_segment_derivation(self):
        from dlt_saga.pipeline_config import default_generate_target_location

        result = default_generate_target_location(
            ["g", "t"],
            "prod",
            "abfss://lake/raw/",
            pipeline_group="custom_group",
        )
        assert result == "abfss://lake/raw/custom_group/t/"

    def test_table_name_kwarg_overrides_segment_derivation(self):
        from dlt_saga.pipeline_config import default_generate_target_location

        result = default_generate_target_location(
            ["g", "t"],
            "prod",
            "abfss://lake/raw/",
            table_name="resolved_via_custom_hook",
        )
        assert result == "abfss://lake/raw/g/resolved_via_custom_hook/"

    def test_both_kwargs_passed_through(self):
        from dlt_saga.pipeline_config import default_generate_target_location

        result = default_generate_target_location(
            ["anything"],
            "prod",
            "abfss://lake/raw/",
            pipeline_group="grp",
            table_name="tbl",
        )
        assert result == "abfss://lake/raw/grp/tbl/"

    def test_empty_segments_uses_default_group(self):
        from dlt_saga.pipeline_config import default_generate_target_location

        result = default_generate_target_location([], "prod", "abfss://lake/raw/")
        # Empty segments → "default" group, table = default_data
        assert result == "abfss://lake/raw/default/default_data/"


@pytest.mark.unit
class TestGetExecutionPlanSchema:
    @pytest.mark.parametrize(
        "is_prod, env_schema, expected",
        [
            (True, None, "dlt_orchestration"),
            (False, "dlt_john", "dlt_john"),
            (False, None, "dlt_dev"),
        ],
    )
    def test_execution_plan_schema_defaults(self, is_prod, env_schema, expected):
        """Without explicit orchestration.schema, falls back to env-based defaults."""
        from dlt_saga.project_config import OrchestrationConfig

        env_vars = {"SAGA_SCHEMA_NAME": env_schema} if env_schema else {}
        with patch("dlt_saga.utility.naming.is_production", return_value=is_prod):
            with patch(
                "dlt_saga.project_config.get_orchestration_config",
                return_value=OrchestrationConfig(),
            ):
                with patch.dict(os.environ, env_vars, clear=True):
                    assert get_execution_plan_schema() == expected

    def test_explicit_schema_overrides_default(self):
        """orchestration.schema in saga_project.yml takes precedence."""
        from dlt_saga.project_config import OrchestrationConfig

        config = OrchestrationConfig(schema="custom_orchestration")
        with patch(
            "dlt_saga.project_config.get_orchestration_config",
            return_value=config,
        ):
            # Even in dev, the explicit schema wins
            with patch("dlt_saga.utility.naming.is_production", return_value=False):
                assert get_execution_plan_schema() == "custom_orchestration"


@pytest.mark.unit
class TestNamingIntegration:
    """Test that default resolve functions produce expected output."""

    def test_prod_naming_consistency(self):
        from dlt_saga.pipeline_config.naming import (
            default_generate_schema_name,
            default_generate_table_name,
        )

        assert (
            default_generate_schema_name(["google_sheets", "x"], "prod", "dlt_dev")
            == "dlt_google_sheets"
        )
        assert (
            default_generate_schema_name(["filesystem", "x"], "prod", "dlt_dev")
            == "dlt_filesystem"
        )
        assert (
            default_generate_table_name(["google_sheets", "my_table"], "prod")
            == "my_table"
        )
        from dlt_saga.project_config import OrchestrationConfig

        with patch("dlt_saga.utility.naming.is_production", return_value=True):
            with patch(
                "dlt_saga.project_config.get_orchestration_config",
                return_value=OrchestrationConfig(),
            ):
                assert get_execution_plan_schema() == "dlt_orchestration"

    def test_dev_naming_consistency(self):
        from dlt_saga.pipeline_config.naming import (
            default_generate_schema_name,
            default_generate_table_name,
        )
        from dlt_saga.project_config import OrchestrationConfig

        assert (
            default_generate_schema_name(["google_sheets", "x"], "dev", "dlt_developer")
            == "dlt_developer"
        )
        assert (
            default_generate_schema_name(["filesystem", "x"], "dev", "dlt_developer")
            == "dlt_developer"
        )
        assert (
            default_generate_table_name(["google_sheets", "my_table"], "dev")
            == "google_sheets__my_table"
        )

        with patch(
            "dlt_saga.utility.cli.context.get_execution_context",
            side_effect=ImportError,
        ):
            with patch.dict(os.environ, {"SAGA_SCHEMA_NAME": "dlt_developer"}):
                with patch("dlt_saga.utility.naming.is_production", return_value=False):
                    with patch(
                        "dlt_saga.project_config.get_orchestration_config",
                        return_value=OrchestrationConfig(),
                    ):
                        assert get_execution_plan_schema() == "dlt_developer"


@pytest.mark.unit
class TestLoadNamingModule:
    def test_no_naming_module_key(self):
        """When project config has no naming_module key, returns False."""
        from dlt_saga.pipeline_config.naming import load_naming_module

        result = load_naming_module({"pipelines": {"foo": "bar"}})
        assert result is False

    def test_empty_project_config(self):
        """When project config is empty, returns False."""
        from dlt_saga.pipeline_config.naming import load_naming_module

        result = load_naming_module({})
        assert result is False

    def test_invalid_module_warns(self, caplog):
        """When naming module can't be imported, warns and returns False."""
        import logging

        from dlt_saga.pipeline_config.naming import load_naming_module

        with caplog.at_level(logging.WARNING):
            result = load_naming_module({"naming_module": "nonexistent.module"})
        assert result is False
        assert "Failed to load custom naming module" in caplog.text


@pytest.mark.unit
class TestPipelineName:
    """Test the pipeline_name property preserves double underscores."""

    def test_single_subfolder(self):
        """google_sheets/asm/salgsmal.yml -> google_sheets__asm__salgsmal"""
        from dlt_saga.pipeline_config.base_config import PipelineConfig

        config = PipelineConfig(
            pipeline_group="google_sheets",
            pipeline_name="google_sheets__asm__salgsmal",
            table_name="google_sheets__asm__salgsmal",
            identifier="configs/google_sheets/asm/salgsmal.yml",
            config_dict={"base_table_name": "asm__salgsmal"},
            enabled=True,
            tags=[],
            schema_name="dlt_google_sheets",
        )
        assert config.pipeline_name == "google_sheets__asm__salgsmal"

    def test_deep_subfolders(self):
        """Preserves __ between each folder level."""
        from dlt_saga.pipeline_config.base_config import PipelineConfig

        config = PipelineConfig(
            pipeline_group="google_sheets",
            pipeline_name="google_sheets__regional_budget__2024_2025__buskerud_vestfold",
            table_name="google_sheets__regional_budget__2024_2025__buskerud_vestfold",
            identifier="x",
            config_dict={
                "base_table_name": "regional_budget__2024_2025__buskerud_vestfold"
            },
            enabled=True,
            tags=[],
            schema_name="dlt_google_sheets",
        )
        assert (
            config.pipeline_name
            == "google_sheets__regional_budget__2024_2025__buskerud_vestfold"
        )

    def test_no_subfolders(self):
        """filesystem/query_balance_view.yml -> filesystem__query_balance_view"""
        from dlt_saga.pipeline_config.base_config import PipelineConfig

        config = PipelineConfig(
            pipeline_group="filesystem",
            pipeline_name="filesystem__query_balance_view",
            table_name="filesystem__query_balance_view",
            identifier="x",
            config_dict={"base_table_name": "query_balance_view"},
            enabled=True,
            tags=[],
            schema_name="dlt_filesystem",
        )
        assert config.pipeline_name == "filesystem__query_balance_view"


@pytest.mark.unit
class TestSanitizeSegment:
    def test_lowercase(self):
        from dlt_saga.pipeline_config.naming import _sanitize_segment

        assert _sanitize_segment("Google") == "google"

    def test_hyphen_to_underscore(self):
        from dlt_saga.pipeline_config.naming import _sanitize_segment

        assert _sanitize_segment("my-segment") == "my_segment"

    def test_space_to_underscore(self):
        from dlt_saga.pipeline_config.naming import _sanitize_segment

        assert _sanitize_segment("my segment") == "my_segment"

    def test_combined(self):
        from dlt_saga.pipeline_config.naming import _sanitize_segment

        assert _sanitize_segment("My-Segment Name") == "my_segment_name"

    def test_no_change_already_clean(self):
        from dlt_saga.pipeline_config.naming import _sanitize_segment

        assert _sanitize_segment("my_segment") == "my_segment"


@pytest.mark.unit
class TestSingleSegmentTableName:
    """len(segments) == 1 is a distinct code path in default_generate_table_name."""

    def test_single_segment_prod(self):
        from dlt_saga.pipeline_config.naming import default_generate_table_name

        assert default_generate_table_name(["only_group"], "prod") == "only_group"

    def test_single_segment_dev(self):
        from dlt_saga.pipeline_config.naming import default_generate_table_name

        # dev: f"{first_segment}__{base_name}" where both are the same segment
        assert (
            default_generate_table_name(["only_group"], "dev")
            == "only_group__only_group"
        )

    def test_single_segment_with_hyphen_sanitized(self):
        from dlt_saga.pipeline_config.naming import default_generate_table_name

        assert default_generate_table_name(["my-group"], "prod") == "my_group"


@pytest.mark.unit
class TestLoadNamingModuleCaching:
    def test_cached_result_returned_on_second_call(self):
        """After the first call sets _naming_module=False, subsequent calls hit the cache."""
        from dlt_saga.pipeline_config.naming import load_naming_module

        result1 = load_naming_module({})
        assert result1 is False
        # Second call within same test: _naming_module is now False (not None),
        # so the early-return cache branch fires.
        result2 = load_naming_module({})
        assert result2 is False
