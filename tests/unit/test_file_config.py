"""Unit tests for file-based pipeline configuration."""

from unittest.mock import patch

import pytest

from dlt_saga.pipeline_config.file_config import FilePipelineConfig


@pytest.mark.unit
class TestDeriveBaseTableName:
    @pytest.mark.parametrize(
        "path, expected",
        [
            ("configs/google_sheets/data.yml", "data"),
            ("configs/google_sheets/asm/salgsmal.yml", "asm__salgsmal"),
            ("configs/api/livewrapped/stats.yml", "livewrapped__stats"),
            ("configs/google_sheets/region/norway/oslo.yml", "region__norway__oslo"),
            ("configs/google_sheets/My File.yml", "my_file"),
            ("configs/google_sheets/my-file.yml", "my_file"),
            ("configs/google_sheets/MyFile.yml", "myfile"),
            ("configs/google_sheets/data.yaml", "data"),
            ("", "default_data"),
        ],
    )
    def test_derive_base_table_name(self, path, expected):
        assert FilePipelineConfig()._derive_base_table_name(path) == expected


@pytest.mark.unit
class TestGetPipelineGroupFromPath:
    @pytest.mark.parametrize(
        "path, expected",
        [
            ("configs/google_sheets/data.yml", "google_sheets"),
            ("configs/filesystem/files.yml", "filesystem"),
            ("configs/api/stats.yml", "api"),
            ("configs/google_sheets/asm/salgsmal.yml", "google_sheets"),
            ("configs/api/livewrapped/stats.yml", "api"),
        ],
    )
    def test_extract_type(self, path, expected):
        assert FilePipelineConfig()._get_pipeline_group_from_path(path) == expected

    @pytest.mark.parametrize(
        "path",
        [
            "google_sheets/data.yml",
            "configs/",
        ],
    )
    def test_invalid_path_raises(self, path):
        with pytest.raises(ValueError, match="Could not determine pipeline group"):
            FilePipelineConfig()._get_pipeline_group_from_path(path)


@pytest.mark.unit
class TestGetPathSegments:
    @pytest.mark.parametrize(
        "path, expected",
        [
            ("configs/google_sheets/data.yml", ["google_sheets"]),
            ("configs/google_sheets/asm/salgsmal.yml", ["google_sheets", "asm"]),
            (
                "configs/google_sheets/region/norway/oslo.yml",
                ["google_sheets", "region", "norway"],
            ),
            ("configs/api/livewrapped/stats.yml", ["api", "livewrapped"]),
        ],
    )
    def test_path_segments(self, path, expected):
        assert FilePipelineConfig()._get_path_segments(path) == expected


@pytest.mark.unit
class TestResolveValue:
    @pytest.mark.parametrize(
        "parent, child, inherit, expected",
        [
            # No parent
            (None, "child", True, "child"),
            (None, "child", False, "child"),
            # No child
            ("parent", None, True, "parent"),
            ("parent", None, False, None),
            # Override mode (inherit=False)
            ("parent", "child", False, "child"),
            ([1, 2], [3, 4], False, [3, 4]),
            ({"a": 1}, {"b": 2}, False, {"b": 2}),
            # Inherit primitives (child wins)
            ("parent", "child", True, "child"),
            (10, 20, True, 20),
            (True, False, True, False),
        ],
    )
    def test_resolve_value(self, parent, child, inherit, expected):
        assert FilePipelineConfig()._resolve_value(parent, child, inherit) == expected

    @pytest.mark.parametrize(
        "parent, child, expected",
        [
            ([1, 2], [3, 4], [1, 2, 3, 4]),
            ([1, 2], [2, 3], [1, 2, 3]),
            (["daily"], ["hourly", "critical"], ["daily", "hourly", "critical"]),
        ],
    )
    def test_inherit_lists(self, parent, child, expected):
        assert (
            FilePipelineConfig()._resolve_value(parent, child, inherit=True) == expected
        )

    @pytest.mark.parametrize(
        "parent, child, expected",
        [
            ({"a": 1}, {"b": 2}, {"a": 1, "b": 2}),
            ({"a": 1, "b": 2}, {"b": 3, "c": 4}, {"a": 1, "b": 3, "c": 4}),
        ],
    )
    def test_inherit_dicts(self, parent, child, expected):
        assert (
            FilePipelineConfig()._resolve_value(parent, child, inherit=True) == expected
        )


@pytest.mark.unit
class TestIsPathSegmentDict:
    @pytest.mark.parametrize(
        "value, expected",
        [
            ({"google_sheets": {}}, True),
            ({"asm": {}, "tags": []}, True),
            ({"tags": [], "ENABLED": True}, True),
            ({"TAGS": []}, False),
            ([], False),
            ("string", False),
            (123, False),
        ],
    )
    def test_is_path_segment_dict(self, value, expected):
        assert FilePipelineConfig()._is_path_segment_dict(value) is expected


@pytest.mark.unit
class TestResolveConfig:
    def test_without_project_config(self):
        config = FilePipelineConfig()
        config.project_config = {}
        file_config = {"tags": ["daily"], "enabled": True}

        result = config._resolve_config("configs/google_sheets/data.yml", file_config)
        assert result == file_config

    def test_with_project_defaults(self):
        config = FilePipelineConfig()
        config.project_config = {
            "pipelines": {"tags": ["default_tag"], "enabled": True}
        }

        result = config._resolve_config("configs/google_sheets/data.yml", {})
        assert result["tags"] == ["default_tag"]
        assert result["enabled"] is True

    def test_with_folder_hierarchy(self):
        config = FilePipelineConfig()
        config.project_config = {
            "pipelines": {
                "tags": ["default"],
                "google_sheets": {"+tags": ["sheets_tag"], "enabled": True},
            }
        }

        result = config._resolve_config("configs/google_sheets/data.yml", {})
        assert "default" in result["tags"]
        assert "sheets_tag" in result["tags"]
        assert result["enabled"] is True

    def test_file_override(self):
        config = FilePipelineConfig()
        config.project_config = {"pipelines": {"tags": ["default"], "enabled": True}}

        result = config._resolve_config(
            "configs/google_sheets/data.yml", {"tags": ["custom"], "enabled": False}
        )
        assert result["tags"] == ["custom"]
        assert result["enabled"] is False

    def test_inherit_syntax(self):
        config = FilePipelineConfig()
        config.project_config = {"pipelines": {"tags": ["default"], "enabled": True}}

        result = config._resolve_config(
            "configs/google_sheets/data.yml", {"+tags": ["custom"], "enabled": False}
        )
        assert "default" in result["tags"]
        assert "custom" in result["tags"]
        assert result["enabled"] is False


@pytest.mark.unit
class TestLoadProjectConfig:
    def test_nonexistent(self):
        with patch("pathlib.Path.exists", return_value=False):
            assert FilePipelineConfig().project_config == {}

    def test_valid(self):
        mock_config = {"pipelines": {"tags": ["default"]}}
        with patch("builtins.open", create=True):
            with patch("yaml.safe_load", return_value=mock_config):
                with patch("pathlib.Path.exists", return_value=True):
                    assert FilePipelineConfig().project_config == mock_config

    def test_invalid_yaml(self):
        with patch("builtins.open", create=True):
            with patch("yaml.safe_load", side_effect=Exception("Invalid YAML")):
                with patch("pathlib.Path.exists", return_value=True):
                    assert FilePipelineConfig().project_config == {}


@pytest.mark.unit
class TestApplyLevelConfig:
    @pytest.mark.parametrize(
        "resolved, level_config, expected_tags",
        [
            ({"tags": ["existing"]}, {"tags": ["new"]}, ["new"]),
            ({"tags": ["existing"]}, {"+tags": ["new"]}, ["existing", "new"]),
        ],
    )
    def test_apply(self, resolved, level_config, expected_tags):
        config = FilePipelineConfig()
        config._apply_level_config(resolved, level_config, "segment")
        assert resolved["tags"] == expected_tags

    def test_new_key(self):
        config = FilePipelineConfig()
        resolved = {}
        config._apply_level_config(resolved, {"enabled": True}, "segment")
        assert resolved["enabled"] is True


@pytest.mark.unit
class TestExtractLevelConfig:
    def test_without_path_segments(self):
        config = FilePipelineConfig()
        current = {"tags": ["daily"], "enabled": True}
        assert config._extract_level_config(current, is_last_segment=True) == current

    def test_skips_path_segments(self):
        config = FilePipelineConfig()
        current = {"tags": ["daily"], "google_sheets": {"tags": ["nested"]}}

        result = config._extract_level_config(current, is_last_segment=False)
        assert result == {"tags": ["daily"]}

        result = config._extract_level_config(current, is_last_segment=True)
        assert "google_sheets" in result


@pytest.mark.unit
class TestFilePipelineConfigIntegration:
    def test_inheritance_chain(self):
        config = FilePipelineConfig()
        config.project_config = {
            "pipelines": {
                "tags": ["global"],
                "enabled": True,
                "google_sheets": {
                    "+tags": ["sheets"],
                    "asm": {"+tags": ["asm"]},
                },
            }
        }

        result = config._resolve_config(
            "configs/google_sheets/asm/data.yml", {"+tags": ["file_level"]}
        )
        for tag in ["global", "sheets", "asm", "file_level"]:
            assert tag in result["tags"]
        assert result["enabled"] is True
