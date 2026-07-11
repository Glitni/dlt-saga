"""Unit tests for shared CLI helpers (dlt_saga.utility.cli.common)."""

import pytest

import dlt_saga.utility.cli.common as common
from dlt_saga.pipeline_config import FilePipelineConfig
from dlt_saga.project_config import ConfigSourceConfig

pytestmark = pytest.mark.unit


class TestGetConfigSource:
    @pytest.fixture(autouse=True)
    def _reset_singleton(self):
        """The config source is a module-global singleton — reset around each test."""
        common._config_source = None
        yield
        common._config_source = None

    def test_uses_all_configured_paths(self, monkeypatch):
        """list/validate/plan must discover every configured directory.

        Regression: get_config_source used the first-path-only `.path` accessor,
        so multi-path projects saw fewer pipelines here than local runs (which
        go through Session._create_config_source and pass the full `.paths`).
        """
        settings = ConfigSourceConfig(type="file", paths=["configs", "extra_configs"])
        monkeypatch.setattr(common, "get_config_source_settings", lambda: settings)

        source = common.get_config_source()

        assert isinstance(source, FilePipelineConfig)
        assert source._root_dirs == ["configs", "extra_configs"]

    def test_single_path_still_works(self, monkeypatch):
        settings = ConfigSourceConfig(type="file", paths=["configs"])
        monkeypatch.setattr(common, "get_config_source_settings", lambda: settings)

        source = common.get_config_source()

        assert source._root_dirs == ["configs"]
