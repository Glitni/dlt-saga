"""Unit tests for dlt_saga.project_config module."""

import pytest

from dlt_saga.project_config import (
    ConfigSourceConfig,
    OrchestrationConfig,
    ProvidersConfig,
    SagaProjectConfig,
    _reset_cache,
    get_config_source_settings,
    get_orchestration_config,
    get_project_config,
    get_providers_config,
)


@pytest.fixture(autouse=True)
def _reset():
    """Reset the cached config between tests."""
    _reset_cache()
    yield
    _reset_cache()


@pytest.mark.unit
class TestGetProjectConfig:
    def test_returns_parsed_yaml(self, tmp_path, monkeypatch):
        yml = tmp_path / "saga_project.yml"
        yml.write_text("config_source:\n  type: file\n  path: configs\n")
        monkeypatch.chdir(tmp_path)

        result = get_project_config()
        assert isinstance(result, SagaProjectConfig)
        assert result.config_source.type == "file"
        assert result.config_source.path == "configs"

    def test_returns_defaults_when_missing(self, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        result = get_project_config()
        assert isinstance(result, SagaProjectConfig)
        assert result.config_source is None
        assert result.providers is None
        assert result.pipelines is None

    def test_caches_result(self, tmp_path, monkeypatch):
        yml = tmp_path / "saga_project.yml"
        yml.write_text("naming_module: my_module\n")
        monkeypatch.chdir(tmp_path)

        first = get_project_config()
        # Modify file — should still get cached result
        yml.write_text("naming_module: other_module\n")
        second = get_project_config()
        assert first is second
        assert second.naming_module == "my_module"

    def test_handles_invalid_yaml(self, tmp_path, monkeypatch):
        yml = tmp_path / "saga_project.yml"
        yml.write_text(": invalid: yaml: [")
        monkeypatch.chdir(tmp_path)

        result = get_project_config()
        assert isinstance(result, SagaProjectConfig)
        assert result.config_source is None

    def test_pipelines_section(self, tmp_path, monkeypatch):
        yml = tmp_path / "saga_project.yml"
        yml.write_text(
            "pipelines:\n"
            "  dataset_access:\n"
            "    - 'OWNER:serviceAccount:sa@proj.iam.gserviceaccount.com'\n"
        )
        monkeypatch.chdir(tmp_path)

        result = get_project_config()
        assert result.pipelines is not None
        assert "dataset_access" in result.pipelines


@pytest.mark.unit
class TestGetProvidersConfig:
    def test_returns_providers_section(self, tmp_path, monkeypatch):
        yml = tmp_path / "saga_project.yml"
        yml.write_text("providers:\n  google_secrets:\n    project_id: my-project\n")
        monkeypatch.chdir(tmp_path)

        result = get_providers_config()
        assert isinstance(result, ProvidersConfig)
        assert result.google_secrets is not None
        assert result.google_secrets.project_id == "my-project"

    def test_returns_empty_when_no_providers(self, tmp_path, monkeypatch):
        yml = tmp_path / "saga_project.yml"
        yml.write_text("config_source:\n  type: file\n")
        monkeypatch.chdir(tmp_path)

        result = get_providers_config()
        assert isinstance(result, ProvidersConfig)
        assert result.google_secrets is None


@pytest.mark.unit
class TestGetConfigSourceSettings:
    def test_returns_config_source_section(self, tmp_path, monkeypatch):
        yml = tmp_path / "saga_project.yml"
        yml.write_text("config_source:\n  type: file\n  path: my_configs\n")
        monkeypatch.chdir(tmp_path)

        result = get_config_source_settings()
        assert isinstance(result, ConfigSourceConfig)
        assert result.type == "file"
        assert result.path == "my_configs"

    def test_returns_defaults_when_missing(self, tmp_path, monkeypatch):
        yml = tmp_path / "saga_project.yml"
        yml.write_text("other_key: value\n")
        monkeypatch.chdir(tmp_path)

        result = get_config_source_settings()
        assert isinstance(result, ConfigSourceConfig)
        assert result.type == "file"
        assert result.path == "configs"


@pytest.mark.unit
class TestGetOrchestrationConfig:
    def test_returns_orchestration_section(self, tmp_path, monkeypatch):
        yml = tmp_path / "saga_project.yml"
        yml.write_text("orchestration:\n  provider: cloud_run\n  region: us-central1\n")
        monkeypatch.chdir(tmp_path)

        result = get_orchestration_config()
        assert isinstance(result, OrchestrationConfig)
        assert result.provider == "cloud_run"
        assert result.region == "us-central1"
        assert result.job_name is None

    def test_stdout_provider(self, tmp_path, monkeypatch):
        yml = tmp_path / "saga_project.yml"
        yml.write_text("orchestration:\n  provider: stdout\n")
        monkeypatch.chdir(tmp_path)

        result = get_orchestration_config()
        assert result.provider == "stdout"

    def test_returns_empty_when_no_orchestration(self, tmp_path, monkeypatch):
        yml = tmp_path / "saga_project.yml"
        yml.write_text("config_source:\n  type: file\n")
        monkeypatch.chdir(tmp_path)

        result = get_orchestration_config()
        assert isinstance(result, OrchestrationConfig)
        assert result.provider is None

    def test_returns_empty_when_no_file(self, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        result = get_orchestration_config()
        assert isinstance(result, OrchestrationConfig)
        assert result.provider is None


@pytest.mark.unit
class TestSagaProjectConfigFromDict:
    def test_full_config(self):
        data = {
            "config_source": {"type": "file", "path": "my_configs"},
            "providers": {
                "google_secrets": {
                    "project_id": "my-project",
                    "sheets_secret_name": "my-secret",
                }
            },
            "orchestration": {
                "provider": "cloud_run",
                "region": "eu-north1",
                "job_name": "my-job",
            },
            "naming_module": "my_naming",
            "pipelines": {
                "dataset_access": [
                    "OWNER:serviceAccount:sa@proj.iam.gserviceaccount.com"
                ]
            },
        }
        config = SagaProjectConfig.from_dict(data)
        assert config.config_source.type == "file"
        assert config.config_source.path == "my_configs"
        assert config.providers.google_secrets.project_id == "my-project"
        assert config.providers.google_secrets.sheets_secret_name == "my-secret"
        assert config.orchestration.provider == "cloud_run"
        assert config.orchestration.region == "eu-north1"
        assert config.orchestration.job_name == "my-job"
        assert config.naming_module == "my_naming"
        assert "dataset_access" in config.pipelines

    def test_empty_dict(self):
        config = SagaProjectConfig.from_dict({})
        assert config.config_source is None
        assert config.providers is None
        assert config.orchestration is None
        assert config.naming_module is None
        assert config.pipelines is None
