"""Unit tests for dlt_saga.project_config module."""

import pytest

from dlt_saga.project_config import (
    ConfigSourceConfig,
    HistorizeProjectConfig,
    OrchestrationConfig,
    ProvidersConfig,
    SagaProjectConfig,
    _reset_cache,
    get_config_source_settings,
    get_historize_project_config,
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

    def test_renders_env_var_templates(self, tmp_path, monkeypatch):
        monkeypatch.setenv("CONFIG_DIR", "my_configs")
        yml = tmp_path / "saga_project.yml"
        yml.write_text(
            "config_source:\n  type: file\n  path: \"{{ env_var('CONFIG_DIR') }}\"\n"
        )
        monkeypatch.chdir(tmp_path)

        result = get_project_config()
        assert result.config_source.path == "my_configs"

    def test_returns_defaults_when_missing(self, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        result = get_project_config()
        assert isinstance(result, SagaProjectConfig)
        assert result.config_source is None

    def test_found_from_subdirectory(self, tmp_path, monkeypatch):
        # Running saga from a subdirectory must still find the project root's
        # saga_project.yml (walk up), not silently load empty defaults.
        yml = tmp_path / "saga_project.yml"
        yml.write_text("config_source:\n  type: file\n  path: configs\n")
        subdir = tmp_path / "configs" / "api"
        subdir.mkdir(parents=True)
        monkeypatch.chdir(subdir)

        result = get_project_config()
        assert result.config_source is not None
        assert result.config_source.type == "file"
        assert result.config_source.path == "configs"
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
            "  schema_access:\n"
            "    - 'OWNER:serviceAccount:sa@proj.iam.gserviceaccount.com'\n"
        )
        monkeypatch.chdir(tmp_path)

        result = get_project_config()
        assert result.pipelines is not None
        assert "schema_access" in result.pipelines

    def test_legacy_dataset_access_key_is_normalized(self, tmp_path, monkeypatch):
        """The legacy ``dataset_access`` key is rewritten to ``schema_access``
        at YAML load — projects can keep the legacy spelling indefinitely."""
        yml = tmp_path / "saga_project.yml"
        yml.write_text(
            "pipelines:\n"
            "  dataset_access:\n"
            "    - 'OWNER:serviceAccount:sa@proj.iam.gserviceaccount.com'\n"
        )
        monkeypatch.chdir(tmp_path)

        result = get_project_config()
        assert result.pipelines is not None
        assert "schema_access" in result.pipelines
        assert "dataset_access" not in result.pipelines


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

    def test_schema_access_defaults_to_none(self, tmp_path, monkeypatch):
        yml = tmp_path / "saga_project.yml"
        yml.write_text("orchestration:\n  provider: cloud_run\n")
        monkeypatch.chdir(tmp_path)

        result = get_orchestration_config()
        assert result.schema_access is None

    def test_worker_concurrency_defaults_to_none(self, tmp_path, monkeypatch):
        yml = tmp_path / "saga_project.yml"
        yml.write_text("orchestration:\n  provider: cloud_run\n")
        monkeypatch.chdir(tmp_path)

        result = get_orchestration_config()
        assert result.worker_concurrency is None

    def test_worker_concurrency_parsed_when_present(self, tmp_path, monkeypatch):
        yml = tmp_path / "saga_project.yml"
        yml.write_text(
            "orchestration:\n  provider: cloud_run\n  worker_concurrency: 2\n"
        )
        monkeypatch.chdir(tmp_path)

        result = get_orchestration_config()
        assert result.worker_concurrency == 2

    def test_worker_concurrency_rejects_zero(self):
        with pytest.raises(ValueError, match="worker_concurrency must be >= 1"):
            OrchestrationConfig(worker_concurrency=0)

    def test_schema_access_parsed_when_present(self, tmp_path, monkeypatch):
        yml = tmp_path / "saga_project.yml"
        yml.write_text(
            "orchestration:\n"
            "  provider: cloud_run\n"
            "  schema_access:\n"
            "    - 'READER:serviceAccount:airflow@example.iam.gserviceaccount.com'\n"
            "    - 'READER:group:data@example.com'\n"
        )
        monkeypatch.chdir(tmp_path)

        result = get_orchestration_config()
        assert result.schema_access == [
            "READER:serviceAccount:airflow@example.iam.gserviceaccount.com",
            "READER:group:data@example.com",
        ]

    def test_legacy_dataset_access_key_normalized_on_orchestration(
        self, tmp_path, monkeypatch
    ):
        """orchestration.dataset_access (legacy spelling) is read via the alias."""
        yml = tmp_path / "saga_project.yml"
        yml.write_text(
            "orchestration:\n"
            "  provider: cloud_run\n"
            "  dataset_access:\n"
            "    - 'READER:group:data@example.com'\n"
        )
        monkeypatch.chdir(tmp_path)

        result = get_orchestration_config()
        assert result.schema_access == ["READER:group:data@example.com"]


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
                "schema_access": [
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
        assert "schema_access" in config.pipelines

    def test_empty_dict(self):
        config = SagaProjectConfig.from_dict({})
        assert config.config_source is None
        assert config.providers is None
        assert config.orchestration is None
        assert config.naming_module is None
        assert config.pipelines is None


@pytest.mark.unit
class TestHistorizeProjectConfig:
    def test_defaults(self):
        cfg = HistorizeProjectConfig()
        assert cfg.placement == "table_suffix"
        assert cfg.table_suffix == "_historized"
        assert cfg.schema_suffix == "_historized"

    def test_from_dict_defaults_when_empty(self):
        cfg = HistorizeProjectConfig.from_dict({})
        assert cfg.placement == "table_suffix"

    def test_from_dict_schema_suffix(self):
        cfg = HistorizeProjectConfig.from_dict(
            {"placement": "schema_suffix", "schema_suffix": "_hist"}
        )
        assert cfg.placement == "schema_suffix"
        assert cfg.schema_suffix == "_hist"

    def test_invalid_placement_raises(self):
        with pytest.raises(ValueError, match="placement"):
            HistorizeProjectConfig(placement="invalid")

    def test_invalid_suffix_raises(self):
        with pytest.raises(ValueError, match="table_suffix"):
            HistorizeProjectConfig(table_suffix="123bad")

    def test_saga_project_yml_parsed(self, tmp_path, monkeypatch):
        yml = tmp_path / "saga_project.yml"
        yml.write_text("historize:\n  placement: schema_suffix\n  schema_suffix: _h\n")
        monkeypatch.chdir(tmp_path)

        cfg = get_historize_project_config()
        assert cfg.placement == "schema_suffix"
        assert cfg.schema_suffix == "_h"

    def test_saga_project_yml_absent_returns_default(self, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        cfg = get_historize_project_config()
        assert cfg.placement == "table_suffix"
