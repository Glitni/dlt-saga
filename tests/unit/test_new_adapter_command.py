"""Unit tests for dlt_saga.new_adapter_command."""

import py_compile
import re

import pytest
import yaml

import dlt_saga.new_adapter_command as nac
from dlt_saga.new_adapter_command import (
    _normalize_name,
    _pascal_case,
    effective_namespace,
    existing_adapter_files,
    resolve_inputs,
    run_new_adapter,
)

pytestmark = pytest.mark.unit


# ---------------------------------------------------------------------------
# Naming helpers
# ---------------------------------------------------------------------------


class TestNaming:
    def test_normalize_lowercases_and_hyphen_to_underscore(self):
        assert _normalize_name("My-Service") == "my_service"

    def test_normalize_rejects_empty(self):
        with pytest.raises(ValueError):
            _normalize_name("   ")

    def test_normalize_rejects_invalid_identifier(self):
        with pytest.raises(ValueError):
            _normalize_name("123 bad name!")

    def test_normalize_rejects_keyword(self):
        with pytest.raises(ValueError):
            _normalize_name("class")

    def test_pascal_case(self):
        assert _pascal_case("my_service") == "MyService"
        assert _pascal_case("weather") == "Weather"


# ---------------------------------------------------------------------------
# Interactive resolution
# ---------------------------------------------------------------------------


class TestResolveInputs:
    def test_passthrough_when_all_provided(self):
        # Nothing missing → no prompting, values returned as-is.
        assert resolve_inputs("my_service", "database", "api", no_input=False) == (
            "my_service",
            "database",
            "api",
        )

    def test_no_input_uses_defaults_for_kind_and_group(self):
        assert resolve_inputs("my_service", None, None, no_input=True) == (
            "my_service",
            "api",
            "generic",
        )

    def test_no_input_requires_name(self):
        with pytest.raises(ValueError):
            resolve_inputs(None, None, None, no_input=True)

    def test_prompts_for_missing_values(self, monkeypatch):
        prompts = iter(["my_service", "api"])  # name, then group
        monkeypatch.setattr(nac.typer, "prompt", lambda *a, **k: next(prompts))
        monkeypatch.setattr(nac.typer, "echo", lambda *a, **k: None)
        # kind provided, name + group prompted.
        assert resolve_inputs(None, None, "generic", no_input=False) == (
            "my_service",
            "api",
            "generic",
        )


# ---------------------------------------------------------------------------
# Scaffolding
# ---------------------------------------------------------------------------


class TestScaffold:
    def test_generic_creates_expected_files(self, tmp_path):
        adapter = run_new_adapter("my_service", target=tmp_path)

        assert adapter == "local.api.my_service"
        adapter_dir = tmp_path / "pipelines" / "api" / "my_service"
        assert (adapter_dir / "config.py").exists()
        assert (adapter_dir / "pipeline.py").exists()
        # Generic adapters split connection/I/O into client.py.
        assert (adapter_dir / "client.py").exists()
        assert (adapter_dir / "__init__.py").exists()
        # Package __init__ at each level makes it importable.
        assert (tmp_path / "pipelines" / "__init__.py").exists()
        assert (tmp_path / "pipelines" / "api" / "__init__.py").exists()
        # Starter config is discoverable.
        assert (tmp_path / "configs" / "api" / "my_service.yml").exists()

    def test_registers_namespace_in_packages_yml(self, tmp_path):
        run_new_adapter("my_service", target=tmp_path)

        data = yaml.safe_load((tmp_path / "packages.yml").read_text())
        assert {"namespace": "local", "path": "./pipelines"} in data["packages"]

    def test_namespace_registered_only_once(self, tmp_path):
        run_new_adapter("first", target=tmp_path)
        run_new_adapter("second", target=tmp_path)

        data = yaml.safe_load((tmp_path / "packages.yml").read_text())
        local_entries = [e for e in data["packages"] if e["namespace"] == "local"]
        assert len(local_entries) == 1

    def test_reuses_existing_namespace_for_same_path(self, tmp_path):
        # Path ./pipelines is already registered under a different namespace.
        (tmp_path / "packages.yml").write_text(
            "packages:\n- namespace: bm\n  path: ./pipelines\n"
        )
        adapter = run_new_adapter("ecb", group="api", target=tmp_path)

        # Adapter uses the existing namespace, not the default 'local'.
        assert adapter == "bm.api.ecb"
        data = yaml.safe_load((tmp_path / "packages.yml").read_text())
        # No duplicate ./pipelines entry was added.
        pipelines_entries = [e for e in data["packages"] if e["path"] == "./pipelines"]
        assert len(pipelines_entries) == 1
        assert pipelines_entries[0]["namespace"] == "bm"
        # Generated config references the reused namespace.
        config_yml = (tmp_path / "configs" / "api" / "ecb.yml").read_text()
        assert "adapter: bm.api.ecb" in config_yml

    def test_effective_namespace_resolves_existing_path(self, tmp_path):
        (tmp_path / "packages.yml").write_text(
            "packages:\n- namespace: bm\n  path: ./pipelines\n"
        )
        assert effective_namespace(tmp_path, "local", "pipelines") == "bm"
        # Unregistered path falls back to the requested namespace.
        assert effective_namespace(tmp_path, "local", "other") == "local"

    def test_namespace_conflict_with_different_path_raises(self, tmp_path):
        # 'local' is taken, mapped to a different directory.
        (tmp_path / "packages.yml").write_text(
            "packages:\n- namespace: local\n  path: ./other\n"
        )
        with pytest.raises(ValueError, match="already registered"):
            run_new_adapter("ecb", target=tmp_path)

    def test_custom_group_and_namespace(self, tmp_path):
        adapter = run_new_adapter(
            "orders", group="database", namespace="acme", target=tmp_path
        )
        assert adapter == "acme.database.orders"
        assert (tmp_path / "pipelines" / "database" / "orders" / "config.py").exists()
        assert (tmp_path / "configs" / "database" / "orders.yml").exists()

    def test_invalid_kind_raises(self, tmp_path):
        with pytest.raises(ValueError):
            run_new_adapter("x", kind="nonsense", target=tmp_path)

    def test_existing_adapter_raises_instead_of_silent_skip(self, tmp_path):
        run_new_adapter("my_service", target=tmp_path)
        # Re-running the same name/group must refuse, not pretend success.
        with pytest.raises(ValueError, match="already exists"):
            run_new_adapter("my_service", target=tmp_path)

    def test_same_name_different_group_is_allowed(self, tmp_path):
        run_new_adapter("orders", group="api", target=tmp_path)
        # Different group is a different adapter — no collision.
        run_new_adapter("orders", group="database", target=tmp_path)
        assert (tmp_path / "pipelines" / "database" / "orders" / "config.py").exists()

    def test_existing_adapter_files_detects_collision(self, tmp_path):
        assert existing_adapter_files(tmp_path, "pipelines", "api", "x") == []
        run_new_adapter("x", target=tmp_path)
        found = existing_adapter_files(tmp_path, "pipelines", "api", "x")
        assert [p.name for p in found] == ["config.py", "pipeline.py"]


# ---------------------------------------------------------------------------
# Generated code quality — the whole point of the scaffolder
# ---------------------------------------------------------------------------


class TestGeneratedCodeConventions:
    @pytest.mark.parametrize("kind", ["generic", "api"])
    def test_generated_files_compile(self, tmp_path, kind):
        run_new_adapter("my_service", kind=kind, target=tmp_path)
        adapter_dir = tmp_path / "pipelines" / "api" / "my_service"
        # Raises py_compile.PyCompileError on a syntax error.
        for py in adapter_dir.glob("*.py"):
            py_compile.compile(str(py), doraise=True)

    def test_generic_config_uses_secretstr_not_secret_naming(self, tmp_path):
        run_new_adapter("my_service", target=tmp_path)
        config = (
            tmp_path / "pipelines" / "api" / "my_service" / "config.py"
        ).read_text()
        assert "SecretStr" in config
        assert "coerce_secret" in config
        # Named by content (the credential field), never by secrecy.
        assert "credential" in config
        # No field *declared* with a secrecy suffix (e.g. `api_secret: ...`).
        # Matches field declarations at line start, not `env_secret::` in prose.
        assert not re.search(r"^\s*\w+_(secret|plaintext)\s*:", config, re.MULTILINE)

    def test_generic_creates_client_with_fetch(self, tmp_path):
        run_new_adapter("my_service", target=tmp_path)
        adapter_dir = tmp_path / "pipelines" / "api" / "my_service"
        client = (adapter_dir / "client.py").read_text()
        assert "class MyServiceClient" in client
        assert "def fetch(self" in client
        # Pipeline delegates to the client rather than fetching inline.
        pipeline = (adapter_dir / "pipeline.py").read_text()
        assert "MyServiceClient" in pipeline
        assert "self.client.fetch(" in pipeline

    def test_generic_pipeline_implements_idempotent_incremental(self, tmp_path):
        run_new_adapter("my_service", target=tmp_path)
        pipeline = (
            tmp_path / "pipelines" / "api" / "my_service" / "pipeline.py"
        ).read_text()
        # Required contract, not the wrong extract()/target_writer pattern.
        assert "def extract_data(self)" in pipeline
        # High-water-mark lookup with initial_value fallback — no hardcoded date.
        assert "get_max_column_value" in pipeline
        assert "initial_value" in pipeline

    def test_api_kind_inherits_base_api_pipeline(self, tmp_path):
        run_new_adapter("weather", kind="api", target=tmp_path)
        pipeline = (
            tmp_path / "pipelines" / "api" / "weather" / "pipeline.py"
        ).read_text()
        assert "BaseApiPipeline" in pipeline
        assert "_create_api_config" in pipeline

    def test_date_window_kind_inherits_date_window_pipeline(self, tmp_path):
        run_new_adapter("metrics", kind="api-date-window", target=tmp_path)
        adapter_dir = tmp_path / "pipelines" / "api" / "metrics"
        pipeline = (adapter_dir / "pipeline.py").read_text()
        config = (adapter_dir / "config.py").read_text()
        # Inherits the reusable date-window base (an API adapter), not BasePipeline.
        assert "DateWindowApiPipeline" in pipeline
        assert "_fetch_window" in pipeline
        assert "DateWindowApiConfig" in config
        # API adapter → no client.py (the base owns the HTTP layer).
        assert not (adapter_dir / "client.py").exists()
        # Starter config wires the window into the request.
        starter = (tmp_path / "configs" / "api" / "metrics.yml").read_text()
        assert "incremental_column" in starter
        assert "start_param" in starter

    def test_date_window_kind_is_valid(self):
        # api-date-window is an accepted kind alongside generic/api.
        from dlt_saga.new_adapter_command import _VALID_KINDS

        assert "api-date-window" in _VALID_KINDS
