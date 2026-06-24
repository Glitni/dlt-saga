"""Unit tests for dlt_saga.new_config_command."""

import pytest
import yaml

import dlt_saga.new_config_command as ncc
from dlt_saga.new_config_command import (
    _classify_fields,
    _placeholder,
    _prompt_adapter,
    config_file_path,
    render_config_yaml,
    resolve_config_inputs,
    run_new_config,
)
from dlt_saga.pipelines.api.config import ApiConfig

pytestmark = pytest.mark.unit


# ---------------------------------------------------------------------------
# Interactive resolution
# ---------------------------------------------------------------------------


class TestResolveConfigInputs:
    def test_passthrough_when_all_provided(self):
        assert resolve_config_inputs(
            "orders", "database", "dlt_saga.database", no_input=False
        ) == ("orders", "database", "dlt_saga.database")

    def test_no_input_defaults_group(self):
        assert resolve_config_inputs("sales", None, "dlt_saga.api", no_input=True) == (
            "sales",
            "api",
            "dlt_saga.api",
        )

    def test_no_input_requires_adapter(self):
        with pytest.raises(ValueError, match="adapter"):
            resolve_config_inputs("sales", "api", None, no_input=True)

    def test_adapter_prompt_lists_choices_and_accepts_number(self, monkeypatch):
        monkeypatch.setattr(
            ncc,
            "available_adapters",
            lambda: [("dlt_saga.api", "builtin"), ("dlt_saga.database", "builtin")],
        )
        monkeypatch.setattr(ncc.typer, "echo", lambda *a, **k: None)
        monkeypatch.setattr(ncc.typer, "prompt", lambda *a, **k: "2")
        # Picking "2" resolves to the second listed adapter.
        assert _prompt_adapter(None, no_input=False) == "dlt_saga.database"

    def test_adapter_prompt_accepts_custom_string(self, monkeypatch):
        monkeypatch.setattr(
            ncc, "available_adapters", lambda: [("dlt_saga.api", "builtin")]
        )
        monkeypatch.setattr(ncc.typer, "echo", lambda *a, **k: None)
        monkeypatch.setattr(ncc.typer, "prompt", lambda *a, **k: "local.api.bespoke")
        assert _prompt_adapter(None, no_input=False) == "local.api.bespoke"


# ---------------------------------------------------------------------------
# Field introspection
# ---------------------------------------------------------------------------


class TestIntrospection:
    def test_classify_skips_meta_fields(self):
        required, optional = _classify_fields(ApiConfig)
        names = {f.name for f in required + optional}
        assert "adapter" not in names
        assert "tags" not in names
        assert "enabled" not in names

    def test_required_fields_flagged_via_metadata(self):
        required, _ = _classify_fields(ApiConfig)
        names = {f.name for f in required}
        # base_url / endpoint default to "" but carry metadata required=True.
        assert "base_url" in names
        assert "endpoint" in names

    def test_placeholder_prefers_default(self):
        fields_by_name = {f.name: f for f in ApiConfig.__dataclass_fields__.values()}
        # timeout default is 30; auth_type default is "none" (quoted so it isn't null).
        assert _placeholder(fields_by_name["timeout"]) == "30"
        assert _placeholder(fields_by_name["auth_type"]) == '"none"'


# ---------------------------------------------------------------------------
# Rendering
# ---------------------------------------------------------------------------


class TestRender:
    def test_render_is_valid_yaml_with_adapter(self):
        text = render_config_yaml("dlt_saga.api", ApiConfig)
        parsed = yaml.safe_load(text)
        assert parsed["adapter"] == "dlt_saga.api"
        assert parsed["write_disposition"] == "append"

    def test_required_active_optional_commented(self):
        text = render_config_yaml("dlt_saga.api", ApiConfig)
        # Required field is active (uncommented).
        assert "\nbase_url:" in text
        # Optional field is commented out.
        assert "# timeout:" in text
        # Curated load options present.
        assert "# primary_key: [id]" in text


# ---------------------------------------------------------------------------
# Scaffolding
# ---------------------------------------------------------------------------


class TestRunNewConfig:
    def test_creates_config_for_builtin_adapter(self, tmp_path):
        path = run_new_config("sales", adapter="dlt_saga.api", target=tmp_path)
        assert path == config_file_path(tmp_path, "api", "sales")
        assert path.exists()
        parsed = yaml.safe_load(path.read_text())
        assert parsed["adapter"] == "dlt_saga.api"

    def test_custom_group(self, tmp_path):
        path = run_new_config(
            "orders", adapter="dlt_saga.database", group="database", target=tmp_path
        )
        assert path == tmp_path / "configs" / "database" / "orders.yml"
        assert path.exists()

    def test_existing_config_raises(self, tmp_path):
        run_new_config("sales", adapter="dlt_saga.api", target=tmp_path)
        with pytest.raises(ValueError, match="already exists"):
            run_new_config("sales", adapter="dlt_saga.api", target=tmp_path)

    def test_unresolvable_adapter_raises(self, tmp_path):
        with pytest.raises(ValueError, match="Could not resolve"):
            run_new_config("sales", adapter="nope.does.not.exist", target=tmp_path)

    def test_links_schema_when_present(self, tmp_path):
        # A generated schema for the api adapter already exists on disk.
        schema_dir = tmp_path / "schemas"
        schema_dir.mkdir()
        (schema_dir / "api_config.json").write_text("{}", encoding="utf-8")

        path = run_new_config("sales", adapter="dlt_saga.api", target=tmp_path)
        first_line = path.read_text().splitlines()[0]
        assert first_line.startswith("# yaml-language-server: $schema=")
        assert "api_config.json" in first_line

    def test_no_modeline_when_schema_absent(self, tmp_path):
        path = run_new_config("sales", adapter="dlt_saga.api", target=tmp_path)
        assert not path.read_text().startswith("# yaml-language-server")
