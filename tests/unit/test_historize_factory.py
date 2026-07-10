"""Unit tests for historize factory helpers."""

import sys
from types import ModuleType
from unittest.mock import MagicMock

import pytest

from dlt_saga.historize.config import HistorizeConfig
from dlt_saga.historize.factory import (
    _apply_naming_module_historize_overrides,
    _resolve_historize_schema_access,
    _resolve_historize_storage_path,
    _resolve_historize_table_format,
)


def _make_historize_config(**kwargs) -> HistorizeConfig:
    return HistorizeConfig.from_dict(kwargs, top_level_primary_key=["id"])


def _make_context(
    table_format: str = "native",
    historize_table_format=None,
    historize_storage_path=None,
    storage_path=None,
):
    ctx = MagicMock()
    ctx.get_table_format.return_value = table_format
    ctx.get_historize_table_format.return_value = historize_table_format
    ctx.get_historize_storage_path.return_value = historize_storage_path
    ctx.get_storage_path.return_value = storage_path
    return ctx


@pytest.mark.unit
class TestResolveHistorizeTableFormat:
    def test_pipeline_historize_wins(self):
        cfg = _make_historize_config(table_format="iceberg")
        ctx = _make_context(table_format="native", historize_table_format="delta")
        assert _resolve_historize_table_format(cfg, {}, ctx) == "iceberg"

    def test_pipeline_level_table_format_second(self):
        cfg = _make_historize_config()
        ctx = _make_context(table_format="native", historize_table_format="delta")
        result = _resolve_historize_table_format(cfg, {"table_format": "iceberg"}, ctx)
        assert result == "iceberg"

    def test_profile_historize_third(self):
        cfg = _make_historize_config()
        ctx = _make_context(table_format="native", historize_table_format="delta")
        result = _resolve_historize_table_format(cfg, {}, ctx)
        assert result == "delta"

    def test_profile_table_format_fourth(self):
        cfg = _make_historize_config()
        ctx = _make_context(table_format="iceberg", historize_table_format=None)
        result = _resolve_historize_table_format(cfg, {}, ctx)
        assert result == "iceberg"

    def test_falls_back_to_native(self):
        cfg = _make_historize_config()
        ctx = _make_context(table_format="native", historize_table_format=None)
        result = _resolve_historize_table_format(cfg, {}, ctx)
        assert result == "native"


@pytest.mark.unit
class TestResolveHistorizeStoragePath:
    def test_profile_historize_storage_path_first(self):
        ctx = _make_context(
            historize_storage_path="gs://historize-bucket/",
            storage_path="gs://default-bucket/",
        )
        assert _resolve_historize_storage_path(ctx) == "gs://historize-bucket/"

    def test_profile_storage_path_fallback(self):
        ctx = _make_context(historize_storage_path=None, storage_path="gs://bucket/")
        assert _resolve_historize_storage_path(ctx) == "gs://bucket/"

    def test_none_when_neither_set(self):
        ctx = _make_context(historize_storage_path=None, storage_path=None)
        assert _resolve_historize_storage_path(ctx) is None


@pytest.mark.unit
class TestResolveHistorizeSchemaAccess:
    def test_no_overlay_returns_ingest_list(self):
        result = _resolve_historize_schema_access(
            {"schema_access": ["OWNER:serviceAccount:sa@p.iam.gserviceaccount.com"]}
        )
        assert result == ["OWNER:serviceAccount:sa@p.iam.gserviceaccount.com"]

    def test_no_overlay_no_ingest_returns_none(self):
        assert _resolve_historize_schema_access({}) is None

    def test_overlay_merges_over_ingest(self):
        result = _resolve_historize_schema_access(
            {
                "schema_access": [
                    "OWNER:serviceAccount:owner@p.iam.gserviceaccount.com",
                    "WRITER:serviceAccount:writer@p.iam.gserviceaccount.com",
                ],
                "historize_schema_access": [
                    "READER:group:analysts@example.com",
                ],
            }
        )
        assert result == [
            "OWNER:serviceAccount:owner@p.iam.gserviceaccount.com",
            "WRITER:serviceAccount:writer@p.iam.gserviceaccount.com",
            "READER:group:analysts@example.com",
        ]

    def test_overlay_dedupes_duplicates(self):
        result = _resolve_historize_schema_access(
            {
                "schema_access": [
                    "OWNER:serviceAccount:owner@p.iam.gserviceaccount.com"
                ],
                "historize_schema_access": [
                    "OWNER:serviceAccount:owner@p.iam.gserviceaccount.com",
                    "READER:group:analysts@example.com",
                ],
            }
        )
        assert result == [
            "OWNER:serviceAccount:owner@p.iam.gserviceaccount.com",
            "READER:group:analysts@example.com",
        ]

    def test_overlay_only_no_ingest_returns_overlay(self):
        result = _resolve_historize_schema_access(
            {"historize_schema_access": ["READER:group:analysts@example.com"]}
        )
        assert result == ["READER:group:analysts@example.com"]


@pytest.mark.unit
class TestApplyNamingModuleHistorizeOverrides:
    """The factory honours a custom naming module's layer-aware hooks."""

    @pytest.fixture(autouse=True)
    def _reset_naming_module(self):
        import dlt_saga.pipeline_config.naming as naming_mod

        naming_mod._naming_module = None
        yield
        naming_mod._naming_module = None

    @pytest.fixture(autouse=True)
    def _reset_project_cache(self):
        from dlt_saga.project_config import _reset_cache

        _reset_cache()
        yield
        _reset_cache()

    @pytest.fixture(autouse=True)
    def _dev_schema(self, monkeypatch):
        """The dev naming path resolves a dev schema; configure one so it
        doesn't trip the hard error on the (removed) unconfigured fallback."""
        monkeypatch.setenv("SAGA_SCHEMA_NAME", "dlt_dev")

    def _install_naming_module(self, name: str, **funcs):
        mod = ModuleType(name)
        for k, v in funcs.items():
            setattr(mod, k, v)
        sys.modules[name] = mod
        return mod

    def _project_yaml(self, tmp_path, naming_module_name: str):
        yml = tmp_path / "saga_project.yml"
        yml.write_text(f"naming_module: {naming_module_name}\n")

    def test_no_segments_is_noop(self, tmp_path, monkeypatch):
        """Without a config_path we can't derive segments; hook isn't called."""
        cfg = HistorizeConfig.from_dict({}, top_level_primary_key=["id"])

        # No saga_project.yml → no naming module → noop regardless of segments
        monkeypatch.chdir(tmp_path)

        _apply_naming_module_historize_overrides(
            cfg, {}, source_schema="dlt_prod", source_table="orders"
        )
        assert cfg.output_schema is None
        assert cfg.output_table is None

    def test_layer_aware_schema_override(self, tmp_path, monkeypatch):
        def generate_schema_name(segments, environment, default, *, layer="ingest"):
            base = f"dlt_{segments[0]}"
            return f"{base}_{layer}"

        self._install_naming_module(
            "naming_layer_aware_schema", generate_schema_name=generate_schema_name
        )
        self._project_yaml(tmp_path, "naming_layer_aware_schema")
        monkeypatch.chdir(tmp_path)

        cfg = HistorizeConfig.from_dict({}, top_level_primary_key=["id"])
        _apply_naming_module_historize_overrides(
            cfg,
            {"config_path": "configs/google_sheets/asm/salgsmal.yml"},
            source_schema="dlt_google_sheets_ingest",
            source_table="asm__salgsmal",
        )
        assert cfg.output_schema == "dlt_google_sheets_historize"
        assert cfg.output_table is None  # no table override

    def test_layer_aware_table_override(self, tmp_path, monkeypatch):
        def generate_table_name(segments, environment, *, layer="ingest"):
            return f"{segments[-1]}_{layer}"

        self._install_naming_module(
            "naming_layer_aware_table", generate_table_name=generate_table_name
        )
        self._project_yaml(tmp_path, "naming_layer_aware_table")
        monkeypatch.chdir(tmp_path)

        cfg = HistorizeConfig.from_dict({}, top_level_primary_key=["id"])
        _apply_naming_module_historize_overrides(
            cfg,
            {"config_path": "configs/google_sheets/asm/salgsmal.yml"},
            source_schema="dlt_google_sheets",
            source_table="salgsmal_ingest",
        )
        assert cfg.output_table == "salgsmal_historize"

    def test_layer_agnostic_module_leaves_overrides_unset(self, tmp_path, monkeypatch):
        """A module returning the same name regardless of layer must not write
        an override that equals the ingest value — keeps placement-suffix flow alive."""

        def generate_schema_name(segments, environment, default, *, layer="ingest"):
            return f"dlt_{segments[0]}"  # ignores layer

        self._install_naming_module(
            "naming_layer_agnostic", generate_schema_name=generate_schema_name
        )
        self._project_yaml(tmp_path, "naming_layer_agnostic")
        monkeypatch.chdir(tmp_path)

        cfg = HistorizeConfig.from_dict({}, top_level_primary_key=["id"])
        _apply_naming_module_historize_overrides(
            cfg,
            {"config_path": "configs/google_sheets/asm/salgsmal.yml"},
            source_schema="dlt_google_sheets",
            source_table="asm__salgsmal",
        )
        assert cfg.output_schema is None

    def test_legacy_hook_without_layer_kwarg_still_called(self, tmp_path, monkeypatch):
        """Hooks predating ``layer`` must still work — call_hook drops kwargs
        the signature can't absorb."""
        called = {}

        def generate_schema_name(segments, environment, default):
            called["called"] = True
            return f"legacy_{segments[0]}"

        self._install_naming_module(
            "naming_legacy_signature", generate_schema_name=generate_schema_name
        )
        self._project_yaml(tmp_path, "naming_legacy_signature")
        monkeypatch.chdir(tmp_path)

        cfg = HistorizeConfig.from_dict({}, top_level_primary_key=["id"])
        _apply_naming_module_historize_overrides(
            cfg,
            {"config_path": "configs/google_sheets/asm/salgsmal.yml"},
            source_schema="dlt_google_sheets",
            source_table="asm__salgsmal",
        )
        assert called == {"called": True}
        # Legacy hook returns "legacy_google_sheets" which != source, so it's adopted.
        assert cfg.output_schema == "legacy_google_sheets"

    def test_explicit_output_schema_takes_precedence(self, tmp_path, monkeypatch):
        def generate_schema_name(segments, environment, default, *, layer="ingest"):
            return f"dlt_{segments[0]}_{layer}"

        self._install_naming_module(
            "naming_pinned_override", generate_schema_name=generate_schema_name
        )
        self._project_yaml(tmp_path, "naming_pinned_override")
        monkeypatch.chdir(tmp_path)

        cfg = HistorizeConfig.from_dict(
            {"output_schema": "explicit_archive"},
            top_level_primary_key=["id"],
        )
        _apply_naming_module_historize_overrides(
            cfg,
            {"config_path": "configs/google_sheets/asm/salgsmal.yml"},
            source_schema="dlt_google_sheets",
            source_table="asm__salgsmal",
        )
        assert cfg.output_schema == "explicit_archive"
