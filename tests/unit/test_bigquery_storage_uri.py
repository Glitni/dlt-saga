"""Unit tests for BigQuery storage-URI routing through generate_target_location."""

import sys
from types import ModuleType

import pytest

from dlt_saga.destinations.bigquery.config import BigQueryDestinationConfig
from dlt_saga.destinations.bigquery.destination import BigQueryDestination


@pytest.fixture(autouse=True)
def _reset_naming_module():
    import dlt_saga.pipeline_config.naming as naming_mod

    naming_mod._naming_module = None
    yield
    naming_mod._naming_module = None


@pytest.fixture(autouse=True)
def _reset_project_cache():
    from dlt_saga.project_config import _reset_cache

    _reset_cache()
    yield
    _reset_cache()


def _make_iceberg_destination(
    *,
    dataset_name: str = "dlt_google_sheets",
    storage_path: str = "gs://bucket/lake/",
) -> BigQueryDestination:
    cfg = BigQueryDestinationConfig(
        project_id="p",
        dataset_name=dataset_name,
        table_format="iceberg",
        storage_path=storage_path,
    )
    return BigQueryDestination(cfg)


def _install_naming_module(name: str, **funcs) -> ModuleType:
    mod = ModuleType(name)
    for k, v in funcs.items():
        setattr(mod, k, v)
    sys.modules[name] = mod
    return mod


def _project_yaml(tmp_path, naming_module_name: str):
    yml = tmp_path / "saga_project.yml"
    yml.write_text(f"naming_module: {naming_module_name}\n")


@pytest.mark.unit
class TestStorageUriDefault:
    """Without a custom naming module, URI shape matches the historical default."""

    def test_ingest_uri_default(self, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        dest = _make_iceberg_destination()
        assert (
            dest._resolve_storage_uri("dlt_google_sheets", "monthly", layer="ingest")
            == "gs://bucket/lake/dlt_google_sheets/monthly/"
        )

    def test_historize_uri_default(self, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        dest = _make_iceberg_destination()
        # Default impl is layer-agnostic — same shape either way.
        assert (
            dest._resolve_storage_uri("dlt_google_sheets", "monthly", layer="historize")
            == "gs://bucket/lake/dlt_google_sheets/monthly/"
        )

    def test_storage_path_without_trailing_slash(self, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        dest = _make_iceberg_destination(storage_path="gs://bucket/lake")
        assert dest._resolve_storage_uri("ds", "tbl") == "gs://bucket/lake/ds/tbl/"

    def test_non_gs_storage_path_raises(self, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        dest = _make_iceberg_destination(storage_path="s3://bucket/lake/")
        with pytest.raises(ValueError, match="must start with gs://"):
            dest._resolve_storage_uri("ds", "tbl")


@pytest.mark.unit
class TestStorageUriHookRouting:
    """A custom naming module's generate_target_location shapes the URI."""

    def test_layer_aware_shape(self, tmp_path, monkeypatch):
        def generate_target_location(
            segments,
            environment,
            default_storage_root,
            *,
            layer="ingest",
            schema=None,
            table=None,
        ):
            base = default_storage_root.rstrip("/")
            return f"{base}/{layer}/{schema}/{table}/"

        _install_naming_module(
            "naming_layer_uri", generate_target_location=generate_target_location
        )
        _project_yaml(tmp_path, "naming_layer_uri")
        monkeypatch.chdir(tmp_path)

        dest = _make_iceberg_destination()
        assert (
            dest._resolve_storage_uri("dlt_filesystem", "reports", layer="ingest")
            == "gs://bucket/lake/ingest/dlt_filesystem/reports/"
        )
        assert (
            dest._resolve_storage_uri(
                "dlt_filesystem_historized", "reports", layer="historize"
            )
            == "gs://bucket/lake/historize/dlt_filesystem_historized/reports/"
        )

    def test_hook_returning_none_falls_back_to_default(self, tmp_path, monkeypatch):
        def generate_target_location(*args, **kwargs):
            return None

        _install_naming_module(
            "naming_uri_none", generate_target_location=generate_target_location
        )
        _project_yaml(tmp_path, "naming_uri_none")
        monkeypatch.chdir(tmp_path)

        dest = _make_iceberg_destination()
        assert dest._resolve_storage_uri("ds", "tbl") == "gs://bucket/lake/ds/tbl/"

    def test_hook_raising_falls_back_to_default(self, tmp_path, monkeypatch):
        def generate_target_location(*args, **kwargs):
            raise RuntimeError("buggy hook")

        _install_naming_module(
            "naming_uri_raises", generate_target_location=generate_target_location
        )
        _project_yaml(tmp_path, "naming_uri_raises")
        monkeypatch.chdir(tmp_path)

        dest = _make_iceberg_destination()
        # Hook failure must not abort — caller falls back to the framework default.
        assert dest._resolve_storage_uri("ds", "tbl") == "gs://bucket/lake/ds/tbl/"

    def test_legacy_hook_without_layer_kwarg(self, tmp_path, monkeypatch):
        """Hook predating the layer/dataset/table kwargs still works — call_hook
        drops kwargs the signature can't absorb."""

        def generate_target_location(segments, environment, default_storage_root):
            return f"{default_storage_root.rstrip('/')}/legacy/"

        _install_naming_module(
            "naming_uri_legacy", generate_target_location=generate_target_location
        )
        _project_yaml(tmp_path, "naming_uri_legacy")
        monkeypatch.chdir(tmp_path)

        dest = _make_iceberg_destination()
        assert dest._resolve_storage_uri("ds", "tbl") == "gs://bucket/lake/legacy/"
