"""Unit tests for pipeline registry."""

import pytest

from dlt_saga.pipelines.registry import (
    _NAMESPACE_REGISTRY,
    _find_pipeline_class,
    discover_implementations,
    get_pipeline_class,
)


@pytest.mark.unit
class TestNamespaceResolution:
    def test_builtin_namespace_explicit(self):
        """adapter with dlt_saga namespace resolves built-in implementation."""
        cls = get_pipeline_class("api", adapter="dlt_saga.api")
        assert cls.__name__ == "ApiPipeline"

    def test_builtin_namespace_bare_name(self):
        """Bare name (no namespace prefix) resolves via default namespace."""
        cls = get_pipeline_class("api", adapter="api")
        assert cls.__name__ == "ApiPipeline"

    def test_builtin_base_implementation(self):
        """Base implementation resolves when no adapter given."""
        cls = get_pipeline_class("google_sheets")
        assert cls.__name__ == "GoogleSheetsPipeline"

    def test_folder_fallback_still_works(self):
        """Folder-structure fallback works when adapter omitted."""
        cls = get_pipeline_class(
            "google_sheets",
            config_path="configs/google_sheets/test.yml",
        )
        assert cls.__name__ == "GoogleSheetsPipeline"

    def test_invalid_adapter_raises(self):
        """Invalid adapter raises ImportError."""
        with pytest.raises(ImportError):
            get_pipeline_class("api", adapter="dlt_saga.api.nonexistent")

    def test_invalid_namespace_raises(self):
        """Unknown namespace with no default fallback raises ImportError."""
        with pytest.raises(ImportError):
            get_pipeline_class("api", adapter="unknown_ns.api.something")

    def test_each_impl_has_unique_class_name(self):
        """All API implementations should have unique class names."""
        impls = discover_implementations()
        api_impls = [i for i in impls if i["path"].startswith("api")]
        class_names = [i["class_name"] for i in api_impls]
        # Base ApiPipeline is shared, but all specialized ones should be unique
        specialized = [n for n in class_names if n != "ApiPipeline"]
        assert len(specialized) == len(set(specialized))


@pytest.mark.unit
class TestClassAutoDiscovery:
    def test_discovers_concrete_class(self):
        """Auto-discovery finds the concrete class defined in the module."""
        import importlib

        mod = importlib.import_module("dlt_saga.pipelines.filesystem.pipeline")
        cls = _find_pipeline_class(mod)
        assert cls.__name__ == "FilesystemPipeline"
        assert cls.__module__ == "dlt_saga.pipelines.filesystem.pipeline"

    def test_discovers_base_class(self):
        """Auto-discovery works for base pipeline modules too."""
        import importlib

        mod = importlib.import_module("dlt_saga.pipelines.google_sheets.pipeline")
        cls = _find_pipeline_class(mod)
        assert cls.__name__ == "GoogleSheetsPipeline"


@pytest.mark.unit
class TestDiscoverImplementations:
    def test_discovers_all_builtin(self):
        """discover_implementations finds all built-in pipeline modules."""
        impls = discover_implementations()
        paths = {i["path"] for i in impls}

        assert "api" in paths
        assert "database" in paths
        assert "filesystem" in paths
        assert "google_sheets" in paths

    def test_all_are_builtin(self):
        """All discovered implementations should be built-in."""
        impls = discover_implementations()
        assert all(i["source"] == "built-in" for i in impls)

    def test_default_namespace_registered(self):
        """dlt_saga namespace is always registered."""
        assert "dlt_saga" in _NAMESPACE_REGISTRY
