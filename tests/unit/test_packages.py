"""Unit tests for packages.yml loading."""

import sys
import textwrap

import pytest

from dlt_saga.packages import (
    PackageEntry,
    _register_package,
    load_packages,
    reset,
)
from dlt_saga.pipelines.registry import _NAMESPACE_REGISTRY


@pytest.fixture(autouse=True)
def _clean_state():
    """Reset package loader and registry state between tests."""
    # Save original state
    original_registry = dict(_NAMESPACE_REGISTRY)
    original_sys_path = sys.path.copy()

    yield

    # Restore state
    reset()
    _NAMESPACE_REGISTRY.clear()
    _NAMESPACE_REGISTRY.update(original_registry)
    sys.path[:] = original_sys_path


@pytest.mark.unit
class TestLoadPackages:
    def test_no_packages_yml(self, tmp_path):
        """No packages.yml → no error, no extra namespaces."""
        load_packages(project_root=tmp_path)
        assert "dlt_saga" in _NAMESPACE_REGISTRY

    def test_empty_packages_list(self, tmp_path):
        """packages.yml with empty list → no extra registrations."""
        (tmp_path / "packages.yml").write_text("packages: []\n")
        load_packages(project_root=tmp_path)

    def test_idempotent(self, tmp_path):
        """Calling load_packages twice only loads once."""
        (tmp_path / "packages.yml").write_text("packages: []\n")
        load_packages(project_root=tmp_path)
        load_packages(project_root=tmp_path)  # should be a no-op

    def test_loads_local_package(self, tmp_path):
        """Local path package registers namespace and adds to sys.path."""
        # Create a minimal Python package
        pkg_dir = tmp_path / "my_pipelines"
        pkg_dir.mkdir()
        (pkg_dir / "__init__.py").write_text("")

        packages_yml = tmp_path / "packages.yml"
        packages_yml.write_text(
            textwrap.dedent("""\
                packages:
                  - namespace: local
                    path: ./my_pipelines
            """)
        )

        load_packages(project_root=tmp_path)

        assert "local" in _NAMESPACE_REGISTRY
        assert _NAMESPACE_REGISTRY["local"] == "my_pipelines"
        assert str(tmp_path) in sys.path

    def test_invalid_yaml_warns(self, tmp_path):
        """Invalid YAML in packages.yml warns and continues."""
        (tmp_path / "packages.yml").write_text("!!invalid: {{{")
        # Should not raise
        load_packages(project_root=tmp_path)

    def test_nonexistent_path_warns(self, tmp_path, caplog):
        """Nonexistent package path warns and skips."""
        packages_yml = tmp_path / "packages.yml"
        packages_yml.write_text(
            textwrap.dedent("""\
                packages:
                  - namespace: bad
                    path: ./does_not_exist
            """)
        )

        load_packages(project_root=tmp_path)
        assert "does not exist" in caplog.text
        assert "bad" not in _NAMESPACE_REGISTRY

    def test_namespace_with_dots_warns(self, tmp_path, caplog):
        """Namespace containing dots warns and skips."""
        pkg_dir = tmp_path / "my_pipelines"
        pkg_dir.mkdir()
        (pkg_dir / "__init__.py").write_text("")

        packages_yml = tmp_path / "packages.yml"
        packages_yml.write_text(
            textwrap.dedent("""\
                packages:
                  - namespace: my.dotted.ns
                    path: ./my_pipelines
            """)
        )

        load_packages(project_root=tmp_path)
        assert "must not contain dots" in caplog.text
        assert "my.dotted.ns" not in _NAMESPACE_REGISTRY

    def test_missing_namespace_warns(self, tmp_path, caplog):
        """Package entry without namespace warns and skips."""
        pkg_dir = tmp_path / "my_pipelines"
        pkg_dir.mkdir()

        packages_yml = tmp_path / "packages.yml"
        packages_yml.write_text(
            textwrap.dedent("""\
                packages:
                  - path: ./my_pipelines
            """)
        )

        load_packages(project_root=tmp_path)
        assert "must have a 'namespace'" in caplog.text

    def test_missing_path_warns(self, tmp_path, caplog):
        """Package entry without path warns and skips."""
        packages_yml = tmp_path / "packages.yml"
        packages_yml.write_text(
            textwrap.dedent("""\
                packages:
                  - namespace: local
            """)
        )

        load_packages(project_root=tmp_path)
        assert "must have a 'path'" in caplog.text
        assert "local" not in _NAMESPACE_REGISTRY

    def test_bad_entry_does_not_block_subsequent(self, tmp_path, caplog):
        """A bad package entry doesn't prevent loading subsequent valid packages."""
        pkg_dir = tmp_path / "good_pipelines"
        pkg_dir.mkdir()
        (pkg_dir / "__init__.py").write_text("")

        packages_yml = tmp_path / "packages.yml"
        packages_yml.write_text(
            textwrap.dedent("""\
                packages:
                  - namespace: bad
                    path: ./does_not_exist
                  - namespace: good
                    path: ./good_pipelines
            """)
        )

        load_packages(project_root=tmp_path)
        assert "bad" not in _NAMESPACE_REGISTRY
        assert "good" in _NAMESPACE_REGISTRY


@pytest.mark.unit
class TestRegisterPackage:
    def test_registers_namespace(self, tmp_path):
        """_register_package adds namespace to registry."""
        pkg_dir = tmp_path / "ext_pipelines"
        pkg_dir.mkdir()
        (pkg_dir / "__init__.py").write_text("")

        _register_package(
            PackageEntry(namespace="ext", path="./ext_pipelines"), tmp_path
        )

        assert "ext" in _NAMESPACE_REGISTRY
        assert _NAMESPACE_REGISTRY["ext"] == "ext_pipelines"

    def test_sys_path_added_once(self, tmp_path):
        """Parent dir added to sys.path only once even if called twice."""
        pkg_dir = tmp_path / "pkg"
        pkg_dir.mkdir()
        (pkg_dir / "__init__.py").write_text("")

        _register_package(PackageEntry(namespace="a", path="./pkg"), tmp_path)
        count_before = sys.path.count(str(tmp_path))

        _register_package(PackageEntry(namespace="b", path="./pkg"), tmp_path)
        count_after = sys.path.count(str(tmp_path))

        assert count_before == count_after
