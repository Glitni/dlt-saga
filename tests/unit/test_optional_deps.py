"""Unit tests for dlt_saga.utility.optional_deps."""

import pytest


@pytest.mark.unit
class TestRequireOptional:
    def test_stdlib_package_does_not_raise(self):
        from dlt_saga.utility.optional_deps import require_optional

        require_optional("json", "stdlib test")  # json is always available

    def test_missing_package_raises_import_error(self):
        from dlt_saga.utility.optional_deps import require_optional

        with pytest.raises(
            ImportError, match="requires the '_nonexistent_xyz_12345_' package"
        ):
            require_optional("_nonexistent_xyz_12345_", "test purpose")

    def test_error_message_includes_purpose(self):
        from dlt_saga.utility.optional_deps import require_optional

        with pytest.raises(ImportError, match="test purpose"):
            require_optional("_nonexistent_pkg_abc_", "test purpose")

    def test_error_message_includes_install_hint(self):
        from dlt_saga.utility.optional_deps import require_optional

        with pytest.raises(ImportError, match="pip install"):
            require_optional("_nonexistent_pkg_abc_", "test purpose")

    def test_dotted_package_resolves_top_level(self):
        from dlt_saga.utility.optional_deps import require_optional

        # os.path is always available — the top-level "os" import should succeed
        require_optional("os.path", "os.path test")

    def test_transitive_import_error_propagates(self, monkeypatch):
        """A transitive import failure (target package installed, but one of its
        own deps missing) must surface the real cause, not be masked as "install
        the target package"."""
        import builtins

        from dlt_saga.utility import optional_deps

        real_import = builtins.__import__

        def fake_import(name, *args, **kwargs):
            if name == "pkg_present_bad_dep":
                err = ImportError("No module named 'some_missing_dep'")
                err.name = "some_missing_dep"
                raise err
            return real_import(name, *args, **kwargs)

        monkeypatch.setattr(builtins, "__import__", fake_import)

        with pytest.raises(ImportError, match="some_missing_dep") as excinfo:
            optional_deps.require_optional("pkg_present_bad_dep", "test purpose")
        # The real transitive error, not the friendly "requires the ... package".
        assert "requires the 'pkg_present_bad_dep' package" not in str(excinfo.value)
