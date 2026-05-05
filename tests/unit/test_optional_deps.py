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
