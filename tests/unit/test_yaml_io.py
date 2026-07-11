"""Unit tests for UTF-8 YAML config loading."""

import pytest

from dlt_saga.utility.yaml_io import load_yaml


@pytest.mark.unit
class TestLoadYaml:
    def test_reads_non_ascii_as_utf8(self, tmp_path):
        """A UTF-8 file with non-ASCII content must not be mangled.

        Regression: reading with the platform default (cp1252 on Windows)
        turned `ø` into `Ã¸`. `load_yaml` pins UTF-8, so the character survives.
        """
        path = tmp_path / "config.yml"
        path.write_bytes("description: tilhørende Amedia\n".encode("utf-8"))

        data = load_yaml(path)

        assert data["description"] == "tilhørende Amedia"
        # Guard against the specific cp1252-misread failure mode.
        assert "Ã¸" not in data["description"]

    def test_empty_file_returns_empty_dict(self, tmp_path):
        path = tmp_path / "empty.yml"
        path.write_bytes(b"")
        assert load_yaml(path) == {}

    def test_parses_structure(self, tmp_path):
        path = tmp_path / "config.yml"
        path.write_bytes(b"a: 1\nb: [x, y]\n")
        assert load_yaml(path) == {"a": 1, "b": ["x", "y"]}

    def test_duplicate_key_raises(self, tmp_path):
        """Duplicate mapping keys are a typo, not last-wins — fail loudly."""
        path = tmp_path / "dupe.yml"
        path.write_bytes(b"write_disposition: append\nwrite_disposition: replace\n")
        with pytest.raises(ValueError, match="duplicate key"):
            load_yaml(path)

    def test_merge_key_is_resolved(self, tmp_path):
        """`<<: *anchor` merge keys must load (common in profiles.yml).

        Regression: the custom loader replaced safe_load without calling
        flatten_mapping, so merge nodes raised "could not determine a
        constructor for the tag ...:merge".
        """
        path = tmp_path / "merge.yml"
        path.write_bytes(
            b"defaults: &defaults\n"
            b"  project: my-project\n"
            b"  location: EU\n"
            b"outputs:\n"
            b"  dev:\n"
            b"    <<: *defaults\n"
            b"    environment: dev\n"
        )
        data = load_yaml(path)
        assert data["outputs"]["dev"] == {
            "project": "my-project",
            "location": "EU",
            "environment": "dev",
        }

    def test_explicit_key_overrides_merged_value(self, tmp_path):
        """An explicit key wins over a merged one — not a duplicate error."""
        path = tmp_path / "override.yml"
        path.write_bytes(
            b"defaults: &defaults\n"
            b"  environment: dev\n"
            b"prod:\n"
            b"  <<: *defaults\n"
            b"  environment: prod\n"
        )
        data = load_yaml(path)
        assert data["prod"]["environment"] == "prod"

    def test_top_level_list_raises(self, tmp_path):
        """A top-level non-mapping would crash deep in the config merge."""
        path = tmp_path / "list.yml"
        path.write_bytes(b"- a\n- b\n")
        with pytest.raises(ValueError, match="mapping at the top level"):
            load_yaml(path)

    def test_top_level_scalar_raises(self, tmp_path):
        path = tmp_path / "scalar.yml"
        path.write_bytes(b"just a string\n")
        with pytest.raises(ValueError, match="mapping at the top level"):
            load_yaml(path)

    def test_syntax_error_names_path(self, tmp_path):
        path = tmp_path / "bad.yml"
        path.write_bytes(b"a: [unclosed\n")
        with pytest.raises(ValueError, match="bad.yml"):
            load_yaml(path)
