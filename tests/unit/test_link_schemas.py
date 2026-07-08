"""Unit tests for the config-file schema linker (modeline injection)."""

from pathlib import Path
from types import SimpleNamespace

import pytest

from dlt_saga.utility.link_schemas import (
    MODELINE_PREFIX,
    _existing_modeline_index,
    _relative_schema_path,
    apply_modeline,
    link_config_schemas,
)


@pytest.mark.unit
class TestRelativeSchemaPath:
    def test_posix_relative_path_two_levels_up(self):
        rel = _relative_schema_path(
            "/proj/configs/filesystem/sample.yml",
            "/proj/schemas",
            "filesystem_config.json",
        )
        assert rel == "../../schemas/filesystem_config.json"

    def test_always_forward_slashes(self):
        rel = _relative_schema_path(
            "/proj/configs/a.yml", "/proj/schemas", "api_config.json"
        )
        assert "\\" not in rel


@pytest.mark.unit
class TestExistingModelineIndex:
    def test_finds_modeline_first_line(self):
        lines = ["# yaml-language-server: $schema=../x.json", "tags: [daily]"]
        assert _existing_modeline_index(lines) == 0

    def test_finds_modeline_not_first(self):
        lines = ["# header comment", "# yaml-language-server: $schema=../x.json"]
        assert _existing_modeline_index(lines) == 1

    def test_none_when_absent(self):
        assert _existing_modeline_index(["tags: [daily]", "# just a comment"]) is None


@pytest.mark.unit
class TestApplyModeline:
    def _write(self, tmp_path, content):
        cfg = tmp_path / "configs" / "filesystem" / "sample.yml"
        cfg.parent.mkdir(parents=True)
        cfg.write_text(content, encoding="utf-8")
        return cfg

    def test_inserts_when_absent(self, tmp_path):
        cfg = self._write(tmp_path, "tags: [daily]\nwrite_disposition: append\n")
        schema_dir = tmp_path / "schemas"

        changed = apply_modeline(str(cfg), str(schema_dir), "filesystem_config.json")

        assert changed is True
        lines = cfg.read_text(encoding="utf-8").splitlines()
        assert lines[0] == f"{MODELINE_PREFIX}../../schemas/filesystem_config.json"
        # Rest of the file is preserved.
        assert "tags: [daily]" in lines
        assert "write_disposition: append" in lines

    def test_bom_prefixed_config_not_corrupted(self, tmp_path):
        # A UTF-8 BOM must be stripped on read, not pushed mid-stream by the
        # inserted modeline (which would make the first key parse as "﻿tags").
        cfg = tmp_path / "configs" / "filesystem" / "sample.yml"
        cfg.parent.mkdir(parents=True)
        cfg.write_bytes(b"\xef\xbb\xbftags: [daily]\n")

        changed = apply_modeline(
            str(cfg), str(tmp_path / "schemas"), "filesystem_config.json"
        )

        assert changed is True
        raw = cfg.read_bytes()
        # No BOM anywhere in the rewritten file.
        assert b"\xef\xbb\xbf" not in raw
        lines = cfg.read_text(encoding="utf-8").splitlines()
        assert lines[0] == f"{MODELINE_PREFIX}../../schemas/filesystem_config.json"
        assert "tags: [daily]" in lines

    def test_idempotent_when_correct(self, tmp_path):
        cfg = self._write(
            tmp_path,
            "# yaml-language-server: $schema=../../schemas/filesystem_config.json\n"
            "tags: [daily]\n",
        )
        before = cfg.read_text(encoding="utf-8")

        changed = apply_modeline(
            str(cfg), str(tmp_path / "schemas"), "filesystem_config.json"
        )

        assert changed is False
        assert cfg.read_text(encoding="utf-8") == before

    def test_replaces_stale_modeline(self, tmp_path):
        cfg = self._write(
            tmp_path,
            "# yaml-language-server: $schema=../../schemas/WRONG_config.json\n"
            "tags: [daily]\n",
        )

        changed = apply_modeline(
            str(cfg), str(tmp_path / "schemas"), "filesystem_config.json"
        )

        assert changed is True
        lines = cfg.read_text(encoding="utf-8").splitlines()
        assert lines[0] == f"{MODELINE_PREFIX}../../schemas/filesystem_config.json"
        # No duplicate modeline left behind.
        assert sum(1 for line in lines if "yaml-language-server" in line) == 1


@pytest.mark.unit
class TestLinkConfigSchemas:
    def test_links_each_config_by_adapter(self, tmp_path, monkeypatch):
        cfg = tmp_path / "configs" / "filesystem" / "sample.yml"
        cfg.parent.mkdir(parents=True)
        cfg.write_text("tags: [daily]\n", encoding="utf-8")

        fake_config = SimpleNamespace(
            source_type="file",
            identifier=str(cfg),
            pipeline_name="filesystem__sample",
            adapter="dlt_saga.filesystem",
        )
        fake_source = SimpleNamespace(
            discover=lambda: ({"filesystem": [fake_config]}, {})
        )

        # Keep the test hermetic: don't import adapter packages.
        monkeypatch.setattr(
            "dlt_saga.utility.link_schemas.schema_filename_for_adapter",
            lambda adapter, pipeline_group="", config_path=None: (
                "filesystem_config.json"
            ),
        )
        # No project-level files under the temp root, and no external profiles.
        monkeypatch.setattr(
            "dlt_saga.utility.cli.profiles._find_profiles_file", lambda: None
        )

        results = link_config_schemas(
            tmp_path / "schemas",
            config_source=fake_source,
            project_root=tmp_path,
        )

        assert len(results) == 1
        assert results[0].schema_filename == "filesystem_config.json"
        assert results[0].changed is True
        assert (
            cfg.read_text(encoding="utf-8").splitlines()[0].startswith(MODELINE_PREFIX)
        )

    def test_skips_when_adapter_unresolvable(self, tmp_path, monkeypatch):
        cfg = tmp_path / "configs" / "mystery" / "x.yml"
        cfg.parent.mkdir(parents=True)
        cfg.write_text("tags: [daily]\n", encoding="utf-8")

        fake_config = SimpleNamespace(
            source_type="file",
            identifier=str(cfg),
            pipeline_name="mystery__x",
            adapter="unknown.adapter",
        )
        fake_source = SimpleNamespace(discover=lambda: ({"mystery": [fake_config]}, {}))
        monkeypatch.setattr(
            "dlt_saga.utility.link_schemas.schema_filename_for_adapter",
            lambda adapter, pipeline_group="", config_path=None: None,
        )
        monkeypatch.setattr(
            "dlt_saga.utility.cli.profiles._find_profiles_file", lambda: None
        )

        results = link_config_schemas(
            tmp_path / "schemas",
            config_source=fake_source,
            project_root=tmp_path,
        )

        assert len(results) == 1
        assert results[0].schema_filename is None
        assert results[0].changed is False
        assert results[0].skipped_reason
        # Unresolvable config is left untouched.
        assert "yaml-language-server" not in cfg.read_text(encoding="utf-8")


@pytest.mark.unit
class TestLinkRootFiles:
    def _empty_source(self):
        return SimpleNamespace(discover=lambda: ({}, {}))

    def test_links_project_root_files(self, tmp_path, monkeypatch):
        (tmp_path / "saga_project.yml").write_text("pipelines: {}\n", encoding="utf-8")
        (tmp_path / "packages.yml").write_text("packages: []\n", encoding="utf-8")
        monkeypatch.setattr(
            "dlt_saga.utility.cli.profiles._find_profiles_file", lambda: None
        )

        results = link_config_schemas(
            tmp_path / "schemas",
            config_source=self._empty_source(),
            project_root=tmp_path,
        )

        by_name = {Path(r.config_path).name: r for r in results}
        assert by_name["saga_project.yml"].schema_filename == "saga_project_config.json"
        assert by_name["packages.yml"].schema_filename == "packages_config.json"
        for name, schema in (
            ("saga_project.yml", "saga_project_config.json"),
            ("packages.yml", "packages_config.json"),
        ):
            first = (tmp_path / name).read_text(encoding="utf-8").splitlines()[0]
            assert first == f"{MODELINE_PREFIX}schemas/{schema}"

    def test_skips_absent_root_files(self, tmp_path, monkeypatch):
        monkeypatch.setattr(
            "dlt_saga.utility.cli.profiles._find_profiles_file", lambda: None
        )

        results = link_config_schemas(
            tmp_path / "schemas",
            config_source=self._empty_source(),
            project_root=tmp_path,
        )

        assert results == []

    def test_links_profiles_inside_project(self, tmp_path, monkeypatch):
        profiles = tmp_path / "profiles.yml"
        profiles.write_text("default:\n", encoding="utf-8")
        monkeypatch.setattr(
            "dlt_saga.utility.cli.profiles._find_profiles_file", lambda: profiles
        )

        results = link_config_schemas(
            tmp_path / "schemas",
            config_source=self._empty_source(),
            project_root=tmp_path,
        )

        assert len(results) == 1
        assert results[0].schema_filename == "profiles_config.json"
        assert results[0].changed is True
        first = profiles.read_text(encoding="utf-8").splitlines()[0]
        assert first == f"{MODELINE_PREFIX}schemas/profiles_config.json"

    def test_suggests_for_external_profiles(self, tmp_path, monkeypatch):
        external = tmp_path / "outside"
        external.mkdir()
        profiles = external / "profiles.yml"
        profiles.write_text("default:\n", encoding="utf-8")
        project = tmp_path / "project"
        project.mkdir()
        monkeypatch.setattr(
            "dlt_saga.utility.cli.profiles._find_profiles_file", lambda: profiles
        )

        results = link_config_schemas(
            project / "schemas",
            config_source=self._empty_source(),
            project_root=project,
        )

        assert len(results) == 1
        r = results[0]
        assert r.changed is False
        assert r.suggestion is not None
        # An absolute schema path is suggested, not a brittle relative one.
        schema_abs = str((project / "schemas" / "profiles_config.json").resolve())
        assert schema_abs.replace("\\", "/") in r.suggestion
        # The external file is never modified.
        assert "yaml-language-server" not in profiles.read_text(encoding="utf-8")
