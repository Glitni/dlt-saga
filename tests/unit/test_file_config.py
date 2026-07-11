"""Unit tests for file-based pipeline configuration."""

from unittest.mock import patch

import pytest

from dlt_saga.pipeline_config.file_config import FilePipelineConfig


@pytest.fixture(autouse=True)
def _dev_schema(monkeypatch):
    """Dev discovery resolves a dev schema. Configure one (same value the old
    silent fallback produced) so tests don't trip the hard error that now
    replaces the unconfigured ``dlt_dev`` default."""
    monkeypatch.setenv("SAGA_SCHEMA_NAME", "dlt_dev")


@pytest.mark.unit
class TestDeriveBaseTableName:
    @pytest.mark.parametrize(
        "path, expected",
        [
            ("configs/google_sheets/data.yml", "data"),
            ("configs/google_sheets/asm/salgsmal.yml", "asm__salgsmal"),
            ("configs/api/livewrapped/stats.yml", "livewrapped__stats"),
            ("configs/google_sheets/region/norway/oslo.yml", "region__norway__oslo"),
            ("configs/google_sheets/My File.yml", "my_file"),
            ("configs/google_sheets/my-file.yml", "my_file"),
            ("configs/google_sheets/MyFile.yml", "myfile"),
            ("configs/google_sheets/data.yaml", "data"),
            ("", "default_data"),
        ],
    )
    def test_derive_base_table_name(self, path, expected):
        assert FilePipelineConfig()._derive_base_table_name(path) == expected


@pytest.mark.unit
class TestDiscoveryCache:
    """discover() memoizes the expensive walk but hands back fresh containers."""

    def _make_source(self, tmp_path):
        configs = tmp_path / "configs" / "google_sheets"
        configs.mkdir(parents=True)
        (configs / "data.yml").write_text(
            "write_disposition: append\n", encoding="utf-8"
        )
        return FilePipelineConfig(root_dir=str(tmp_path / "configs"))

    def test_walk_runs_once_across_calls(self, tmp_path):
        source = self._make_source(tmp_path)
        with patch.object(source, "_discover_all", wraps=source._discover_all) as spy:
            source.discover()
            source.discover()
            source.get_config("google_sheets__data")  # also goes through discover()
        spy.assert_called_once()

    def test_returns_fresh_containers_each_call(self, tmp_path):
        source = self._make_source(tmp_path)
        enabled_a, _ = source.discover()
        enabled_b, _ = source.discover()
        # New dict + new list objects, so a caller mutating one result can't
        # corrupt the cache or a later call.
        assert enabled_a is not enabled_b
        assert enabled_a["google_sheets"] is not enabled_b["google_sheets"]
        enabled_a["google_sheets"].clear()
        enabled_a["injected"] = []
        enabled_c, _ = source.discover()
        assert "injected" not in enabled_c
        assert len(enabled_c["google_sheets"]) == 1


@pytest.mark.unit
class TestSchemaNameResolutionGate:
    """`resolve_schema_names=False` lets linking load configs without a profile.

    Linking only needs each config's adapter; resolving the dev schema would
    otherwise require a configured profile just to write modelines.
    """

    def _write_config(self, tmp_path):
        configs = tmp_path / "configs" / "google_sheets"
        configs.mkdir(parents=True)
        (configs / "data.yml").write_text(
            "write_disposition: append\n", encoding="utf-8"
        )
        return str(tmp_path / "configs")

    def test_default_resolves_schema_name(self, tmp_path):
        # Default (run/ingest/report path): schema name is resolved eagerly.
        root = self._write_config(tmp_path)
        with patch.object(
            FilePipelineConfig, "resolve_schema_name", return_value="dlt_custom"
        ) as spy:
            enabled, _ = FilePipelineConfig(root_dir=root).discover()
        spy.assert_called()
        assert enabled["google_sheets"][0].schema_name == "dlt_custom"

    def test_link_mode_never_resolves_schema_name(self, tmp_path):
        # Link mode must not call the resolver at all (so an unresolved dev
        # schema can't crash it), and leaves schema_name empty.
        root = self._write_config(tmp_path)
        with patch.object(
            FilePipelineConfig,
            "resolve_schema_name",
            side_effect=AssertionError("resolver must not be called in link mode"),
        ):
            enabled, _ = FilePipelineConfig(
                root_dir=root, resolve_schema_names=False
            ).discover()
        assert enabled["google_sheets"][0].schema_name == ""

    def test_link_mode_does_not_poison_a_normal_source(self, tmp_path):
        # The empty schema_name is confined to the link-mode instance's own
        # cache; a separate default source over the same configs still resolves.
        root = self._write_config(tmp_path)
        link_enabled, _ = FilePipelineConfig(
            root_dir=root, resolve_schema_names=False
        ).discover()
        assert link_enabled["google_sheets"][0].schema_name == ""

        with patch.object(
            FilePipelineConfig, "resolve_schema_name", return_value="dlt_resolved"
        ):
            normal_enabled, _ = FilePipelineConfig(root_dir=root).discover()
        assert normal_enabled["google_sheets"][0].schema_name == "dlt_resolved"


@pytest.mark.unit
class TestGetPipelineGroupFromPath:
    @pytest.mark.parametrize(
        "path, expected",
        [
            ("configs/google_sheets/data.yml", "google_sheets"),
            ("configs/filesystem/files.yml", "filesystem"),
            ("configs/api/stats.yml", "api"),
            ("configs/google_sheets/asm/salgsmal.yml", "google_sheets"),
            ("configs/api/livewrapped/stats.yml", "api"),
        ],
    )
    def test_extract_type(self, path, expected):
        assert FilePipelineConfig()._get_pipeline_group_from_path(path) == expected

    @pytest.mark.parametrize(
        "path",
        [
            "google_sheets/data.yml",
            "configs/",
        ],
    )
    def test_invalid_path_raises(self, path):
        with pytest.raises(ValueError, match="Could not determine pipeline group"):
            FilePipelineConfig()._get_pipeline_group_from_path(path)


@pytest.mark.unit
class TestGetPathSegments:
    @pytest.mark.parametrize(
        "path, expected",
        [
            ("configs/google_sheets/data.yml", ["google_sheets"]),
            ("configs/google_sheets/asm/salgsmal.yml", ["google_sheets", "asm"]),
            (
                "configs/google_sheets/region/norway/oslo.yml",
                ["google_sheets", "region", "norway"],
            ),
            ("configs/api/livewrapped/stats.yml", ["api", "livewrapped"]),
        ],
    )
    def test_path_segments(self, path, expected):
        assert FilePipelineConfig()._get_path_segments(path) == expected


@pytest.mark.unit
class TestResolveValue:
    @pytest.mark.parametrize(
        "parent, child, inherit, expected",
        [
            # No parent
            (None, "child", True, "child"),
            (None, "child", False, "child"),
            # No child
            ("parent", None, True, "parent"),
            ("parent", None, False, None),
            # Override mode (inherit=False)
            ("parent", "child", False, "child"),
            ([1, 2], [3, 4], False, [3, 4]),
            ({"a": 1}, {"b": 2}, False, {"b": 2}),
            # Inherit primitives (child wins)
            ("parent", "child", True, "child"),
            (10, 20, True, 20),
            (True, False, True, False),
        ],
    )
    def test_resolve_value(self, parent, child, inherit, expected):
        assert FilePipelineConfig()._resolve_value(parent, child, inherit) == expected

    @pytest.mark.parametrize(
        "parent, child, expected",
        [
            ([1, 2], [3, 4], [1, 2, 3, 4]),
            ([1, 2], [2, 3], [1, 2, 3]),
            (["daily"], ["hourly", "critical"], ["daily", "hourly", "critical"]),
            # Lists of dicts (e.g. `+filters:` / `+columns:` entries) are
            # unhashable — a set-based dedup would raise TypeError.
            (
                [{"field": "a"}],
                [{"field": "b"}],
                [{"field": "a"}, {"field": "b"}],
            ),
            # Identical dict entries still dedupe (by equality, order preserved).
            (
                [{"field": "a"}],
                [{"field": "a"}, {"field": "b"}],
                [{"field": "a"}, {"field": "b"}],
            ),
        ],
    )
    def test_inherit_lists(self, parent, child, expected):
        assert (
            FilePipelineConfig()._resolve_value(parent, child, inherit=True) == expected
        )

    @pytest.mark.parametrize(
        "parent, child, expected",
        [
            ({"a": 1}, {"b": 2}, {"a": 1, "b": 2}),
            ({"a": 1, "b": 2}, {"b": 3, "c": 4}, {"a": 1, "b": 3, "c": 4}),
        ],
    )
    def test_inherit_dicts(self, parent, child, expected):
        assert (
            FilePipelineConfig()._resolve_value(parent, child, inherit=True) == expected
        )


@pytest.mark.unit
class TestIsPathSegmentDict:
    """A dict is a folder scope only when its key names a real config sub-directory
    — config-value dicts (persist_docs, historize, …) are not folders.
    """

    def _config(self, folders):
        c = FilePipelineConfig()
        c._folder_segment_names_cache = set(folders)
        return c

    @pytest.mark.parametrize(
        "key, value, folders, expected",
        [
            ("google_sheets", {"+tags": []}, {"google_sheets"}, True),  # real folder
            ("+google_sheets", {"+tags": []}, {"google_sheets"}, True),  # + stripped
            (
                "persist_docs",
                {"columns": True},
                {"google_sheets"},
                False,
            ),  # config block
            ("historize", {"track_deletions": True}, {"google_sheets"}, False),
            ("google_sheets", [], {"google_sheets"}, False),  # not a dict
            ("google_sheets", "string", {"google_sheets"}, False),
            ("google_sheets", {"a": 1}, set(), False),  # no such folder
        ],
    )
    def test_is_path_segment_dict(self, key, value, folders, expected):
        assert self._config(folders)._is_path_segment_dict(key, value) is expected


@pytest.mark.unit
class TestResolveConfig:
    def test_without_project_config(self):
        config = FilePipelineConfig()
        config.project_config = {}
        file_config = {"tags": ["daily"], "enabled": True}

        result = config._resolve_config("configs/google_sheets/data.yml", file_config)
        assert result == file_config

    def test_with_project_defaults(self):
        config = FilePipelineConfig()
        config.project_config = {
            "pipelines": {"tags": ["default_tag"], "enabled": True}
        }

        result = config._resolve_config("configs/google_sheets/data.yml", {})
        assert result["tags"] == ["default_tag"]
        assert result["enabled"] is True

    def test_with_folder_hierarchy(self):
        config = FilePipelineConfig()
        config._folder_segment_names_cache = {"google_sheets"}
        config.project_config = {
            "pipelines": {
                "tags": ["default"],
                "google_sheets": {"+tags": ["sheets_tag"], "enabled": True},
            }
        }

        result = config._resolve_config("configs/google_sheets/data.yml", {})
        assert "default" in result["tags"]
        assert "sheets_tag" in result["tags"]
        assert result["enabled"] is True

    def test_file_override(self):
        config = FilePipelineConfig()
        config.project_config = {"pipelines": {"tags": ["default"], "enabled": True}}

        result = config._resolve_config(
            "configs/google_sheets/data.yml", {"tags": ["custom"], "enabled": False}
        )
        assert result["tags"] == ["custom"]
        assert result["enabled"] is False

    def test_inherit_syntax(self):
        config = FilePipelineConfig()
        config.project_config = {"pipelines": {"tags": ["default"], "enabled": True}}

        result = config._resolve_config(
            "configs/google_sheets/data.yml", {"+tags": ["custom"], "enabled": False}
        )
        assert "default" in result["tags"]
        assert "custom" in result["tags"]
        assert result["enabled"] is False

    def test_dict_valued_project_default_applies(self):
        # Regression: a dict-valued project default (the documented
        # `pipelines: {+persist_docs: {columns: true}}` opt-in) was dropped by the
        # old "any dict with lowercase keys is a folder" heuristic.
        config = FilePipelineConfig()
        config._folder_segment_names_cache = {"google_sheets"}
        config.project_config = {"pipelines": {"+persist_docs": {"columns": True}}}

        result = config._resolve_config("configs/google_sheets/data.yml", {})
        assert result["persist_docs"] == {"columns": True}

    def test_group_dict_default_reaches_subfolder_config(self):
        # Regression: group-level dict defaults (e.g. +historize) applied to
        # configs directly in the group folder but were dropped for subfolder
        # configs (the group block was skipped as a "path segment").
        config = FilePipelineConfig()
        config._folder_segment_names_cache = {"google_sheets", "team_a"}
        config.project_config = {
            "pipelines": {
                "google_sheets": {
                    "+historize": {"track_deletions": True},
                    "team_a": {"+tags": ["a"]},
                }
            }
        }

        result = config._resolve_config("configs/google_sheets/team_a/data.yml", {})
        assert result["historize"] == {"track_deletions": True}
        assert "a" in result["tags"]


@pytest.mark.unit
class TestConfigInterpolation:
    """Env-var / Jinja interpolation is applied at YAML load time."""

    def test_pipeline_config_renders_env_var_with_filter(self, tmp_path, monkeypatch):
        monkeypatch.setenv("GCP_DATASET", "my-proj-dev")
        configs = tmp_path / "configs" / "google_sheets"
        configs.mkdir(parents=True)
        cfg = configs / "data.yml"
        cfg.write_text(
            "write_disposition: append\n"
            "dataset: \"{{ env_var('GCP_DATASET') | replace('-', '_') }}\"\n",
            encoding="utf-8",
        )

        source = FilePipelineConfig(root_dir=str(tmp_path / "configs"))
        result = source._load_config_file(str(cfg))

        assert result.config_dict["dataset"] == "my_proj_dev"

    def test_project_defaults_render_env_var(self, tmp_path, monkeypatch):
        monkeypatch.setenv("TEAM_TAG", "platform")
        (tmp_path / "saga_project.yml").write_text(
            "pipelines:\n  +tags: [\"{{ env_var('TEAM_TAG') }}\"]\n",
            encoding="utf-8",
        )
        configs = tmp_path / "configs" / "google_sheets"
        configs.mkdir(parents=True)

        source = FilePipelineConfig(root_dir=str(tmp_path / "configs"))
        assert source.project_config["pipelines"]["+tags"] == ["platform"]


@pytest.mark.unit
class TestDevOverrides:
    """The `dev:` block overrides config keys in dev, is stripped everywhere."""

    def _config(self, project_config=None):
        c = FilePipelineConfig()
        c.project_config = project_config or {}
        return c

    def test_applied_in_dev_and_stripped(self):
        c = self._config()
        with patch(
            "dlt_saga.pipeline_config.file_config.get_environment", return_value="dev"
        ):
            resolved = c._apply_dev_overrides(
                {"initial_value": "2020-01-01", "dev": {"initial_value": "2026-06-20"}}
            )
        assert resolved["initial_value"] == "2026-06-20"
        assert "dev" not in resolved

    def test_ignored_in_prod_and_stripped(self):
        c = self._config()
        with patch(
            "dlt_saga.pipeline_config.file_config.get_environment", return_value="prod"
        ):
            resolved = c._apply_dev_overrides(
                {"initial_value": "2020-01-01", "dev": {"initial_value": "2026-06-20"}}
            )
        assert resolved["initial_value"] == "2020-01-01"
        assert "dev" not in resolved

    def test_no_dev_block_is_noop(self):
        c = self._config()
        with patch(
            "dlt_saga.pipeline_config.file_config.get_environment", return_value="dev"
        ):
            resolved = c._apply_dev_overrides({"initial_value": "2020-01-01"})
        assert resolved == {"initial_value": "2020-01-01"}

    def test_folder_group_dev_default_inherits_and_is_scoped(self):
        # The natural placement: a folder (pipeline-group) default. Must reach
        # the group at any depth, and not leak to other groups.
        c = self._config(
            {"pipelines": {"api": {"+dev": {"initial_value": "API_GROUP"}}}}
        )
        with patch(
            "dlt_saga.pipeline_config.file_config.get_environment", return_value="dev"
        ):
            shallow = c._apply_dev_overrides(c._resolve_config("configs/api/x.yml", {}))
            nested = c._apply_dev_overrides(
                c._resolve_config("configs/api/myservice/x.yml", {})
            )
            other = c._apply_dev_overrides(
                c._resolve_config("configs/google_sheets/x.yml", {})
            )
        assert shallow["initial_value"] == "API_GROUP"
        assert nested["initial_value"] == "API_GROUP"  # dev under non-last segment
        assert "initial_value" not in other  # scoped to the api group

    def test_project_wide_dev_default_inherits(self):
        # A top-level `pipelines: +dev:` default must reach pipelines despite the
        # folder-segment heuristic (dev is a reserved config block, not a folder).
        c = self._config({"pipelines": {"+dev": {"initial_value": "PROJECT"}}})
        with patch(
            "dlt_saga.pipeline_config.file_config.get_environment", return_value="dev"
        ):
            resolved = c._apply_dev_overrides(
                c._resolve_config("configs/api/x.yml", {})
            )
        assert resolved["initial_value"] == "PROJECT"
        assert "dev" not in resolved

    def test_file_dev_overrides_project_default(self):
        c = self._config({"pipelines": {"+dev": {"initial_value": "PROJECT"}}})
        with patch(
            "dlt_saga.pipeline_config.file_config.get_environment", return_value="dev"
        ):
            resolved = c._apply_dev_overrides(
                c._resolve_config(
                    "configs/api/x.yml", {"dev": {"initial_value": "FILE"}}
                )
            )
        assert resolved["initial_value"] == "FILE"

    def test_dynamic_rolling_date_via_jinja(self, tmp_path):
        # End-to-end: a Jinja datetime expression in the dev block resolves to a
        # concrete rolling date at load time.
        from datetime import datetime, timedelta, timezone

        configs = tmp_path / "configs" / "api"
        configs.mkdir(parents=True)
        (configs / "events.yml").write_text(
            "write_disposition: append\n"
            "incremental_column: event_date\n"
            "initial_value: 2020-01-01\n"
            "dev:\n"
            '  initial_value: "{{ (datetime.now(timezone.utc) - timedelta(days=7))'
            ".strftime('%Y-%m-%d') }}\"\n",
            encoding="utf-8",
        )
        expected = (datetime.now(timezone.utc) - timedelta(days=7)).strftime("%Y-%m-%d")

        source = FilePipelineConfig(root_dir=str(tmp_path / "configs"))
        with patch(
            "dlt_saga.pipeline_config.file_config.get_environment", return_value="dev"
        ):
            result = source._load_config_file(str(configs / "events.yml"))
        assert result.config_dict["initial_value"] == expected


@pytest.mark.unit
class TestLoadProjectConfig:
    def test_nonexistent(self):
        with patch("pathlib.Path.exists", return_value=False):
            assert FilePipelineConfig().project_config == {}

    def test_valid(self):
        mock_config = {"pipelines": {"tags": ["default"]}}
        with patch(
            "dlt_saga.pipeline_config.file_config.load_yaml",
            return_value=mock_config,
        ):
            with patch("pathlib.Path.exists", return_value=True):
                assert FilePipelineConfig().project_config == mock_config

    def test_invalid_yaml(self):
        with patch(
            "dlt_saga.pipeline_config.file_config.load_yaml",
            side_effect=ValueError("Invalid YAML"),
        ):
            with patch("pathlib.Path.exists", return_value=True):
                assert FilePipelineConfig().project_config == {}


@pytest.mark.unit
class TestApplyLevelConfig:
    @pytest.mark.parametrize(
        "resolved, level_config, expected_tags",
        [
            ({"tags": ["existing"]}, {"tags": ["new"]}, ["new"]),
            ({"tags": ["existing"]}, {"+tags": ["new"]}, ["existing", "new"]),
        ],
    )
    def test_apply(self, resolved, level_config, expected_tags):
        config = FilePipelineConfig()
        config._apply_level_config(resolved, level_config, "segment")
        assert resolved["tags"] == expected_tags

    def test_new_key(self):
        config = FilePipelineConfig()
        resolved = {}
        config._apply_level_config(resolved, {"enabled": True}, "segment")
        assert resolved["enabled"] is True


@pytest.mark.unit
class TestExtractLevelConfig:
    def test_without_path_segments(self):
        config = FilePipelineConfig()
        current = {"tags": ["daily"], "enabled": True}
        assert config._extract_level_config(current, is_last_segment=True) == current

    def test_skips_path_segments(self):
        config = FilePipelineConfig()
        config._folder_segment_names_cache = {"google_sheets"}
        current = {"tags": ["daily"], "google_sheets": {"tags": ["nested"]}}

        result = config._extract_level_config(current, is_last_segment=False)
        assert result == {"tags": ["daily"]}

        result = config._extract_level_config(current, is_last_segment=True)
        assert "google_sheets" in result

    def test_keeps_dict_valued_config_block(self):
        # A dict config value whose key is not a folder (persist_docs) must be
        # kept even when it isn't the last segment.
        config = FilePipelineConfig()
        config._folder_segment_names_cache = {"google_sheets"}
        current = {"+persist_docs": {"columns": True}, "google_sheets": {"tags": []}}
        result = config._extract_level_config(current, is_last_segment=False)
        assert result == {"+persist_docs": {"columns": True}}


@pytest.mark.unit
class TestGetNamingSegments:
    @pytest.mark.parametrize(
        "path, expected",
        [
            (
                "configs/google_sheets/asm/salgsmal.yml",
                ["google_sheets", "asm", "salgsmal"],
            ),
            ("configs/filesystem/data.yml", ["filesystem", "data"]),
            ("configs/api/service.yaml", ["api", "service"]),
        ],
    )
    def test_segments_from_path(self, path, expected):
        assert FilePipelineConfig().get_naming_segments(path) == expected

    def test_empty_path_returns_empty(self):
        assert FilePipelineConfig().get_naming_segments("") == []


@pytest.mark.unit
class TestMultipleRootDirs:
    def test_list_constructor_stores_all_roots(self):
        fpc = FilePipelineConfig(root_dir=["configs", "extra_configs"])
        assert fpc._root_dirs == ["configs", "extra_configs"]
        assert fpc.root_dir == "configs"

    def test_empty_list_falls_back_to_default(self):
        fpc = FilePipelineConfig(root_dir=[])
        assert fpc._root_dirs == ["configs"]

    def test_single_string_constructor(self):
        fpc = FilePipelineConfig(root_dir="my_configs")
        assert fpc._root_dirs == ["my_configs"]
        assert fpc.root_dir == "my_configs"


@pytest.mark.unit
class TestMalformedConfigFails:
    """A malformed config must fail discovery loudly, not silently vanish."""

    def test_collect_records_failure_instead_of_swallowing(self):
        fpc = FilePipelineConfig()
        failures: list = []
        with patch.object(fpc, "_load_config_file", side_effect=ValueError("bad yaml")):
            fpc._collect_config_file("configs/x.yml", {}, {}, {}, [], failures)
        assert len(failures) == 1
        assert "configs/x.yml" in failures[0]

    def test_discover_raises_on_malformed_config(self, tmp_path):
        cfg_dir = tmp_path / "configs" / "grp"
        cfg_dir.mkdir(parents=True)
        (cfg_dir / "bad.yml").write_text("foo: [unclosed", encoding="utf-8")
        fpc = FilePipelineConfig(root_dir=str(tmp_path / "configs"))
        with pytest.raises(ValueError, match="Failed to load pipeline config"):
            fpc.discover()


@pytest.mark.unit
class TestDiscoverNonExistentDirectory:
    def test_warns_and_returns_empty_on_missing_dir(self, caplog):
        import logging

        fpc = FilePipelineConfig(root_dir="__nonexistent_dir_xyz__")
        with caplog.at_level(logging.WARNING):
            enabled, disabled = fpc.discover()
        assert enabled == {}
        assert disabled == {}
        assert any("__nonexistent_dir_xyz__" in m for m in caplog.messages)


@pytest.mark.unit
class TestFilePipelineConfigIntegration:
    def test_inheritance_chain(self):
        config = FilePipelineConfig()
        config.project_config = {
            "pipelines": {
                "tags": ["global"],
                "enabled": True,
                "google_sheets": {
                    "+tags": ["sheets"],
                    "asm": {"+tags": ["asm"]},
                },
            }
        }

        result = config._resolve_config(
            "configs/google_sheets/asm/data.yml", {"+tags": ["file_level"]}
        )
        for tag in ["global", "sheets", "asm", "file_level"]:
            assert tag in result["tags"]
        assert result["enabled"] is True
