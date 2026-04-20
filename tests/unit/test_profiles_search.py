"""Unit tests for profiles.yml search path logic."""

from pathlib import Path

import pytest

from dlt_saga.utility.cli.profiles import (
    ProfilesConfig,
    ProfileTarget,
    _find_profiles_file,
)

SAMPLE_PROFILES = """\
default:
  target: dev
  outputs:
    dev:
      type: bigquery
      database: test-project
      location: EU
"""


@pytest.fixture(autouse=True)
def _reset_profiles_singleton():
    """Reset the global profiles singleton between tests."""
    import dlt_saga.utility.cli.profiles as mod

    mod._profiles_config = None
    yield
    mod._profiles_config = None


@pytest.mark.unit
class TestFindProfilesFile:
    def test_env_var_takes_priority(self, tmp_path, monkeypatch):
        """SAGA_PROFILES_DIR should be checked first."""
        env_dir = tmp_path / "custom"
        env_dir.mkdir()
        (env_dir / "profiles.yml").write_text(SAMPLE_PROFILES)
        # Also create repo-root file to prove env var wins
        (tmp_path / "profiles.yml").write_text(SAMPLE_PROFILES)

        monkeypatch.chdir(tmp_path)
        monkeypatch.setenv("SAGA_PROFILES_DIR", str(env_dir))

        result = _find_profiles_file()
        assert result == env_dir / "profiles.yml"

    def test_finds_repo_root_profiles(self, tmp_path, monkeypatch):
        """./profiles.yml at repo root is found."""
        (tmp_path / "profiles.yml").write_text(SAMPLE_PROFILES)

        monkeypatch.chdir(tmp_path)
        monkeypatch.delenv("SAGA_PROFILES_DIR", raising=False)

        result = _find_profiles_file()
        assert result == Path("profiles.yml")

    def test_returns_none_when_not_found(self, tmp_path, monkeypatch):
        """Returns None when no profiles.yml exists anywhere."""
        monkeypatch.chdir(tmp_path)
        monkeypatch.delenv("SAGA_PROFILES_DIR", raising=False)

        result = _find_profiles_file()
        assert result is None

    def test_env_var_dir_without_file(self, tmp_path, monkeypatch):
        """If SAGA_PROFILES_DIR points to a dir without profiles.yml, continue searching."""
        env_dir = tmp_path / "empty_dir"
        env_dir.mkdir()
        # But put one in repo root
        (tmp_path / "profiles.yml").write_text(SAMPLE_PROFILES)

        monkeypatch.chdir(tmp_path)
        monkeypatch.setenv("SAGA_PROFILES_DIR", str(env_dir))

        result = _find_profiles_file()
        assert result == Path("profiles.yml")


@pytest.mark.unit
class TestProfilesConfigSearchPath:
    def test_profiles_exist_with_repo_root(self, tmp_path, monkeypatch):
        (tmp_path / "profiles.yml").write_text(SAMPLE_PROFILES)
        monkeypatch.chdir(tmp_path)
        monkeypatch.delenv("SAGA_PROFILES_DIR", raising=False)

        config = ProfilesConfig()
        assert config.profiles_exist()

    def test_profiles_exist_false_when_missing(self, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        monkeypatch.delenv("SAGA_PROFILES_DIR", raising=False)

        config = ProfilesConfig()
        assert not config.profiles_exist()

    def test_load_from_repo_root(self, tmp_path, monkeypatch):
        (tmp_path / "profiles.yml").write_text(SAMPLE_PROFILES)
        monkeypatch.chdir(tmp_path)
        monkeypatch.delenv("SAGA_PROFILES_DIR", raising=False)

        config = ProfilesConfig()
        target = config.get_target("default", "dev")
        assert target.database == "test-project"


@pytest.mark.unit
class TestProfileFieldAliases:
    """Test that destination_type and dataset are accepted as aliases."""

    def test_destination_type_alias(self):
        target = ProfileTarget.from_dict(
            "dev",
            {
                "destination_type": "bigquery",
                "database": "my-project",
            },
        )
        assert target.destination_type == "bigquery"

    def test_type_takes_precedence_over_destination_type(self):
        target = ProfileTarget.from_dict(
            "dev",
            {
                "type": "bigquery",
                "destination_type": "duckdb",
                "database": "my-project",
            },
        )
        assert target.destination_type == "bigquery"

    def test_dataset_alias(self):
        target = ProfileTarget.from_dict(
            "dev",
            {
                "type": "duckdb",
                "dataset": "my_schema",
            },
        )
        assert target.schema == "my_schema"

    def test_schema_takes_precedence_over_dataset(self):
        target = ProfileTarget.from_dict(
            "dev",
            {
                "type": "duckdb",
                "schema": "canonical_name",
                "dataset": "alias_name",
            },
        )
        assert target.schema == "canonical_name"

    def test_aliases_not_in_destination_config(self):
        target = ProfileTarget.from_dict(
            "dev",
            {
                "destination_type": "bigquery",
                "dataset": "my_schema",
                "database": "my-project",
            },
        )
        assert "destination_type" not in target.destination_config
        assert "dataset" not in target.destination_config
