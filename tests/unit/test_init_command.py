"""Unit tests for dlt_saga.init_command."""

import pytest

from dlt_saga.init_command import (
    _VALID_DESTINATION_TYPES,
    _check_initialized,
    _dlt_config_toml,
    _packages_yml,
    _profiles_yml_bigquery,
    _profiles_yml_databricks,
    _profiles_yml_duckdb,
    _readme_md,
    _saga_project_yml,
    _sample_csv_data,
    _sample_pipeline_config_runnable,
    _sample_pipeline_config_template,
    _write_if_absent,
    run_init,
)

# ---------------------------------------------------------------------------
# _check_initialized
# ---------------------------------------------------------------------------


class TestCheckInitialized:
    def test_empty_directory_all_missing(self, tmp_path):
        present, missing = _check_initialized(tmp_path)
        assert present == set()
        assert missing == {"profiles.yml", "saga_project.yml", "configs"}

    def test_fully_initialized(self, tmp_path):
        (tmp_path / "profiles.yml").write_text("")
        (tmp_path / "saga_project.yml").write_text("")
        (tmp_path / "configs").mkdir()
        present, missing = _check_initialized(tmp_path)
        assert present == {"profiles.yml", "saga_project.yml", "configs"}
        assert missing == set()

    def test_partially_initialized(self, tmp_path):
        (tmp_path / "profiles.yml").write_text("")
        present, missing = _check_initialized(tmp_path)
        assert "profiles.yml" in present
        assert "saga_project.yml" in missing
        assert "configs" in missing


# ---------------------------------------------------------------------------
# Template helpers
# ---------------------------------------------------------------------------


class TestTemplates:
    def test_profiles_bigquery_contains_project(self):
        result = _profiles_yml_bigquery("my-project", "EU", "dlt_dev")
        assert "my-project" in result
        assert "EU" in result
        assert "dlt_dev" in result
        assert "type: bigquery" in result

    def test_profiles_bigquery_env_var_syntax(self):
        result = _profiles_yml_bigquery("proj", "EU", "myschema")
        # Must produce literal {{ }} for YAML env_var interpolation
        assert "{{ env_var('SAGA_SCHEMA_NAME', 'myschema') }}" in result

    def test_profiles_duckdb_contains_schema(self):
        result = _profiles_yml_duckdb("my_schema")
        assert "type: duckdb" in result
        assert "my_schema" in result
        assert "local.duckdb" in result

    def test_saga_project_yml_with_gcp_project(self):
        result = _saga_project_yml("my-project")
        assert "config_source" in result
        assert "my-project" in result

    def test_saga_project_yml_without_gcp_project(self):
        result = _saga_project_yml(None)
        assert "config_source" in result
        assert "<your-gcp-project>" in result

    def test_dlt_config_toml_is_comments_only(self):
        result = _dlt_config_toml()
        non_comment_lines = [
            line
            for line in result.splitlines()
            if line.strip() and not line.startswith("#")
        ]
        assert non_comment_lines == []

    def test_sample_pipeline_config_template_is_commented(self):
        result = _sample_pipeline_config_template()
        non_comment_lines = [
            line
            for line in result.splitlines()
            if line.strip() and not line.startswith("#")
        ]
        assert non_comment_lines == []

    def test_sample_pipeline_config_runnable_has_content(self):
        result = _sample_pipeline_config_runnable()
        non_comment_lines = [
            line
            for line in result.splitlines()
            if line.strip() and not line.startswith("#")
        ]
        assert len(non_comment_lines) > 0
        assert "write_disposition" in result
        assert "file_type" in result

    def test_sample_csv_data_has_header_and_rows(self):
        result = _sample_csv_data()
        lines = [line for line in result.strip().splitlines() if line.strip()]
        assert len(lines) >= 2  # header + at least one data row
        assert "id" in lines[0]  # has header

    def test_readme_bigquery_includes_auth_step(self):
        result = _readme_md("bigquery")
        assert "gcloud auth" in result

    def test_readme_duckdb_excludes_auth_step(self):
        result = _readme_md("duckdb")
        assert "gcloud auth" not in result

    def test_readme_contains_quick_start(self):
        result = _readme_md("duckdb")
        assert "saga list" in result
        assert "saga ingest" in result

    def test_readme_databricks_has_browser_auth(self):
        result = _readme_md("databricks")
        assert "gcloud auth" not in result
        assert (
            "browser" in result.lower()
            or "u2m" in result.lower()
            or "saga ingest" in result
        )

    def test_profiles_databricks_pat_contains_access_token(self):
        result = _profiles_yml_databricks(
            "adb-1234.azuredatabricks.net",
            "/sql/1.0/warehouses/abc",
            "my_catalog",
            "pat",
            "dlt_dev",
        )
        assert "auth_mode: pat" in result
        assert "access_token" in result
        assert "adb-1234.azuredatabricks.net" in result
        assert "my_catalog" in result

    def test_profiles_databricks_m2m_contains_client_id(self):
        result = _profiles_yml_databricks(
            "adb-1234.azuredatabricks.net",
            "/sql/1.0/warehouses/abc",
            "my_catalog",
            "m2m",
            "dlt_dev",
        )
        assert "auth_mode: m2m" in result
        assert "client_id" in result
        assert "client_secret" in result

    def test_profiles_databricks_u2m_no_credentials(self):
        result = _profiles_yml_databricks(
            "adb-1234.azuredatabricks.net",
            "/sql/1.0/warehouses/abc",
            "my_catalog",
            "u2m",
            "dlt_dev",
        )
        assert "auth_mode: u2m" in result
        assert "access_token" not in result
        # The prod block always uses m2m — check dev block has no client_secret
        dev_block = result.split("prod:")[0]
        assert "client_secret" not in dev_block

    def test_profiles_databricks_has_dev_target(self):
        result = _profiles_yml_databricks("host", "/path", "cat", "pat", "schema")
        assert "dev:" in result
        assert "type: databricks" in result
        # Template intentionally omits prod block — users add it when ready
        assert "Add a prod target" in result

    def test_valid_destination_types_includes_databricks(self):
        assert "databricks" in _VALID_DESTINATION_TYPES

    def test_packages_yml_has_empty_packages_list(self):
        result = _packages_yml()
        # Must contain a valid packages key (empty list or commented examples)
        assert "packages:" in result
        # Non-comment, non-packages lines should only be the empty list declaration
        non_comment_lines = [
            line.strip()
            for line in result.splitlines()
            if line.strip() and not line.strip().startswith("#")
        ]
        assert non_comment_lines == ["packages: []"]


# ---------------------------------------------------------------------------
# _write_if_absent
# ---------------------------------------------------------------------------


class TestWriteIfAbsent:
    def test_creates_new_file(self, tmp_path):
        created, skipped = [], []
        _write_if_absent(tmp_path / "new.txt", "hello", created, skipped)
        assert (tmp_path / "new.txt").read_text() == "hello"
        assert created == [str(tmp_path / "new.txt")]
        assert skipped == []

    def test_skips_existing_file(self, tmp_path):
        existing = tmp_path / "existing.txt"
        existing.write_text("original")
        created, skipped = [], []
        _write_if_absent(existing, "overwrite", created, skipped)
        assert existing.read_text() == "original"  # not overwritten
        assert skipped == [str(existing)]
        assert created == []

    def test_creates_parent_directories(self, tmp_path):
        created, skipped = [], []
        nested = tmp_path / "a" / "b" / "c.txt"
        _write_if_absent(nested, "content", created, skipped)
        assert nested.exists()


# ---------------------------------------------------------------------------
# run_init — fresh init (no_input mode)
# ---------------------------------------------------------------------------


class TestRunInitFresh:
    def test_creates_all_files(self, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        run_init(no_input=True)
        assert (tmp_path / "profiles.yml").exists()
        assert (tmp_path / "saga_project.yml").exists()
        assert (tmp_path / "configs").is_dir()
        assert (tmp_path / ".dlt" / "config.toml").exists()
        assert (tmp_path / "packages.yml").exists()
        # DuckDB (--no-input default) creates a runnable pipeline + sample data
        assert (tmp_path / "configs" / "filesystem" / "sample.yml").exists()
        assert (tmp_path / "data" / "sample.csv").exists()
        assert (tmp_path / "README.md").exists()

    def test_uses_duckdb(self, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        run_init(no_input=True)
        assert "type: duckdb" in (tmp_path / "profiles.yml").read_text()

    def test_uses_dlt_dev_schema(self, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        run_init(no_input=True)
        assert "dlt_dev" in (tmp_path / "profiles.yml").read_text()

    def test_readme_excludes_gcloud_for_duckdb(self, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        run_init(no_input=True)
        assert "gcloud" not in (tmp_path / "README.md").read_text()


# ---------------------------------------------------------------------------
# run_init — partial init
# ---------------------------------------------------------------------------


class TestRunInitPartial:
    def test_creates_only_missing_core_files(self, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        (tmp_path / "profiles.yml").write_text("existing")
        run_init(no_input=True)
        # Existing core file must not be overwritten
        assert (tmp_path / "profiles.yml").read_text() == "existing"
        # Missing core files must be created
        assert (tmp_path / "saga_project.yml").exists()
        assert (tmp_path / "configs").is_dir()

    def test_does_not_create_starter_files_on_partial_init(self, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        (tmp_path / "profiles.yml").write_text("existing")
        run_init(no_input=True)
        # Core files must be created
        assert (tmp_path / "packages.yml").exists()
        # Starter files must NOT be created when core markers were already present
        assert not (tmp_path / ".dlt" / "config.toml").exists()
        assert not (tmp_path / "configs" / "filesystem" / "sample.yml").exists()
        assert not (tmp_path / "data" / "sample.csv").exists()
        assert not (tmp_path / "README.md").exists()


# ---------------------------------------------------------------------------
# run_init — already initialised
# ---------------------------------------------------------------------------


class TestRunInitAlreadyInitialized:
    def test_exits_cleanly(self, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        (tmp_path / "profiles.yml").write_text("")
        (tmp_path / "saga_project.yml").write_text("")
        (tmp_path / "configs").mkdir()

        import typer

        with pytest.raises((typer.Exit, SystemExit)):
            run_init(no_input=True)

    def test_idempotent_second_run_exits(self, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        run_init(no_input=True)

        import typer

        with pytest.raises((typer.Exit, SystemExit)):
            run_init(no_input=True)
