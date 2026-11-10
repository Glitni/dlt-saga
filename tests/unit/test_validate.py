"""Unit tests for pipeline config validation."""

from types import SimpleNamespace
from unittest.mock import patch

import pytest

from dlt_saga.pipeline_config.base_config import (
    DEFAULT_WRITE_DISPOSITION,
    PipelineConfig,
)
from dlt_saga.validate import (
    ValidationResult,
    is_collision_check_failure,
    is_collision_finding,
    is_deprecated_key_finding,
    validate_pipeline_config,
    validate_pipeline_configs,
)


def _make_config(
    pipeline_group="google_sheets",
    table_name="google_sheets__test",
    config_dict=None,
    adapter=None,
    tags=None,
):
    """Helper to create a PipelineConfig for testing."""
    base_dict = {
        "base_table_name": "test",
        "write_disposition": "append",
    }
    if config_dict:
        base_dict.update(config_dict)
    return PipelineConfig(
        pipeline_group=pipeline_group,
        pipeline_name=f"{pipeline_group}__test",
        table_name=table_name,
        identifier=f"configs/{pipeline_group}/test.yml",
        config_dict=base_dict,
        enabled=True,
        tags=tags or [],
        adapter=adapter,
    )


@pytest.mark.unit
class TestValidationResult:
    def test_valid_when_no_errors(self):
        result = ValidationResult(pipeline_name="test")
        assert result.is_valid

    def test_invalid_when_errors(self):
        result = ValidationResult(pipeline_name="test", errors=["bad"])
        assert not result.is_valid

    def test_valid_with_warnings_only(self):
        result = ValidationResult(pipeline_name="test", warnings=["warn"])
        assert result.is_valid


@pytest.mark.unit
class TestValidateWriteDisposition:
    def test_valid_append(self):
        config = _make_config(
            config_dict={
                "write_disposition": "append",
                "spreadsheet_id": "abc123",
                "sheet_name": "Sheet1",
            }
        )
        result = validate_pipeline_config(config)
        assert result.is_valid

    def test_valid_append_historize(self):
        config = _make_config(
            config_dict={
                "write_disposition": "append+historize",
                "primary_key": ["id"],
                "spreadsheet_id": "abc123",
                "sheet_name": "Sheet1",
            }
        )
        result = validate_pipeline_config(config)
        assert result.is_valid

    def test_valid_merge(self):
        config = _make_config(
            config_dict={
                "write_disposition": "merge",
                "spreadsheet_id": "abc123",
                "sheet_name": "Sheet1",
            }
        )
        result = validate_pipeline_config(config)
        assert result.is_valid

    def test_valid_merge_historize(self):
        config = _make_config(
            config_dict={
                "write_disposition": "merge+historize",
                "primary_key": ["id"],
                "spreadsheet_id": "abc123",
                "sheet_name": "Sheet1",
            }
        )
        result = validate_pipeline_config(config)
        assert result.is_valid

    def test_valid_replace_historize(self):
        config = _make_config(
            config_dict={
                "write_disposition": "replace+historize",
                "primary_key": ["id"],
                "spreadsheet_id": "abc123",
                "sheet_name": "Sheet1",
            }
        )
        result = validate_pipeline_config(config)
        assert result.is_valid

    def test_valid_historize_only(self):
        config = _make_config(
            config_dict={
                "write_disposition": "historize",
                "primary_key": ["id"],
                "source_schema": "raw",
                "source_table": "orders",
                "spreadsheet_id": "abc123",
            }
        )
        result = validate_pipeline_config(config)
        assert result.is_valid

    def test_invalid_write_disposition(self):
        config = _make_config(config_dict={"write_disposition": "upsert"})
        result = validate_pipeline_config(config)
        assert not result.is_valid
        assert any("write_disposition" in e for e in result.errors)

    def test_omitted_write_disposition_warns_but_is_valid(self):
        config = PipelineConfig(
            pipeline_group="google_sheets",
            pipeline_name="google_sheets__test",
            table_name="google_sheets__test",
            identifier="configs/google_sheets/test.yml",
            config_dict={
                "base_table_name": "test",
                "spreadsheet_id": "abc123",
                "sheet_name": "Sheet1",
            },
            enabled=True,
            tags=[],
        )
        result = validate_pipeline_config(config)
        assert result.is_valid
        assert any(
            "write_disposition is not set" in w and DEFAULT_WRITE_DISPOSITION in w
            for w in result.warnings
        )

    def test_explicit_write_disposition_does_not_warn(self):
        config = _make_config(
            config_dict={
                "write_disposition": "append",
                "spreadsheet_id": "abc123",
                "sheet_name": "Sheet1",
            }
        )
        result = validate_pipeline_config(config)
        assert not any("write_disposition is not set" in w for w in result.warnings)


@pytest.mark.unit
class TestValidatePipelineImpl:
    def test_valid_builtin_type(self):
        """google_sheets resolves via base implementation."""
        config = _make_config(pipeline_group="google_sheets")
        result = validate_pipeline_config(config)
        # Should not have adapter errors
        impl_errors = [e for e in result.errors if "implementation" in e.lower()]
        assert not impl_errors

    def test_invalid_adapter(self):
        config = _make_config(adapter="dlt_saga.nonexistent.source")
        result = validate_pipeline_config(config)
        assert not result.is_valid
        assert any("implementation" in e.lower() for e in result.errors)

    def test_explicit_valid_impl(self):
        config = _make_config(
            pipeline_group="api",
            table_name="api__test",
            adapter="dlt_saga.api",
        )
        result = validate_pipeline_config(config)
        impl_errors = [e for e in result.errors if "implementation" in e.lower()]
        assert not impl_errors

    def test_historize_only_skips_adapter_resolution(self):
        """A historize-only pipeline (write_disposition: historize) has no ingest
        step, so there is no source adapter to resolve — validation must not flag a
        missing implementation, even for a group with no built-in adapter."""
        config = _make_config(
            pipeline_group="streams",
            table_name="streams__orders",
            config_dict={
                "write_disposition": "historize",
                "primary_key": ["id"],
                "source_schema": "raw",
                "source_table": "orders",
            },
        )
        result = validate_pipeline_config(config)
        assert result.is_valid
        assert not any("implementation" in e.lower() for e in result.errors)

    def test_historize_only_still_validates_historize_config(self):
        """Skipping the adapter/source-config checks for historize-only pipelines
        must not also skip _validate_historize_config — a broken historize block
        (e.g. missing primary_key) should still surface."""
        config = _make_config(
            pipeline_group="streams",
            table_name="streams__orders",
            config_dict={
                "write_disposition": "historize",
                # primary_key intentionally omitted — historize config must flag this.
                "source_schema": "raw",
                "source_table": "orders",
            },
        )
        result = validate_pipeline_config(config)
        assert not result.is_valid
        assert any("primary_key" in e.lower() for e in result.errors)


@pytest.mark.unit
class TestValidateSourceConfig:
    def test_missing_required_field(self):
        """Missing spreadsheet_id for google_sheets should error."""
        config = _make_config(
            pipeline_group="google_sheets",
            config_dict={"spreadsheet_id": ""},
        )
        result = validate_pipeline_config(config)
        assert not result.is_valid
        assert any("spreadsheet_id" in e for e in result.errors)

    def test_valid_source_config(self):
        """Valid google_sheets config passes."""
        config = _make_config(
            pipeline_group="google_sheets",
            config_dict={"spreadsheet_id": "abc123", "sheet_name": "Sheet1"},
        )
        result = validate_pipeline_config(config)
        source_errors = [e for e in result.errors if "source config" in e.lower()]
        assert not source_errors


@pytest.mark.unit
class TestValidateHistorizeConfig:
    def test_historize_without_primary_key(self):
        config = _make_config(
            config_dict={
                "write_disposition": "append+historize",
            }
        )
        result = validate_pipeline_config(config)
        assert not result.is_valid
        assert any("primary_key" in e for e in result.errors)

    def test_historize_with_primary_key(self):
        config = _make_config(
            config_dict={
                "write_disposition": "append+historize",
                "primary_key": ["id"],
            }
        )
        result = validate_pipeline_config(config)
        historize_errors = [e for e in result.errors if "historize" in e.lower()]
        assert not historize_errors

    def test_historize_only_missing_source_schema(self):
        """historize-only with source_table but no source_schema should error."""
        config = _make_config(
            config_dict={
                "write_disposition": "historize",
                "primary_key": ["id"],
                "source_table": "orders",
            }
        )
        result = validate_pipeline_config(config)
        assert not result.is_valid
        assert any("source_schema" in e for e in result.errors)

    def test_no_historize_skips_check(self):
        """Non-historize config doesn't trigger historize validation."""
        config = _make_config(config_dict={"write_disposition": "append"})
        result = validate_pipeline_config(config)
        historize_errors = [e for e in result.errors if "historize" in e.lower()]
        assert not historize_errors


@pytest.mark.unit
class TestMultipleErrors:
    def test_collects_multiple_errors(self):
        """Validation doesn't fail-fast — collects all errors."""
        config = _make_config(
            config_dict={
                "write_disposition": "invalid_disposition",
                "spreadsheet_id": "",
            }
        )
        result = validate_pipeline_config(config)
        assert not result.is_valid
        assert len(result.errors) >= 2


def _gs_config(name, table, config_path=None):
    """A valid google_sheets config with a distinct pipeline_name and target."""
    path = config_path or f"configs/{name}.yml"
    return PipelineConfig(
        pipeline_group="google_sheets",
        pipeline_name=name,
        table_name=table,
        identifier=path,
        config_dict={
            "base_table_name": table,
            "write_disposition": "append",
            "spreadsheet_id": "abc123",
            "sheet_name": "Sheet1",
            "config_path": path,
            "_target": ("dlt_dev", table),
        },
        enabled=True,
        tags=[],
        adapter=None,
    )


def _collision_source(configs):
    """Fake ConfigSource that resolves each config to the target carried in its
    ``config_dict['_target']`` and discovers the whole set as the project."""
    targets = {c.config_dict["config_path"]: c.config_dict["_target"] for c in configs}

    class _Source:
        def resolve_ingest_target(
            self,
            config_path,
            *,
            schema_override=None,
            table_override=None,
            environment=None,
        ):
            schema, table = targets[config_path]
            return (schema_override or schema, table_override or table)

        def discover(self):
            return ({"google_sheets": list(configs)}, {})

    return _Source()


def _target_collision_errors(result):
    return [e for e in result.errors if is_collision_finding(e)]


@pytest.mark.unit
class TestValidateConfigsCollisions:
    """Project-wide target-collision detection in validate_pipeline_configs."""

    def test_no_collision_when_distinct_targets(self):
        configs = [
            _gs_config("google_sheets__a", "a"),
            _gs_config("google_sheets__b", "b"),
        ]
        results = validate_pipeline_configs(
            configs, _collision_source(configs), environments=["dev"]
        )
        assert all(not _target_collision_errors(r) for r in results)
        assert all(r.is_valid for r in results)

    def test_collision_flags_every_involved_config(self):
        # Both resolve to dlt_dev.shared → a collision naming each other.
        configs = [
            _gs_config("google_sheets__a", "shared"),
            _gs_config("google_sheets__b", "shared"),
        ]
        results = validate_pipeline_configs(
            configs, _collision_source(configs), environments=["dev"]
        )
        by_name = {r.pipeline_name: r for r in results}
        errs_a = _target_collision_errors(by_name["google_sheets__a"])
        errs_b = _target_collision_errors(by_name["google_sheets__b"])
        # Each participant carries the same canonical message naming the whole
        # group, so both is_valid flags trip and the CLI can dedupe to one line.
        assert errs_a == errs_b
        assert "google_sheets__a" in errs_a[0] and "google_sheets__b" in errs_a[0]
        assert not by_name["google_sheets__a"].is_valid
        assert not by_name["google_sheets__b"].is_valid

    def test_collision_findings_dedupes_to_one_per_target(self):
        from dlt_saga.validate import collision_findings

        configs = [
            _gs_config("google_sheets__a", "shared"),
            _gs_config("google_sheets__b", "shared"),
        ]
        results = validate_pipeline_configs(
            configs, _collision_source(configs), environments=["dev"]
        )
        # Two pipelines, one shared target → exactly one deduped finding.
        assert len(collision_findings(results)) == 1

    def test_empty_selection_returns_empty(self):
        assert validate_pipeline_configs([], _collision_source([])) == []

    def test_clean_selection_is_all_pipeline_kind(self):
        configs = [_gs_config("google_sheets__a", "a")]
        results = validate_pipeline_configs(
            configs, _collision_source(configs), environments=["dev"]
        )
        assert all(r.kind == "pipeline" for r in results)

    def test_collision_check_failure_yields_check_result(self):
        configs = [_gs_config("google_sheets__a", "a")]
        with patch(
            "dlt_saga.utility.collisions.collisions_for_selection",
            side_effect=RuntimeError("boom"),
        ):
            results = validate_pipeline_configs(
                configs, _collision_source(configs), environments=["dev"]
            )
        check = [r for r in results if r.kind == "check"]
        assert len(check) == 1
        assert any(is_collision_check_failure(w) for w in check[0].warnings)
        # A check that couldn't run is a warning, not an error — the pipeline
        # config itself stays valid.
        assert results[0].is_valid


@pytest.mark.unit
class TestValidateConfigsLegacyKeys:
    """Deprecated-key scan — warnings, never errors."""

    def _source(self, project_path=None):
        return SimpleNamespace(
            project_config_path=project_path,
            resolve_ingest_target=lambda *a, **k: ("dlt_dev", "t"),
            discover=lambda: ({"google_sheets": []}, {}),
        )

    def test_legacy_key_in_config_file_warns_but_stays_valid(self, tmp_path):
        cfg = tmp_path / "a.yml"
        cfg.write_text("historize:\n  output_table: orders\n")
        config = _gs_config("google_sheets__a", "a", config_path=str(cfg))
        with patch(
            "dlt_saga.utility.cli.profiles.get_profiles_config",
            side_effect=RuntimeError,
        ):
            results = validate_pipeline_configs(
                [config], self._source(), environments=["dev"]
            )
        result = next(r for r in results if r.pipeline_name == "google_sheets__a")
        assert result.is_valid  # advisory only
        assert any("output_table" in w and "table_name" in w for w in result.warnings)

    def test_legacy_key_in_project_file_becomes_synthetic_result(self, tmp_path):
        cfg = tmp_path / "a.yml"
        cfg.write_text("write_disposition: append\n")
        proj = tmp_path / "saga_project.yml"
        proj.write_text("pipelines:\n  dataset_access:\n    - OWNER:x\n")
        config = _gs_config("google_sheets__a", "a", config_path=str(cfg))
        with patch(
            "dlt_saga.utility.cli.profiles.get_profiles_config",
            side_effect=RuntimeError,
        ):
            results = validate_pipeline_configs(
                [config], self._source(project_path=str(proj)), environments=["dev"]
            )
        synthetic = [r for r in results if r.pipeline_name == str(proj)]
        assert len(synthetic) == 1
        assert synthetic[0].kind == "project"
        assert synthetic[0].is_valid
        assert all(is_deprecated_key_finding(w) for w in synthetic[0].warnings)
        assert any(
            "dataset_access" in w and "schema_access" in w
            for w in synthetic[0].warnings
        )


@pytest.mark.unit
class TestValidateConfigsTaskConcurrency:
    """task_concurrency agreement within a task_group."""

    def _grouped(self, name, concurrency):
        config = _gs_config(name, name.split("__")[-1])
        config.config_dict["task_group"] = "limited_api"
        if concurrency is not None:
            config.config_dict["task_concurrency"] = concurrency
        return config

    def _errors(self, results, name):
        result = next(r for r in results if r.pipeline_name == name)
        return [e for e in result.errors if "task_concurrency" in e]

    def test_agreeing_group_is_valid(self):
        configs = [
            self._grouped("google_sheets__a", 1),
            self._grouped("google_sheets__b", 1),
        ]
        results = validate_pipeline_configs(
            configs, _collision_source(configs), environments=["dev"]
        )
        assert all(r.is_valid for r in results)

    def test_disagreement_errors_every_selected_member(self):
        configs = [
            self._grouped("google_sheets__a", 1),
            self._grouped("google_sheets__b", None),
        ]
        results = validate_pipeline_configs(
            configs, _collision_source(configs), environments=["dev"]
        )
        assert self._errors(results, "google_sheets__a")
        assert self._errors(results, "google_sheets__b")

    def test_disagreement_with_unselected_member_is_flagged(self):
        selected = self._grouped("google_sheets__a", 1)
        project = [selected, self._grouped("google_sheets__b", 2)]
        results = validate_pipeline_configs(
            [selected], _collision_source(project), environments=["dev"]
        )
        assert self._errors(results, "google_sheets__a")


@pytest.mark.unit
class TestValidateConfigsJobLabels:
    """The active BigQuery target's job_labels are checked offline."""

    def _run(self, destination_config, destination_type="bigquery"):
        from dlt_saga.utility.cli.context import execution_context_scope
        from dlt_saga.utility.cli.profiles import ProfileTarget

        target = ProfileTarget(
            name="prod",
            environment="prod",
            destination_type=destination_type,
            destination_config=destination_config,
        )
        configs = [_gs_config("google_sheets__a", "a")]
        with execution_context_scope(target):
            return validate_pipeline_configs(
                configs, _collision_source(configs), environments=["dev"]
            )

    def test_reserved_key_is_a_project_error(self):
        results = self._run({"database": "p", "job_labels": {"saga-task": "x"}})
        project = [r for r in results if r.kind == "project"]
        assert len(project) == 1
        assert project[0].pipeline_name == "profile target 'prod'"
        assert not project[0].is_valid
        assert "reserved" in project[0].errors[0]
        assert results[0].is_valid  # the pipeline config itself is fine

    def test_valid_labels_add_nothing(self):
        results = self._run({"database": "p", "job_labels": {"team": "t"}})
        assert all(r.kind == "pipeline" for r in results)

    def test_destination_without_job_tagging_warns(self):
        results = self._run(
            {"database_path": "x.duckdb", "job_labels": {"team": "t"}},
            destination_type="duckdb",
        )
        project = [r for r in results if r.kind == "project"]
        assert len(project) == 1
        assert project[0].is_valid
        assert "no effect on a duckdb target" in project[0].warnings[0]

    def test_no_declaration_adds_nothing(self):
        results = self._run({"database": "p"})
        assert all(r.kind == "pipeline" for r in results)


@pytest.mark.unit
class TestValidateNotifications:
    """A pipeline's own `notifications.slack` block is a free dict on the
    config class, so this is where a misspelling or a misplaced `+channel`
    surfaces; the notifier ignores what it does not know."""

    def _check(self, slack, project_token=True, project=True):
        from types import SimpleNamespace
        from unittest.mock import patch

        from dlt_saga.project_config import SlackNotificationConfig
        from dlt_saga.validate import _validate_notifications

        if project:
            slack_config = (
                SlackNotificationConfig(token="env_secret::T", channel="#ops")
                if project_token
                else SlackNotificationConfig(webhook_url="env_secret::HOOK")
            )
            project_config = SimpleNamespace(
                notifications=SimpleNamespace(slack=slack_config)
            )
        else:
            project_config = SimpleNamespace(notifications=None)
        config = _make_config(config_dict={"notifications": {"slack": slack}})
        result = ValidationResult(pipeline_name=config.pipeline_name)
        with patch(
            "dlt_saga.project_config.get_project_config", return_value=project_config
        ):
            _validate_notifications(config, result)
        return result

    def test_valid_block_is_clean(self):
        result = self._check(
            {
                "enabled": True,
                "mentions": ["<@U1>"],
                "channel": ["#shop"],
                "repeat_after_hours": 6,
            }
        )
        assert result.is_valid and not result.warnings

    @pytest.mark.parametrize("key", ["+channel", "token", "enabeld"])
    def test_unknown_key_warns(self, key):
        result = self._check({key: "x"})
        assert result.is_valid
        assert key in result.warnings[0]
        assert "top-level keys only" in result.warnings[0]

    def test_channel_under_a_webhook_project_is_an_error(self):
        result = self._check({"channel": "#shop"}, project_token=False)
        assert not result.is_valid
        assert "bot-token transport" in result.errors[0]

    def test_channel_under_a_token_project_is_fine(self):
        assert self._check({"channel": "#shop"}).is_valid

    def test_channel_without_project_notifications_is_fine(self):
        assert self._check({"channel": "#shop"}, project=False).is_valid

    @pytest.mark.parametrize("channel", [42, [""], ["#a", 1], {"a": 1}])
    def test_bad_channel_value_is_an_error(self, channel):
        result = self._check({"channel": channel})
        assert "notifications.slack.channel must be" in result.errors[0]

    def test_non_bool_enabled_is_an_error(self):
        result = self._check({"enabled": "false"})
        assert "enabled must be true or false" in result.errors[0]

    @pytest.mark.parametrize("value", ["24h", 0, True])
    def test_bad_repeat_after_hours_is_an_error(self, value):
        result = self._check({"repeat_after_hours": value})
        assert "repeat_after_hours must be a positive" in result.errors[0]

    def test_bad_mentions_is_an_error(self):
        result = self._check({"mentions": {"a": 1}})
        assert "mentions" in result.errors[0]

    def test_non_mapping_blocks_are_errors(self):
        config = _make_config(config_dict={"notifications": "nope"})
        result = ValidationResult(pipeline_name=config.pipeline_name)
        from dlt_saga.validate import _validate_notifications

        _validate_notifications(config, result)
        assert "notifications must be a mapping" in result.errors[0]
