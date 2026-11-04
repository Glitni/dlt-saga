"""Unit tests for the job context (utility/job_labels.py) and its BigQuery
labelling (utility/gcp/job_labels.py)."""

import json
from unittest.mock import MagicMock

import pytest
from google.cloud import bigquery

from dlt_saga.utility import job_labels as neutral
from dlt_saga.utility.cli.context import execution_context_scope
from dlt_saga.utility.cli.logging import bind_log_context, log_context
from dlt_saga.utility.cli.profiles import ProfileTarget
from dlt_saga.utility.gcp import job_labels as jl
from dlt_saga.utility.gcp.job_labels import (
    MAX_USER_LABELS,
    apply_default_job_labels,
    attach_job_labels_to_dlt_destination,
    clean_label_value,
    current_job_labels,
    label_bigquery_jobs,
    validate_job_labels,
    with_job_labels,
)
from dlt_saga.utility.job_labels import (
    ENV_JOB_LABELS,
    job_context,
    scalar_labels,
    user_job_labels,
)


@pytest.fixture(autouse=True)
def _reset_state(monkeypatch):
    """Process-wide log context and the once-per-declaration caches must not
    leak between tests."""
    import dlt_saga.utility.cli.logging as cli_logging

    monkeypatch.delenv(ENV_JOB_LABELS, raising=False)
    monkeypatch.delenv("SAGA_ENVIRONMENT", raising=False)
    cli_logging._process_context.clear()
    neutral._scalar_labels.cache_clear()
    neutral._warn_once.cache_clear()
    neutral._forwarded_labels.cache_clear()
    jl._lenient_labels.cache_clear()
    yield
    cli_logging._process_context.clear()


def _target(name="prod", **destination_config):
    return ProfileTarget(
        name=name,
        environment="prod",
        destination_type="bigquery",
        destination_config={"database": "proj", **destination_config},
    )


# ---------------------------------------------------------------------------
# Destination-neutral context
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestJobContext:
    def test_app_and_environment_always_present(self):
        assert job_context() == {"app": "dlt-saga", "environment": "dev"}

    def test_fields_from_log_context(self):
        bind_log_context(execution_id="EXEC-1", task_index=3)
        with log_context(pipeline="google_sheets__sales", command="historize"):
            context = job_context()
        assert context == {
            "app": "dlt-saga",
            "environment": "dev",
            "command": "historize",
            "pipeline": "google_sheets__sales",
            "execution_id": "EXEC-1",
            "task_index": "3",
        }

    def test_process_command_yields_to_the_pipeline_command(self):
        bind_log_context(command="run")
        assert job_context()["command"] == "run"
        with log_context(command="ingest"):
            assert job_context()["command"] == "ingest"
        assert job_context()["command"] == "run"

    def test_environment_from_profile_target(self):
        with execution_context_scope(_target()):
            assert job_context()["environment"] == "prod"


@pytest.mark.unit
class TestUserJobLabels:
    def test_profile_labels_as_declared(self):
        target = _target(job_labels={"team": "Data", "airflow-run": "X:Y", "n": 7})
        with execution_context_scope(target):
            labels = user_job_labels()
        assert labels == {"team": "Data", "airflow-run": "X:Y", "n": "7"}

    def test_no_target_no_labels(self):
        assert user_job_labels() == {}

    def test_forwarded_env_labels_merge_over_profile(self, monkeypatch):
        monkeypatch.setenv(
            ENV_JOB_LABELS, json.dumps({"airflow-run": "forwarded", "extra": "e"})
        )
        target = _target(job_labels={"airflow-run": "own", "team": "t"})
        with execution_context_scope(target):
            labels = user_job_labels()
        assert labels == {"airflow-run": "forwarded", "team": "t", "extra": "e"}

    def test_invalid_forwarded_env_is_ignored(self, monkeypatch, caplog):
        monkeypatch.setenv(ENV_JOB_LABELS, "{not json")
        assert user_job_labels() == {}
        assert "not valid JSON" in caplog.text

    def test_forwarded_non_object_is_ignored(self, monkeypatch, caplog):
        monkeypatch.setenv(ENV_JOB_LABELS, "[1, 2]")
        assert user_job_labels() == {}
        assert "expected a mapping" in caplog.text

    def test_non_scalar_entry_dropped_with_warning_once(self, caplog):
        target = _target(job_labels={"nested": {"a": "b"}, "team": "t"})
        with execution_context_scope(target):
            first = user_job_labels()
            second = user_job_labels()
        assert first == second == {"team": "t"}
        assert caplog.text.count("not a scalar") == 1


@pytest.mark.unit
class TestScalarLabels:
    def test_non_mapping_warns_and_yields_nothing(self, caplog):
        assert scalar_labels("a=b", "here") == {}
        assert "expected a mapping" in caplog.text

    def test_values_stringified(self):
        assert scalar_labels({"flag": True, "n": 1}, "here") == {
            "flag": "True",
            "n": "1",
        }


# ---------------------------------------------------------------------------
# BigQuery formatting
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestCleanLabelValue:
    def test_lowercases_and_replaces_disallowed_characters(self):
        assert (
            clean_label_value("scheduled__2026-11-04T10:00:00+00:00")
            == "scheduled__2026-11-04t10-00-00-00-00"
        )

    def test_truncates_to_63(self):
        assert len(clean_label_value("x" * 100)) == 63

    def test_keeps_valid_values(self):
        assert clean_label_value("google_sheets__sales") == "google_sheets__sales"


@pytest.mark.unit
class TestValidateJobLabels:
    def test_none_is_empty(self):
        assert validate_job_labels(None, "here") == {}

    def test_cleans_values_and_drops_empty(self):
        labels = validate_job_labels(
            {"team": "Data Platform", "unset": "", "run": "A:B"}, "here"
        )
        assert labels == {"team": "data-platform", "run": "a-b"}

    def test_non_mapping_rejected(self):
        with pytest.raises(ValueError, match="must be a mapping"):
            validate_job_labels(["a=b"], "here")

    @pytest.mark.parametrize("key", ["Team", "1st", "with space", "", "x" * 64])
    def test_invalid_key_rejected(self, key):
        with pytest.raises(ValueError, match="is invalid"):
            validate_job_labels({key: "v"}, "here")

    @pytest.mark.parametrize("key", ["app", "saga-pipeline", "saga-anything"])
    def test_reserved_key_rejected(self, key):
        with pytest.raises(ValueError, match="reserved"):
            validate_job_labels({key: "v"}, "here")

    def test_nested_value_rejected(self):
        with pytest.raises(ValueError, match="scalar"):
            validate_job_labels({"team": {"a": "b"}}, "here")

    def test_scalar_non_string_values_accepted(self):
        assert validate_job_labels({"n": 7, "flag": True}, "here") == {
            "n": "7",
            "flag": "true",
        }

    def test_too_many_rejected(self):
        many = {f"k{i}": "v" for i in range(MAX_USER_LABELS + 1)}
        with pytest.raises(ValueError, match="at most"):
            validate_job_labels(many, "here")

    def test_error_names_the_location(self):
        with pytest.raises(ValueError, match="^profile target 'prod': job_labels"):
            validate_job_labels({"app": "x"}, "profile target 'prod': job_labels")


@pytest.mark.unit
class TestCurrentJobLabels:
    def test_app_label_always_present(self):
        labels = current_job_labels()
        assert labels["app"] == "dlt-saga"
        assert labels["saga-environment"] == "dev"

    def test_context_fields_prefixed_and_cleaned(self):
        bind_log_context(execution_id="EXEC-1", task_index=3)
        with log_context(pipeline="google_sheets__sales", command="historize"):
            labels = current_job_labels()
        assert labels["saga-pipeline"] == "google_sheets__sales"
        assert labels["saga-command"] == "historize"
        assert labels["saga-execution-id"] == "exec-1"
        assert labels["saga-task"] == "3"

    def test_cli_command_fills_in_outside_a_pipeline(self):
        bind_log_context(command="notify")
        assert current_job_labels()["saga-command"] == "notify"
        with log_context(command="ingest"):
            assert current_job_labels()["saga-command"] == "ingest"

    def test_absent_fields_are_left_out(self):
        labels = current_job_labels()
        assert "saga-pipeline" not in labels
        assert "saga-command" not in labels
        assert "saga-execution-id" not in labels

    def test_profile_job_labels_cleaned(self):
        target = _target(job_labels={"team": "Data", "airflow-run": "X:Y"})
        with execution_context_scope(target):
            labels = current_job_labels()
        assert labels["team"] == "data"
        assert labels["airflow-run"] == "x-y"

    def test_built_in_labels_win_over_declared(self):
        # Reserved keys are refused at validation; the lenient resolver still
        # never lets a declaration shadow saga's own.
        target = _target(job_labels={"app": "other", "team": "t"})
        with execution_context_scope(target):
            labels = current_job_labels()
        assert labels["app"] == "dlt-saga"
        assert labels["team"] == "t"

    def test_invalid_declared_entry_is_skipped_not_raised(self, caplog):
        target = _target(job_labels={"Bad Key": "v", "team": "t"})
        with execution_context_scope(target):
            labels = current_job_labels()
        assert labels["team"] == "t"
        assert "Bad Key" not in labels
        assert "Skipping BigQuery job label" in caplog.text

    def test_result_is_a_fresh_dict_each_call(self):
        target = _target(job_labels={"team": "t"})
        with execution_context_scope(target):
            first = current_job_labels()
            first["mutated"] = "x"
            assert "mutated" not in current_job_labels()


@pytest.mark.unit
class TestWithJobLabels:
    def test_none_config_gets_new_config(self):
        cfg = with_job_labels(None, bigquery.QueryJobConfig, {"app": "dlt-saga"})
        assert isinstance(cfg, bigquery.QueryJobConfig)
        assert cfg.labels == {"app": "dlt-saga"}

    def test_explicit_labels_win_and_original_untouched(self):
        original = bigquery.QueryJobConfig(labels={"app": "mine"})
        original.default_dataset = "proj.ds"
        cfg = with_job_labels(
            original, bigquery.QueryJobConfig, {"app": "dlt-saga", "team": "t"}
        )
        assert cfg.labels == {"app": "mine", "team": "t"}
        assert cfg.default_dataset.dataset_id == "ds"
        assert original.labels == {"app": "mine"}

    def test_no_labels_returns_config_as_is(self):
        original = bigquery.LoadJobConfig()
        assert with_job_labels(original, bigquery.LoadJobConfig, {}) is original


@pytest.mark.unit
class TestLabelBigQueryJobs:
    def test_query_without_config_gets_labelled_config(self):
        client = MagicMock()
        original = client.query
        label_bigquery_jobs(client)
        with log_context(pipeline="p1", command="ingest"):
            client.query("SELECT 1")
        _, job_config = original.call_args.args[:2]
        assert job_config.labels["saga-pipeline"] == "p1"
        assert job_config.labels["app"] == "dlt-saga"

    def test_labels_follow_the_calling_thread_context(self):
        client = MagicMock()
        original = client.query
        label_bigquery_jobs(client)
        with log_context(pipeline="first", command="ingest"):
            client.query("SELECT 1", job_config=bigquery.QueryJobConfig())
        with log_context(pipeline="second", command="historize"):
            client.query("SELECT 2", job_config=bigquery.QueryJobConfig())
        first, second = (c.args[1] for c in original.call_args_list)
        assert first.labels["saga-pipeline"] == "first"
        assert second.labels["saga-pipeline"] == "second"
        assert second.labels["saga-command"] == "historize"

    def test_other_arguments_pass_through(self):
        client = MagicMock()
        original = client.query
        label_bigquery_jobs(client)
        cfg = bigquery.QueryJobConfig(query_parameters=[])
        client.query("SELECT 1", job_config=cfg, location="EU")
        assert original.call_args.kwargs["location"] == "EU"
        assert original.call_args.args[1] is not cfg

    def test_pooled_client_is_labelled(self, monkeypatch):
        from dlt_saga.utility.gcp import client_pool

        fake_client = MagicMock()
        original_query = fake_client.query
        monkeypatch.setattr(bigquery, "Client", lambda **kwargs: fake_client)
        monkeypatch.setattr(client_pool, "_apply_request_timeout", lambda c, t: None)
        pool = client_pool.BigQueryClientPool()
        pool.clear_cache()
        client = pool.get_client("proj", "EU")
        with log_context(pipeline="p", command="ingest"):
            client.query("SELECT 1")
        assert original_query.call_args.args[1].labels["saga-pipeline"] == "p"


@pytest.mark.unit
class TestDefaultJobLabels:
    def _client(self, default_query=None):
        client = bigquery.Client.__new__(bigquery.Client)
        client._default_query_job_config = default_query
        client._default_load_job_config = None
        return client

    def test_sets_query_and_load_defaults(self):
        client = self._client()
        apply_default_job_labels(client, {"app": "dlt-saga", "saga-pipeline": "p"})
        expected = {"app": "dlt-saga", "saga-pipeline": "p"}
        assert client.default_query_job_config.labels == expected
        assert client.default_load_job_config.labels == expected

    def test_existing_default_config_kept(self):
        client = self._client(bigquery.QueryJobConfig(use_legacy_sql=False))
        apply_default_job_labels(client, {"app": "dlt-saga"})
        assert client.default_query_job_config.use_legacy_sql is False
        assert client.default_query_job_config.labels == {"app": "dlt-saga"}

    def test_library_merges_defaults_into_explicit_configs(self):
        # The guarantee the dlt path relies on: a job config built without
        # labels (as dlt's are) picks the client's default labels up.
        client = self._client()
        apply_default_job_labels(client, {"app": "dlt-saga"})
        load = bigquery.LoadJobConfig(autodetect=False)._fill_from_default(
            client.default_load_job_config
        )
        query = bigquery.QueryJobConfig(default_dataset="p.d")._fill_from_default(
            client.default_query_job_config
        )
        assert load.labels == {"app": "dlt-saga"}
        assert load.autodetect is False
        assert query.labels == {"app": "dlt-saga"}

    def test_no_labels_leaves_client_alone(self):
        client = MagicMock()
        apply_default_job_labels(client, {})
        assert not client.method_calls


@pytest.mark.unit
class TestAttachToDltDestination:
    def _destination(self):
        native = bigquery.Client.__new__(bigquery.Client)
        native._default_query_job_config = None
        native._default_load_job_config = None
        sql_client = MagicMock()
        sql_client.open_connection.return_value = native
        job_client = MagicMock(sql_client=sql_client)
        destination = MagicMock()
        destination.client.return_value = job_client
        return destination, job_client, native

    def test_labels_applied_when_dlt_opens_its_client(self):
        destination, job_client, native = self._destination()
        attach_job_labels_to_dlt_destination(destination, {"saga-pipeline": "p"})
        assert destination.client("schema") is job_client
        opened = job_client.sql_client.open_connection()
        assert opened is native
        assert native.default_query_job_config.labels == {"saga-pipeline": "p"}
        assert native.default_load_job_config.labels == {"saga-pipeline": "p"}

    def test_labels_fixed_at_attach_time(self):
        destination, job_client, native = self._destination()
        with log_context(pipeline="p"):
            attach_job_labels_to_dlt_destination(destination, current_job_labels())
        destination.client("schema")
        job_client.sql_client.open_connection()  # no log context here
        assert native.default_load_job_config.labels["saga-pipeline"] == "p"

    def test_no_labels_leaves_destination_alone(self):
        destination = MagicMock()
        original = destination.client
        attach_job_labels_to_dlt_destination(destination, {})
        assert destination.client is original
