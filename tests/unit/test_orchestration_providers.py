"""Tests for orchestration providers and CLI helpers."""

import json
from unittest.mock import MagicMock, patch

import pytest

from dlt_saga.pipeline_config.base_config import PipelineConfig
from dlt_saga.project_config import OrchestrationConfig
from dlt_saga.utility.orchestration.providers import (
    CloudRunProvider,
    OrchestrationProvider,
    StdoutProvider,
    TriggerResult,
    resolve_provider,
)

# ---------------------------------------------------------------------------
# TriggerResult
# ---------------------------------------------------------------------------


class TestTriggerResult:
    def test_fields(self):
        r = TriggerResult(execution_reference="test-ref")
        assert r.execution_reference == "test-ref"


# ---------------------------------------------------------------------------
# StdoutProvider
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestStdoutProvider:
    def test_trigger_outputs_json(self, capsys):
        provider = StdoutProvider()
        result = provider.trigger(
            execution_id="abc-123",
            task_count=5,
            command="run",
            debug=True,
            worker_concurrency=2,
        )

        captured = capsys.readouterr()
        output = json.loads(captured.out)

        assert output["execution_id"] == "abc-123"
        assert output["task_count"] == 5
        assert output["command"] == "run"
        assert output["worker_concurrency"] == 2
        assert result.execution_reference == "stdout:abc-123"

    def test_trigger_defaults(self, capsys):
        provider = StdoutProvider()
        result = provider.trigger(execution_id="x", task_count=1)

        captured = capsys.readouterr()
        output = json.loads(captured.out)

        assert output["command"] == "ingest"
        assert output["worker_concurrency"] is None
        assert result.execution_reference == "stdout:x"


# ---------------------------------------------------------------------------
# CloudRunProvider
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestCloudRunProvider:
    @patch.dict("os.environ", {}, clear=True)
    def test_init_from_explicit_args(self):
        provider = CloudRunProvider(
            project_id="my-project",
            region="us-central1",
            job_name="my-job",
        )
        assert provider.project_id == "my-project"
        assert provider.region == "us-central1"
        assert provider.job_name == "my-job"

    @patch.dict("os.environ", {}, clear=True)
    @patch("dlt_saga.utility.orchestration.providers.get_env", return_value=None)
    def test_init_missing_project_raises(self, mock_get_env):
        with pytest.raises(ValueError, match="project_id must be provided"):
            CloudRunProvider()

    @patch.dict("os.environ", {}, clear=True)
    @patch("dlt_saga.utility.orchestration.providers.get_env", return_value="proj")
    def test_init_missing_job_name_raises(self, mock_get_env):
        with pytest.raises(ValueError, match="job_name must be provided"):
            CloudRunProvider()

    @patch(
        "dlt_saga.utility.orchestration.providers.get_env", return_value="env-project"
    )
    @patch.dict(
        "os.environ", {"CLOUD_RUN_REGION": "eu-west1", "CLOUD_RUN_JOB": "env-job"}
    )
    def test_init_from_env(self, mock_get_env):
        provider = CloudRunProvider()
        assert provider.project_id == "env-project"
        assert provider.region == "eu-west1"
        assert provider.job_name == "env-job"

    @patch.dict("os.environ", {"CLOUD_RUN_JOB": "hourly-job"})
    def test_env_var_overrides_explicit_job_name(self):
        """CLOUD_RUN_JOB must win so each Cloud Run job re-triggers itself."""
        provider = CloudRunProvider(project_id="p", job_name="daily-job")
        assert provider.job_name == "hourly-job"

    @patch.dict("os.environ", {"CLOUD_RUN_REGION": "auto-region"})
    def test_env_var_overrides_explicit_region(self):
        provider = CloudRunProvider(
            project_id="p", region="config-region", job_name="j"
        )
        assert provider.region == "auto-region"

    @patch.dict("os.environ", {}, clear=True)
    @patch("dlt_saga.utility.orchestration.cloud_run_trigger.CloudRunJobTrigger")
    def test_trigger_delegates_to_cloud_run_trigger(self, mock_trigger_cls):
        mock_instance = MagicMock()
        mock_instance.trigger_execution.return_value = (
            "projects/p/locations/r/jobs/j/executions/e"
        )
        mock_trigger_cls.return_value = mock_instance

        provider = CloudRunProvider(project_id="p", region="r", job_name="j")
        result = provider.trigger(
            execution_id="exec-1",
            task_count=3,
            command="historize",
            debug=True,
        )

        mock_trigger_cls.assert_called_once_with(
            project_id="p", region="r", job_name="j"
        )
        mock_instance.trigger_execution.assert_called_once_with(
            execution_id="exec-1",
            task_count=3,
            debug_logging=True,
            worker_command="historize",
            force=False,
            worker_concurrency=None,
        )
        assert (
            result.execution_reference == "projects/p/locations/r/jobs/j/executions/e"
        )

    @patch.dict("os.environ", {}, clear=True)
    @patch("dlt_saga.utility.orchestration.cloud_run_trigger.CloudRunJobTrigger")
    def test_trigger_forwards_worker_concurrency(self, mock_trigger_cls):
        mock_instance = MagicMock()
        mock_instance.trigger_execution.return_value = "ref"
        mock_trigger_cls.return_value = mock_instance

        provider = CloudRunProvider(project_id="p", region="r", job_name="j")
        provider.trigger(
            execution_id="exec-1",
            task_count=1,
            worker_concurrency=2,
        )

        mock_instance.trigger_execution.assert_called_once_with(
            execution_id="exec-1",
            task_count=1,
            debug_logging=False,
            worker_command="ingest",
            force=False,
            worker_concurrency=2,
        )


# ---------------------------------------------------------------------------
# OrchestrationProvider interface
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestOrchestrationProviderInterface:
    def test_cannot_instantiate_abc(self):
        with pytest.raises(TypeError):
            OrchestrationProvider()

    def test_custom_provider(self):
        class TestProvider(OrchestrationProvider):
            def trigger(
                self,
                execution_id,
                task_count,
                command="ingest",
                debug=False,
                force=False,
                worker_concurrency=None,
            ):
                return TriggerResult(execution_reference=f"test:{execution_id}")

        provider = TestProvider()
        result = provider.trigger("id-1", 2)
        assert result.execution_reference == "test:id-1"


# ---------------------------------------------------------------------------
# resolve_provider
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestResolveProvider:
    @patch.dict("os.environ", {}, clear=True)
    def test_cloud_run_provider(self):
        config = OrchestrationConfig(
            provider="cloud_run", region="us-central1", job_name="my-job"
        )
        provider = resolve_provider(config=config, project_id="proj")
        assert isinstance(provider, CloudRunProvider)
        assert provider.region == "us-central1"
        assert provider.job_name == "my-job"

    def test_stdout_provider(self):
        config = OrchestrationConfig(provider="stdout")
        provider = resolve_provider(config=config, project_id="proj")
        assert isinstance(provider, StdoutProvider)

    def test_unknown_provider_raises(self):
        config = OrchestrationConfig(provider="k8s")
        with pytest.raises(ValueError, match="Unknown orchestration provider.*k8s"):
            resolve_provider(config=config, project_id="proj")

    def test_no_config_returns_none(self):
        provider = resolve_provider(config=None, project_id="proj")
        assert provider is None

    def test_empty_config_returns_none(self):
        """OrchestrationConfig with provider=None means no orchestration."""
        config = OrchestrationConfig()
        provider = resolve_provider(config=config, project_id="proj")
        assert provider is None

    @patch.dict("os.environ", {}, clear=True)
    def test_cloud_run_passes_region_and_job(self):
        config = OrchestrationConfig(
            provider="cloud_run", region="eu-north1", job_name="custom-job"
        )
        provider = resolve_provider(config=config, project_id="p")
        assert isinstance(provider, CloudRunProvider)
        assert provider.region == "eu-north1"
        assert provider.job_name == "custom-job"

    def test_cloud_run_env_overrides_config(self):
        """CLOUD_RUN_JOB/CLOUD_RUN_REGION env vars override config values."""
        config = OrchestrationConfig(
            provider="cloud_run", region="config-region", job_name="config-job"
        )
        with patch.dict(
            "os.environ",
            {"CLOUD_RUN_REGION": "env-region", "CLOUD_RUN_JOB": "env-job"},
        ):
            provider = resolve_provider(config=config, project_id="p")
        assert isinstance(provider, CloudRunProvider)
        assert provider.region == "env-region"
        assert provider.job_name == "env-job"

    def test_cloud_run_none_fields_use_env(self):
        """When region/job_name are None in config, CloudRunProvider reads env vars."""
        config = OrchestrationConfig(provider="cloud_run")
        with patch.dict(
            "os.environ",
            {"CLOUD_RUN_REGION": "asia-east1", "CLOUD_RUN_JOB": "env-job"},
        ):
            provider = resolve_provider(config=config, project_id="p")
        assert isinstance(provider, CloudRunProvider)
        assert provider.region == "asia-east1"
        assert provider.job_name == "env-job"


# ---------------------------------------------------------------------------
# CLI helpers: _calculate_task_count and _build_task_assignments
# ---------------------------------------------------------------------------


def _make_config(
    name: str, task_group: str = None, schema_name: str = ""
) -> PipelineConfig:
    """Helper to create a minimal PipelineConfig."""
    config_dict = {"base_table_name": name}
    if task_group:
        config_dict["task_group"] = task_group
    return PipelineConfig(
        pipeline_group="test",
        pipeline_name=f"test__{name}",
        table_name=name,
        identifier=f"configs/test/{name}.yml",
        config_dict=config_dict,
        enabled=True,
        tags=[],
        source_type="file",
        schema_name=schema_name,
    )


@pytest.mark.unit
class TestCalculateTaskCount:
    def test_ungrouped_only(self):
        from dlt_saga.cli import _calculate_task_count

        configs = [_make_config("a"), _make_config("b"), _make_config("c")]
        assert _calculate_task_count(configs) == 3

    def test_grouped_only(self):
        from dlt_saga.cli import _calculate_task_count

        configs = [
            _make_config("a", task_group="g1"),
            _make_config("b", task_group="g1"),
            _make_config("c", task_group="g2"),
        ]
        assert _calculate_task_count(configs) == 2

    def test_mixed(self):
        from dlt_saga.cli import _calculate_task_count

        configs = [
            _make_config("a", task_group="g1"),
            _make_config("b", task_group="g1"),
            _make_config("c"),
            _make_config("d"),
        ]
        assert _calculate_task_count(configs) == 3  # 1 group + 2 individual

    def test_empty(self):
        from dlt_saga.cli import _calculate_task_count

        assert _calculate_task_count([]) == 0


@pytest.mark.unit
class TestBuildTaskAssignments:
    def test_ungrouped_pipelines(self):
        from dlt_saga.cli import _build_task_assignments

        configs = [_make_config("a"), _make_config("b")]
        tasks = _build_task_assignments(configs)

        assert len(tasks) == 2
        assert tasks[0]["task_index"] == 0
        assert tasks[0]["pipelines"] == ["test__a"]
        assert tasks[1]["task_index"] == 1
        assert tasks[1]["pipelines"] == ["test__b"]
        assert "task_group" not in tasks[0]

    def test_grouped_pipelines(self):
        from dlt_saga.cli import _build_task_assignments

        configs = [
            _make_config("a", task_group="analytics"),
            _make_config("b", task_group="analytics"),
            _make_config("c"),
        ]
        tasks = _build_task_assignments(configs)

        assert len(tasks) == 2
        # First task is the group
        assert tasks[0]["task_group"] == "analytics"
        assert set(tasks[0]["pipelines"]) == {"test__a", "test__b"}
        # Second task is ungrouped
        assert tasks[1]["pipelines"] == ["test__c"]
        assert tasks[1]["task_index"] == 1

    def test_empty(self):
        from dlt_saga.cli import _build_task_assignments

        assert _build_task_assignments([]) == []

    def test_task_groups_interleave_by_schema(self):
        """Multiple groups in different schemas round-robin by schema."""
        from dlt_saga.cli import _build_task_assignments

        configs = [
            _make_config("a", task_group="g1", schema_name="schema_A"),
            _make_config("b", task_group="g1", schema_name="schema_A"),
            _make_config("c", task_group="g2", schema_name="schema_A"),
            _make_config("d", task_group="g3", schema_name="schema_B"),
        ]
        tasks = _build_task_assignments(configs)

        assert len(tasks) == 3
        # Bucket A had 2 groups, bucket B had 1 — round-robin starts with A.
        assert tasks[0]["task_group"] == "g1"
        assert tasks[1]["task_group"] == "g3"  # schema_B unit diluted between A's
        assert tasks[2]["task_group"] == "g2"

    def test_groups_and_singletons_interleave_together(self):
        """Same-schema groups get diluted by other-schema singletons (issue #85)."""
        from dlt_saga.cli import _build_task_assignments

        configs = [
            _make_config("g1a", task_group="g1", schema_name="schema_A"),
            _make_config("g1b", task_group="g1", schema_name="schema_A"),
            _make_config("g2a", task_group="g2", schema_name="schema_A"),
            _make_config("g2b", task_group="g2", schema_name="schema_A"),
            _make_config("single_b", schema_name="schema_B"),
            _make_config("single_c", schema_name="schema_C"),
        ]
        tasks = _build_task_assignments(configs)

        schemas_per_task = []
        for task in tasks:
            # Use the first pipeline's schema as the bucket key for assertion.
            name = task["pipelines"][0]
            if name.startswith("test__g1"):
                schemas_per_task.append("A")
            elif name.startswith("test__g2"):
                schemas_per_task.append("A")
            elif "single_b" in name:
                schemas_per_task.append("B")
            elif "single_c" in name:
                schemas_per_task.append("C")
        # Consecutive same-schema runs of length > 1 mean clustering wasn't fixed.
        max_run = 1
        run = 1
        for prev, curr in zip(schemas_per_task, schemas_per_task[1:]):
            if prev == curr:
                run += 1
                max_run = max(max_run, run)
            else:
                run = 1
        assert max_run == 1, (
            f"Same-schema task units should not cluster, got order {schemas_per_task}"
        )

    def test_singletons_only_preserves_existing_interleave(self):
        """Regression: an all-singleton plan must order identically to today."""
        from dlt_saga.cli import _build_task_assignments

        configs = [
            _make_config("a", schema_name="schema_A"),
            _make_config("b", schema_name="schema_A"),
            _make_config("c", schema_name="schema_B"),
            _make_config("d", schema_name="schema_B"),
        ]
        tasks = _build_task_assignments(configs)

        names = [t["pipelines"][0] for t in tasks]
        # Old behavior was schema-round-robin: A0, B0, A1, B1 (insertion order
        # within each bucket).
        assert names == ["test__a", "test__c", "test__b", "test__d"]

    def test_all_same_schema_groups_keep_stable_order(self):
        """When every group lives in one schema, fall back to declaration order."""
        from dlt_saga.cli import _build_task_assignments

        configs = [
            _make_config("a", task_group="g1", schema_name="schema_A"),
            _make_config("b", task_group="g2", schema_name="schema_A"),
            _make_config("c", task_group="g3", schema_name="schema_A"),
        ]
        tasks = _build_task_assignments(configs)

        assert [t["task_group"] for t in tasks] == ["g1", "g2", "g3"]


# ---------------------------------------------------------------------------
# _get_worker_environment
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestGetWorkerEnvironment:
    def test_explicit_args(self):
        from dlt_saga.cli import _get_worker_environment

        eid, tidx = _get_worker_environment(execution_id="explicit-id", task_index=7)
        assert eid == "explicit-id"
        assert tidx == 7

    @patch.dict("os.environ", {"CLOUD_RUN_TASK_INDEX": "3"})
    @patch("dlt_saga.cli.get_env", return_value="env-id")
    def test_env_var_fallback(self, mock_get_env):
        from dlt_saga.cli import _get_worker_environment

        eid, tidx = _get_worker_environment()
        assert eid == "env-id"
        assert tidx == 3

    @patch.dict("os.environ", {"SAGA_TASK_INDEX": "5"}, clear=False)
    def test_saga_task_index_fallback(self):
        # Remove CLOUD_RUN_TASK_INDEX if set
        import os

        from dlt_saga.cli import _get_worker_environment

        os.environ.pop("CLOUD_RUN_TASK_INDEX", None)

        eid, tidx = _get_worker_environment(execution_id="x")
        assert tidx == 5

    def test_explicit_overrides_env(self):
        from dlt_saga.cli import _get_worker_environment

        with patch.dict("os.environ", {"CLOUD_RUN_TASK_INDEX": "99"}):
            with patch("dlt_saga.cli.get_env", return_value="env-id"):
                eid, tidx = _get_worker_environment(
                    execution_id="override", task_index=0
                )
                assert eid == "override"
                assert tidx == 0

    @patch("dlt_saga.cli.get_env", return_value=None)
    def test_missing_execution_id_exits(self, mock_get_env):
        import typer

        from dlt_saga.cli import _get_worker_environment

        # Newer typer versions raise typer.Exit (typer._click.exceptions.Exit)
        with pytest.raises(typer.Exit):
            _get_worker_environment()

    @patch("dlt_saga.cli.get_env", return_value="id")
    @patch.dict("os.environ", {}, clear=True)
    def test_missing_task_index_exits(self, mock_get_env):
        import os

        import typer

        from dlt_saga.cli import _get_worker_environment

        os.environ.pop("CLOUD_RUN_TASK_INDEX", None)
        os.environ.pop("SAGA_TASK_INDEX", None)
        # Newer typer versions raise typer.Exit (typer._click.exceptions.Exit)
        with pytest.raises(typer.Exit):
            _get_worker_environment()


# ---------------------------------------------------------------------------
# _resolve_worker_concurrency precedence chain
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestResolveWorkerConcurrency:
    """Precedence: CLI override > env var > saga_project.yml > default 4."""

    def setup_method(self):
        from dlt_saga.project_config import _reset_cache

        _reset_cache()

    def teardown_method(self):
        from dlt_saga.project_config import _reset_cache

        _reset_cache()

    @patch.dict("os.environ", {}, clear=True)
    def test_default_when_nothing_set(self, tmp_path, monkeypatch):
        from dlt_saga.cli import (
            _DEFAULT_WORKER_CONCURRENCY,
            _resolve_worker_concurrency,
        )

        monkeypatch.chdir(tmp_path)
        assert _resolve_worker_concurrency() == _DEFAULT_WORKER_CONCURRENCY

    @patch.dict("os.environ", {"SAGA_WORKER_CONCURRENCY": "2"}, clear=True)
    def test_env_var_used(self, tmp_path, monkeypatch):
        from dlt_saga.cli import _resolve_worker_concurrency

        monkeypatch.chdir(tmp_path)
        assert _resolve_worker_concurrency() == 2

    @patch.dict("os.environ", {}, clear=True)
    def test_project_config_used(self, tmp_path, monkeypatch):
        from dlt_saga.cli import _resolve_worker_concurrency

        yml = tmp_path / "saga_project.yml"
        yml.write_text(
            "orchestration:\n  provider: cloud_run\n  worker_concurrency: 3\n"
        )
        monkeypatch.chdir(tmp_path)
        assert _resolve_worker_concurrency() == 3

    @patch.dict("os.environ", {"SAGA_WORKER_CONCURRENCY": "5"}, clear=True)
    def test_env_var_overrides_project_config(self, tmp_path, monkeypatch):
        from dlt_saga.cli import _resolve_worker_concurrency

        yml = tmp_path / "saga_project.yml"
        yml.write_text(
            "orchestration:\n  provider: cloud_run\n  worker_concurrency: 3\n"
        )
        monkeypatch.chdir(tmp_path)
        assert _resolve_worker_concurrency() == 5

    @patch.dict("os.environ", {"SAGA_WORKER_CONCURRENCY": "5"}, clear=True)
    def test_cli_override_wins(self, tmp_path, monkeypatch):
        from dlt_saga.cli import _resolve_worker_concurrency

        yml = tmp_path / "saga_project.yml"
        yml.write_text(
            "orchestration:\n  provider: cloud_run\n  worker_concurrency: 3\n"
        )
        monkeypatch.chdir(tmp_path)
        assert _resolve_worker_concurrency(cli_override=1) == 1

    @patch.dict("os.environ", {"SAGA_WORKER_CONCURRENCY": "not-an-int"}, clear=True)
    def test_invalid_env_var_falls_through(self, tmp_path, monkeypatch):
        from dlt_saga.cli import (
            _DEFAULT_WORKER_CONCURRENCY,
            _resolve_worker_concurrency,
        )

        monkeypatch.chdir(tmp_path)
        assert _resolve_worker_concurrency() == _DEFAULT_WORKER_CONCURRENCY

    @patch.dict("os.environ", {"SAGA_WORKER_CONCURRENCY": "0"}, clear=True)
    def test_non_positive_env_var_falls_through(self, tmp_path, monkeypatch):
        from dlt_saga.cli import (
            _DEFAULT_WORKER_CONCURRENCY,
            _resolve_worker_concurrency,
        )

        monkeypatch.chdir(tmp_path)
        assert _resolve_worker_concurrency() == _DEFAULT_WORKER_CONCURRENCY


# ---------------------------------------------------------------------------
# _execute_worker_parallel honors max_workers
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestExecuteWorkerParallel:
    def test_caps_at_max_workers(self):
        """Concurrency is capped even when there are more configs than workers."""
        import threading

        from dlt_saga.cli import _execute_worker_parallel

        configs = [_make_config(f"t{i}") for i in range(8)]
        active_lock = threading.Lock()
        active_count = {"current": 0, "peak": 0}
        gate = threading.Event()

        def run_fn(config, log_prefix):
            with active_lock:
                active_count["current"] += 1
                if active_count["current"] > active_count["peak"]:
                    active_count["peak"] = active_count["current"]
            gate.wait(timeout=0.5)
            with active_lock:
                active_count["current"] -= 1
            return True

        # Release the gate slightly later so threads pile up first.
        timer = threading.Timer(0.05, gate.set)
        timer.start()
        try:
            failed = _execute_worker_parallel(
                configs, task_index=0, label="ingest", run_fn=run_fn, max_workers=2
            )
        finally:
            timer.cancel()
            gate.set()

        assert failed == []
        assert active_count["peak"] <= 2

    def test_caps_at_len_configs_when_max_workers_larger(self):
        """max_workers larger than len(configs) means len(configs) threads, no idle ones."""
        from dlt_saga.cli import _execute_worker_parallel

        configs = [_make_config("a"), _make_config("b")]
        ran = []

        def run_fn(config, log_prefix):
            ran.append(config.table_name)
            return True

        failed = _execute_worker_parallel(
            configs, task_index=0, label="ingest", run_fn=run_fn, max_workers=10
        )
        assert failed == []
        assert sorted(ran) == ["a", "b"]

    def test_empty_configs_short_circuits(self):
        from dlt_saga.cli import _execute_worker_parallel

        called = []

        def run_fn(config, log_prefix):
            called.append(config)
            return True

        failed = _execute_worker_parallel(
            configs=[],
            task_index=0,
            label="ingest",
            run_fn=run_fn,
            max_workers=4,
        )
        assert failed == []
        assert called == []
