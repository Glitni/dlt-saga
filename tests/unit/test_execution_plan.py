"""Tests for ExecutionPlanManager (orchestrator-side plan creation)."""

import json

import pytest

from dlt_saga.pipeline_config.base_config import PipelineConfig
from dlt_saga.utility.orchestration.execution_plan import (
    ExecutionMetadata,
    ExecutionPlanManager,
)


class _RecordingDestination:
    """Minimal stand-in for a Destination, capturing executed SQL."""

    def __init__(self):
        self.sql_calls: list[str] = []

    def get_full_table_id(self, schema: str, table: str) -> str:
        return f"{schema}.{table}"

    def type_name(self, logical_type: str) -> str:
        return logical_type.upper()

    def json_type_name(self) -> str:
        return "JSON"

    def partition_ddl(self, column: str) -> str:
        return ""

    def cluster_ddl(self, columns) -> str:
        return ""

    def ensure_schema_exists(self, schema: str) -> None:
        pass

    def current_timestamp_expression(self) -> str:
        return "CURRENT_TIMESTAMP()"

    def parse_json_expression(self, value_sql: str) -> str:
        return f"PARSE_JSON({value_sql})"

    def create_or_replace_view(self, schema: str, name: str, sql: str) -> None:
        pass

    def execute_sql(self, sql: str, schema: str = ""):
        self.sql_calls.append(sql)
        return []


def _make_config(name: str, **extra) -> PipelineConfig:
    return PipelineConfig(
        pipeline_group="api",
        pipeline_name=f"api__{name}",
        table_name=name,
        identifier=f"api/{name}",
        config_dict={"write_disposition": "append", **extra},
        enabled=True,
        tags=[],
        schema_name="dlt_api",
    )


def _extract_stored_configs(sql_calls: list[str]) -> list[dict]:
    """Pull every embedded JSON literal out of captured INSERT statements."""
    marker = "PARSE_JSON('"
    found = []
    for sql in sql_calls:
        parts = sql.split(marker)
        # First chunk is everything before the first PARSE_JSON; skip it.
        for chunk in parts[1:]:
            end = chunk.find("')")
            if end < 0:
                continue
            payload = chunk[:end]
            # Un-escape the SQL single-quote doubling done by _escape
            payload = payload.replace("''", "'").replace("\\\\", "\\")
            try:
                found.append(json.loads(payload))
            except json.JSONDecodeError:
                continue
    return found


@pytest.mark.unit
class TestExecutionPlanOverrideBakeIn:
    """`saga ingest --orchestrate --start-value-override` must reach workers.

    Cloud Run workers don't see the orchestrator's CLI flags; the only state
    they pick up is the per-pipeline config_json from the plan table. Verify
    that orchestrator-supplied overrides are baked into stored_config.
    """

    def test_overrides_baked_into_stored_config(self):
        dest = _RecordingDestination()
        manager = ExecutionPlanManager(destination=dest, schema="dlt_orch")

        manager.create_execution_plan(
            [_make_config("orders"), _make_config("customers")],
            metadata=ExecutionMetadata(
                command="ingest",
                start_value_override="202604",
                end_value_override="202612",
            ),
        )

        stored = _extract_stored_configs(dest.sql_calls)
        pipeline_configs = [c for c in stored if "pipeline_name" in c]
        assert len(pipeline_configs) == 2
        for cfg in pipeline_configs:
            assert cfg["start_value_override"] == "202604"
            assert cfg["end_value_override"] == "202612"

    def test_no_overrides_when_metadata_empty(self):
        dest = _RecordingDestination()
        manager = ExecutionPlanManager(destination=dest, schema="dlt_orch")

        manager.create_execution_plan(
            [_make_config("orders")], metadata=ExecutionMetadata()
        )

        stored = _extract_stored_configs(dest.sql_calls)
        pipeline_configs = [c for c in stored if "pipeline_name" in c]
        assert len(pipeline_configs) == 1
        assert "start_value_override" not in pipeline_configs[0]
        assert "end_value_override" not in pipeline_configs[0]

    def test_metadata_override_replaces_config_dict_value(self):
        """CLI override wins over a value already present in config_dict."""
        dest = _RecordingDestination()
        manager = ExecutionPlanManager(destination=dest, schema="dlt_orch")

        manager.create_execution_plan(
            [_make_config("orders", start_value_override="config-value")],
            metadata=ExecutionMetadata(start_value_override="cli-value"),
        )

        stored = _extract_stored_configs(dest.sql_calls)
        pipeline_configs = [c for c in stored if "pipeline_name" in c]
        assert pipeline_configs[0]["start_value_override"] == "cli-value"

    def test_metadata_default_does_not_require_argument(self):
        """Calling without metadata still works (no overrides leak in)."""
        dest = _RecordingDestination()
        manager = ExecutionPlanManager(destination=dest, schema="dlt_orch")

        manager.create_execution_plan([_make_config("orders")])

        stored = _extract_stored_configs(dest.sql_calls)
        pipeline_configs = [c for c in stored if "pipeline_name" in c]
        assert "start_value_override" not in pipeline_configs[0]


# ---------------------------------------------------------------------------
# Force propagation through the orchestration provider stack
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestForcePropagation:
    """`--force` must reach workers via SAGA_FORCE env var.

    `--full-refresh` is intentionally *not* propagated — it requires
    interactive confirmation and must run from a local orchestrator.
    """

    def test_stdout_provider_includes_force(self, capsys):
        from dlt_saga.utility.orchestration.providers import StdoutProvider

        StdoutProvider().trigger(execution_id="x", task_count=1, force=True)
        out = json.loads(capsys.readouterr().out)
        assert out["force"] is True

    def test_stdout_provider_force_defaults_to_false(self, capsys):
        from dlt_saga.utility.orchestration.providers import StdoutProvider

        StdoutProvider().trigger(execution_id="x", task_count=1)
        out = json.loads(capsys.readouterr().out)
        assert out["force"] is False

    def _run_trigger_with_fake_run_v2(self, **trigger_kwargs):
        """Construct a CloudRunJobTrigger with a stub run_v2 module and call it.

        Returns the captured RunJobRequest so the caller can inspect env vars.
        """
        from unittest.mock import MagicMock

        run_v2 = MagicMock()
        run_v2.EnvVar = lambda name, value: {"name": name, "value": value}

        class _ContainerOverride:
            def __init__(self, env):
                self.env = env

        class _Overrides:
            def __init__(self, container_overrides, task_count):
                self.container_overrides = container_overrides
                self.task_count = task_count

        class _RunJobRequest:
            def __init__(self, name, overrides):
                self.name = name
                self.overrides = overrides

        run_v2.RunJobRequest = _RunJobRequest
        run_v2.RunJobRequest.Overrides = _Overrides
        run_v2.RunJobRequest.Overrides.ContainerOverride = _ContainerOverride

        captured: dict = {}

        class _FakeJobsClient:
            def run_job(self, request):
                captured["request"] = request
                op = MagicMock()
                op.metadata.name = "fake-execution"
                return op

        from dlt_saga.utility.orchestration import cloud_run_trigger as crt

        trigger = crt.CloudRunJobTrigger.__new__(crt.CloudRunJobTrigger)
        trigger._run_v2 = run_v2
        trigger.project_id = "p"
        trigger.region = "r"
        trigger.job_name = "j"
        trigger.client = _FakeJobsClient()

        trigger.trigger_execution(**trigger_kwargs)
        return captured["request"]

    def test_cloud_run_trigger_emits_saga_force_env_var(self):
        """When force=True, the Cloud Run trigger sends SAGA_FORCE=true."""
        request = self._run_trigger_with_fake_run_v2(
            execution_id="exec-1", task_count=2, force=True
        )
        env = request.overrides.container_overrides[0].env
        names = {e["name"]: e["value"] for e in env}
        assert names.get("SAGA_FORCE") == "true"
        assert names.get("SAGA_WORKER_MODE") == "true"
        assert names.get("SAGA_EXECUTION_ID") == "exec-1"

    def test_cloud_run_trigger_omits_saga_force_when_false(self):
        request = self._run_trigger_with_fake_run_v2(
            execution_id="x", task_count=1, force=False
        )
        env = request.overrides.container_overrides[0].env
        names = {e["name"] for e in env}
        assert "SAGA_FORCE" not in names
