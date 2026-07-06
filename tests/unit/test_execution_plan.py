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

    def escape_string_literal(self, value: str) -> str:
        # Backslash dialect (BigQuery/Databricks), matching Destination base.
        return (
            value.replace("\\", "\\\\")
            .replace("'", "\\'")
            .replace("\n", "\\n")
            .replace("\r", "\\r")
            .replace("\t", "\\t")
            .replace("\x00", "")
        )

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

    def partition_cluster_ddl(self, partition_column, cluster_columns) -> str:
        parts = []
        if partition_column:
            parts.append(self.partition_ddl(partition_column))
        if cluster_columns:
            parts.append(self.cluster_ddl(cluster_columns))
        return "\n".join(p for p in parts if p)

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


def _make_config(name: str, schema_name: str = "dlt_api", **extra) -> PipelineConfig:
    return PipelineConfig(
        pipeline_group="api",
        pipeline_name=f"api__{name}",
        table_name=name,
        identifier=f"api/{name}",
        config_dict={"write_disposition": "append", **extra},
        enabled=True,
        tags=[],
        schema_name=schema_name,
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
class TestPlansTableDdl:
    """The `_saga_execution_plans` DDL must reconcile partition/cluster per
    destination, not emit both raw (Databricks rejects the combination)."""

    def test_ddl_routes_through_partition_cluster_reconciler(self):
        dest = _RecordingDestination()
        dest.partition_ddl = lambda *a, **k: pytest.fail(
            "partition_ddl called directly — must go through partition_cluster_ddl"
        )
        dest.cluster_ddl = lambda *a, **k: pytest.fail(
            "cluster_ddl called directly — must go through partition_cluster_ddl"
        )
        dest.partition_cluster_ddl = lambda partition, cluster: "<<RECONCILED>>"

        manager = ExecutionPlanManager(destination=dest, schema="dlt_orch")
        manager.ensure_table_exists()

        plans_ddl = next(
            sql for sql in dest.sql_calls if "CREATE TABLE IF NOT EXISTS" in sql
        )
        assert "<<RECONCILED>>" in plans_ddl


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


@pytest.mark.unit
class TestExecutionIdControl:
    """Execution ID can be explicitly provided or auto-generated."""

    def test_explicit_execution_id_is_used(self):
        """When explicit execution_id is provided, it's used instead of auto-generating."""
        dest = _RecordingDestination()
        manager = ExecutionPlanManager(destination=dest, schema="dlt_orch")

        returned_id = manager.create_execution_plan(
            [_make_config("orders")],
            execution_id="my-custom-run-001",
        )

        assert returned_id == "my-custom-run-001"
        # Verify it appears in the SQL calls
        all_sql = " ".join(dest.sql_calls)
        assert "my-custom-run-001" in all_sql

    def test_execution_id_auto_generated_when_not_provided(self):
        """When no execution_id is provided, a UUID is auto-generated."""
        dest = _RecordingDestination()
        manager = ExecutionPlanManager(destination=dest, schema="dlt_orch")

        returned_id = manager.create_execution_plan([_make_config("orders")])

        # Should be a valid UUID string (36 chars for UUID4 with hyphens)
        assert len(returned_id) == 36
        assert returned_id.count("-") == 4

    def test_explicit_execution_id_in_plan_rows(self):
        """Explicit execution_id appears in all plan rows."""
        dest = _RecordingDestination()
        manager = ExecutionPlanManager(destination=dest, schema="dlt_orch")

        execution_id = "orchestration-run-2026-06-03"
        manager.create_execution_plan(
            [_make_config("orders"), _make_config("customers")],
            execution_id=execution_id,
        )

        # Check that the execution_id appears in the SQL calls
        all_sql = " ".join(dest.sql_calls)
        assert execution_id in all_sql


# ---------------------------------------------------------------------------
# Task-unit interleaving across groups and singletons (issue #85)
# ---------------------------------------------------------------------------


def _task_index_for(sql_calls: list[str], pipeline_name: str) -> int:
    """Recover the task_index assigned to a given pipeline_name in the INSERT."""
    rows = _extract_stored_configs(sql_calls)
    target = next(r for r in rows if r.get("pipeline_name") == pipeline_name)
    # The INSERT clause for that row contains the task_index literal — find it
    # by scanning for the row's pipeline identifier.
    for sql in sql_calls:
        if target["pipeline_name"] not in sql:
            continue
        # Split by row separator and find the chunk mentioning this pipeline.
        chunks = sql.split("(CURRENT_TIMESTAMP()")
        for chunk in chunks[1:]:
            if target["pipeline_name"] not in chunk:
                continue
            parts = [p.strip() for p in chunk.split(",")]
            # parts: ['', '<exec_id>', '<task_index>', ...]
            return int(parts[2].strip())
    raise AssertionError(f"task_index not found for {pipeline_name}")


@pytest.mark.unit
class TestTaskUnitInterleaving:
    """Task groups participate in schema-interleaving alongside singletons."""

    def test_mixed_groups_and_singletons_interleave_by_schema(self):
        """Same-schema groups get diluted by other-schema units (issue #85)."""
        dest = _RecordingDestination()
        manager = ExecutionPlanManager(destination=dest, schema="dlt_orch")

        configs = [
            _make_config("g1a", schema_name="schema_A", task_group="g1"),
            _make_config("g1b", schema_name="schema_A", task_group="g1"),
            _make_config("g2a", schema_name="schema_A", task_group="g2"),
            _make_config("g2b", schema_name="schema_A", task_group="g2"),
            _make_config("single_b", schema_name="schema_B"),
            _make_config("single_c", schema_name="schema_C"),
        ]
        manager.create_execution_plan(configs)

        # Build task_index -> schema by reading back what we persisted.
        schema_by_task: dict[int, str] = {}
        for cfg in configs:
            ti = _task_index_for(dest.sql_calls, cfg.pipeline_name)
            # Every member of a group shares the same task_index; the schema
            # we assert against is the unit's first-pipeline schema.
            schema_by_task.setdefault(ti, cfg.schema_name)

        ordered_schemas = [schema_by_task[i] for i in sorted(schema_by_task)]
        # No two consecutive units should share a schema with the prior unit.
        for prev, curr in zip(ordered_schemas, ordered_schemas[1:]):
            assert prev != curr, f"Same-schema clustering not fixed: {ordered_schemas}"

    def test_all_singletons_match_previous_interleave(self):
        """Regression: singleton-only plans keep today's round-robin order."""
        dest = _RecordingDestination()
        manager = ExecutionPlanManager(destination=dest, schema="dlt_orch")

        configs = [
            _make_config("a", schema_name="schema_A"),
            _make_config("b", schema_name="schema_A"),
            _make_config("c", schema_name="schema_B"),
            _make_config("d", schema_name="schema_B"),
        ]
        manager.create_execution_plan(configs)

        idx_a = _task_index_for(dest.sql_calls, "api__a")
        idx_b = _task_index_for(dest.sql_calls, "api__b")
        idx_c = _task_index_for(dest.sql_calls, "api__c")
        idx_d = _task_index_for(dest.sql_calls, "api__d")
        # A0, B0, A1, B1
        assert (idx_a, idx_c, idx_b, idx_d) == (0, 1, 2, 3)

    def test_all_same_schema_groups_stable_order(self):
        """Single-schema groups can't be interleaved — order stays declared."""
        dest = _RecordingDestination()
        manager = ExecutionPlanManager(destination=dest, schema="dlt_orch")

        configs = [
            _make_config("a", schema_name="schema_A", task_group="g1"),
            _make_config("b", schema_name="schema_A", task_group="g2"),
            _make_config("c", schema_name="schema_A", task_group="g3"),
        ]
        manager.create_execution_plan(configs)

        idx_a = _task_index_for(dest.sql_calls, "api__a")
        idx_b = _task_index_for(dest.sql_calls, "api__b")
        idx_c = _task_index_for(dest.sql_calls, "api__c")
        assert (idx_a, idx_b, idx_c) == (0, 1, 2)


# ---------------------------------------------------------------------------
# Force propagation through the orchestration provider stack
# ---------------------------------------------------------------------------


def _executions_task_count(sql_calls: list[str]) -> int:
    """Recover the task_count literal from the _saga_executions INSERT."""
    for sql in sql_calls:
        if "_saga_executions" not in sql or "INSERT INTO" not in sql:
            continue
        values_idx = sql.upper().rfind("VALUES")
        payload = sql[values_idx:]
        open_paren = payload.find("(")
        close_paren = payload.rfind(")")
        parts = [p.strip() for p in payload[open_paren + 1 : close_paren].split(",")]
        # Column order: execution_id, created_at, command, pipeline_count, task_count, ...
        return int(parts[4])
    raise AssertionError("_saga_executions INSERT not found in captured SQL")


@pytest.mark.unit
class TestExecutionsTaskCount:
    """The executions row must record the actual task count, not n - 1."""

    def test_single_task_records_count_one(self):
        dest = _RecordingDestination()
        manager = ExecutionPlanManager(destination=dest, schema="dlt_orch")

        manager.create_execution_plan([_make_config("orders")])

        assert _executions_task_count(dest.sql_calls) == 1

    def test_multiple_singletons_record_full_count(self):
        dest = _RecordingDestination()
        manager = ExecutionPlanManager(destination=dest, schema="dlt_orch")

        manager.create_execution_plan([_make_config(name) for name in ("a", "b", "c")])

        assert _executions_task_count(dest.sql_calls) == 3

    def test_task_group_collapses_to_one_task(self):
        """Pipelines sharing a task_group count as a single task unit."""
        dest = _RecordingDestination()
        manager = ExecutionPlanManager(destination=dest, schema="dlt_orch")

        manager.create_execution_plan(
            [
                _make_config("a", task_group="g1"),
                _make_config("b", task_group="g1"),
                _make_config("c"),
            ]
        )

        assert _executions_task_count(dest.sql_calls) == 2


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

    def test_cloud_run_trigger_emits_worker_concurrency_env_var(self):
        """worker_concurrency must reach workers via SAGA_WORKER_CONCURRENCY."""
        request = self._run_trigger_with_fake_run_v2(
            execution_id="exec-2", task_count=4, worker_concurrency=2
        )
        env = request.overrides.container_overrides[0].env
        names = {e["name"]: e["value"] for e in env}
        assert names.get("SAGA_WORKER_CONCURRENCY") == "2"

    def test_cloud_run_trigger_omits_worker_concurrency_when_none(self):
        """When worker_concurrency is unset, no env var is sent (worker uses defaults)."""
        request = self._run_trigger_with_fake_run_v2(
            execution_id="exec-3", task_count=1
        )
        env = request.overrides.container_overrides[0].env
        names = {e["name"] for e in env}
        assert "SAGA_WORKER_CONCURRENCY" not in names


@pytest.mark.unit
class TestRecordLocalRun:
    """record_local_run captures local (non-orchestrated) run outcomes."""

    def _records(self):
        return [
            {
                "pipeline_type": "api",
                "pipeline_identifier": "api/orders",
                "table_name": "orders",
                "status": "completed",
                "error_message": None,
            },
            {
                "pipeline_type": "database",
                "pipeline_identifier": "database/geozone",
                "table_name": "geozone",
                "status": "failed",
                "error_message": "NotFound: 404 table missing",
            },
        ]

    def test_writes_terminal_rows_flagged_local(self):
        dest = _RecordingDestination()
        manager = ExecutionPlanManager(destination=dest, schema="dlt_orch")

        eid = manager.record_local_run(
            self._records(),
            metadata=ExecutionMetadata(command="run", environment="dev"),
            execution_id="local-001",
        )

        assert eid == "local-001"
        all_sql = " ".join(dest.sql_calls)
        # Both the plans insert and the executions insert flag the run local.
        assert "is_orchestrated" in all_sql
        assert "FALSE" in all_sql
        assert "TRUE" not in all_sql  # nothing orchestrated was written
        # Terminal statuses and the real error are recorded.
        assert "'completed'" in all_sql
        assert "'failed'" in all_sql
        assert "NotFound: 404 table missing" in all_sql

    def test_empty_results_writes_nothing(self):
        dest = _RecordingDestination()
        manager = ExecutionPlanManager(destination=dest, schema="dlt_orch")

        assert manager.record_local_run([]) is None
        assert dest.sql_calls == []

    def test_auto_generates_execution_id(self):
        dest = _RecordingDestination()
        manager = ExecutionPlanManager(destination=dest, schema="dlt_orch")

        eid = manager.record_local_run(self._records())
        assert eid is not None and len(eid) == 36

    def test_multiline_error_message_does_not_break_sql(self):
        """A multi-line traceback must not leave a raw newline inside a literal.

        Regression: an unescaped newline previously produced BigQuery
        "Unclosed string literal" and silently dropped the run record.
        """
        dest = _RecordingDestination()
        manager = ExecutionPlanManager(destination=dest, schema="dlt_orch")

        multiline = "PipelineStepFailed:\n  400 Syntax error\nO'Brien \\ path"
        records = [
            {
                "pipeline_type": "api",
                "pipeline_identifier": "api/orders",
                "table_name": "orders",
                "status": "failed",
                "error_message": multiline,
            }
        ]

        eid = manager.record_local_run(records, execution_id="local-002")
        assert eid == "local-002"

        # The error is embedded as a well-formed single-quoted literal whose body
        # carries no raw newline (which is what previously broke the statement).
        plans_sql = next(s for s in dest.sql_calls if "error_message" in s)
        escaped = dest.escape_string_literal(multiline)
        assert "\n" not in escaped
        assert f"'{escaped}'" in plans_sql


@pytest.mark.unit
class TestIsOrchestratedFlag:
    """Orchestrated writes flag is_orchestrated TRUE; the column is in the schema."""

    def test_orchestrated_plan_marked_true(self):
        dest = _RecordingDestination()
        manager = ExecutionPlanManager(destination=dest, schema="dlt_orch")

        manager.create_execution_plan([_make_config("orders")])

        all_sql = " ".join(dest.sql_calls)
        assert "is_orchestrated" in all_sql
        assert "TRUE" in all_sql

    def test_schema_includes_is_orchestrated_column(self):
        dest = _RecordingDestination()
        manager = ExecutionPlanManager(destination=dest, schema="dlt_orch")

        manager.ensure_table_exists()

        create_sql = " ".join(s for s in dest.sql_calls if "CREATE TABLE" in s.upper())
        # Present in both the plans and the executions table DDL.
        assert create_sql.count("is_orchestrated") >= 2
