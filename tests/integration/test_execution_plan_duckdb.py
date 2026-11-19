"""The execution plan store round-trips a pipeline config through a real table."""

import pytest

from dlt_saga.notify import _as_dict
from dlt_saga.pipeline_config.base_config import PipelineConfig
from dlt_saga.utility.orchestration.execution_plan import ExecutionPlanManager

ORCHESTRATION_SCHEMA = "dlt_orchestration"


def _config() -> PipelineConfig:
    return PipelineConfig(
        pipeline_group="native_load",
        pipeline_name="native_load__events",
        table_name="events",
        identifier="configs/native_load/events.yml",
        config_dict={
            "write_disposition": "append",
            "note": 'it\'s a "quoted" value with a \\ backslash',
            "columns": {
                "zeta": {"data_type": "text"},
                "app_id": {"data_type": "bigint"},
            },
        },
        enabled=True,
        tags=[],
        schema_name="dlt_native_load",
    )


@pytest.mark.integration
class TestStoredConfigRoundTrip:
    def test_worker_reads_the_config_as_written(self, duckdb_destination):
        manager = ExecutionPlanManager(duckdb_destination, ORCHESTRATION_SCHEMA)
        manager.ensure_table_exists()
        config = _config()
        execution_id = manager.create_execution_plan([config])

        (assigned,) = manager.get_task_assignment(execution_id, 0)

        assert assigned.config_dict["note"] == config.config_dict["note"]
        assert list(assigned.config_dict["columns"]) == ["zeta", "app_id"]

    def test_config_survives_status_updates(self, duckdb_destination):
        manager = ExecutionPlanManager(duckdb_destination, ORCHESTRATION_SCHEMA)
        manager.ensure_table_exists()
        config = _config()
        execution_id = manager.create_execution_plan([config])

        manager.update_task_status(execution_id, 0, "running")
        manager.record_pipeline_outcomes(
            execution_id, 0, {config.identifier: ("failed", "boom")}
        )

        (assigned,) = manager.get_task_assignment(execution_id, 0)
        assert assigned.config_dict["note"] == config.config_dict["note"]
        assert list(assigned.config_dict["columns"]) == ["zeta", "app_id"]

    def test_notify_reads_the_stored_cell(self, duckdb_destination):
        manager = ExecutionPlanManager(duckdb_destination, ORCHESTRATION_SCHEMA)
        manager.ensure_table_exists()
        execution_id = manager.create_execution_plan([_config()])

        (row,) = duckdb_destination.execute_sql(
            f"SELECT config_json FROM {manager.plans_view_id} "
            f"WHERE execution_id = '{execution_id}'",
            ORCHESTRATION_SCHEMA,
        )

        assert _as_dict(row.config_json)["pipeline_name"] == "native_load__events"
