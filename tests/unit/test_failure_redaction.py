"""A failure's message leaves the process through channels the log handlers'
redaction filter never sees: the end-of-run summary, the run records in the
warehouse, and notifiers. Each must mask registered secrets itself."""

from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from dlt_saga.utility.secrets import redaction

pytestmark = pytest.mark.unit

SECRET = "k3y-value-123"
MASK = redaction.REDACTION_MASK


@pytest.fixture(autouse=True)
def _registered_secret():
    redaction._reset_for_testing()
    redaction.register_secret(SECRET)
    yield
    redaction._reset_for_testing()


def _error_text():
    return f"Max retries exceeded with url: /data?api_key={SECRET}"


def test_pipeline_result_error_is_redacted():
    from dlt_saga.session import PipelineResult

    result = PipelineResult(pipeline_name="p", success=False, error=_error_text())

    assert SECRET not in result.error
    assert MASK in result.error


def test_local_run_record_is_redacted():
    from dlt_saga.utility.orchestration.execution_plan import ExecutionPlanManager

    dest = MagicMock()
    dest.escape_string_literal.side_effect = lambda v: v.replace("'", "\\'")
    manager = ExecutionPlanManager(destination=dest, schema="orch")

    manager.record_local_run(
        [
            {
                "pipeline_type": "api",
                "pipeline_identifier": "api/orders",
                "table_name": "orders",
                "status": "failed",
                "error_message": _error_text(),
            }
        ]
    )

    written = " ".join(c.args[0] for c in dest.execute_sql.call_args_list)
    assert SECRET not in written
    assert MASK in written


def test_task_status_update_is_redacted():
    from dlt_saga.utility.orchestration.execution_plan import ExecutionPlanManager

    current = SimpleNamespace(
        pipeline_type="api",
        pipeline_identifier="api/orders",
        table_name="orders",
        config_json="{}",
        started_at=None,
        completed_at=None,
    )
    dest = MagicMock()
    dest.escape_string_literal.side_effect = lambda v: v.replace("'", "\\'")
    dest.execute_sql.side_effect = lambda sql, schema: (
        [current] if sql.lstrip().startswith("SELECT") else []
    )
    manager = ExecutionPlanManager(destination=dest, schema="orch")

    manager.update_task_status("exec-1", 0, "failed", _error_text())

    inserts = [
        c.args[0] for c in dest.execute_sql.call_args_list if "INSERT" in c.args[0]
    ]
    assert inserts
    assert all(SECRET not in sql for sql in inserts)


def test_worker_error_masked_before_truncation():
    """A secret cut in half by truncation no longer matches the registry, so
    masking has to come first."""
    from dlt_saga.utility.cli.run_modes import (
        _MAX_ERROR_MESSAGE_LEN,
        _format_run_error,
    )

    prefix = "x" * (_MAX_ERROR_MESSAGE_LEN - len("ValueError: ") - 5)
    message = _format_run_error(ValueError(prefix + SECRET))

    assert SECRET[:5] not in message
    assert len(message) <= _MAX_ERROR_MESSAGE_LEN
