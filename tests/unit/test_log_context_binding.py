"""Every per-pipeline runner binds the log context around the pipeline's
execution, so records emitted by the run — saga's, dlt's, an HTTP library's —
carry ``pipeline`` and ``command`` under the JSON console format.

Four entry points execute a pipeline: ``Session`` (local ingest/historize) and
``run_modes`` (worker ingest/historize). Each must bind, and each must have
released the context once it returns, or a pool thread would carry one
pipeline's fields into the next task it picks up.
"""

from unittest.mock import MagicMock, patch

import pytest

import dlt_saga.utility.cli.run_modes as run_modes
from dlt_saga.pipeline_config import PipelineConfig
from dlt_saga.session import Session
from dlt_saga.utility.cli.logging import current_log_context


def _config(name: str = "grp__p") -> PipelineConfig:
    return PipelineConfig(
        pipeline_group="grp",
        pipeline_name=name,
        table_name="p",
        identifier="configs/grp/p.yml",
        config_dict={"base_table_name": "p", "write_disposition": "append"},
        enabled=True,
        tags=[],
        source_type="file",
        schema_name="",
    )


def _capturing_runner(seen: list):
    runner = MagicMock()

    def run():
        seen.append(current_log_context())
        return {"status": "completed", "mode": "incremental", "duration": 0.1}

    runner.run.side_effect = run
    return runner


@pytest.mark.unit
class TestLogContextBinding:
    def test_session_ingest(self):
        seen: list = []
        with patch(
            "dlt_saga.session.execute_pipeline",
            side_effect=lambda *a, **k: seen.append(current_log_context()) or [],
        ):
            Session._execute_single_ingest(_config(), "[1/1]")
        assert seen == [{"pipeline": "grp__p", "command": "ingest"}]
        assert current_log_context() == {}

    def test_session_historize(self):
        seen: list = []
        with patch(
            "dlt_saga.historize.factory.build_historize_runner",
            return_value=_capturing_runner(seen),
        ):
            Session._execute_single_historize(_config(), full_refresh=False)
        assert seen == [{"pipeline": "grp__p", "command": "historize"}]
        assert current_log_context() == {}

    def test_worker_ingest(self):
        seen: list = []
        with patch(
            "dlt_saga.pipelines.executor.execute_pipeline",
            side_effect=lambda *a, **k: seen.append(current_log_context()) or [],
        ):
            run_modes._run_pipeline_safe(_config(), "[1/1]")
        assert seen == [{"pipeline": "grp__p", "command": "ingest"}]
        assert current_log_context() == {}

    def test_worker_historize(self):
        seen: list = []
        with patch.object(
            run_modes, "_build_historize_runner", return_value=_capturing_runner(seen)
        ):
            run_modes._run_historize_safe(_config(), False, "[1/1]")
        assert seen == [{"pipeline": "grp__p", "command": "historize"}]
        assert current_log_context() == {}

    def test_context_released_on_failure(self):
        with patch(
            "dlt_saga.session.execute_pipeline", side_effect=RuntimeError("boom")
        ):
            Session._execute_single_ingest(_config(), "")
        assert current_log_context() == {}
