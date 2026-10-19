"""execute_pipeline separates a failure preparing a pipeline from one in its run."""

from unittest.mock import MagicMock, patch

import pytest

from dlt_saga.pipelines.executor import PipelineConfigError, execute_pipeline

pytestmark = pytest.mark.unit


def _config():
    cfg = MagicMock()
    cfg.pipeline_group = "grp"
    cfg.identifier = "configs/grp/p.yml"
    cfg.adapter = None
    return cfg


def _execute(pipeline_class=None, resolve_error=None):
    with (
        patch(
            "dlt_saga.pipelines.executor.get_pipeline_class",
            return_value=pipeline_class,
            side_effect=resolve_error,
        ),
        patch(
            "dlt_saga.pipelines.executor.ConfigSource.prepare_for_execution",
            return_value={},
        ),
    ):
        return execute_pipeline(_config())


def test_adapter_resolution_valueerror_is_a_config_error():
    with pytest.raises(PipelineConfigError, match="unknown adapter"):
        _execute(resolve_error=ValueError("unknown adapter"))


def test_construction_valueerror_is_a_config_error():
    pipeline_class = MagicMock(side_effect=ValueError("base_url is required"))
    with pytest.raises(PipelineConfigError, match="base_url is required") as exc:
        _execute(pipeline_class)
    assert isinstance(exc.value.__cause__, ValueError)


def test_valueerror_from_the_run_propagates_unchanged():
    error = ValueError("API request failed with status 401")
    pipeline_class = MagicMock()
    pipeline_class.return_value.run.side_effect = error
    with pytest.raises(ValueError) as exc:
        _execute(pipeline_class)
    assert exc.value is error
    assert not isinstance(exc.value, PipelineConfigError)


def test_other_construction_errors_propagate_unchanged():
    pipeline_class = MagicMock(side_effect=RuntimeError("auth failed"))
    with pytest.raises(RuntimeError, match="auth failed"):
        _execute(pipeline_class)
