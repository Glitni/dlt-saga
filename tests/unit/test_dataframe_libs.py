"""Unit tests for preloading the dataframe libraries dlt sniffs for."""

import importlib
import sys
from unittest.mock import MagicMock, patch

import pytest

from dlt_saga.utility.dataframe_libs import DATAFRAME_LIBS, preload_dataframe_libs

pytestmark = pytest.mark.unit

# The class dlt reads off each module in ``dlt.common.libs``.
_SNIFFED_CLASS = {"pyarrow": "Table", "pandas": "DataFrame", "polars": "DataFrame"}


def test_preloads_every_library_dlt_sniffs_for():
    preload_dataframe_libs()

    assert set(DATAFRAME_LIBS) == set(_SNIFFED_CLASS)
    for name, cls in _SNIFFED_CLASS.items():
        assert hasattr(sys.modules[name], cls), name


def test_missing_library_is_skipped():
    real_import = importlib.import_module

    def _import(name):
        if name == "polars":
            raise ImportError("No module named 'polars'")
        return real_import(name)

    with patch("dlt_saga.utility.dataframe_libs.importlib.import_module", _import):
        preload_dataframe_libs()


def test_session_preloads_before_the_ingest_pool_starts():
    from dlt_saga.session import Session

    order = []
    executor = MagicMock()
    executor.__enter__.return_value = executor
    submit = MagicMock(side_effect=lambda *a, **k: order.append("submit") or {})
    with (
        patch(
            "dlt_saga.session.preload_dataframe_libs",
            side_effect=lambda: order.append("preload"),
        ),
        patch("dlt_saga.session.ThreadPoolExecutor", return_value=executor),
        patch("dlt_saga.session.submit_with_group_caps", submit),
        patch("dlt_saga.session.as_completed", return_value=[]),
    ):
        session = Session.__new__(Session)
        session._execute_pipelines_tracked([MagicMock()], workers=2)

    assert order == ["preload", "submit"]


def test_worker_preloads_before_the_ingest_pool_starts():
    import dlt_saga.utility.cli.run_modes as cli

    order = []
    config = MagicMock(ingest_enabled=True)
    with (
        patch.object(
            cli, "preload_dataframe_libs", side_effect=lambda: order.append("preload")
        ),
        patch.object(
            cli,
            "_execute_worker_parallel",
            side_effect=lambda *a, **k: order.append("pool") or [],
        ),
    ):
        cli._execute_worker_ingest([config], task_index=1)

    assert order == ["preload", "pool"]
