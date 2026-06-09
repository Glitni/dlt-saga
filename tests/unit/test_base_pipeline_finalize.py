"""Unit tests for BasePipeline._sync_destination_table_options."""

from unittest.mock import MagicMock

import pytest


def _make_pipeline(destination, dataset_name="ds"):
    """Construct a BasePipeline shell without running __init__."""
    from dlt_saga.pipelines.base_pipeline import BasePipeline

    pipeline = object.__new__(BasePipeline)
    pipeline.destination = destination
    # _sync_destination_table_options reads dataset off self.pipeline.dataset_name.
    pipeline.pipeline = MagicMock()
    pipeline.pipeline.dataset_name = dataset_name
    return pipeline


@pytest.mark.unit
class TestSyncDestinationTableOptions:
    def test_calls_destination_for_each_loaded_table(self):
        dest = MagicMock()
        pipeline = _make_pipeline(dest, dataset_name="my_dataset")
        pipeline._sync_destination_table_options(["table_a", "table_b"])
        assert dest.sync_table_options.call_count == 2
        dest.sync_table_options.assert_any_call("my_dataset", "table_a")
        dest.sync_table_options.assert_any_call("my_dataset", "table_b")

    def test_noop_for_empty_table_list(self):
        dest = MagicMock()
        pipeline = _make_pipeline(dest)
        pipeline._sync_destination_table_options([])
        dest.sync_table_options.assert_not_called()

    def test_swallows_errors_per_table(self):
        # A failure on one table must not skip subsequent tables or fail the run.
        dest = MagicMock()
        dest.sync_table_options.side_effect = [RuntimeError("boom"), None]
        pipeline = _make_pipeline(dest)
        # Should not raise.
        pipeline._sync_destination_table_options(["a", "b"])
        assert dest.sync_table_options.call_count == 2
