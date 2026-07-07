"""BasePipeline.run() records the run's extraction-start as each load record's
started_at, not dlt's load-package start.

This is the shared fix for the change-detection race in the sheets / sharepoint /
filesystem sources: they skip extraction when the source's modification time is
<= MAX(started_at). If started_at were the load-package start (after extraction),
a source modified *during* extract/load would have mtime < started_at and be
skipped forever. Recording the extraction-start (captured before the source is
read) closes that window — and makes the reported duration span extraction→load
rather than the load step alone.
"""

import logging
import time
from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

import pytest

from dlt_saga.pipelines.base_pipeline import BasePipeline


def _make_pipeline(captured):
    """A BasePipeline shell whose extract/process/finalize steps are stubbed so
    run()'s started_at stamping can be observed in isolation."""
    pipeline = object.__new__(BasePipeline)
    pipeline.logger = logging.getLogger("test")

    def fake_extract():
        # Extraction takes measurable time, so the load-package start recorded
        # below is strictly later than the run's extraction-start.
        time.sleep(0.01)
        return [("resource", "description")]

    def fake_process(resource, description):
        # Mimic dlt: load-package started_at is captured AFTER extraction.
        load_started = datetime.now(timezone.utc)
        captured["load_started_at"] = load_started
        load_info = {
            "started_at": load_started,
            "finished_at": load_started,
            "row_counts": {"t": 1},
        }
        return load_info, ["t"]

    def fake_finalize(all_load_info, loaded_tables):
        # Snapshot what would be persisted to _saga_load_info.
        captured["saved"] = [dict(li) for li in all_load_info]
        return 0.0

    pipeline.extract_data = fake_extract
    pipeline._process_resource_data = fake_process
    pipeline._finalize_pipeline_run = fake_finalize
    pipeline._add_timing_breakdown = lambda *a, **k: None
    return pipeline


@pytest.mark.unit
class TestRunStartedAt:
    def test_started_at_is_extraction_start_not_load_start(self):
        captured = {}
        pipeline = _make_pipeline(captured)

        with patch(
            "dlt_saga.utility.cli.context.get_execution_context",
            return_value=MagicMock(update_access=False),
        ):
            pipeline.run()

        saved = captured["saved"][0]
        # started_at is the run's extraction-start — strictly before the
        # load-package start (which is what dlt reports, after extraction).
        assert isinstance(saved["started_at"], datetime)
        assert saved["started_at"] < captured["load_started_at"]
        # finished_at is left as the load finish (conventional end of a load
        # record), so duration now spans extraction→load.
        assert saved["finished_at"] == captured["load_started_at"]
