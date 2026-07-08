"""BasePipeline must not treat dlt system tables as loaded user tables.

`loaded_tables` feeds access grants, table-option sync, and description
reconcile. It's built from dlt's `row_counts`, which includes `_dlt_loads`,
`_dlt_pipeline_state`, `_dlt_version` — granting end users SELECT on those (or
documenting them) is wrong. Real nested child tables (`<table>__<child>`) are
NOT `_dlt_`-prefixed and must be kept. The list is also deduped so each real
table is processed once.
"""

import logging
from unittest.mock import MagicMock, patch

import pytest

from dlt_saga.pipelines.base_pipeline import BasePipeline


@pytest.mark.unit
class TestProcessResourceExcludesDltTables:
    def _pipeline(self, row_counts):
        p = object.__new__(BasePipeline)
        p.logger = logging.getLogger("test")
        p._inject_ingested_at = lambda r: r
        p._apply_filters = lambda r: r
        p._apply_row_limit = lambda r: r
        p._build_destination_hints = lambda d: {}
        p._capture_trace_timings = lambda li: None
        p.destination = MagicMock()
        p.destination.apply_hints = lambda r, **k: r
        run_result = MagicMock()
        run_result.asdict.return_value = {"row_counts": row_counts}
        p.destination.run_pipeline.return_value = run_result
        p.target_writer = MagicMock()
        p.target_writer.apply_hints = lambda r: r
        p.pipeline = MagicMock()
        return p

    def test_dlt_system_tables_excluded_child_tables_kept(self):
        p = self._pipeline(
            {
                "orders": 5,
                "orders__items": 2,  # real nested child table — keep
                "_dlt_loads": 1,
                "_dlt_pipeline_state": 1,
                "_dlt_version": 1,
            }
        )
        _, tables = p._process_resource_data(MagicMock(), "desc")
        assert set(tables) == {"orders", "orders__items"}


@pytest.mark.unit
class TestRunDedupesLoadedTables:
    def test_duplicate_tables_across_resources_deduped(self):
        captured = {}

        p = object.__new__(BasePipeline)
        p.logger = logging.getLogger("test")
        p.extract_data = lambda: [("r1", "d"), ("r2", "d")]
        p._process_resource_data = lambda resource, description: (
            {"row_counts": {}, "started_at": None, "finished_at": None},
            ["orders", "orders"],
        )
        p._finalize_pipeline_run = lambda all_load_info, loaded_tables: (
            captured.__setitem__("loaded_tables", loaded_tables) or 0.0
        )
        p._add_timing_breakdown = lambda *a, **k: None

        with patch(
            "dlt_saga.utility.cli.context.get_execution_context",
            return_value=MagicMock(update_access=False),
        ):
            p.run()

        # Four entries in (["orders","orders"] × 2 resources) collapse to one.
        assert captured["loaded_tables"] == ["orders"]
