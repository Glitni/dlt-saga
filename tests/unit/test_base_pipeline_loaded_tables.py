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
        # No row guard and no empty-replace warning in this fixture — both are
        # covered by tests/unit/test_row_guard.py.
        p.target_writer.config.min_rows = None
        p.target_writer.config.write_disposition = "append"
        p.table_name = "orders"
        p.pipeline_name = "shop__orders"
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


@pytest.mark.unit
class TestLegacyDestinationCompatibility:
    """External destination plugins implement the documented
    ``run_pipeline(pipeline, data)`` signature (Plugin Development guide).
    Passing ``guard=`` to one unconditionally would break every run on that
    destination, guarded or not.
    """

    def _pipeline(self, destination, min_rows=None):
        p = object.__new__(BasePipeline)
        p.logger = logging.getLogger("test")
        p.pipeline_name = "shop__orders"
        p.table_name = "orders"
        p.pipeline = MagicMock()
        p.destination = destination
        p.target_writer = MagicMock()
        p.target_writer.config.min_rows = min_rows
        p.target_writer.config.write_disposition = "replace"
        return p

    def _legacy_destination(self):
        dest = MagicMock()
        # A two-argument override, as the plugin docs describe.
        dest.run_pipeline = lambda pipeline, data: "load-info"
        return dest

    def test_unguarded_run_works_on_a_two_argument_override(self):
        p = self._pipeline(self._legacy_destination())

        assert p._run_with_guard("data") == "load-info"

    def test_guard_is_not_passed_when_unconfigured(self):
        """Even on a modern destination — nothing to enforce, nothing to pass."""
        dest = MagicMock()
        p = self._pipeline(dest)

        p._run_with_guard("data")

        assert "guard" not in dest.run_pipeline.call_args.kwargs

    def test_configured_guard_on_a_legacy_destination_is_a_clear_error(self):
        """Better than the TypeError the caller would otherwise see, and better
        than silently dropping the guard the config asked for.
        """
        p = self._pipeline(self._legacy_destination(), min_rows=100)

        with pytest.raises(ValueError, match="older run_pipeline"):
            p._run_with_guard("data")

    def test_guard_is_passed_to_a_destination_that_accepts_it(self):
        dest = MagicMock()
        dest.run_pipeline = MagicMock(
            side_effect=lambda pipeline, data, guard=None: "load-info"
        )
        p = self._pipeline(dest, min_rows=5)

        p._run_with_guard("data")

        assert dest.run_pipeline.call_args.kwargs["guard"].min_rows == 5

    def test_kwargs_override_is_accepted(self):
        dest = MagicMock()
        dest.run_pipeline = lambda pipeline, data, **kw: "load-info"
        p = self._pipeline(dest, min_rows=5)

        assert p._run_with_guard("data") == "load-info"

    def test_builtin_destinations_all_accept_the_guard(self):
        """Regression: each shipped destination must stay on the new signature."""
        import inspect

        from dlt_saga.destinations.base import Destination
        from dlt_saga.destinations.bigquery.destination import BigQueryDestination
        from dlt_saga.destinations.databricks.destination import DatabricksDestination
        from dlt_saga.destinations.duckdb.destination import DuckDBDestination

        for cls in (
            Destination,
            BigQueryDestination,
            DatabricksDestination,
            DuckDBDestination,
        ):
            params = inspect.signature(cls.run_pipeline).parameters
            assert "guard" in params, f"{cls.__name__}.run_pipeline lost `guard`"
