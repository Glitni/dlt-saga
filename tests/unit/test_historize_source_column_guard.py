"""Unit tests for the pre-flight source-column guard.

snapshot_column / primary_key / merge_key are spliced straight into generated
SQL. When one names a column the source doesn't have, the run must say which
config key is wrong before any query runs — not surface the destination's own
"unrecognized name" error mid-run.

A pseudo-column is accepted as the snapshot column (the SQL builder projects it
under an alias) but not as a primary or merge key, which are written into the
historized table under names the destination reserves.
"""

import logging
from unittest.mock import MagicMock

import pytest

from dlt_saga.historize.config import HistorizeConfig
from dlt_saga.historize.runner import HistorizeRunner


def _make_runner(columns, **config_kwargs) -> HistorizeRunner:
    dest = MagicMock()
    dest.is_pseudo_column.side_effect = lambda name: name.lower().startswith(
        "_partition"
    )

    runner = object.__new__(HistorizeRunner)
    runner.logger = logging.getLogger(__name__)
    runner.pipeline_name = "grp__tbl"
    runner.destination = dest
    runner.schema = "ds"
    runner.source_table_id = "proj.ds.src"
    runner.config = HistorizeConfig(primary_key=["id"], **config_kwargs)
    runner._source_columns_cache = list(columns)
    return runner


@pytest.mark.unit
class TestSourceColumnGuard:
    def test_valid_config_passes(self):
        runner = _make_runner(["id", "snapshot_date", "city"])
        runner.config.snapshot_column = "snapshot_date"
        runner._guard_source_columns()  # must not raise

    def test_case_difference_is_accepted(self):
        # Every supported destination resolves column references
        # case-insensitively, so rejecting these would break working configs.
        runner = _make_runner(["Id", "snapshotAt"])
        runner.config.snapshot_column = "snapshotat"
        runner.config.primary_key = ["ID"]
        runner._guard_source_columns()  # must not raise

    def test_unknown_snapshot_column_names_the_config_key(self):
        runner = _make_runner(["id", "city"])
        runner.config.snapshot_column = "delivery_date"
        with pytest.raises(ValueError) as excinfo:
            runner._guard_source_columns()
        msg = str(excinfo.value)
        assert "snapshot_column: 'delivery_date'" in msg
        assert "Available columns: id, city" in msg

    def test_unknown_primary_key_and_merge_key_reported(self):
        runner = _make_runner(["id", "snapshot_date"], merge_key=["dbinstance"])
        runner.config.snapshot_column = "snapshot_date"
        runner.config.primary_key = ["order_id"]
        msg = str(pytest.raises(ValueError, runner._guard_source_columns).value)
        assert "primary_key: 'order_id'" in msg
        assert "merge_key: 'dbinstance'" in msg

    def test_pseudo_snapshot_column_is_accepted(self):
        # The only timestamp an ingestion-time partitioned table has.
        runner = _make_runner(["id", "customer_state"])
        runner.config.snapshot_column = "_PARTITIONTIME"
        runner._guard_source_columns()  # must not raise

    def test_pseudo_primary_key_is_refused_with_the_reason(self):
        runner = _make_runner(["id", "customer_state"])
        runner.config.snapshot_column = "customer_state"
        runner.config.primary_key = ["_PARTITIONDATE"]
        msg = str(pytest.raises(ValueError, runner._guard_source_columns).value)
        assert "pseudo-column" in msg
        assert "primary_key" in msg

    def test_empty_source_reports_a_missing_table(self):
        runner = _make_runner([])
        msg = str(pytest.raises(ValueError, runner._guard_source_columns).value)
        assert "has no columns" in msg


@pytest.mark.unit
class TestSourceColumnsCaching:
    def test_pseudo_column_needs_no_catalog_lookup(self):
        runner = _make_runner([])
        runner._source_columns_cache = ["id"]
        # Absent from the catalog, but referenceable on a direct source scan.
        assert runner._source_can_reference("_PARTITIONTIME") is True

    def test_catalog_is_read_once_per_run(self):
        runner = _make_runner([])
        runner._source_columns_cache = None
        runner._src_database, runner._src_schema, runner._src_table = "p", "ds", "src"
        runner.destination.execute_sql.return_value = [
            MagicMock(column_name="id"),
            MagicMock(column_name="city"),
        ]

        runner.destination.is_pseudo_column.return_value = False

        assert runner._source_columns() == ["id", "city"]
        assert runner._source_can_reference("CITY") is True
        assert runner._source_can_reference("nope") is False
        runner.destination.execute_sql.assert_called_once()
