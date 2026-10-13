"""Unit tests for the historized table's physical layout.

Covers:
- the create-time clustering default (``[valid_to, valid_from]``) and when it
  is withheld (explicit config, destination without clustering, Databricks
  Iceberg)
- the SQL builder forwarding the resolved clustering and emitting no
  partition DDL unless ``partition_column`` is set
- the post-run layout reconcile: explicit ``cluster_columns`` enforced in
  place, explicit ``partition_column`` mismatches reported, unset keys left
  alone, Databricks' partition/cluster exclusivity respected, failures
  downgraded to a warning
"""

import logging
from unittest.mock import MagicMock

import pytest

from dlt_saga.destinations.base import TableLayout
from dlt_saga.historize.config import HistorizeConfig
from dlt_saga.historize.runner import HistorizeRunner


def _runner(config_overrides=None, *, supports_clustering=True, full_refresh=False):
    dest = MagicMock()
    dest.supports_clustering.return_value = supports_clustering
    dest.supports_clustering_reconcile.return_value = True
    dest.supports_clustering_on_partitioned_table.return_value = True

    runner = object.__new__(HistorizeRunner)
    runner.logger = logging.getLogger(__name__)
    runner.pipeline_name = "grp__pipe"
    runner.database = "proj"
    runner.target_schema = "ds"
    runner.target_table_name = "pipe_historized"
    runner.full_refresh = full_refresh
    runner.config = HistorizeConfig(primary_key=["id"], **(config_overrides or {}))
    runner.destination = dest
    return runner, dest


@pytest.mark.unit
class TestCreateClusterColumns:
    def test_default_is_validity_columns(self):
        runner, _ = _runner()
        assert runner._create_cluster_columns() == ["_dlt_valid_to", "_dlt_valid_from"]

    def test_default_follows_renamed_columns(self):
        runner, _ = _runner(
            {"valid_from_column": "gyldig_fra", "valid_to_column": "gyldig_til"}
        )
        assert runner._create_cluster_columns() == ["gyldig_til", "gyldig_fra"]

    def test_explicit_wins(self):
        runner, _ = _runner({"cluster_columns": ["id"]})
        assert runner._create_cluster_columns() == ["id"]

    def test_explicit_empty_means_unclustered(self):
        runner, _ = _runner({"cluster_columns": []})
        assert runner._create_cluster_columns() is None

    def test_withheld_without_clustering_support(self):
        runner, _ = _runner(supports_clustering=False)
        assert runner._create_cluster_columns() is None

    def test_withheld_on_databricks_iceberg(self):
        runner, dest = _runner({"table_format": "iceberg"})
        type(dest).__name__ = "DatabricksDestination"
        assert runner._create_cluster_columns() is None

    def test_applied_on_databricks_delta(self):
        runner, dest = _runner({"table_format": "delta"})
        type(dest).__name__ = "DatabricksDestination"
        assert runner._create_cluster_columns() == ["_dlt_valid_to", "_dlt_valid_from"]


@pytest.mark.unit
class TestCreateTableSqlLayout:
    def _builder(self, config_overrides=None):
        from dlt_saga.historize.sql import HistorizeSqlBuilder

        dest = MagicMock()
        dest.quote_identifier.side_effect = lambda s: f"`{s}`"
        dest.type_name.side_effect = lambda t: t.upper()
        dest.is_pseudo_column.return_value = False
        captured = {}

        def _capture(**kw):
            captured["hints"] = kw["hints"]
            return "CREATE"

        dest.build_historize_create_table_sql.side_effect = _capture
        builder = HistorizeSqlBuilder(
            config=HistorizeConfig(primary_key=["id"], **(config_overrides or {})),
            destination=dest,
            source_table_id="proj.ds.src",
            target_table_id="proj.ds.tgt",
            primary_key=["id"],
            source_database="proj",
            source_schema="ds",
            source_table="src",
            target_table_name="tgt",
            target_schema="ds",
        )
        return builder, captured

    def test_no_partition_by_default(self):
        builder, captured = self._builder()
        builder.build_create_target_table_sql(["a"])
        assert captured["hints"].partition_column is None

    def test_explicit_partition_forwarded(self):
        builder, captured = self._builder({"partition_column": "snapshot_dt"})
        builder.build_create_target_table_sql(["a"])
        assert captured["hints"].partition_column == "snapshot_dt"

    def test_resolved_clustering_forwarded(self):
        builder, captured = self._builder({"cluster_columns": ["ignored"]})
        builder.build_create_target_table_sql(["a"], cluster_columns=["x", "y"])
        # The builder takes the runner's resolved value, never the raw config.
        assert captured["hints"].cluster_columns == ["x", "y"]

    def test_no_clustering_when_none_resolved(self):
        builder, captured = self._builder()
        builder.build_create_target_table_sql(["a"], cluster_columns=None)
        assert captured["hints"].cluster_columns is None


@pytest.mark.unit
class TestReconcileLayout:
    def test_unset_keys_never_read_the_table(self):
        runner, dest = _runner()
        runner._reconcile_layout()
        dest.get_table_layout.assert_not_called()
        dest.set_clustering_columns.assert_not_called()

    def test_skipped_after_full_refresh(self):
        runner, dest = _runner({"cluster_columns": ["id"]}, full_refresh=True)
        runner._reconcile_layout()
        dest.get_table_layout.assert_not_called()

    def test_skipped_without_reconcile_support(self):
        runner, dest = _runner({"cluster_columns": ["id"]})
        dest.supports_clustering_reconcile.return_value = False
        runner._reconcile_layout()
        dest.get_table_layout.assert_not_called()

    def test_absent_table_is_noop(self):
        runner, dest = _runner({"cluster_columns": ["id"]})
        dest.get_table_layout.return_value = None
        runner._reconcile_layout()
        dest.set_clustering_columns.assert_not_called()

    def test_matching_clustering_not_written(self):
        runner, dest = _runner({"cluster_columns": ["ID"]})
        dest.get_table_layout.return_value = TableLayout(cluster_columns=["id"])
        runner._reconcile_layout()
        dest.set_clustering_columns.assert_not_called()

    def test_drifted_clustering_written(self):
        runner, dest = _runner({"cluster_columns": ["_dlt_valid_to", "id"]})
        dest.get_table_layout.return_value = TableLayout(cluster_columns=["id"])
        runner._reconcile_layout()
        dest.set_clustering_columns.assert_called_once_with(
            "ds", "pipe_historized", ["_dlt_valid_to", "id"]
        )

    def test_explicit_empty_removes_clustering(self):
        runner, dest = _runner({"cluster_columns": []})
        dest.get_table_layout.return_value = TableLayout(cluster_columns=["id"])
        runner._reconcile_layout()
        dest.set_clustering_columns.assert_called_once_with("ds", "pipe_historized", [])

    def test_unset_clustering_leaves_table_alone(self):
        # Only an explicit partition_column is checked; the table's clustering
        # (whatever it is) is not touched by the create-time default.
        runner, dest = _runner({"partition_column": "snapshot_dt"})
        dest.get_table_layout.return_value = TableLayout(
            partition_column="snapshot_dt", cluster_columns=[]
        )
        runner._reconcile_layout()
        dest.set_clustering_columns.assert_not_called()

    def test_partitioned_table_without_in_place_support_warns(self, caplog):
        runner, dest = _runner({"cluster_columns": ["id"]})
        dest.supports_clustering_on_partitioned_table.return_value = False
        dest.get_table_layout.return_value = TableLayout(
            partition_column="_dlt_valid_from", cluster_columns=[]
        )
        with caplog.at_level(logging.WARNING):
            runner._reconcile_layout()
        dest.set_clustering_columns.assert_not_called()
        assert "--full-refresh" in caplog.text

    def test_partitioned_table_with_in_place_support_written(self):
        runner, dest = _runner({"cluster_columns": ["id"]})
        dest.get_table_layout.return_value = TableLayout(
            partition_column="_dlt_valid_from", cluster_columns=[]
        )
        runner._reconcile_layout()
        dest.set_clustering_columns.assert_called_once()

    def test_partition_mismatch_warns(self, caplog):
        runner, dest = _runner({"partition_column": "snapshot_dt"})
        dest.get_table_layout.return_value = TableLayout(
            partition_column="_dlt_valid_from", cluster_columns=[]
        )
        with caplog.at_level(logging.WARNING):
            runner._reconcile_layout()
        assert "partitioned by '_dlt_valid_from'" in caplog.text
        assert "--full-refresh" in caplog.text

    def test_unpartitioned_table_with_explicit_partition_warns(self, caplog):
        runner, dest = _runner({"partition_column": "snapshot_dt"})
        dest.get_table_layout.return_value = TableLayout()
        with caplog.at_level(logging.WARNING):
            runner._reconcile_layout()
        assert "partitioned by nothing" in caplog.text

    def test_partition_match_is_case_insensitive(self, caplog):
        runner, dest = _runner({"partition_column": "Snapshot_Dt"})
        dest.get_table_layout.return_value = TableLayout(partition_column="snapshot_dt")
        with caplog.at_level(logging.WARNING):
            runner._reconcile_layout()
        assert caplog.text == ""

    def test_partition_dropped_for_clustering_is_not_a_mismatch(self, caplog):
        # Databricks creates the table clustered-only when both are set.
        runner, dest = _runner(
            {"partition_column": "snapshot_dt", "cluster_columns": ["id"]}
        )
        dest.supports_clustering_on_partitioned_table.return_value = False
        dest.get_table_layout.return_value = TableLayout(cluster_columns=["id"])
        with caplog.at_level(logging.WARNING):
            runner._reconcile_layout()
        assert caplog.text == ""

    def test_failure_is_a_warning(self, caplog):
        runner, dest = _runner({"cluster_columns": ["id"]})
        dest.get_table_layout.side_effect = RuntimeError("boom")
        with caplog.at_level(logging.WARNING):
            runner._reconcile_layout()  # must not raise
        assert "Failed to reconcile layout" in caplog.text
