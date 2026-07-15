"""Unit tests for clustering reconciliation.

Covers the internal-tables registry, the destination-agnostic
``reconcile_clustering`` orchestrator (stub-self, no warehouse), and the
BigQuery / Databricks primitives (mocked clients). ``saga maintenance`` and
``saga doctor`` wiring is exercised via the shared helpers rather than rendered
CLI output.
"""

from unittest.mock import MagicMock, patch

import pytest

from dlt_saga.destinations.base import Destination
from dlt_saga.utility.internal_tables import (
    clustering_state,
    resolve_internal_log_tables,
)


@pytest.mark.unit
class TestResolveInternalLogTables:
    def test_per_schema_native_and_historize_logs(self):
        tables = resolve_internal_log_tables(["dlt_a", "dlt_b"], None)
        labels = {(t.schema, t.label) for t in tables}
        assert ("dlt_a", "native_load log") in labels
        assert ("dlt_a", "historize log") in labels
        assert ("dlt_b", "native_load log") in labels
        # No orchestration schema → no execution-plan log.
        assert all(t.label != "execution-plan log" for t in tables)

    def test_orchestration_schema_adds_execution_plan_log(self):
        tables = resolve_internal_log_tables(["dlt_a"], "dlt_orchestration")
        plan = [t for t in tables if t.label == "execution-plan log"]
        assert len(plan) == 1
        assert plan[0].schema == "dlt_orchestration"
        assert plan[0].cluster_columns == ["execution_id", "task_index"]

    def test_duplicate_and_empty_schemas_deduped(self):
        tables = resolve_internal_log_tables(["dlt_a", "dlt_a", "", None], None)
        schemas = {t.schema for t in tables}
        assert schemas == {"dlt_a"}

    def test_cluster_columns_match_create_time_constants(self):
        from dlt_saga.historize.state import LOG_CLUSTER_COLUMNS as HIST
        from dlt_saga.pipelines.native_load.state import (
            LOG_CLUSTER_COLUMNS as NATIVE,
        )

        tables = resolve_internal_log_tables(["dlt_a"], None)
        by_label = {t.label: t.cluster_columns for t in tables}
        assert by_label["native_load log"] == NATIVE
        assert by_label["historize log"] == HIST


@pytest.mark.unit
class TestClusteringState:
    def test_absent_when_current_none(self):
        assert clustering_state(None, ["a"]) == "absent"

    def test_ok_case_insensitive(self):
        assert clustering_state(["Pipeline_Name"], ["pipeline_name"]) == "ok"

    def test_drift_on_different_columns(self):
        assert clustering_state(["a"], ["b"]) == "drift"

    def test_drift_is_order_sensitive(self):
        # Clustering column order is significant.
        assert clustering_state(["a", "b"], ["b", "a"]) == "drift"

    def test_drift_when_unclustered(self):
        assert clustering_state([], ["a"]) == "drift"


def _reconcile(fake, current, cluster_columns):
    fake.supports_clustering_reconcile.return_value = True
    fake.get_clustering_columns.return_value = current
    return Destination.reconcile_clustering(fake, "ds", "t", cluster_columns)


@pytest.mark.unit
class TestReconcileClusteringOrchestrator:
    def test_unsupported_is_noop(self):
        fake = MagicMock()
        fake.supports_clustering_reconcile.return_value = False
        assert Destination.reconcile_clustering(fake, "ds", "t", ["a"]) == "unsupported"
        fake.get_clustering_columns.assert_not_called()
        fake.set_clustering_columns.assert_not_called()

    def test_absent_table_not_written(self):
        fake = MagicMock()
        assert _reconcile(fake, None, ["a"]) == "absent"
        fake.set_clustering_columns.assert_not_called()

    def test_unchanged_not_written(self):
        fake = MagicMock()
        assert _reconcile(fake, ["a", "b"], ["a", "b"]) == "unchanged"
        fake.set_clustering_columns.assert_not_called()

    def test_unchanged_case_insensitive(self):
        fake = MagicMock()
        assert _reconcile(fake, ["A"], ["a"]) == "unchanged"
        fake.set_clustering_columns.assert_not_called()

    def test_drift_written(self):
        fake = MagicMock()
        assert _reconcile(fake, [], ["a"]) == "reconciled"
        fake.set_clustering_columns.assert_called_once_with("ds", "t", ["a"])


@pytest.mark.unit
class TestBigQueryClusteringReconcile:
    def _dest(self, client):
        from dlt_saga.destinations.bigquery.destination import BigQueryDestination

        dest = MagicMock(spec=BigQueryDestination)
        dest._description_client_and_ref.return_value = (client, "proj.ds.t")
        return dest, BigQueryDestination

    def test_get_clustering_columns_absent(self):
        from google.cloud.exceptions import NotFound

        client = MagicMock()
        client.get_table.side_effect = NotFound("nope")
        dest, cls = self._dest(client)
        assert cls.get_clustering_columns(dest, "ds", "t") is None

    def test_get_clustering_columns_unclustered(self):
        client = MagicMock()
        client.get_table.return_value = MagicMock(clustering_fields=None)
        dest, cls = self._dest(client)
        assert cls.get_clustering_columns(dest, "ds", "t") == []

    def test_reconcile_absent(self):
        from google.cloud.exceptions import NotFound

        client = MagicMock()
        client.get_table.side_effect = NotFound("nope")
        dest, cls = self._dest(client)
        assert cls.reconcile_clustering(dest, "ds", "t", ["a"]) == "absent"
        client.update_table.assert_not_called()

    def test_reconcile_unchanged(self):
        client = MagicMock()
        client.get_table.return_value = MagicMock(clustering_fields=["a", "b"])
        dest, cls = self._dest(client)
        assert cls.reconcile_clustering(dest, "ds", "t", ["a", "b"]) == "unchanged"
        client.update_table.assert_not_called()

    def test_reconcile_drift_updates_clustering_fields(self):
        client = MagicMock()
        tbl = MagicMock(clustering_fields=None)
        client.get_table.return_value = tbl
        dest, cls = self._dest(client)
        assert (
            cls.reconcile_clustering(dest, "ds", "t", ["pipeline_name"]) == "reconciled"
        )
        assert tbl.clustering_fields == ["pipeline_name"]
        client.update_table.assert_called_once_with(tbl, ["clustering_fields"])


@pytest.mark.unit
class TestDatabricksClusteringPrimitives:
    def _dest(self):
        from dlt_saga.destinations.databricks.destination import DatabricksDestination

        dest = MagicMock(spec=DatabricksDestination)
        dest.get_full_table_id.return_value = "cat.ds.t"
        dest.quote_identifier.side_effect = lambda c: f"`{c}`"
        return dest, DatabricksDestination

    def test_get_clustering_columns_returns_list(self):
        dest, cls = self._dest()
        dest.execute_sql.return_value = [MagicMock(clusteringColumns=["pipeline_name"])]
        assert cls.get_clustering_columns(dest, "ds", "t") == ["pipeline_name"]

    def test_get_clustering_columns_unclustered(self):
        dest, cls = self._dest()
        dest.execute_sql.return_value = [MagicMock(clusteringColumns=None)]
        assert cls.get_clustering_columns(dest, "ds", "t") == []

    def test_get_clustering_columns_numpy_array(self):
        # The connector returns clusteringColumns as a numpy array, whose truth
        # value is ambiguous — the parse must not branch on the array itself.
        import numpy as np

        dest, cls = self._dest()
        dest.execute_sql.return_value = [
            MagicMock(clusteringColumns=np.array(["pipeline_name", "cursor_value"]))
        ]
        assert cls.get_clustering_columns(dest, "ds", "t") == [
            "pipeline_name",
            "cursor_value",
        ]

    def test_get_clustering_columns_empty_numpy_array(self):
        import numpy as np

        dest, cls = self._dest()
        dest.execute_sql.return_value = [
            MagicMock(clusteringColumns=np.array([], dtype=object))
        ]
        assert cls.get_clustering_columns(dest, "ds", "t") == []

    def test_get_clustering_columns_missing_table_is_none(self):
        dest, cls = self._dest()
        dest.execute_sql.side_effect = Exception("TABLE_OR_VIEW_NOT_FOUND: cat.ds.t")
        assert cls.get_clustering_columns(dest, "ds", "t") is None

    def test_get_clustering_columns_reraises_other_errors(self):
        dest, cls = self._dest()
        dest.execute_sql.side_effect = Exception("PERMISSION_DENIED")
        with pytest.raises(Exception, match="PERMISSION_DENIED"):
            cls.get_clustering_columns(dest, "ds", "t")

    def test_set_clustering_columns_emits_alter(self):
        dest, cls = self._dest()
        cls.set_clustering_columns(dest, "ds", "t", ["pipeline_name", "cursor_value"])
        sql = dest.execute_sql.call_args[0][0]
        assert (
            sql == "ALTER TABLE cat.ds.t CLUSTER BY (`pipeline_name`, `cursor_value`)"
        )


@pytest.mark.unit
class TestDefaultDestinationNoClusteringReconcile:
    def test_base_default_unsupported(self):
        from tests.unit.test_destinations_base import _ConcreteDestination

        assert _ConcreteDestination().supports_clustering_reconcile() is False


def _table(schema="ds", table="t", cols=None, label="native_load log"):
    from dlt_saga.utility.internal_tables import InternalLogTable

    return InternalLogTable(schema, table, cols or ["pipeline_name"], label)


@pytest.mark.unit
class TestReconcileInternalClustering:
    """The maintenance sweep: apply / dry-run classification.

    Connection lifecycle is the caller's concern, so these operate on an
    already-"connected" mock and never assert connect/close.
    """

    def test_apply_delegates_to_reconcile_clustering(self):
        from dlt_saga.maintenance import reconcile_internal_clustering

        dest = MagicMock()
        dest.reconcile_clustering.return_value = "reconciled"
        counts = reconcile_internal_clustering(dest, [_table()], dry_run=False)
        dest.reconcile_clustering.assert_called_once_with("ds", "t", ["pipeline_name"])
        assert counts["reconciled"] == 1

    def test_dry_run_never_writes_but_counts_drift(self):
        from dlt_saga.maintenance import reconcile_internal_clustering

        dest = MagicMock()
        dest.get_clustering_columns.return_value = []  # exists, unclustered → drift
        counts = reconcile_internal_clustering(dest, [_table()], dry_run=True)
        dest.reconcile_clustering.assert_not_called()
        dest.set_clustering_columns.assert_not_called()
        assert counts["reconciled"] == 1

    def test_dry_run_absent_and_ok_classified(self):
        from dlt_saga.maintenance import reconcile_internal_clustering

        dest = MagicMock()
        dest.get_clustering_columns.side_effect = [None, ["pipeline_name"]]
        counts = reconcile_internal_clustering(dest, [_table(), _table()], dry_run=True)
        assert counts["absent"] == 1
        assert counts["unchanged"] == 1

    def test_does_not_manage_connection(self):
        # Lifecycle belongs to the caller (which shares one connection across
        # checks), so the sweep must not connect or close itself.
        from dlt_saga.maintenance import reconcile_internal_clustering

        dest = MagicMock()
        dest.reconcile_clustering.return_value = "unchanged"
        reconcile_internal_clustering(dest, [_table()], dry_run=False)
        dest.connect.assert_not_called()
        dest.close.assert_not_called()


@pytest.mark.unit
class TestScanClustering:
    """The doctor read-only scan classifies each table on a connected destination."""

    def test_classifies_each_table(self):
        from dlt_saga.maintenance import scan_clustering

        dest = MagicMock()
        dest.get_clustering_columns.side_effect = [None, ["pipeline_name"], []]
        tables = [_table(), _table(), _table()]
        results = scan_clustering(dest, tables)
        assert [state for _, state in results] == ["absent", "ok", "drift"]

    def test_does_not_manage_connection(self):
        from dlt_saga.maintenance import scan_clustering

        dest = MagicMock()
        dest.get_clustering_columns.return_value = []
        scan_clustering(dest, [_table()])
        dest.connect.assert_not_called()
        dest.close.assert_not_called()


@pytest.mark.unit
class TestRunClusteringMaintenance:
    """The shared entry point behind `saga maintenance` and `Session.maintenance`."""

    def _configs(self):
        cfg = MagicMock()
        cfg.schema_name = "dlt_api"
        return {"api": [cfg]}

    def test_unsupported_destination_returns_zero_without_connecting(self):
        from dlt_saga.maintenance import run_clustering_maintenance

        dest = MagicMock()
        dest.supports_clustering_reconcile.return_value = False
        ctx = MagicMock()
        ctx.get_destination_type.return_value = "duckdb"
        with patch(
            "dlt_saga.utility.cli.common.build_destination_from_configs",
            return_value=dest,
        ):
            counts = run_clustering_maintenance(ctx, self._configs(), dry_run=False)
        assert counts == {"absent": 0, "unchanged": 0, "reconciled": 0}
        dest.connect.assert_not_called()

    def test_supported_connects_reconciles_and_closes(self):
        from dlt_saga.maintenance import run_clustering_maintenance

        dest = MagicMock()
        dest.supports_clustering_reconcile.return_value = True
        dest.reconcile_clustering.return_value = "reconciled"
        ctx = MagicMock()
        ctx.get_destination_type.return_value = "bigquery"
        with patch(
            "dlt_saga.utility.cli.common.build_destination_from_configs",
            return_value=dest,
        ):
            counts = run_clustering_maintenance(ctx, self._configs(), dry_run=False)
        dest.connect.assert_called_once()
        dest.close.assert_called_once()
        # At least the native_load + historize logs of the one schema.
        assert counts["reconciled"] >= 2


@pytest.mark.unit
class TestDoctorCheckDestination:
    """doctor connects once (a single impersonation setup) for both the
    connectivity check and the clustering scan."""

    def _run(self, dest, tables, scan_result=None, scan_exc=None):
        from dlt_saga import cli

        emit = MagicMock()
        with (
            patch(
                "dlt_saga.utility.cli.common.build_destination_from_configs",
                return_value=dest,
            ),
            patch(
                "dlt_saga.maintenance.resolve_internal_log_tables_for",
                return_value=tables,
            ),
            patch("dlt_saga.maintenance.scan_clustering") as scan,
        ):
            if scan_exc:
                scan.side_effect = scan_exc
            else:
                scan.return_value = scan_result or []
            # profile_target=None → execute_with_impersonation runs the probe
            # directly, with no impersonation setup.
            ok = cli._doctor_check_destination(
                "bigquery", MagicMock(), {"g": []}, None, False, emit
            )
        return ok, emit

    def test_single_connection_for_both_checks(self):
        dest = MagicMock()
        dest.supports_clustering_reconcile.return_value = True
        ok, _ = self._run(dest, [_table()], scan_result=[(_table(), "ok")])
        assert ok is True
        dest.connect.assert_called_once()
        dest.close.assert_called_once()

    def test_scan_error_does_not_fail_connection(self):
        dest = MagicMock()
        dest.supports_clustering_reconcile.return_value = True
        ok, emit = self._run(dest, [_table()], scan_exc=RuntimeError("denied"))
        assert ok is True  # connection is still OK
        dest.close.assert_called_once()
        symbols = [c.args[0] for c in emit.call_args_list]
        assert "!" in symbols  # clustering degraded to an advisory note

    def test_connect_failure_returns_false(self):
        dest = MagicMock()
        dest.supports_clustering_reconcile.return_value = True
        dest.connect.side_effect = RuntimeError("no auth")
        ok, _ = self._run(dest, [_table()])
        assert ok is False

    def test_unsupported_destination_still_checks_connectivity(self):
        dest = MagicMock()
        dest.supports_clustering_reconcile.return_value = False
        ok, _ = self._run(dest, [_table()])
        assert ok is True
        dest.connect.assert_called_once()
