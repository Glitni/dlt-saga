"""Unit test for Phase 9: DROP before CREATE OR REPLACE in full-refresh."""

from unittest.mock import MagicMock

import pytest


@pytest.mark.unit
class TestCreateTargetTableFullRefresh:
    def _make_runner_with_mocks(self):
        """Build a HistorizeRunner with all deps mocked to avoid DB calls."""
        from dlt_saga.historize.runner import HistorizeRunner

        dest = MagicMock()
        dest.get_full_table_id.side_effect = lambda s, t: f"{s}.{t}"
        dest.type_name.return_value = "TIMESTAMP"
        dest.partition_ddl.return_value = ""
        dest.cluster_ddl.return_value = ""

        # Bypass __init__ entirely to avoid live connections
        runner = object.__new__(HistorizeRunner)
        runner.destination = dest
        runner.schema = "schema"
        runner.target_schema = "schema"
        runner.target_table_id = "schema.hist_table"

        sql_builder = MagicMock()
        sql_builder.build_drop_target_table_sql.return_value = (
            "DROP TABLE IF EXISTS schema.hist_table"
        )
        sql_builder.build_create_target_table_sql.return_value = (
            "CREATE TABLE IF NOT EXISTS schema.hist_table (...)"
        )
        runner.sql_builder = sql_builder

        return runner, dest

    def test_drop_called_before_create_on_replace(self):
        runner, dest = self._make_runner_with_mocks()
        runner._create_target_table(["col1", "col2"], replace=True)

        calls = dest.execute_sql.call_args_list
        assert len(calls) == 2
        drop_sql = calls[0][0][0]
        create_sql = calls[1][0][0]
        assert "DROP TABLE IF EXISTS" in drop_sql
        assert "CREATE" in create_sql

    def test_no_drop_when_not_replace(self):
        runner, dest = self._make_runner_with_mocks()
        runner._create_target_table(["col1"], replace=False)

        calls = dest.execute_sql.call_args_list
        assert len(calls) == 1
        sql = calls[0][0][0]
        assert "DROP" not in sql
        assert "CREATE" in sql
