"""Change-detection skips are confirmed against the target's actual state.

Skipping extraction is sound only while "the source hasn't changed" implies
"the target is still correct". An emptied or dropped target breaks that
implication and nothing notices: the run reports success, the table stays
wrong, and the next run reaches the same conclusion. That is how an emptied
`replace` target stayed empty until someone ran `--force` (#496).

These pin the confirmation itself, and that all three adapters carrying this
skip logic go through it — the trap was never filesystem-specific.
"""

import logging
from unittest.mock import MagicMock

import pytest

from dlt_saga.pipelines.base_pipeline import BasePipeline


def _pipeline(has_rows):
    """A BasePipeline stub whose destination reports *has_rows* for the target."""
    p = object.__new__(BasePipeline)
    p.logger = logging.getLogger("test")
    p.base_table_name = "orders"
    p.table_name = "shop__orders"
    p.pipeline = MagicMock()
    p.pipeline.dataset_name = "dlt_prod"
    p.destination = MagicMock()
    p.destination.table_has_rows.return_value = has_rows
    return p


@pytest.mark.unit
class TestConfirmSkip:
    def test_populated_target_skips(self):
        """The normal case: nothing changed upstream, the data is already there."""
        p = _pipeline(has_rows=True)

        assert p._confirm_skip("no files modified since last load") is True

    def test_empty_target_extracts_anyway(self):
        """#496: the source is unchanged but the target no longer holds it."""
        p = _pipeline(has_rows=False)

        assert p._confirm_skip("no files modified since last load") is False

    def test_unknown_target_state_extracts_anyway(self):
        """Redundant work is recoverable; a silent skip over a wrong target is not."""
        p = _pipeline(has_rows=None)

        assert p._confirm_skip("no files modified since last load") is False

    def test_skip_log_carries_the_reason(self, caplog):
        p = _pipeline(has_rows=True)

        with caplog.at_level(logging.INFO):
            p._confirm_skip("sheet not modified since last load")

        assert "Skipping extraction" in caplog.text
        assert "sheet not modified since last load" in caplog.text

    def test_override_log_says_why_it_ignored_the_skip(self, caplog):
        p = _pipeline(has_rows=False)

        with caplog.at_level(logging.INFO):
            p._confirm_skip("no files modified since last load")

        assert "despite no source changes" in caplog.text
        assert "empty or missing" in caplog.text

    def test_unknown_state_is_described_as_such(self, caplog):
        p = _pipeline(has_rows=None)

        with caplog.at_level(logging.INFO):
            p._confirm_skip("no files modified since last load")

        assert "unknown state" in caplog.text

    def test_checks_the_pipeline_s_own_target(self):
        p = _pipeline(has_rows=True)

        p._confirm_skip("unchanged")

        p.destination.table_has_rows.assert_called_once_with("dlt_prod", "shop__orders")


@pytest.mark.unit
class TestAllSkippingAdaptersAreCovered:
    """The trap lives in the shared skip pattern, not in one adapter."""

    ADAPTERS = [
        "dlt_saga.pipelines.filesystem.pipeline",
        "dlt_saga.pipelines.google_sheets.pipeline",
        "dlt_saga.pipelines.sharepoint.pipeline",
    ]

    @pytest.mark.parametrize("module_name", ADAPTERS)
    def test_adapter_confirms_its_skip(self, module_name):
        import importlib
        import inspect

        source = inspect.getsource(importlib.import_module(module_name))
        assert "_should_skip_extraction" in source, "fixture is stale"
        assert "_confirm_skip(" in source, (
            f"{module_name} decides to skip without confirming the target state"
        )

    @pytest.mark.parametrize("module_name", ADAPTERS)
    def test_adapter_has_no_unconfirmed_skip(self, module_name):
        """A bare `return True` inside the skip decision would bypass the check."""
        import importlib
        import inspect

        module = importlib.import_module(module_name)
        cls = next(
            obj
            for _, obj in inspect.getmembers(module, inspect.isclass)
            if issubclass(obj, BasePipeline)
            and obj is not BasePipeline
            and "_should_skip_extraction" in obj.__dict__
        )
        body = inspect.getsource(cls.__dict__["_should_skip_extraction"])
        # `return False` (extract) is always fine; `return True` must be the
        # confirmation's answer, never a decision taken on its own.
        assert "return True" not in body, (
            f"{module_name} skips unconditionally somewhere in _should_skip_extraction"
        )


@pytest.mark.unit
class TestTableHasRowsContract:
    """Base implementation — destinations may override with something cheaper."""

    def _destination(self, exists=True, rows=None, raises=None):
        from dlt_saga.destinations.base import Destination

        dest = MagicMock(spec=Destination)
        dest.table_exists.return_value = exists
        dest.get_full_table_id.return_value = "proj.ds.tbl"
        if raises is not None:
            dest.execute_sql.side_effect = raises
        else:
            dest.execute_sql.return_value = rows if rows is not None else []
        return dest

    def _call(self, dest):
        from dlt_saga.destinations.base import Destination

        return Destination.table_has_rows(dest, "ds", "tbl")

    def test_rows_present(self):
        assert self._call(self._destination(rows=[(1,)])) is True

    def test_table_empty(self):
        assert self._call(self._destination(rows=[])) is False

    def test_missing_table_is_not_populated(self):
        dest = self._destination(exists=False)

        assert self._call(dest) is False
        dest.execute_sql.assert_not_called()

    def test_error_is_unknown_not_false(self):
        """None and False both extract, but they log differently — an error must
        not masquerade as a confidently empty table.
        """
        assert self._call(self._destination(raises=RuntimeError("denied"))) is None

    def test_probe_is_bounded(self):
        """A full COUNT(*) on a large table would be a real cost on a path that
        runs every time change detection short-circuits.
        """
        dest = self._destination(rows=[(1,)])

        self._call(dest)

        sql = dest.execute_sql.call_args[0][0]
        assert "LIMIT 1" in sql
        assert "COUNT" not in sql.upper()


@pytest.mark.unit
class TestBigQueryTableHasRows:
    """BigQuery answers from table metadata — no query job, so no cost."""

    def _destination(self, num_rows=None, raises=None):
        from google.cloud.exceptions import NotFound

        from dlt_saga.destinations.bigquery.destination import BigQueryDestination

        dest = MagicMock(spec=BigQueryDestination)
        dest.config = MagicMock()
        dest.config.project_id = "proj"
        client = MagicMock()
        if raises is not None:
            client.get_table.side_effect = raises
        else:
            table = MagicMock()
            table.num_rows = num_rows
            client.get_table.return_value = table
        dest._client.return_value = client
        return dest, NotFound

    def _call(self, dest):
        from dlt_saga.destinations.bigquery.destination import BigQueryDestination

        return BigQueryDestination.table_has_rows(dest, "ds", "tbl")

    def test_populated(self):
        dest, _ = self._destination(num_rows=42)
        assert self._call(dest) is True

    def test_empty(self):
        dest, _ = self._destination(num_rows=0)
        assert self._call(dest) is False

    def test_null_row_count_is_empty(self):
        dest, _ = self._destination(num_rows=None)
        assert self._call(dest) is False

    def test_missing_table(self):
        dest, not_found = self._destination(raises=None)
        dest._client.return_value.get_table.side_effect = not_found("gone")
        assert self._call(dest) is False

    def test_other_errors_are_unknown(self):
        dest, _ = self._destination(raises=RuntimeError("permission denied"))
        assert self._call(dest) is None

    def test_runs_no_query_job(self):
        dest, _ = self._destination(num_rows=1)

        self._call(dest)

        dest.execute_sql.assert_not_called()
