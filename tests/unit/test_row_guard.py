"""Pre-load row-count guard.

The guard exists because dlt reports a short load as a successful one. For
`replace` a zero-row extraction still truncates and swaps, and for scd2 a
*partial* extraction retires every key that failed to arrive — both leave a
green run over damaged data. These tests pin the two behaviours that make the
guard safe: it decides before `load()` is called, and it drops the pending
package so a rejected load cannot be picked up by the next run.
"""

import logging
from unittest.mock import MagicMock

import pytest

from dlt_saga.pipelines.row_guard import (
    MinRowsNotMetError,
    RowGuard,
    build_row_guard,
    resolve_guarded_row_count,
    warn_on_empty_replace,
)


def _pipeline(row_counts):
    """dlt pipeline double whose normalize reports *row_counts*."""
    pipeline = MagicMock()
    normalize_info = MagicMock()
    normalize_info.row_counts = row_counts
    pipeline.normalize.return_value = normalize_info
    pipeline.load.return_value = "load-info"
    return pipeline


@pytest.mark.unit
class TestResolveGuardedRowCount:
    def test_prefers_the_root_table(self):
        counts = {"orders": 5, "orders__items": 99, "_dlt_loads": 1}
        assert resolve_guarded_row_count(counts, "orders") == 5

    def test_sums_non_system_tables_when_root_absent(self):
        counts = {"orders": 5, "orders__items": 2, "_dlt_loads": 1}
        assert resolve_guarded_row_count(counts, "something_else") == 7

    def test_missing_and_empty_counts_are_zero(self):
        assert resolve_guarded_row_count(None, "orders") == 0
        assert resolve_guarded_row_count({}, "orders") == 0
        assert resolve_guarded_row_count({"_dlt_loads": 3}, "orders") == 0

    def test_null_count_is_not_an_error(self):
        assert resolve_guarded_row_count({"orders": None}, "orders") == 0


@pytest.mark.unit
class TestRowGuardRun:
    def test_load_proceeds_when_threshold_met(self):
        pipeline = _pipeline({"orders": 10})
        guard = RowGuard(min_rows=10, table_name="orders")

        assert guard.run(pipeline, "data") == "load-info"
        pipeline.extract.assert_called_once_with("data")
        pipeline.load.assert_called_once()
        pipeline.drop_pending_packages.assert_not_called()

    def test_empty_replace_is_rejected_before_load(self):
        pipeline = _pipeline({"orders": 0})
        guard = RowGuard(
            min_rows=1,
            table_name="orders",
            pipeline_name="shop__orders",
            write_disposition="replace",
        )

        with pytest.raises(MinRowsNotMetError) as exc:
            guard.run(pipeline, "data")

        # The whole point: the destination is never touched.
        pipeline.load.assert_not_called()
        assert "still holds the previous run's data" in str(exc.value)
        assert "shop__orders" in str(exc.value)

    def test_rejected_load_drops_the_pending_package(self):
        """Without this the package is loaded by the *next* run instead."""
        pipeline = _pipeline({"orders": 0})

        with pytest.raises(MinRowsNotMetError):
            RowGuard(min_rows=1, table_name="orders").run(pipeline, "data")

        pipeline.drop_pending_packages.assert_called_once()

    def test_partial_scd2_batch_is_rejected(self):
        """A batch that merely shrank still retires the absent keys."""
        pipeline = _pipeline({"customers": 1})
        guard = RowGuard(min_rows=100, table_name="customers")

        with pytest.raises(MinRowsNotMetError) as exc:
            guard.run(pipeline, "data")

        pipeline.load.assert_not_called()
        assert "extracted 1 row(s)" in str(exc.value)
        assert "min_rows=100" in str(exc.value)

    def test_zero_row_message_points_at_the_source(self):
        pipeline = _pipeline({"orders": 0})
        with pytest.raises(MinRowsNotMetError) as exc:
            RowGuard(min_rows=1, table_name="orders").run(pipeline, "data")
        assert "source moved or a glob stopped matching" in str(exc.value)

    def test_short_but_nonzero_message_omits_source_hint(self):
        pipeline = _pipeline({"orders": 3})
        with pytest.raises(MinRowsNotMetError) as exc:
            RowGuard(min_rows=10, table_name="orders").run(pipeline, "data")
        assert "source moved" not in str(exc.value)

    def test_missing_normalize_row_counts_counts_as_zero(self):
        pipeline = MagicMock()
        pipeline.normalize.return_value = object()  # no row_counts attribute

        with pytest.raises(MinRowsNotMetError):
            RowGuard(min_rows=1, table_name="orders").run(pipeline, "data")


@pytest.mark.unit
class TestErrorClassification:
    def test_not_a_value_error(self):
        """A ValueError from a pipeline is read as a pre-run config error and is
        deliberately *not* recorded as a run outcome (``session.py``), which
        would hide a tripped guard from ``saga report`` and ``state:failed``.
        """
        assert not issubclass(MinRowsNotMetError, ValueError)
        assert issubclass(MinRowsNotMetError, RuntimeError)


@pytest.mark.unit
class TestBuildRowGuard:
    @pytest.mark.parametrize("value", [None, 0])
    def test_unset_threshold_means_no_guard(self, value):
        assert (
            build_row_guard(
                value,
                table_name="orders",
                pipeline_name="shop__orders",
                write_disposition="replace",
            )
            is None
        )

    def test_configured_threshold_builds_a_guard(self):
        guard = build_row_guard(
            5,
            table_name="orders",
            pipeline_name="shop__orders",
            write_disposition="replace",
        )
        assert guard.min_rows == 5
        assert guard.table_name == "orders"
        assert guard.pipeline_name == "shop__orders"


@pytest.mark.unit
class TestWarnOnEmptyReplace:
    def _warn(self, caplog, **kwargs):
        defaults = {
            "write_disposition": "replace",
            "table_name": "orders",
            "min_rows": None,
        }
        defaults.update(kwargs)
        counts = defaults.pop("row_counts", {"orders": 0})
        with caplog.at_level(logging.WARNING):
            return warn_on_empty_replace(counts, **defaults)

    def test_warns_when_replace_emptied_the_table(self, caplog):
        assert self._warn(caplog) is True
        assert "replaced with 0 rows" in caplog.text
        assert "min_rows" in caplog.text

    def test_silent_when_replace_loaded_rows(self, caplog):
        assert self._warn(caplog, row_counts={"orders": 7}) is False
        assert caplog.text == ""

    @pytest.mark.parametrize("disposition", ["append", "merge", "historize"])
    def test_silent_for_non_replace_dispositions(self, caplog, disposition):
        """Other dispositions produce no load package at all when empty."""
        assert self._warn(caplog, write_disposition=disposition) is False
        assert caplog.text == ""

    def test_replace_historize_still_warns(self, caplog):
        assert self._warn(caplog, write_disposition="replace+historize") is True

    def test_silent_when_min_rows_already_guards_it(self, caplog):
        """A configured guard would have aborted the load; no post-hoc warning."""
        assert self._warn(caplog, min_rows=1) is False
        assert caplog.text == ""
