"""min_rows guard on the native_load adapter.

native_load bypasses dlt entirely, so the pre-load guard built for dlt-based
pipelines does not apply here. Its `replace` is a single
`CREATE OR REPLACE TABLE ... AS SELECT` over the discovered files, which makes a
partial file set silently rewrite the target with less data than it held.

The guard counts the source before writing. These tests pin the two properties
that make that safe: the count covers the *whole* file set rather than a chunk
(chunk 1 is the destructive statement, so a per-chunk check would fire too
late), and a destination that cannot pre-count is refused rather than left
quietly unguarded.
"""

import logging
from unittest.mock import MagicMock, patch

import pytest

from dlt_saga.pipelines.native_load.pipeline import NativeLoadPipeline
from dlt_saga.pipelines.native_load.storage.base import StorageObject
from dlt_saga.pipelines.row_guard import MinRowsNotMetError


def _make_pipeline(*, full_refresh=False, precount=True, **config_overrides):
    config = {
        "pipeline_name": "test__my_table",
        "base_table_name": "my_table",
        "table_name": "test__my_table",
        "schema_name": "my_dataset",
        "source_uri": "gs://bucket/prefix/",
        "file_type": "parquet",
        "write_disposition": "replace",
    }
    config.update(config_overrides)

    dest = MagicMock()
    dest.supports_native_load.return_value = True
    dest.supported_native_load_uri_schemes.return_value = {"gs"}
    dest.supports_native_load_precount.return_value = precount
    dest.type_name.side_effect = lambda t: t.upper()
    dest.native_load_file_name_expr.return_value = "_FILE_NAME"
    dest.parse_filename_timestamp_expr.return_value = "SAFE.PARSE_TIMESTAMP(...)"
    dest.table_exists.return_value = False
    dest.config = MagicMock()
    dest.config.billing_project_id = None
    dest.config.project_id = "my-project"
    dest.config.__class__.__name__ = "BigQueryDestinationConfig"

    context = MagicMock()
    context.get_destination_type.return_value = "bigquery"
    context.update_access = False
    context.full_refresh = full_refresh

    with (
        patch(
            "dlt_saga.utility.cli.context.get_execution_context", return_value=context
        ),
        patch(
            "dlt_saga.destinations.factory.DestinationFactory.create_from_context",
            return_value=dest,
        ),
        patch(
            "dlt_saga.pipelines.native_load.pipeline.get_storage_client",
            return_value=MagicMock(),
        ),
        patch(
            "dlt_saga.pipelines.native_load.pipeline.NativeLoadStateManager",
            return_value=MagicMock(),
        ),
    ):
        p = NativeLoadPipeline(config)

    p.destination = dest
    p.context = context
    p.state_manager = MagicMock()
    p.storage_client = MagicMock()
    return p


def _files(n, cursors=1):
    """n files spread over `cursors` cursor groups, as discovery returns them."""
    out: dict = {}
    for i in range(n):
        cursor = f"c{i % cursors}"
        out.setdefault(cursor, []).append(
            StorageObject(
                name=f"prefix/f{i}.parquet",
                full_uri=f"gs://bucket/prefix/f{i}.parquet",
                size=100,
                generation=1,
                updated=None,
            )
        )
    return out


@pytest.mark.unit
class TestMinRowsValidation:
    def test_unset_by_default(self):
        assert _make_pipeline()._min_rows is None

    def test_positive_accepted(self):
        assert _make_pipeline(min_rows=500)._min_rows == 500

    @pytest.mark.parametrize("value", [0, -1])
    def test_non_positive_rejected(self, value):
        with pytest.raises(ValueError, match="min_rows must be >= 1"):
            _make_pipeline(min_rows=value)

    @pytest.mark.parametrize("value", ["many", True, 1.5])
    def test_non_integer_rejected(self, value):
        with pytest.raises(ValueError, match="min_rows must be a positive integer"):
            _make_pipeline(min_rows=value)

    def test_full_refresh_warns_that_guard_is_not_enforced(self, caplog):
        """The target is dropped during init, before anything can be counted."""
        with caplog.at_level(logging.WARNING):
            _make_pipeline(min_rows=10, full_refresh=True)

        assert "not enforced under --full-refresh" in caplog.text


@pytest.mark.unit
class TestGuardEnforcement:
    def test_no_count_when_unconfigured(self):
        """An unguarded pipeline must not pay for an extra pass over the data."""
        p = _make_pipeline()

        p._enforce_min_rows(_files(3))

        p.destination.native_load_count_rows.assert_not_called()

    def test_short_source_is_refused(self):
        p = _make_pipeline(min_rows=1000)
        p.destination.native_load_count_rows.return_value = 12

        with pytest.raises(MinRowsNotMetError) as exc:
            p._enforce_min_rows(_files(3))

        assert "12 row(s)" in str(exc.value)
        assert "min_rows=1000" in str(exc.value)
        assert "is unchanged" in str(exc.value)

    def test_zero_row_source_is_refused(self):
        p = _make_pipeline(min_rows=1)
        p.destination.native_load_count_rows.return_value = 0

        with pytest.raises(MinRowsNotMetError):
            p._enforce_min_rows(_files(2))

    def test_sufficient_source_passes(self):
        p = _make_pipeline(min_rows=10)
        p.destination.native_load_count_rows.return_value = 10

        p._enforce_min_rows(_files(2))  # exactly at the threshold

    def test_all_counting_precedes_any_loading(self):
        """The property that matters. Counting is chunked (one external table
        over every URI would blow the per-statement source-URI limit that
        load_batch_size exists to respect), but every chunk is counted before
        any chunk is loaded — chunk 1 is the CREATE OR REPLACE, so a check
        interleaved with loading would fire after the target was rewritten.
        """
        p = _make_pipeline(min_rows=1000)
        p.destination.native_load_count_rows.return_value = 1
        p.native_config.load_batch_size = 2  # 5 chunks over 10 files

        with pytest.raises(MinRowsNotMetError):
            p._enforce_min_rows(_files(10, cursors=3))

        assert p.destination.native_load_count_rows.call_count == 5
        p.destination.native_load_chunk.assert_not_called()

    def test_counting_is_chunked_like_the_load(self):
        p = _make_pipeline(min_rows=1000)
        p.destination.native_load_count_rows.return_value = 1
        p.native_config.load_batch_size = 4  # 10 files -> 4 + 4 + 2

        with pytest.raises(MinRowsNotMetError):
            p._enforce_min_rows(_files(10))

        sizes = [
            len(call[0][0].source_uris)
            for call in p.destination.native_load_count_rows.call_args_list
        ]
        assert sizes == [4, 4, 2]
        # Every discovered file is counted exactly once.
        counted = [
            uri
            for call in p.destination.native_load_count_rows.call_args_list
            for uri in call[0][0].source_uris
        ]
        assert len(counted) == len(set(counted)) == 10

    def test_counting_stops_once_the_floor_is_cleared(self):
        """A floor needs no exact total — a healthy run over a large source
        pays for one chunk, not a full second pass.
        """
        p = _make_pipeline(min_rows=10)
        p.destination.native_load_count_rows.return_value = 50
        p.native_config.load_batch_size = 2  # would be 5 chunks

        p._enforce_min_rows(_files(10))

        assert p.destination.native_load_count_rows.call_count == 1

    def test_short_source_is_counted_in_full_before_failing(self):
        """The reported number must cover every file, or it would understate
        how short the source actually was.
        """
        p = _make_pipeline(min_rows=100)
        p.destination.native_load_count_rows.return_value = 3
        p.native_config.load_batch_size = 2  # 5 chunks x 3 rows = 15

        with pytest.raises(MinRowsNotMetError) as exc:
            p._enforce_min_rows(_files(10))

        assert p.destination.native_load_count_rows.call_count == 5
        assert "15 row(s)" in str(exc.value)
        assert "10 file(s)" in str(exc.value)

    def test_count_spec_carries_the_load_filters(self):
        """A count that ignored filters would overstate what the load writes."""
        p = _make_pipeline(min_rows=1, filters=[{"column": "region", "value": "eu"}])
        p.destination.native_load_count_rows.return_value = 5

        p._enforce_min_rows(_files(2))

        spec = p.destination.native_load_count_rows.call_args[0][0]
        assert spec.filters == p._filters
        assert spec.filters

    def test_count_spec_matches_the_load_spec(self):
        """Both paths go through _build_spec, so the count reads what the load
        would read — schema, file type, format options and all.
        """
        p = _make_pipeline(min_rows=1)
        p.destination.native_load_count_rows.return_value = 5
        uris = ["gs://bucket/prefix/f0.parquet", "gs://bucket/prefix/f1.parquet"]

        p._enforce_min_rows(_files(2))
        count_spec = p.destination.native_load_count_rows.call_args[0][0]
        load_spec = p._build_spec(uris, "chunk 1/1")

        assert count_spec.source_uris == load_spec.source_uris
        assert count_spec.target_schema == load_spec.target_schema
        assert count_spec.target_table == load_spec.target_table
        assert count_spec.file_type == load_spec.file_type
        assert count_spec.autodetect_schema == load_spec.autodetect_schema
        assert count_spec.external_schema == load_spec.external_schema
        assert count_spec.write_disposition == load_spec.write_disposition

    def test_guard_time_is_recorded_as_its_own_phase(self):
        p = _make_pipeline(min_rows=1)
        p.destination.native_load_count_rows.return_value = 5

        p._enforce_min_rows(_files(1))

        assert "guard" in p._phase_timings

    def test_no_guard_phase_when_unconfigured(self):
        """Keeps the timing breakdown free of a phase that never ran."""
        p = _make_pipeline()

        p._enforce_min_rows(_files(1))

        assert "guard" not in p._phase_timings


@pytest.mark.unit
class TestUnsupportedDestination:
    def test_configured_guard_on_a_destination_that_cannot_count_is_an_error(self):
        """Silently skipping would leave the pipeline unguarded while its config
        says otherwise — the one outcome worse than not offering the guard.
        """
        p = _make_pipeline(min_rows=100, precount=False)

        with pytest.raises(ValueError, match="cannot count"):
            p._enforce_min_rows(_files(2))

        p.destination.native_load_count_rows.assert_not_called()

    def test_unconfigured_guard_is_unaffected(self):
        p = _make_pipeline(precount=False)

        p._enforce_min_rows(_files(2))  # no guard configured, nothing to enforce


@pytest.mark.unit
class TestFailureReporting:
    def test_guard_failure_is_not_a_config_error(self):
        """`session.py` reads a ValueError from a pipeline as a pre-run config
        error and skips recording it as a run outcome — which would hide a
        tripped guard from `saga report` and `--select "state:failed"`.
        """
        assert not issubclass(MinRowsNotMetError, ValueError)

    def test_guard_failure_logs_no_traceback(self, caplog):
        """The message is self-explanatory; a traceback is noise. Mirrors the
        dlt-based path in BasePipeline.run.
        """
        p = _make_pipeline(min_rows=100)
        p.context.update_access = False
        p._ensure_staging_schema = MagicMock()
        p._sweep_orphan_ext_tables = MagicMock()
        p._discover_new_files = MagicMock(return_value=_files(2))
        p.destination.native_load_count_rows.return_value = 1

        with caplog.at_level(logging.ERROR):
            with pytest.raises(MinRowsNotMetError):
                p.run()

        assert "Native load failed" not in caplog.text

    def test_load_is_never_reached(self):
        """The guard runs before any chunk is loaded."""
        p = _make_pipeline(min_rows=100)
        p._ensure_staging_schema = MagicMock()
        p._sweep_orphan_ext_tables = MagicMock()
        p._discover_new_files = MagicMock(return_value=_files(2))
        p._load_files = MagicMock()
        p.destination.native_load_count_rows.return_value = 1

        with pytest.raises(MinRowsNotMetError):
            p.run()

        p._load_files.assert_not_called()
        p.destination.native_load_chunk.assert_not_called()
