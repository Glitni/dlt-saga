"""Unit tests for BasePipeline._inject_ingested_at gating across dispositions.

Regression guard for replace+historize: the snapshot column (_dlt_ingested_at,
the historize default) must be stamped at ingest time for `replace` too, not
just `append` — otherwise historization fails with "Unrecognized name:
_dlt_ingested_at". Plain `merge` (bare/delete-insert/upsert/insert-only) is also
stamped — upsert by key has no content comparison, so the timestamp is a safe
last-upserted-per-key marker. Only `scd2` stays excluded: it versions by
row-content hash, so a per-run timestamp would open a spurious version each run.
"""

from unittest.mock import MagicMock

import pytest

from dlt_saga.pipelines.target.config import MergeStrategy


class FakeResource:
    """Minimal stand-in for a dlt resource: records add_map transforms."""

    def __init__(self):
        self.maps = []

    def add_map(self, fn):
        self.maps.append(fn)
        return self

    def apply(self, row):
        for fn in self.maps:
            row = fn(row)
        return row


def _make_pipeline(write_disposition, *, merge_strategy=None, supports_clustering=True):
    """Construct a BasePipeline shell without running __init__."""
    from dlt_saga.pipelines.base_pipeline import BasePipeline

    pipeline = object.__new__(BasePipeline)
    pipeline.config_dict = {}

    target_config = MagicMock()
    target_config.write_disposition = write_disposition
    target_config.merge_strategy = merge_strategy
    target_config.cluster_columns = None
    pipeline.target_writer = MagicMock()
    pipeline.target_writer.config = target_config

    pipeline.destination = MagicMock()
    pipeline.destination.supports_clustering.return_value = supports_clustering
    return pipeline


@pytest.mark.unit
class TestInjectIngestedAt:
    @pytest.mark.parametrize(
        "disposition",
        [
            "append",
            "replace",
            "append+historize",
            "replace+historize",
            "merge",
            "merge+historize",
        ],
    )
    def test_column_injected_for_append_replace_and_merge(self, disposition):
        pipeline = _make_pipeline(disposition)
        resource = FakeResource()

        result = pipeline._inject_ingested_at(resource)

        assert result is resource
        assert resource.maps, f"{disposition!r} should register an add_map transform"
        row = resource.apply({"company_id": 1})
        assert "_dlt_ingested_at" in row, (
            f"{disposition!r} must stamp _dlt_ingested_at at ingest time"
        )

    @pytest.mark.parametrize(
        "strategy",
        [
            None,  # bare merge → dlt default (delete-insert)
            MergeStrategy.DELETE_INSERT,
            MergeStrategy.UPSERT,
            MergeStrategy.INSERT_ONLY,
        ],
    )
    def test_non_scd2_merge_strategies_inject(self, strategy):
        # Every merge strategy except scd2 is upsert/insert-by-key with no content
        # comparison, so the run timestamp is safe.
        pipeline = _make_pipeline("merge", merge_strategy=strategy)
        resource = FakeResource()

        pipeline._inject_ingested_at(resource)

        assert resource.maps, (
            f"merge strategy {strategy!r} should stamp _dlt_ingested_at"
        )
        assert "_dlt_ingested_at" in resource.apply({"company_id": 1})

    @pytest.mark.parametrize("disposition", ["merge", "merge+historize"])
    def test_column_not_injected_for_scd2(self, disposition):
        # scd2 versions by row-content hash; a per-run timestamp would open a
        # spurious version every run.
        pipeline = _make_pipeline(disposition, merge_strategy=MergeStrategy.SCD2)
        resource = FakeResource()

        result = pipeline._inject_ingested_at(resource)

        assert result is resource
        assert resource.maps == [], (
            f"{disposition!r} scd2 must not inject _dlt_ingested_at"
        )

    @pytest.mark.parametrize("disposition", ["merge", "merge+historize"])
    def test_merge_does_not_auto_cluster(self, disposition):
        # A merge table's timestamps are scattered per key, not a single snapshot,
        # so auto-clustering by _dlt_ingested_at (append-only) must not fire.
        pipeline = _make_pipeline(disposition)
        pipeline._inject_ingested_at(FakeResource())
        assert pipeline.target_writer.config.cluster_columns is None

    def test_append_auto_clusters_by_ingested_at(self):
        pipeline = _make_pipeline("append")
        pipeline._inject_ingested_at(FakeResource())
        assert pipeline.target_writer.config.cluster_columns == ["_dlt_ingested_at"]

    @pytest.mark.parametrize("disposition", ["replace", "replace+historize"])
    def test_replace_does_not_auto_cluster(self, disposition):
        # A replace table holds one snapshot with a single ~constant timestamp,
        # so clustering by it is useless.
        pipeline = _make_pipeline(disposition)
        pipeline._inject_ingested_at(FakeResource())
        assert pipeline.target_writer.config.cluster_columns is None

    def test_append_respects_explicit_cluster_columns(self):
        pipeline = _make_pipeline("append")
        pipeline.target_writer.config.cluster_columns = ["company_id"]
        pipeline._inject_ingested_at(FakeResource())
        assert pipeline.target_writer.config.cluster_columns == ["company_id"]


@pytest.mark.unit
class TestInjectIngestedAtArrow:
    """The PyArrow branch must stamp one run-level timestamp (not datetime.now()
    per batch) and produce a tz-aware TIMESTAMP consistent with the dict path.
    """

    def _apply(self, disposition="append+historize"):
        pipeline = _make_pipeline(disposition)
        resource = FakeResource()
        pipeline._inject_ingested_at(resource)
        return resource

    def test_all_arrow_batches_share_one_run_timestamp(self):
        # Two separate Tables model two batches from one physical run — they must
        # get the identical _dlt_ingested_at, or a single run splits into several
        # spurious SCD2 snapshots.
        import pyarrow as pa

        resource = self._apply()
        b1 = resource.apply(pa.table({"company_id": [1, 2]}))
        b2 = resource.apply(pa.table({"company_id": [3]}))
        vals = b1.column("_dlt_ingested_at").to_pylist() + (
            b2.column("_dlt_ingested_at").to_pylist()
        )
        assert len(set(vals)) == 1, "all rows across batches must share one timestamp"

    def test_arrow_timestamp_is_tz_aware(self):
        import pyarrow as pa

        resource = self._apply()
        tbl = resource.apply(pa.table({"company_id": [1]}))
        field = tbl.schema.field("_dlt_ingested_at")
        assert pa.types.is_timestamp(field.type)
        assert field.type.tz == "UTC", "arrow snapshot column must be tz-aware UTC"

    def test_arrow_and_dict_paths_agree_within_a_run(self):
        # Cross-path consistency: an Arrow batch and a dict row processed by the
        # same run stamp the same instant (guards the tz/type drift too).
        from datetime import datetime

        import pyarrow as pa

        resource = self._apply()
        row = resource.apply({"company_id": 1})
        tbl = resource.apply(pa.table({"company_id": [1]}))
        dict_ts = datetime.fromisoformat(row["_dlt_ingested_at"])
        arrow_ts = tbl.column("_dlt_ingested_at")[0].as_py()
        assert dict_ts == arrow_ts
