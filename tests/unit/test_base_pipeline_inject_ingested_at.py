"""Unit tests for BasePipeline._inject_ingested_at gating across dispositions.

Regression guard for replace+historize: the snapshot column (_dlt_ingested_at,
the historize default) must be stamped at ingest time for `replace` too, not
just `append` — otherwise historization fails with "Unrecognized name:
_dlt_ingested_at". merge/scd2 stays excluded (dlt manages its own columns).
"""

from unittest.mock import MagicMock

import pytest


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


def _make_pipeline(write_disposition, *, supports_clustering=True):
    """Construct a BasePipeline shell without running __init__."""
    from dlt_saga.pipelines.base_pipeline import BasePipeline

    pipeline = object.__new__(BasePipeline)
    pipeline.config_dict = {}

    target_config = MagicMock()
    target_config.write_disposition = write_disposition
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
        ["append", "replace", "append+historize", "replace+historize"],
    )
    def test_column_injected_for_append_and_replace(self, disposition):
        pipeline = _make_pipeline(disposition)
        resource = FakeResource()

        result = pipeline._inject_ingested_at(resource)

        assert result is resource
        assert resource.maps, f"{disposition!r} should register an add_map transform"
        row = resource.apply({"company_id": 1})
        assert "_dlt_ingested_at" in row, (
            f"{disposition!r} must stamp _dlt_ingested_at at ingest time"
        )

    @pytest.mark.parametrize("disposition", ["merge", "merge+historize"])
    def test_column_not_injected_for_merge(self, disposition):
        # merge/scd2: dlt manages its own columns; an injected timestamp would
        # trigger false change detection.
        pipeline = _make_pipeline(disposition)
        resource = FakeResource()

        result = pipeline._inject_ingested_at(resource)

        assert result is resource
        assert resource.maps == [], f"{disposition!r} must not inject _dlt_ingested_at"

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
