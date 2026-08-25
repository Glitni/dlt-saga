"""The ``write_disposition`` default must be one value across every layer.

The config layer (selection, ``saga list``, ``ingest_enabled``, ``saga validate``,
reports) and the runtime (the dlt hand-off in ``BasePipeline``, the filesystem
metadata path, native_load) used to derive the default independently — append on
one side, replace on the other — so a config that omitted the key was listed and
validated as an append while dlt overwrote the table every run. These tests pin
the single source of truth and the one place it's materialized.
"""

from types import SimpleNamespace

import pytest

from dlt_saga.pipeline_config.base_config import (
    DEFAULT_WRITE_DISPOSITION,
    ConfigSource,
    PipelineConfig,
    resolve_write_disposition,
)
from dlt_saga.pipelines.target.config import TargetConfig


def _config(**config_dict) -> PipelineConfig:
    return PipelineConfig(
        pipeline_group="filesystem",
        pipeline_name="filesystem__orders",
        table_name="filesystem__orders",
        identifier="configs/filesystem/orders.yml",
        schema_name="dlt_dev",
        config_dict={"base_table_name": "orders", **config_dict},
        enabled=True,
        tags=[],
    )


@pytest.mark.unit
class TestResolveWriteDisposition:
    @pytest.mark.parametrize("value", [{}, {"write_disposition": None}])
    def test_falls_back_to_default(self, value):
        assert resolve_write_disposition(value) == DEFAULT_WRITE_DISPOSITION

    def test_empty_string_falls_back_to_default(self):
        assert resolve_write_disposition({"write_disposition": ""}) == (
            DEFAULT_WRITE_DISPOSITION
        )

    def test_explicit_value_passes_through(self):
        assert (
            resolve_write_disposition({"write_disposition": "replace+historize"})
            == "replace+historize"
        )

    def test_default_is_non_destructive(self):
        """Omitting the key must never rewrite the table."""
        assert DEFAULT_WRITE_DISPOSITION == "append"


@pytest.mark.unit
class TestLayersAgree:
    def test_config_layer_default(self):
        assert _config().raw_write_disposition == DEFAULT_WRITE_DISPOSITION

    def test_target_config_default_matches_config_layer(self):
        """The dlt hand-off default and the config-layer default are one value."""
        assert TargetConfig().write_disposition == DEFAULT_WRITE_DISPOSITION

    def test_prepare_for_execution_materializes_the_resolved_value(self):
        """The runtime reads a value rather than re-deriving a default."""
        prepared = ConfigSource.prepare_for_execution(_config())
        assert prepared["write_disposition"] == DEFAULT_WRITE_DISPOSITION

    def test_prepare_for_execution_preserves_an_explicit_value(self):
        prepared = ConfigSource.prepare_for_execution(
            _config(write_disposition="replace+historize")
        )
        assert prepared["write_disposition"] == "replace+historize"

    def test_filesystem_metadata_injection_agrees_with_the_default(self):
        """File metadata is injected for append — and append is the default."""
        from dlt_saga.pipelines.filesystem.pipeline import FilesystemPipeline

        needs_metadata = FilesystemPipeline._needs_file_metadata(
            SimpleNamespace(config_dict={})
        )
        assert needs_metadata is (DEFAULT_WRITE_DISPOSITION == "append")

    def test_historize_only_detection_unaffected_by_the_default(self):
        assert _config().historize_enabled is False
        assert _config(write_disposition="historize").historize_enabled is True
        assert _config(write_disposition="historize").ingest_enabled is False
