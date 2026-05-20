"""Unit tests for TargetWriter merge-config construction.

Focused on pinning the dict shape we hand to dlt's ``write_disposition`` hint
for each merge strategy, so future dlt API changes are caught here rather than
in integration.
"""

from unittest.mock import MagicMock

import pytest

from dlt_saga.pipelines.target.config import MergeStrategy, TargetConfig
from dlt_saga.pipelines.target.writer import TargetWriter


def _make_writer(**overrides) -> TargetWriter:
    config = TargetConfig(**overrides)
    return TargetWriter(config)


@pytest.mark.unit
class TestBuildMergeConfigInsertOnly:
    def test_insert_only_merge_config_includes_primary_key(self):
        writer = _make_writer(
            write_disposition="merge",
            merge_strategy="insert-only",
            primary_key="id",
        )
        merge_config = writer._build_merge_config()
        assert merge_config == {
            "disposition": "merge",
            "strategy": "insert-only",
            "primary_key": ["id"],
        }

    def test_insert_only_merge_config_with_composite_key(self):
        writer = _make_writer(
            write_disposition="merge",
            merge_strategy="insert-only",
            primary_key=["org_id", "event_id"],
        )
        merge_config = writer._build_merge_config()
        assert merge_config["primary_key"] == ["org_id", "event_id"]
        assert merge_config["strategy"] == "insert-only"

    def test_apply_hints_passes_insert_only_to_dlt(self):
        writer = _make_writer(
            write_disposition="merge",
            merge_strategy="insert-only",
            primary_key="id",
        )
        data = MagicMock()
        writer.apply_hints(data)

        # The merge config dict must be the first apply_hints call.
        first_call = data.apply_hints.call_args_list[0]
        assert first_call.kwargs["write_disposition"] == {
            "disposition": "merge",
            "strategy": "insert-only",
            "primary_key": ["id"],
        }


@pytest.mark.unit
class TestBuildMergeConfigOtherStrategies:
    """Regression coverage so we don't accidentally rewrite the dict shape for
    the pre-existing strategies."""

    def test_delete_insert_includes_primary_key(self):
        writer = _make_writer(
            write_disposition="merge",
            merge_strategy=MergeStrategy.DELETE_INSERT,
            primary_key="id",
        )
        merge_config = writer._build_merge_config()
        assert merge_config["strategy"] == "delete-insert"
        assert merge_config["primary_key"] == ["id"]

    def test_upsert_includes_primary_key(self):
        writer = _make_writer(
            write_disposition="merge",
            merge_strategy=MergeStrategy.UPSERT,
            primary_key="id",
        )
        merge_config = writer._build_merge_config()
        assert merge_config["strategy"] == "upsert"
        assert merge_config["primary_key"] == ["id"]

    def test_scd2_includes_validity_column_names(self):
        writer = _make_writer(
            write_disposition="merge",
            merge_strategy=MergeStrategy.SCD2,
            primary_key="id",
        )
        merge_config = writer._build_merge_config()
        assert merge_config["strategy"] == "scd2"
        assert merge_config["validity_column_names"] == [
            "_dlt_valid_from",
            "_dlt_valid_to",
        ]
