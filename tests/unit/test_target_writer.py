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


def _hint_kwargs(data) -> dict:
    """Merge all apply_hints kwargs from a mock into one dict for assertions."""
    merged: dict = {}
    for call in data.apply_hints.call_args_list:
        merged.update(call.kwargs)
    return merged


@pytest.mark.unit
class TestMergeWithoutStrategy:
    """A bare `write_disposition: merge` (no merge_strategy) is valid — dlt
    defaults to delete-insert. merge_key/primary_key must still be applied,
    otherwise merge silently degrades to keyless append and rows duplicate."""

    def test_bare_merge_applies_merge_key(self):
        writer = _make_writer(write_disposition="merge", merge_key="id")
        data = MagicMock()
        writer.apply_hints(data)

        hints = _hint_kwargs(data)
        assert hints["write_disposition"] == "merge"
        assert hints["merge_key"] == ["id"]

    def test_bare_merge_applies_primary_key(self):
        writer = _make_writer(write_disposition="merge", primary_key="id")
        data = MagicMock()
        writer.apply_hints(data)

        hints = _hint_kwargs(data)
        assert hints["write_disposition"] == "merge"
        assert hints["primary_key"] == ["id"]

    def test_bare_merge_applies_both_keys(self):
        writer = _make_writer(
            write_disposition="merge", merge_key="mk", primary_key="pk"
        )
        data = MagicMock()
        writer.apply_hints(data)

        hints = _hint_kwargs(data)
        assert hints["merge_key"] == ["mk"]
        assert hints["primary_key"] == ["pk"]

    def test_bare_merge_with_historize_suffix_applies_merge_key(self):
        writer = _make_writer(write_disposition="merge+historize", merge_key="id")
        data = MagicMock()
        writer.apply_hints(data)

        hints = _hint_kwargs(data)
        assert hints["write_disposition"] == "merge"
        assert hints["merge_key"] == ["id"]

    def test_append_still_ignores_merge_key(self):
        """merge_key is meaningless for append — it must not leak into append."""
        writer = _make_writer(write_disposition="append", merge_key="id")
        data = MagicMock()
        writer.apply_hints(data)

        hints = _hint_kwargs(data)
        assert hints["write_disposition"] == "append"
        assert "merge_key" not in hints


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
