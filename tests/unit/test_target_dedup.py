"""Deduplication and hard-delete config reaching dlt.

``deduplicate`` was once accepted and never read, so ``deduplicate: false``
changed nothing; and ``dedup_sort_column`` / ``hard_delete_column`` replaced
the column's whole ``columns`` entry, dropping its configured type and
description. These tests pin the hints dlt actually receives.
"""

import dlt
import pytest

from dlt_saga.pipelines.target.config import TargetConfig
from dlt_saga.pipelines.target.writer import TargetWriter


def _config(**overrides) -> TargetConfig:
    return TargetConfig(
        **{"table_name": "t", "schema_name": "s", "primary_key": ["id"], **overrides}
    )


def _table_schema(config: TargetConfig) -> dict:
    resource = dlt.resource([{"id": 1}], name="t")
    TargetWriter(config).apply_hints(resource)
    return resource.compute_table_schema()


@pytest.mark.unit
class TestDeduplicate:
    @pytest.mark.parametrize("strategy", [None, "delete-insert"])
    def test_false_skips_dlt_deduplication(self, strategy):
        config = _config(
            write_disposition="merge", merge_strategy=strategy, deduplicate=False
        )
        # dlt's flag is inverted: "deduplicated" declares the data unique.
        assert _table_schema(config).get("x-stage-data-deduplicated") is True

    @pytest.mark.parametrize("strategy", [None, "delete-insert"])
    def test_default_leaves_dlt_deduplicating(self, strategy):
        config = _config(write_disposition="merge", merge_strategy=strategy)
        assert not _table_schema(config).get("x-stage-data-deduplicated")

    def test_bare_merge_keeps_the_destination_default_strategy(self):
        config = _config(write_disposition="merge", deduplicate=False)
        assert "x-merge-strategy" not in _table_schema(config)

    @pytest.mark.parametrize("strategy", ["upsert", "scd2", "insert-only"])
    def test_false_rejected_for_other_merge_strategies(self, strategy):
        with pytest.raises(ValueError, match=f"merge_strategy='{strategy}'"):
            _config(
                write_disposition="merge", merge_strategy=strategy, deduplicate=False
            )

    @pytest.mark.parametrize("disposition", ["append", "replace", "append+historize"])
    def test_false_rejected_outside_merge(self, disposition):
        with pytest.raises(ValueError, match="deduplicate: false is not supported"):
            _config(write_disposition=disposition, deduplicate=False)

    def test_false_allowed_with_historize_suffix(self):
        _config(write_disposition="merge+historize", deduplicate=False)


@pytest.mark.unit
class TestColumnHintsMergeIntoColumnConfig:
    _COLUMNS = {"updated_at": {"data_type": "timestamp", "description": "Changed"}}

    def test_dedup_sort_keeps_the_columns_config(self):
        config = _config(
            write_disposition="merge",
            columns={k: dict(v) for k, v in self._COLUMNS.items()},
            dedup_sort="desc",
            dedup_sort_column="updated_at",
        )
        assert config.columns["updated_at"] == {
            "data_type": "timestamp",
            "description": "Changed",
            "dedup_sort": "desc",
        }

    def test_hard_delete_keeps_the_columns_config(self):
        config = _config(
            write_disposition="merge",
            columns={"deleted": {"data_type": "bool", "description": "Gone"}},
            hard_delete_column="deleted",
        )
        assert config.columns["deleted"] == {
            "data_type": "bool",
            "description": "Gone",
            "hard_delete": True,
        }

    def test_hint_on_unconfigured_column(self):
        config = _config(write_disposition="merge", hard_delete_column="deleted")
        assert config.columns["deleted"] == {"hard_delete": True}

    def test_merged_hints_reach_dlt(self):
        config = _config(
            write_disposition="merge",
            columns={k: dict(v) for k, v in self._COLUMNS.items()},
            dedup_sort="desc",
            dedup_sort_column="updated_at",
        )
        column = _table_schema(config)["columns"]["updated_at"]
        assert column["data_type"] == "timestamp"
        assert column["dedup_sort"] == "desc"
