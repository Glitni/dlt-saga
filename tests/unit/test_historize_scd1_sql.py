"""Unit tests for scd1 SQL shape that DuckDB integration tests can't catch."""

import pytest

from dlt_saga.historize.config import HistorizeConfig
from dlt_saga.historize.sql import HistorizeSqlBuilder
from tests.unit.test_historize_filters import _stub_destination


def _builder(**historize):
    config = HistorizeConfig(
        strategy="scd1",
        primary_key=["id"],
        snapshot_column="snapshot_date",
        **historize,
    )
    return HistorizeSqlBuilder(
        config=config,
        destination=_stub_destination(),
        source_table_id="proj.ds.src",
        target_table_id="proj.ds.tgt",
        primary_key=["id"],
        source_database="proj",
        source_schema="ds",
        source_table="src",
        target_table_name="tgt",
        target_schema="ds",
    )


@pytest.mark.unit
class TestScd1RankColumn:
    def test_batch_rank_column_reads_the_source_not_a_sibling_alias(self):
        # BigQuery can't reference an alias from the same SELECT (DuckDB can).
        sql = _builder(track_deletions=True).build_scd1_sql(["plan"])
        batch = sql[sql.index("batch AS (") : sql.index("FROM proj.ds.src")]

        assert "`snapshot_date` AS _saga_rank_at" in batch
        assert "`_dlt_valid_from` AS _saga_rank_at" not in batch

    def test_no_rank_column_without_deletions(self):
        sql = _builder().build_scd1_sql(["plan"])

        assert "_saga_rank_at" not in sql
