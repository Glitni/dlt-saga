"""``deduplicate`` changes what a merge loads, against real DuckDB.

The key was once accepted and never read, so this asserts on the rows a
delete-insert merge leaves behind for a batch that repeats a primary key.
"""

import dlt
import duckdb
import pytest

from dlt_saga.pipelines.base_pipeline import BasePipeline
from dlt_saga.testing import run_pipeline_test

_SCHEMA = "dedup_test"
_TABLE = "orders"


class _OrdersPipeline(BasePipeline):
    """One batch carrying id 1 twice."""

    def extract_data(self):
        @dlt.resource(name=self.table_name)
        def rows():
            yield {"id": 1, "val": "a"}
            yield {"id": 1, "val": "b"}
            yield {"id": 2, "val": "c"}

        return [(rows(), "orders")]


def _rows_loaded(db_path, **overrides):
    result = run_pipeline_test(
        _OrdersPipeline,
        config_dict={
            "schema_name": _SCHEMA,
            "table_name": _TABLE,
            "pipeline_name": f"dedup__{_TABLE}",
            "write_disposition": "merge",
            "primary_key": ["id"],
            "database_path": db_path,
            **overrides,
        },
        schema_name=_SCHEMA,
        database_path=db_path,
    )
    assert result.success, result.error
    conn = duckdb.connect(db_path)
    try:
        return conn.execute(f"SELECT COUNT(*) FROM {_SCHEMA}.{_TABLE}").fetchone()[0]
    finally:
        conn.close()


@pytest.fixture
def db_path(tmp_path):
    return str(tmp_path / "dedup.duckdb")


@pytest.mark.integration
class TestDeduplicate:
    def test_default_collapses_repeated_keys(self, db_path):
        assert _rows_loaded(db_path) == 2

    @pytest.mark.parametrize("strategy", [None, "delete-insert"])
    def test_false_loads_every_row(self, db_path, strategy):
        overrides = {"deduplicate": False}
        if strategy:
            overrides["merge_strategy"] = strategy
        assert _rows_loaded(db_path, **overrides) == 3
