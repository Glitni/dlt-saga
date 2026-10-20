"""A pipeline's ``replace_strategy`` reaches dlt, against real DuckDB.

dlt reads the replace strategy from destination config only; a strategy in a
resource's ``write_disposition`` hint is dropped without a trace. The config
key once travelled that way and changed nothing, so these tests assert on what
the load actually did rather than on the hint saga sets.
"""

import dlt
import duckdb
import pytest

from dlt_saga.pipelines.base_pipeline import BasePipeline
from dlt_saga.testing import run_pipeline_test

_SCHEMA = "strategy_test"
_TABLE = "orders"


class _OrdersPipeline(BasePipeline):
    def extract_data(self):
        @dlt.resource(name=self.table_name, write_disposition="replace")
        def rows():
            for i in range(3):
                yield {"id": i}

        return [(rows(), "orders")]


def _run(db_path, **overrides):
    return run_pipeline_test(
        _OrdersPipeline,
        config_dict={
            "schema_name": _SCHEMA,
            "table_name": _TABLE,
            "pipeline_name": f"strategy__{_TABLE}",
            "write_disposition": "replace",
            "database_path": db_path,
            **overrides,
        },
        schema_name=_SCHEMA,
        database_path=db_path,
    )


def _staged(db_path):
    """Whether the load wrote the table through the staging dataset."""
    conn = duckdb.connect(db_path)
    try:
        rows = conn.execute(
            "SELECT 1 FROM information_schema.tables"
            " WHERE table_schema = ? AND table_name = ?",
            [f"{_SCHEMA}_staging", _TABLE],
        )
        return rows.fetchone() is not None
    finally:
        conn.close()


@pytest.fixture
def db_path(tmp_path):
    return str(tmp_path / "strategy.duckdb")


@pytest.mark.integration
class TestReplaceStrategyIsApplied:
    def test_default_loads_through_staging(self, db_path):
        assert _run(db_path).success
        assert _staged(db_path)

    def test_truncate_and_insert_skips_staging(self, db_path):
        result = _run(db_path, replace_strategy="truncate-and-insert")

        assert result.success, result.error
        assert not _staged(db_path), (
            "replace_strategy was ignored: the load went through staging"
        )

    def test_unsupported_strategy_is_a_config_error(self, db_path):
        result = _run(db_path, replace_strategy="staging-optimized")

        assert not result.success
        assert "replace_strategy 'staging-optimized' is not supported" in result.error
