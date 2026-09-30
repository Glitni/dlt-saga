"""An emptied target is refilled on the next run, against real DuckDB.

Reproduces #496 end to end. The incident: a `replace` pipeline's source stopped
matching, the run "succeeded" with 0 rows and replaced the target with an empty
table. The source was fixed — and every later run logged "no files modified
since last load" and skipped extraction, leaving the table empty until someone
ran `--force`.

The unit tests pin the decision logic against mocks. This one proves the whole
chain: a real load writes the watermark, a real emptying happens, and the next
run over an *unchanged* source does the work anyway.
"""

import dlt
import duckdb
import pytest

from dlt_saga.pipelines.base_pipeline import BasePipeline
from dlt_saga.testing import run_pipeline_test

_SCHEMA = "skip_test"
_TABLE = "orders"

_ROWS = {"n": 3}


class _OrdersPipeline(BasePipeline):
    """Yields rows, and reports its source as never having changed.

    The source is deliberately static: every run after the first would skip on
    change detection alone, which is exactly the condition #496 is about.
    """

    def _source_unchanged(self) -> bool:
        return self._get_last_load_with_data(self.table_name) is not None

    def _should_skip_extraction(self) -> bool:
        if not self._source_unchanged():
            return False
        return self._confirm_skip("source unchanged since last load")

    def extract_data(self):
        if self._should_skip_extraction():
            return []

        table = self.table_name

        @dlt.resource(name=table, write_disposition="replace")
        def rows():
            for i in range(_ROWS["n"]):
                yield {"id": i, "val": f"v{i}"}

        return [(rows(), "orders")]


def _config(db_path):
    return {
        "schema_name": _SCHEMA,
        "table_name": _TABLE,
        "pipeline_name": f"skip__{_TABLE}",
        "write_disposition": "replace",
        "database_path": db_path,
    }


def _run(db_path):
    return run_pipeline_test(
        _OrdersPipeline,
        config_dict=_config(db_path),
        schema_name=_SCHEMA,
        database_path=db_path,
    )


def _row_count(db_path):
    conn = duckdb.connect(db_path)
    try:
        return conn.execute(f"SELECT COUNT(*) FROM {_SCHEMA}.{_TABLE}").fetchone()[0]
    finally:
        conn.close()


def _empty_the_target(db_path):
    """Simulate the incident's aftermath without needing the incident itself."""
    conn = duckdb.connect(db_path)
    try:
        conn.execute(f"DELETE FROM {_SCHEMA}.{_TABLE}")
    finally:
        conn.close()


@pytest.fixture
def db_path(tmp_path):
    _ROWS["n"] = 3
    return str(tmp_path / "skip.duckdb")


@pytest.mark.integration
class TestEmptiedTargetIsRefilled:
    def test_unchanged_source_still_skips_when_the_target_is_intact(self, db_path):
        """The optimisation must survive the fix — this is the common path."""
        assert _run(db_path).success
        assert _row_count(db_path) == 3

        # Nothing changed upstream and the target still holds its rows.
        _ROWS["n"] = 99  # would be visible if extraction ran
        assert _run(db_path).success

        assert _row_count(db_path) == 3, "extraction ran when it should have skipped"

    def test_emptied_target_is_refilled_on_the_next_run(self, db_path):
        """#496: before the fix this stayed empty until --force."""
        assert _run(db_path).success
        assert _row_count(db_path) == 3

        _empty_the_target(db_path)
        assert _row_count(db_path) == 0

        # Source is unchanged — change detection alone would skip forever.
        result = _run(db_path)

        assert result.success, result.error
        assert _row_count(db_path) == 3, "the emptied target was never refilled"

    def test_dropped_target_fails_loudly_instead_of_skipping_silently(self, db_path):
        """A dropped table is the same divergence by another route, but only
        half of it is this fix's to solve.

        The skip is correctly abandoned — that is what this change does. The
        load then fails, because dlt's own schema state still believes the table
        exists and emits `DELETE FROM ... WHERE 1=1` against it; clearing that
        needs `--full-refresh`. Recovering from an external drop was never
        automatic and still isn't. What changed is that the pipeline now says so
        instead of reporting success over a table that isn't there.
        """
        assert _run(db_path).success

        conn = duckdb.connect(db_path)
        try:
            conn.execute(f"DROP TABLE {_SCHEMA}.{_TABLE}")
        finally:
            conn.close()

        result = _run(db_path)

        assert not result.success
        # Extraction was attempted rather than skipped: the error comes from the
        # load, not from a run that quietly decided there was nothing to do.
        assert "step=load" in result.error

    def test_recovery_needs_no_force_flag(self, db_path):
        """`--force` was the only way out before; it must not be the way out now."""
        from dlt_saga.utility.cli.context import get_execution_context

        assert _run(db_path).success
        _empty_the_target(db_path)

        _run(db_path)

        # The run that recovered did so without force being set anywhere.
        assert not getattr(get_execution_context(), "force", False)
        assert _row_count(db_path) == 3
