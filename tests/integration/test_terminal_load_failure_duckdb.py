"""A terminal load failure must not wedge the pipeline, against real DuckDB.

Since dlt 1.30 a terminally failed job leaves its package pending, and the next
``run()`` loads pending packages and returns without extracting. Without
``load.auto_abort_on_terminal_error`` every later run replays the stale package
instead of the current source — after the cause is fixed it "succeeds" with the
old data, and while it isn't, it fails forever on the same package.
"""

import dlt
import duckdb
import pytest

from dlt_saga.defaults import apply_dlt_defaults
from dlt_saga.pipelines.base_pipeline import BasePipeline
from dlt_saga.testing import run_pipeline_test

_SCHEMA = "terminal_test"
_TABLE = "orders"

_ROWS = {"n": 3, "null_val": False}


class _OrdersPipeline(BasePipeline):
    """Yields ``_ROWS["n"]`` rows into the configured table."""

    def extract_data(self):
        table = self.table_name

        @dlt.resource(name=table, write_disposition="replace")
        def rows():
            for i in range(_ROWS["n"]):
                yield {"id": i, "val": None if _ROWS["null_val"] else f"v{i}"}

        return [(rows(), "orders")]


def _run(db_path):
    return run_pipeline_test(
        _OrdersPipeline,
        config_dict={
            "schema_name": _SCHEMA,
            "table_name": _TABLE,
            "pipeline_name": f"terminal__{_TABLE}",
            "write_disposition": "replace",
            "database_path": db_path,
        },
        schema_name=_SCHEMA,
        database_path=db_path,
    )


def _execute(db_path, sql):
    conn = duckdb.connect(db_path)
    try:
        return conn.execute(sql).fetchall()
    finally:
        conn.close()


@pytest.fixture
def db_path(tmp_path, monkeypatch):
    """Fresh DuckDB file, with dlt's working dir under tmp_path too.

    The pipelines dir lives under ``~/.dlt``; pointing home at tmp_path keeps a
    package left behind by one run of this test out of the next.

    ``run_pipeline_test`` doesn't build a Session, so the saga dlt defaults the
    fix lives in are registered explicitly, as a real run would.
    """
    apply_dlt_defaults()
    monkeypatch.setenv("HOME", str(tmp_path))
    monkeypatch.setenv("USERPROFILE", str(tmp_path))
    _ROWS.update(n=3, null_val=False)
    return str(tmp_path / "terminal.duckdb")


@pytest.mark.integration
class TestTerminalLoadFailureDoesNotWedge:
    def test_run_after_failure_extracts_current_source(self, db_path):
        assert _run(db_path).success

        # A constraint dlt doesn't know about makes the load job itself fail
        # terminally — the class of failure a retry can never fix.
        _execute(
            db_path, f"ALTER TABLE {_SCHEMA}.{_TABLE} ALTER COLUMN val SET NOT NULL"
        )
        _ROWS["null_val"] = True
        failed = _run(db_path)
        assert not failed.success
        assert "step=load" in failed.error

        # Cause fixed, source changed. A wedged pipeline would replay the stale
        # package here and exit without extracting.
        _execute(
            db_path, f"ALTER TABLE {_SCHEMA}.{_TABLE} ALTER COLUMN val DROP NOT NULL"
        )
        _ROWS["null_val"] = False
        _ROWS["n"] = 7
        recovered = _run(db_path)

        assert recovered.success, recovered.error
        count = _execute(db_path, f"SELECT COUNT(*) FROM {_SCHEMA}.{_TABLE}")[0][0]
        assert count == 7, "the stale package was loaded instead of the current source"

    def test_run_after_setup_failure_extracts_current_source(
        self, db_path, monkeypatch
    ):
        """A terminal error before any job runs is outside dlt's auto-abort."""
        # truncate-and-insert truncates the target while the load is being set
        # up; saga's default insert-from-staging does it inside a job instead.
        monkeypatch.setenv("DESTINATION__REPLACE_STRATEGY", "truncate-and-insert")
        assert _run(db_path).success

        # Move the target away: dlt's schema still believes it exists, so the
        # setup-time truncate fails before any job runs.
        _execute(db_path, f"ALTER TABLE {_SCHEMA}.{_TABLE} RENAME TO {_TABLE}_moved")
        failed = _run(db_path)
        assert not failed.success
        assert "step=load" in failed.error

        _execute(db_path, f"ALTER TABLE {_SCHEMA}.{_TABLE}_moved RENAME TO {_TABLE}")
        _ROWS["n"] = 7
        recovered = _run(db_path)

        assert recovered.success, recovered.error
        count = _execute(db_path, f"SELECT COUNT(*) FROM {_SCHEMA}.{_TABLE}")[0][0]
        assert count == 7, "the stale package was loaded instead of the current source"
