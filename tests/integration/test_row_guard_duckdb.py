"""Row guard against real DuckDB.

The unit tests mock dlt, so they prove the guard's decision logic but not that
aborting mid-run actually spares the table. These run a real replace pipeline
twice — first with data, then with none — and assert on the rows DuckDB holds
afterwards.

The unguarded case is pinned too, because it is the behaviour that motivated
the guard and is easy to mistake for a saga bug: dlt emits a load job for a
replace even with nothing to write, so the truncate/swap runs and a green run
leaves an empty table behind.
"""

import dlt
import duckdb
import pytest

from dlt_saga.pipelines.base_pipeline import BasePipeline
from dlt_saga.testing import run_pipeline_test

_SCHEMA = "guard_test"
_TABLE = "orders"

# Row count for the next run, read by the pipeline class below. A module-level
# dial keeps the pipeline class itself trivial, which matters because
# run_pipeline_test instantiates it rather than taking an instance.
_ROWS = {"n": 3}


class _OrdersPipeline(BasePipeline):
    """Yields ``_ROWS["n"]`` rows into the configured table."""

    def extract_data(self):
        table = self.table_name

        @dlt.resource(name=table, write_disposition="replace")
        def rows():
            for i in range(_ROWS["n"]):
                yield {"id": i, "val": f"v{i}"}

        return [(rows(), "orders")]


def _config(db_path, **overrides):
    config = {
        "schema_name": _SCHEMA,
        "table_name": _TABLE,
        "pipeline_name": f"guard__{_TABLE}",
        "write_disposition": "replace",
        "database_path": db_path,
    }
    config.update(overrides)
    return config


def _row_count(db_path):
    # Not read-only: the DuckDB destination may still hold a read/write
    # connection to the same file in-process, and DuckDB refuses a second
    # connection opened with a different configuration.
    conn = duckdb.connect(db_path)
    try:
        return conn.execute(f"SELECT COUNT(*) FROM {_SCHEMA}.{_TABLE}").fetchone()[0]
    finally:
        conn.close()


@pytest.fixture
def db_path(tmp_path):
    """File-backed DuckDB so state survives between the two runs."""
    _ROWS["n"] = 3
    return str(tmp_path / "guard.duckdb")


@pytest.mark.integration
class TestMinRowsGuard:
    def test_empty_replace_is_abandoned_and_table_survives(self, db_path):
        first = run_pipeline_test(
            _OrdersPipeline,
            config_dict=_config(db_path, min_rows=1),
            schema_name=_SCHEMA,
            database_path=db_path,
        )
        assert first.success, first.error
        assert _row_count(db_path) == 3

        _ROWS["n"] = 0
        second = run_pipeline_test(
            _OrdersPipeline,
            config_dict=_config(db_path, min_rows=1),
            schema_name=_SCHEMA,
            database_path=db_path,
        )

        assert not second.success
        assert "min_rows=1" in second.error
        assert _row_count(db_path) == 3, (
            "guard fired but the table was still replaced with an empty load"
        )

    def test_next_run_with_data_recovers(self, db_path):
        """A dropped package must not resurface and empty the table later."""
        run_pipeline_test(
            _OrdersPipeline,
            config_dict=_config(db_path, min_rows=1),
            schema_name=_SCHEMA,
            database_path=db_path,
        )

        _ROWS["n"] = 0
        run_pipeline_test(
            _OrdersPipeline,
            config_dict=_config(db_path, min_rows=1),
            schema_name=_SCHEMA,
            database_path=db_path,
        )

        _ROWS["n"] = 5
        recovered = run_pipeline_test(
            _OrdersPipeline,
            config_dict=_config(db_path, min_rows=1),
            schema_name=_SCHEMA,
            database_path=db_path,
        )

        assert recovered.success, recovered.error
        assert _row_count(db_path) == 5

    def test_threshold_met_loads_normally(self, db_path):
        result = run_pipeline_test(
            _OrdersPipeline,
            config_dict=_config(db_path, min_rows=3),
            schema_name=_SCHEMA,
            database_path=db_path,
        )
        assert result.success, result.error
        assert _row_count(db_path) == 3

    def test_partial_load_below_threshold_is_abandoned(self, db_path):
        """The scd2-shaped failure: a batch that shrank, not one that vanished."""
        run_pipeline_test(
            _OrdersPipeline,
            config_dict=_config(db_path, min_rows=2),
            schema_name=_SCHEMA,
            database_path=db_path,
        )
        assert _row_count(db_path) == 3

        _ROWS["n"] = 1
        result = run_pipeline_test(
            _OrdersPipeline,
            config_dict=_config(db_path, min_rows=2),
            schema_name=_SCHEMA,
            database_path=db_path,
        )

        assert not result.success
        assert _row_count(db_path) == 3


@pytest.mark.integration
class TestUnguardedReplaceStillEmpties:
    def test_zero_row_replace_empties_the_table_without_min_rows(self, db_path):
        """Documents dlt's behaviour the guard exists to prevent."""
        run_pipeline_test(
            _OrdersPipeline,
            config_dict=_config(db_path),
            schema_name=_SCHEMA,
            database_path=db_path,
        )
        assert _row_count(db_path) == 3

        _ROWS["n"] = 0
        result = run_pipeline_test(
            _OrdersPipeline,
            config_dict=_config(db_path),
            schema_name=_SCHEMA,
            database_path=db_path,
        )

        assert result.success, "dlt reports this as a successful run"
        assert _row_count(db_path) == 0


@pytest.mark.integration
class TestGuardFailureIsRecorded:
    """A tripped guard must reach the telemetry `state:failed` selects on.

    `MinRowsNotMetError` is deliberately not a ValueError: the session reads a
    ValueError from a pipeline as a pre-run config error and skips recording it
    as a run outcome, which would leave a guarded pipeline looking like it never
    ran.
    """

    def test_cli_ingest_records_a_failed_outcome_and_keeps_the_table(
        self, tmp_path, monkeypatch
    ):
        from typer.testing import CliRunner

        from dlt_saga.cli import app
        from dlt_saga.init_command import run_init
        from dlt_saga.utility.cli.context import clear_execution_context

        monkeypatch.chdir(tmp_path)
        run_init(no_input=True)

        config = tmp_path / "configs" / "filesystem" / "sample.yml"
        config.write_text(
            "tags: [daily]\n"
            "write_disposition: replace\n"
            "min_rows: 1\n"
            "\n"
            "filesystem_type: file\n"
            "bucket_name: data\n"
            'file_glob: "*.csv"\n'
            "file_type: csv\n",
            encoding="utf-8",
        )

        runner = CliRunner()
        first = runner.invoke(app, ["ingest", "--select", "filesystem__sample"])
        assert first.exit_code == 0, first.output
        clear_execution_context()

        def sample_rows():
            conn = duckdb.connect(str(tmp_path / "local.duckdb"))
            try:
                conn.execute("use dlt_dev")
                return conn.sql("select count(*) from filesystem__sample").fetchone()[0]
            finally:
                conn.close()

        assert sample_rows() == 3

        # The incident being modelled: the source files move somewhere else.
        (tmp_path / "data" / "sample.csv").rename(tmp_path / "data" / "moved.csv.bak")

        second = runner.invoke(app, ["ingest", "--select", "filesystem__sample"])
        assert second.exit_code != 0, (
            f"guarded empty ingest should fail the run:\n{second.output}"
        )
        assert sample_rows() == 3, "the guard fired but the table was emptied anyway"

        conn = duckdb.connect(str(tmp_path / "local.duckdb"))
        try:
            conn.execute("use dlt_dev")
            outcomes = [
                row[0]
                for row in conn.sql(
                    "select status from _saga_execution_plans order by started_at"
                ).fetchall()
            ]
        finally:
            conn.close()

        assert outcomes == ["completed", "failed"], (
            f"expected the guarded run to be recorded as failed, got {outcomes}"
        )
