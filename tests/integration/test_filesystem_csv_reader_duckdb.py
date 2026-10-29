"""Filesystem CSV ingest into real DuckDB through saga's DuckDB CSV reader.

The unit tests cover the reader's output. This runs the whole pipeline — saga's
``_dlt_ingested_at`` injection, column defaults, dlt normalize and load — over
files whose columns DuckDB types differently, which dlt must reconcile.
"""

import duckdb
import pytest

from dlt_saga.pipelines.filesystem.pipeline import FilesystemPipeline
from dlt_saga.testing import run_pipeline_test

_SCHEMA = "csv_test"
_TABLE = "feed"


def _config(db_path, csv_dir):
    return {
        "schema_name": _SCHEMA,
        "table_name": _TABLE,
        "pipeline_name": f"filesystem__{_TABLE}",
        "write_disposition": "replace",
        "database_path": db_path,
        "filesystem_type": "file",
        "bucket_name": str(csv_dir),
        "file_glob": "*.csv",
        "file_type": "csv",
        "columns": {"region": {"default": "unknown"}},
    }


def _rows(db_path):
    conn = duckdb.connect(db_path)
    try:
        return conn.execute(
            f"SELECT id, region, _dlt_ingested_at IS NOT NULL "
            f"FROM {_SCHEMA}.{_TABLE} ORDER BY id"
        ).fetchall()
    finally:
        conn.close()


@pytest.fixture
def csv_dir(tmp_path):
    data = tmp_path / "data"
    data.mkdir()
    (data / "a.csv").write_text("id,region\n1,north\n2,\n", encoding="utf-8")
    # region is empty throughout this file, so DuckDB types it differently here
    (data / "b.csv").write_text("id,region\n3,\n", encoding="utf-8")
    return data


@pytest.mark.integration
def test_csv_loads_rows_with_defaults(tmp_path, csv_dir):
    db_path = str(tmp_path / "csv.duckdb")
    result = run_pipeline_test(
        FilesystemPipeline,
        config_dict=_config(db_path, csv_dir),
        schema_name=_SCHEMA,
        database_path=db_path,
    )

    assert result.success, result.error
    assert _rows(db_path) == [
        (1, "north", True),
        (2, "unknown", True),
        (3, "unknown", True),
    ]
