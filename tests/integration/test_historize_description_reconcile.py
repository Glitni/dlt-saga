"""Integration test: description/classification reconcile on historized tables.

The historized table is built by saga SQL (dlt never sees it), so the reconcile
in HistorizeRunner is its only source of descriptions. Verifies source-column
docs, SCD2 system-column canned descriptions, the table description, and the
``historize.persist_docs`` override — all on in-process DuckDB.
"""

import pytest

from dlt_saga.destinations.duckdb.config import DuckDBDestinationConfig
from dlt_saga.destinations.duckdb.destination import DuckDBDestination
from dlt_saga.historize.config import HistorizeConfig
from dlt_saga.historize.runner import HistorizeRunner

SCHEMA = "hist_desc"
SOURCE_TABLE = "raw_events"
TARGET_TABLE = "raw_events_historized"


@pytest.fixture
def dest():
    cfg = DuckDBDestinationConfig(
        project_id="test",
        location="local",
        schema_name=SCHEMA,
        database_path=":memory:",
    )
    d = DuckDBDestination(cfg)
    yield d
    d.close()


def _seed(d):
    d.execute_sql(f'CREATE SCHEMA IF NOT EXISTS "{SCHEMA}"', SCHEMA)
    d.execute_sql(
        f'CREATE TABLE IF NOT EXISTS "{SCHEMA}"."{SOURCE_TABLE}" '
        "(id INTEGER, email VARCHAR, snapshot_date TIMESTAMP)",
        SCHEMA,
    )
    d.execute_sql(
        f'INSERT INTO "{SCHEMA}"."{SOURCE_TABLE}" '
        "VALUES (1, 'a@example.no', TIMESTAMP '2026-01-01')",
        SCHEMA,
    )


def _runner(d, config_dict):
    # Build HistorizeConfig from the historize section (as the real loader does)
    # so historize-level doc overrides are parsed.
    hist = {
        "snapshot_column": "snapshot_date",
        "track_deletions": True,
        **config_dict.get("historize", {}),
    }
    config = HistorizeConfig.from_dict(hist, config_dict.get("primary_key"))
    return HistorizeRunner(
        pipeline_name="test__raw_events",
        historize_config=config,
        destination=d,
        database="test",
        schema=SCHEMA,
        source_table_name=SOURCE_TABLE,
        target_table_name=TARGET_TABLE,
        config_dict=config_dict,
        full_refresh=True,
    )


@pytest.mark.integration
class TestHistorizeDescriptionReconcile:
    def test_column_table_and_scd2_descriptions_applied(self, dest):
        _seed(dest)
        stats = _runner(
            dest,
            {
                "write_disposition": "historize",
                "primary_key": ["id"],
                "description": "Event history",
                "persist_docs": True,  # column docs are opt-in
                "columns": {
                    "email": {"description": "Contact email", "classification": ["pii"]}
                },
            },
        ).run()

        assert stats["status"] == "completed"
        cols = dest.get_column_descriptions(SCHEMA, TARGET_TABLE)
        # Source-column doc + classification, composed identically to the ingest table.
        assert cols["email"] == "Contact email [saga:classification=pii]"
        # SCD2 system columns get canned documentation.
        assert cols["_dlt_valid_from"].startswith("SCD2 validity start")
        assert cols["_dlt_valid_to"].startswith("SCD2 validity end")
        assert cols["_dlt_is_deleted"].startswith("SCD2 deletion marker")
        assert dest.get_table_description(SCHEMA, TARGET_TABLE) == "Event history"

    def test_historize_doc_overrides_take_precedence(self, dest):
        """historize.description/classification/columns override the top level,
        merging per-column (override only the keys given, inherit the rest)."""
        _seed(dest)
        _runner(
            dest,
            {
                "write_disposition": "historize",
                "primary_key": ["id"],
                "description": "Top-level table desc",
                "classification": ["internal"],
                "persist_docs": True,  # column docs are opt-in
                "columns": {
                    "email": {"description": "Contact email", "classification": ["pii"]}
                },
                "historize": {
                    "description": "Historized table desc",
                    "classification": ["historical"],
                    "columns": {
                        # override only classification; inherit the description
                        "email": {"classification": ["pii", "historical"]}
                    },
                },
            },
        ).run()

        cols = dest.get_column_descriptions(SCHEMA, TARGET_TABLE)
        # email: description inherited from top level, classification overridden
        assert cols["email"] == "Contact email [saga:classification=historical,pii]"
        # table: description + classification both from the historize override
        assert (
            dest.get_table_description(SCHEMA, TARGET_TABLE)
            == "Historized table desc [saga:classification=historical]"
        )

    def test_historize_persist_docs_override_disables_column_docs(self, dest):
        _seed(dest)
        _runner(
            dest,
            {
                "write_disposition": "historize",
                "primary_key": ["id"],
                # Top-level enables column docs; the historize override turns
                # them back off for the historized table only.
                "persist_docs": {"columns": True},
                "columns": {
                    "email": {"description": "Contact", "classification": ["pii"]}
                },
                "historize": {"persist_docs": {"columns": False}},
            },
        ).run()

        cols = dest.get_column_descriptions(SCHEMA, TARGET_TABLE)
        # columns:false suppresses both source-column docs and SCD2 descriptions.
        assert cols.get("email", "") == ""
        assert cols.get("_dlt_valid_from", "") == ""
