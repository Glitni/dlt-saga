"""Integration tests for description/classification reconciliation on DuckDB.

DuckDB gets no descriptions from dlt, so this reconcile is its only source of
column/table comments — and the in-process engine lets us exercise the full
generic reconcile path (read → diff → write) without cloud credentials.
"""

import pytest

from dlt_saga.pipelines.target.config import TargetConfig

SCHEMA = "recon_test"


def _create_table(dest, table, columns):
    dest.execute_sql(f'CREATE SCHEMA IF NOT EXISTS "{SCHEMA}"')
    cols = ", ".join(f'"{c}" VARCHAR' for c in columns)
    dest.execute_sql(f'CREATE TABLE "{SCHEMA}"."{table}" ({cols})')


@pytest.mark.integration
class TestDescriptionReconcileDuckDB:
    def test_sets_column_and_table_descriptions(self, duckdb_destination):
        d = duckdb_destination
        _create_table(d, "orders", ["order_id", "email"])

        d.reconcile_descriptions(
            SCHEMA,
            "orders",
            descriptions={
                "order_id": "The order id",
                "email": "Contact [saga:classification=pii]",
            },
            table_description="Orders table",
        )

        cols = d.get_column_descriptions(SCHEMA, "orders")
        assert cols["order_id"] == "The order id"
        assert cols["email"] == "Contact [saga:classification=pii]"
        assert d.get_table_description(SCHEMA, "orders") == "Orders table"

    def test_idempotent_second_run_leaves_state(self, duckdb_destination):
        d = duckdb_destination
        _create_table(d, "t1", ["a"])
        desc = {"a": "col a"}

        d.reconcile_descriptions(
            SCHEMA, "t1", descriptions=desc, table_description="tbl"
        )
        d.reconcile_descriptions(
            SCHEMA, "t1", descriptions=desc, table_description="tbl"
        )

        assert d.get_column_descriptions(SCHEMA, "t1")["a"] == "col a"
        assert d.get_table_description(SCHEMA, "t1") == "tbl"

    def test_drift_updates_description(self, duckdb_destination):
        d = duckdb_destination
        _create_table(d, "t2", ["a"])

        d.reconcile_descriptions(SCHEMA, "t2", descriptions={"a": "old"})
        d.reconcile_descriptions(SCHEMA, "t2", descriptions={"a": "new"})

        assert d.get_column_descriptions(SCHEMA, "t2")["a"] == "new"

    def test_classification_removal_reconciled(self, duckdb_destination):
        d = duckdb_destination
        _create_table(d, "t3", ["email"])

        d.reconcile_descriptions(
            SCHEMA, "t3", descriptions={"email": "Contact [saga:classification=pii]"}
        )
        d.reconcile_descriptions(SCHEMA, "t3", descriptions={"email": "Contact"})

        assert d.get_column_descriptions(SCHEMA, "t3")["email"] == "Contact"

    def test_column_absent_from_table_is_skipped(self, duckdb_destination):
        d = duckdb_destination
        _create_table(d, "t4", ["a"])

        d.reconcile_descriptions(
            SCHEMA, "t4", descriptions={"a": "col a", "ghost": "should be ignored"}
        )

        cols = d.get_column_descriptions(SCHEMA, "t4")
        assert cols["a"] == "col a"
        assert "ghost" not in cols

    def test_source_cased_config_key_matches_normalized_column(
        self, duckdb_destination
    ):
        d = duckdb_destination
        _create_table(d, "t5", ["company_name"])

        # Config key in source CamelCase must map to dlt's snake_case column.
        d.reconcile_descriptions(SCHEMA, "t5", descriptions={"CompanyName": "The name"})

        assert d.get_column_descriptions(SCHEMA, "t5")["company_name"] == "The name"

    def test_description_with_semicolon_and_non_ascii(self, duckdb_destination):
        """Semicolons (multi-key blocks / prose) and non-ASCII must round-trip."""
        d = duckdb_destination
        _create_table(d, "t7", ["a"])
        desc = "Tilhørende Amedia; se intern dok [saga:classification=pii;owner=data]"

        d.reconcile_descriptions(SCHEMA, "t7", descriptions={"a": desc})

        assert d.get_column_descriptions(SCHEMA, "t7")["a"] == desc

    def test_end_to_end_via_target_config(self, duckdb_destination):
        d = duckdb_destination
        _create_table(d, "t6", ["email", "amount"])

        cfg = TargetConfig(
            persist_docs=True,
            columns={"email": {"description": "Contact", "classification": ["pii"]}},
        )
        d.reconcile_descriptions(
            SCHEMA, "t6", descriptions=cfg.get_column_description_map()
        )

        assert (
            d.get_column_descriptions(SCHEMA, "t6")["email"]
            == "Contact [saga:classification=pii]"
        )
