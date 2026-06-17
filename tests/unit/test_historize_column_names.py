"""Unit tests for configurable SCD2 output column names in historize.

Covers:
- HistorizeConfig defaults and overrides for valid_from/valid_to/is_deleted columns
- partition_column falling back to the valid_from column
- HistorizeSqlBuilder emitting the configured names in full-reprocess, incremental,
  and create-table SQL (and the defaults when not overridden)
- SQL-identifier validation of the new column-name fields
"""

from unittest.mock import MagicMock

import pytest

from dlt_saga.historize.config import HistorizeConfig
from dlt_saga.historize.sql import HistorizeSqlBuilder
from dlt_saga.historize.state import HistorizeStateManager

CUSTOM = {
    "valid_from_column": "zyx_ErGyldigFraDato",
    "valid_to_column": "zyx_ErGyldigTilDato",
    "is_deleted_column": "zyx_ErSlettetFlagg",
}
DEFAULT_NAMES = ("_dlt_valid_from", "_dlt_valid_to", "_dlt_is_deleted")


def _stub_destination():
    dest = MagicMock()
    dest.quote_identifier.side_effect = lambda s: f"`{s}`"
    dest.hash_expression.side_effect = lambda cols: f"HASH({', '.join(cols)})"
    dest.cast_to_string.side_effect = lambda expr: f"CAST({expr} AS STRING)"
    dest.type_name.side_effect = lambda t: t.upper()
    dest.get_full_table_id.side_effect = lambda ds, tbl: f"proj.{ds}.{tbl}"
    dest.columns_query.side_effect = lambda db, sc, t: "SELECT column_name FROM info"
    dest.build_historize_create_table_sql.side_effect = lambda **kw: (
        f"CREATE TABLE {kw['target_table_id']} AS {kw['select_body']}"
    )
    return dest


def _make_builder(historize_dict=None):
    config = HistorizeConfig.from_dict(
        historize_dict or {}, top_level_primary_key=["id"]
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
class TestHistorizeConfigColumnNames:
    def test_defaults(self):
        c = HistorizeConfig.from_dict({}, top_level_primary_key=["id"])
        assert c.valid_from_column == "_dlt_valid_from"
        assert c.valid_to_column == "_dlt_valid_to"
        assert c.is_deleted_column == "_dlt_is_deleted"

    def test_overrides(self):
        c = HistorizeConfig.from_dict(dict(CUSTOM), top_level_primary_key=["id"])
        assert c.valid_from_column == "zyx_ErGyldigFraDato"
        assert c.valid_to_column == "zyx_ErGyldigTilDato"
        assert c.is_deleted_column == "zyx_ErSlettetFlagg"

    def test_partition_column_defaults_to_valid_from(self):
        c = HistorizeConfig.from_dict(dict(CUSTOM), top_level_primary_key=["id"])
        assert c.partition_column == "zyx_ErGyldigFraDato"

    def test_explicit_partition_column_preserved(self):
        c = HistorizeConfig.from_dict(
            {**CUSTOM, "partition_column": "snapshot_dt"}, top_level_primary_key=["id"]
        )
        assert c.partition_column == "snapshot_dt"

    def test_invalid_identifier_rejected(self):
        with pytest.raises(ValueError):
            HistorizeConfig.from_dict(
                {"valid_from_column": "not a column"}, top_level_primary_key=["id"]
            )


@pytest.mark.unit
class TestHistorizeSqlColumnNames:
    def test_full_reprocess_uses_custom_names(self):
        sql = _make_builder(dict(CUSTOM)).build_full_reprocess_sql(["a", "b"])
        for name in CUSTOM.values():
            assert name in sql
        for default in DEFAULT_NAMES:
            assert default not in sql

    def test_full_reprocess_defaults(self):
        sql = _make_builder().build_full_reprocess_sql(["a", "b"])
        for default in DEFAULT_NAMES:
            assert default in sql

    def test_incremental_uses_custom_names(self):
        sql = _make_builder(dict(CUSTOM)).build_incremental_sql(
            ["a", "b"], new_snapshots=["2024-01-01"]
        )
        for name in CUSTOM.values():
            assert name in sql
        for default in DEFAULT_NAMES:
            assert default not in sql

    def test_create_table_uses_custom_names(self):
        sql = _make_builder(dict(CUSTOM)).build_create_target_table_sql(["a", "b"])
        for name in CUSTOM.values():
            assert name in sql

    def test_rollback_uses_custom_names(self):
        stmts = _make_builder(dict(CUSTOM)).build_rollback_sql(
            "2024-01-01T00:00:00", run_id="abc", dataset="ds"
        )
        joined = "\n".join(stmts)
        assert "zyx_ErGyldigFraDato" in joined
        assert "zyx_ErGyldigTilDato" in joined
        assert "_dlt_valid_from" not in joined


@pytest.mark.unit
class TestBigQueryIcebergRenameCollision:
    """When the user renames an SCD2 column to a name that also exists on the
    source, the BigQuery Iceberg explicit-column DDL must not emit both — the
    explicit SCD2 column wins, the source-side same-name column is dropped."""

    def _invoke(self, source_cols, **rename):
        from dlt_saga.destinations.base import MaterializationHints
        from dlt_saga.destinations.bigquery.config import BigQueryDestinationConfig
        from dlt_saga.destinations.bigquery.destination import BigQueryDestination

        dest = MagicMock(spec=BigQueryDestination)
        dest.config = BigQueryDestinationConfig(
            project_id="p", storage_path="gs://b/p/", table_format="iceberg"
        )
        dest._HISTORIZE_EXCLUDE_COLS = BigQueryDestination._HISTORIZE_EXCLUDE_COLS
        dest.type_name.side_effect = lambda t: t.upper()
        dest.columns_query.side_effect = lambda db, sc, t: (
            "SELECT column_name FROM info"
        )
        dest.execute_sql.return_value = [
            MagicMock(column_name=n, data_type="STRING") for n in source_cols
        ]
        dest.partition_ddl.side_effect = lambda c: f"PARTITION BY DATE({c})"

        valid_from = rename.get("valid_from_column", "_dlt_valid_from")
        hints = MaterializationHints(
            partition_column=valid_from,
            cluster_columns=None,
            table_format="iceberg",
            table_name="tgt",
            schema="ds",
            source_database="proj",
            source_schema="ds",
            source_table="src",
            valid_from_column=valid_from,
            valid_to_column=rename.get("valid_to_column", "_dlt_valid_to"),
            is_deleted_column=rename.get("is_deleted_column", "_dlt_is_deleted"),
        )
        return BigQueryDestination.build_historize_create_table_sql(
            dest,
            create_clause="CREATE TABLE",
            target_table_id="proj.ds.tgt",
            select_body="ignored",
            hints=hints,
        )

    def test_source_column_colliding_with_renamed_is_deleted_is_dropped(self):
        """Source has `is_deleted`; SCD2 is renamed to `is_deleted`. The source
        column must NOT appear — the explicit SCD2 definition is the only one."""
        sql = self._invoke(
            source_cols=["id", "name", "is_deleted"],
            valid_from_column="valid_from",
            valid_to_column="valid_to",
            is_deleted_column="is_deleted",
        )
        # `is_deleted` appears exactly once — as the explicit BOOL column at the bottom.
        assert sql.count("`is_deleted`") == 1
        assert "`is_deleted` BOOL" in sql

    def test_source_column_colliding_with_renamed_valid_from_is_dropped(self):
        sql = self._invoke(
            source_cols=["id", "valid_from"],
            valid_from_column="valid_from",
            valid_to_column="valid_to",
            is_deleted_column="is_deleted",
        )
        assert sql.count("`valid_from`") == 1
        assert "`valid_from` TIMESTAMP" in sql

    def test_default_names_still_filter_dlt_columns(self):
        """No rename — defaults still drop the standard `_dlt_*` system columns
        from the source side (existing behavior preserved)."""
        sql = self._invoke(source_cols=["id", "_dlt_valid_from", "_dlt_id"])
        # _dlt_valid_from appears exactly once (the explicit SCD2 column),
        # not twice (the source-side _dlt_valid_from is dropped by the existing exclude).
        assert sql.count("`_dlt_valid_from`") == 1
        assert "`_dlt_id`" not in sql


@pytest.mark.unit
class TestColumnNamesInFingerprint:
    """Renaming an SCD2 column must change the config fingerprint, so the
    framework forces a full refresh instead of emitting SQL against columns the
    existing historized table doesn't have."""

    def _fp(self, **overrides):
        cfg = HistorizeConfig.from_dict(dict(overrides), top_level_primary_key=["id"])
        return HistorizeStateManager.compute_fingerprint(cfg)

    def test_changes_when_valid_from_renamed(self):
        assert self._fp() != self._fp(valid_from_column="valid_from")

    def test_changes_when_valid_to_renamed(self):
        assert self._fp() != self._fp(valid_to_column="valid_to")

    def test_changes_when_is_deleted_renamed(self):
        assert self._fp() != self._fp(is_deleted_column="is_deleted")

    def test_changes_when_partition_renamed(self):
        assert self._fp() != self._fp(partition_column="snapshot_dt")

    def test_stable_for_unrelated_change(self):
        # output_table is not part of the fingerprint — renaming it must not
        # trigger a full refresh.
        assert self._fp() == self._fp(output_table="custom_name")
