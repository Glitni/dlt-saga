"""Unit tests for HistorizeConfig.merge_key and the SQL-shape changes that
scope deletion-detection and reappearance logic to merge_key groups.

The motivating case: source unions independently-delivered partitions sharing a
single snapshot column (e.g. multi-instance / multi-tenant feeds). Without
scoping, a snapshot date from one group flags keys in *other* groups as
disappeared. With merge_key set, deletion / reappearance only fire within the
same group.
"""

from unittest.mock import MagicMock

import pytest

from dlt_saga.historize.config import HistorizeConfig
from dlt_saga.historize.sql import HistorizeSqlBuilder
from dlt_saga.historize.state import HistorizeStateManager


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


def _builder(merge_key=None, primary_key=("instance", "id"), track_deletions=True):
    config = HistorizeConfig.from_dict(
        {
            "primary_key": list(primary_key),
            "track_deletions": track_deletions,
            **({"merge_key": list(merge_key)} if merge_key else {}),
        },
        top_level_primary_key=list(primary_key),
    )
    return HistorizeSqlBuilder(
        config=config,
        destination=_stub_destination(),
        source_table_id="proj.ds.src",
        target_table_id="proj.ds.tgt",
        primary_key=list(primary_key),
        source_database="proj",
        source_schema="ds",
        source_table="src",
        target_table_name="tgt",
        target_schema="ds",
    )


@pytest.mark.unit
class TestMergeKeyConfig:
    def test_defaults_to_none(self):
        c = HistorizeConfig.from_dict({}, top_level_primary_key=["id"])
        assert c.merge_key is None

    def test_string_coerced_to_list(self):
        c = HistorizeConfig.from_dict(
            {"primary_key": ["instance", "id"], "merge_key": "instance"},
            top_level_primary_key=["instance", "id"],
        )
        assert c.merge_key == ["instance"]

    def test_list_preserved(self):
        c = HistorizeConfig.from_dict(
            {"primary_key": ["a", "b", "id"], "merge_key": ["a", "b"]},
            top_level_primary_key=["a", "b", "id"],
        )
        assert c.merge_key == ["a", "b"]

    def test_must_be_subset_of_primary_key(self):
        c = HistorizeConfig.from_dict(
            {"primary_key": ["instance", "id"], "merge_key": ["region"]},
            top_level_primary_key=["instance", "id"],
        )
        with pytest.raises(ValueError, match="subset of"):
            c.validate()

    def test_full_pk_allowed(self):
        """merge_key == primary_key is a no-op (every PK is its own group) but legal."""
        c = HistorizeConfig.from_dict(
            {"primary_key": ["instance", "id"], "merge_key": ["instance", "id"]},
            top_level_primary_key=["instance", "id"],
        )
        c.validate()  # must not raise

    def test_invalid_identifier_rejected(self):
        with pytest.raises(ValueError, match="merge_key"):
            HistorizeConfig.from_dict(
                {"primary_key": ["instance", "id"], "merge_key": ["not a column"]},
                top_level_primary_key=["instance", "id"],
            )


@pytest.mark.unit
class TestMergeKeyFingerprint:
    """Changing merge_key after the historized table exists must trigger the
    standard 'config changed → run with --full-refresh' guard."""

    def _fp(self, **overrides):
        cfg = HistorizeConfig.from_dict(
            {"primary_key": ["instance", "id"], **overrides},
            top_level_primary_key=["instance", "id"],
        )
        return HistorizeStateManager.compute_fingerprint(cfg)

    def test_changes_when_merge_key_added(self):
        assert self._fp() != self._fp(merge_key=["instance"])

    def test_changes_when_merge_key_removed(self):
        with_mk = self._fp(merge_key=["instance"])
        without = self._fp()
        assert with_mk != without

    def test_stable_when_unrelated_changes(self):
        a = self._fp(merge_key=["instance"])
        b = self._fp(merge_key=["instance"], table_name="something")
        assert a == b


@pytest.mark.unit
class TestMergeKeyScopesSnapshotSequence:
    """When merge_key is set, the all_snapshots / snapshot_sequence CTEs project
    the merge_key columns and the window LAG/LEAD partitions by them."""

    def test_full_reprocess_partitions_snapshot_sequence(self):
        sql = _builder(merge_key=["instance"]).build_full_reprocess_sql(["a", "b"])
        assert "PARTITION BY `instance` ORDER BY snapshot_date" in sql
        # The deletion-side JOIN must also scope on instance (seq-aliased on ss).
        assert (
            "ss.seq_snapshot_date = kp.snapshot_date "
            "AND ss.`seq_instance` = kp.`instance`"
        ) in sql

    def test_incremental_partitions_snapshot_sequence(self):
        sql = _builder(merge_key=["instance"]).build_incremental_sql(
            ["a", "b"], new_snapshots=["2024-01-02"], last_historized_snapshot=None
        )
        assert "PARTITION BY `instance` ORDER BY snapshot_date" in sql
        # The deletion-side JOIN must also scope on instance.
        assert (
            "ss.seq_snapshot_date = dc.snapshot_date "
            "AND ss.`seq_instance` = dc.`instance`"
        ) in sql

    def test_unset_merge_key_yields_global_window(self):
        """No merge_key configured → no PARTITION BY in snapshot_sequence; current
        behavior preserved bit-for-bit."""
        sql = _builder().build_full_reprocess_sql(["a", "b"])
        assert "PARTITION BY `instance` ORDER BY snapshot_date" not in sql
        # The plain global LAG/LEAD must still be present.
        assert "LAG(snapshot_date) OVER (ORDER BY snapshot_date)" in sql
        assert "LEAD(snapshot_date) OVER (ORDER BY snapshot_date)" in sql

    def test_change_detection_join_also_scoped(self):
        """with_context joins snapshot_sequence to detect gaps for change detection;
        that JOIN must also include the merge_key predicate so a missed snapshot
        in a sibling group doesn't drive false-reappearance rows in this one."""
        sql = _builder(merge_key=["instance"]).build_full_reprocess_sql(["a", "b"])
        # The change-detection JOIN (alias d for deduped) must include the
        # seq-aliased merge_key predicate.
        assert (
            "ss.seq_snapshot_date = d.`_dlt_ingested_at` "
            "AND ss.`seq_instance` = d.`instance`"
        ) in sql

    def test_snapshot_sequence_aliases_merge_key_with_seq_prefix(self):
        """The projection inside snapshot_sequence aliases each merge_key
        column with a ``seq_`` prefix to avoid name collisions in downstream
        JOINs (would make WINDOW PARTITION BY references ambiguous)."""
        sql = _builder(merge_key=["instance"]).build_full_reprocess_sql(["a", "b"])
        assert "`instance` AS `seq_instance`, snapshot_date AS seq_snapshot_date" in sql


@pytest.mark.unit
class TestMergeKeyMultiColumn:
    """Multi-column merge_key projects and partitions on each column."""

    def test_two_column_merge_key(self):
        sql = _builder(
            merge_key=["region", "tenant"],
            primary_key=("region", "tenant", "id"),
        ).build_full_reprocess_sql(["a"])
        # Both seq-aliased in the snapshot_sequence projection.
        assert (
            "`region` AS `seq_region`, `tenant` AS `seq_tenant`, "
            "snapshot_date AS seq_snapshot_date"
        ) in sql
        # Both in the PARTITION BY (using original names — they exist on all_snapshots).
        assert "PARTITION BY `region`, `tenant` ORDER BY snapshot_date" in sql
        # Both in the JOIN predicate (seq-aliased on the ss side).
        assert "ss.`seq_region` = d.`region` AND ss.`seq_tenant` = d.`tenant`" in sql
