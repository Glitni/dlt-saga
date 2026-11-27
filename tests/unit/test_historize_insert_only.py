"""Unit tests for historize ``strategy: insert_only``: config, fingerprint and SQL shape."""

from datetime import date

import pytest

from dlt_saga.historize.config import HistorizeConfig
from dlt_saga.historize.sql import HistorizeSqlBuilder, coalesce_day_ranges
from dlt_saga.historize.state import HistorizeStateManager
from tests.unit.test_historize_filters import _stub_destination


def _config(**historize):
    return HistorizeConfig.from_dict(
        {"strategy": "insert_only", **historize}, top_level_primary_key=["id"]
    )


def _builder(**historize):
    config = HistorizeConfig(
        strategy="insert_only",
        primary_key=["id"],
        snapshot_column="snapshot_date",
        **historize,
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
class TestInsertOnlyConfig:
    def test_shape_flags(self):
        config = _config()
        assert config.is_insert_only
        assert not config.is_scd1
        assert not config.has_validity_end
        assert config.track_deletions is False
        assert config.effective_order_by == []

    def test_batching_is_off_unless_set(self):
        assert _config().batch_max_rows is None
        assert _config(batch_max_rows=10).batch_max_rows == 10

    @pytest.mark.parametrize(
        "option",
        [
            {"order_by": ["event_time"]},
            {"track_columns": ["payload"]},
            {"ignore_columns": ["payload"]},
            {"track_deletions": True},
            {"merge_key": ["id"]},
            {"valid_to_column": "ended_at"},
            {"is_deleted_column": "gone"},
        ],
    )
    def test_options_without_meaning_rejected(self, option):
        key = next(iter(option))
        with pytest.raises(ValueError, match=f"historize.{key} is not supported"):
            _config(**option)

    def test_window_requires_partition_column(self):
        with pytest.raises(ValueError, match="partition_column"):
            _config(dedup_window_days=1)
        assert (
            _config(
                dedup_window_days=0, partition_column="event_time"
            ).dedup_window_days
            == 0
        )

    @pytest.mark.parametrize(
        "option",
        [
            {"dedup_window_days": -1, "partition_column": "event_time"},
            {"dedup_window_days": True, "partition_column": "event_time"},
            {"batch_max_rows": 0},
            {"batch_max_rows": "many"},
        ],
    )
    def test_invalid_numbers_rejected(self, option):
        with pytest.raises(ValueError, match="must be an integer of at least"):
            _config(**option)

    @pytest.mark.parametrize("strategy", ["scd1", "scd2"])
    @pytest.mark.parametrize(
        "option",
        [
            {"dedup_window_days": 1, "partition_column": "event_time"},
            {"batch_max_rows": 10},
        ],
    )
    def test_insert_only_options_rejected_elsewhere(self, strategy, option):
        with pytest.raises(ValueError, match="only applies to strategy: insert_only"):
            HistorizeConfig.from_dict(
                {"strategy": strategy, **option}, top_level_primary_key=["id"]
            )


@pytest.mark.unit
class TestInsertOnlyFingerprint:
    def _decoded(self, **historize):
        config = _config(**historize)
        return HistorizeStateManager.decode_fingerprint(
            HistorizeStateManager.compute_fingerprint(config)
        )

    def test_strategy_recorded_and_delivery_pinned(self):
        decoded = self._decoded(delivery="incremental")
        assert decoded["strategy"] == "insert_only"
        assert decoded["delivery"] == "full"

    def test_run_tuning_keys_not_fingerprinted(self):
        plain = self._decoded(partition_column="event_time")
        tuned = self._decoded(
            partition_column="event_time", dedup_window_days=3, batch_max_rows=5
        )
        assert plain == tuned


@pytest.mark.unit
class TestCoalesceDayRanges:
    def test_adjacent_days_merge(self):
        days = [date(2026, 1, 1), date(2026, 1, 2), date(2026, 1, 3)]
        assert coalesce_day_ranges(days, 0) == [(date(2026, 1, 1), date(2026, 1, 4))]

    def test_window_widens_each_day_both_ways(self):
        assert coalesce_day_ranges([date(2026, 1, 10)], 2) == [
            (date(2026, 1, 8), date(2026, 1, 13))
        ]

    def test_distant_days_stay_separate(self):
        days = [date(2026, 1, 1), date(2026, 3, 1)]
        assert coalesce_day_ranges(days, 1) == [
            (date(2025, 12, 31), date(2026, 1, 3)),
            (date(2026, 2, 28), date(2026, 3, 3)),
        ]

    def test_cap_merges_the_closest_ranges(self):
        days = [date(2026, 1, 1), date(2026, 1, 5), date(2026, 6, 1)]
        assert coalesce_day_ranges(days, 0, max_ranges=2) == [
            (date(2026, 1, 1), date(2026, 1, 6)),
            (date(2026, 6, 1), date(2026, 6, 2)),
        ]

    def test_no_days_no_ranges(self):
        assert coalesce_day_ranges([], 3) == []


@pytest.mark.unit
class TestStoredScope:
    def test_ranges_are_constants_on_the_target_column(self):
        scope = _builder(
            partition_column="event_time", dedup_window_days=1
        ).build_stored_scope_sql(["2026-01-10"], "TIMESTAMP")

        assert scope == (
            "(t.`event_time` >= TIMESTAMP '2026-01-09' "
            "AND t.`event_time` < TIMESTAMP '2026-01-12')"
        )

    @pytest.mark.parametrize(
        "column_type, keyword",
        [
            ("DATE", "DATE"),
            ("DATETIME", "DATETIME"),
            ("timestamp_ntz", "TIMESTAMP"),
            ("TIMESTAMP WITH TIME ZONE", "TIMESTAMP"),
        ],
    )
    def test_literal_follows_the_column_type(self, column_type, keyword):
        scope = _builder(
            partition_column="event_time", dedup_window_days=0
        ).build_stored_scope_sql(["2026-01-10"], column_type)

        assert f">= {keyword} '2026-01-10'" in scope

    def test_null_day_adds_the_null_rows(self):
        scope = _builder(
            partition_column="event_time", dedup_window_days=0
        ).build_stored_scope_sql(["2026-01-10", None], "TIMESTAMP")

        assert scope.endswith("OR t.`event_time` IS NULL")

    def test_empty_batch_has_no_scope(self):
        builder = _builder(partition_column="event_time", dedup_window_days=0)
        assert builder.build_stored_scope_sql([], "TIMESTAMP") is None

    def test_other_column_type_rejected(self):
        builder = _builder(partition_column="event_time", dedup_window_days=0)
        with pytest.raises(ValueError, match="date or timestamp"):
            builder.build_stored_scope_sql([], "STRING")


@pytest.mark.unit
class TestInsertOnlySql:
    def test_key_check_reads_only_keys_and_never_updates(self):
        builder = _builder()
        sql = builder.build_insert_only_sql(
            ["payload"], builder.insert_only_source_filter(snapshots=["2026-01-01"])
        )

        assert "NOT EXISTS" in sql
        assert "t.`id` = r.`id`" in sql
        assert "MERGE" not in sql
        assert "UPDATE" not in sql

    def test_one_statement_so_the_batch_is_read_once(self):
        builder = _builder()
        sql = builder.build_insert_only_sql(
            ["payload"], builder.insert_only_source_filter(snapshots=["2026-01-01"])
        )

        assert "TEMP TABLE" not in sql
        assert sql.count("INSERT INTO") == 1
        assert sql.count("FROM proj.ds.src") == 1

    def test_stored_scope_is_added_to_the_key_check(self):
        builder = _builder(partition_column="event_time", dedup_window_days=0)
        scope = builder.build_stored_scope_sql(["2026-01-10"], "TIMESTAMP")
        sql = builder.build_insert_only_sql(
            ["event_time"],
            builder.insert_only_source_filter(snapshots=["2026-01-01"]),
            scope,
        )

        assert f"AND ({scope})" in sql

    def test_late_scope_replaces_the_snapshot_range(self):
        source_filter = _builder().insert_only_source_filter(scope_sql="late = 1")

        assert source_filter == "late = 1"
