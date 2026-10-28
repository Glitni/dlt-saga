"""Unit tests for historize ``strategy: scd1`` config and fingerprint handling."""

import base64
import json

import pytest

from dlt_saga.historize.config import HistorizeConfig
from dlt_saga.historize.state import HistorizeStateManager


def _config(**historize):
    return HistorizeConfig.from_dict(historize, top_level_primary_key=["id"])


@pytest.mark.unit
class TestStrategy:
    def test_defaults_to_scd2(self):
        config = _config()
        assert config.strategy == "scd2"
        assert not config.is_scd1

    def test_unknown_strategy_rejected(self):
        with pytest.raises(ValueError, match="strategy must be one of"):
            _config(strategy="scd3")

    def test_track_deletions_defaults_follow_strategy(self):
        assert _config().track_deletions is True
        assert _config(strategy="scd1").track_deletions is False

    def test_explicit_track_deletions_false_kept_for_scd2(self):
        assert _config(track_deletions=False).track_deletions is False


@pytest.mark.unit
class TestScd1RejectsScd2Options:
    @pytest.mark.parametrize(
        "option",
        [
            {"merge_key": ["id"]},
            {"track_columns": ["payload"]},
            {"ignore_columns": ["payload"]},
        ],
    )
    def test_rejected(self, option):
        key = next(iter(option))
        with pytest.raises(ValueError, match=f"historize.{key} is not supported"):
            _config(strategy="scd1", **option)

    def test_track_deletions_false_allowed(self):
        assert _config(strategy="scd1", track_deletions=False).is_scd1


@pytest.mark.unit
class TestOrderBy:
    def test_defaults_to_snapshot_column(self):
        config = _config(strategy="scd1", snapshot_column="ingested_at")
        assert config.effective_order_by == ["ingested_at"]

    def test_snapshot_column_appended_as_tiebreaker(self):
        config = _config(
            strategy="scd1", snapshot_column="ingested_at", order_by=["event_time"]
        )
        assert config.effective_order_by == ["event_time", "ingested_at"]

    def test_snapshot_column_not_duplicated(self):
        config = _config(
            strategy="scd1",
            snapshot_column="ingested_at",
            order_by=["ingested_at", "landing_offset"],
        )
        assert config.effective_order_by == ["ingested_at", "landing_offset"]

    def test_string_normalized_to_list(self):
        assert _config(strategy="scd1", order_by="event_time").order_by == [
            "event_time"
        ]

    def test_empty_list_rejected(self):
        with pytest.raises(ValueError, match="non-empty list"):
            _config(strategy="scd1", order_by=[])

    def test_rejected_under_scd2(self):
        with pytest.raises(ValueError, match="only applies to strategy: scd1"):
            _config(order_by=["event_time"])

    def test_invalid_identifier_rejected(self):
        with pytest.raises(ValueError, match="order_by contains invalid SQL"):
            _config(strategy="scd1", order_by=["event_time; DROP"])

    def test_scd2_has_no_effective_order(self):
        assert _config().effective_order_by == []


@pytest.mark.unit
class TestFingerprint:
    def _fp(self, **historize):
        return HistorizeStateManager.decode_fingerprint(
            HistorizeStateManager.compute_fingerprint(_config(**historize))
        )

    def test_strategy_change_detected(self):
        assert self._fp()["strategy"] != self._fp(strategy="scd1")["strategy"]

    def test_order_by_change_detected(self):
        a = self._fp(strategy="scd1", order_by=["event_time"])
        b = self._fp(strategy="scd1", order_by=["landing_offset"])
        assert a["order_by"] != b["order_by"]

    def test_pre_strategy_fingerprint_reads_as_scd2(self):
        """A fingerprint stored before strategy existed must still flag a switch."""
        legacy = self._fp()
        del legacy["strategy"], legacy["order_by"]
        encoded = base64.b64encode(json.dumps(legacy).encode()).decode()

        manager = HistorizeStateManager.__new__(HistorizeStateManager)
        state = HistorizeStateManager.PipelineState(
            last_snapshot_value="2026-01-01 00:00:00",
            config_fingerprint=encoded,
            has_successful_run=True,
        )

        assert not manager.config_changed(state, _config())
        assert manager.config_changed(state, _config(strategy="scd1"))


@pytest.mark.unit
class TestScd1RejectsRenamedScd2Columns:
    @pytest.mark.parametrize("key", ["valid_to_column", "is_deleted_column"])
    def test_rejected_when_renamed(self, key):
        with pytest.raises(ValueError, match=f"historize.{key} is not supported"):
            _config(strategy="scd1", **{key: "renamed"})

    def test_valid_from_rename_allowed(self):
        assert _config(strategy="scd1", valid_from_column="since").is_scd1

    def test_mixed_case_snapshot_in_order_by_not_duplicated(self):
        config = _config(
            strategy="scd1", snapshot_column="ingested_at", order_by=["Ingested_At"]
        )
        assert config.effective_order_by == ["Ingested_At"]


@pytest.mark.unit
class TestScd1Deletions:
    def test_track_deletions_allowed(self):
        config = _config(strategy="scd1", track_deletions=True)
        assert config.track_deletions is True

    def test_merge_key_allowed_with_deletions(self):
        config = _config(strategy="scd1", track_deletions=True, merge_key=["id"])
        assert config.merge_key == ["id"]

    def test_is_deleted_rename_allowed_with_deletions(self):
        config = _config(
            strategy="scd1", track_deletions=True, is_deleted_column="deleted"
        )
        assert config.is_deleted_column == "deleted"

    def test_order_by_not_led_by_snapshot_rejected_with_deletions(self):
        with pytest.raises(ValueError, match=r"order_by: \[loaded_at, event_time\]"):
            _config(
                strategy="scd1",
                track_deletions=True,
                snapshot_column="loaded_at",
                order_by=["event_time", "loaded_at"],
            )

    def test_order_by_led_by_snapshot_allowed_with_deletions(self):
        config = _config(
            strategy="scd1",
            track_deletions=True,
            snapshot_column="loaded_at",
            order_by=["Loaded_At", "event_time"],
        )
        assert config.effective_order_by == ["Loaded_At", "event_time"]

    def test_order_by_naming_only_the_snapshot_column_allowed(self):
        config = _config(
            strategy="scd1",
            track_deletions=True,
            snapshot_column="loaded_at",
            order_by=["Loaded_At"],
        )
        assert config.track_deletions is True

    def test_rejected_with_incremental_delivery(self):
        with pytest.raises(ValueError, match="delivery: incremental"):
            _config(strategy="scd1", track_deletions=True, delivery="incremental")
