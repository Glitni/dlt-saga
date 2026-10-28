"""Unit tests for historize ``delivery: full | incremental`` config handling."""

import base64
import json

import pytest

from dlt_saga.historize.config import HistorizeConfig
from dlt_saga.historize.state import HistorizeStateManager


def _config(**historize):
    return HistorizeConfig.from_dict(historize, top_level_primary_key=["id"])


@pytest.mark.unit
class TestDelivery:
    def test_defaults_to_full(self):
        config = _config()
        assert config.delivery == "full"
        assert config.is_full_delivery

    def test_unknown_value_rejected(self):
        with pytest.raises(ValueError, match="delivery must be one of"):
            _config(delivery="partial")

    def test_track_deletions_off_by_default_for_incremental(self):
        assert _config(delivery="incremental").track_deletions is False

    def test_track_deletions_still_on_by_default_for_full(self):
        assert _config(delivery="full").track_deletions is True

    @pytest.mark.parametrize(
        "option", [{"track_deletions": True}, {"merge_key": ["id"]}]
    )
    def test_absence_based_options_rejected_for_incremental(self, option):
        key = next(iter(option))
        with pytest.raises(
            ValueError,
            match=f"historize.{key} is not supported with delivery: incremental",
        ):
            _config(delivery="incremental", **option)

    def test_explicit_track_deletions_false_allowed_for_incremental(self):
        assert _config(delivery="incremental", track_deletions=False).delivery == (
            "incremental"
        )

    def test_accepted_with_scd1(self):
        assert _config(strategy="scd1", delivery="incremental").is_scd1


@pytest.mark.unit
class TestDeliveryFingerprint:
    def _fp(self, **historize):
        return HistorizeStateManager.decode_fingerprint(
            HistorizeStateManager.compute_fingerprint(_config(**historize))
        )

    def test_change_detected(self):
        assert self._fp()["delivery"] != self._fp(delivery="incremental")["delivery"]

    def test_pre_delivery_fingerprint_reads_as_full(self):
        # track_deletions off on both sides, so only delivery differs.
        legacy = self._fp(track_deletions=False)
        del legacy["delivery"]
        encoded = base64.b64encode(json.dumps(legacy).encode()).decode()

        manager = HistorizeStateManager.__new__(HistorizeStateManager)
        state = HistorizeStateManager.PipelineState(
            last_snapshot_value="2026-01-01 00:00:00",
            config_fingerprint=encoded,
            has_successful_run=True,
        )

        assert not manager.config_changed(state, _config(track_deletions=False))
        assert manager.config_changed(
            state, _config(delivery="incremental", track_deletions=False)
        )

    def test_scd1_fingerprint_ignores_delivery(self):
        """scd1 output doesn't depend on delivery, so changing it forces no rebuild."""
        full = self._fp(strategy="scd1")
        incremental = self._fp(strategy="scd1", delivery="incremental")
        assert full == incremental
