"""Unit tests for the date-window incremental API pipeline."""

import logging
from datetime import date, datetime

import pytest

from dlt_saga.pipelines.api.date_window.config import DateWindowApiConfig
from dlt_saga.pipelines.api.date_window.pipeline import DateWindowApiPipeline

# ---------------------------------------------------------------------------
# Test doubles
# ---------------------------------------------------------------------------


class _FakeDestination:
    def __init__(self, max_value=None):
        self._max_value = max_value
        self.calls = []

    def get_max_column_value(self, table_id, column):
        self.calls.append((table_id, column))
        return self._max_value


class _Pipeline:
    dataset_name = "dlt_api"


class FakeDateWindowPipeline(DateWindowApiPipeline):
    """Bypasses BasePipeline.__init__; canned responses for _make_request."""

    def __init__(self, config_dict, responses=None, max_value=None):
        self.api_config = self._create_api_config(config_dict)
        self.logger = logging.getLogger("test")
        self.table_name = "events"
        self.destination_database = "proj"
        self.destination = _FakeDestination(max_value)
        self.pipeline = _Pipeline()
        self._responses = list(responses or [])
        self._call_count = 0
        self._calls = []

    def _make_request(self, url=None, query_params=None):
        # Snapshot the params actually used for this request.
        self._calls.append(dict(self.api_config.query_params or {}))
        resp = self._responses[self._call_count]
        self._call_count += 1
        return resp


def _base_cfg(**overrides):
    cfg = {
        "base_url": "https://api.example.com",
        "endpoint": "/events",
        "incremental_column": "created_at",
    }
    cfg.update(overrides)
    return cfg


# ---------------------------------------------------------------------------
# Config validation
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestDateWindowConfig:
    def test_defaults(self):
        cfg = DateWindowApiConfig(**_base_cfg())
        assert cfg.incremental is True
        assert cfg.overlap == 1
        assert cfg.window_end == "today"
        assert cfg.on_first_run == "error"
        assert cfg.date_format == "%Y-%m-%d"

    def test_incremental_column_required(self):
        with pytest.raises(ValueError, match="incremental_column is required"):
            DateWindowApiConfig(base_url="https://x.test", endpoint="/e")

    def test_negative_overlap_rejected(self):
        with pytest.raises(ValueError, match="overlap must be >= 0"):
            DateWindowApiConfig(**_base_cfg(overlap=-1))

    def test_bad_window_end_rejected(self):
        with pytest.raises(ValueError, match="window_end must be one of"):
            DateWindowApiConfig(**_base_cfg(window_end="tomorrow"))

    def test_bad_on_first_run_rejected(self):
        with pytest.raises(ValueError, match="on_first_run must be one of"):
            DateWindowApiConfig(**_base_cfg(on_first_run="whenever"))


# ---------------------------------------------------------------------------
# Window resolution
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestResolveWindow:
    def test_override_wins(self):
        p = FakeDateWindowPipeline(
            _base_cfg(
                start_value_override="2024-03-01", end_value_override="2024-03-05"
            ),
            max_value="2024-06-01",
        )
        start, end = p.resolve_window()
        assert (start, end) == (date(2024, 3, 1), date(2024, 3, 5))
        # Watermark not consulted when an override is present
        assert p.destination.calls == []

    def test_watermark_with_default_overlap(self):
        p = FakeDateWindowPipeline(
            _base_cfg(window_end="yesterday"), max_value="2024-06-10"
        )
        start, _end = p.resolve_window()
        # overlap=1 -> reload just the watermark day
        assert start == date(2024, 6, 10)

    def test_watermark_with_multiday_overlap(self):
        p = FakeDateWindowPipeline(_base_cfg(overlap=3), max_value="2024-06-10")
        start, _end = p.resolve_window()
        assert start == date(2024, 6, 8)

    def test_overlap_zero_resumes_after_watermark(self):
        p = FakeDateWindowPipeline(_base_cfg(overlap=0), max_value="2024-06-10")
        start, _end = p.resolve_window()
        assert start == date(2024, 6, 11)

    def test_watermark_accepts_datetime(self):
        p = FakeDateWindowPipeline(_base_cfg(), max_value=datetime(2024, 6, 10, 14, 30))
        start, _end = p.resolve_window()
        assert start == date(2024, 6, 10)

    def test_first_run_initial_value(self):
        p = FakeDateWindowPipeline(
            _base_cfg(initial_value="2024-01-01"), max_value=None
        )
        start, _end = p.resolve_window()
        assert start == date(2024, 1, 1)

    def test_first_run_error_by_default(self):
        p = FakeDateWindowPipeline(_base_cfg(), max_value=None)
        with pytest.raises(ValueError, match="no initial_value configured"):
            p.resolve_window()

    def test_first_run_today_policy(self):
        p = FakeDateWindowPipeline(_base_cfg(on_first_run="today"), max_value=None)
        start, end = p.resolve_window()
        assert start == end  # today..today

    def test_non_incremental_loads_full_range_from_initial_value(self):
        p = FakeDateWindowPipeline(
            _base_cfg(
                incremental=False,
                initial_value="2024-01-01",
                window_end="yesterday",
            ),
            max_value="2024-06-10",
        )
        start, end = p.resolve_window()
        assert start == date(2024, 1, 1)
        assert end > start  # whole range, not a single day
        # Watermark must not be consulted in non-incremental mode
        assert p.destination.calls == []

    def test_non_incremental_requires_initial_value(self):
        p = FakeDateWindowPipeline(_base_cfg(incremental=False), max_value=None)
        with pytest.raises(ValueError, match="reloads the full range"):
            p.resolve_window()

    def test_non_incremental_still_honors_override(self):
        p = FakeDateWindowPipeline(
            _base_cfg(
                incremental=False,
                start_value_override="2024-03-01",
                end_value_override="2024-03-05",
            ),
        )
        assert p.resolve_window() == (date(2024, 3, 1), date(2024, 3, 5))

    def test_window_end_today_vs_yesterday(self):
        today_p = FakeDateWindowPipeline(
            _base_cfg(initial_value="2024-01-01"), max_value=None
        )
        _s, end_today = today_p.resolve_window()
        yday_p = FakeDateWindowPipeline(
            _base_cfg(initial_value="2024-01-01", window_end="yesterday"),
            max_value=None,
        )
        _s2, end_yday = yday_p.resolve_window()
        assert (end_today - end_yday).days == 1


# ---------------------------------------------------------------------------
# Fetch
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestFetchWindow:
    def test_single_window_injects_params(self):
        p = FakeDateWindowPipeline(
            _base_cfg(
                start_value_override="2024-03-01",
                end_value_override="2024-03-03",
                start_param="from",
                end_param="to",
            ),
            responses=[[{"id": 1}, {"id": 2}]],
        )
        resources = p.extract_data()
        # One request for the whole window
        assert len(p._calls) == 1
        assert p._calls[0] == {"from": "2024-03-01", "to": "2024-03-03"}
        resource, _desc = resources[0]
        assert [r["id"] for r in resource] == [1, 2]

    def test_end_param_exclusive_adds_a_day(self):
        p = FakeDateWindowPipeline(
            _base_cfg(
                start_value_override="2024-03-01",
                end_value_override="2024-03-03",
                start_param="from",
                end_param="to",
                end_param_inclusive=False,
            ),
            responses=[[]],
        )
        p.extract_data()
        assert p._calls[0]["to"] == "2024-03-04"

    def test_per_period_requests_one_per_day(self):
        p = FakeDateWindowPipeline(
            _base_cfg(
                start_value_override="2024-03-01",
                end_value_override="2024-03-03",
                start_param="date",
                per_period_requests=True,
            ),
            responses=[[{"id": 1}], [{"id": 2}], [{"id": 3}]],
        )
        resources = p.extract_data()
        assert [c["date"] for c in p._calls] == [
            "2024-03-01",
            "2024-03-02",
            "2024-03-03",
        ]
        resource, _desc = resources[0]
        assert [r["id"] for r in resource] == [1, 2, 3]

    def test_preserves_other_query_params(self):
        p = FakeDateWindowPipeline(
            _base_cfg(
                start_value_override="2024-03-01",
                end_value_override="2024-03-01",
                start_param="from",
                end_param="to",
                query_params={"status": "active"},
            ),
            responses=[[]],
        )
        p.extract_data()
        assert p._calls[0]["status"] == "active"

    def test_custom_date_format(self):
        p = FakeDateWindowPipeline(
            _base_cfg(
                start_value_override="2024-03-01",
                end_value_override="2024-03-01",
                start_param="from",
                date_format="%Y%m%d",
            ),
            responses=[[]],
        )
        p.extract_data()
        assert p._calls[0]["from"] == "20240301"

    def test_missing_params_raises(self):
        p = FakeDateWindowPipeline(
            _base_cfg(
                start_value_override="2024-03-01", end_value_override="2024-03-01"
            ),
            responses=[[]],
        )
        with pytest.raises(ValueError, match="needs start_param and/or end_param"):
            p.extract_data()

    def test_empty_window_loads_nothing(self):
        # start (override) after end -> no request, empty resource
        p = FakeDateWindowPipeline(
            _base_cfg(
                start_value_override="2024-03-10",
                end_value_override="2024-03-01",
                start_param="from",
            ),
            responses=[[{"id": 1}]],
        )
        resources = p.extract_data()
        assert p._calls == []
        resource, _desc = resources[0]
        assert list(resource) == []


# ---------------------------------------------------------------------------
# Custom _fetch_window override (e.g. report-body APIs)
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestFetchWindowOverride:
    def test_subclass_can_override_fetch_window(self):
        class ReportPipeline(FakeDateWindowPipeline):
            def _fetch_window(self, start, end):
                # No start_param/end_param needed — subclass owns the request.
                self.seen_window = (start, end)
                return [{"start": start.isoformat(), "end": end.isoformat()}]

        p = ReportPipeline(
            _base_cfg(
                start_value_override="2024-03-01", end_value_override="2024-03-05"
            ),
        )
        resources = p.extract_data()
        assert p.seen_window == (date(2024, 3, 1), date(2024, 3, 5))
        resource, _desc = resources[0]
        assert list(resource) == [{"start": "2024-03-01", "end": "2024-03-05"}]
