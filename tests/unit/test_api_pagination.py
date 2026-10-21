"""Unit tests for API pipeline pagination."""

import json
import logging
from contextlib import contextmanager
from unittest.mock import patch

import pytest
import requests
from requests.adapters import HTTPAdapter
from requests.exceptions import ChunkedEncodingError

from dlt_saga.pipelines.api.base import ApiRequestError, BaseApiPipeline
from dlt_saga.pipelines.api.config import ApiConfig
from dlt_saga.utility.secrets import redaction

# ---------------------------------------------------------------------------
# Config validation
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestPaginationConfigValidation:
    def test_missing_type_raises(self):
        with pytest.raises(ValueError, match="pagination.type is required"):
            ApiConfig(
                base_url="https://api.example.com",
                endpoint="/data",
                pagination={"limit": 100},
            )

    def test_invalid_type_raises(self):
        with pytest.raises(ValueError, match="pagination.type must be one of"):
            ApiConfig(
                base_url="https://api.example.com",
                endpoint="/data",
                pagination={"type": "magic"},
            )

    def test_offset_requires_limit(self):
        with pytest.raises(ValueError, match="pagination.limit is required"):
            ApiConfig(
                base_url="https://api.example.com",
                endpoint="/data",
                pagination={"type": "offset"},
            )

    def test_page_requires_limit(self):
        with pytest.raises(ValueError, match="pagination.limit is required"):
            ApiConfig(
                base_url="https://api.example.com",
                endpoint="/data",
                pagination={"type": "page"},
            )

    def test_cursor_requires_cursor_path(self):
        with pytest.raises(ValueError, match="pagination.cursor_path is required"):
            ApiConfig(
                base_url="https://api.example.com",
                endpoint="/data",
                pagination={"type": "cursor"},
            )

    def test_next_url_requires_next_url_path(self):
        with pytest.raises(ValueError, match="pagination.next_url_path is required"):
            ApiConfig(
                base_url="https://api.example.com",
                endpoint="/data",
                pagination={"type": "next_url"},
            )

    def test_valid_offset_config(self):
        cfg = ApiConfig(
            base_url="https://api.example.com",
            endpoint="/data",
            pagination={"type": "offset", "limit": 100},
        )
        assert cfg.pagination["type"] == "offset"

    def test_valid_cursor_config(self):
        cfg = ApiConfig(
            base_url="https://api.example.com",
            endpoint="/data",
            pagination={"type": "cursor", "cursor_path": "meta.next"},
        )
        assert cfg.pagination["type"] == "cursor"

    def test_no_pagination_is_valid(self):
        cfg = ApiConfig(
            base_url="https://api.example.com",
            endpoint="/data",
        )
        assert cfg.pagination is None


# ---------------------------------------------------------------------------
# _resolve_json_path
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestResolveJsonPath:
    def test_simple_key(self):
        assert BaseApiPipeline._resolve_json_path({"total": 42}, "total") == 42

    def test_nested_path(self):
        data = {"meta": {"pagination": {"next": "abc"}}}
        assert BaseApiPipeline._resolve_json_path(data, "meta.pagination.next") == "abc"

    def test_missing_key_returns_none(self):
        assert BaseApiPipeline._resolve_json_path({"a": 1}, "b") is None

    def test_missing_nested_key_returns_none(self):
        assert BaseApiPipeline._resolve_json_path({"a": {"b": 1}}, "a.c") is None

    def test_non_dict_intermediate_returns_none(self):
        assert BaseApiPipeline._resolve_json_path({"a": "string"}, "a.b") is None


# ---------------------------------------------------------------------------
# Pagination strategies (mock _make_request)
# ---------------------------------------------------------------------------


class FakeApiPipeline(BaseApiPipeline):
    """Test double that replaces HTTP calls with canned responses."""

    def __init__(self, config_dict, responses):
        # Bypass BasePipeline.__init__ entirely — we only test pagination logic
        self.api_config = self._create_api_config(config_dict)
        self.logger = logging.getLogger("test")
        self._responses = list(responses)
        self._call_count = 0
        self._calls = []

    def _make_request(self, url=None, query_params=None):
        self._calls.append({"url": url, "query_params": query_params})
        if self._call_count >= len(self._responses):
            raise ValueError("No more canned responses")
        resp = self._responses[self._call_count]
        self._call_count += 1
        return resp


@pytest.mark.unit
class TestOffsetPagination:
    def test_basic_offset(self):
        pipeline = FakeApiPipeline(
            {
                "base_url": "https://api.example.com",
                "endpoint": "/items",
                "pagination": {"type": "offset", "limit": 2},
            },
            [
                {"data": [{"id": 1}, {"id": 2}]},
                {"data": [{"id": 3}]},  # short page — NOT necessarily the last
                {"data": []},  # empty page terminates
            ],
        )
        pipeline.api_config.response_path = "data"

        records = list(pipeline._fetch_all_pages())
        assert [r["id"] for r in records] == [1, 2, 3]
        # Offset advances by the actual page size (2, then 1), not the limit.
        assert [c["query_params"]["offset"] for c in pipeline._calls] == [0, 2, 3]

    def test_offset_capped_page_size_not_truncated(self):
        # The server caps the page size at 2 even though limit=5 is requested. A
        # short page must not end iteration (that was the truncation bug) — the
        # loop continues, advancing by the real page size, until an empty page.
        pipeline = FakeApiPipeline(
            {
                "base_url": "https://api.example.com",
                "endpoint": "/items",
                "pagination": {"type": "offset", "limit": 5},
            },
            [
                {"data": [{"id": 1}, {"id": 2}]},
                {"data": [{"id": 3}, {"id": 4}]},
                {"data": [{"id": 5}]},
                {"data": []},
            ],
        )
        pipeline.api_config.response_path = "data"

        records = list(pipeline._fetch_all_pages())
        assert [r["id"] for r in records] == [1, 2, 3, 4, 5]
        # Advanced by real page size (2, 2, 1), not the requested limit of 5.
        assert [c["query_params"]["offset"] for c in pipeline._calls] == [0, 2, 4, 5]

    def test_offset_stops_on_empty(self):
        pipeline = FakeApiPipeline(
            {
                "base_url": "https://api.example.com",
                "endpoint": "/items",
                "pagination": {"type": "offset", "limit": 10},
            },
            [{"data": []}],
        )
        pipeline.api_config.response_path = "data"

        records = list(pipeline._fetch_all_pages())
        assert records == []

    def test_offset_with_total_path(self):
        pipeline = FakeApiPipeline(
            {
                "base_url": "https://api.example.com",
                "endpoint": "/items",
                "pagination": {
                    "type": "offset",
                    "limit": 2,
                    "total_path": "meta.total",
                },
            },
            [
                {"data": [{"id": 1}, {"id": 2}], "meta": {"total": 3}},
                {"data": [{"id": 3}], "meta": {"total": 3}},
            ],
        )
        pipeline.api_config.response_path = "data"

        records = list(pipeline._fetch_all_pages())
        assert len(records) == 3
        # Should stop after page 2 because offset(2) + limit(2) >= total(3)
        assert pipeline._call_count == 2

    def test_offset_custom_params(self):
        pipeline = FakeApiPipeline(
            {
                "base_url": "https://api.example.com",
                "endpoint": "/items",
                "pagination": {
                    "type": "offset",
                    "limit": 5,
                    "offset_param": "skip",
                    "limit_param": "take",
                    "start_offset": 10,
                },
            },
            [{"data": []}],
        )
        pipeline.api_config.response_path = "data"

        list(pipeline._fetch_all_pages())
        assert pipeline._calls[0]["query_params"]["skip"] == 10
        assert pipeline._calls[0]["query_params"]["take"] == 5

    def test_offset_preserves_base_query_params(self):
        pipeline = FakeApiPipeline(
            {
                "base_url": "https://api.example.com",
                "endpoint": "/items",
                "query_params": {"status": "active"},
                "pagination": {"type": "offset", "limit": 10},
            },
            [{"data": []}],
        )
        pipeline.api_config.response_path = "data"

        list(pipeline._fetch_all_pages())
        assert pipeline._calls[0]["query_params"]["status"] == "active"


@pytest.mark.unit
class TestPagePagination:
    def test_basic_page(self):
        pipeline = FakeApiPipeline(
            {
                "base_url": "https://api.example.com",
                "endpoint": "/items",
                "pagination": {"type": "page", "limit": 2},
            },
            [
                {"results": [{"id": 1}, {"id": 2}]},
                {"results": [{"id": 3}]},
            ],
        )
        pipeline.api_config.response_path = "results"

        records = list(pipeline._fetch_all_pages())
        assert [r["id"] for r in records] == [1, 2, 3]
        assert pipeline._calls[0]["query_params"]["page"] == 1
        assert pipeline._calls[1]["query_params"]["page"] == 2

    def test_page_custom_start(self):
        pipeline = FakeApiPipeline(
            {
                "base_url": "https://api.example.com",
                "endpoint": "/items",
                "pagination": {
                    "type": "page",
                    "limit": 10,
                    "start_page": 0,
                    "page_param": "p",
                    "page_size_param": "size",
                },
            },
            [{"data": []}],
        )
        pipeline.api_config.response_path = "data"

        list(pipeline._fetch_all_pages())
        assert pipeline._calls[0]["query_params"]["p"] == 0
        assert pipeline._calls[0]["query_params"]["size"] == 10


@pytest.mark.unit
class TestCursorPagination:
    def test_basic_cursor(self):
        pipeline = FakeApiPipeline(
            {
                "base_url": "https://api.example.com",
                "endpoint": "/items",
                "pagination": {
                    "type": "cursor",
                    "cursor_path": "meta.next_cursor",
                },
            },
            [
                {"items": [{"id": 1}], "meta": {"next_cursor": "abc"}},
                {"items": [{"id": 2}], "meta": {"next_cursor": "def"}},
                {"items": [{"id": 3}], "meta": {"next_cursor": None}},
            ],
        )
        pipeline.api_config.response_path = "items"

        records = list(pipeline._fetch_all_pages())
        assert [r["id"] for r in records] == [1, 2, 3]
        # First call has no cursor, subsequent calls pass cursor
        assert "cursor" not in (pipeline._calls[0]["query_params"] or {})
        assert pipeline._calls[1]["query_params"]["cursor"] == "abc"
        assert pipeline._calls[2]["query_params"]["cursor"] == "def"

    def test_cursor_stops_on_empty_string(self):
        pipeline = FakeApiPipeline(
            {
                "base_url": "https://api.example.com",
                "endpoint": "/items",
                "pagination": {
                    "type": "cursor",
                    "cursor_path": "next",
                },
            },
            [
                {"data": [{"id": 1}], "next": ""},
            ],
        )
        pipeline.api_config.response_path = "data"

        records = list(pipeline._fetch_all_pages())
        assert len(records) == 1
        assert pipeline._call_count == 1

    def test_cursor_with_limit(self):
        pipeline = FakeApiPipeline(
            {
                "base_url": "https://api.example.com",
                "endpoint": "/items",
                "pagination": {
                    "type": "cursor",
                    "cursor_path": "next",
                    "limit": 50,
                    "limit_param": "count",
                },
            },
            [{"data": [{"id": 1}], "next": None}],
        )
        pipeline.api_config.response_path = "data"

        list(pipeline._fetch_all_pages())
        assert pipeline._calls[0]["query_params"]["count"] == 50


@pytest.mark.unit
class TestNextUrlPagination:
    def test_basic_next_url(self):
        pipeline = FakeApiPipeline(
            {
                "base_url": "https://api.example.com",
                "endpoint": "/items",
                "pagination": {
                    "type": "next_url",
                    "next_url_path": "links.next",
                },
            },
            [
                {
                    "data": [{"id": 1}],
                    "links": {"next": "https://api.example.com/items?page=2"},
                },
                {
                    "data": [{"id": 2}],
                    "links": {"next": None},
                },
            ],
        )
        pipeline.api_config.response_path = "data"

        records = list(pipeline._fetch_all_pages())
        assert [r["id"] for r in records] == [1, 2]
        # First call uses default URL, second uses next_url
        assert pipeline._calls[0]["url"] is None
        assert pipeline._calls[1]["url"] == "https://api.example.com/items?page=2"

    def test_next_url_stops_on_missing_key(self):
        pipeline = FakeApiPipeline(
            {
                "base_url": "https://api.example.com",
                "endpoint": "/items",
                "pagination": {
                    "type": "next_url",
                    "next_url_path": "paging.next_link",
                },
            },
            [
                {"data": [{"id": 1}]},  # no "paging" key at all
            ],
        )
        pipeline.api_config.response_path = "data"

        records = list(pipeline._fetch_all_pages())
        assert len(records) == 1


@pytest.mark.unit
class TestPaginationStopCondition:
    def test_next_url_stops_on_truthy_stop_path(self):
        """The API keeps returning a next URL, but a truthy is_last flag stops
        pagination after the current page."""
        pipeline = FakeApiPipeline(
            {
                "base_url": "https://api.example.com",
                "endpoint": "/items",
                "pagination": {
                    "type": "next_url",
                    "next_url_path": "links.next",
                    "stop_path": "meta.is_last",
                },
            },
            [
                {
                    "data": [{"id": 1}],
                    "links": {"next": "https://api.example.com/items?page=2"},
                    "meta": {"is_last": False},
                },
                {
                    "data": [{"id": 2}],
                    # next URL is STILL present — only the flag ends pagination
                    "links": {"next": "https://api.example.com/items?page=3"},
                    "meta": {"is_last": True},
                },
            ],
        )
        pipeline.api_config.response_path = "data"

        records = list(pipeline._fetch_all_pages())
        assert [r["id"] for r in records] == [1, 2]
        assert pipeline._call_count == 2  # did not fetch page 3

    def test_next_url_stops_on_stop_value_match(self):
        """has_more: false shape — stop when the value equals stop_value."""
        pipeline = FakeApiPipeline(
            {
                "base_url": "https://api.example.com",
                "endpoint": "/items",
                "pagination": {
                    "type": "next_url",
                    "next_url_path": "links.next",
                    "stop_path": "has_more",
                    "stop_value": False,
                },
            },
            [
                {
                    "data": [{"id": 1}],
                    "links": {"next": "https://api.example.com/items?page=2"},
                    "has_more": True,
                },
                {
                    "data": [{"id": 2}],
                    "links": {"next": "https://api.example.com/items?page=3"},
                    "has_more": False,
                },
            ],
        )
        pipeline.api_config.response_path = "data"

        records = list(pipeline._fetch_all_pages())
        assert [r["id"] for r in records] == [1, 2]
        assert pipeline._call_count == 2

    def test_cursor_honours_stop_path(self):
        pipeline = FakeApiPipeline(
            {
                "base_url": "https://api.example.com",
                "endpoint": "/items",
                "pagination": {
                    "type": "cursor",
                    "cursor_path": "meta.next",
                    "stop_path": "meta.done",
                },
            },
            [
                {"data": [{"id": 1}], "meta": {"next": "c2", "done": False}},
                # cursor still present, but done flag stops us
                {"data": [{"id": 2}], "meta": {"next": "c3", "done": True}},
            ],
        )
        pipeline.api_config.response_path = "data"

        records = list(pipeline._fetch_all_pages())
        assert [r["id"] for r in records] == [1, 2]
        assert pipeline._call_count == 2

    def test_stop_value_requires_stop_path(self):
        with pytest.raises(ValueError, match="stop_value requires .*stop_path"):
            ApiConfig(
                base_url="https://api.example.com",
                endpoint="/items",
                pagination={
                    "type": "next_url",
                    "next_url_path": "links.next",
                    "stop_value": False,
                },
            )

    def test_stop_path_must_be_string(self):
        with pytest.raises(ValueError, match="stop_path must be a string"):
            ApiConfig(
                base_url="https://api.example.com",
                endpoint="/items",
                pagination={
                    "type": "next_url",
                    "next_url_path": "links.next",
                    "stop_path": ["not", "a", "string"],
                },
            )


# ---------------------------------------------------------------------------
# Incremental loading
# ---------------------------------------------------------------------------


class _FakeDestination:
    """Destination double returning a canned high-water mark."""

    def __init__(self, max_value):
        self._max_value = max_value
        self.calls = []

    def get_max_column_value(self, table_id, column):
        self.calls.append((table_id, column))
        return self._max_value


class IncrementalApiPipeline(BaseApiPipeline):
    """Test double exposing the incremental machinery without HTTP/BasePipeline."""

    def __init__(self, config_dict, max_value=None):
        self.api_config = self._create_api_config(config_dict)
        self.logger = logging.getLogger("test")
        self.table_name = "events"
        self.destination_database = "proj"
        self.destination = _FakeDestination(max_value)

        class _Pipeline:
            dataset_name = "dlt_api"

        self.pipeline = _Pipeline()


@pytest.mark.unit
class TestIncrementalFilter:
    def test_no_op_when_incremental_disabled(self):
        pipeline = IncrementalApiPipeline(
            {
                "base_url": "https://api.example.com",
                "endpoint": "/events",
                "query_params": {"updated_since": "{incremental_value}"},
            }
        )
        pipeline._apply_incremental_filter()
        # Placeholder left untouched — incremental off
        assert (
            pipeline.api_config.query_params["updated_since"] == "{incremental_value}"
        )
        assert pipeline.destination.calls == []

    def test_substitutes_watermark_from_destination(self):
        pipeline = IncrementalApiPipeline(
            {
                "base_url": "https://api.example.com",
                "endpoint": "/events",
                "incremental": True,
                "incremental_column": "created_at",
                "initial_value": "2024-01-01",
                "query_params": {"updated_since": "{incremental_value}"},
            },
            max_value="2024-06-15",
        )
        pipeline._apply_incremental_filter()
        assert pipeline.api_config.query_params["updated_since"] == "2024-06-15"
        assert pipeline.destination.calls == [("proj.dlt_api.events", "created_at")]

    def test_start_value_override_wins_over_watermark(self):
        pipeline = IncrementalApiPipeline(
            {
                "base_url": "https://api.example.com",
                "endpoint": "/events",
                "incremental": True,
                "incremental_column": "created_at",
                "initial_value": "2024-01-01",
                "start_value_override": "2023-03-01",  # backfill
                "query_params": {"updated_since": "{incremental_value}"},
            },
            max_value="2024-06-15",
        )
        pipeline._apply_incremental_filter()
        # Override takes precedence over both the watermark and initial_value
        assert pipeline.api_config.query_params["updated_since"] == "2023-03-01"
        assert pipeline.destination.calls == []

    def test_falls_back_to_initial_value_on_first_run(self):
        pipeline = IncrementalApiPipeline(
            {
                "base_url": "https://api.example.com",
                "endpoint": "/events",
                "incremental": True,
                "incremental_column": "created_at",
                "initial_value": "2024-01-01",
                "query_params": {"updated_since": "{incremental_value}"},
            },
            max_value=None,
        )
        pipeline._apply_incremental_filter()
        assert pipeline.api_config.query_params["updated_since"] == "2024-01-01"

    def test_drops_placeholder_when_no_watermark(self):
        pipeline = IncrementalApiPipeline(
            {
                "base_url": "https://api.example.com",
                "endpoint": "/events",
                "incremental": True,
                "incremental_column": "created_at",
                # no initial_value, empty destination -> full load
                "query_params": {
                    "updated_since": "{incremental_value}",
                    "status": "active",
                },
            },
            max_value=None,
        )
        pipeline._apply_incremental_filter()
        assert "updated_since" not in pipeline.api_config.query_params
        # Unrelated params preserved
        assert pipeline.api_config.query_params["status"] == "active"

    def test_preserves_other_params_and_column_placeholder(self):
        pipeline = IncrementalApiPipeline(
            {
                "base_url": "https://api.example.com",
                "endpoint": "/events",
                "incremental": True,
                "incremental_column": "created_at",
                "initial_value": "2024-01-01",
                "query_params": {
                    "filter_field": "{incremental_column}",
                    "filter_since": "{incremental_value}",
                    "status": "active",
                },
            },
            max_value="2024-06-15",
        )
        pipeline._apply_incremental_filter()
        params = pipeline.api_config.query_params
        assert params["filter_field"] == "created_at"
        assert params["filter_since"] == "2024-06-15"
        assert params["status"] == "active"

    def test_raises_when_no_placeholder(self):
        pipeline = IncrementalApiPipeline(
            {
                "base_url": "https://api.example.com",
                "endpoint": "/events",
                "incremental": True,
                "incremental_column": "created_at",
                "initial_value": "2024-01-01",
                "query_params": {"status": "active"},
            },
            max_value="2024-06-15",
        )
        with pytest.raises(ValueError, match="incremental is enabled but no query"):
            pipeline._apply_incremental_filter()

    def test_raises_when_query_params_missing_entirely(self):
        pipeline = IncrementalApiPipeline(
            {
                "base_url": "https://api.example.com",
                "endpoint": "/events",
                "incremental": True,
                "incremental_column": "created_at",
                "initial_value": "2024-01-01",
            },
            max_value="2024-06-15",
        )
        with pytest.raises(ValueError, match="incremental is enabled but no query"):
            pipeline._apply_incremental_filter()

    def test_incremental_requires_column_at_config_time(self):
        # Guard rail lives in BaseConfig.__post_init__
        with pytest.raises(ValueError, match="incremental_column is required"):
            ApiConfig(
                base_url="https://api.example.com",
                endpoint="/events",
                incremental=True,
            )


# ---------------------------------------------------------------------------
# extract_data() incremental gating
#
# These drive the real extract_data() path (filter + fetch + resource build),
# not just _apply_incremental_filter() in isolation — that gap is what let a
# regression slip past: the base extract_data() unconditionally applied the
# placeholder filter and raised for fetch_data-overriding subclasses that
# manage incremental themselves.
# ---------------------------------------------------------------------------


class _FetchOverridingPipeline(BaseApiPipeline):
    """Subclass that overrides fetch_data and manages incremental itself.

    Models the real date-window subclasses (google_ad_manager,
    omnystudio date-range) that set ``incremental: true`` but build their own
    request rather than using the ``{incremental_value}`` placeholder.
    """

    def __init__(self, config_dict):
        self.api_config = self._create_api_config(config_dict)
        self.logger = logging.getLogger("test")
        self.table_name = "events"

    def fetch_data(self):
        return [{"id": 1}, {"id": 2}]


class _DefaultPathPipeline(BaseApiPipeline):
    """Generic pipeline that uses the base fetch path (no fetch_data override)."""

    def __init__(self, config_dict, responses, max_value=None):
        self.api_config = self._create_api_config(config_dict)
        self.logger = logging.getLogger("test")
        self.table_name = "events"
        self.destination_database = "proj"
        self.destination = _FakeDestination(max_value)

        class _Pipeline:
            dataset_name = "dlt_api"

        self.pipeline = _Pipeline()
        self._responses = list(responses)
        self._call_count = 0

    def _make_request(self, url=None, query_params=None):
        resp = self._responses[self._call_count]
        self._call_count += 1
        return resp


@pytest.mark.unit
class TestExtractDataIncrementalGating:
    def test_uses_default_fetch_path_detection(self):
        # Generic pipeline (no fetch_data override) → default path
        assert _DefaultPathPipeline(
            {"base_url": "https://api.example.com", "endpoint": "/events"},
            responses=[[]],
        )._uses_default_fetch_path()
        # Subclass overriding fetch_data → not the default path
        assert not _FetchOverridingPipeline(
            {"base_url": "https://api.example.com", "endpoint": "/events"}
        )._uses_default_fetch_path()

    def test_fetch_data_override_skips_base_filter(self):
        # Regression: incremental enabled, no {incremental_value} placeholder.
        # A fetch_data-overriding subclass must NOT raise — it manages its own
        # incremental loading. (Before the fix, base extract_data() raised here.)
        pipeline = _FetchOverridingPipeline(
            {
                "base_url": "https://api.example.com",
                "endpoint": "/events",
                "incremental": True,
                "incremental_column": "created_at",
            }
        )
        resources = pipeline.extract_data()
        assert len(resources) == 1
        resource, _description = resources[0]
        assert [r["id"] for r in resource] == [1, 2]

    def test_default_path_substitutes_cursor_through_extract_data(self):
        # End-to-end default path: filter resolves the watermark and substitutes
        # it into query_params before the request, then data flows through.
        pipeline = _DefaultPathPipeline(
            {
                "base_url": "https://api.example.com",
                "endpoint": "/events",
                "incremental": True,
                "incremental_column": "created_at",
                "initial_value": "2024-01-01",
                "query_params": {"updated_since": "{incremental_value}"},
            },
            responses=[[{"id": 1}]],
            max_value="2024-06-15",
        )
        resources = pipeline.extract_data()
        assert pipeline.api_config.query_params["updated_since"] == "2024-06-15"
        resource, _description = resources[0]
        assert [r["id"] for r in resource] == [1]

    def test_default_path_still_raises_without_placeholder(self):
        # The helpful hard error is preserved for direct config users on the
        # default fetch path.
        pipeline = _DefaultPathPipeline(
            {
                "base_url": "https://api.example.com",
                "endpoint": "/events",
                "incremental": True,
                "incremental_column": "created_at",
                "initial_value": "2024-01-01",
                "query_params": {"status": "active"},
            },
            responses=[[{"id": 1}]],
        )
        with pytest.raises(ValueError, match="incremental is enabled but no query"):
            pipeline.extract_data()


class RetryApiPipeline(BaseApiPipeline):
    """Drives the REAL request path; tests stub the transport underneath it."""

    def __init__(self, **overrides):
        cfg = {
            "base_url": "https://api.example.com",
            "endpoint": "/data",
            "max_retries": 2,
            "retry_backoff_base": 2,
        }
        cfg.update(overrides)
        self.api_config = self._create_api_config(cfg)
        self.logger = logging.getLogger("test")


def _response(status=200, json_body=None, *, content=None, headers=None, reason=""):
    """A real ``requests.Response``, as an HTTP adapter would return it."""
    resp = requests.Response()
    resp.status_code = status
    resp.reason = reason
    resp.headers.update(headers or {})
    if content is None:
        content = json.dumps({} if json_body is None else json_body).encode()
    resp._content = content
    resp.url = "https://api.example.com/data"
    return resp


@contextmanager
def _transport(*outcomes):
    """Stub the HTTP adapter below the retry layer; sleeps are recorded, not taken.

    Each request gets the next outcome: a response is returned, an exception raised.
    """
    with (
        patch.object(HTTPAdapter, "send", side_effect=list(outcomes)) as send,
        patch("tenacity.nap.time.sleep") as sleep,
    ):
        yield send, sleep


@contextmanager
def _sent_requests(*responses):
    """Capture the arguments each request reaches ``requests.Session.request`` with."""
    sent = []
    queue = list(responses) or [_response(200, {"data": []})]

    def _fake(method, url, **kwargs):
        sent.append({"method": method, "url": url, **kwargs})
        return queue.pop(0)

    with patch.object(requests.Session, "request", side_effect=_fake):
        yield sent


@pytest.mark.unit
class TestMakeRequestRetry:
    def test_connection_error_retried_then_succeeds(self):
        pipeline = RetryApiPipeline()
        ok = _response(200, {"data": [{"id": 1}]})
        with _transport(requests.ConnectionError("connection reset"), ok) as (send, _):
            result = pipeline._make_request()
        assert result == {"data": [{"id": 1}]}
        assert send.call_count == 2  # retried once, then succeeded

    def test_connection_error_exhausts_retries(self):
        pipeline = RetryApiPipeline()
        with _transport(*[requests.ConnectionError("connection reset")] * 3) as (
            send,
            _,
        ):
            with pytest.raises(ValueError, match="connection error"):
                pipeline._make_request()
        assert send.call_count == 3  # initial + 2 retries

    def test_incomplete_response_retried(self):
        pipeline = RetryApiPipeline()
        ok = _response(200, {"data": []})
        with _transport(ChunkedEncodingError("body cut off"), ok) as (send, _):
            pipeline._make_request()
        assert send.call_count == 2

    def test_non_transient_request_error_not_retried(self):
        pipeline = RetryApiPipeline()
        with _transport(requests.exceptions.InvalidURL("bad url")) as (send, _):
            with pytest.raises(ValueError, match="API request failed"):
                pipeline._make_request()
        assert send.call_count == 1  # fail fast, no retry

    def test_non_retryable_4xx_carries_status_code(self):
        """A 404 raises ApiRequestError with the status code and URL attached,
        so a caller can tell it apart from other failures without message parsing.
        Still a ValueError, so existing handling is unaffected."""
        pipeline = RetryApiPipeline()
        with _transport(_response(404, content=b"not found")) as (send, _):
            with pytest.raises(ApiRequestError) as exc_info:
                pipeline._make_request()
        assert isinstance(exc_info.value, ValueError)
        assert exc_info.value.status_code == 404
        assert exc_info.value.url == "https://api.example.com/data"
        assert send.call_count == 1  # 4xx (non-429) is not retried

    def test_retries_exhausted_5xx_carries_status_code(self):
        pipeline = RetryApiPipeline()
        down = _response(503, content=b"down", reason="Service Unavailable")
        with _transport(*[down] * 3) as (send, _):
            with pytest.raises(ApiRequestError, match="after 2 retries") as exc_info:
                pipeline._make_request()
        assert exc_info.value.status_code == 503
        assert send.call_count == 3  # initial + 2 retries

    def test_zero_retries_sends_once(self):
        pipeline = RetryApiPipeline(max_retries=0)
        with _transport(_response(503, content=b"down")) as (send, _):
            with pytest.raises(ApiRequestError, match="with status 503"):
                pipeline._make_request()
        assert send.call_count == 1

    def test_connection_error_has_no_status_code(self):
        """Non-HTTP failures carry status_code=None — the signal that no HTTP
        status was returned."""
        pipeline = RetryApiPipeline()
        with _transport(*[requests.ConnectionError("connection reset")] * 3):
            with pytest.raises(ApiRequestError) as exc_info:
                pipeline._make_request()
        assert exc_info.value.status_code is None
        assert exc_info.value.url == "https://api.example.com/data"

    def test_default_backoff_waits_one_then_two_seconds(self):
        pipeline = RetryApiPipeline()
        down = _response(503, content=b"down")
        with _transport(down, down, _response(200)) as (_, sleep):
            pipeline._make_request()
        assert [c.args[0] for c in sleep.call_args_list] == [1, 2]

    def test_retry_after_header_sets_the_wait(self):
        pipeline = RetryApiPipeline()
        limited = _response(429, headers={"Retry-After": "7"})
        with _transport(limited, _response(200)) as (_, sleep):
            pipeline._make_request()
        assert [c.args[0] for c in sleep.call_args_list] == [7]

    def test_post_is_retried(self):
        pipeline = RetryApiPipeline(method="POST", body={"report": "daily"})
        with _transport(_response(502), _response(200)) as (send, _):
            pipeline._make_request()
        assert send.call_count == 2
        assert send.call_args.args[0].method == "POST"

    def test_transient_status_logged_without_query_string(self, caplog):
        pipeline = RetryApiPipeline()
        limited = _response(429, reason="Too Many Requests")
        limited.url = "https://api.example.com/data?api_key=k3y"
        with (
            _transport(limited, _response(200)),
            caplog.at_level(logging.DEBUG, logger="test"),
        ):
            pipeline._make_request()
        (message,) = [m for m in caplog.messages if "transient error" in m]
        assert "HTTP 429 (Too Many Requests)" in message
        assert "k3y" not in message


@pytest.mark.unit
class TestSendRequest:
    def test_form_post_to_another_host_carries_no_configured_auth(self):
        pipeline = RetryApiPipeline(auth_type="bearer", auth_token="tok")
        with _transport(_response(200, {"access_token": "abc"})) as (send, _):
            response = pipeline._send_request(
                "POST",
                "https://auth.example.org/token",
                data={"grant_type": "client_credentials"},
            )
        prepared = send.call_args.args[0]
        assert prepared.url == "https://auth.example.org/token"
        assert prepared.body == "grant_type=client_credentials"
        assert prepared.headers["Content-Type"] == "application/x-www-form-urlencoded"
        assert "Authorization" not in prepared.headers
        assert response.json() == {"access_token": "abc"}

    def test_returns_raw_bytes_for_non_json_bodies(self):
        pipeline = RetryApiPipeline()
        csv = "name,city\nÅse,Tromsø\n".encode()
        with _transport(
            _response(200, content=csv, headers={"Content-Type": "text/csv"})
        ):
            response = pipeline._send_request("GET", "https://api.example.com/export")
        assert response.content.decode("utf-8") == "name,city\nÅse,Tromsø\n"

    def test_error_status_raises_api_request_error(self):
        pipeline = RetryApiPipeline()
        with _transport(_response(401, content=b"unauthorized")):
            with pytest.raises(ApiRequestError) as exc_info:
                pipeline._send_request("GET", "https://api.example.com/report")
        assert exc_info.value.status_code == 401
        assert exc_info.value.url == "https://api.example.com/report"

    def test_html_error_page_summarised_in_message(self):
        """The message carries the page title, not the markup; the full body
        stays on the exception for adapters that parse it."""
        page = b"<html><head><title>Bad Gateway</title><style>x{}</style></head></html>"
        pipeline = RetryApiPipeline(max_retries=0)
        error = _response(502, content=page, headers={"Content-Type": "text/html"})
        with _transport(error):
            with pytest.raises(ApiRequestError) as exc_info:
                pipeline._send_request("GET", "https://api.example.com/report")
        assert str(exc_info.value) == "API request failed with status 502: Bad Gateway"
        assert exc_info.value.response_text == page.decode()

    def test_session_reused_across_requests(self):
        pipeline = RetryApiPipeline()
        with _transport(_response(200), _response(200)):
            pipeline._send_request("GET", "https://api.example.com/a")
            first = pipeline._http_session()
            pipeline._send_request("GET", "https://api.example.com/b")
        assert pipeline._http_session() is first


@pytest.mark.unit
class TestMaxPagesSafety:
    def test_max_pages_limits_iteration(self):
        """max_pages prevents infinite loops."""
        # Return full pages forever — max_pages should stop us
        infinite_responses = [
            {"data": [{"id": i}], "meta": {"next": "keep_going"}} for i in range(100)
        ]
        pipeline = FakeApiPipeline(
            {
                "base_url": "https://api.example.com",
                "endpoint": "/items",
                "pagination": {
                    "type": "cursor",
                    "cursor_path": "meta.next",
                    "max_pages": 3,
                },
            },
            infinite_responses,
        )
        pipeline.api_config.response_path = "data"

        records = list(pipeline._fetch_all_pages())
        assert len(records) == 3
        assert pipeline._call_count == 3

    def test_hitting_cap_warns_of_truncation(self, caplog):
        # Reaching max_pages without a natural stop means the result is likely
        # truncated — that must be surfaced, not silent.
        infinite_responses = [
            {"data": [{"id": i}], "meta": {"next": "keep_going"}} for i in range(10)
        ]
        pipeline = FakeApiPipeline(
            {
                "base_url": "https://api.example.com",
                "endpoint": "/items",
                "pagination": {
                    "type": "cursor",
                    "cursor_path": "meta.next",
                    "max_pages": 2,
                },
            },
            infinite_responses,
        )
        pipeline.api_config.response_path = "data"

        with caplog.at_level(logging.WARNING):
            list(pipeline._fetch_all_pages())
        assert "max_pages cap (2)" in caplog.text
        assert "truncated" in caplog.text

    def test_natural_stop_does_not_warn(self, caplog):
        pipeline = FakeApiPipeline(
            {
                "base_url": "https://api.example.com",
                "endpoint": "/items",
                "pagination": {
                    "type": "cursor",
                    "cursor_path": "meta.next",
                    "max_pages": 100,
                },
            },
            [{"data": [{"id": 1}], "meta": {"next": None}}],
        )
        pipeline.api_config.response_path = "data"

        with caplog.at_level(logging.WARNING):
            list(pipeline._fetch_all_pages())
        assert "max_pages" not in caplog.text


@pytest.mark.unit
class TestRequestLogging:
    @pytest.fixture(autouse=True)
    def _clean_redaction_registry(self):
        redaction._reset_for_testing()
        yield
        redaction._reset_for_testing()

    def _request_log_messages(self, caplog):
        return [
            r.getMessage()
            for r in caplog.records
            if r.getMessage().startswith("Making API request")
        ]

    def test_query_params_logged(self, caplog):
        pipeline = RetryApiPipeline(query_params={"offset": 200, "limit": 100})
        with (
            _transport(_response(503), _response(200)),
            caplog.at_level(logging.DEBUG, logger="test"),
        ):
            pipeline._make_request()
        (message,) = self._request_log_messages(caplog)
        assert "params={'offset': 200, 'limit': 100}" in message

    def test_override_params_logged(self, caplog):
        pipeline = RetryApiPipeline(query_params={"limit": 100})
        with _sent_requests(), caplog.at_level(logging.DEBUG, logger="test"):
            pipeline._make_request(query_params={"cursor": "abc"})
        assert "params={'cursor': 'abc'}" in self._request_log_messages(caplog)[0]

    def test_no_params_logged_as_empty(self, caplog):
        pipeline = RetryApiPipeline()
        with _sent_requests(), caplog.at_level(logging.DEBUG, logger="test"):
            pipeline._make_request()
        assert "params={}" in self._request_log_messages(caplog)[0]

    def test_resolved_secret_in_params_is_redacted(self, caplog):
        """A provider-resolved secret passed as a query param (an adapter
        putting an API key in the query string) is masked by the handler-level
        redaction filter, including characters URL-encoding would rewrite."""
        secret = "k3y+with/base64="
        redaction.register_secret(secret)
        pipeline = RetryApiPipeline()
        with _sent_requests(), caplog.at_level(logging.DEBUG, logger="test"):
            pipeline._make_request(query_params={"api_key": secret})
        (record,) = [
            r for r in caplog.records if r.getMessage().startswith("Making API request")
        ]
        redaction.SecretRedactingFilter().filter(record)
        message = record.getMessage()
        assert secret not in message
        assert f"'api_key': '{redaction.REDACTION_MASK}'" in message


@pytest.mark.unit
class TestRequestBody:
    def test_body_sent_as_json_payload(self):
        pipeline = RetryApiPipeline(method="POST", body={"query": "select 1"})
        with _sent_requests() as sent:
            pipeline._make_request()
        assert sent[0]["json"] == {"query": "select 1"}
        assert sent[0]["method"] == "POST"

    def test_no_body_omits_json_kwarg(self):
        pipeline = RetryApiPipeline()
        with _sent_requests() as sent:
            pipeline._make_request()
        assert "json" not in sent[0]


@pytest.mark.unit
class TestAuthHeaders:
    def test_basic_auth_builds_authorization_header(self):
        from base64 import b64encode

        pipeline = RetryApiPipeline(
            auth_type="basic", auth_username="user", auth_password="pass"
        )
        headers = pipeline._get_auth_headers()
        expected = b64encode(b"user:pass").decode("ascii")
        assert headers["Authorization"] == f"Basic {expected}"

    def test_basic_auth_handles_non_ascii_credentials(self):
        from base64 import b64encode

        pipeline = RetryApiPipeline(
            auth_type="basic", auth_username="ünïcode", auth_password="påss"
        )
        headers = pipeline._get_auth_headers()
        expected = b64encode("ünïcode:påss".encode()).decode("ascii")
        assert headers["Authorization"] == f"Basic {expected}"

    def test_bearer_auth_still_works(self):
        pipeline = RetryApiPipeline(auth_type="bearer", auth_token="tok")
        headers = pipeline._get_auth_headers()
        assert headers["Authorization"] == "Bearer tok"

    def test_basic_auth_requires_both_credentials(self):
        with pytest.raises(ValueError, match="requires both auth_username"):
            ApiConfig(
                base_url="https://api.example.com",
                endpoint="/data",
                auth_type="basic",
                auth_username="user",
            )

    def test_basic_auth_credentials_coerced_to_secretstr(self):
        from dlt_saga.utility.secrets.secret_str import SecretStr

        cfg = ApiConfig(
            base_url="https://api.example.com",
            endpoint="/data",
            auth_type="basic",
            auth_username="user",
            auth_password="pass",
        )
        assert isinstance(cfg.auth_username, SecretStr)
        assert isinstance(cfg.auth_password, SecretStr)


def _query_auth_config(**overrides):
    cfg = {
        "base_url": "https://api.example.com",
        "endpoint": "/data",
        "auth_type": "api_key",
        "auth_token": "k3y+with/base64=",
        "auth_location": "query",
        "auth_param_name": "api_key",
    }
    cfg.update(overrides)
    return cfg


@pytest.mark.unit
class TestQueryAuthConfig:
    def test_defaults_to_header(self):
        cfg = ApiConfig(base_url="https://api.example.com", endpoint="/data")
        assert cfg.auth_location == "header"

    def test_valid_query_config(self):
        cfg = ApiConfig(**_query_auth_config())
        assert cfg.auth_location == "query"
        assert cfg.auth_param_name == "api_key"

    def test_invalid_location_raises(self):
        with pytest.raises(ValueError, match="auth_location must be"):
            ApiConfig(**_query_auth_config(auth_location="body"))

    def test_query_requires_param_name(self):
        with pytest.raises(ValueError, match="requires auth_param_name"):
            ApiConfig(**_query_auth_config(auth_param_name=None))

    @pytest.mark.parametrize("auth_type", ["bearer", "none"])
    def test_query_only_for_api_key(self, auth_type):
        with pytest.raises(ValueError, match="only supported for auth_type 'api_key'"):
            ApiConfig(**_query_auth_config(auth_type=auth_type))

    def test_query_rejects_header_name(self):
        with pytest.raises(ValueError, match="auth_header_name does not apply"):
            ApiConfig(**_query_auth_config(auth_header_name="X-Key"))

    def test_param_name_requires_query_location(self):
        with pytest.raises(ValueError, match="only applies with auth_location: query"):
            ApiConfig(**_query_auth_config(auth_location="header"))

    def test_param_name_colliding_with_query_params_raises(self):
        with pytest.raises(ValueError, match="query_params already sets 'api_key'"):
            ApiConfig(**_query_auth_config(query_params={"api_key": "other"}))


@pytest.mark.unit
class TestQueryAuthRequest:
    @pytest.fixture(autouse=True)
    def _clean_redaction_registry(self):
        redaction._reset_for_testing()
        yield
        redaction._reset_for_testing()

    def _request(self, pipeline, **kwargs):
        with _sent_requests() as sent:
            pipeline._make_request(**kwargs)
        return sent[0]

    def test_key_sent_as_query_param_not_header(self):
        pipeline = RetryApiPipeline(**_query_auth_config(query_params={"limit": 10}))
        sent = self._request(pipeline)
        assert sent["params"] == {"limit": 10, "api_key": "k3y+with/base64="}
        assert "X-API-Key" not in sent["headers"]

    def test_key_added_to_pagination_override_params(self):
        pipeline = RetryApiPipeline(**_query_auth_config())
        sent = self._request(pipeline, query_params={"offset": 100})
        assert sent["params"] == {"offset": 100, "api_key": "k3y+with/base64="}

    def test_config_query_params_not_mutated(self):
        pipeline = RetryApiPipeline(**_query_auth_config(query_params={"limit": 10}))
        self._request(pipeline)
        assert pipeline.api_config.query_params == {"limit": 10}

    def test_next_url_already_carrying_key_not_duplicated(self):
        pipeline = RetryApiPipeline(**_query_auth_config())
        sent = self._request(
            pipeline,
            url="https://api.example.com/data?page=2&api_key=k3y",
            query_params={},
        )
        assert sent["params"] == {}

    def test_next_url_without_key_gets_it(self):
        pipeline = RetryApiPipeline(**_query_auth_config())
        sent = self._request(
            pipeline, url="https://api.example.com/data?page=2", query_params={}
        )
        assert sent["params"] == {"api_key": "k3y+with/base64="}

    def test_header_location_sends_no_auth_params(self):
        pipeline = RetryApiPipeline(auth_type="api_key", auth_token="tok-value")
        sent = self._request(pipeline)
        assert sent["params"] is None
        assert sent["headers"]["X-API-Key"] == "tok-value"

    def test_key_registered_raw_and_url_encoded(self):
        """A plain-value key is not registered by the secret resolver, and
        requests embeds the URL-encoded form in its exception messages, so both
        forms must be masked."""
        pipeline = RetryApiPipeline(**_query_auth_config())
        sent = self._request(pipeline)
        # The URL exactly as requests encodes it, so a drift between its
        # encoding and the registered form fails here.
        prepared_url = (
            requests.Request("GET", sent["url"], params=sent["params"]).prepare().url
        )
        masked = redaction.redact(f"params={sent['params']} url: {prepared_url}")
        assert "k3y" not in masked
