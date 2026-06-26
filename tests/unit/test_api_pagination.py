"""Unit tests for API pipeline pagination."""

import logging

import pytest

from dlt_saga.pipelines.api.base import BaseApiPipeline
from dlt_saga.pipelines.api.config import ApiConfig

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
                {"data": [{"id": 3}]},  # fewer than limit → last page
            ],
        )
        pipeline.api_config.response_path = "data"

        records = list(pipeline._fetch_all_pages())
        assert [r["id"] for r in records] == [1, 2, 3]
        # Check offset progression
        assert pipeline._calls[0]["query_params"]["offset"] == 0
        assert pipeline._calls[1]["query_params"]["offset"] == 2

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
