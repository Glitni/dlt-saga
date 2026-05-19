"""Unit tests for ScheduleTag and schedule-based selector functionality."""

from datetime import datetime
from unittest.mock import patch

import pytest

from dlt_saga.pipeline_config.base_config import (
    PipelineConfig,
    ScheduleTag,
    normalize_weekday,
    parse_tag,
    parse_tags,
)
from dlt_saga.utility.cli.selectors import PipelineSelector


def _config(table_name, tags):
    """Create a minimal PipelineConfig for testing."""
    return PipelineConfig(
        pipeline_group="api",
        pipeline_name=f"api__{table_name}",
        table_name=table_name,
        identifier=f"{table_name}.yml",
        config_dict={"base_table_name": table_name},
        enabled=True,
        tags=tags,
        source_type="file",
    )


@pytest.mark.unit
class TestScheduleTag:
    @pytest.mark.parametrize(
        "name, values, match_name, match_value, exact, expected",
        [
            # Simple tag matches by name
            ("daily", None, "daily", None, False, True),
            ("daily", None, "hourly", None, False, False),
            # Simple tag matches any value (non-exact), not exact
            ("daily", None, "daily", 1, False, True),
            ("daily", None, "daily", 15, False, True),
            ("daily", None, "daily", 1, True, False),
            # Tag with values matches specific values
            ("hourly", [1, 10], "hourly", None, False, True),
            ("hourly", [1, 10], "hourly", 1, False, True),
            ("hourly", [1, 10], "hourly", 10, False, True),
            ("hourly", [1, 10], "hourly", 5, False, False),
            ("hourly", [1, 10], "hourly", 10, True, True),
            ("hourly", [1, 10], "hourly", 5, True, False),
            # Single value
            ("daily", [15], "daily", None, False, True),
            ("daily", [15], "daily", 15, False, True),
            ("daily", [15], "daily", 1, False, False),
        ],
    )
    def test_matches(self, name, values, match_name, match_value, exact, expected):
        tag = ScheduleTag(name, values)
        assert tag.matches(match_name, match_value, exact=exact) is expected

    @pytest.mark.parametrize(
        "name, values, expected_str",
        [
            ("daily", None, "daily"),
            ("hourly", [1, 10], "hourly:1,10"),
            ("daily", [2, "monday"], "daily:2,monday"),
        ],
    )
    def test_str(self, name, values, expected_str):
        assert str(ScheduleTag(name, values)) == expected_str

    def test_equality(self):
        assert ScheduleTag("daily") == ScheduleTag("daily")
        assert ScheduleTag("daily") != ScheduleTag("daily", [1])
        assert ScheduleTag("daily") != ScheduleTag("hourly")
        # Backwards compatibility with string
        assert ScheduleTag("daily") == "daily"
        assert ScheduleTag("daily", [1]) != "daily"

    def test_hash(self):
        tag_set = {
            ScheduleTag("daily"),
            ScheduleTag("daily"),
            ScheduleTag("hourly", [1, 10]),
        }
        assert len(tag_set) == 2


@pytest.mark.unit
class TestParseTag:
    @pytest.mark.parametrize(
        "input_tag, expected_name, expected_values",
        [
            ("daily", "daily", None),
            ({"hourly": [1, 10]}, "hourly", [1, 10]),
            ({"daily": 15}, "daily", [15]),
            ({"daily": ["monday", "friday"]}, "daily", ["monday", "friday"]),
            ({"daily": ["mon", "fri"]}, "daily", ["monday", "friday"]),
            ({"daily": ["Monday", "FRIDAY"]}, "daily", ["monday", "friday"]),
            ({"daily": [2, "monday"]}, "daily", [2, "monday"]),
        ],
    )
    def test_parse_tag(self, input_tag, expected_name, expected_values):
        tag = parse_tag(input_tag)
        assert tag.name == expected_name
        assert tag.values == expected_values

    @pytest.mark.parametrize(
        "invalid_input, error_match",
        [
            ({"hourly": [1], "daily": [2]}, "exactly one key"),
            (123, "Invalid tag format"),
            ({"daily": ["xyz"]}, "Invalid weekday name 'xyz'"),
            ({"hourly": [{"monday": [6], "tuesday": [6]}]}, "exactly one weekday key"),
            ({"hourly": [{"xyz": [6]}]}, "Invalid weekday name 'xyz'"),
            ({"hourly": [{"monday": ["six"]}]}, "must be an integer"),
            ({"hourly": [{"monday": [True]}]}, "must be an integer"),
        ],
    )
    def test_parse_invalid(self, invalid_input, error_match):
        with pytest.raises(ValueError, match=error_match):
            parse_tag(invalid_input)

    @pytest.mark.parametrize(
        "input_tag, expected_values",
        [
            # Single per-weekday entry, list of hours
            ({"hourly": [{"monday": [6]}]}, [{"monday": [6]}]),
            # Multiple per-weekday entries mixed with bare int
            (
                {"hourly": [{"monday": [6]}, {"tuesday": [6]}, 9]},
                [{"monday": [6]}, {"tuesday": [6]}, 9],
            ),
            # Weekday key normalization (abbreviations + case)
            ({"hourly": [{"Mon": [6]}]}, [{"monday": [6]}]),
            ({"hourly": [{"FRI": [14, 18]}]}, [{"friday": [14, 18]}]),
            # Scalar hour gets wrapped into a list
            ({"hourly": [{"monday": 6}]}, [{"monday": [6]}]),
            # Bare weekday in hourly (every hour on that weekday)
            ({"hourly": ["monday"]}, ["monday"]),
        ],
    )
    def test_parse_per_weekday_entries(self, input_tag, expected_values):
        tag = parse_tag(input_tag)
        assert tag.name == "hourly"
        assert tag.values == expected_values

    def test_parse_tags_mixed(self):
        tags = parse_tags(["daily", {"hourly": [1, 10]}, "critical"])
        assert len(tags) == 3
        assert tags[0] == ScheduleTag("daily")
        assert tags[1] == ScheduleTag("hourly", [1, 10])
        assert tags[2] == ScheduleTag("critical")

    def test_parse_tags_with_weekday(self):
        tags = parse_tags(["critical", {"daily": [2, "monday"]}])
        assert tags[1] == ScheduleTag("daily", [2, "monday"])


@pytest.mark.unit
class TestNormalizeWeekday:
    @pytest.mark.parametrize(
        "input_val, expected",
        [
            ("monday", "monday"),
            ("friday", "friday"),
            ("Monday", "monday"),
            ("FRIDAY", "friday"),
            ("mon", "monday"),
            ("Tue", "tuesday"),
            ("WED", "wednesday"),
            ("thu", "thursday"),
            ("fri", "friday"),
            ("sat", "saturday"),
            ("sun", "sunday"),
            ("  monday  ", "monday"),
            ("xyz", None),
            ("", None),
            ("mo", None),
        ],
    )
    def test_normalize(self, input_val, expected):
        assert normalize_weekday(input_val) == expected


@pytest.mark.unit
class TestPipelineConfigTags:
    def test_has_tag(self):
        config = _config("test", [ScheduleTag("hourly", [1, 10]), ScheduleTag("daily")])

        assert config.has_tag("hourly")
        assert config.has_tag("hourly", 1)
        assert config.has_tag("hourly", 10)
        assert not config.has_tag("hourly", 5)
        assert config.has_tag("daily")
        assert config.has_tag("daily", 15)
        assert not config.has_tag("weekly")

    def test_get_tag_names(self):
        config = _config("test", [ScheduleTag("hourly", [1, 10]), ScheduleTag("daily")])
        assert config.get_tag_names() == ["hourly", "daily"]


@pytest.mark.unit
class TestScheduleTagWeekday:
    @pytest.mark.parametrize(
        "values, weekday, expected",
        [
            (["monday", "friday"], "monday", True),
            (["monday", "friday"], "friday", True),
            (["monday", "friday"], "wednesday", False),
            (["monday"], "monday", True),
            (None, "monday", True),  # simple tag matches any weekday (non-exact)
        ],
    )
    def test_weekday_matches(self, values, weekday, expected):
        tag = ScheduleTag("daily", values)
        assert tag.matches("daily", query_weekday=weekday) is expected

    def test_mixed_day_and_weekday(self):
        tag = ScheduleTag("daily", [2, "monday"])
        assert tag.matches("daily", 2)
        assert not tag.matches("daily", 15)
        assert tag.matches("daily", query_weekday="monday")
        assert not tag.matches("daily", query_weekday="friday")
        assert tag.matches("daily", 15, query_weekday="monday")
        assert not tag.matches("daily", 15, query_weekday="friday")

    def test_exact_weekday(self):
        tag = ScheduleTag("daily", ["monday"])
        assert tag.matches("daily", exact=True, query_weekday="monday")
        assert not tag.matches("daily", exact=True, query_weekday="friday")

    def test_simple_tag_exact_weekday(self):
        tag = ScheduleTag("daily")
        assert not tag.matches("daily", exact=True, query_weekday="monday")


@pytest.mark.unit
class TestScheduleTagPerWeekdayHours:
    """Matcher behavior for per-weekday dict entries in `hourly` tags."""

    @pytest.mark.parametrize(
        "query_value, query_weekday, expected",
        [
            # Mon@6 matches
            (6, "monday", True),
            # Mon@9 falls through to the bare int entry (every day @ 9)
            (9, "monday", True),
            # Tue@6 not declared (no tuesday dict entry, no bare 6)
            (6, "tuesday", False),
            # Tue@9 matches via bare int entry
            (9, "tuesday", True),
            # Mon@10 no match
            (10, "monday", False),
        ],
    )
    def test_runtime_match(self, query_value, query_weekday, expected):
        tag = ScheduleTag("hourly", [{"monday": [6]}, 9])
        assert (
            tag.matches("hourly", query_value, query_weekday=query_weekday) is expected
        )

    def test_explicit_value_matches_dict_entry(self):
        # Selector "tag:hourly:6" (exact=True, no weekday) should still match
        # configs whose only declared hour-6 run is on a specific weekday.
        tag = ScheduleTag("hourly", [{"monday": [6]}])
        assert tag.matches("hourly", 6, exact=True)
        assert not tag.matches("hourly", 7, exact=True)

    def test_bare_weekday_in_hourly(self):
        # `hourly: [monday]` means "every hour on Mondays"
        tag = ScheduleTag("hourly", ["monday"])
        assert tag.matches("hourly", 6, query_weekday="monday")
        assert tag.matches("hourly", 23, query_weekday="monday")
        assert not tag.matches("hourly", 6, query_weekday="tuesday")

    def test_str_dict_entry(self):
        tag = ScheduleTag("hourly", [{"monday": [6]}, {"tuesday": [6, 14]}, 9])
        assert str(tag) == "hourly:monday:6,tuesday:6,14,9"

    def test_hash_dict_entry(self):
        tag1 = ScheduleTag("hourly", [{"monday": [6]}])
        tag2 = ScheduleTag("hourly", [{"monday": [6]}])
        assert hash(tag1) == hash(tag2)
        # Different hour list → different hash
        tag3 = ScheduleTag("hourly", [{"monday": [7]}])
        assert hash(tag1) != hash(tag3)


@pytest.mark.unit
class TestSelectorWithScheduleValues:
    @pytest.fixture
    def schedule_configs(self):
        return {
            "api": [
                _config("runs_at_1_and_10", [ScheduleTag("hourly", [1, 10])]),
                _config("runs_every_hour", [ScheduleTag("hourly")]),
                _config("runs_on_2nd_and_28th", [ScheduleTag("daily", [2, 28])]),
                _config("runs_every_day", [ScheduleTag("daily")]),
            ]
        }

    def _select_at(self, configs, selector, mock_time):
        s = PipelineSelector(configs)
        with patch("dlt_saga.utility.cli.selectors.datetime") as mock_dt:
            mock_dt.now.return_value = mock_time
            return s.select(selector)

    @pytest.mark.parametrize(
        "mock_time, selector, expected_names",
        [
            # Hour 10, day 2: hourly matches runs_at_1_and_10 + runs_every_hour
            (
                datetime(2024, 1, 2, 10, 0),
                ["tag:hourly"],
                {"runs_at_1_and_10", "runs_every_hour"},
            ),
            # Hour 10, day 2: daily matches runs_on_2nd_and_28th + runs_every_day
            (
                datetime(2024, 1, 2, 10, 0),
                ["tag:daily"],
                {"runs_on_2nd_and_28th", "runs_every_day"},
            ),
            # Hour 5, day 15: only runs_every_hour
            (datetime(2024, 1, 15, 5, 0), ["tag:hourly"], {"runs_every_hour"}),
            # Hour 5, day 15: only runs_every_day
            (datetime(2024, 1, 15, 5, 0), ["tag:daily"], {"runs_every_day"}),
        ],
    )
    def test_schedule_aware_selection(
        self, schedule_configs, mock_time, selector, expected_names
    ):
        result = self._select_at(schedule_configs, selector, mock_time)
        assert {c.table_name for c in result.get("api", [])} == expected_names

    @pytest.mark.parametrize(
        "selector, expected_names",
        [
            # Explicit value selectors (exact match, no time-based)
            (["tag:hourly:10"], {"runs_at_1_and_10"}),
            (["tag:hourly:5"], set()),
            (["tag:daily:2"], {"runs_on_2nd_and_28th"}),
            (["tag:daily:15"], set()),
            (["tag:weekly:1"], set()),
            # Intersection
            (["tag:hourly:1,group:api"], {"runs_at_1_and_10"}),
            # Union
            (
                ["tag:hourly:1", "tag:daily:28"],
                {"runs_at_1_and_10", "runs_on_2nd_and_28th"},
            ),
        ],
    )
    def test_explicit_value_selection(self, schedule_configs, selector, expected_names):
        result = PipelineSelector(schedule_configs).select(selector)
        assert {c.table_name for c in result.get("api", [])} == expected_names

    def test_non_schedule_tag(self, schedule_configs):
        schedule_configs["api"].append(
            _config(
                "critical_pipeline",
                [ScheduleTag("critical"), ScheduleTag("hourly", [1])],
            )
        )
        result = PipelineSelector(schedule_configs).select(["tag:critical"])
        assert {c.table_name for c in result["api"]} == {"critical_pipeline"}


@pytest.mark.unit
class TestSelectorWithWeekday:
    @pytest.fixture
    def weekday_configs(self):
        return {
            "api": [
                _config("runs_on_monday", [ScheduleTag("daily", ["monday"])]),
                _config(
                    "runs_on_2nd_and_monday", [ScheduleTag("daily", [2, "monday"])]
                ),
                _config("runs_every_day", [ScheduleTag("daily")]),
                _config("runs_on_15th", [ScheduleTag("daily", [15])]),
            ]
        }

    def _select_at(self, configs, selector, mock_time):
        s = PipelineSelector(configs)
        with patch("dlt_saga.utility.cli.selectors.datetime") as mock_dt:
            mock_dt.now.return_value = mock_time
            return s.select(selector)

    @pytest.mark.parametrize(
        "mock_time, expected_names",
        [
            # Monday the 10th
            (
                datetime(2024, 6, 10, 12, 0),
                {"runs_on_monday", "runs_on_2nd_and_monday", "runs_every_day"},
            ),
            # Friday the 2nd
            (datetime(2024, 8, 2, 12, 0), {"runs_on_2nd_and_monday", "runs_every_day"}),
            # Wednesday the 10th
            (datetime(2024, 7, 10, 12, 0), {"runs_every_day"}),
        ],
    )
    def test_weekday_selection(self, weekday_configs, mock_time, expected_names):
        result = self._select_at(weekday_configs, ["tag:daily"], mock_time)
        assert {c.table_name for c in result.get("api", [])} == expected_names

    @pytest.mark.parametrize(
        "selector, expected_names",
        [
            (["tag:daily:monday"], {"runs_on_monday", "runs_on_2nd_and_monday"}),
            (["tag:daily:Monday"], {"runs_on_monday", "runs_on_2nd_and_monday"}),
            (["tag:daily:mon"], {"runs_on_monday", "runs_on_2nd_and_monday"}),
        ],
    )
    def test_explicit_weekday(self, weekday_configs, selector, expected_names):
        result = PipelineSelector(weekday_configs).select(selector)
        assert {c.table_name for c in result.get("api", [])} == expected_names


@pytest.mark.unit
class TestSelectorWithPerWeekdayHours:
    """End-to-end selector behavior for the nested `hourly` syntax.

    Mirrors the orchestrator use case: a single cron fires `tag:hourly` every
    hour, and the matching is constrained by per-pipeline (weekday, hour)
    bindings declared in config.
    """

    @pytest.fixture
    def per_weekday_configs(self):
        return {
            "api": [
                # Mon@6 OR Tue@6 OR every day @ 9
                _config(
                    "mon_tue_6_and_daily_9",
                    [ScheduleTag("hourly", [{"monday": [6]}, {"tuesday": [6]}, 9])],
                ),
                # Every hour on Mondays
                _config("mon_every_hour", [ScheduleTag("hourly", ["monday"])]),
                # Plain hourly @ 6
                _config("daily_at_6", [ScheduleTag("hourly", [6])]),
                # Catch-all
                _config("every_hour", [ScheduleTag("hourly")]),
            ]
        }

    def _select_at(self, configs, selector, mock_time):
        s = PipelineSelector(configs)
        with patch("dlt_saga.utility.cli.selectors.datetime") as mock_dt:
            mock_dt.now.return_value = mock_time
            return s.select(selector)

    @pytest.mark.parametrize(
        "mock_time, expected_names",
        [
            # Monday 6am — per-weekday Mon@6, bare weekday Mon, daily@6, catch-all
            (
                datetime(2024, 6, 10, 6, 0),
                {
                    "mon_tue_6_and_daily_9",
                    "mon_every_hour",
                    "daily_at_6",
                    "every_hour",
                },
            ),
            # Tuesday 6am — per-weekday Tue@6 fires; bare-Mon does NOT
            (
                datetime(2024, 6, 11, 6, 0),
                {"mon_tue_6_and_daily_9", "daily_at_6", "every_hour"},
            ),
            # Wednesday 6am — only daily@6 + catch-all (no per-weekday entry for Wed)
            (
                datetime(2024, 6, 12, 6, 0),
                {"daily_at_6", "every_hour"},
            ),
            # Wednesday 9am — bare int 9 fires; daily@6 does not
            (
                datetime(2024, 6, 12, 9, 0),
                {"mon_tue_6_and_daily_9", "every_hour"},
            ),
            # Monday 14:00 — bare weekday Mon fires; per-weekday Mon@6 does not
            (
                datetime(2024, 6, 10, 14, 0),
                {"mon_every_hour", "every_hour"},
            ),
        ],
    )
    def test_per_weekday_hourly_runtime(
        self, per_weekday_configs, mock_time, expected_names
    ):
        result = self._select_at(per_weekday_configs, ["tag:hourly"], mock_time)
        assert {c.table_name for c in result.get("api", [])} == expected_names

    def test_explicit_hour_selector_matches_per_weekday(self, per_weekday_configs):
        # tag:hourly:6 (no weekday) should still match pipelines whose only
        # hour-6 binding is per-weekday.
        result = PipelineSelector(per_weekday_configs).select(["tag:hourly:6"])
        names = {c.table_name for c in result.get("api", [])}
        assert "mon_tue_6_and_daily_9" in names
        assert "daily_at_6" in names
