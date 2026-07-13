"""Unit tests for pipeline selector system."""

from datetime import datetime
from unittest.mock import patch

import pytest

from dlt_saga.pipeline_config.base_config import PipelineConfig
from dlt_saga.utility.cli.selectors import PipelineSelector, format_config_list


@pytest.mark.unit
class TestPipelineSelector:
    def test_init_flattens_configs(self, sample_configs):
        selector = PipelineSelector(sample_configs)
        assert len(selector.flat_configs) == 5
        assert selector.all_configs == sample_configs

    @pytest.mark.parametrize("select_arg", [None, []])
    def test_select_none_or_empty_returns_all(self, sample_configs, select_arg):
        result = PipelineSelector(sample_configs).select(select_arg)
        assert result == sample_configs

    @pytest.mark.parametrize(
        "selector, expected_types, expected_counts",
        [
            # Tag selectors
            (
                ["tag:daily"],
                {"google_sheets", "filesystem", "api"},
                {"google_sheets": 1, "filesystem": 1, "api": 1},
            ),
            (
                ["tag:hourly"],
                {"google_sheets", "filesystem"},
                {"google_sheets": 1, "filesystem": 1},
            ),
            (["tag:critical"], {"google_sheets"}, {"google_sheets": 1}),
            # Group selectors
            (["group:google_sheets"], {"google_sheets"}, {"google_sheets": 2}),
            (["group:filesystem"], {"filesystem"}, {"filesystem": 2}),
            (["group:api"], {"api"}, {"api": 1}),
            # Name selectors
            (
                ["google_sheets__query_balance_view"],
                {"google_sheets"},
                {"google_sheets": 1},
            ),
            (["di_avvik_hourly"], {"filesystem"}, {"filesystem": 1}),
            # Glob patterns
            (["google_sheets__*"], {"google_sheets"}, {"google_sheets": 2}),
            (["*balance*"], {"google_sheets"}, {"google_sheets": 1}),
            (["*avvik*"], {"filesystem"}, {"filesystem": 2}),
            # No matches
            (["tag:nonexistent"], set(), {}),
            (["group:nonexistent"], set(), {}),
            (["nonexistent_pipeline"], set(), {}),
        ],
    )
    def test_select(self, sample_configs, selector, expected_types, expected_counts):
        s = PipelineSelector(sample_configs)
        mock_time = datetime(2024, 1, 15, 10, 0, 0)
        with patch("dlt_saga.utility.cli.selectors.datetime") as mock_dt:
            mock_dt.now.return_value = mock_time
            result = s.select(selector)

        if not expected_types:
            assert result == {}
        else:
            assert set(result.keys()) == expected_types
            for t, count in expected_counts.items():
                assert len(result[t]) == count

    def test_zero_match_selector_warns(self, sample_configs, caplog):
        """A selector matching nothing must warn, not fail silently."""
        import logging

        with caplog.at_level(logging.WARNING, logger="dlt_saga.utility.cli.selectors"):
            PipelineSelector(sample_configs).select(["tag:nonexistent"])
        assert any(
            "tag:nonexistent" in r.message and r.levelno == logging.WARNING
            for r in caplog.records
        )

    def test_matching_selector_does_not_warn(self, sample_configs, caplog):
        import logging

        with caplog.at_level(logging.WARNING, logger="dlt_saga.utility.cli.selectors"):
            PipelineSelector(sample_configs).select(["group:google_sheets"])
        assert not any(r.levelno == logging.WARNING for r in caplog.records)

    def test_warn_on_no_match_false_suppresses_warning(self, sample_configs, caplog):
        """The disabled-config probe passes warn_on_no_match=False: a non-match
        against that subset is expected and must not warn (it would otherwise
        contradict a successful enabled match)."""
        import logging

        with caplog.at_level(logging.WARNING, logger="dlt_saga.utility.cli.selectors"):
            result = PipelineSelector(sample_configs).select(
                ["tag:nonexistent"], warn_on_no_match=False
            )
        assert result == {}
        assert not any(r.levelno == logging.WARNING for r in caplog.records)

    def test_union_space_separated(self, sample_configs):
        """Space-separated = UNION (OR)."""
        result = PipelineSelector(sample_configs).select(["tag:daily group:filesystem"])
        assert set(result.keys()) == {"google_sheets", "filesystem", "api"}
        assert len(result["filesystem"]) == 2  # type match gets both

    def test_union_multiple_args(self, sample_configs):
        """Multiple --select args = UNION (OR)."""
        result = PipelineSelector(sample_configs).select(["tag:critical", "group:api"])
        assert "google_sheets" in result and "api" in result

    def test_intersection_comma_separated(self, sample_configs):
        """Comma-separated = INTERSECTION (AND)."""
        result = PipelineSelector(sample_configs).select(
            ["tag:daily,group:google_sheets"]
        )
        assert len(result) == 1
        assert len(result["google_sheets"]) == 1
        assert result["google_sheets"][0].has_tag("daily")

    def test_intersection_multiple_tags(self, sample_configs):
        """Only API pipeline has both daily and api tags."""
        result = PipelineSelector(sample_configs).select(["tag:daily,tag:api"])
        assert len(result) == 1 and len(result["api"]) == 1

    def test_intersection_comma_space_equivalent_to_comma(self, sample_configs):
        """A space after the comma reads as intersection, not union.

        Previously the whitespace split ran first, so "a, b" became "a," (which
        intersected against an empty selector → nothing) UNION "b" — silently
        returning just "b" instead of the intended "a AND b".
        """
        result = PipelineSelector(sample_configs).select(
            ["tag:daily, group:google_sheets"]
        )
        assert len(result) == 1
        assert len(result["google_sheets"]) == 1
        assert result["google_sheets"][0].has_tag("daily")

    def test_trailing_comma_ignored(self, sample_configs):
        """A trailing comma yields an empty token that must be dropped, not
        intersected against (which would silently select nothing)."""
        result = PipelineSelector(sample_configs).select(["tag:daily,"])
        assert set(result.keys()) == {"google_sheets", "filesystem", "api"}

    def test_doubled_and_padded_commas_tolerated(self, sample_configs):
        """Empty tokens from doubled/padded commas are dropped; the real
        intersection still applies."""
        result = PipelineSelector(sample_configs).select(
            ["tag:daily ,, group:google_sheets"]
        )
        assert len(result) == 1
        assert len(result["google_sheets"]) == 1

    def test_complex_combination(self, sample_configs):
        """(tag:daily AND group:google_sheets) OR tag:hourly."""
        result = PipelineSelector(sample_configs).select(
            ["tag:daily,group:google_sheets", "tag:hourly"]
        )
        assert len(result["google_sheets"]) == 2
        assert len(result["filesystem"]) == 1

    def test_deduplication(self, sample_configs):
        """Same pipeline selected by different criteria appears once."""
        result = PipelineSelector(sample_configs).select(
            ["group:google_sheets", "tag:critical"]
        )
        assert len(result["google_sheets"]) == 2

    def test_empty_configs(self, empty_configs):
        s = PipelineSelector(empty_configs)
        assert s.select(["tag:daily"]) == {}
        assert s.select(None) == {}

    def test_group_selector(self, sample_configs):
        """group: prefix works as the primary group selector."""
        result = PipelineSelector(sample_configs).select(["group:google_sheets"])
        assert set(result.keys()) == {"google_sheets"}
        assert len(result["google_sheets"]) == 2

    def test_organize_by_group(self, sample_configs):
        selector = PipelineSelector(sample_configs)
        organized = selector._organize_by_group(selector.flat_configs)
        assert len(organized["google_sheets"]) == 2
        assert len(organized["filesystem"]) == 2
        assert len(organized["api"]) == 1


@pytest.mark.unit
class TestFormatConfigList:
    def test_format_basic(self, sample_configs):
        result = format_config_list(sample_configs)
        assert "Available pipelines:" in result
        assert "google_sheets (2 pipelines):" in result
        assert "Total: 5 enabled pipelines" in result

    def test_format_with_tags(self, sample_configs):
        result = format_config_list(sample_configs)
        assert "[tags: daily, critical]" in result
        assert "[tags: hourly]" in result
        assert "[tags: daily, api]" in result

    def test_format_with_disabled(self, sample_configs):
        disabled = {
            "google_sheets": [
                PipelineConfig(
                    pipeline_group="google_sheets",
                    pipeline_name="google_sheets__disabled_pipeline",
                    table_name="disabled_pipeline",
                    identifier="configs/google_sheets/disabled.yml",
                    config_dict={"base_table_name": "disabled_pipeline"},
                    enabled=False,
                    tags=[],
                    source_type="file",
                ),
            ]
        }
        result = format_config_list(sample_configs, disabled)
        assert "Disabled (1):" in result
        assert "google_sheets__disabled_pipeline [disabled]" in result

    def test_format_empty(self, empty_configs):
        result = format_config_list(empty_configs)
        assert "Total: 0 enabled pipeline" in result

    def test_format_singular(self):
        single = {
            "api": [
                PipelineConfig(
                    pipeline_group="api",
                    pipeline_name="api__single",
                    table_name="single",
                    identifier="configs/api/single.yml",
                    config_dict={"base_table_name": "single"},
                    enabled=True,
                    tags=[],
                    source_type="file",
                ),
            ]
        }
        assert "Total: 1 enabled pipeline" in format_config_list(single)


@pytest.mark.unit
class TestDiscoverAndSelectNoSpuriousWarning:
    """Regression: a selector matching an enabled pipeline (but nothing in the
    disabled set) must not emit 'did not match any pipelines' from the disabled
    probe — the warning previously contradicted the run that followed."""

    def _cfg(self, pipeline_name, group):
        from unittest.mock import MagicMock

        cfg = MagicMock()
        cfg.pipeline_name = pipeline_name
        cfg.pipeline_group = group
        cfg.table_name = pipeline_name.split("__")[-1]
        cfg.identifier = f"configs/{group}/{pipeline_name}.yml"
        return cfg

    def test_enabled_match_disabled_nonmatch_does_not_warn(self, caplog):
        import logging
        from unittest.mock import MagicMock, patch

        from dlt_saga.utility.cli import common

        enabled = {"api": [self._cfg("api__foo__ad_exchange_revenue", "api")]}
        disabled = {"api": [self._cfg("api__foo__something_else", "api")]}

        source = MagicMock()
        source.discover.return_value = (enabled, disabled)

        with (
            patch.object(common, "get_config_source", return_value=source),
            caplog.at_level(logging.WARNING, logger="dlt_saga.utility.cli.selectors"),
        ):
            selected, _ = common.discover_and_select_configs(["*ad_exchange_revenue"])

        assert "api" in selected  # the enabled pipeline was selected and will run
        assert not any(r.levelno == logging.WARNING for r in caplog.records)
