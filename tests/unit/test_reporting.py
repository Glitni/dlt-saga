"""Unit tests for pipeline execution result reporting."""

import re

import pytest

from dlt_saga.utility.cli.reporting import _setup_remainder, summarize_load_info


def _load_info(**overrides):
    """A load_info dict with the fields summarize_load_info reads."""
    base = {
        "pipeline": {"pipeline_name": "api__orders"},
        "destination_name": "bigquery",
        "dataset_name": "dlt_dev",
        "row_counts": {"orders": 100, "_dlt_loads": 1},
        "total_pipeline_duration": 63.6,
        "initialization_duration": 0.1,
        "actual_extract_duration": 8.7,
        "actual_normalize_duration": 2.7,
        "actual_load_duration": 25.4,
        "finalize_duration": 5.3,
    }
    base.update(overrides)
    return base


def _parse_phases(line: str) -> dict:
    """Extract the `phase: N.Ns` pairs from a summary line's breakdown."""
    return {k: float(v) for k, v in re.findall(r"(\w+): (\d+\.\d+)s", line)}


def _parse_total(line: str) -> float:
    """Extract the wall-clock total (formatted as `N.Ns total`)."""
    return float(re.search(r"(\d+\.\d+)s total", line).group(1))


@pytest.mark.unit
class TestSetupRemainder:
    def test_reconciles_to_total(self):
        # 10 - (1 + 2 + 3) == 4
        assert _setup_remainder(10.0, 1.0, 2.0, 3.0) == pytest.approx(4.0)

    def test_missing_phase_counts_as_zero(self):
        assert _setup_remainder(10.0, 1.0, None, 3.0) == pytest.approx(6.0)

    def test_floored_at_zero(self):
        # Trace timings can slightly exceed wall-clock; never go negative.
        assert _setup_remainder(5.0, 3.0, 3.0) == 0.0


@pytest.mark.unit
class TestSummarizeLoadInfo:
    def test_breakdown_includes_setup_and_no_init(self):
        line = summarize_load_info([_load_info()])
        assert "setup:" in line
        assert "init:" not in line

    def test_phases_sum_to_total(self):
        line = summarize_load_info([_load_info()])
        phases = _parse_phases(line)
        assert sum(phases.values()) == pytest.approx(_parse_total(line), abs=0.05)

    def test_setup_absorbs_the_gap(self):
        line = summarize_load_info([_load_info()])
        phases = _parse_phases(line)
        # 63.6 - 8.7 - 2.7 - 25.4 - 5.3 == 21.5 (init folded in)
        assert phases["setup"] == pytest.approx(21.5, abs=0.05)

    def test_legacy_format_without_normalize(self):
        line = summarize_load_info([_load_info(actual_normalize_duration=None)])
        assert "setup:" in line
        assert "normalize:" not in line
        phases = _parse_phases(line)
        assert sum(phases.values()) == pytest.approx(_parse_total(line), abs=0.05)

    def test_meta_tables_excluded_from_row_count(self):
        line = summarize_load_info([_load_info()])
        # 100 orders rows; _dlt_loads excluded.
        assert "100 rows" in line
        assert "(1 table)" in line
