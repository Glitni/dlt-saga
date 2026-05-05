"""Unit tests for snapshot date extraction in BasePipeline._resolve_ingested_at."""

import re
from datetime import datetime, timezone

import pytest


@pytest.mark.unit
class TestResolveIngestedAt:
    """Tests for the _resolve_ingested_at priority chain."""

    def _resolve(self, item, regex_str=None, fmt=None, extraction_ts=None):
        """Call _resolve_ingested_at without instantiating BasePipeline."""
        from dlt_saga.pipelines.base_pipeline import BasePipeline

        compiled = re.compile(regex_str) if regex_str else None
        ts = extraction_ts or "2026-01-01T00:00:00+00:00"
        pipeline = object.__new__(BasePipeline)
        return pipeline._resolve_ingested_at(item, compiled, fmt, ts)

    # --- Priority 1: regex extraction ---

    def test_regex_hit_correct_format(self):
        item = {"_dlt_source_file_name": "gs://bucket/2026-03-15_snapshot.parquet"}
        result = self._resolve(
            item,
            regex_str=r"(\d{4}-\d{2}-\d{2})",
            fmt="%Y-%m-%d",
        )
        assert result == datetime(2026, 3, 15, tzinfo=timezone.utc)

    def test_regex_format_mismatch_falls_back_to_mod_date(self, caplog):
        """When strptime fails the warning is emitted and we fall back to P2."""
        import logging

        mod_date = datetime(2026, 1, 10, tzinfo=timezone.utc)
        item = {
            "_dlt_source_file_name": "data_20260315.parquet",
            "_dlt_source_modification_date": mod_date,
        }
        with caplog.at_level(logging.WARNING):
            result = self._resolve(
                item,
                # regex captures "20260315" but format expects "YYYY-MM-DD" with dashes
                regex_str=r"(\d{8})",
                fmt="%Y-%m-%d",
            )
        assert result == mod_date
        assert "snapshot_date_format" in caplog.text

    def test_regex_format_mismatch_falls_back_to_extraction_ts(self, caplog):
        """When strptime fails and no mod date, fall back to extraction timestamp."""
        import logging

        extraction_ts = "2026-06-01T12:00:00+00:00"
        item = {"_dlt_source_file_name": "data_20260315.parquet"}
        with caplog.at_level(logging.WARNING):
            result = self._resolve(
                item,
                regex_str=r"(\d{8})",
                fmt="%Y-%m-%d",
                extraction_ts=extraction_ts,
            )
        assert result == extraction_ts
        assert "snapshot_date_format" in caplog.text

    def test_regex_no_match_falls_back_to_mod_date(self):
        """No regex match → falls through to Priority 2."""
        mod_date = datetime(2026, 2, 20, tzinfo=timezone.utc)
        item = {
            "_dlt_source_file_name": "no_date_here.parquet",
            "_dlt_source_modification_date": mod_date,
        }
        result = self._resolve(item, regex_str=r"(\d{4}-\d{2}-\d{2})", fmt="%Y-%m-%d")
        assert result == mod_date

    # --- Priority 2: file modification date ---

    def test_mod_date_used_when_no_regex(self):
        mod_date = datetime(2026, 3, 5, tzinfo=timezone.utc)
        item = {"_dlt_source_modification_date": mod_date}
        result = self._resolve(item)
        assert result == mod_date

    # --- Priority 3: extraction timestamp fallback ---

    def test_extraction_ts_fallback(self):
        extraction_ts = "2026-09-01T00:00:00+00:00"
        result = self._resolve({}, extraction_ts=extraction_ts)
        assert result == extraction_ts
