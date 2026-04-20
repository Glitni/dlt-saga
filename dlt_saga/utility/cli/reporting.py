"""Reporting utilities for pipeline execution results.

This module provides functions for formatting and summarizing pipeline
execution results for display.
"""

from typing import Any, Dict, List


def summarize_load_info(load_info_list: List[Dict[str, Any]]) -> str:
    """Create a human-readable summary of pipeline load information.

    Args:
        load_info_list: List of load info dictionaries from pipeline execution

    Returns:
        Formatted string summary of the load information

    Example output:
        google_sheets__data → bigquery(dlt_dev): 1,234 rows (2 tables) in 68.3s total (init: 0.2s, extract: 25.7s, load: 32.4s, finalize: 10.0s)
          - google_sheets__data: 1,000 rows
          - google_sheets__metadata: 234 rows
    """
    summary_lines = []

    for load_info in load_info_list:
        # Get basic pipeline info
        pipeline_name = load_info.get("pipeline", {}).get("pipeline_name", "Unknown")
        destination = load_info.get("destination_name", "Unknown")
        dataset = load_info.get("dataset_name", "Unknown")

        # Get comprehensive pipeline timing with granular breakdown
        total_pipeline_duration = load_info.get("total_pipeline_duration")
        init_duration = load_info.get("initialization_duration")

        # Prefer actual timings from dlt trace over wrapper timings
        extraction_duration = load_info.get("actual_extract_duration")
        normalize_duration = load_info.get("actual_normalize_duration")
        load_duration = load_info.get("actual_load_duration")

        # Fall back to legacy timings if actual timings not available
        if extraction_duration is None:
            extraction_duration = load_info.get("extraction_duration")
        if load_duration is None:
            load_duration = load_info.get("load_duration")

        finalize_duration = load_info.get("finalize_duration")

        if (
            total_pipeline_duration is not None
            and extraction_duration is not None
            and normalize_duration is not None
        ):
            # New format with actual dlt phase timings: 68.3s total (init: 0.2s, extract: 25.7s, normalize: 10.2s, load: 32.4s, finalize: 10.0s)
            timing_info = f"{total_pipeline_duration:.1f}s total (init: {init_duration:.1f}s, extract: {extraction_duration:.1f}s, normalize: {normalize_duration:.1f}s, load: {load_duration:.1f}s, finalize: {finalize_duration:.1f}s)"
        elif total_pipeline_duration is not None and extraction_duration is not None:
            # Legacy format: 68.3s total (init: 0.2s, extract: 25.7s, load: 32.4s, finalize: 10.0s)
            timing_info = f"{total_pipeline_duration:.1f}s total (init: {init_duration:.1f}s, extract: {extraction_duration:.1f}s, load: {load_duration:.1f}s, finalize: {finalize_duration:.1f}s)"
        elif total_pipeline_duration is not None:
            # Fallback to old format if granular breakdown not available
            started_at = load_info.get("started_at")
            finished_at = load_info.get("finished_at")
            dlt_duration = (
                f"{((finished_at - started_at).total_seconds()):.1f}s"
                if started_at and finished_at
                else "?"
            )
            processing_duration = load_info.get("processing_duration")
            timing_info = f"total: {total_pipeline_duration:.1f}s (init: {init_duration:.1f}s, processing: {processing_duration:.1f}s, DLT: {dlt_duration})"
        else:
            # Minimal format if no timing data available
            timing_info = "?"

        # Get tables loaded and their row counts (excluding _dlt_ meta tables)
        all_tables = load_info.get("row_counts", {})
        tables = {k: v for k, v in all_tables.items() if not k.startswith("_dlt_")}
        total_rows = sum(tables.values()) if tables else 0
        table_str = (
            f"({len(tables)} table{'s' if len(tables) != 1 else ''})" if tables else ""
        )

        # Format output line
        summary_lines.append(
            f"{pipeline_name} → {destination}({dataset}): {total_rows:,} rows {table_str} in {timing_info}"
        )

        if tables:
            for table, rows in tables.items():
                if not table.startswith("_dlt_"):
                    summary_lines.append(f"  - {table}: {rows:,} rows")

    return "\n".join(summary_lines)
