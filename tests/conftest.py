"""Shared pytest fixtures for testing."""

import pytest

from dlt_saga.pipeline_config.base_config import PipelineConfig, ScheduleTag


@pytest.fixture(autouse=True)
def _clear_bigquery_client_pool():
    """Isolate the process-singleton BigQuery client pool between tests.

    Destination query paths resolve their client through the pool, which caches
    per (thread, project, location). Without clearing, a client cached under one
    test's patched ``bigquery.Client`` leaks into later tests that expect their
    own patched client. clear_cache() is a cheap no-op when the pool is empty.
    """
    import dlt_saga.utility.gcp.client_pool as client_pool

    client_pool.bigquery_pool.clear_cache()
    yield
    client_pool.bigquery_pool.clear_cache()


@pytest.fixture
def sample_configs():
    """Create sample pipeline configs for testing.

    Returns:
        Dictionary of configs organized by pipeline type
    """
    configs = {
        "google_sheets": [
            PipelineConfig(
                pipeline_group="google_sheets",
                pipeline_name="google_sheets__asm__salgsmal_nasjonal_input_2025",
                table_name="asm__salgsmal_nasjonal_input_2025",
                identifier="configs/google_sheets/asm/salgsmal_nasjonal_input_2025.yml",
                config_dict={"base_table_name": "asm__salgsmal_nasjonal_input_2025"},
                enabled=True,
                tags=[ScheduleTag("daily"), ScheduleTag("critical")],
                source_type="file",
            ),
            PipelineConfig(
                pipeline_group="google_sheets",
                pipeline_name="google_sheets__query_balance_view",
                table_name="query_balance_view",
                identifier="configs/google_sheets/query_balance_view.yml",
                config_dict={"base_table_name": "query_balance_view"},
                enabled=True,
                tags=[ScheduleTag("hourly")],
                source_type="file",
            ),
        ],
        "filesystem": [
            PipelineConfig(
                pipeline_group="filesystem",
                pipeline_name="filesystem__di_avvik_hourly",
                table_name="di_avvik_hourly",
                identifier="configs/filesystem/di_avvik_hourly.yml",
                config_dict={"base_table_name": "di_avvik_hourly"},
                enabled=True,
                tags=[ScheduleTag("hourly")],
                source_type="file",
            ),
            PipelineConfig(
                pipeline_group="filesystem",
                pipeline_name="filesystem__di_avvik_daily",
                table_name="di_avvik_daily",
                identifier="configs/filesystem/di_avvik_daily.yml",
                config_dict={"base_table_name": "di_avvik_daily"},
                enabled=True,
                tags=[ScheduleTag("daily")],
                source_type="file",
            ),
        ],
        "api": [
            PipelineConfig(
                pipeline_group="api",
                pipeline_name="api__livewrapped__stats",
                table_name="livewrapped__stats",
                identifier="configs/api/livewrapped/stats.yml",
                config_dict={"base_table_name": "livewrapped__stats"},
                enabled=True,
                tags=[ScheduleTag("daily"), ScheduleTag("api")],
                source_type="file",
            ),
        ],
    }
    return configs


@pytest.fixture
def empty_configs():
    """Create empty configs dictionary for testing edge cases."""
    return {}
