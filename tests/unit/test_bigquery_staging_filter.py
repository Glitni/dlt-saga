"""BigQueryBaseDestination._filter_staging_access_entries: staging datasets are
matched by the "_staging" suffix, not a substring — so a real dataset whose name
merely contains "_staging" keeps its READER grants."""

from types import SimpleNamespace

import pytest

from dlt_saga.destinations.bigquery.base import BigQueryBaseDestination

pytestmark = pytest.mark.unit


def _entries():
    return [
        SimpleNamespace(role="OWNER"),
        SimpleNamespace(role="WRITER"),
        SimpleNamespace(role="READER"),
    ]


def _roles(dataset_name):
    filtered = BigQueryBaseDestination._filter_staging_access_entries(
        _entries(), dataset_name
    )
    return [e.role for e in filtered]


class TestStagingAccessFilter:
    def test_staging_suffix_strips_reader(self):
        assert _roles("dlt_sales_staging") == ["OWNER", "WRITER"]

    def test_omni_staging_suffix_strips_reader(self):
        assert _roles("dlt_sales_omni_staging") == ["OWNER", "WRITER"]

    def test_substring_staging_is_not_treated_as_staging(self):
        # The bug: "_staging" in "sales_staging_data" matched → READER stripped.
        assert _roles("sales_staging_data") == ["OWNER", "WRITER", "READER"]

    def test_plain_dataset_keeps_all_entries(self):
        assert _roles("dlt_sales") == ["OWNER", "WRITER", "READER"]
