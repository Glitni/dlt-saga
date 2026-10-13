"""Unit tests for the dataset lookup in ``_sync_dataset_and_access_static``.

The sync may run on a client built for the billing project (the ingest
pre-create sweep in ``BigQueryDestination.prepare_for_execution``), so the
dataset lookup must be qualified with the data project — a bare name resolves
against the client's default project, and a NotFound there sends an existing
dataset through the create path, where ``exists_ok`` makes the access sync a
silent no-op.
"""

from unittest.mock import MagicMock, patch

import pytest
from google.cloud.exceptions import NotFound

from dlt_saga.destinations.bigquery.base import BigQueryBaseDestination

DATA_PROJECT = "data-proj"
BILLING_PROJECT = "billing-proj"


def _billing_client(get_dataset_side_effect=None):
    client = MagicMock()
    client.project = BILLING_PROJECT
    if get_dataset_side_effect is not None:
        client.get_dataset.side_effect = get_dataset_side_effect
    return client


def _sync(client, schema_access=None):
    return BigQueryBaseDestination._sync_dataset_and_access_static(
        project_id=DATA_PROJECT,
        location="EU",
        dataset_name="dlt_restricted",
        schema_access=schema_access,
        client=client,
    )


@pytest.mark.unit
class TestDatasetLookupIsProjectQualified:
    def test_lookup_uses_data_project_not_client_default(self):
        client = _billing_client()
        assert _sync(client) is True
        client.get_dataset.assert_called_once_with(f"{DATA_PROJECT}.dlt_restricted")
        client.create_dataset.assert_not_called()

    def test_existing_dataset_reaches_access_sync(self):
        # The regression: an existing dataset must hit the update path, not be
        # "created" with exists_ok (which drops the access entries).
        client = _billing_client()
        existing = client.get_dataset.return_value
        entries = [MagicMock(name="entry")]
        with (
            patch.object(
                BigQueryBaseDestination, "_prepare_access_entries", return_value=entries
            ),
            patch.object(BigQueryBaseDestination, "_update_access_if_needed") as upd,
        ):
            assert _sync(client, schema_access=["user:a@example.com"]) is True
        upd.assert_called_once_with(client, existing, "dlt_restricted", entries)
        client.create_dataset.assert_not_called()

    def test_missing_dataset_created_in_data_project(self):
        client = _billing_client(get_dataset_side_effect=NotFound("missing"))
        with patch(
            "dlt_saga.utility.cli.context.get_execution_context",
            return_value=MagicMock(dry_run=False),
        ):
            assert _sync(client) is True
        client.create_dataset.assert_called_once()
        dataset = client.create_dataset.call_args[0][0]
        assert dataset.project == DATA_PROJECT
        assert dataset.dataset_id == "dlt_restricted"
