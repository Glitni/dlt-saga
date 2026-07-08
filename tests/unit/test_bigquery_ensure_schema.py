"""Unit tests for BigQueryDestination.ensure_schema_exists.

Covers the race-safety contract: only a genuine NotFound triggers creation,
other errors propagate, and a parallel create (Conflict) is tolerated.
"""

from unittest.mock import MagicMock

import pytest
from google.api_core.exceptions import Conflict, Forbidden
from google.cloud.exceptions import NotFound

from dlt_saga.destinations.bigquery.destination import BigQueryDestination


def _make_dest(get_dataset_side_effect=None):
    dest = MagicMock(spec=BigQueryDestination)
    dest.config = MagicMock()
    dest.config.project_id = "proj"
    dest.config.location = "EU"
    client = MagicMock()
    if get_dataset_side_effect is not None:
        client.get_dataset.side_effect = get_dataset_side_effect
    dest._client.return_value = client
    return dest, client


@pytest.mark.unit
class TestBigQueryEnsureSchemaExists:
    def test_existing_dataset_is_not_recreated(self):
        dest, client = _make_dest()
        BigQueryDestination.ensure_schema_exists(dest, "my_schema")
        client.get_dataset.assert_called_once()
        client.create_dataset.assert_not_called()

    def test_missing_dataset_is_created_with_exists_ok(self):
        dest, client = _make_dest(get_dataset_side_effect=NotFound("missing"))
        BigQueryDestination.ensure_schema_exists(dest, "my_schema")
        client.create_dataset.assert_called_once()
        _, kwargs = client.create_dataset.call_args
        assert kwargs.get("exists_ok") is True

    def test_permission_error_propagates_without_create(self):
        # A non-NotFound error must not be mistaken for a missing dataset.
        dest, client = _make_dest(get_dataset_side_effect=Forbidden("no access"))
        with pytest.raises(Forbidden):
            BigQueryDestination.ensure_schema_exists(dest, "my_schema")
        client.create_dataset.assert_not_called()

    def test_parallel_create_conflict_is_swallowed(self):
        dest, client = _make_dest(get_dataset_side_effect=NotFound("missing"))
        client.create_dataset.side_effect = Conflict("already exists")
        # Should not raise — another process won the race.
        BigQueryDestination.ensure_schema_exists(dest, "my_schema")
        client.create_dataset.assert_called_once()
