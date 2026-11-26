"""Authorized dataset/view entries whose target doesn't exist yet.

BigQuery rejects a whole dataset create or access update when one authorized
target is missing, so such entries are left out and every other grant applies.
"""

from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest
from google.cloud.exceptions import NotFound

from dlt_saga.destinations.bigquery.base import (
    BigQueryBaseDestination,
    _authorized_target,
)
from dlt_saga.destinations.bigquery.destination import BigQueryDestination

OWNER = "OWNER:serviceAccount:owner@example.iam.gserviceaccount.com"
READER = "READER:group:readers@example.com"
MISSING_DATASET = "AUTHORIZED_DATASET:models-proj.not_yet"
EXISTING_DATASET = "AUTHORIZED_DATASET:models-proj.exists"
MISSING_VIEW = "AUTHORIZED_VIEW:models-proj.exists.not_yet_view"


def _client(missing_refs, found=None):
    """A client whose dataset/table lookups raise NotFound for ``missing_refs``
    and return ``found[ref]`` when given."""
    client = MagicMock()

    def lookup(ref):
        if ref in missing_refs:
            raise NotFound(ref)
        return (found or {}).get(ref, MagicMock())

    client.get_dataset.side_effect = lookup
    client.get_table.side_effect = lookup
    return client


def _entries(*config):
    return BigQueryBaseDestination._parse_dataset_access_static(list(config))


def _targets(entries):
    return {_authorized_target(e) for e in entries} - {None}


@pytest.mark.unit
class TestDropMissingAuthorizedEntries:
    def test_missing_dataset_dropped_rest_kept(self, caplog):
        entries = _entries(OWNER, READER, MISSING_DATASET, EXISTING_DATASET)
        with caplog.at_level("WARNING"):
            kept = BigQueryBaseDestination._drop_missing_authorized_entries(
                _client({"models-proj.not_yet"}), entries, "dlt_x"
            )
        assert len(kept) == 3
        assert _targets(kept) == {EXISTING_DATASET}
        assert any(MISSING_DATASET in r.message for r in caplog.records)

    def test_missing_view_checked_as_table(self):
        client = _client({"models-proj.exists.not_yet_view"})
        kept = BigQueryBaseDestination._drop_missing_authorized_entries(
            client, _entries(OWNER, MISSING_VIEW), "dlt_x"
        )
        assert _targets(kept) == set()
        client.get_table.assert_called_once_with("models-proj.exists.not_yet_view")

    def test_entry_already_on_dataset_is_not_checked(self):
        from dlt_saga.destinations.bigquery.base import _stable_entry_key

        entries = _entries(OWNER, MISSING_DATASET)
        client = _client({"models-proj.not_yet"})
        kept = BigQueryBaseDestination._drop_missing_authorized_entries(
            client,
            entries,
            "dlt_x",
            present_keys=frozenset({_stable_entry_key(entries[1])}),
        )
        assert _targets(kept) == {MISSING_DATASET}
        client.get_dataset.assert_not_called()

    def test_unreadable_target_is_kept(self):
        client = MagicMock()
        client.get_dataset.side_effect = PermissionError("403")
        kept = BigQueryBaseDestination._drop_missing_authorized_entries(
            client, _entries(MISSING_DATASET), "dlt_x"
        )
        assert _targets(kept) == {MISSING_DATASET}


@pytest.mark.unit
class TestSyncWithMissingAuthorizedTarget:
    @pytest.fixture(autouse=True)
    def _prod(self):
        ctx = MagicMock(dry_run=False)
        with (
            patch("dlt_saga.utility.naming.get_environment", return_value="prod"),
            patch(
                "dlt_saga.utility.cli.context.get_execution_context",
                return_value=ctx,
            ),
        ):
            yield

    def _sync(self, client):
        return BigQueryBaseDestination._sync_dataset_and_access_static(
            project_id="data-proj",
            location="EU",
            dataset_name="dlt_x",
            schema_access=[OWNER, READER, MISSING_DATASET],
            client=client,
        )

    def test_create_applies_every_grant_but_the_missing_target(self):
        client = _client({"data-proj.dlt_x", "models-proj.not_yet"})
        assert self._sync(client) is True
        dataset = client.create_dataset.call_args[0][0]
        assert len(dataset.access_entries) == 2
        assert _targets(dataset.access_entries) == set()

    def test_update_grants_the_rest_without_the_missing_target(self):
        existing = SimpleNamespace(access_entries=_entries(OWNER))
        client = _client({"models-proj.not_yet"}, found={"data-proj.dlt_x": existing})
        assert self._sync(client) is True
        client.update_dataset.assert_called_once()
        assert len(existing.access_entries) == 2
        assert _targets(existing.access_entries) == set()


@pytest.mark.unit
class TestPrepareForExecutionCaching:
    @pytest.fixture(autouse=True)
    def _clear_synced(self):
        BigQueryBaseDestination._synced_datasets.clear()
        yield
        BigQueryBaseDestination._synced_datasets.clear()

    @pytest.mark.parametrize("synced", [True, False])
    def test_caches_only_a_successful_sync(self, synced):
        ctx = MagicMock()
        ctx.get_database.return_value = "data-proj"
        ctx.get_location.return_value = "EU"
        ctx.profile_target = None
        config = SimpleNamespace(config_dict={}, schema_name="dlt_x")
        with (
            patch(
                "dlt_saga.utility.cli.context.get_execution_context",
                return_value=ctx,
            ),
            patch("dlt_saga.utility.gcp.client_pool.bigquery_pool"),
            patch.object(
                BigQueryBaseDestination,
                "_sync_dataset_and_access_static",
                return_value=synced,
            ),
        ):
            BigQueryDestination.prepare_for_execution([config])
        assert (
            ("data-proj", "dlt_x") in BigQueryBaseDestination._synced_datasets
        ) is synced
