"""Tests for ``saga update-access`` dry-run support and the grant/revoke
counters that feed the run-end summary.
"""

from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from dlt_saga.destinations.bigquery.base import BigQueryBaseDestination
from dlt_saga.utility.cli.context import (
    ExecutionContext,
    clear_execution_context,
    execution_context_scope,
    get_execution_context,
    set_execution_context,
)


@pytest.fixture(autouse=True)
def _clear_context():
    """Tests share a module-level context global; reset between tests."""
    clear_execution_context()
    yield
    clear_execution_context()


# ---------------------------------------------------------------------------
# ExecutionContext: new fields
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestExecutionContextDryRunAndCounters:
    def test_dry_run_defaults_to_false(self):
        ctx = ExecutionContext()
        assert ctx.dry_run is False

    def test_dry_run_threaded_through_set_execution_context(self):
        set_execution_context(None, update_access=True, dry_run=True)
        assert get_execution_context().dry_run is True

    def test_dry_run_threaded_through_execution_context_scope(self):
        with execution_context_scope(None, update_access=True, dry_run=True) as ctx:
            assert ctx.dry_run is True

    def test_counters_start_at_zero(self):
        ctx = ExecutionContext()
        assert ctx.access_grants_applied == 0
        assert ctx.access_revokes_applied == 0


# ---------------------------------------------------------------------------
# _update_access_if_needed: dry-run skip + counter bump
# ---------------------------------------------------------------------------


def _make_access_entry(role, entity_type, entity_id):
    """A SimpleNamespace that quacks like google.cloud.bigquery.AccessEntry
    enough for the code paths we exercise."""
    return SimpleNamespace(role=role, entity_type=entity_type, entity_id=entity_id)


def _make_existing_dataset(entries):
    ds = MagicMock()
    ds.access_entries = entries
    return ds


@pytest.mark.unit
class TestUpdateAccessIfNeededDryRun:
    """In dry-run mode, ``_update_access_if_needed`` must NOT call
    ``client.update_dataset`` — but it must still emit the diff log lines
    (with ``[DRY RUN]`` prefix and ``would grant`` / ``would revoke``
    verbs) and bump the run-level counters so the summary reflects
    what *would* have been applied."""

    def _call(self, dry_run, existing_entries, desired_entries):
        client = MagicMock()
        existing_dataset = _make_existing_dataset(existing_entries)

        set_execution_context(None, update_access=True, dry_run=dry_run)
        BigQueryBaseDestination._update_access_if_needed(
            client, existing_dataset, "my_dataset", desired_entries
        )
        return client, get_execution_context()

    def test_dry_run_skips_update_dataset_call(self):
        existing = [
            _make_access_entry("OWNER", "userByEmail", "owner@example.com"),
        ]
        desired = [
            _make_access_entry("OWNER", "userByEmail", "owner@example.com"),
            _make_access_entry("READER", "userByEmail", "new@example.com"),
        ]
        client, _ = self._call(
            dry_run=True, existing_entries=existing, desired_entries=desired
        )
        client.update_dataset.assert_not_called()

    def test_non_dry_run_calls_update_dataset(self):
        existing = [
            _make_access_entry("OWNER", "userByEmail", "owner@example.com"),
        ]
        desired = [
            _make_access_entry("OWNER", "userByEmail", "owner@example.com"),
            _make_access_entry("READER", "userByEmail", "new@example.com"),
        ]
        client, _ = self._call(
            dry_run=False, existing_entries=existing, desired_entries=desired
        )
        client.update_dataset.assert_called_once()

    def test_counters_bump_in_dry_run(self):
        """The summary counters track what *would* be applied — same value
        in dry-run as in a real run."""
        existing = [
            _make_access_entry("OWNER", "userByEmail", "owner@example.com"),
            _make_access_entry("WRITER", "userByEmail", "stale@example.com"),
        ]
        desired = [
            _make_access_entry("OWNER", "userByEmail", "owner@example.com"),
            _make_access_entry("READER", "userByEmail", "new@example.com"),
        ]
        _, ctx = self._call(
            dry_run=True, existing_entries=existing, desired_entries=desired
        )
        assert ctx.access_grants_applied == 1  # added new@
        assert ctx.access_revokes_applied == 1  # removed stale@

    def test_dry_run_uses_same_log_wording_as_real_run(self, caplog):
        """The per-line diff format is identical in both modes — the DRY
        RUN banner at run start and the ``(DRY RUN — nothing applied)``
        suffix on the summary carry the "nothing applied" signal. Operator
        reads each diff line the same way in either mode."""
        existing = [
            _make_access_entry("OWNER", "userByEmail", "owner@example.com"),
            _make_access_entry("WRITER", "userByEmail", "stale@example.com"),
        ]
        desired = [
            _make_access_entry("OWNER", "userByEmail", "owner@example.com"),
            _make_access_entry("READER", "userByEmail", "new@example.com"),
        ]
        with caplog.at_level("INFO"):
            self._call(dry_run=True, existing_entries=existing, desired_entries=desired)
        joined = "\n".join(r.message for r in caplog.records)
        # No "would" verbs anywhere; same wording as a real run.
        assert "would " not in joined
        assert "[DRY RUN]" not in joined
        assert "Updated access controls for dataset my_dataset" in joined
        assert "granted READER:userByEmail:new@example.com" in joined
        assert "revoked WRITER:userByEmail:stale@example.com" in joined

    def test_real_run_log_uses_granted_revoked(self, caplog):
        existing = [
            _make_access_entry("OWNER", "userByEmail", "owner@example.com"),
            _make_access_entry("WRITER", "userByEmail", "stale@example.com"),
        ]
        desired = [
            _make_access_entry("OWNER", "userByEmail", "owner@example.com"),
            _make_access_entry("READER", "userByEmail", "new@example.com"),
        ]
        with caplog.at_level("INFO"):
            self._call(
                dry_run=False, existing_entries=existing, desired_entries=desired
            )
        joined = "\n".join(r.message for r in caplog.records)
        assert "Updated access controls for dataset my_dataset" in joined
        assert "granted READER:userByEmail:new@example.com" in joined
        assert "revoked WRITER:userByEmail:stale@example.com" in joined

    def test_noop_when_already_in_sync(self):
        """No diff → early return; no counter bump, no log."""
        existing = [_make_access_entry("OWNER", "userByEmail", "owner@example.com")]
        desired = [_make_access_entry("OWNER", "userByEmail", "owner@example.com")]
        client, ctx = self._call(
            dry_run=False, existing_entries=existing, desired_entries=desired
        )
        client.update_dataset.assert_not_called()
        assert ctx.access_grants_applied == 0
        assert ctx.access_revokes_applied == 0


# ---------------------------------------------------------------------------
# _apply_policy_update: dry-run skip + counter bump
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestApplyPolicyUpdateDryRun:
    """Mirror of the dataset-level dry-run checks for table-level IAM."""

    def _call(self, dry_run, missing, revoked):
        from dlt_saga.destinations.bigquery.access import BigQueryAccessManager

        manager = MagicMock(spec=BigQueryAccessManager)
        manager.client = MagicMock()
        set_execution_context(None, update_access=True, dry_run=dry_run)

        BigQueryAccessManager._apply_policy_update(
            manager,
            "proj.ds.tbl",
            MagicMock(),
            missing_members=missing,
            revoked_members=revoked,
            table_id="my_table",
        )
        return manager.client, get_execution_context()

    def test_dry_run_skips_set_iam_policy(self):
        client, _ = self._call(
            dry_run=True,
            missing={"user:alice@example.com"},
            revoked={"user:bob@example.com"},
        )
        client.set_iam_policy.assert_not_called()

    def test_non_dry_run_calls_set_iam_policy(self):
        client, _ = self._call(
            dry_run=False,
            missing={"user:alice@example.com"},
            revoked=set(),
        )
        client.set_iam_policy.assert_called_once()

    def test_counters_bump_for_table_level_changes(self):
        _, ctx = self._call(
            dry_run=True,
            missing={"user:alice@example.com", "user:carol@example.com"},
            revoked={"user:bob@example.com"},
        )
        assert ctx.access_grants_applied == 2
        assert ctx.access_revokes_applied == 1

    def test_no_changes_returns_without_logging_or_bumping(self):
        client, ctx = self._call(dry_run=False, missing=set(), revoked=set())
        client.set_iam_policy.assert_not_called()
        assert ctx.access_grants_applied == 0
        assert ctx.access_revokes_applied == 0
