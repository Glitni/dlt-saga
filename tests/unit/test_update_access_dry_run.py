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
# Table-level IAM reconcile (BigQueryAccessManager over the base template):
# dry-run skip + counter bump + special-member protection
# ---------------------------------------------------------------------------


def _make_viewer_policy(members):
    """A fake IAM policy carrying a dataViewer binding with `members`."""
    return SimpleNamespace(
        bindings=[{"role": "roles/bigquery.dataViewer", "members": list(members)}]
    )


@pytest.mark.unit
class TestBigQueryAccessManagerReconcile:
    """Table-level access is diff-based: grant/revoke only the delta, honour
    --dry-run (skip set_iam_policy but still bump counters + log), and never
    revoke special (non-managed) IAM members. The control flow now lives in
    the AccessManager base template; this exercises it via the BigQuery hooks.
    """

    def _run(self, dry_run, current_members, access_config, revoke_extra=True):
        from unittest.mock import PropertyMock, patch

        from dlt_saga.destinations.bigquery.access import BigQueryAccessManager

        client = MagicMock()
        client.get_iam_policy.return_value = _make_viewer_policy(current_members)

        mgr = BigQueryAccessManager(project_id="proj", dataset_id="ds")
        set_execution_context(None, update_access=True, dry_run=dry_run)
        with patch.object(
            BigQueryAccessManager, "client", new_callable=PropertyMock
        ) as client_prop:
            client_prop.return_value = client
            mgr.manage_access_for_tables(
                ["tbl"], access_config=access_config, revoke_extra=revoke_extra
            )
        return client, get_execution_context()

    def test_dry_run_skips_set_iam_policy(self):
        client, _ = self._run(
            dry_run=True,
            current_members=["user:owner@example.com"],
            access_config=["user:new@example.com"],
        )
        client.set_iam_policy.assert_not_called()

    def test_non_dry_run_calls_set_iam_policy(self):
        client, _ = self._run(
            dry_run=False,
            current_members=[],
            access_config=["user:new@example.com"],
        )
        client.set_iam_policy.assert_called_once()

    def test_counters_bump_in_dry_run(self):
        _, ctx = self._run(
            dry_run=True,
            current_members=["user:stale@example.com"],
            access_config=["user:new@example.com"],
        )
        assert ctx.access_grants_applied == 1  # added new@
        assert ctx.access_revokes_applied == 1  # removed stale@

    def test_no_changes_returns_without_bumping(self):
        client, ctx = self._run(
            dry_run=False,
            current_members=["user:keep@example.com"],
            access_config=["user:keep@example.com"],
        )
        client.set_iam_policy.assert_not_called()
        assert ctx.access_grants_applied == 0
        assert ctx.access_revokes_applied == 0

    def test_special_members_are_not_revoked(self):
        # A non-managed entry (no user:/group:/serviceAccount: prefix) must
        # survive revoke_extra — table-level management never removes it.
        _, ctx = self._run(
            dry_run=False,
            current_members=["projectOwner:proj", "user:stale@example.com"],
            access_config=["user:new@example.com"],
            revoke_extra=True,
        )
        assert ctx.access_grants_applied == 1
        assert ctx.access_revokes_applied == 1  # only user:stale, not projectOwner

    def test_dry_run_uses_same_log_wording_as_real_run(self, caplog):
        with caplog.at_level("INFO"):
            self._run(
                dry_run=True,
                current_members=["user:stale@example.com"],
                access_config=["user:new@example.com"],
            )
        joined = "\n".join(r.message for r in caplog.records)
        assert "would " not in joined
        assert "[DRY RUN]" not in joined
        assert "Updated access for table tbl" in joined
        assert "+ granted user:new@example.com" in joined
        assert "- revoked user:stale@example.com" in joined
