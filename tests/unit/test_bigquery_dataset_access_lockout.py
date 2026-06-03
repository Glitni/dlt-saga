"""Tests for the ``_update_access_if_needed`` lockout safeguard.

`saga update-access` rebuilds the managed access entries on a dataset
from the declared ``dataset_access:`` list. If the running principal's
own OWNER entry is omitted from the list, applying the change strips
the very permission saga needs to make further changes — the next
update-access run 403s and recovery requires a project-Owner to
re-grant manually. The safeguard refuses to apply the change in that
case and tells the operator what to add.
"""

from unittest.mock import patch

import pytest

from dlt_saga.destinations.bigquery.base import (
    BigQueryBaseDestination,
    DatasetAccessLockoutError,
    DatasetAccessMissingOwnerError,
    _running_principal_email,
    _suggest_owner_entry,
)

PRINCIPAL = "dlt-run-sa@amedia-adp-sources.iam.gserviceaccount.com"
PRINCIPAL_OWNER_KEY = ("OWNER", "userByEmail", PRINCIPAL)


# ---------------------------------------------------------------------------
# _running_principal_email
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestRunningPrincipalEmail:
    def test_returns_impersonated_sa_when_env_set(self, monkeypatch):
        monkeypatch.setenv("GOOGLE_IMPERSONATE_SERVICE_ACCOUNT", PRINCIPAL)
        assert _running_principal_email() == PRINCIPAL

    def test_falls_back_to_adc_service_account_email(self, monkeypatch):
        monkeypatch.delenv("GOOGLE_IMPERSONATE_SERVICE_ACCOUNT", raising=False)

        fake_creds = type(
            "FakeSACreds",
            (),
            {"service_account_email": "adc-sa@example.iam.gserviceaccount.com"},
        )()
        with patch("google.auth.default", return_value=(fake_creds, "proj")):
            assert (
                _running_principal_email() == "adc-sa@example.iam.gserviceaccount.com"
            )

    def test_returns_none_for_user_credentials(self, monkeypatch):
        """User credentials from ``gcloud auth application-default login``
        don't expose ``service_account_email`` — the safeguard degrades to
        a no-op rather than blocking."""
        monkeypatch.delenv("GOOGLE_IMPERSONATE_SERVICE_ACCOUNT", raising=False)
        fake_creds = type("FakeUserCreds", (), {})()  # no service_account_email
        with patch("google.auth.default", return_value=(fake_creds, "proj")):
            assert _running_principal_email() is None

    def test_returns_none_when_adc_throws(self, monkeypatch):
        monkeypatch.delenv("GOOGLE_IMPERSONATE_SERVICE_ACCOUNT", raising=False)
        with patch("google.auth.default", side_effect=RuntimeError("no creds")):
            assert _running_principal_email() is None


# ---------------------------------------------------------------------------
# _suggest_owner_entry
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestSuggestOwnerEntry:
    def test_service_account_uses_service_account_prefix(self):
        assert (
            _suggest_owner_entry("sa@project.iam.gserviceaccount.com")
            == "OWNER:serviceAccount:sa@project.iam.gserviceaccount.com"
        )

    def test_human_user_uses_user_prefix(self):
        assert (
            _suggest_owner_entry("alice@example.com") == "OWNER:user:alice@example.com"
        )


# ---------------------------------------------------------------------------
# _check_lockout
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestCheckLockout:
    """The safeguard fires only when the running principal currently has a
    managed OWNER entry that the desired list would remove."""

    def test_raises_when_desired_would_remove_running_owner(self, monkeypatch):
        monkeypatch.setenv("GOOGLE_IMPERSONATE_SERVICE_ACCOUNT", PRINCIPAL)
        existing = {
            PRINCIPAL_OWNER_KEY,
            ("READER", "userByEmail", "viewer@example.com"),
        }
        desired = {("READER", "userByEmail", "viewer@example.com")}  # OWNER absent

        with pytest.raises(DatasetAccessLockoutError) as exc_info:
            BigQueryBaseDestination._check_lockout(existing, desired, "my_dataset")

        msg = str(exc_info.value)
        assert "my_dataset" in msg
        assert PRINCIPAL in msg
        # The suggested entry the operator should paste back is in the message.
        assert f"OWNER:serviceAccount:{PRINCIPAL}" in msg

    def test_noop_when_desired_preserves_running_owner(self, monkeypatch):
        monkeypatch.setenv("GOOGLE_IMPERSONATE_SERVICE_ACCOUNT", PRINCIPAL)
        existing = {PRINCIPAL_OWNER_KEY}
        desired = {PRINCIPAL_OWNER_KEY, ("READER", "userByEmail", "new@example.com")}

        # Should not raise
        BigQueryBaseDestination._check_lockout(existing, desired, "my_dataset")

    def test_noop_when_running_principal_doesnt_currently_own(self, monkeypatch):
        """If the principal doesn't have OWNER on the dataset today
        (maybe they have project-level IAM instead), there's nothing the
        access-list change can strip — the safeguard is silent."""
        monkeypatch.setenv("GOOGLE_IMPERSONATE_SERVICE_ACCOUNT", PRINCIPAL)
        existing = {("READER", "userByEmail", "viewer@example.com")}
        desired = {("READER", "userByEmail", "viewer@example.com")}

        BigQueryBaseDestination._check_lockout(existing, desired, "my_dataset")

    def test_noop_when_principal_cannot_be_identified(self, monkeypatch):
        """User-credential dev runs (no service_account_email) skip the
        check. The bug only surfaces in prod-impersonation runs anyway."""
        monkeypatch.delenv("GOOGLE_IMPERSONATE_SERVICE_ACCOUNT", raising=False)
        existing = {PRINCIPAL_OWNER_KEY}
        desired: set = set()

        with patch(
            "dlt_saga.destinations.bigquery.base._running_principal_email",
            return_value=None,
        ):
            BigQueryBaseDestination._check_lockout(existing, desired, "my_dataset")


# ---------------------------------------------------------------------------
# _require_owner_entry
# ---------------------------------------------------------------------------


def _access_entry(role: str | None, entity_type: str, entity_id: str):
    """Build a stand-in AccessEntry-shaped object for tests. The real type
    is ``google.cloud.bigquery.AccessEntry`` but ``_require_owner_entry``
    only reads ``.role``, so a SimpleNamespace is enough."""
    from types import SimpleNamespace

    return SimpleNamespace(role=role, entity_type=entity_type, entity_id=entity_id)


@pytest.mark.unit
class TestRequireOwnerEntry:
    """Any ``dataset_access:`` list applied to a BigQuery dataset must
    declare at least one OWNER, otherwise the BQ-auto-added creator
    OWNER gets stripped on the first apply, leaving the dataset with no
    managed owner."""

    def test_raises_when_list_has_no_owner(self, monkeypatch):
        monkeypatch.setenv("GOOGLE_IMPERSONATE_SERVICE_ACCOUNT", PRINCIPAL)
        entries = [
            _access_entry("READER", "userByEmail", "reader@example.com"),
            _access_entry("WRITER", "userByEmail", "writer@example.com"),
        ]
        with pytest.raises(DatasetAccessMissingOwnerError) as exc_info:
            BigQueryBaseDestination._require_owner_entry(entries, "dlt_orchestration")
        msg = str(exc_info.value)
        assert "dlt_orchestration" in msg
        assert "no OWNER entry" in msg
        # Suggested entry uses the running principal.
        assert f"OWNER:serviceAccount:{PRINCIPAL}" in msg

    def test_noop_when_list_has_owner(self, monkeypatch):
        monkeypatch.setenv("GOOGLE_IMPERSONATE_SERVICE_ACCOUNT", PRINCIPAL)
        entries = [
            _access_entry("OWNER", "userByEmail", PRINCIPAL),
            _access_entry("READER", "userByEmail", "reader@example.com"),
        ]
        # Should not raise
        BigQueryBaseDestination._require_owner_entry(entries, "dlt_api")

    def test_raises_when_only_authorized_dataset_entries(self, monkeypatch):
        """AUTHORIZED_DATASET entries carry ``role=None`` — they don't
        satisfy the OWNER requirement, and a list of only those would
        strip the creator's OWNER on apply."""
        monkeypatch.setenv("GOOGLE_IMPERSONATE_SERVICE_ACCOUNT", PRINCIPAL)
        entries = [
            _access_entry(None, "dataset", "project-id.dataset-id"),
        ]
        with pytest.raises(DatasetAccessMissingOwnerError):
            BigQueryBaseDestination._require_owner_entry(entries, "dlt_api")

    def test_error_message_omits_specific_suggestion_when_principal_unknown(
        self, monkeypatch
    ):
        """User-credential dev runs (no identifiable principal) still
        block — but the hint can't be paste-back-specific, so it generalises."""
        monkeypatch.delenv("GOOGLE_IMPERSONATE_SERVICE_ACCOUNT", raising=False)
        entries = [_access_entry("READER", "userByEmail", "reader@example.com")]
        with patch(
            "dlt_saga.destinations.bigquery.base._running_principal_email",
            return_value=None,
        ):
            with pytest.raises(DatasetAccessMissingOwnerError) as exc_info:
                BigQueryBaseDestination._require_owner_entry(entries, "dlt_api")
        msg = str(exc_info.value)
        assert "no OWNER entry" in msg
        # Generic hint, no `OWNER:serviceAccount:` paste-back line.
        assert "OWNER:serviceAccount:" not in msg
