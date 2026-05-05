"""Unit tests for the GcpAuthProvider wrapper.

Covers happy-path validation, the impersonation context-manager lifecycle,
and the AuthenticationError surface that user-facing CLI commands expect.
"""

from unittest.mock import MagicMock, patch

import pytest

from dlt_saga.utility.auth.gcp import GcpAuthProvider
from dlt_saga.utility.auth.providers import AuthenticationError


@pytest.mark.unit
class TestGcpAuthProviderValidate:
    def test_validate_succeeds_when_adc_configured(self):
        with patch(
            "dlt_saga.utility.cli.gcloud_auth.check_adc_configured", return_value=True
        ):
            GcpAuthProvider().validate()

    def test_validate_raises_when_adc_not_configured(self):
        with patch(
            "dlt_saga.utility.cli.gcloud_auth.check_adc_configured", return_value=False
        ):
            with pytest.raises(
                AuthenticationError, match="GCP credentials not configured"
            ):
                GcpAuthProvider().validate()

    def test_validate_message_points_user_to_remedy(self):
        with patch(
            "dlt_saga.utility.cli.gcloud_auth.check_adc_configured", return_value=False
        ):
            with pytest.raises(
                AuthenticationError, match="gcloud auth application-default login"
            ):
                GcpAuthProvider().validate()


@pytest.mark.unit
class TestGcpAuthProviderImpersonation:
    def test_supports_impersonation_returns_true(self):
        assert GcpAuthProvider().supports_impersonation() is True

    def test_impersonate_runs_setup_then_teardown(self):
        manager = MagicMock()
        with patch(
            "dlt_saga.utility.cli.gcloud_auth.ImpersonationManager",
            return_value=manager,
        ):
            with GcpAuthProvider().impersonate("sa@project.iam.gserviceaccount.com"):
                manager.setup_impersonation.assert_called_once_with(
                    "sa@project.iam.gserviceaccount.com"
                )
                manager.teardown_impersonation.assert_not_called()
            manager.teardown_impersonation.assert_called_once()

    def test_impersonate_teardown_runs_when_body_raises(self):
        manager = MagicMock()
        with patch(
            "dlt_saga.utility.cli.gcloud_auth.ImpersonationManager",
            return_value=manager,
        ):
            with pytest.raises(RuntimeError):
                with GcpAuthProvider().impersonate(
                    "sa@project.iam.gserviceaccount.com"
                ):
                    raise RuntimeError("boom")
            manager.teardown_impersonation.assert_called_once()

    def test_impersonate_setup_failure_wrapped_in_authentication_error(self):
        manager = MagicMock()
        manager.setup_impersonation.side_effect = RuntimeError("permission denied")
        with patch(
            "dlt_saga.utility.cli.gcloud_auth.ImpersonationManager",
            return_value=manager,
        ):
            with pytest.raises(AuthenticationError, match="permission denied"):
                with GcpAuthProvider().impersonate(
                    "sa@project.iam.gserviceaccount.com"
                ):
                    pass  # pragma: no cover — body never enters
            manager.teardown_impersonation.assert_called_once()
