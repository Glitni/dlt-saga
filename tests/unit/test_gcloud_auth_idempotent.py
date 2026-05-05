"""Unit test for Phase 10: patch_google_auth_default idempotency."""

import pytest


@pytest.mark.unit
class TestPatchGoogleAuthDefaultIdempotency:
    def test_second_call_does_not_rewrap(self, monkeypatch):
        import google.auth

        from dlt_saga.utility.cli.gcloud_auth import (
            _PATCH_SENTINEL,
            patch_google_auth_default,
        )

        def fake_original(*args, **kwargs):
            return None, "project"

        monkeypatch.setattr(google.auth, "default", fake_original)

        patch_google_auth_default()
        patched_once = google.auth.default

        assert getattr(patched_once, _PATCH_SENTINEL, False) is True

        patch_google_auth_default()
        assert google.auth.default is patched_once, (
            "Second call to patch_google_auth_default() replaced the wrapper — "
            "this would cause doubly-impersonated credentials."
        )
