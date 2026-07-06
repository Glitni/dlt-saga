"""Unit tests for gcloud_auth: patch idempotency and deterministic ADC source."""

import os

import pytest


@pytest.mark.unit
class TestResolveAdcExcludingGac:
    """resolve_adc_excluding_gac must source impersonation from the profile
    identity, excluding a stray GOOGLE_APPLICATION_CREDENTIALS only for the
    duration of the resolution call and restoring it afterwards."""

    def test_gac_excluded_during_call_and_restored(self, monkeypatch):
        from dlt_saga.utility.cli.gcloud_auth import (
            _GAC_ENV,
            resolve_adc_excluding_gac,
        )

        monkeypatch.setenv(_GAC_ENV, "/tmp/key.json")

        seen = {}

        def fake_default(*args, **kwargs):
            seen["gac"] = os.environ.get(_GAC_ENV, "<unset>")
            return "creds", "project"

        result = resolve_adc_excluding_gac(fake_default)

        assert result == ("creds", "project")
        assert seen["gac"] == "<unset>", "GAC must be excluded during resolution"
        assert os.environ.get(_GAC_ENV) == "/tmp/key.json", "GAC must be restored"

    def test_gac_restored_on_exception(self, monkeypatch):
        from dlt_saga.utility.cli.gcloud_auth import (
            _GAC_ENV,
            resolve_adc_excluding_gac,
        )

        monkeypatch.setenv(_GAC_ENV, "/tmp/key.json")

        def boom(*args, **kwargs):
            raise RuntimeError("resolution failed")

        with pytest.raises(RuntimeError):
            resolve_adc_excluding_gac(boom)

        assert os.environ.get(_GAC_ENV) == "/tmp/key.json"

    def test_noop_when_gac_unset(self, monkeypatch):
        from dlt_saga.utility.cli.gcloud_auth import (
            _GAC_ENV,
            resolve_adc_excluding_gac,
        )

        monkeypatch.delenv(_GAC_ENV, raising=False)

        seen = {}

        def fake_default(*args, **kwargs):
            seen["present"] = _GAC_ENV in os.environ
            return "creds", "project"

        resolve_adc_excluding_gac(fake_default)

        assert seen["present"] is False
        assert _GAC_ENV not in os.environ

    def test_passes_through_args(self, monkeypatch):
        from dlt_saga.utility.cli.gcloud_auth import resolve_adc_excluding_gac

        captured = {}

        def fake_default(*args, **kwargs):
            captured["args"] = args
            captured["kwargs"] = kwargs
            return None, None

        resolve_adc_excluding_gac(fake_default, "pos", scopes=["scope-a"])

        assert captured["args"] == ("pos",)
        assert captured["kwargs"] == {"scopes": ["scope-a"]}


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
