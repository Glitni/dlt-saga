"""Unit tests for the SharePoint config and its deprecated-key handling."""

import logging

import pytest

from dlt_saga.lint import AdapterTarget, check_secret_field_names
from dlt_saga.pipelines.sharepoint.config import (
    SharePointConfig,
    apply_deprecated_aliases,
)

pytestmark = pytest.mark.unit

_BODY = "grant_type=client_credentials&client_id=a@b&client_secret=s&resource=r"


def _base_kwargs(**overrides):
    kwargs = {
        "tenant_id": "tenant-guid",
        "site_url": "https://contoso.sharepoint.com/sites/X",
        "file_path": "/sites/X/Shared Documents/r.xlsx",
        "file_type": "xlsx",
        "token_request_body": _BODY,
    }
    kwargs.update(overrides)
    return kwargs


class TestSharePointConfig:
    def test_builds_with_token_request_body(self):
        cfg = SharePointConfig(**_base_kwargs())
        assert cfg.token_request_body is not None

    def test_missing_token_request_body_raises(self):
        with pytest.raises(ValueError, match="token_request_body is required"):
            SharePointConfig(**_base_kwargs(token_request_body=None))

    def test_no_secret_named_field(self):
        # Regression for #134: the config must not trip saga lint's secret-naming
        # check (no field named *_secret / *_plaintext).
        target = AdapterTarget(
            adapter="dlt_saga.sharepoint",
            source="built-in",
            config_class=SharePointConfig,
            pipeline_file=None,
        )
        assert list(check_secret_field_names(target)) == []


class TestDeprecatedAliases:
    def test_maps_auth_secret_to_token_request_body(self, caplog):
        with caplog.at_level(logging.WARNING):
            out = apply_deprecated_aliases({"auth_secret": _BODY, "tenant_id": "t"})
        assert out["token_request_body"] == _BODY
        assert "auth_secret" not in out
        assert out["tenant_id"] == "t"
        assert "deprecated" in caplog.text.lower()

    def test_current_key_wins_when_both_present(self):
        out = apply_deprecated_aliases(
            {"auth_secret": "old", "token_request_body": "new"}
        )
        assert out["token_request_body"] == "new"
        assert "auth_secret" not in out

    def test_noop_without_deprecated_key(self):
        original = {"token_request_body": _BODY, "tenant_id": "t"}
        assert apply_deprecated_aliases(original) == original

    def test_deprecated_config_still_builds_via_alias(self):
        # End-to-end: an old config dict (auth_secret) resolves through the alias.
        old_config = _base_kwargs()
        old_config["auth_secret"] = old_config.pop("token_request_body")
        cfg = SharePointConfig(**apply_deprecated_aliases(old_config))
        assert cfg.token_request_body is not None
