"""Unit tests for SharePoint token acquisition and auth-method routing."""

import base64
import logging
import sys
import types
from unittest.mock import MagicMock

import pytest

from dlt_saga.pipelines.sharepoint.client import SharePointClient
from dlt_saga.pipelines.sharepoint.config import SharePointConfig

pytestmark = pytest.mark.unit

_BODY = "grant_type=client_credentials&client_id=a@b&client_secret=s&resource=r"
_CERT = "-----BEGIN PRIVATE KEY-----\nMIIB\n-----END PRIVATE KEY-----"


def _cert_config(**overrides):
    kwargs = {
        "tenant_id": "tenant-guid",
        "site_url": "https://contoso.sharepoint.com/sites/X",
        "file_path": "/sites/X/Shared Documents/r.xlsx",
        "file_type": "xlsx",
        "client_id": "client-guid",
        "certificate": _CERT,
    }
    kwargs.update(overrides)
    return SharePointConfig(**kwargs)


def _acs_config(**overrides):
    kwargs = {
        "tenant_id": "tenant-guid",
        "site_url": "https://contoso.sharepoint.com/sites/X",
        "file_path": "/sites/X/Shared Documents/r.xlsx",
        "file_type": "xlsx",
        "token_request_body": _BODY,
    }
    kwargs.update(overrides)
    return SharePointConfig(**kwargs)


@pytest.fixture
def fake_certificate_credential(monkeypatch):
    """Install a fake azure.identity.CertificateCredential and expose the mock."""
    credential_instance = MagicMock()
    credential_instance.get_token.return_value = MagicMock(
        token="entra-token", expires_on=123
    )
    credential_cls = MagicMock(return_value=credential_instance)

    module = types.ModuleType("azure.identity")
    module.CertificateCredential = credential_cls
    monkeypatch.setitem(sys.modules, "azure.identity", module)
    return credential_cls


class TestEntraIdAuth:
    def test_certificate_path_returns_entra_token(self, fake_certificate_credential):
        client = SharePointClient(_cert_config())
        assert client._acquire_token() == "entra-token"

    def test_certificate_scope_derived_from_site_url(self, fake_certificate_credential):
        client = SharePointClient(_cert_config())
        client._acquire_token()

        credential_instance = fake_certificate_credential.return_value
        scope = credential_instance.get_token.call_args.args[0]
        assert scope == "https://contoso.sharepoint.com/.default"

    def test_certificate_passed_to_credential(self, fake_certificate_credential):
        client = SharePointClient(_cert_config())
        client._acquire_token()

        kwargs = fake_certificate_credential.call_args.kwargs
        assert kwargs["tenant_id"] == "tenant-guid"
        assert kwargs["client_id"] == "client-guid"
        assert kwargs["certificate_data"] == _CERT.encode("utf-8")

    def test_certificate_path_does_not_warn(self, fake_certificate_credential, caplog):
        client = SharePointClient(_cert_config())
        with caplog.at_level(logging.WARNING):
            client._acquire_token()
        assert "Azure ACS" not in caplog.text

    def test_client_id_and_tenant_id_resolved_via_secrets(
        self, fake_certificate_credential, monkeypatch
    ):
        # Identifiers may be secret URIs / env vars — resolved at runtime.
        resolved = {
            "secret::cid": "resolved-client-id",
            "secret::tid": "resolved-tenant-id",
            "secret::cert": _CERT,
        }

        def fake_resolve(value):
            key = (
                value.get_secret_value()
                if hasattr(value, "get_secret_value")
                else value
            )
            return resolved.get(key, key)

        monkeypatch.setattr(
            "dlt_saga.pipelines.sharepoint.client.resolve_secret", fake_resolve
        )
        client = SharePointClient(
            _cert_config(
                client_id="secret::cid",
                tenant_id="secret::tid",
                certificate="secret::cert",
            )
        )
        client._acquire_token()

        kwargs = fake_certificate_credential.call_args.kwargs
        assert kwargs["client_id"] == "resolved-client-id"
        assert kwargs["tenant_id"] == "resolved-tenant-id"


class TestLegacyAcsAuth:
    def test_acs_path_posts_to_acs_endpoint(self, monkeypatch):
        post = MagicMock()
        post.return_value = MagicMock(
            json=lambda: {"access_token": "acs-token", "expires_in": 3600}
        )
        monkeypatch.setattr("dlt_saga.pipelines.sharepoint.client.requests.post", post)

        client = SharePointClient(_acs_config())
        token = client._acquire_token()

        assert token == "acs-token"
        url = post.call_args.args[0]
        assert "accesscontrol.windows.net" in url

    def test_acs_path_emits_deprecation_warning(self, monkeypatch, caplog):
        monkeypatch.setattr(
            "dlt_saga.pipelines.sharepoint.client.requests.post",
            MagicMock(
                return_value=MagicMock(json=lambda: {"access_token": "acs-token"})
            ),
        )
        client = SharePointClient(_acs_config())
        with caplog.at_level(logging.WARNING):
            client._acquire_token()
        assert "Azure ACS" in caplog.text
        assert "retired" in caplog.text.lower()

    def test_certificate_takes_precedence_over_acs(self, fake_certificate_credential):
        # Both configured → certificate path wins, ACS is not touched.
        config = _cert_config(token_request_body=_BODY)
        client = SharePointClient(config)
        assert client._acquire_token() == "entra-token"


class TestScopeDerivation:
    def test_resource_strips_path(self):
        client = SharePointClient(_cert_config())
        assert client._sharepoint_resource() == "https://contoso.sharepoint.com"


class TestCertificateBytes:
    def test_pem_returned_as_utf8_bytes(self):
        client = SharePointClient(_cert_config(certificate=_CERT))
        assert client._certificate_bytes() == _CERT.encode("utf-8")

    def test_base64_pkcs12_decoded_to_raw_bytes(self):
        # Azure Key Vault returns PKCS#12 certs as base64 (no PEM header).
        pfx_bytes = b"\x30\x82\x01\x00fake-pfx-der"
        encoded = base64.b64encode(pfx_bytes).decode("ascii")
        client = SharePointClient(_cert_config(certificate=encoded))
        assert client._certificate_bytes() == pfx_bytes

    def test_invalid_certificate_raises(self):
        client = SharePointClient(_cert_config(certificate="not base64!!"))
        with pytest.raises(ValueError, match="neither PEM.*nor"):
            client._certificate_bytes()
