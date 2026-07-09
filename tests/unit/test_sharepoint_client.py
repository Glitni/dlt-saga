"""Unit tests for SharePoint token acquisition and auth-method routing."""

import base64
import io
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
        resp = MagicMock(ok=True, status_code=200)
        resp.json.return_value = {"access_token": "acs-token", "expires_in": 3600}
        request = MagicMock(return_value=resp)
        monkeypatch.setattr(
            "dlt_saga.pipelines.sharepoint.client.requests.request", request
        )

        client = SharePointClient(_acs_config())
        token = client._acquire_token()

        assert token == "acs-token"
        # requests.request(method, url, ...) → method then url positionally.
        assert request.call_args.args[0] == "POST"
        assert "accesscontrol.windows.net" in request.call_args.args[1]

    def test_acs_path_emits_deprecation_warning(self, monkeypatch, caplog):
        resp = MagicMock(ok=True, status_code=200)
        resp.json.return_value = {"access_token": "acs-token"}
        monkeypatch.setattr(
            "dlt_saga.pipelines.sharepoint.client.requests.request",
            MagicMock(return_value=resp),
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


class TestRequestRetryAndErrors:
    """_request retries throttling/5xx and surfaces the response body on error."""

    def _client(self):
        client = SharePointClient(_cert_config())
        client._token = "tok"
        return client

    def test_retries_on_429_then_succeeds(self, monkeypatch):
        throttled = MagicMock(ok=False, status_code=429)
        throttled.headers = {"Retry-After": "0"}
        ok = MagicMock(ok=True, status_code=200)
        request = MagicMock(side_effect=[throttled, ok])
        monkeypatch.setattr(
            "dlt_saga.pipelines.sharepoint.client.requests.request", request
        )
        monkeypatch.setattr(
            "dlt_saga.pipelines.sharepoint.client.time.sleep", lambda *_: None
        )

        result = self._client()._request("GET", "https://x/y")
        assert result is ok
        assert request.call_count == 2

    def test_honors_retry_after_header(self, monkeypatch):
        throttled = MagicMock(ok=False, status_code=503)
        throttled.headers = {"Retry-After": "7"}
        ok = MagicMock(ok=True, status_code=200)
        monkeypatch.setattr(
            "dlt_saga.pipelines.sharepoint.client.requests.request",
            MagicMock(side_effect=[throttled, ok]),
        )
        slept = []
        monkeypatch.setattr(
            "dlt_saga.pipelines.sharepoint.client.time.sleep", slept.append
        )

        self._client()._request("GET", "https://x/y")
        assert slept == [7.0]

    def test_error_body_included_in_raised_message(self, monkeypatch):
        resp = MagicMock(ok=False, status_code=403, reason="Forbidden")
        resp.headers = {}
        resp.text = '{"error":{"message":"Access denied. You do not have permission."}}'
        monkeypatch.setattr(
            "dlt_saga.pipelines.sharepoint.client.requests.request",
            MagicMock(return_value=resp),
        )

        import requests

        with pytest.raises(requests.HTTPError, match="Access denied"):
            self._client()._request("GET", "https://x/y")


class TestCsvParsing:
    def test_duplicate_headers_deduped_not_collapsed(self):
        # A repeated "id" column must survive as id / id_2 instead of the second
        # silently overwriting the first (csv.DictReader's behaviour).
        client = SharePointClient(_cert_config(file_type="csv"))
        rows = client.read_csv(b"id,name,id\n1,alice,2\n")
        assert rows == [{"id": "1", "name": "alice", "id_2": "2"}]

    def test_utf8_bom_stripped_from_first_header(self):
        # BOM-prefixed export (e.g. Excel "CSV UTF-8"): the leading BOM must not
        # end up glued to the first column name.
        client = SharePointClient(_cert_config(file_type="csv"))
        rows = client.read_csv("id,name\n1,alice\n".encode("utf-8-sig"))
        assert rows == [{"id": "1", "name": "alice"}]

    def test_short_row_filled_with_none(self):
        client = SharePointClient(_cert_config(file_type="csv"))
        rows = client.read_csv(b"a,b,c\n1,2\n")
        assert rows == [{"a": "1", "b": "2", "c": None}]

    def test_empty_file_yields_no_rows(self):
        client = SharePointClient(_cert_config(file_type="csv"))
        assert client.read_csv(b"") == []


class TestExcelParsing:
    def test_duplicate_headers_deduped_not_collapsed(self):
        openpyxl = pytest.importorskip("openpyxl")
        wb = openpyxl.Workbook()
        ws = wb.active
        ws.append(["id", "name", "id"])
        ws.append([1, "alice", 2])
        buf = io.BytesIO()
        wb.save(buf)

        client = SharePointClient(_cert_config(file_type="xlsx"))
        rows = client.read_excel(buf.getvalue())
        assert rows == [{"id": 1, "name": "alice", "id_2": 2}]


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
