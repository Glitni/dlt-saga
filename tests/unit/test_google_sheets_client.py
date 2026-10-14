"""Unit tests for GSheetsClient service caching."""

from unittest.mock import MagicMock, patch

import pytest

from dlt_saga.pipelines.google_sheets.client import GSheetsClient
from dlt_saga.pipelines.google_sheets.config import GSheetsConfig


def _client() -> GSheetsClient:
    config = GSheetsConfig(spreadsheet_id="sheet-1", sheet_name="Sheet1")
    return GSheetsClient(config)


@pytest.mark.unit
class TestGSheetsServiceCaching:
    """Each API service is built once per client and reused, so a run doesn't
    re-fetch the discovery document for every call."""

    def test_sheets_service_built_once_and_reused(self):
        with patch("dlt_saga.utility.gcp.client_pool.google_api_pool") as mock_pool:
            mock_pool.get_client.return_value = MagicMock(name="sheets_service")
            client = _client()
            first = client.get_sheets_service()
            second = client.get_sheets_service()
            # get_service() delegates to the same cached sheets service.
            third = client.get_service()

        assert first is second is third
        mock_pool.get_client.assert_called_once()
        assert mock_pool.get_client.call_args.args == ("sheets", "v4")

    def test_drive_service_built_once_and_reused(self):
        with patch("dlt_saga.utility.gcp.client_pool.google_api_pool") as mock_pool:
            mock_pool.get_client.return_value = MagicMock(name="drive_service")
            client = _client()
            first = client.get_drive_service()
            second = client.get_drive_service()

        assert first is second
        mock_pool.get_client.assert_called_once()
        assert mock_pool.get_client.call_args.args == ("drive", "v3")

    def test_sheets_and_drive_are_distinct_builds(self):
        with patch("dlt_saga.utility.gcp.client_pool.google_api_pool") as mock_pool:
            mock_pool.get_client.side_effect = [
                MagicMock(name="sheets"),
                MagicMock(name="drive"),
            ]
            client = _client()
            client.get_sheets_service()
            client.get_drive_service()

        assert mock_pool.get_client.call_count == 2


def _service_returning(values):
    """Build a fake Sheets service whose values().get().execute() yields values."""
    service = MagicMock()
    service.spreadsheets.return_value.values.return_value.get.return_value.execute.return_value = {  # noqa: E501
        "values": values
    }
    return service


@pytest.mark.unit
class TestGetSheetData:
    def test_duplicate_headers_deduped_not_collapsed(self):
        client = _client()
        service = _service_returning([["id", "name", "id"], ["1", "alice", "2"]])
        with patch.object(client, "get_service", return_value=service):
            rows = list(client.get_sheet_data("sheet-1", "Sheet1"))
        assert rows == [{"id": "1", "name": "alice", "id_2": "2"}]

    def test_short_row_padded_with_empty_string(self):
        client = _client()
        service = _service_returning([["a", "b", "c"], ["1", "2"]])
        with patch.object(client, "get_service", return_value=service):
            rows = list(client.get_sheet_data("sheet-1", "Sheet1"))
        assert rows == [{"a": "1", "b": "2", "c": ""}]

    def test_empty_sheet_yields_nothing(self):
        client = _client()
        service = _service_returning([])
        with patch.object(client, "get_service", return_value=service):
            rows = list(client.get_sheet_data("sheet-1", "Sheet1"))
        assert rows == []


@pytest.mark.unit
class TestGSheetsCredentialArgs:
    """credentials (preferred) resolves secret URIs by value; secret_id and the
    sheets_secret_name project default keep working as deprecated fallbacks."""

    def _args(self, **config):
        client = GSheetsClient(
            GSheetsConfig(spreadsheet_id="sheet-1", sheet_name="Sheet1", **config)
        )
        with patch(
            "dlt_saga.utility.gcp.client_pool._get_google_secrets_value",
            return_value="project-default-secret",
        ):
            args = client._prepare_credentials_args()
        assert args.pop("scopes")
        return args

    def test_credentials_secret_uri_resolves_to_service_account_json(self, monkeypatch):
        monkeypatch.setenv("SAGA_TEST_SHEETS_SA", '{"type": "service_account"}')
        args = self._args(credentials="env_secret::SAGA_TEST_SHEETS_SA")
        assert args == {"credentials_dict": {"type": "service_account"}}

    def test_credentials_dict_and_path_still_work(self):
        assert self._args(credentials={"type": "service_account"}) == {
            "credentials_dict": {"type": "service_account"}
        }
        assert self._args(credentials="/keys/sa.json") == {
            "credentials_path": "/keys/sa.json"
        }

    def test_credentials_take_precedence_over_secret_id(self):
        args = self._args(credentials="/keys/sa.json", secret_id="legacy")
        assert args == {"credentials_path": "/keys/sa.json"}

    def test_deprecated_secret_id_still_works(self):
        assert self._args(secret_id="legacy") == {"secret_id": "legacy"}

    def test_deprecated_project_default_still_works(self):
        assert self._args() == {"secret_id": "project-default-secret"}

    def test_no_credentials_falls_back_to_adc_without_warning(self, caplog):
        client = GSheetsClient(
            GSheetsConfig(spreadsheet_id="sheet-1", sheet_name="Sheet1")
        )
        with patch(
            "dlt_saga.utility.gcp.client_pool._get_google_secrets_value",
            return_value=None,
        ):
            args = client._prepare_credentials_args()
        assert args["secret_id"] is None
        assert not caplog.records


@pytest.mark.unit
class TestGSheetsCredentialWarnings:
    """Deprecated credential sources warn, once per client; credentials
    silently overriding a pipeline's own secret_id is surfaced."""

    def _warnings(self, caplog, project_default=None, **config):
        client = GSheetsClient(
            GSheetsConfig(spreadsheet_id="sheet-1", sheet_name="Sheet1", **config)
        )
        with (
            caplog.at_level(
                "WARNING", logger="dlt_saga.pipelines.google_sheets.client"
            ),
            patch(
                "dlt_saga.utility.gcp.client_pool._get_google_secrets_value",
                return_value=project_default,
            ),
        ):
            # both the Sheets and the Drive service resolve credentials
            client._prepare_credentials_args()
            client._prepare_credentials_args()
        return [r.getMessage() for r in caplog.records if r.levelname == "WARNING"]

    def test_secret_id_warns_deprecated_once(self, caplog):
        (message,) = self._warnings(caplog, secret_id="legacy")
        assert "secret_id is deprecated" in message

    def test_project_default_warns_deprecated(self, caplog):
        (message,) = self._warnings(caplog, project_default="project-default")
        assert "sheets_secret_name is deprecated" in message

    def test_credentials_with_secret_id_warns_secret_id_ignored(self, caplog):
        (message,) = self._warnings(
            caplog, credentials="/keys/sa.json", secret_id="legacy"
        )
        assert "secret_id is ignored" in message

    def test_credentials_alone_does_not_warn(self, caplog):
        assert self._warnings(caplog, credentials="/keys/sa.json") == []
