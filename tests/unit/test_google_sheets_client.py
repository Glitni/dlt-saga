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
