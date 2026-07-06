"""Unit tests for Google Sheets config + one-config-one-table extraction.

A google_sheets config maps to exactly one sheet/tab (one destination table);
`sheet_name` is required, and extraction never fans out to multiple tables.
"""

from unittest.mock import MagicMock

import pytest

from dlt_saga.pipelines.google_sheets.client import GSheetsClient
from dlt_saga.pipelines.google_sheets.config import GSheetsConfig
from dlt_saga.pipelines.google_sheets.pipeline import GoogleSheetsPipeline


@pytest.mark.unit
class TestGSheetsConfigValidation:
    def test_valid_config(self):
        cfg = GSheetsConfig(spreadsheet_id="abc", sheet_name="Sheet1")
        assert cfg.sheet_name == "Sheet1"

    def test_sheet_name_required(self):
        with pytest.raises(ValueError, match="sheet_name is required"):
            GSheetsConfig(spreadsheet_id="abc")

    def test_range_defaults_to_full_sheet(self):
        # Default range is unset so the whole sheet is read (no A:Z truncation).
        assert GSheetsConfig(spreadsheet_id="abc", sheet_name="s").range is None

    def test_spreadsheet_id_required(self):
        with pytest.raises(ValueError, match="spreadsheet_id is required"):
            GSheetsConfig(sheet_name="Sheet1")


@pytest.mark.unit
class TestExtractDataSingleTable:
    def _pipeline(self):
        p = object.__new__(GoogleSheetsPipeline)
        p.source_config = GSheetsConfig(spreadsheet_id="abc", sheet_name="Sheet1")
        p.table_name = "google_sheets__book"
        p.client = MagicMock()
        p.client.get_spreadsheet_title.return_value = "Book"
        p.client.get_sheet_data.return_value = [{"a": 1}]
        p._should_skip_extraction = MagicMock(return_value=False)
        return p

    def test_extract_emits_one_table_named_table_name(self):
        p = self._pipeline()
        resources = p.extract_data()
        assert len(resources) == 1
        resource, _description = resources[0]
        # One config → one table, named exactly self.table_name (no sheet suffix).
        assert resource.name == "google_sheets__book"
        # Reads exactly the configured sheet, passing the (unset) range through.
        p.client.get_sheet_data.assert_called_once_with("abc", "Sheet1", None)


@pytest.mark.unit
class TestGetSheetDataRange:
    """No range → request the bare sheet name (full grid); an explicit range is
    appended with '!'. A fixed default like A:Z silently truncates wide sheets.
    """

    def _client(self):
        client = object.__new__(GSheetsClient)
        service = MagicMock()
        service.spreadsheets().values().get().execute.return_value = {"values": []}
        client.get_service = MagicMock(return_value=service)
        return client, service

    def test_no_range_reads_full_sheet(self):
        client, service = self._client()
        list(client.get_sheet_data("sid", "Sheet1"))
        _, kwargs = service.spreadsheets().values().get.call_args
        assert kwargs["range"] == "'Sheet1'"

    def test_explicit_range_appended(self):
        client, service = self._client()
        list(client.get_sheet_data("sid", "Sheet1", "A1:D10"))
        _, kwargs = service.spreadsheets().values().get.call_args
        assert kwargs["range"] == "'Sheet1'!A1:D10"
