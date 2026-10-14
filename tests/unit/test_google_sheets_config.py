"""Unit tests for Google Sheets config + one-config-one-table extraction.

A google_sheets config maps to exactly one sheet/tab (one destination table);
`sheet_name` or `sheet_id` is required, and extraction never fans out to
multiple tables.
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

    def test_sheet_name_or_sheet_id_required(self):
        with pytest.raises(ValueError, match="sheet_name or sheet_id is required"):
            GSheetsConfig(spreadsheet_id="abc")

    def test_sheet_id_zero_is_valid(self):
        # gid 0 is the first tab: must not be treated as "unset"
        assert GSheetsConfig(spreadsheet_id="abc", sheet_id=0).sheet_id == 0

    def test_sheet_name_and_sheet_id_are_exclusive(self):
        with pytest.raises(ValueError, match="not both"):
            GSheetsConfig(spreadsheet_id="abc", sheet_name="Sheet1", sheet_id=0)

    @pytest.mark.parametrize("sheet_id", [-1, "0", True, 1.5])
    def test_sheet_id_must_be_non_negative_int(self, sheet_id):
        with pytest.raises(ValueError, match="non-negative integer"):
            GSheetsConfig(spreadsheet_id="abc", sheet_id=sheet_id)

    def test_range_defaults_to_full_sheet(self):
        # Default range is unset so the whole sheet is read (no A:Z truncation).
        assert GSheetsConfig(spreadsheet_id="abc", sheet_name="s").range is None

    def test_spreadsheet_id_required(self):
        with pytest.raises(ValueError, match="spreadsheet_id is required"):
            GSheetsConfig(sheet_name="Sheet1")


@pytest.mark.unit
class TestExtractDataSingleTable:
    def _pipeline(self, **sheet):
        p = object.__new__(GoogleSheetsPipeline)
        p.source_config = GSheetsConfig(
            spreadsheet_id="abc", **(sheet or {"sheet_name": "Sheet1"})
        )
        p.table_name = "google_sheets__book"
        p.logger = MagicMock()
        p.client = MagicMock()
        p.client.get_spreadsheet_info.return_value = (
            "Book",
            {0: "Sheet1", 1234: "Renamed tab"},
        )
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

    def test_sheet_id_reads_the_tab_by_its_current_name(self):
        p = self._pipeline(sheet_id=1234)
        _resource, description = p.extract_data()[0]
        p.client.get_sheet_data.assert_called_once_with("abc", "Renamed tab", None)
        assert "Sheet: Renamed tab" in description

    def test_unknown_sheet_id_lists_available_tabs(self):
        p = self._pipeline(sheet_id=99)
        with pytest.raises(
            ValueError, match=r"Available sheet_ids: 0 \('Sheet1'\), 1234"
        ):
            p.extract_data()
        p.client.get_sheet_data.assert_not_called()


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

    def test_quote_in_sheet_name_is_escaped(self):
        client, service = self._client()
        list(client.get_sheet_data("sid", "Kim's tab", "A:B"))
        _, kwargs = service.spreadsheets().values().get.call_args
        assert kwargs["range"] == "'Kim''s tab'!A:B"

    def test_explicit_range_appended(self):
        client, service = self._client()
        list(client.get_sheet_data("sid", "Sheet1", "A1:D10"))
        _, kwargs = service.spreadsheets().values().get.call_args
        assert kwargs["range"] == "'Sheet1'!A1:D10"


@pytest.mark.unit
class TestGetSpreadsheetInfo:
    def test_returns_title_and_tabs_from_one_metadata_call(self):
        client = object.__new__(GSheetsClient)
        service = MagicMock()
        service.spreadsheets().get().execute.return_value = {
            "properties": {"title": "Book"},
            "sheets": [
                {"properties": {"sheetId": 0, "title": "Sheet1"}},
                {"properties": {"sheetId": 1234, "title": "Data"}},
            ],
        }
        client.get_service = MagicMock(return_value=service)

        assert client.get_spreadsheet_info("sid") == (
            "Book",
            {0: "Sheet1", 1234: "Data"},
        )
        _, kwargs = service.spreadsheets().get.call_args
        assert kwargs["fields"] == "properties.title,sheets.properties(sheetId,title)"
