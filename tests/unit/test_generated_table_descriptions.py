"""Generated table descriptions name the source the table is loaded from."""

from types import SimpleNamespace

import pytest

from dlt_saga.pipelines.filesystem.pipeline import FilesystemPipeline
from dlt_saga.pipelines.google_sheets.pipeline import GoogleSheetsPipeline
from dlt_saga.pipelines.sharepoint.pipeline import SharePointPipeline


def _shell(cls, **attrs):
    pipeline = object.__new__(cls)
    for name, value in attrs.items():
        setattr(pipeline, name, value)
    return pipeline


@pytest.mark.unit
class TestFilesystemDescription:
    def test_bucket_and_glob(self):
        pipeline = _shell(
            FilesystemPipeline,
            client=SimpleNamespace(fs_url="gs://my-bucket"),
            source_config=SimpleNamespace(
                file_glob="exports/*.csv", file_id=None, file_type="csv"
            ),
        )
        assert (
            pipeline._table_description()
            == "Data from gs://my-bucket/exports/*.csv (csv)"
        )

    def test_gdrive_file_id_ignores_glob(self):
        pipeline = _shell(
            FilesystemPipeline,
            client=SimpleNamespace(fs_url="gdrive://abc123"),
            source_config=SimpleNamespace(
                file_glob=None, file_id="abc123", file_type="json"
            ),
        )
        assert pipeline._table_description() == "Data from gdrive://abc123 (json)"


@pytest.mark.unit
class TestGoogleSheetsDescription:
    def _pipeline(self, range_=None):
        return _shell(
            GoogleSheetsPipeline,
            source_config=SimpleNamespace(spreadsheet_id="sheet-id", range=range_),
        )

    def test_links_to_the_sheet_tab(self):
        description = self._pipeline()._table_description(
            spreadsheet_title="Budget", sheet_name="2026", sheets={0: "2025", 7: "2026"}
        )
        assert description == (
            'Data from Google Sheet "Budget", sheet "2026": '
            "https://docs.google.com/spreadsheets/d/sheet-id/edit#gid=7"
        )

    def test_range_only_when_set(self):
        description = self._pipeline(range_="A1:C10")._table_description(
            spreadsheet_title="Budget", sheet_name="2026", sheets={7: "2026"}
        )
        assert ", range A1:C10: https://" in description


@pytest.mark.unit
class TestSharePointDescription:
    def _pipeline(self, file_type="xlsx", sheet_name=None):
        return _shell(
            SharePointPipeline,
            source_config=SimpleNamespace(
                site_url="https://example.sharepoint.com/",
                file_path="/sites/Team/Shared Documents/report.xlsx",
                file_type=file_type,
                sheet_name=sheet_name,
            ),
        )

    def test_full_file_url(self):
        assert self._pipeline()._table_description() == (
            "Data from SharePoint "
            "https://example.sharepoint.com/sites/Team/Shared Documents/report.xlsx"
        )

    def test_sheet_for_xlsx(self):
        assert (
            self._pipeline(sheet_name="Data")
            ._table_description()
            .endswith(', sheet "Data"')
        )
