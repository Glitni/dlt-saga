"""Unit tests for dlt_saga.report.uploader."""

from unittest.mock import MagicMock, patch

import pytest

from dlt_saga.report.uploader import (
    REMOTE_SCHEMES,
    _upload_to_gcs,
    is_remote_uri,
    upload_report,
)


@pytest.mark.unit
class TestIsRemoteUri:
    @pytest.mark.parametrize(
        "uri",
        [
            "gs://bucket/path",
            "s3://bucket/path",
            "az://container/path",
            "slack://channel",
        ],
    )
    def test_recognised_schemes_are_remote(self, uri):
        assert is_remote_uri(uri) is True

    @pytest.mark.parametrize(
        "path", ["report.html", "/tmp/report.html", "reports/weekly.html", "gcs://typo"]
    )
    def test_local_paths_are_not_remote(self, path):
        assert is_remote_uri(path) is False

    def test_all_remote_schemes_covered(self):
        """Every scheme in REMOTE_SCHEMES is recognised by is_remote_uri."""
        for scheme in REMOTE_SCHEMES:
            assert is_remote_uri(f"{scheme}bucket/path") is True


@pytest.mark.unit
class TestUploadReportDispatch:
    def test_gs_uri_dispatches_to_gcs(self):
        with patch(
            "dlt_saga.report.uploader._upload_to_gcs", return_value="gs://b/f.html"
        ) as mock:
            result = upload_report("/tmp/r.html", "gs://b/f.html")
        mock.assert_called_once_with("/tmp/r.html", "b", "f.html")
        assert result == "gs://b/f.html"

    def test_gs_uri_without_object_defaults_to_saga_report(self):
        with patch(
            "dlt_saga.report.uploader._upload_to_gcs",
            return_value="gs://b/saga_report.html",
        ) as mock:
            upload_report("/tmp/r.html", "gs://bucket")
        mock.assert_called_once_with("/tmp/r.html", "bucket", "saga_report.html")

    def test_gs_uri_nested_path(self):
        with patch("dlt_saga.report.uploader._upload_to_gcs") as mock:
            upload_report("/tmp/r.html", "gs://my-bucket/reports/2024/saga.html")
        mock.assert_called_once_with(
            "/tmp/r.html", "my-bucket", "reports/2024/saga.html"
        )

    def test_s3_raises_not_implemented(self):
        with pytest.raises(NotImplementedError, match="S3"):
            upload_report("/tmp/r.html", "s3://bucket/path.html")

    def test_az_raises_not_implemented(self):
        with pytest.raises(NotImplementedError, match="Azure"):
            upload_report("/tmp/r.html", "az://container/path.html")

    def test_slack_raises_not_implemented(self):
        with pytest.raises(NotImplementedError, match="Slack"):
            upload_report("/tmp/r.html", "slack://my-channel")

    def test_unknown_scheme_raises_value_error(self):
        with pytest.raises(ValueError, match="Unrecognised output URI scheme"):
            upload_report("/tmp/r.html", "ftp://host/path.html")

    def test_local_path_raises_value_error(self):
        with pytest.raises(ValueError, match="Unrecognised output URI scheme"):
            upload_report("/tmp/r.html", "report.html")


@pytest.mark.unit
class TestUploadToGcs:
    def test_uploads_file_and_returns_uri(self, tmp_path):
        report_file = tmp_path / "report.html"
        report_file.write_text("<html/>")

        mock_client = MagicMock()
        mock_blob = MagicMock()
        mock_client.bucket.return_value.blob.return_value = mock_blob

        with patch("google.cloud.storage.Client", return_value=mock_client):
            result = _upload_to_gcs(str(report_file), "my-bucket", "reports/r.html")

        mock_client.bucket.assert_called_once_with("my-bucket")
        mock_client.bucket.return_value.blob.assert_called_once_with("reports/r.html")
        assert mock_blob.content_type == "text/html; charset=utf-8"
        mock_blob.upload_from_filename.assert_called_once_with(str(report_file))
        assert result == "gs://my-bucket/reports/r.html"
