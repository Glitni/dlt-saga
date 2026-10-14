"""Unit tests for the filesystem adapter's Google Drive (gdrive) support.

The end-to-end tests drive dlt's real ``GoogleDriveFileSystem`` against a fake
Drive v3 service, so URL building, globbing, modification dates and file reads
all go through the same code path as a live run.
"""

from datetime import datetime, timezone
from unittest.mock import patch

import pytest
from google.auth.credentials import AnonymousCredentials

from dlt_saga.pipelines.filesystem.client import _GDRIVE_SCOPES, FilesystemClient
from dlt_saga.pipelines.filesystem.config import FilesystemConfig

FOLDER_ID = "folder123"


def _gdrive_config(**overrides):
    values = dict(
        filesystem_type="gdrive",
        bucket_name=FOLDER_ID,
        file_type="csv",
        file_glob="*.csv",
    )
    values.update(overrides)
    return FilesystemConfig(**values)


class _Request:
    def __init__(self, result):
        self._result = result
        self.headers = {}

    def execute(self):
        if callable(self._result):
            return self._result(self.headers)
        return self._result


class _FakeFiles:
    """Minimal ``service.files()`` for a flat folder of binary files."""

    def __init__(self, files):
        # files: {file_id: (name, mime_type, modified_time, content)}
        self._files = files

    def list(self, q, **_):
        assert q.strip() == f"'{FOLDER_ID}' in parents"
        return _Request(
            {
                "files": [
                    {
                        "id": file_id,
                        "name": name,
                        "mimeType": mime,
                        "modifiedTime": modified,
                        "size": str(len(content)),
                    }
                    for file_id, (name, mime, modified, content) in self._files.items()
                ]
            }
        )

    def get(self, fileId, **_):
        if fileId == FOLDER_ID:
            return _Request(
                {
                    "id": FOLDER_ID,
                    "name": FOLDER_ID,
                    "mimeType": "application/vnd.google-apps.folder",
                }
            )
        name, mime, modified, content = self._files[fileId]
        return _Request(
            {
                "id": fileId,
                "name": name,
                "mimeType": mime,
                "modifiedTime": modified,
                "size": str(len(content)),
            }
        )

    def get_media(self, fileId):
        content = self._files[fileId][3]

        def _read(headers):
            if "Range" not in headers:
                return content
            start, end = headers["Range"].removeprefix("bytes=").split("-")
            return content[int(start) : int(end) + 1]

        return _Request(_read)


class _FakeService:
    def __init__(self, files):
        self._files = _FakeFiles(files)

    def files(self):
        return self._files


class _FakeMediaIoBaseDownload:
    """Stand-in for googleapiclient's chunked downloader: one chunk, done."""

    downloads = 0

    def __init__(self, buffer, request):
        self._buffer = buffer
        self._request = request

    def next_chunk(self):
        type(self).downloads += 1
        self._buffer.write(self._request.execute())
        return None, True


@pytest.fixture
def fake_drive():
    """Patch Drive discovery + credential resolution; yield the pool mock."""
    from dlt.common.storages.fsspecs import google_drive

    files = {
        "id-orders": (
            "orders.csv",
            "text/csv",
            "2026-10-01T08:00:00.000Z",
            b"order_id\tamount\n1\t10\n2\t20\n",
        ),
        "id-sheet": (
            "budget",
            "application/vnd.google-apps.spreadsheet",
            "2026-10-02T08:00:00.000Z",
            b"",
        ),
    }
    service = _FakeService(files)
    google_drive.FILE_ID_CACHE.clear()
    _FakeMediaIoBaseDownload.downloads = 0
    with (
        patch.object(google_drive, "build", return_value=service),
        patch(
            "dlt_saga.utility.gcp.client_pool.google_api_pool.get_client",
            return_value=service,
        ),
        patch("googleapiclient.http.MediaIoBaseDownload", _FakeMediaIoBaseDownload),
        patch(
            "dlt_saga.utility.gcp.client_pool.google_api_pool.get_credentials",
            return_value=AnonymousCredentials(),
        ) as get_credentials,
    ):
        yield get_credentials
    google_drive.FILE_ID_CACHE.clear()


@pytest.mark.unit
class TestGdriveConfig:
    def test_gdrive_is_accepted(self):
        assert _gdrive_config().filesystem_type == "gdrive"

    def test_file_id_replaces_bucket_name_and_file_glob(self):
        config = _gdrive_config(bucket_name="", file_glob=None, file_id="id-orders")
        assert config.file_id == "id-orders"

    def test_file_id_rejected_alongside_folder_selection(self):
        with pytest.raises(ValueError, match="file_id replaces bucket_name"):
            _gdrive_config(file_id="id-orders")

    def test_file_id_rejected_for_other_filesystems(self):
        with pytest.raises(ValueError, match="file_id is only supported"):
            FilesystemConfig(filesystem_type="gs", file_type="csv", file_id="id-orders")

    def test_folder_selection_still_required_without_file_id(self):
        with pytest.raises(ValueError, match="bucket_name is required"):
            _gdrive_config(bucket_name="")


@pytest.mark.unit
class TestGdriveClient:
    def test_builds_gdrive_url_from_folder_id(self):
        assert FilesystemClient(_gdrive_config()).fs_url == f"gdrive://{FOLDER_ID}"

    @pytest.mark.parametrize(
        "credentials, expected",
        [
            # no credentials: Application Default Credentials
            (None, {}),
            ("/keys/sa.json", {"credentials_path": "/keys/sa.json"}),
            (
                {"type": "service_account"},
                {"credentials_dict": {"type": "service_account"}},
            ),
            # plain JSON string: service account content, no secret involved
            (
                '{"type": "service_account"}',
                {"credentials_dict": {"type": "service_account"}},
            ),
            # secret URI: the value decides it is a secret, not the field
            (
                "env_secret::SAGA_TEST_GDRIVE_SA",
                {"credentials_dict": {"type": "service_account", "k": "v"}},
            ),
            (
                {
                    "type": "service_account",
                    "private_key": "env_secret::SAGA_TEST_GDRIVE_KEY",
                },
                {"credentials_dict": {"type": "service_account", "private_key": "pem"}},
            ),
        ],
    )
    def test_resolves_credentials_read_only(
        self, fake_drive, monkeypatch, credentials, expected
    ):
        monkeypatch.setenv(
            "SAGA_TEST_GDRIVE_SA", '{"type": "service_account", "k": "v"}'
        )
        monkeypatch.setenv("SAGA_TEST_GDRIVE_KEY", "pem")
        kwargs = FilesystemClient(
            _gdrive_config(credentials=credentials)
        ).get_filesystem_kwargs()

        call = dict(fake_drive.call_args.kwargs)
        assert call.pop("scopes") == _GDRIVE_SCOPES
        assert call == expected
        assert kwargs["kwargs"] == {"access": "read_only"}
        assert kwargs["credentials"].to_native_credentials() is fake_drive.return_value

    def test_accepts_service_account_credentials(self, fake_drive):
        from cryptography.hazmat.primitives import serialization
        from cryptography.hazmat.primitives.asymmetric import rsa
        from google.oauth2.service_account import Credentials

        pem = (
            rsa.generate_private_key(public_exponent=65537, key_size=2048)
            .private_bytes(
                serialization.Encoding.PEM,
                serialization.PrivateFormat.PKCS8,
                serialization.NoEncryption(),
            )
            .decode()
        )
        native = Credentials.from_service_account_info(
            {
                "type": "service_account",
                "project_id": "p",
                "private_key": pem,
                "client_email": "reader@p.iam.gserviceaccount.com",
                "token_uri": "https://oauth2.googleapis.com/token",
            },
            scopes=_GDRIVE_SCOPES,
        )
        fake_drive.return_value = native

        creds = FilesystemClient(_gdrive_config()).get_filesystem_kwargs()[
            "credentials"
        ]
        assert creds.to_native_credentials() is native

    def test_credentials_resolved_once_per_client(self, fake_drive):
        client = FilesystemClient(_gdrive_config())
        client.get_filesystem_kwargs()
        client.get_filesystem_kwargs()
        assert fake_drive.call_count == 1

    def test_lists_matching_files_with_modification_date(self, fake_drive):
        files = list(FilesystemClient(_gdrive_config()).get_files())

        assert [f["file_name"] for f in files] == ["orders.csv"]
        assert files[0]["modification_date"] == datetime(
            2026, 10, 1, 8, tzinfo=timezone.utc
        )

    def test_change_detection_uses_drive_modified_time(self, fake_drive):
        client = FilesystemClient(_gdrive_config())
        assert client.has_files_modified_since(
            datetime(2026, 9, 30, tzinfo=timezone.utc)
        )
        assert not client.has_files_modified_since(
            datetime(2026, 10, 1, 9, tzinfo=timezone.utc)
        )

    def test_reads_rows_with_file_metadata(self, fake_drive):
        client = FilesystemClient(_gdrive_config(csv_separator="\t"))
        rows = list(client.read_with_metadata())

        assert [(r["order_id"], r["amount"]) for r in rows] == [
            ("1", "10"),
            ("2", "20"),
        ]
        assert {r["_dlt_source_file_name"] for r in rows} == {"orders.csv"}


def _file_id_config(file_id="id-orders", **overrides):
    return _gdrive_config(bucket_name="", file_glob=None, file_id=file_id, **overrides)


@pytest.mark.unit
class TestGdriveFileId:
    def test_lists_the_file_without_downloading_it(self, fake_drive):
        files = list(FilesystemClient(_file_id_config()).get_files())

        assert [f["file_name"] for f in files] == ["orders.csv"]
        assert files[0]["modification_date"] == datetime(
            2026, 10, 1, 8, tzinfo=timezone.utc
        )
        assert _FakeMediaIoBaseDownload.downloads == 0

    def test_change_detection_uses_drive_modified_time(self, fake_drive):
        client = FilesystemClient(_file_id_config())
        assert client.has_files_modified_since(
            datetime(2026, 9, 30, tzinfo=timezone.utc)
        )
        assert not client.has_files_modified_since(
            datetime(2026, 10, 1, 9, tzinfo=timezone.utc)
        )
        assert _FakeMediaIoBaseDownload.downloads == 0

    def test_reads_rows_with_file_metadata(self, fake_drive):
        client = FilesystemClient(_file_id_config(csv_separator="\t"))
        rows = list(client.read_with_metadata())

        assert [(r["order_id"], r["amount"]) for r in rows] == [
            ("1", "10"),
            ("2", "20"),
        ]
        assert {r["_dlt_source_file_name"] for r in rows} == {"orders.csv"}
        assert _FakeMediaIoBaseDownload.downloads == 1

    def test_header_read_and_data_read_share_one_download(self, fake_drive):
        # column defaults / pandas dtype hints read the CSV header first; the
        # data read must not download the file a second time
        client = FilesystemClient(_file_id_config(csv_separator="	"))
        assert client._read_csv_headers() == ["order_id", "amount"]
        rows = list(client.read_with_metadata())

        assert len(rows) == 2
        assert _FakeMediaIoBaseDownload.downloads == 1

    def test_reads_rows_with_duckdb_csv_reader(self, fake_drive):
        # replace-mode path: no metadata injection, DuckDB CSV reader
        client = FilesystemClient(_file_id_config(csv_separator="\t"))
        rows = list(client.read_csv())

        assert [(r["order_id"], r["amount"]) for r in rows] == [(1, 10), (2, 20)]

    def test_rejects_native_google_files(self, fake_drive):
        client = FilesystemClient(_file_id_config(file_id="id-sheet"))
        with pytest.raises(Exception, match="google_sheets adapter"):
            list(client.get_files())

    @pytest.mark.parametrize(
        "initial_value, expected_rows", [("2026-09-30", 2), ("2026-10-02", 0)]
    )
    def test_incremental_filters_on_modification_date(
        self, fake_drive, initial_value, expected_rows
    ):
        client = FilesystemClient(_file_id_config(csv_separator="\t"))
        rows = list(client.read_incremental_with_metadata(initial_value=initial_value))

        assert len(rows) == expected_rows
