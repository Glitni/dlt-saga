"""GCS requests and SFTP reads must carry a timeout.

gcsfs sends every request with no timeout unless given one, and paramiko reads
an open SFTP channel with none, so a connection that stops delivering bytes
hung the pipeline until the platform killed it.
"""

from unittest.mock import MagicMock, patch

import pytest

from dlt_saga.pipelines.filesystem.client import FilesystemClient
from dlt_saga.pipelines.filesystem.config import FilesystemConfig


def _kwargs(filesystem_type: str) -> dict:
    return FilesystemClient(
        FilesystemConfig(
            bucket_name="my-bucket",
            filesystem_type=filesystem_type,
            file_type="parquet",
            file_glob="*",
        )
    ).get_filesystem_kwargs()


@pytest.mark.unit
class TestGcsRequestTimeout:
    def test_gcs_bounds_connect_and_read_stall(self):
        aiohttp = pytest.importorskip("aiohttp")
        timeout = _kwargs("gs")["kwargs"]["requests_timeout"]

        assert isinstance(timeout, aiohttp.ClientTimeout)
        assert timeout.sock_connect and timeout.sock_read

    def test_gcs_does_not_cap_total_transfer_time(self):
        """A large file may take long to download; only a stall should fail it."""
        pytest.importorskip("aiohttp")
        assert _kwargs("gs")["kwargs"]["requests_timeout"].total is None

    @pytest.mark.parametrize("filesystem_type", ["s3", "azure", "file"])
    def test_other_protocols_unchanged(self, filesystem_type):
        assert "kwargs" not in _kwargs(filesystem_type)


def _sftp_client(**overrides) -> FilesystemClient:
    return FilesystemClient(
        FilesystemConfig(
            **{
                "filesystem_type": "sftp",
                "hostname": "sftp.example.com",
                "bucket_name": "data",
                "file_type": "csv",
                "file_glob": "*.csv",
                "username": "user",
                "password": "pass",
                **overrides,
            }
        )
    )


@pytest.fixture
def opened_sftp():
    """Patch the SFTP open; yields the mock opener and the filesystem it returns."""
    fs = MagicMock()
    with patch(
        "dlt_saga.pipelines.filesystem.sftp.KnownHostsSFTPFileSystem",
        return_value=fs,
    ) as opener:
        yield opener, fs


@pytest.mark.unit
class TestSftpReadTimeout:
    def test_dlt_reads_through_the_timed_out_filesystem(self, opened_sftp):
        _, fs = opened_sftp

        assert _sftp_client().get_filesystem_kwargs()["credentials"] is fs

    def test_channel_read_stall_is_bounded(self, opened_sftp):
        _, fs = opened_sftp

        _sftp_client().get_filesystem_kwargs()

        fs.ftp.get_channel.return_value.settimeout.assert_called_once()
        (timeout,) = fs.ftp.get_channel.return_value.settimeout.call_args.args
        assert timeout

    def test_connect_is_bounded(self, opened_sftp):
        opener, _ = opened_sftp

        _sftp_client().get_filesystem_kwargs()

        assert opener.call_args.kwargs["timeout"]
        assert opener.call_args.kwargs["username"] == "user"

    def test_configured_credentials_reach_the_connection(self, opened_sftp):
        opener, _ = opened_sftp

        _sftp_client(port=2222).get_filesystem_kwargs()

        assert opener.call_args.args == ("sftp.example.com",)
        assert opener.call_args.kwargs["port"] == 2222

    def test_filesystem_kept_out_of_the_fsspec_instance_cache(self, opened_sftp):
        """A cached instance is shared with anyone opening the same host in the
        process, and one broken by a stall would be reused by the next run."""
        opener, _ = opened_sftp

        _sftp_client().get_filesystem_kwargs()

        assert opener.call_args.kwargs["skip_instance_cache"] is True

    def test_one_connection_per_client(self, opened_sftp):
        """Header and data reads each build a resource; they share a connection."""
        opener, _ = opened_sftp
        client = _sftp_client()

        client.get_filesystem_kwargs()
        client.get_filesystem_kwargs()

        opener.assert_called_once()
