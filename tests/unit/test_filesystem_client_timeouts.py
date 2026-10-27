"""GCS requests must carry a timeout.

gcsfs sends every request with no timeout unless given one, so a connection
that stops delivering bytes hung the pipeline until the platform killed it.
"""

import pytest

from dlt_saga.pipelines.filesystem.client import FilesystemClient
from dlt_saga.pipelines.filesystem.config import FilesystemConfig

aiohttp = pytest.importorskip("aiohttp")


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
        timeout = _kwargs("gs")["kwargs"]["requests_timeout"]

        assert isinstance(timeout, aiohttp.ClientTimeout)
        assert timeout.sock_connect and timeout.sock_read

    def test_gcs_does_not_cap_total_transfer_time(self):
        """A large file may take long to download; only a stall should fail it."""
        assert _kwargs("gs")["kwargs"]["requests_timeout"].total is None

    @pytest.mark.parametrize("filesystem_type", ["s3", "azure", "file"])
    def test_other_protocols_unchanged(self, filesystem_type):
        assert "kwargs" not in _kwargs(filesystem_type)
