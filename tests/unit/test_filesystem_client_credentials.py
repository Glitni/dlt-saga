"""FilesystemClient must not mutate process-global credential env vars.

Popping GOOGLE_APPLICATION_CREDENTIALS in __init__ (the old behaviour) stripped
it for every concurrently-running pipeline under the shared-process
ThreadPoolExecutor. GCS credential selection is left to google.auth / ADC
(impersonation-aware via the auth provider), same as the BigQuery destination.
"""

import os

import pytest

from dlt_saga.pipelines.filesystem.client import FilesystemClient
from dlt_saga.pipelines.filesystem.config import FilesystemConfig


@pytest.mark.unit
class TestNoGlobalEnvMutation:
    def _gcs_config(self):
        return FilesystemConfig(
            bucket_name="my-bucket",
            filesystem_type="gs",
            file_type="csv",
            file_glob="*",
        )

    def test_gcs_client_preserves_gac_env(self, monkeypatch):
        monkeypatch.setenv("GOOGLE_APPLICATION_CREDENTIALS", "/tmp/key.json")
        FilesystemClient(self._gcs_config())
        assert os.environ.get("GOOGLE_APPLICATION_CREDENTIALS") == "/tmp/key.json"

    def test_gcs_client_does_not_add_gac_env(self, monkeypatch):
        monkeypatch.delenv("GOOGLE_APPLICATION_CREDENTIALS", raising=False)
        FilesystemClient(self._gcs_config())
        assert "GOOGLE_APPLICATION_CREDENTIALS" not in os.environ
