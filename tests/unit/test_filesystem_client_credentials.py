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


@pytest.mark.unit
class TestCredentialSecretUris:
    """Secret URIs in ``credentials`` are resolved by value for every
    filesystem type, whole or inside an object; plain values pass through."""

    def _kwargs(self, filesystem_type, credentials):
        return FilesystemClient(
            FilesystemConfig(
                bucket_name="my-bucket",
                filesystem_type=filesystem_type,
                file_type="csv",
                file_glob="*",
                credentials=credentials,
            )
        ).get_filesystem_kwargs()

    def test_resolves_secret_uri_string(self, monkeypatch):
        monkeypatch.setenv("SAGA_TEST_GCS_SA", '{"type": "service_account"}')
        kwargs = self._kwargs("gs", "env_secret::SAGA_TEST_GCS_SA")
        assert kwargs["credentials"] == '{"type": "service_account"}'

    def test_resolves_secret_uris_inside_object(self, monkeypatch):
        monkeypatch.setenv("SAGA_TEST_S3_SECRET", "s3cr3t")
        kwargs = self._kwargs(
            "s3",
            {
                "aws_access_key_id": "AKIA",
                "aws_secret_access_key": "env_secret::SAGA_TEST_S3_SECRET",
            },
        )
        assert kwargs["credentials"] == {
            "aws_access_key_id": "AKIA",
            "aws_secret_access_key": "s3cr3t",
        }

    def test_plain_path_passes_through(self):
        kwargs = self._kwargs("gs", "/keys/sa.json")
        assert kwargs["credentials"] == "/keys/sa.json"
