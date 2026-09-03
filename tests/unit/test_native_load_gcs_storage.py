"""Unit tests for the native_load GCS storage client.

Mocks google.cloud.storage so the tests run without real GCP credentials.
Covers URI parsing, glob filtering, and StorageObject construction.
"""

from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

import pytest


def _fake_blob(name: str, size: int = 100, generation: int = 1, updated=None):
    blob = MagicMock()
    blob.name = name
    blob.size = size
    blob.generation = generation
    blob.updated = updated or datetime(2026, 5, 5, tzinfo=timezone.utc)
    return blob


def _make_client_with_blobs(blobs):
    """Build a GcsStorageClient with google.cloud.storage.Client mocked."""
    bucket = MagicMock()
    bucket.list_blobs.return_value = iter(blobs)
    fake_client = MagicMock()
    fake_client.bucket.return_value = bucket

    fake_storage_module = MagicMock()
    fake_storage_module.Client.return_value = fake_client

    with patch.dict(
        "sys.modules",
        {"google": MagicMock(), "google.cloud": MagicMock()},
    ):
        with patch("google.cloud.storage", fake_storage_module, create=True):
            from dlt_saga.pipelines.native_load.storage.gcs import GcsStorageClient

            client = GcsStorageClient(billing_project="my-project")
    return client, bucket


@pytest.mark.unit
class TestGcsStorageClientUriHandling:
    def test_non_gcs_uri_raises(self):
        client, _ = _make_client_with_blobs([])
        with pytest.raises(ValueError, match="gs://"):
            list(client.list_files("s3://bucket/prefix/", "*.parquet"))

    def test_uri_without_prefix_lists_bucket_root(self):
        blobs = [_fake_blob("a.parquet"), _fake_blob("b.parquet")]
        client, bucket = _make_client_with_blobs(blobs)
        list(client.list_files("gs://my-bucket", "*.parquet"))
        bucket.list_blobs.assert_called_once_with(prefix="", delimiter="/")

    def test_uri_with_prefix_passes_prefix_to_list_blobs(self):
        blobs = [_fake_blob("data/2026/file.parquet")]
        client, bucket = _make_client_with_blobs(blobs)
        list(client.list_files("gs://my-bucket/data/2026/", "*.parquet"))
        bucket.list_blobs.assert_called_once_with(prefix="data/2026/", delimiter="/")

    def test_start_offset_passed_to_list_blobs(self):
        client, bucket = _make_client_with_blobs([])
        list(
            client.list_files(
                "gs://my-bucket/data/", "*.parquet", start_offset="data/cursor.parquet"
            )
        )
        bucket.list_blobs.assert_called_once_with(
            prefix="data/", start_offset="data/cursor.parquet", delimiter="/"
        )


@pytest.mark.unit
class TestGcsStorageClientPathScoping:
    """file_pattern is matched against the path relative to the listed URI."""

    def test_flat_pattern_excludes_subfolders(self):
        blobs = [
            _fake_blob("root/a.parquet"),
            _fake_blob("root/legacy/a.parquet"),
        ]
        client, _ = _make_client_with_blobs(blobs)
        results = list(client.list_files("gs://my-bucket/root/", "*.parquet"))
        assert [r.name for r in results] == ["root/a.parquet"]

    def test_recursive_pattern_includes_subfolders(self):
        blobs = [
            _fake_blob("root/a.parquet"),
            _fake_blob("root/legacy/b.parquet"),
            _fake_blob("root/legacy/deep/c.parquet"),
        ]
        client, _ = _make_client_with_blobs(blobs)
        results = list(client.list_files("gs://my-bucket/root/", "**/*.parquet"))
        assert [r.name for r in results] == [
            "root/a.parquet",
            "root/legacy/b.parquet",
            "root/legacy/deep/c.parquet",
        ]

    def test_pattern_with_folder_segment_matches_that_level_only(self):
        blobs = [
            _fake_blob("root/a.parquet"),
            _fake_blob("root/legacy/b.parquet"),
            _fake_blob("root/legacy/deep/c.parquet"),
        ]
        client, _ = _make_client_with_blobs(blobs)
        results = list(client.list_files("gs://my-bucket/root/", "legacy/*.parquet"))
        assert [r.name for r in results] == ["root/legacy/b.parquet"]

    def test_recursive_pattern_does_not_prune_listing(self):
        client, bucket = _make_client_with_blobs([])
        list(client.list_files("gs://my-bucket/root/", "**/*.parquet"))
        bucket.list_blobs.assert_called_once_with(prefix="root/")

    def test_multi_segment_pattern_does_not_prune_listing(self):
        client, bucket = _make_client_with_blobs([])
        list(client.list_files("gs://my-bucket/root/", "legacy/*.parquet"))
        bucket.list_blobs.assert_called_once_with(prefix="root/")

    def test_partial_prefix_does_not_prune_listing(self):
        # "data" selects objects by string prefix, so one level has no meaning
        client, bucket = _make_client_with_blobs([])
        list(client.list_files("gs://my-bucket/data", "*.parquet"))
        bucket.list_blobs.assert_called_once_with(prefix="data")


@pytest.mark.unit
class TestGcsStorageClientGlobFiltering:
    def test_single_pattern_filters_basename(self):
        blobs = [
            _fake_blob("data/a.parquet"),
            _fake_blob("data/b.csv"),
            _fake_blob("data/c.parquet"),
        ]
        client, _ = _make_client_with_blobs(blobs)
        results = list(client.list_files("gs://my-bucket/data/", "*.parquet"))
        names = [r.name for r in results]
        assert names == ["data/a.parquet", "data/c.parquet"]

    def test_pattern_list_matches_any(self):
        blobs = [
            _fake_blob("data/a.parquet"),
            _fake_blob("data/b.csv"),
            _fake_blob("data/c.json"),
        ]
        client, _ = _make_client_with_blobs(blobs)
        results = list(
            client.list_files("gs://my-bucket/data/", ["*.parquet", "*.csv"])
        )
        names = [r.name for r in results]
        assert names == ["data/a.parquet", "data/b.csv"]

    def test_directory_marker_blob_skipped(self):
        # GCS returns an empty-basename blob for prefix markers (e.g. "data/")
        blobs = [
            _fake_blob("data/"),
            _fake_blob("data/a.parquet"),
        ]
        client, _ = _make_client_with_blobs(blobs)
        results = list(client.list_files("gs://my-bucket/data/", "*.parquet"))
        assert [r.name for r in results] == ["data/a.parquet"]

    def test_storage_object_fields_populated(self):
        ts = datetime(2026, 5, 5, 10, 0, 0, tzinfo=timezone.utc)
        blobs = [_fake_blob("data/a.parquet", size=2048, generation=42, updated=ts)]
        client, _ = _make_client_with_blobs(blobs)
        results = list(client.list_files("gs://my-bucket/data/", "*.parquet"))
        assert len(results) == 1
        obj = results[0]
        assert obj.name == "data/a.parquet"
        assert obj.full_uri == "gs://my-bucket/data/a.parquet"
        assert obj.size == 2048
        assert obj.generation == 42
        assert obj.updated == ts
