"""Unit tests for the native_load S3 storage client.

Mocks boto3 so the tests run without AWS credentials or the boto3 package.
Covers URI parsing, glob filtering, pagination, and StorageObject construction.
"""

from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

import pytest


def _obj(key: str, size: int = 100, last_modified=None) -> dict:
    return {
        "Key": key,
        "Size": size,
        "LastModified": last_modified or datetime(2026, 5, 5, tzinfo=timezone.utc),
    }


def _make_client_with_pages(pages, region="eu-west-1"):
    """Build an S3StorageClient with boto3 mocked to yield the given pages."""
    fake_s3 = MagicMock()
    paginator = MagicMock()
    paginator.paginate.return_value = pages
    fake_s3.get_paginator.return_value = paginator

    fake_boto3 = MagicMock()
    fake_boto3.client.return_value = fake_s3

    with patch.dict("sys.modules", {"boto3": fake_boto3}):
        from dlt_saga.pipelines.native_load.storage.s3 import S3StorageClient

        client = S3StorageClient(
            aws_access_key_id="AKIA_TEST",
            aws_secret_access_key="secret",
            region_name=region,
        )
    return client, fake_s3, fake_boto3


@pytest.mark.unit
class TestS3StorageClientUriHandling:
    def test_non_s3_uri_raises(self):
        client, _, _ = _make_client_with_pages([])
        with pytest.raises(ValueError, match="s3://"):
            list(client.list_files("gs://bucket/prefix/", "*.parquet"))

    def test_uri_without_prefix_lists_bucket_root(self):
        client, s3, _ = _make_client_with_pages([{"Contents": [_obj("a.parquet")]}])
        list(client.list_files("s3://my-bucket", "*.parquet"))
        s3.get_paginator.return_value.paginate.assert_called_once_with(
            Bucket="my-bucket", Prefix=""
        )

    def test_uri_with_prefix_passed_to_paginate(self):
        client, s3, _ = _make_client_with_pages(
            [{"Contents": [_obj("data/2026/file.parquet")]}]
        )
        list(client.list_files("s3://my-bucket/data/2026/", "*.parquet"))
        s3.get_paginator.return_value.paginate.assert_called_once_with(
            Bucket="my-bucket", Prefix="data/2026/"
        )

    def test_start_offset_is_ignored(self):
        # S3 has no lexicographic start; start_offset must not reach paginate.
        client, s3, _ = _make_client_with_pages([{"Contents": []}])
        list(
            client.list_files(
                "s3://my-bucket/data/", "*.parquet", start_offset="data/x.parquet"
            )
        )
        _, kwargs = s3.get_paginator.return_value.paginate.call_args
        assert "start_offset" not in kwargs
        assert kwargs == {"Bucket": "my-bucket", "Prefix": "data/"}

    def test_credentials_and_region_passed_to_boto3(self):
        _, _, boto3 = _make_client_with_pages([], region="eu-west-1")
        boto3.client.assert_called_once_with(
            "s3",
            aws_access_key_id="AKIA_TEST",
            aws_secret_access_key="secret",
            region_name="eu-west-1",
        )


@pytest.mark.unit
class TestS3StorageClientGlobFiltering:
    def test_single_pattern_filters_basename(self):
        pages = [
            {
                "Contents": [
                    _obj("data/a.parquet"),
                    _obj("data/b.csv"),
                    _obj("data/c.parquet"),
                ]
            }
        ]
        client, _, _ = _make_client_with_pages(pages)
        results = list(client.list_files("s3://my-bucket/data/", "*.parquet"))
        assert [r.name for r in results] == ["data/a.parquet", "data/c.parquet"]

    def test_pattern_list_matches_any(self):
        pages = [
            {
                "Contents": [
                    _obj("data/a.parquet"),
                    _obj("data/b.csv"),
                    _obj("data/c.json"),
                ]
            }
        ]
        client, _, _ = _make_client_with_pages(pages)
        results = list(
            client.list_files("s3://my-bucket/data/", ["*.parquet", "*.csv"])
        )
        assert [r.name for r in results] == ["data/a.parquet", "data/b.csv"]

    def test_directory_marker_key_skipped(self):
        pages = [{"Contents": [_obj("data/"), _obj("data/a.parquet")]}]
        client, _, _ = _make_client_with_pages(pages)
        results = list(client.list_files("s3://my-bucket/data/", "*.parquet"))
        assert [r.name for r in results] == ["data/a.parquet"]

    def test_pagination_across_pages(self):
        pages = [
            {"Contents": [_obj("data/a.parquet")]},
            {"Contents": [_obj("data/b.parquet")]},
        ]
        client, _, _ = _make_client_with_pages(pages)
        results = list(client.list_files("s3://my-bucket/data/", "*.parquet"))
        assert [r.name for r in results] == ["data/a.parquet", "data/b.parquet"]

    def test_page_without_contents_is_safe(self):
        # A prefix that matches nothing yields a page with no "Contents" key.
        pages = [{}, {"Contents": [_obj("data/a.parquet")]}]
        client, _, _ = _make_client_with_pages(pages)
        results = list(client.list_files("s3://my-bucket/data/", "*.parquet"))
        assert [r.name for r in results] == ["data/a.parquet"]


@pytest.mark.unit
class TestS3StorageObjectFields:
    def test_fields_populated_with_generation_from_last_modified(self):
        ts = datetime(2026, 5, 5, 10, 0, 0, tzinfo=timezone.utc)
        pages = [{"Contents": [_obj("data/a.parquet", size=2048, last_modified=ts)]}]
        client, _, _ = _make_client_with_pages(pages)
        results = list(client.list_files("s3://my-bucket/data/", "*.parquet"))
        assert len(results) == 1
        obj = results[0]
        assert obj.name == "data/a.parquet"
        assert obj.full_uri == "s3://my-bucket/data/a.parquet"
        assert obj.size == 2048
        assert obj.generation == int(ts.timestamp() * 1000)
        assert obj.updated == ts

    def test_generation_changes_when_object_reuploaded(self):
        older = datetime(2026, 5, 5, 10, 0, 0, tzinfo=timezone.utc)
        newer = datetime(2026, 5, 6, 10, 0, 0, tzinfo=timezone.utc)
        c1, _, _ = _make_client_with_pages(
            [{"Contents": [_obj("data/a.parquet", last_modified=older)]}]
        )
        c2, _, _ = _make_client_with_pages(
            [{"Contents": [_obj("data/a.parquet", last_modified=newer)]}]
        )
        g1 = list(c1.list_files("s3://b/data/", "*.parquet"))[0].generation
        g2 = list(c2.list_files("s3://b/data/", "*.parquet"))[0].generation
        assert g1 != g2
