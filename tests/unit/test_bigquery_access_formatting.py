"""Tests for the human-readable access-key formatter used by
``saga update-access`` log output."""

import pytest

from dlt_saga.destinations.bigquery.base import _format_access_key


@pytest.mark.unit
class TestFormatAccessKey:
    """``_format_access_key`` renders ``_stable_entry_key`` tuples in the
    same ``ROLE:entity_type:entity_id`` shape that ``dataset_access:`` uses
    in YAML, so a log line ``+ READER:userByEmail:foo@example.com`` is
    something an operator can paste straight back into config to reproduce."""

    def test_role_based_user(self):
        key = ("READER", "userByEmail", "alice@example.com")
        assert _format_access_key(key) == "READER:userByEmail:alice@example.com"

    def test_role_based_group(self):
        key = ("WRITER", "groupByEmail", "data-team@example.com")
        assert _format_access_key(key) == "WRITER:groupByEmail:data-team@example.com"

    def test_role_based_service_account(self):
        key = ("OWNER", "userByEmail", "sa@project.iam.gserviceaccount.com")
        assert (
            _format_access_key(key)
            == "OWNER:userByEmail:sa@project.iam.gserviceaccount.com"
        )

    def test_authorized_dataset_drops_role_segment(self):
        """Authorized dataset entries carry ``role=None`` — render without
        a leading ``None:`` segment."""
        key = (None, "AUTHORIZED_DATASET", "project-id.dataset-id")
        assert _format_access_key(key) == "AUTHORIZED_DATASET:project-id.dataset-id"

    def test_authorized_view_drops_role_segment(self):
        key = (None, "AUTHORIZED_VIEW", "project-id.dataset-id.view-id")
        assert (
            _format_access_key(key) == "AUTHORIZED_VIEW:project-id.dataset-id.view-id"
        )
