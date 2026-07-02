"""Unit tests for Destination.reconcile_descriptions diff logic.

Uses a stub self (the method is destination-agnostic given the primitives), so
these run without any warehouse and pin the idempotency / normalization rules.
"""

from unittest.mock import MagicMock

import pytest

from dlt_saga.destinations.base import Destination


def _fake(current_columns, current_table_description=""):
    fake = MagicMock()
    fake.supports_description_reconcile.return_value = True
    fake.get_column_descriptions.return_value = current_columns
    fake.get_table_description.return_value = current_table_description
    return fake


def _reconcile(fake, **kwargs):
    Destination.reconcile_descriptions(fake, "ds", "t", **kwargs)


@pytest.mark.unit
class TestReconcileColumnDescriptions:
    def test_no_writes_when_unchanged(self):
        fake = _fake({"email": "Contact"}, "Table desc")
        _reconcile(
            fake, descriptions={"email": "Contact"}, table_description="Table desc"
        )
        fake.set_column_descriptions.assert_not_called()
        fake.set_table_description.assert_not_called()

    def test_only_changed_columns_written(self):
        fake = _fake({"a": "old a", "b": "keep b"})
        _reconcile(fake, descriptions={"a": "new a", "b": "keep b"})
        fake.set_column_descriptions.assert_called_once_with("ds", "t", {"a": "new a"})

    def test_absent_column_skipped(self):
        fake = _fake({"a": ""})
        _reconcile(fake, descriptions={"a": "desc a", "ghost": "x"})
        fake.set_column_descriptions.assert_called_once_with("ds", "t", {"a": "desc a"})

    def test_config_key_normalized_to_stored_column(self):
        fake = _fake({"order_item_id": ""})
        _reconcile(fake, descriptions={"OrderItem_ID": "the id"})
        fake.set_column_descriptions.assert_called_once_with(
            "ds", "t", {"order_item_id": "the id"}
        )

    def test_table_description_none_leaves_table_untouched(self):
        fake = _fake({"a": ""})
        _reconcile(fake, descriptions={}, table_description=None)
        fake.get_table_description.assert_not_called()
        fake.set_table_description.assert_not_called()

    def test_table_description_changed_written(self):
        fake = _fake({}, "old")
        _reconcile(fake, descriptions={}, table_description="new")
        fake.set_table_description.assert_called_once_with("ds", "t", "new")

    def test_unsupported_destination_is_noop(self):
        fake = MagicMock()
        fake.supports_description_reconcile.return_value = False
        _reconcile(fake, descriptions={"a": "x"}, table_description="y")
        fake.get_column_descriptions.assert_not_called()
        fake.set_column_descriptions.assert_not_called()
