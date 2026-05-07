"""Unit tests for historize factory helpers."""

from unittest.mock import MagicMock

import pytest

from dlt_saga.historize.config import HistorizeConfig
from dlt_saga.historize.factory import (
    _resolve_historize_storage_path,
    _resolve_historize_table_format,
)


def _make_historize_config(**kwargs) -> HistorizeConfig:
    return HistorizeConfig.from_dict(kwargs, top_level_primary_key=["id"])


def _make_context(
    table_format: str = "native",
    historize_table_format=None,
    historize_storage_path=None,
    storage_path=None,
):
    ctx = MagicMock()
    ctx.get_table_format.return_value = table_format
    ctx.get_historize_table_format.return_value = historize_table_format
    ctx.get_historize_storage_path.return_value = historize_storage_path
    ctx.get_storage_path.return_value = storage_path
    return ctx


@pytest.mark.unit
class TestResolveHistorizeTableFormat:
    def test_pipeline_historize_wins(self):
        cfg = _make_historize_config(table_format="iceberg")
        ctx = _make_context(table_format="native", historize_table_format="delta")
        assert _resolve_historize_table_format(cfg, {}, ctx) == "iceberg"

    def test_pipeline_level_table_format_second(self):
        cfg = _make_historize_config()
        ctx = _make_context(table_format="native", historize_table_format="delta")
        result = _resolve_historize_table_format(cfg, {"table_format": "iceberg"}, ctx)
        assert result == "iceberg"

    def test_profile_historize_third(self):
        cfg = _make_historize_config()
        ctx = _make_context(table_format="native", historize_table_format="delta")
        result = _resolve_historize_table_format(cfg, {}, ctx)
        assert result == "delta"

    def test_profile_table_format_fourth(self):
        cfg = _make_historize_config()
        ctx = _make_context(table_format="iceberg", historize_table_format=None)
        result = _resolve_historize_table_format(cfg, {}, ctx)
        assert result == "iceberg"

    def test_falls_back_to_native(self):
        cfg = _make_historize_config()
        ctx = _make_context(table_format="native", historize_table_format=None)
        result = _resolve_historize_table_format(cfg, {}, ctx)
        assert result == "native"


@pytest.mark.unit
class TestResolveHistorizeStoragePath:
    def test_profile_historize_storage_path_first(self):
        ctx = _make_context(
            historize_storage_path="gs://historize-bucket/",
            storage_path="gs://default-bucket/",
        )
        assert _resolve_historize_storage_path(ctx) == "gs://historize-bucket/"

    def test_profile_storage_path_fallback(self):
        ctx = _make_context(historize_storage_path=None, storage_path="gs://bucket/")
        assert _resolve_historize_storage_path(ctx) == "gs://bucket/"

    def test_none_when_neither_set(self):
        ctx = _make_context(historize_storage_path=None, storage_path=None)
        assert _resolve_historize_storage_path(ctx) is None
