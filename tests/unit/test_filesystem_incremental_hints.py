"""FilesystemClient._apply_incremental_hints: an end_date upper bound is honored
whether or not initial_value is set."""

from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

import pytest

from dlt_saga.pipelines.filesystem.client import FilesystemClient
from dlt_saga.pipelines.filesystem.config import FilesystemConfig

pytestmark = pytest.mark.unit

_INC = "dlt_saga.pipelines.filesystem.client.dlt.sources.incremental"


def _client(**overrides) -> FilesystemClient:
    cfg = {
        "filesystem_type": "file",
        "bucket_name": "/data",
        "file_type": "csv",
        "file_glob": "*.csv",
    }
    cfg.update(overrides)
    return FilesystemClient(FilesystemConfig(**cfg))


class TestApplyIncrementalHints:
    def test_end_date_without_initial_value_applies_upper_bound(self):
        # The regression: a bare end_date fell through to the no-bound branch and
        # was silently dropped. It must now cap the load with initial_value=None.
        client = _client()
        fs = MagicMock()
        end = datetime(2026, 6, 1, 23, 59, 59, tzinfo=timezone.utc)
        with patch(_INC) as inc:
            client._apply_incremental_hints(fs, "date", None, end)
        inc.assert_called_once()
        assert inc.call_args.kwargs["initial_value"] is None
        assert inc.call_args.kwargs["end_value"] == end
        fs.apply_hints.assert_called_once()

    def test_initial_and_end_apply_both_bounds(self):
        client = _client()
        fs = MagicMock()
        start = datetime(2026, 1, 1, tzinfo=timezone.utc)
        end = datetime(2026, 6, 1, tzinfo=timezone.utc)
        with patch(_INC) as inc:
            client._apply_incremental_hints(fs, "date", start, end)
        assert inc.call_args.kwargs["initial_value"] == start
        assert inc.call_args.kwargs["end_value"] == end

    def test_initial_only_has_no_end_bound(self):
        client = _client()
        fs = MagicMock()
        start = datetime(2026, 1, 1, tzinfo=timezone.utc)
        with patch(_INC) as inc:
            client._apply_incremental_hints(fs, "date", start, None)
        assert inc.call_args.kwargs["initial_value"] == start
        assert "end_value" not in inc.call_args.kwargs

    def test_neither_bound_is_plain_incremental(self):
        client = _client()
        fs = MagicMock()
        with patch(_INC) as inc:
            client._apply_incremental_hints(fs, "date", None, None)
        assert inc.call_args.args == ("date",)
        assert inc.call_args.kwargs == {}
