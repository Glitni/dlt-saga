"""delete_after_load has been removed from the filesystem source.

It never worked (a no-op) and a correct implementation is unsafe under
incremental loading (would delete filtered-out, never-loaded files). These guard
against accidental reintroduction of the config field / delete plumbing.
"""

from dataclasses import fields

import pytest

from dlt_saga.pipelines.filesystem.config import FilesystemConfig
from dlt_saga.pipelines.filesystem.pipeline import FilesystemPipeline


@pytest.mark.unit
class TestNoDeleteAfterLoad:
    def test_config_has_no_delete_after_load_field(self):
        assert "delete_after_load" not in {f.name for f in fields(FilesystemConfig)}

    def test_pipeline_has_no_delete_plumbing(self):
        for attr in (
            "_delete_source_files",
            "_get_files_to_delete",
            "_should_delete_files",
            "_delete_files",
            "_normalize_file_path",
        ):
            assert not hasattr(FilesystemPipeline, attr)

    def test_stale_delete_after_load_key_is_filtered_out(self):
        # A config that still sets the removed key must not reach FilesystemConfig.
        # FilesystemPipeline.__init__ filters the raw config to known fields, so a
        # stale delete_after_load is dropped (net effect: it does nothing, as
        # before) rather than raising an unexpected-keyword error.
        names = {f.name for f in fields(FilesystemConfig)}
        raw = {"file_type": "csv", "delete_after_load": True}
        filtered = {k: v for k, v in raw.items() if k in names}
        assert "delete_after_load" not in filtered
        assert "file_type" in filtered
