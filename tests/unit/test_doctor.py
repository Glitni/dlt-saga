"""Unit tests for `saga doctor` resolution reporting.

These assert on the structured (symbol, label, detail) tuples passed to the
`emit` callback — not on rendered CLI text — so they're immune to terminal
width/ANSI/encoding differences.
"""

from types import SimpleNamespace
from unittest.mock import patch

import pytest

from dlt_saga import cli
from dlt_saga.utility.cli import doctor


def _cfg(pipeline_name, schema_name, table_name):
    return SimpleNamespace(
        pipeline_name=pipeline_name,
        schema_name=schema_name,
        table_name=table_name,
    )


class _CaptureEmit:
    """Callable that records emit(symbol, label, detail) calls."""

    def __init__(self):
        self.calls = []

    def __call__(self, symbol, label, detail=""):
        self.calls.append((symbol, label, detail))


@pytest.mark.unit
class TestDoctorCheckConfigs:
    def test_reports_distinct_resolved_schemas(self):
        configs = [
            _cfg("database__a", "dbt_grindheim", "database__a"),
            _cfg("google_sheets__b", "dbt_grindheim", "google_sheets__b"),
        ]
        context = SimpleNamespace(get_database=lambda: "proj")
        emit = _CaptureEmit()

        with (
            patch.object(
                doctor, "discover_and_select_configs", return_value=({"g": configs}, {})
            ),
            patch.object(doctor, "flatten_configs", return_value=configs),
        ):
            doctor._doctor_check_configs(None, context, verbose=False, emit=emit)

        symbol, label, detail = emit.calls[0]
        assert symbol == "✓"
        assert label == "Pipeline configs"
        # Distinct set, sorted, deduplicated.
        assert "schema(s): dbt_grindheim" in detail
        assert "2 pipeline(s)" in detail

    def test_lists_per_pipeline_target_when_selected(self, capsys):
        configs = [_cfg("database__a", "dbt_grindheim", "database__a")]
        context = SimpleNamespace(get_database=lambda: "proj")
        emit = _CaptureEmit()

        with (
            patch.object(
                doctor, "discover_and_select_configs", return_value=({"g": configs}, {})
            ),
            patch.object(doctor, "flatten_configs", return_value=configs),
        ):
            doctor._doctor_check_configs(
                ["database__a"], context, verbose=False, emit=emit
            )

        out = capsys.readouterr().out
        assert "proj.dbt_grindheim.database__a" in out

    def test_target_omits_missing_project(self, capsys):
        # DuckDB and friends have no project; the target should degrade to
        # schema.table rather than emit a leading dot.
        configs = [_cfg("fs__x", "dlt_dev", "fs__x")]
        context = SimpleNamespace(get_database=lambda: None)
        emit = _CaptureEmit()

        with (
            patch.object(
                doctor, "discover_and_select_configs", return_value=({"g": configs}, {})
            ),
            patch.object(doctor, "flatten_configs", return_value=configs),
        ):
            doctor._doctor_check_configs(["fs__x"], context, verbose=False, emit=emit)

        out = capsys.readouterr().out
        assert "fs__x → dlt_dev.fs__x" in out
        assert ".dlt_dev" not in out.replace("dlt_dev.fs__x", "")

    def test_caps_listing_and_reports_hidden_count(self, capsys):
        configs = [_cfg(f"g__p{i}", "s", f"g__p{i}") for i in range(25)]
        context = SimpleNamespace(get_database=lambda: "proj")
        emit = _CaptureEmit()

        with (
            patch.object(
                doctor, "discover_and_select_configs", return_value=({"g": configs}, {})
            ),
            patch.object(doctor, "flatten_configs", return_value=configs),
        ):
            doctor._doctor_check_configs(["g"], context, verbose=False, emit=emit)

        out = capsys.readouterr().out
        # 20 listed, 5 hidden — no silent truncation.
        assert "and 5 more" in out

    def test_verbose_lists_all(self, capsys):
        configs = [_cfg(f"g__p{i}", "s", f"g__p{i}") for i in range(25)]
        context = SimpleNamespace(get_database=lambda: "proj")
        emit = _CaptureEmit()

        with (
            patch.object(
                doctor, "discover_and_select_configs", return_value=({"g": configs}, {})
            ),
            patch.object(doctor, "flatten_configs", return_value=configs),
        ):
            doctor._doctor_check_configs(None, context, verbose=True, emit=emit)

        out = capsys.readouterr().out
        assert "and " not in out or "more" not in out
        assert "g__p24" in out


@pytest.mark.unit
class TestDoctorCheckStrayProjectConfig:
    """The layout guard warns (never fails) when project defaults are unreadable."""

    def _run(self, root):
        emit = _CaptureEmit()
        with patch.object(doctor, "find_project_root", return_value=root):
            doctor._doctor_check_stray_project_config(emit)
        return emit.calls

    def test_silent_when_canonical_file_present(self, tmp_path):
        (tmp_path / "saga_project.yml").write_text("pipelines: {}\n")
        # A shadowed look-alike alongside the real file is inert → no warning.
        (tmp_path / "configs").mkdir()
        (tmp_path / "configs" / "dlt_project.yml").write_text("project: {}\n")
        assert self._run(tmp_path) == []

    def test_warns_on_old_docs_name_in_configs(self, tmp_path):
        (tmp_path / "configs").mkdir()
        (tmp_path / "configs" / "dlt_project.yml").write_text("project: {}\n")
        calls = self._run(tmp_path)
        assert len(calls) == 1
        symbol, label, detail = calls[0]
        assert symbol == "!"
        assert label == "Project defaults"
        assert "dlt_project.yml" in detail
        assert "old docs name" in detail
        # Names the path actually read.
        assert str(tmp_path / "saga_project.yml") in detail

    def test_warns_on_right_name_wrong_directory(self, tmp_path):
        (tmp_path / "configs").mkdir()
        (tmp_path / "configs" / "saga_project.yml").write_text("pipelines: {}\n")
        symbol, label, detail = self._run(tmp_path)[0]
        assert symbol == "!"
        assert "in configs/ instead of the project root" in detail

    def test_warns_on_near_miss_spellings_at_root(self, tmp_path):
        (tmp_path / "saga_project.yaml").write_text("pipelines: {}\n")
        (tmp_path / "saga-project.yml").write_text("pipelines: {}\n")
        detail = self._run(tmp_path)[0][2]
        assert "saga_project.yaml" in detail
        assert "saga-project.yml" in detail

    def test_warns_on_dlt_hyphen_variant(self, tmp_path):
        # The hyphen typo of the *old* base name — the gap that hand-listing left.
        (tmp_path / "dlt-project.yml").write_text("project: {}\n")
        detail = self._run(tmp_path)[0][2]
        assert "dlt-project.yml" in detail
        assert "old docs name" in detail
        assert "hyphen instead of underscore" in detail

    def test_warns_when_no_project_file_but_multiple_groups(self, tmp_path):
        configs = tmp_path / "configs"
        configs.mkdir()
        (configs / "google_sheets").mkdir()
        (configs / "filesystem").mkdir()
        symbol, label, detail = self._run(tmp_path)[0]
        assert symbol == "!"
        assert "2 pipeline" in detail

    def test_silent_when_no_project_file_and_single_group(self, tmp_path):
        configs = tmp_path / "configs"
        configs.mkdir()
        (configs / "google_sheets").mkdir()
        assert self._run(tmp_path) == []

    def test_silent_on_empty_project(self, tmp_path):
        # Nothing to warn about: no project file, no configs tree.
        assert self._run(tmp_path) == []


@pytest.mark.unit
class TestDoctorEmitVersion:
    def test_marks_editable_vs_installed(self):
        emit = _CaptureEmit()
        doctor._doctor_emit_version(emit)
        symbol, label, detail = emit.calls[0]
        assert symbol == "✓"
        assert label.startswith("dlt-saga ")
        assert ("editable/local" in detail) or ("installed" in detail)


@pytest.mark.unit
def test_doctor_command_exposes_select_option():
    # Introspect the registered click command rather than parsing --help text.
    from typer.main import get_command

    command = get_command(cli.app)
    doctor_cmd = command.commands["doctor"]
    opt_names = {opt.name for opt in doctor_cmd.params}
    assert "select" in opt_names
