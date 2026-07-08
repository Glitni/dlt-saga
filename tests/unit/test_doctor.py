"""Unit tests for `saga doctor` resolution reporting.

These assert on the structured (symbol, label, detail) tuples passed to the
`emit` callback — not on rendered CLI text — so they're immune to terminal
width/ANSI/encoding differences.
"""

from types import SimpleNamespace
from unittest.mock import patch

import pytest

from dlt_saga import cli


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
                cli, "discover_and_select_configs", return_value=({"g": configs}, {})
            ),
            patch.object(cli, "flatten_configs", return_value=configs),
        ):
            cli._doctor_check_configs(None, context, verbose=False, emit=emit)

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
                cli, "discover_and_select_configs", return_value=({"g": configs}, {})
            ),
            patch.object(cli, "flatten_configs", return_value=configs),
        ):
            cli._doctor_check_configs(
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
                cli, "discover_and_select_configs", return_value=({"g": configs}, {})
            ),
            patch.object(cli, "flatten_configs", return_value=configs),
        ):
            cli._doctor_check_configs(["fs__x"], context, verbose=False, emit=emit)

        out = capsys.readouterr().out
        assert "fs__x → dlt_dev.fs__x" in out
        assert ".dlt_dev" not in out.replace("dlt_dev.fs__x", "")

    def test_caps_listing_and_reports_hidden_count(self, capsys):
        configs = [_cfg(f"g__p{i}", "s", f"g__p{i}") for i in range(25)]
        context = SimpleNamespace(get_database=lambda: "proj")
        emit = _CaptureEmit()

        with (
            patch.object(
                cli, "discover_and_select_configs", return_value=({"g": configs}, {})
            ),
            patch.object(cli, "flatten_configs", return_value=configs),
        ):
            cli._doctor_check_configs(["g"], context, verbose=False, emit=emit)

        out = capsys.readouterr().out
        # 20 listed, 5 hidden — no silent truncation.
        assert "and 5 more" in out

    def test_verbose_lists_all(self, capsys):
        configs = [_cfg(f"g__p{i}", "s", f"g__p{i}") for i in range(25)]
        context = SimpleNamespace(get_database=lambda: "proj")
        emit = _CaptureEmit()

        with (
            patch.object(
                cli, "discover_and_select_configs", return_value=({"g": configs}, {})
            ),
            patch.object(cli, "flatten_configs", return_value=configs),
        ):
            cli._doctor_check_configs(None, context, verbose=True, emit=emit)

        out = capsys.readouterr().out
        assert "and " not in out or "more" not in out
        assert "g__p24" in out


@pytest.mark.unit
class TestDoctorEmitVersion:
    def test_marks_editable_vs_installed(self):
        emit = _CaptureEmit()
        cli._doctor_emit_version(emit)
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
