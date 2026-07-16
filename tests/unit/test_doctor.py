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


def _collision_cfg(name, schema="dlt_dev", table=None):
    table = table if table is not None else name
    return SimpleNamespace(
        pipeline_name=name,
        schema_name=schema,
        table_name=table,
        ingest_enabled=True,
        historize_enabled=False,
        config_dict={"config_path": f"configs/{name}.yml", "_prod": (schema, table)},
    )


def _fake_source_for(selected):
    """A fake ConfigSource resolving each config's target and discovering them.

    The collision guard resolves targets through the source and discovers the
    project-wide set, so doctor tests must inject one; the configs carry their
    intended target.
    """
    all_configs = [c for configs in selected.values() for c in configs]
    targets = {
        c.config_dict["config_path"]: c.config_dict["_prod"] for c in all_configs
    }

    class _FakeSource:
        def resolve_ingest_target(
            self,
            config_path,
            *,
            schema_override=None,
            table_override=None,
            environment=None,
        ):
            schema, table = targets[config_path]
            return (schema_override or schema, table_override or table)

        def discover(self):
            return ({"g": list(all_configs)}, {})

    return _FakeSource()


@pytest.mark.unit
class TestDoctorCheckCollisions:
    def test_passes_when_no_collision(self):
        selected = {"g": [_collision_cfg("g__a"), _collision_cfg("g__b")]}
        emit = _CaptureEmit()
        with patch.object(
            doctor, "get_config_source", return_value=_fake_source_for(selected)
        ):
            ok = doctor._doctor_check_collisions(selected, verbose=False, emit=emit)
        assert ok is True
        symbol, label, _ = emit.calls[0]
        assert symbol == "✓"
        assert label == "Target collisions"

    def test_fails_and_lists_collision(self, capsys):
        selected = {
            "g": [
                _collision_cfg("g__a", table="brands"),
                _collision_cfg("g__b", table="brands"),
            ]
        }
        emit = _CaptureEmit()
        with patch.object(
            doctor, "get_config_source", return_value=_fake_source_for(selected)
        ):
            ok = doctor._doctor_check_collisions(selected, verbose=False, emit=emit)
        assert ok is False
        symbol, label, _ = emit.calls[0]
        assert symbol == "✗"
        assert label == "Target collisions"
        out = capsys.readouterr().out
        # Doctor checks dev ∪ prod; this env-agnostic fake collides in both.
        assert "dlt_dev.brands (ingest, dev+prod)" in out
        assert "g__a, g__b" in out

    def test_empty_selection_passes(self):
        emit = _CaptureEmit()
        assert doctor._doctor_check_collisions({}, verbose=False, emit=emit) is True


@pytest.mark.unit
class TestDoctorCheckLegacyKeys:
    """Advisory scan of raw YAML for deprecated alias keys — never fatal."""

    def _run(self, selected, project_path):
        emit = _CaptureEmit()
        flat = [c for configs in selected.values() for c in configs]
        with (
            patch.object(doctor, "flatten_configs", return_value=flat),
            patch.object(
                doctor,
                "get_config_source",
                return_value=SimpleNamespace(project_config_path=project_path),
            ),
            # profiles are covered elsewhere; skip them here.
            patch(
                "dlt_saga.utility.cli.profiles.get_profiles_config",
                side_effect=RuntimeError,
            ),
        ):
            ok = doctor._doctor_check_legacy_keys(selected, verbose=False, emit=emit)
        return ok, emit

    def test_clean_project_emits_ok(self, tmp_path):
        cfg = tmp_path / "a.yml"
        cfg.write_text("write_disposition: append\nschema_name: analytics\n")
        selected = {"g": [SimpleNamespace(config_dict={"config_path": str(cfg)})]}
        ok, emit = self._run(selected, str(tmp_path / "missing_project.yml"))
        assert ok is True
        symbol, label, detail = emit.calls[0]
        assert symbol == "✓"
        assert label == "Config keys"

    def test_flags_legacy_keys_but_stays_non_fatal(self, tmp_path, capsys):
        cfg = tmp_path / "a.yml"
        cfg.write_text("historize:\n  output_table: orders\n")
        proj = tmp_path / "saga_project.yml"
        proj.write_text("pipelines:\n  dataset_access:\n    - OWNER:x\n")
        selected = {"g": [SimpleNamespace(config_dict={"config_path": str(cfg)})]}
        ok, emit = self._run(selected, str(proj))
        # Advisory only — deprecated-but-working keys don't fail doctor.
        assert ok is True
        symbol, label, _ = emit.calls[0]
        assert symbol == "!"
        assert label == "Config keys"
        out = capsys.readouterr().out
        assert "output_table → table_name" in out
        assert "dataset_access → schema_access" in out


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
