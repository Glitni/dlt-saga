"""Unit tests for the destroy orchestration (Session + destroy module)."""

from types import SimpleNamespace

import pytest

import dlt_saga.destroy as destroy_mod
from dlt_saga.session import Session

pytestmark = pytest.mark.unit


def _config(name="test__t"):
    return SimpleNamespace(pipeline_name=name)


# ---------------------------------------------------------------------------
# resource_type gating in _execute_single_destroy
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "resource_type, expected",
    [
        ("all", ["ingest", "historize"]),
        ("ingest", ["ingest"]),
        ("historize", ["historize"]),
    ],
)
def test_execute_single_destroy_gates_by_resource_type(
    monkeypatch, resource_type, expected
):
    calls = []

    def fake_ingest(c, dry_run, log_prefix=""):
        calls.append("ingest")
        return {"schema": "s", "tables": [], "table_ids": []}

    def fake_historize(c, dry_run, log_prefix=""):
        calls.append("historize")
        return {
            "schema": "s",
            "table": None,
            "table_id": None,
            "dropped": False,
            "log_cleared": False,
        }

    monkeypatch.setattr(destroy_mod, "destroy_ingest_footprint", fake_ingest)
    monkeypatch.setattr(destroy_mod, "destroy_historize_footprint", fake_historize)

    result = Session._execute_single_destroy(_config(), resource_type, False)

    assert result.success
    assert calls == expected


def test_execute_single_destroy_logs_one_named_summary(monkeypatch, caplog):
    """Each pipeline emits a single summary line that names it and its targets."""

    def fake_ingest(c, dry_run, log_prefix=""):
        return {"schema": "s", "tables": ["t"], "table_ids": ["db.s.t"]}

    def fake_historize(c, dry_run, log_prefix=""):
        return {
            "schema": "s",
            "table": "t_historized",
            "table_id": "db.s.t_historized",
            "dropped": True,
            "log_cleared": True,
        }

    monkeypatch.setattr(destroy_mod, "destroy_ingest_footprint", fake_ingest)
    monkeypatch.setattr(destroy_mod, "destroy_historize_footprint", fake_historize)

    with caplog.at_level("INFO", logger="dlt_saga.session"):
        Session._execute_single_destroy(_config("grp__thing"), "all", True, "[2/5]")

    lines = [r.getMessage() for r in caplog.records if r.name == "dlt_saga.session"]
    assert len(lines) == 1
    msg = lines[0]
    assert "grp__thing" in msg  # pipeline is named, not just "[2/5]"
    assert "[2/5]" in msg
    assert "[DRY RUN] would remove" in msg
    assert "db.s.t" in msg and "db.s.t_historized" in msg
    assert "historize log entries" in msg


def test_execute_single_destroy_logs_nothing_to_remove(monkeypatch, caplog):
    monkeypatch.setattr(
        destroy_mod,
        "destroy_ingest_footprint",
        lambda c, dry_run, log_prefix="": {
            "schema": "s",
            "tables": [],
            "table_ids": [],
        },
    )
    monkeypatch.setattr(
        destroy_mod,
        "destroy_historize_footprint",
        lambda c, dry_run, log_prefix="": None,
    )

    with caplog.at_level("INFO", logger="dlt_saga.session"):
        Session._execute_single_destroy(_config("grp__empty"), "all", False)

    lines = [r.getMessage() for r in caplog.records if r.name == "dlt_saga.session"]
    assert len(lines) == 1
    assert "grp__empty" in lines[0]
    assert "nothing to remove" in lines[0]


def test_execute_single_destroy_captures_errors(monkeypatch):
    def boom(c, dry_run, log_prefix=""):
        raise RuntimeError("kaboom")

    monkeypatch.setattr(destroy_mod, "destroy_ingest_footprint", boom)

    result = Session._execute_single_destroy(_config(), "ingest", False)

    assert not result.success
    assert "kaboom" in result.error


# ---------------------------------------------------------------------------
# destroy_historize_footprint tolerates configs with no historize layer
# ---------------------------------------------------------------------------


def test_destroy_historize_footprint_no_layer(monkeypatch):
    def boom(cfg, full_refresh=False, log_prefix=None):
        raise ValueError("primary_key not set")

    monkeypatch.setattr("dlt_saga.historize.factory.build_historize_runner", boom)

    assert destroy_mod.destroy_historize_footprint(_config()) is None


def test_destroy_historize_footprint_delegates_to_runner(monkeypatch):
    sentinel = {"schema": "s", "targets": ["t_historized"], "dropped": ["t_historized"]}
    fake_runner = SimpleNamespace(destroy=lambda dry_run: sentinel)

    monkeypatch.setattr(
        "dlt_saga.historize.factory.build_historize_runner",
        lambda cfg, full_refresh=False, log_prefix=None: fake_runner,
    )

    assert destroy_mod.destroy_historize_footprint(_config(), dry_run=True) is sentinel


# ---------------------------------------------------------------------------
# destroy_ingest_footprint clears incremental state outside the dropped tables
# ---------------------------------------------------------------------------


class _FakeNativeState:
    def __init__(self, has_rows):
        self.has_rows = has_rows
        self.cleared = []

    def has_pipeline_state(self, pipeline_name):
        return self.has_rows

    def clear_pipeline_state(self, pipeline_name):
        self.cleared.append(pipeline_name)


def _patch_ingest_destroy(monkeypatch, tmp_path, tables, native_state):
    """Wire destroy_ingest_footprint to fakes; returns the fake destination."""
    resets = []
    destination = SimpleNamespace(
        config=SimpleNamespace(database="db"),
        get_ingested_targets=lambda schema, name: tables,
        get_full_table_id=lambda schema, table: f"db.{schema}.{table}",
        reset_destination_state=lambda pipeline_name, table_name: resets.append(
            table_name
        ),
        resets=resets,
    )
    context = SimpleNamespace(get_destination_type=lambda: "duckdb")
    monkeypatch.setattr(
        "dlt_saga.utility.cli.context.get_execution_context", lambda: context
    )
    monkeypatch.setattr(
        "dlt_saga.destinations.factory.DestinationFactory.create_from_context",
        lambda dest_type, ctx, cfg: destination,
    )
    monkeypatch.setattr(
        "dlt_saga.pipelines.native_load.state.NativeLoadStateManager",
        lambda dest, schema: native_state,
    )
    monkeypatch.setattr(
        "dlt_saga.pipelines.base_pipeline.local_pipelines_dir",
        lambda dest_type, database, schema: str(tmp_path),
    )
    return destination


def _ingest_config():
    return SimpleNamespace(pipeline_name="grp__thing", schema_name="s", config_dict={})


def _seed_local_state(pipelines_dir, name):
    """Give a local dlt pipeline real state (a schema), then deactivate it."""
    import dlt

    pipeline = dlt.pipeline(pipeline_name=name, pipelines_dir=str(pipelines_dir))
    pipeline.extract([{"id": 1}], table_name="t")
    pipeline.deactivate()


def _has_local_state(pipelines_dir, name):
    probe = destroy_mod._open_local_dlt_state(str(pipelines_dir), name)
    if probe is None:
        return False
    probe.deactivate()
    return True


@pytest.fixture
def no_active_dlt_pipeline():
    """Assert destroy leaves no dlt pipeline active in the process-wide context."""
    from dlt.common.configuration.container import Container
    from dlt.common.pipeline import PipelineContext

    yield
    assert not Container()[PipelineContext].is_active()


def test_destroy_ingest_clears_native_log_and_local_state(
    monkeypatch, tmp_path, no_active_dlt_pipeline
):
    """Regression: destroy left the native_load cursor behind, so a re-created
    pipeline resumed from it instead of its new initial_value."""
    native_state = _FakeNativeState(has_rows=True)
    destination = _patch_ingest_destroy(monkeypatch, tmp_path, ["thing"], native_state)
    _seed_local_state(tmp_path, "grp__thing")

    result = destroy_mod.destroy_ingest_footprint(_ingest_config())

    assert destination.resets == ["thing"]
    assert native_state.cleared == ["grp__thing"]
    assert not _has_local_state(tmp_path, "grp__thing")
    assert result["native_load_log"] is True
    assert result["local_state"] is True


def test_destroy_ingest_twice_is_a_noop(monkeypatch, tmp_path, no_active_dlt_pipeline):
    """dlt's drop() re-creates an empty working folder; that is not state."""
    _patch_ingest_destroy(monkeypatch, tmp_path, [], _FakeNativeState(False))
    _seed_local_state(tmp_path, "grp__thing")

    destroy_mod.destroy_ingest_footprint(_ingest_config())
    second = destroy_mod.destroy_ingest_footprint(_ingest_config(), dry_run=True)

    assert second["local_state"] is False


def test_destroy_ingest_leaves_sibling_local_state(
    monkeypatch, tmp_path, no_active_dlt_pipeline
):
    """Only this pipeline's working folder is dropped, never the shared dir."""
    _patch_ingest_destroy(monkeypatch, tmp_path, [], _FakeNativeState(False))
    for name in ("grp__thing", "grp__sibling"):
        _seed_local_state(tmp_path, name)

    destroy_mod.destroy_ingest_footprint(_ingest_config())

    assert not _has_local_state(tmp_path, "grp__thing")
    assert _has_local_state(tmp_path, "grp__sibling")


def test_destroy_ingest_dry_run_reports_without_clearing(
    monkeypatch, tmp_path, no_active_dlt_pipeline
):
    native_state = _FakeNativeState(has_rows=True)
    destination = _patch_ingest_destroy(monkeypatch, tmp_path, ["thing"], native_state)
    _seed_local_state(tmp_path, "grp__thing")

    result = destroy_mod.destroy_ingest_footprint(_ingest_config(), dry_run=True)

    assert destination.resets == []
    assert native_state.cleared == []
    assert _has_local_state(tmp_path, "grp__thing")
    assert result["native_load_log"] is True
    assert result["local_state"] is True


def test_destroy_ingest_probes_before_dropping(monkeypatch, tmp_path):
    """A failing probe must not leave a half-finished teardown behind."""

    class _UnreadableLog(_FakeNativeState):
        def has_pipeline_state(self, pipeline_name):
            raise RuntimeError("access denied")

    destination = _patch_ingest_destroy(
        monkeypatch, tmp_path, ["thing"], _UnreadableLog(False)
    )

    with pytest.raises(RuntimeError, match="access denied"):
        destroy_mod.destroy_ingest_footprint(_ingest_config())
    assert destination.resets == []


def test_destroy_ingest_without_state_is_a_noop(monkeypatch, tmp_path):
    native_state = _FakeNativeState(has_rows=False)
    destination = _patch_ingest_destroy(monkeypatch, tmp_path, [], native_state)

    result = destroy_mod.destroy_ingest_footprint(_ingest_config())

    assert destination.resets == []
    assert native_state.cleared == []
    assert result["native_load_log"] is False
    assert result["local_state"] is False


def test_execute_single_destroy_names_cleared_state(monkeypatch, caplog):
    monkeypatch.setattr(
        destroy_mod,
        "destroy_ingest_footprint",
        lambda c, dry_run, log_prefix="": {
            "schema": "s",
            "tables": [],
            "table_ids": [],
            "native_load_log": True,
            "local_state": True,
        },
    )

    with caplog.at_level("INFO", logger="dlt_saga.session"):
        Session._execute_single_destroy(_config("grp__thing"), "ingest", False)

    lines = [r.getMessage() for r in caplog.records if r.name == "dlt_saga.session"]
    assert len(lines) == 1
    assert "native_load log entries" in lines[0]
    assert "local dlt state" in lines[0]


# ---------------------------------------------------------------------------
# Session.destroy input validation
# ---------------------------------------------------------------------------


def test_session_destroy_rejects_bad_resource_type():
    # Validation happens before any profile/context work, so a bare instance
    # via __new__ is enough to exercise it.
    session = Session.__new__(Session)
    with pytest.raises(ValueError, match="resource_type"):
        session.destroy(resource_type="bogus")


# ---------------------------------------------------------------------------
# Discovery must not warn "did not match" when the match is a disabled config
# ---------------------------------------------------------------------------


def test_discover_and_select_threads_warn_flag(monkeypatch):
    """destroy suppresses the enabled-set no-match warning; run commands keep it.

    Regression: a selector matching only a disabled config warned spuriously
    because the enabled-set selector ran with warn_on_no_match=True.
    """
    import dlt_saga.session as session_mod

    warn_flags = []

    class FakeSelector:
        def __init__(self, configs, state=None):
            pass

        def select(self, selectors=None, warn_on_no_match=True, exclude=None):
            warn_flags.append(warn_on_no_match)
            return {}

    monkeypatch.setattr(session_mod, "PipelineSelector", FakeSelector)

    session = Session.__new__(Session)
    session._config_source = SimpleNamespace(discover=lambda: ({}, {}))

    # destroy path — enabled selector must be told not to warn.
    warn_flags.clear()
    session._discover_and_select(["x"], filter_fn=None, warn_on_no_match=False)
    assert warn_flags == [False, False]  # enabled, then disabled

    # default (run commands) — enabled selector keeps the warning.
    warn_flags.clear()
    session._discover_and_select(["x"], filter_fn=None)
    assert warn_flags == [True, False]
