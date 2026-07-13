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
        def __init__(self, configs):
            pass

        def select(self, selectors=None, warn_on_no_match=True):
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
