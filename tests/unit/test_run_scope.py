"""Run-notification scope.

`saga run` is one command but two session calls, so without coalescing a
notifier would post one digest per phase. The scope is what makes "one
invocation, one event" true; these tests pin the cases where that guarantee is
easiest to lose — a failing command, an empty selection, and code paths that
run outside any scope at all.
"""

from dataclasses import dataclass

import pytest

from dlt_saga.hooks.registry import ON_RUN_COMPLETE, HookRegistry
from dlt_saga.hooks.run_scope import (
    contribute,
    get_active_scope,
    run_notification_scope,
)


@dataclass
class _Result:
    pipeline_name: str
    success: bool = True


@pytest.fixture
def captured(monkeypatch):
    """Route the scope's fire at a private registry and capture the events."""
    registry = HookRegistry()
    events = []
    registry.register(ON_RUN_COMPLETE, events.append)
    monkeypatch.setattr(
        "dlt_saga.hooks.registry.get_hook_registry", lambda: registry, raising=True
    )
    return events


@pytest.mark.unit
class TestScopeCoalescing:
    def test_phases_merge_into_one_event(self, captured):
        with run_notification_scope("run", select=["tag:daily"], target="prod"):
            contribute([_Result("a")], target="prod", environment="prod")
            contribute([_Result("b", success=False)])

        assert len(captured) == 1
        ctx = captured[0]
        assert ctx.command == "run"
        assert ctx.select == ["tag:daily"]
        assert [r.pipeline_name for r in ctx.results] == ["a", "b"]
        assert ctx.succeeded == 1
        assert ctx.failed == 1

    def test_event_carries_timing(self, captured):
        with run_notification_scope("run"):
            contribute([_Result("a")])

        assert captured[0].duration_seconds is not None
        assert captured[0].duration_seconds >= 0

    def test_empty_scope_still_fires(self, captured):
        """A selection that stopped matching must not fall silent."""
        with run_notification_scope("run"):
            pass

        assert len(captured) == 1
        assert captured[0].results == []

    def test_fires_even_when_the_command_raises(self, captured):
        """A command that died halfway is exactly when the alert matters."""
        with pytest.raises(RuntimeError):
            with run_notification_scope("run"):
                contribute([_Result("a", success=False)])
                raise RuntimeError("exploded")

        assert len(captured) == 1
        assert captured[0].failed == 1

    def test_target_recorded_from_first_contribution(self, captured):
        with run_notification_scope("run"):
            contribute([_Result("a")], target="prod", environment="production")
            contribute([_Result("b")], target="other", environment="other")

        assert captured[0].target == "prod"
        assert captured[0].environment == "production"

    def test_explicit_target_is_not_overwritten(self, captured):
        with run_notification_scope("run", target="prod", environment="production"):
            contribute([_Result("a")], target="ignored", environment="ignored")

        assert captured[0].target == "prod"
        assert captured[0].environment == "production"


@pytest.mark.unit
class TestScopeLifecycle:
    def test_no_scope_by_default(self):
        assert get_active_scope() is None

    def test_scope_is_cleared_on_exit(self, captured):
        with run_notification_scope("run"):
            assert get_active_scope() is not None
        assert get_active_scope() is None

    def test_scope_is_cleared_after_an_exception(self, captured):
        with pytest.raises(RuntimeError):
            with run_notification_scope("run"):
                raise RuntimeError("boom")
        assert get_active_scope() is None

    def test_contribute_outside_a_scope_reports_no_absorption(self, captured):
        """The programmatic API fires immediately; contribute must say so."""
        assert contribute([_Result("a")]) is False
        assert captured == []

    def test_contribute_inside_a_scope_reports_absorption(self, captured):
        with run_notification_scope("run"):
            assert contribute([_Result("a")]) is True


@pytest.mark.unit
class TestScopeIsBestEffort:
    def test_failing_handler_does_not_escape_the_scope(self, monkeypatch):
        registry = HookRegistry()

        def boom(ctx):
            raise RuntimeError("handler exploded")

        registry.register(ON_RUN_COMPLETE, boom)
        monkeypatch.setattr(
            "dlt_saga.hooks.registry.get_hook_registry", lambda: registry, raising=True
        )

        # Must not raise: a broken notifier cannot fail the command.
        with run_notification_scope("run"):
            contribute([_Result("a")])


@pytest.mark.unit
class TestHookReach:
    """Where hooks actually fire, pinned so the documented caveat stays true.

    Every `registry.fire(...)` lives in `session.py`, and saga's own worker mode
    executes pipelines without going through `Session` — so an orchestrated
    deployment fires nothing. That is a real gap (#495); this test exists so it
    cannot change silently in either direction, and so the wiki caveat is
    checkable rather than folklore.
    """

    def _fire_sites(self, module_name):
        import importlib
        import inspect

        source = inspect.getsource(importlib.import_module(module_name))
        return source.count("registry.fire(")

    def test_session_is_the_only_module_that_fires_hooks(self):
        assert self._fire_sites("dlt_saga.session") > 0

    def test_worker_mode_fires_no_hooks(self):
        """If this starts failing, #495 was fixed — update the wiki caveats in
        Configuration.md and Orchestration-Recipes.md.
        """
        assert self._fire_sites("dlt_saga.utility.cli.run_modes") == 0

    def test_worker_execution_bypasses_session(self):
        """The mechanism behind the gap: worker mode calls execute_pipeline
        directly rather than Session.ingest.
        """
        import inspect

        from dlt_saga.utility.cli import run_modes

        source = inspect.getsource(run_modes._run_pipeline_safe)
        assert "execute_pipeline" in source
        assert "Session" not in source
