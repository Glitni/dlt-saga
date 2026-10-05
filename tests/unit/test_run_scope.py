"""Run-notification scope.

`saga run` is one command but two session calls, so without coalescing a
notifier would post one digest per phase. The scope is what makes "one
invocation, one event" true; these tests pin the cases where that guarantee is
easiest to lose — a failing command, an empty selection, and code paths that
run outside any scope at all.
"""

from dataclasses import dataclass
from types import SimpleNamespace

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


class _Runner:
    """Stand-in for HistorizeRunner, returning a canned result."""

    def __init__(self, result):
        self._result = result

    def run(self):
        return self._result


def _config(pipeline_name):
    """A PipelineConfig thin enough for the hook context, real enough to pass."""
    from dlt_saga.pipeline_config import PipelineConfig

    return PipelineConfig(
        pipeline_group="shop",
        pipeline_name=pipeline_name,
        table_name="orders",
        identifier="shop/orders",
        config_dict={"write_disposition": "append"},
        enabled=True,
        tags=[],
        schema_name="dlt_shop",
    )


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
    """Hooks must fire from the worker path too, not only from `Session`.

    Every `registry.fire(...)` used to live in `session.py`, and worker mode
    executes pipelines without going through `Session` — so a custom handler
    silently never ran on exactly the fan-out deployment it was written for
    (#495). These drive the worker's own wrappers and assert the events arrive.
    """

    @pytest.fixture
    def worker_hooks(self, monkeypatch):
        """Capture every per-pipeline event a worker wrapper fires."""
        from dlt_saga.hooks.registry import (
            ON_PIPELINE_COMPLETE,
            ON_PIPELINE_ERROR,
            ON_PIPELINE_START,
        )

        registry = HookRegistry()
        events = []
        for event in (ON_PIPELINE_START, ON_PIPELINE_COMPLETE, ON_PIPELINE_ERROR):
            registry.register(event, lambda ctx, e=event: events.append((e, ctx)))
        monkeypatch.setattr(
            "dlt_saga.hooks.registry.get_hook_registry", lambda: registry, raising=True
        )
        # Registration is this fixture's job; the real loader would read the
        # project config off disk.
        monkeypatch.setattr(
            "dlt_saga.hooks.loader.load_hooks", lambda *a, **k: None, raising=True
        )
        return events

    def test_a_worker_ingest_fires_start_and_complete(self, worker_hooks, monkeypatch):
        from dlt_saga.utility.cli import run_modes

        monkeypatch.setattr(
            "dlt_saga.pipelines.executor.execute_pipeline",
            lambda *a, **k: [],
            raising=True,
        )

        assert run_modes._run_pipeline_safe(_config("shop__orders"), "[1/1]") is None

        assert [e for e, _ in worker_hooks] == [
            "on_pipeline_start",
            "on_pipeline_complete",
        ]
        assert worker_hooks[0][1].pipeline_name == "shop__orders"
        assert worker_hooks[0][1].command == "ingest"

    def test_a_worker_ingest_failure_fires_the_error_event(
        self, worker_hooks, monkeypatch
    ):
        """The case the gap actually cost: an `on_pipeline_error` handler on a
        fan-out deployment.
        """
        from dlt_saga.utility.cli import run_modes

        def boom(*a, **k):
            raise RuntimeError("source unreachable")

        monkeypatch.setattr(
            "dlt_saga.pipelines.executor.execute_pipeline", boom, raising=True
        )

        assert run_modes._run_pipeline_safe(_config("shop__orders"), "[1/1]")

        fired = dict(worker_hooks)
        assert "on_pipeline_error" in fired
        assert str(fired["on_pipeline_error"].error) == "source unreachable"
        assert "on_pipeline_complete" not in fired

    def test_a_worker_historize_fires_too(self, worker_hooks, monkeypatch):
        from dlt_saga.utility.cli import run_modes

        monkeypatch.setattr(
            run_modes,
            "_build_historize_runner",
            lambda *a, **k: _Runner(
                {
                    "status": "completed",
                    "mode": "incremental",
                    "snapshots_processed": 1,
                }
            ),
            raising=True,
        )

        assert run_modes._run_historize_safe(_config("shop__orders"), False, "") is None

        assert [e for e, _ in worker_hooks] == [
            "on_pipeline_start",
            "on_pipeline_complete",
        ]
        assert worker_hooks[0][1].command == "historize"

    def test_a_historize_runner_reporting_failure_fires_the_error_event(
        self, worker_hooks, monkeypatch
    ):
        """The runner reports a failed status rather than raising, so a plain
        try/except would miss it — as it would in `Session`.
        """
        from dlt_saga.utility.cli import run_modes

        monkeypatch.setattr(
            run_modes,
            "_build_historize_runner",
            lambda *a, **k: _Runner({"status": "failed", "error": "no snapshots"}),
            raising=True,
        )

        assert run_modes._run_historize_safe(_config("shop__orders"), False, "")

        fired = dict(worker_hooks)
        assert str(fired["on_pipeline_error"].error) == "no snapshots"

    def test_a_worker_does_not_fire_on_run_complete(self, monkeypatch):
        """One digest per container is the fan-out problem `saga notify` exists
        to solve, so a worker must never fire the per-command event.
        """
        import inspect

        from dlt_saga.utility.cli import run_modes

        source = inspect.getsource(run_modes)
        assert "ON_RUN_COMPLETE" not in source
        assert "run_notification_scope" not in source


@pytest.mark.unit
class TestHookRegistrationReachesWorkers:
    """Firing is only half of it — a worker had nothing registered to fire at.

    `load_hooks()` was called from `Session.__init__` alone, so a container that
    never builds a Session held an empty registry. Firing from the worker path
    without this would have changed nothing at all.
    """

    def test_firing_loads_hooks_when_nothing_has(self, monkeypatch):
        from dlt_saga.hooks import loader
        from dlt_saga.hooks import registry as registry_mod

        loaded = []
        monkeypatch.setattr(
            loader, "load_hooks", lambda *a, **k: loaded.append(True), raising=True
        )
        monkeypatch.setattr(
            registry_mod, "get_hook_registry", lambda: HookRegistry(), raising=True
        )

        registry_mod.fire_pipeline_start(_config("shop__orders"), "ingest")

        assert loaded, "a worker would have fired into an empty registry"

    def test_session_still_loads_hooks_itself(self):
        """The programmatic `Session` API must keep working for `on_run_complete`,
        which is fired from the run scope rather than through these helpers.
        """
        import inspect

        from dlt_saga import session

        assert "load_hooks()" in inspect.getsource(session.Session.__init__)


@pytest.mark.unit
class TestConcurrentRegistration:
    """Loading is now reached from the per-pipeline fire path.

    That path runs in the worker thread executing each pipeline, so on the first
    event of a run several threads arrive at once. `load_hooks` guards a bare
    check-then-set, and the registry appends without deduplicating — so two
    threads getting through would register the same handlers twice, and a user's
    `on_pipeline_error` would be called once per racing thread. Before this, the
    only caller was `Session.__init__` on the main thread and no concurrency
    reached it at all.
    """

    def test_loading_holds_an_exclusive_lock(self, monkeypatch):
        """Asserted on the lock rather than on an outcome: the unguarded window
        is the two bytecodes between the check and the set, which no amount of
        sleeping inside the load widens — a timing test here would pass whether
        or not the guard existed.
        """
        import threading

        from dlt_saga.hooks import loader
        from dlt_saga.hooks import registry as registry_mod

        registry = HookRegistry()
        monkeypatch.setattr(
            registry_mod, "get_hook_registry", lambda: registry, raising=True
        )

        contended = []

        def probe_from_another_thread(*_a, **_k):
            def probe():
                got = loader._load_lock.acquire(timeout=0.2)
                contended.append(not got)
                if got:
                    loader._load_lock.release()

            other = threading.Thread(target=probe)
            other.start()
            other.join()

        monkeypatch.setattr(loader, "load_hooks_from_config", probe_from_another_thread)
        monkeypatch.setattr(loader, "load_notifiers_from_config", lambda *a, **k: None)
        monkeypatch.setattr(
            loader, "load_hooks_from_entry_points", lambda *a, **k: None
        )
        monkeypatch.setattr(
            "dlt_saga.project_config.get_project_config",
            lambda: SimpleNamespace(hooks={"on_pipeline_start": ["m:f"]}),
            raising=True,
        )
        loader._reset_loaded()

        try:
            loader.load_hooks()
            assert contended == [True], (
                "a second thread entered while hooks were loading"
            )
        finally:
            loader._reset_loaded()

    def test_a_second_call_does_not_register_again(self, monkeypatch):
        from dlt_saga.hooks import loader
        from dlt_saga.hooks import registry as registry_mod

        registry = HookRegistry()
        monkeypatch.setattr(
            registry_mod, "get_hook_registry", lambda: registry, raising=True
        )
        monkeypatch.setattr(
            loader,
            "load_hooks_from_config",
            lambda *a, **k: registry.register("on_pipeline_start", lambda ctx: None),
        )
        monkeypatch.setattr(loader, "load_notifiers_from_config", lambda *a, **k: None)
        monkeypatch.setattr(
            loader, "load_hooks_from_entry_points", lambda *a, **k: None
        )
        monkeypatch.setattr(
            "dlt_saga.project_config.get_project_config",
            lambda: SimpleNamespace(hooks={"on_pipeline_start": ["m:f"]}),
            raising=True,
        )
        loader._reset_loaded()

        try:
            loader.load_hooks()
            loader.load_hooks()
            loader.load_hooks()

            assert len(registry._hooks["on_pipeline_start"]) == 1
        finally:
            loader._reset_loaded()


@pytest.mark.unit
class TestHookLoadingNeverFailsAPipeline:
    """Loading moved into the pipeline's own path, so it must not break it.

    `fire` already swallows handler exceptions. Nothing protected getting to
    the handlers, and `load_hooks` reads and parses `saga_project.yml` — which
    used to happen in `Session.__init__`, where a failure was a clean startup
    error rather than a pipeline's.
    """

    def test_a_broken_loader_is_logged_not_raised(self, monkeypatch, caplog):
        import logging

        from dlt_saga.hooks import loader
        from dlt_saga.hooks import registry as registry_mod

        def explode(*_a, **_k):
            raise RuntimeError("saga_project.yml is unparseable")

        monkeypatch.setattr(loader, "load_hooks", explode, raising=True)
        monkeypatch.setattr(
            registry_mod, "get_hook_registry", lambda: HookRegistry(), raising=True
        )

        with caplog.at_level(logging.WARNING):
            registry_mod.fire_pipeline_start(_config("shop__orders"), "ingest")

        assert "Could not load lifecycle hooks" in caplog.text

    def test_the_event_still_reaches_handlers_already_registered(self, monkeypatch):
        """A load failure must not cost handlers that are already in place."""
        from dlt_saga.hooks import loader
        from dlt_saga.hooks import registry as registry_mod

        registry = HookRegistry()
        seen = []
        registry.register("on_pipeline_start", seen.append)
        monkeypatch.setattr(
            loader,
            "load_hooks",
            lambda *a, **k: (_ for _ in ()).throw(RuntimeError("boom")),
            raising=True,
        )
        monkeypatch.setattr(
            registry_mod, "get_hook_registry", lambda: registry, raising=True
        )

        registry_mod.fire_pipeline_start(_config("shop__orders"), "ingest")

        assert len(seen) == 1
