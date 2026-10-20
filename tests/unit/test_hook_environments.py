"""Lifecycle hooks fire only in the environments they're configured for.

``saga_project.yml`` is shared by every target, so a hook declared there — a
``hooks:`` entry, an installed entry-point hook — would otherwise run for a
developer's local run exactly as it does in production.
"""

from dataclasses import fields
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

from dlt_saga.hooks.loader import load_hooks_from_config, load_hooks_from_entry_points
from dlt_saga.hooks.registry import (
    HOOK_EVENTS,
    ON_PIPELINE_ERROR,
    ON_RUN_COMPLETE,
    HookContext,
    HookRegistry,
)
from dlt_saga.project_config import HooksConfig, SagaProjectConfig

_HOOK = "tests.unit.test_hook_environments:recorded_hook"
_calls = []


def recorded_hook(ctx):
    _calls.append(ctx)


@pytest.fixture(autouse=True)
def _clear_calls():
    _calls.clear()


def _context(environment):
    return HookContext(
        pipeline_name="p", config=None, command="ingest", environment=environment
    )


@pytest.mark.unit
class TestRegistryGate:
    """The gate reads the environment from the event, never from global state."""

    def test_ungated_handler_fires_everywhere(self):
        registry = HookRegistry()
        registry.register(ON_PIPELINE_ERROR, recorded_hook)
        registry.fire(ON_PIPELINE_ERROR, _context("dev"))
        assert len(_calls) == 1

    def test_gated_handler_skipped_outside_its_environments(self):
        registry = HookRegistry()
        registry.register(ON_PIPELINE_ERROR, recorded_hook, environments=["prod"])
        registry.fire(ON_PIPELINE_ERROR, _context("dev"))
        assert not registry.has_handlers(ON_PIPELINE_ERROR, "dev")
        assert _calls == []

    def test_gated_handler_fires_in_its_environment(self):
        registry = HookRegistry()
        registry.register(ON_PIPELINE_ERROR, recorded_hook, environments=["prod"])
        registry.fire(ON_PIPELINE_ERROR, _context("prod"))
        assert registry.has_handlers(ON_PIPELINE_ERROR, "prod")
        assert len(_calls) == 1

    def test_gated_handler_never_fires_without_an_environment(self):
        registry = HookRegistry()
        registry.register(ON_PIPELINE_ERROR, recorded_hook, environments=["prod"])
        with patch("dlt_saga.utility.naming.get_environment", return_value="prod"):
            registry.fire(ON_PIPELINE_ERROR, _context(None))
        assert _calls == []

    def test_pipeline_events_carry_the_active_environment(self):
        from dlt_saga.hooks import loader
        from dlt_saga.hooks.registry import fire_pipeline_error, get_hook_registry

        registry = get_hook_registry()
        registry.clear()
        registry.register(ON_PIPELINE_ERROR, recorded_hook, environments=["prod"])
        config = SimpleNamespace(pipeline_name="p")
        try:
            with (
                patch.object(loader, "load_hooks"),
                patch("dlt_saga.utility.naming.get_environment", return_value="prod"),
            ):
                fire_pipeline_error(config, "ingest", Exception("x"))
        finally:
            registry.clear()

        assert [ctx.environment for ctx in _calls] == ["prod"]


@pytest.mark.unit
class TestHooksConfig:
    def test_event_fields_match_the_registry(self):
        events = {f.name for f in fields(HooksConfig)} - {"environments"}
        assert events == set(HOOK_EVENTS)

    def test_parses_environments_and_events(self):
        hooks = HooksConfig.from_dict(
            {"environments": "prod", "on_pipeline_error": [_HOOK]}
        )
        assert hooks.environments == ["prod"]
        assert hooks.events == {"on_pipeline_error": [_HOOK]}

    def test_environments_required_when_hooks_listed(self):
        with pytest.raises(ValueError, match="hooks.environments is required"):
            HooksConfig.from_dict({"on_pipeline_error": [_HOOK]})

    def test_environments_alone_is_valid(self):
        """Entry-point-only projects declare the environments and nothing else."""
        assert HooksConfig.from_dict({"environments": ["prod"]}).events == {}

    def test_project_config_parses_the_block(self):
        config = SagaProjectConfig.from_dict(
            {"hooks": {"environments": ["prod"], "on_run_complete": [_HOOK]}}
        )
        assert config.hooks.events == {"on_run_complete": [_HOOK]}

    @pytest.mark.parametrize(
        "data, match",
        [
            (["mypackage.hooks:x"], "hooks must be a mapping"),
            ({"environments": ["prod"], "on_pipline_error": [_HOOK]}, "Unknown hook"),
            ({"environments": ["prod"], "on_run_complete": _HOOK}, "must be a list"),
            ({"environments": [], "on_run_complete": [_HOOK]}, "hooks.environments"),
        ],
    )
    def test_malformed_blocks_rejected(self, data, match):
        with pytest.raises(ValueError, match=match):
            HooksConfig.from_dict(data)


@pytest.mark.unit
class TestConfigHooks:
    def test_registered_for_the_listed_environments(self):
        registry = HookRegistry()
        load_hooks_from_config(
            HooksConfig.from_dict(
                {"environments": ["prod"], "on_pipeline_error": [_HOOK]}
            ),
            registry,
        )
        assert registry.has_handlers(ON_PIPELINE_ERROR, "prod")
        assert not registry.has_handlers(ON_PIPELINE_ERROR, "dev")


@pytest.mark.unit
class TestEntryPointHooks:
    def _entry_point(self):
        return SimpleNamespace(name="recorder", load=lambda: recorded_hook)

    def test_use_the_hooks_environments(self):
        recorded_hook.saga_hook_events = [ON_RUN_COMPLETE]
        registry = HookRegistry()
        try:
            with patch(
                "importlib.metadata.entry_points", return_value=[self._entry_point()]
            ):
                load_hooks_from_entry_points(registry, environments=["prod"])
        finally:
            del recorded_hook.saga_hook_events

        assert registry.has_handlers(ON_RUN_COMPLETE, "prod")
        assert not registry.has_handlers(ON_RUN_COMPLETE, "dev")

    def test_skipped_with_a_warning_without_environments(self, caplog):
        """Whether one is installed depends on the venv, not the project."""
        registry = HookRegistry()
        with patch(
            "importlib.metadata.entry_points", return_value=[self._entry_point()]
        ):
            load_hooks_from_entry_points(registry, environments=None)

        assert registry.is_empty()
        assert "Skipping installed lifecycle hooks (recorder)" in caplog.text

    def test_loader_passes_hooks_environments(self):
        from dlt_saga.hooks import loader

        project = MagicMock(
            hooks=HooksConfig.from_dict({"environments": ["prod"]}),
            notifications=None,
        )
        with (
            patch("dlt_saga.project_config.get_project_config", return_value=project),
            patch.object(loader, "load_hooks_from_entry_points") as from_entry_points,
        ):
            loader._reset_loaded()
            try:
                loader.load_hooks(HookRegistry())
            finally:
                loader._reset_loaded()

        assert from_entry_points.call_args.kwargs["environments"] == ["prod"]


@pytest.mark.unit
class TestRunCompleteAfterContextCloses:
    """`saga run` fires one `on_run_complete` when its scope closes — after both
    Session calls have restored the previous execution context. The gate must
    read the event's own environment, not whatever context is active by then.
    """

    def test_prod_run_fires_after_its_context_has_closed(self):
        from dlt_saga.hooks.registry import get_hook_registry
        from dlt_saga.hooks.run_scope import contribute, run_notification_scope
        from dlt_saga.utility.cli.context import (
            clear_execution_context,
            execution_context_scope,
        )
        from dlt_saga.utility.cli.profiles import ProfileTarget

        prod = ProfileTarget(
            name="prod",
            environment="prod",
            destination_type="duckdb",
            schema="s",
            destination_config={"database_path": ":memory:"},
        )
        registry = get_hook_registry()
        registry.clear()
        clear_execution_context()
        registry.register(ON_RUN_COMPLETE, recorded_hook, environments=["prod"])
        try:
            with run_notification_scope("run", target="prod", environment="prod"):
                with execution_context_scope(prod):  # what each Session call does
                    contribute([], target="prod", environment="prod")
        finally:
            registry.clear()

        assert [ctx.environment for ctx in _calls] == ["prod"]


@pytest.mark.unit
class TestOneEnvironmentResolver:
    """Every context gets its environment from `environment_of`, so a target-less
    run gated on SAGA_ENVIRONMENT behaves the same for every event.
    """

    def test_target_environment_wins(self, monkeypatch):
        from dlt_saga.utility.naming import environment_of

        monkeypatch.setenv("SAGA_ENVIRONMENT", "prod")
        assert environment_of(SimpleNamespace(environment="dev")) == "dev"

    def test_without_a_target_uses_saga_environment(self, monkeypatch):
        from dlt_saga.utility.naming import environment_of

        monkeypatch.setenv("SAGA_ENVIRONMENT", "prod")
        assert environment_of(None) == "prod"

    def test_without_either_is_dev(self, monkeypatch):
        from dlt_saga.utility.naming import environment_of

        monkeypatch.delenv("SAGA_ENVIRONMENT", raising=False)
        monkeypatch.delenv("DLT_ENVIRONMENT", raising=False)
        assert environment_of(None) == "dev"

    def test_run_complete_without_a_target_follows_saga_environment(self, monkeypatch):
        """No profiles.yml + SAGA_ENVIRONMENT=prod: on_run_complete still fires."""
        from dlt_saga.hooks.registry import get_hook_registry
        from dlt_saga.session import Session

        monkeypatch.setenv("SAGA_ENVIRONMENT", "prod")
        registry = get_hook_registry()
        registry.clear()
        registry.register(ON_RUN_COMPLETE, recorded_hook, environments=["prod"])
        session = Session.__new__(Session)
        session._profile_target = None
        try:
            session._fire_run_complete(
                "ingest", None, SimpleNamespace(pipeline_results=[]), None
            )
        finally:
            registry.clear()

        assert [ctx.environment for ctx in _calls] == ["prod"]
