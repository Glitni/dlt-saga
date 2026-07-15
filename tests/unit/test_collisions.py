"""Unit tests for destination-target collision detection.

Detection resolves every config's target through the config source in one or
more environments, so these tests inject a fake source whose
``resolve_ingest_target`` returns each config's pre-set target (optionally
per-environment) and expose ``discover`` for the project-wide runtime guard.
``resolve_historize_target`` is patched for the historize layer. That keeps the
tests focused on grouping/detection/scope; the name resolution itself is
covered in the naming/factory tests.
"""

from types import SimpleNamespace

import pytest

from dlt_saga.utility.collisions import (
    TargetCollisionError,
    check_target_collisions,
    detect_target_collisions,
    format_collisions,
)

pytestmark = pytest.mark.unit


def _config(
    name,
    *,
    schema="dlt_dev",
    table=None,
    ingest=True,
    historize=False,
    env_targets=None,
):
    """Build a minimal stand-in for a resolved PipelineConfig.

    ``schema`` / ``table`` are the config's target in every environment;
    ``env_targets`` (``{env: (schema, table)}``) overrides per environment for
    the dev-vs-prod tests.
    """
    table = table if table is not None else name
    return SimpleNamespace(
        pipeline_name=name,
        schema_name=schema,
        table_name=table,
        ingest_enabled=ingest,
        historize_enabled=historize,
        config_dict={"config_path": f"configs/{name}.yml"},
        _default_target=(schema, table),
        _env_targets=env_targets or {},
    )


class _FakeSource:
    """Fake ConfigSource: resolves each config's target and discovers them all."""

    def __init__(self, configs):
        self._by_path = {
            c.config_dict["config_path"]: c
            for c in configs
            if "config_path" in c.config_dict
        }
        self._configs = configs

    def resolve_ingest_target(
        self, config_path, *, schema_override=None, environment=None
    ):
        config = self._by_path[config_path]
        schema, table = config._env_targets.get(environment, config._default_target)
        return (schema_override or schema, table)

    def discover(self):
        return ({"g": list(self._configs)}, {})


def _source(configs):
    return _FakeSource(configs)


def _detect(configs, *, environments=("prod",), source=None, **kwargs):
    return detect_target_collisions(
        configs, source or _source(configs), environments=list(environments), **kwargs
    )


class TestIngestCollisions:
    def test_distinct_targets_do_not_collide(self):
        configs = [
            _config("group__a", table="group__a"),
            _config("group__b", table="group__b"),
        ]
        assert _detect(configs) == []

    def test_same_schema_and_table_collide(self):
        configs = [
            _config("group__a", schema="dlt_dev", table="brands"),
            _config("group__b", schema="dlt_dev", table="brands"),
            _config("group__c", schema="dlt_dev", table="brands"),
        ]
        collisions = _detect(configs)
        assert len(collisions) == 1
        collision = collisions[0]
        assert collision.layer == "ingest"
        assert collision.schema == "dlt_dev"
        assert collision.normalized_table == "brands"
        assert collision.display_table == "brands"
        assert collision.pipelines == ["group__a", "group__b", "group__c"]

    def test_same_table_different_schema_does_not_collide(self):
        configs = [
            _config("a", schema="dlt_group_a", table="sales"),
            _config("b", schema="dlt_group_b", table="sales"),
        ]
        assert _detect(configs) == []

    def test_table_name_normalized_before_comparison(self):
        # dlt normalizes table identifiers, so these land in one physical table.
        configs = [
            _config("a", table="MyBrands"),
            _config("b", table="my_brands"),
        ]
        collisions = _detect(configs)
        assert len(collisions) == 1
        assert collisions[0].pipelines == ["a", "b"]

    def test_raw_name_shown_when_single(self):
        # A single raw name → shown verbatim (what the config produces).
        configs = [
            _config("a", table="MyBrands"),
            _config("b", table="MyBrands"),
        ]
        collision = _detect(configs)[0]
        assert collision.normalized_table == "my_brands"
        assert collision.display_table == "MyBrands"

    def test_multi_raw_display_lists_sources(self):
        # Distinct raw names snake-casing to one table → show both raws.
        configs = [
            _config("a", table="MyBrands"),
            _config("b", table="my_brands"),
        ]
        collision = _detect(configs)[0]
        assert collision.normalized_table == "my_brands"
        assert collision.display_table == "my_brands (from MyBrands, my_brands)"

    def test_unresolved_schema_is_skipped(self):
        # schema == "" means unresolved — must not manufacture a collision.
        configs = [
            _config("a", schema="", table="brands"),
            _config("b", schema="", table="brands"),
        ]
        assert _detect(configs) == []

    def test_ingest_disabled_configs_ignored(self):
        configs = [
            _config("a", table="brands", ingest=True),
            _config("b", table="brands", ingest=False),
        ]
        assert _detect(configs) == []

    def test_check_ingest_false_disables_ingest_layer(self):
        configs = [
            _config("a", table="brands"),
            _config("b", table="brands"),
        ]
        assert _detect(configs, check_ingest=False, check_historize=False) == []

    def test_config_without_path_is_skipped(self):
        # A config the source can't resolve (no config_path) is skipped
        # best-effort rather than crashing the guard.
        configs = [
            _config("a", table="brands"),
            _config("b", table="brands"),
        ]
        configs[0].config_dict = {}  # drop config_path
        assert _detect(configs) == []


class TestEnvironmentScope:
    """Detection resolves per environment and unions the results."""

    def test_dev_only_collision_missed_by_prod_caught_by_dev(self):
        # A custom scheme collides in dev but stays distinct in prod.
        a = _config(
            "group__a",
            env_targets={"dev": ("dlt_dev", "shared"), "prod": ("dlt_a", "a")},
        )
        b = _config(
            "group__b",
            env_targets={"dev": ("dlt_dev", "shared"), "prod": ("dlt_b", "b")},
        )
        configs = [a, b]
        src = _source(configs)

        assert detect_target_collisions(configs, src, environments=["prod"]) == []

        dev = detect_target_collisions(configs, src, environments=["dev"])
        assert len(dev) == 1
        assert dev[0].pipelines == ["group__a", "group__b"]
        assert dev[0].environments == ("dev",)
        assert dev[0].env_label == "dev"

    def test_both_env_collision_reported_once(self):
        # Collides in dev AND prod (env-agnostic names) → single collision.
        configs = [
            _config("group__a", table="shared"),
            _config("group__b", table="shared"),
        ]
        collisions = detect_target_collisions(
            configs, _source(configs), environments=["dev", "prod"]
        )
        assert len(collisions) == 1
        assert collisions[0].pipelines == ["group__a", "group__b"]
        # Manifests in both, reported once, carrying both environments.
        assert collisions[0].environments == ("dev", "prod")
        assert collisions[0].env_label == "dev+prod"


class TestHistorizeCollisions:
    def test_historize_targets_collide(self, monkeypatch):
        import dlt_saga.historize.factory as factory

        def fake_resolve(config, **kwargs):
            return (object(), "dlt_dev", "brands_historized")

        monkeypatch.setattr(factory, "resolve_historize_target", fake_resolve)

        configs = [
            _config("a", ingest=False, historize=True),
            _config("b", ingest=False, historize=True),
        ]
        collisions = _detect(configs)
        assert len(collisions) == 1
        assert collisions[0].layer == "historize"
        assert collisions[0].normalized_table == "brands_historized"
        assert collisions[0].pipelines == ["a", "b"]

    def test_historize_only_collision_via_output_target(self, monkeypatch):
        # Two historize-only configs sharing an explicit output schema+table:
        # the ingest layer can't see them (no ingest target), so only the
        # historize layer catches the collision.
        import dlt_saga.historize.factory as factory

        def fake_resolve(config, **kwargs):
            return (object(), "archive", "customer_orders")

        monkeypatch.setattr(factory, "resolve_historize_target", fake_resolve)

        configs = [
            _config("a", ingest=False, historize=True),
            _config("b", ingest=False, historize=True),
        ]
        collisions = _detect(configs)
        assert [c.layer for c in collisions] == ["historize"]
        assert collisions[0].schema == "archive"
        assert collisions[0].normalized_table == "customer_orders"

    def test_ingest_target_threaded_into_historize(self, monkeypatch):
        # The historized target derives from the config's ingest target in the
        # same environment, threaded in as source_schema/source_table.
        import dlt_saga.historize.factory as factory

        seen = {}

        def fake_resolve(
            config, *, environment=None, source_schema=None, source_table=None
        ):
            seen[config.pipeline_name] = (environment, source_schema, source_table)
            return (object(), source_schema, f"{source_table}_historized")

        monkeypatch.setattr(factory, "resolve_historize_target", fake_resolve)

        configs = [
            _config(
                "a",
                schema="dlt_group",
                table="orders",
                ingest=False,
                historize=True,
            ),
        ]
        _detect(configs)
        assert seen["a"] == ("prod", "dlt_group", "orders")

    def test_unresolvable_historize_target_skipped(self, monkeypatch):
        import dlt_saga.historize.factory as factory

        def boom(config, **kwargs):
            raise ValueError("bad historize config")

        monkeypatch.setattr(factory, "resolve_historize_target", boom)

        configs = [
            _config("a", ingest=False, historize=True),
            _config("b", ingest=False, historize=True),
        ]
        # Best-effort: a resolution error must not crash the guard.
        assert _detect(configs) == []

    def test_ingest_and_historize_layers_reported_separately(self, monkeypatch):
        import dlt_saga.historize.factory as factory

        monkeypatch.setattr(
            factory,
            "resolve_historize_target",
            lambda c, **kw: (object(), "dlt_dev", "shared_hist"),
        )

        configs = [
            _config("a", schema="dlt_dev", table="shared_raw", historize=True),
            _config("b", schema="dlt_dev", table="shared_raw", historize=True),
        ]
        collisions = _detect(configs)
        layers = sorted(c.layer for c in collisions)
        assert layers == ["historize", "ingest"]


class TestCheckProjectWide:
    """The runtime guard detects project-wide but raises only for the selection."""

    def test_selected_collides_with_unselected(self):
        # Running only the new pipeline is refused because an existing
        # (unselected) enabled config already claims the target.
        hourly = _config("group__hourly", table="report")
        monthly = _config("group__monthly", table="report")
        source = _source([hourly, monthly])
        with pytest.raises(TargetCollisionError) as exc:
            check_target_collisions([hourly], source)
        message = str(exc.value)
        assert "group__hourly" in message
        assert "group__monthly" in message

    def test_unrelated_selection_not_blocked(self):
        # A and B collide, but only unrelated C is selected → C runs.
        a = _config("group__a", table="shared")
        b = _config("group__b", table="shared")
        c = _config("group__c", table="unique")
        source = _source([a, b, c])
        check_target_collisions([c], source)  # must not raise

    def test_raises_on_collision_within_selection(self):
        configs = [
            _config("a", table="brands"),
            _config("b", table="brands"),
        ]
        with pytest.raises(TargetCollisionError) as exc:
            check_target_collisions(configs, _source(configs))
        message = str(exc.value)
        assert "dlt_dev.brands" in message
        assert "a" in message and "b" in message

    def test_passes_when_no_collision(self):
        configs = [_config("a"), _config("b")]
        check_target_collisions(configs, _source(configs))  # must not raise


class TestFormat:
    def test_format_lists_target_and_pipelines(self):
        configs = [
            _config("a", table="brands"),
            _config("b", table="brands"),
        ]
        rendered = format_collisions(_detect(configs))
        assert "dlt_dev.brands (ingest target, prod)" in rendered
        assert "← a, b" in rendered
