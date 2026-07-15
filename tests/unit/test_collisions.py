"""Unit tests for destination-target collision detection."""

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
):
    """Build a minimal stand-in for a resolved PipelineConfig."""
    return SimpleNamespace(
        pipeline_name=name,
        schema_name=schema,
        table_name=table if table is not None else name,
        ingest_enabled=ingest,
        historize_enabled=historize,
        config_dict={},
    )


class TestIngestCollisions:
    def test_distinct_targets_do_not_collide(self):
        configs = [
            _config("group__a", table="group__a"),
            _config("group__b", table="group__b"),
        ]
        assert detect_target_collisions(configs) == []

    def test_same_schema_and_table_collide(self):
        configs = [
            _config("group__a", schema="dlt_dev", table="brands"),
            _config("group__b", schema="dlt_dev", table="brands"),
            _config("group__c", schema="dlt_dev", table="brands"),
        ]
        collisions = detect_target_collisions(configs)
        assert len(collisions) == 1
        collision = collisions[0]
        assert collision.layer == "ingest"
        assert collision.schema == "dlt_dev"
        assert collision.table == "brands"
        assert collision.pipelines == ["group__a", "group__b", "group__c"]

    def test_same_table_different_schema_does_not_collide(self):
        configs = [
            _config("a", schema="dlt_group_a", table="sales"),
            _config("b", schema="dlt_group_b", table="sales"),
        ]
        assert detect_target_collisions(configs) == []

    def test_table_name_normalized_before_comparison(self):
        # dlt normalizes table identifiers, so these land in one physical table.
        configs = [
            _config("a", table="MyBrands"),
            _config("b", table="my_brands"),
        ]
        collisions = detect_target_collisions(configs)
        assert len(collisions) == 1
        assert collisions[0].pipelines == ["a", "b"]

    def test_unresolved_schema_is_skipped(self):
        # schema_name == "" means unresolved — must not manufacture a collision.
        configs = [
            _config("a", schema="", table="brands"),
            _config("b", schema="", table="brands"),
        ]
        assert detect_target_collisions(configs) == []

    def test_ingest_disabled_configs_ignored(self):
        configs = [
            _config("a", table="brands", ingest=True),
            _config("b", table="brands", ingest=False),
        ]
        assert detect_target_collisions(configs) == []

    def test_check_ingest_false_disables_ingest_layer(self):
        configs = [
            _config("a", table="brands"),
            _config("b", table="brands"),
        ]
        assert (
            detect_target_collisions(configs, check_ingest=False, check_historize=False)
            == []
        )


class TestHistorizeCollisions:
    def test_historize_targets_collide(self, monkeypatch):
        import dlt_saga.historize.factory as factory

        def fake_resolve(config):
            return (object(), "dlt_dev", "brands_historized")

        monkeypatch.setattr(factory, "resolve_historize_target", fake_resolve)

        configs = [
            _config("a", ingest=False, historize=True),
            _config("b", ingest=False, historize=True),
        ]
        collisions = detect_target_collisions(configs)
        assert len(collisions) == 1
        assert collisions[0].layer == "historize"
        assert collisions[0].table == "brands_historized"
        assert collisions[0].pipelines == ["a", "b"]

    def test_unresolvable_historize_target_skipped(self, monkeypatch):
        import dlt_saga.historize.factory as factory

        def boom(config):
            raise ValueError("bad historize config")

        monkeypatch.setattr(factory, "resolve_historize_target", boom)

        configs = [
            _config("a", ingest=False, historize=True),
            _config("b", ingest=False, historize=True),
        ]
        # Best-effort: a resolution error must not crash the guard.
        assert detect_target_collisions(configs) == []

    def test_ingest_and_historize_layers_reported_separately(self, monkeypatch):
        import dlt_saga.historize.factory as factory

        monkeypatch.setattr(
            factory,
            "resolve_historize_target",
            lambda c: (object(), "dlt_dev", "shared_hist"),
        )

        configs = [
            _config("a", schema="dlt_dev", table="shared_raw", historize=True),
            _config("b", schema="dlt_dev", table="shared_raw", historize=True),
        ]
        collisions = detect_target_collisions(configs)
        layers = sorted(c.layer for c in collisions)
        assert layers == ["historize", "ingest"]


class TestCheckAndFormat:
    def test_check_raises_on_collision(self):
        configs = [
            _config("a", table="brands"),
            _config("b", table="brands"),
        ]
        with pytest.raises(TargetCollisionError) as exc:
            check_target_collisions(configs)
        message = str(exc.value)
        assert "dlt_dev.brands" in message
        assert "a" in message and "b" in message

    def test_check_passes_when_no_collision(self):
        configs = [_config("a"), _config("b")]
        # Should not raise.
        check_target_collisions(configs)

    def test_format_lists_target_and_pipelines(self):
        collisions = detect_target_collisions(
            [
                _config("a", table="brands"),
                _config("b", table="brands"),
            ]
        )
        rendered = format_collisions(collisions)
        assert "dlt_dev.brands (ingest target)" in rendered
        assert "← a, b" in rendered
