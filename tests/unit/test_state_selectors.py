"""Unit tests for the ``state:new`` / ``state:failed`` selectors."""

from types import SimpleNamespace

import pytest

from dlt_saga.pipeline_config.base_config import PipelineConfig
from dlt_saga.utility.cli.pipeline_state import (
    PipelineStateResolver,
    StateSelectorError,
    build_state_resolver,
    selection_needs_state,
)
from dlt_saga.utility.cli.selectors import PipelineSelector


class FakeDestination:
    """Minimal destination stand-in for the two metadata reads."""

    def __init__(self, tables=None, plan_rows=None, list_error=None):
        # {schema: [table names]}
        self.tables = tables if tables is not None else {}
        self.plan_rows = plan_rows if plan_rows is not None else []
        self.list_error = list_error
        self.list_calls = []
        self.sql_calls = []

    def list_tables(self, schema):
        self.list_calls.append(schema)
        if self.list_error is not None:
            raise self.list_error
        return list(self.tables.get(schema, []))

    def get_full_table_id(self, schema, table):
        return f"{schema}.{table}"

    def timestamp_n_days_ago(self, days):
        return f"TS_MINUS_{days}"

    def execute_sql(self, sql, schema=None):
        self.sql_calls.append(sql)
        return list(self.plan_rows)


def make_config(
    name,
    group="filesystem",
    schema="dlt_filesystem",
    table=None,
    write_disposition="append",
):
    table = table if table is not None else name
    return PipelineConfig(
        pipeline_group=group,
        pipeline_name=f"{group}__{name}",
        table_name=table,
        identifier=f"configs/{group}/{name}.yml",
        config_dict={
            "base_table_name": table,
            "write_disposition": write_disposition,
        },
        enabled=True,
        tags=[],
        source_type="file",
        schema_name=schema,
    )


def as_groups(configs):
    """Organize a flat list of configs the way a config source hands them over."""
    grouped = {}
    for config in configs:
        grouped.setdefault(config.pipeline_group, []).append(config)
    return grouped


@pytest.fixture
def dev_schema(monkeypatch):
    """``state:failed`` reads the plan log from the environment's own schema."""
    monkeypatch.setenv("SAGA_SCHEMA_NAME", "dlt_dev")


def plan_row(group, table, status):
    return SimpleNamespace(pipeline_type=group, table_name=table, status=status)


def selected_names(result):
    return sorted(c.pipeline_name for group in result.values() for c in group)


# ---------------------------------------------------------------------------
# selection_needs_state
# ---------------------------------------------------------------------------


@pytest.mark.unit
@pytest.mark.parametrize(
    "select, expected",
    [
        (None, False),
        ([], False),
        (["tag:daily"], False),
        (["state:new"], True),
        (["state:failed"], True),
        (["tag:daily,state:new"], True),
        (["tag:daily, state:new"], True),
        (["tag:daily state:new"], True),
        (["group:api", "state:new"], True),
        # A pipeline whose name merely contains the word is not a state selector
        (["state_of_the_union"], False),
    ],
)
def test_selection_needs_state(select, expected):
    assert selection_needs_state(select) is expected


# ---------------------------------------------------------------------------
# state:new
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_state_new_selects_only_missing_ingest_targets():
    built = make_config("already_loaded")
    fresh = make_config("brand_new")
    destination = FakeDestination(tables={"dlt_filesystem": ["already_loaded"]})
    resolver = PipelineStateResolver(destination, layer="ingest")

    result = PipelineSelector(as_groups([built, fresh]), state=resolver).select(
        ["state:new"]
    )

    assert selected_names(result) == ["filesystem__brand_new"]


@pytest.mark.unit
def test_state_new_lists_each_schema_once():
    configs = [
        make_config("a", schema="dlt_filesystem"),
        make_config("b", schema="dlt_filesystem"),
        make_config("c", group="api", schema="dlt_api"),
    ]
    destination = FakeDestination(tables={"dlt_filesystem": ["a"], "dlt_api": []})
    resolver = PipelineStateResolver(destination, layer="ingest")

    PipelineSelector(as_groups(configs), state=resolver).select(["state:new"])

    assert sorted(destination.list_calls) == ["dlt_api", "dlt_filesystem"]


@pytest.mark.unit
def test_state_new_matches_dlt_normalized_table_names():
    """A config-declared name is matched as dlt would have created it."""
    config = make_config("mixed", table="MyBrands")
    destination = FakeDestination(tables={"dlt_filesystem": ["my_brands"]})
    resolver = PipelineStateResolver(destination, layer="ingest")

    assert resolver.is_new(config) is False


@pytest.mark.unit
def test_state_new_propagates_listing_failures():
    """A failed listing must not read as "nothing built yet"."""
    config = make_config("a")
    destination = FakeDestination(list_error=PermissionError("access denied"))
    resolver = PipelineStateResolver(destination, layer="ingest")

    with pytest.raises(PermissionError):
        resolver.is_new(config)


@pytest.mark.unit
def test_state_new_excludes_config_without_resolved_target(caplog):
    config = make_config("a", schema="")
    destination = FakeDestination()
    resolver = PipelineStateResolver(destination, layer="ingest")

    assert resolver.is_new(config) is False
    assert destination.list_calls == []
    assert "cannot judge it" in caplog.text


@pytest.mark.unit
def test_state_new_historize_layer_uses_historized_target(monkeypatch):
    import dlt_saga.historize.factory as factory

    config = make_config("orders", write_disposition="append+historize")
    monkeypatch.setattr(
        factory,
        "resolve_historize_target",
        lambda cfg, **kwargs: (None, "dlt_filesystem", "orders_historized"),
    )
    # Raw table exists, historized one does not.
    destination = FakeDestination(tables={"dlt_filesystem": ["orders"]})

    assert PipelineStateResolver(destination, layer="ingest").is_new(config) is False
    assert PipelineStateResolver(destination, layer="historize").is_new(config) is True
    # layer="any" is the union over the layers the config is enabled for.
    assert PipelineStateResolver(destination, layer="any").is_new(config) is True


@pytest.mark.unit
def test_state_new_excludes_unresolvable_historize_target(monkeypatch, caplog):
    import dlt_saga.historize.factory as factory

    config = make_config("orders", write_disposition="historize")

    def boom(cfg, **kwargs):
        raise ValueError("no placement")

    monkeypatch.setattr(factory, "resolve_historize_target", boom)
    resolver = PipelineStateResolver(FakeDestination(), layer="historize")

    assert resolver.is_new(config) is False
    assert "Could not resolve the historized target" in caplog.text


@pytest.mark.unit
def test_state_new_ignores_layers_the_config_is_not_enabled_for():
    """An ingest-only config is never judged against a historized target."""
    config = make_config("plain", write_disposition="append")
    destination = FakeDestination(tables={"dlt_filesystem": ["plain"]})

    assert PipelineStateResolver(destination, layer="any").is_new(config) is False


@pytest.mark.unit
def test_scoped_resolver_shares_reads():
    config = make_config("orders", write_disposition="append+historize")
    destination = FakeDestination(tables={"dlt_filesystem": ["orders"]})
    resolver = PipelineStateResolver(destination, layer="ingest")

    resolver.is_new(config)
    resolver.scoped("ingest").is_new(config)

    assert destination.list_calls == ["dlt_filesystem"]


# ---------------------------------------------------------------------------
# state:failed
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_state_failed_selects_pipelines_whose_last_run_failed(dev_schema):
    broke = make_config("broke")
    fine = make_config("fine")
    never_ran = make_config("never_ran")
    destination = FakeDestination(
        plan_rows=[
            plan_row("filesystem", "broke", "failed"),
            plan_row("filesystem", "fine", "completed"),
        ]
    )
    resolver = PipelineStateResolver(destination, layer="ingest")

    result = PipelineSelector(
        as_groups([broke, fine, never_ran]), state=resolver
    ).select(["state:failed"])

    assert selected_names(result) == ["filesystem__broke"]


@pytest.mark.unit
@pytest.mark.parametrize(
    "status, expected",
    [
        ("failed", True),
        ("abandoned", True),
        ("FAILED", True),
        ("completed", False),
        ("pending", False),
        ("running", False),
    ],
)
def test_state_failed_status_vocabulary(status, expected, dev_schema):
    config = make_config("a")
    destination = FakeDestination(plan_rows=[plan_row("filesystem", "a", status)])
    resolver = PipelineStateResolver(destination, layer="ingest")

    assert resolver.last_run_failed(config) is expected


@pytest.mark.unit
def test_state_failed_reads_the_plan_log_once(dev_schema):
    configs = [make_config("a"), make_config("b")]
    destination = FakeDestination(plan_rows=[])
    resolver = PipelineStateResolver(destination, layer="ingest")

    PipelineSelector(as_groups(configs), state=resolver).select(["state:failed"])

    assert len(destination.sql_calls) == 1


@pytest.mark.unit
def test_state_failed_with_no_plan_log_matches_nothing(dev_schema):
    config = make_config("a")
    destination = FakeDestination()
    destination.execute_sql = lambda sql, schema=None: (_ for _ in ()).throw(
        Exception("Not found: Table dlt_dev._saga_execution_plans")
    )
    resolver = PipelineStateResolver(destination, layer="ingest")

    assert resolver.last_run_failed(config) is False


@pytest.mark.unit
def test_state_failed_propagates_unexpected_query_failures(dev_schema):
    config = make_config("a")
    destination = FakeDestination()
    destination.execute_sql = lambda sql, schema=None: (_ for _ in ()).throw(
        PermissionError("access denied")
    )
    resolver = PipelineStateResolver(destination, layer="ingest")

    with pytest.raises(PermissionError):
        resolver.last_run_failed(config)


# ---------------------------------------------------------------------------
# Composition and error handling
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_state_composes_with_other_selectors():
    from dlt_saga.pipeline_config.base_config import ScheduleTag

    daily_new = make_config("daily_new")
    daily_new.tags = [ScheduleTag("daily")]
    daily_built = make_config("daily_built")
    daily_built.tags = [ScheduleTag("daily")]
    weekly_new = make_config("weekly_new")
    weekly_new.tags = [ScheduleTag("weekly")]

    configs = as_groups([daily_new, daily_built, weekly_new])
    destination = FakeDestination(tables={"dlt_filesystem": ["daily_built"]})

    def selector():
        return PipelineSelector(
            configs, state=PipelineStateResolver(destination, layer="ingest")
        )

    # INTERSECTION: daily AND new
    assert selected_names(selector().select(["tag:daily,state:new"])) == [
        "filesystem__daily_new"
    ]
    # UNION: daily OR new — the built daily pipeline still comes along
    assert selected_names(selector().select(["tag:daily state:new"])) == [
        "filesystem__daily_built",
        "filesystem__daily_new",
        "filesystem__weekly_new",
    ]


@pytest.mark.unit
def test_unknown_state_keyword_raises():
    selector = PipelineSelector({}, state=PipelineStateResolver(FakeDestination()))

    with pytest.raises(StateSelectorError, match="state:brand-new"):
        selector.select(["state:brand-new"])


@pytest.mark.unit
def test_state_selector_without_resolver_raises():
    with pytest.raises(StateSelectorError, match="warehouse state"):
        PipelineSelector({}).select(["state:new"])


@pytest.mark.unit
def test_build_state_resolver_skips_warehouse_for_stateless_selection():
    assert build_state_resolver(["tag:daily"], "ingest") is None
    assert build_state_resolver(None, None) is None


@pytest.mark.unit
def test_build_state_resolver_rejects_state_without_warehouse_access():
    with pytest.raises(StateSelectorError, match="does not connect"):
        build_state_resolver(["state:new"], None)


@pytest.mark.unit
def test_resolver_rejects_unknown_layer():
    with pytest.raises(ValueError, match="layer must be one of"):
        PipelineStateResolver(FakeDestination(), layer="ingestion")


# ---------------------------------------------------------------------------
# Command wiring
# ---------------------------------------------------------------------------


@pytest.mark.unit
@pytest.mark.parametrize(
    "resource_type, expected",
    [("all", "any"), ("ingest", "ingest"), ("historize", "historize")],
)
def test_resource_type_maps_to_state_layer(resource_type, expected):
    from dlt_saga.utility.cli.pipeline_state import layer_for_resource_type

    assert layer_for_resource_type(resource_type) == expected
