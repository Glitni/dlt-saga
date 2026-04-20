"""Testing utilities for dlt_saga plugin authors.

Provides factory functions and helpers for testing pipeline implementations
against an in-memory DuckDB destination — no cloud credentials required.

Example::

    from dlt_saga.testing import make_pipeline_config, make_destination, run_pipeline_test

    # Test a BasePipeline subclass end-to-end
    result = run_pipeline_test(
        MyPipeline,
        config_dict={"api_key": "test", "endpoint": "https://api.example.com"},
    )
    assert result.success

    # Build a PipelineConfig for unit-testing discovery/selector logic
    config = make_pipeline_config(
        pipeline_group="api",
        tags=["daily"],
        config_dict={"api_key": "test"},
    )
    assert config.has_tag("daily")

    # Create a standalone DuckDB destination for SQL-level tests
    dest = make_destination()
    try:
        dest.execute_sql('CREATE TABLE "test"."t" (id INT)', dataset_name="test")
    finally:
        dest.close()

Pytest fixtures are available separately::

    # In your conftest.py:
    pytest_plugins = ["dlt_saga.testing.fixtures"]
"""

from typing import Any, Dict, List, Optional, Type, Union

from dlt_saga.pipeline_config.base_config import (
    PipelineConfig,
    ScheduleTag,
    parse_tag,
)

__all__ = [
    "make_pipeline_config",
    "make_destination",
    "run_pipeline_test",
]


def make_pipeline_config(
    pipeline_group: str = "test_group",
    pipeline_name: Optional[str] = None,
    table_name: Optional[str] = None,
    schema_name: str = "test",
    config_dict: Optional[Dict[str, Any]] = None,
    tags: Optional[List[Union[str, Dict]]] = None,
    adapter: Optional[str] = None,
    write_disposition: str = "append",
    enabled: bool = True,
) -> PipelineConfig:
    """Create a :class:`PipelineConfig` with sensible testing defaults.

    Args:
        pipeline_group: Pipeline group name (e.g. ``"api"``, ``"filesystem"``).
        pipeline_name: Full pipeline name. Defaults to
            ``"<group>__<table_name>"``.
        table_name: Table name in the destination. Defaults to
            ``"test_table"``.
        schema_name: Schema/dataset to write to. Defaults to ``"test"``.
        config_dict: Additional config key/values merged on top of the base
            config. Keys here override auto-derived defaults.
        tags: Tag strings or dicts (e.g. ``["daily", {"hourly": [1, 10]}]``).
        adapter: Optional explicit pipeline adapter path
            (e.g. ``"dlt_saga.api.genesyscloud"``).
        write_disposition: dlt write disposition. Defaults to ``"append"``.
        enabled: Whether the pipeline is enabled. Defaults to ``True``.

    Returns:
        A :class:`PipelineConfig` ready to pass to :func:`run_pipeline_test`
        or the ``Session`` API.

    Example::

        config = make_pipeline_config(
            pipeline_group="api",
            table_name="events",
            tags=["daily"],
            config_dict={"api_key": "test_key"},
        )
        assert config.pipeline_name == "api__events"
        assert config.has_tag("daily")
        assert config.config_dict["api_key"] == "test_key"
    """
    effective_table = table_name or "test_table"
    effective_name = pipeline_name or f"{pipeline_group}__{effective_table}"

    base: Dict[str, Any] = {
        "schema_name": schema_name,
        "table_name": effective_table,
        "pipeline_name": effective_name,
        "write_disposition": write_disposition,
    }
    if config_dict:
        base.update(config_dict)

    parsed_tags: List[ScheduleTag] = [parse_tag(t) for t in (tags or [])]

    return PipelineConfig(
        pipeline_group=pipeline_group,
        pipeline_name=effective_name,
        table_name=effective_table,
        identifier=f"test/{pipeline_group}/test.yml",
        config_dict=base,
        enabled=enabled,
        tags=parsed_tags,
        adapter=adapter,
        schema_name=schema_name,
        source_type="test",
    )


def make_destination(
    destination_type: str = "duckdb",
    database_path: str = ":memory:",
    schema_name: str = "test",
):
    """Create a pre-configured destination for local testing.

    Args:
        destination_type: Destination type. Currently only ``"duckdb"`` is
            supported.
        database_path: DuckDB database path. Use ``":memory:"`` (default) for
            a fresh in-process database that is discarded after the test.
            Pass a file path for a persistent test database.
        schema_name: Schema/dataset name used for table ID construction.
            Defaults to ``"test"``.

    Returns:
        A configured :class:`~dlt_saga.destinations.duckdb.destination.DuckDBDestination`
        instance.

    Note:
        Call ``destination.close()`` when done to release the connection, or
        use the :func:`saga_duckdb_destination <dlt_saga.testing.fixtures>`
        pytest fixture which handles cleanup automatically.

    Raises:
        ValueError: If ``destination_type`` is not ``"duckdb"``.
    """
    if destination_type != "duckdb":
        raise ValueError(
            f"Unsupported test destination type: {destination_type!r}. "
            "Currently only 'duckdb' is supported."
        )

    from dlt_saga.destinations.duckdb.config import DuckDBDestinationConfig
    from dlt_saga.destinations.duckdb.destination import DuckDBDestination

    config = DuckDBDestinationConfig(
        database_path=database_path,
        schema_name=schema_name,
        project_id="test",
    )
    return DuckDBDestination(config)


def run_pipeline_test(
    pipeline_class: Type,
    config_dict: Optional[Dict[str, Any]] = None,
    *,
    pipeline_config: Optional[PipelineConfig] = None,
    schema_name: str = "test",
    full_refresh: bool = False,
    database_path: str = ":memory:",
):
    """Run a :class:`~dlt_saga.pipelines.base_pipeline.BasePipeline` subclass
    against an in-memory DuckDB destination and return a structured result.

    Sets up a temporary execution context pointing at DuckDB, instantiates
    *pipeline_class*, calls ``.run()``, and returns a
    :class:`~dlt_saga.session.PipelineResult`. No cloud credentials are
    needed.

    Args:
        pipeline_class: A :class:`~dlt_saga.pipelines.base_pipeline.BasePipeline`
            subclass to test.
        config_dict: Pipeline-specific config dict (API keys, endpoints, …).
            Required fields ``schema_name``, ``table_name``, and
            ``pipeline_name`` are filled from *pipeline_config* or defaults
            when omitted. Values in *config_dict* override everything else.
        pipeline_config: Pre-built :class:`PipelineConfig`. When provided,
            its ``config_dict`` seeds the run and *config_dict* is merged on
            top. When omitted a config with testing defaults is generated.
        schema_name: DuckDB schema to write to. Defaults to ``"test"``.
        full_refresh: Run in full-refresh mode (drops pipeline state before
            running). Defaults to ``False``.
        database_path: DuckDB file path or ``":memory:"`` (default). Pass the
            same path across multiple calls to share state between runs.

    Returns:
        A :class:`~dlt_saga.session.PipelineResult` with ``success``,
        ``error``, and ``load_info`` attributes.

    Example::

        import dlt
        from dlt_saga.pipelines.base_pipeline import BasePipeline
        from dlt_saga.testing import run_pipeline_test

        class MyPipeline(BasePipeline):
            def extract_data(self):
                @dlt.resource(name=self.table_name)
                def _rows():
                    yield {"id": 1, "name": "Alice"}
                return [(_rows(), "people")]

        result = run_pipeline_test(
            MyPipeline,
            config_dict={"schema_name": "test", "table_name": "people",
                         "pipeline_name": "test__people"},
        )
        assert result.success
        assert result.load_info  # list of dlt load-info dicts
    """
    from dlt_saga.session import PipelineResult
    from dlt_saga.utility.cli.context import execution_context_scope
    from dlt_saga.utility.cli.profiles import ProfileTarget

    # Resolve effective config dict
    if pipeline_config is not None:
        effective_dict: Dict[str, Any] = dict(pipeline_config.config_dict)
        if config_dict:
            effective_dict.update(config_dict)
        pipeline_name = pipeline_config.pipeline_name
    else:
        default_config = make_pipeline_config(
            schema_name=schema_name,
            config_dict=config_dict,
        )
        effective_dict = dict(default_config.config_dict)
        # config_dict may override pipeline_name inside the config dict; use that
        # value if present so PipelineResult.pipeline_name matches what the user set.
        pipeline_name = effective_dict.get(
            "pipeline_name", default_config.pipeline_name
        )

    # Build a DuckDB ProfileTarget for the test execution context
    profile_target = ProfileTarget(
        name="test",
        environment="dev",
        destination_type="duckdb",
        schema=schema_name,
        destination_config={"database_path": database_path},
    )

    with execution_context_scope(profile_target, full_refresh=full_refresh):
        try:
            pipeline = pipeline_class(effective_dict)
            load_info = pipeline.run()
            return PipelineResult(
                pipeline_name=pipeline_name,
                success=True,
                load_info=load_info,
            )
        except Exception as exc:
            return PipelineResult(
                pipeline_name=pipeline_name,
                success=False,
                error=str(exc),
            )
