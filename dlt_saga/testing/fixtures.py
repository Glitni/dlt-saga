"""pytest fixtures for dlt_saga plugin authors.

To make these fixtures available in your test suite, add to your
``conftest.py``::

    pytest_plugins = ["dlt_saga.testing.fixtures"]

Available fixtures
------------------
``saga_duckdb_destination``
    Fresh in-memory :class:`~dlt_saga.destinations.duckdb.destination.DuckDBDestination`
    per test, closed automatically after each test.

``saga_clean_context``
    Resets the global dlt_saga execution context after each test to prevent
    state leakage between tests.

``saga_pipeline_config``
    Factory function (returns :func:`~dlt_saga.testing.make_pipeline_config`)
    so tests can create :class:`~dlt_saga.pipeline_config.base_config.PipelineConfig`
    objects with custom arguments.
"""

import pytest


@pytest.fixture
def saga_duckdb_destination():
    """Fresh in-memory DuckDB destination, closed after each test.

    Yields a :class:`~dlt_saga.destinations.duckdb.destination.DuckDBDestination`
    backed by an in-memory database. The connection is closed automatically
    when the test finishes.

    Example::

        def test_query(saga_duckdb_destination):
            saga_duckdb_destination.execute_sql(
                'CREATE SCHEMA IF NOT EXISTS "test"',
                dataset_name="test",
            )
            saga_duckdb_destination.execute_sql(
                'CREATE TABLE "test"."t" (id INTEGER)',
                dataset_name="test",
            )
            saga_duckdb_destination.execute_sql(
                'INSERT INTO "test"."t" VALUES (1)',
                dataset_name="test",
            )
            rows = saga_duckdb_destination.execute_sql(
                'SELECT * FROM "test"."t"'
            )
            assert rows[0][0] == 1
    """
    from dlt_saga.testing import make_destination

    dest = make_destination()
    yield dest
    dest.close()


@pytest.fixture
def saga_clean_context():
    """Reset the global dlt_saga execution context after the test.

    Use this fixture whenever your test sets or inspects the global execution
    context (e.g. via :func:`~dlt_saga.utility.cli.context.execution_context_scope`
    or :func:`~dlt_saga.utility.cli.context.set_execution_context`). Prevents
    state from leaking into subsequent tests.

    Example::

        def test_context_isolation(saga_clean_context):
            from dlt_saga.utility.cli.context import (
                get_execution_context,
                set_execution_context,
            )
            set_execution_context(None, full_refresh=True)
            assert get_execution_context().full_refresh is True
            # After the test, context is cleared automatically.
    """
    yield
    from dlt_saga.utility.cli.context import clear_execution_context

    clear_execution_context()


@pytest.fixture
def saga_pipeline_config():
    """Factory function for creating :class:`PipelineConfig` objects.

    Returns :func:`~dlt_saga.testing.make_pipeline_config` so tests can call
    it with arbitrary keyword arguments.

    Example::

        def test_tag_filtering(saga_pipeline_config):
            config = saga_pipeline_config(
                pipeline_group="api",
                table_name="events",
                tags=["daily", "critical"],
                config_dict={"api_key": "test"},
            )
            assert config.has_tag("daily")
            assert config.has_tag("critical")
            assert config.pipeline_name == "api__events"
            assert config.config_dict["api_key"] == "test"
    """
    from dlt_saga.testing import make_pipeline_config

    return make_pipeline_config
