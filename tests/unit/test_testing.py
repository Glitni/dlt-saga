"""Tests for the dlt_saga.testing utilities."""

import pytest

# ---------------------------------------------------------------------------
# make_pipeline_config
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestMakePipelineConfig:
    def test_defaults(self):
        from dlt_saga.testing import make_pipeline_config

        config = make_pipeline_config()
        assert config.pipeline_group == "test_group"
        assert config.pipeline_name == "test_group__test_table"
        assert config.table_name == "test_table"
        assert config.schema_name == "test"
        assert config.enabled is True
        assert config.tags == []
        assert config.config_dict["write_disposition"] == "append"
        assert config.config_dict["schema_name"] == "test"
        assert config.source_type == "test"

    def test_custom_group_and_table(self):
        from dlt_saga.testing import make_pipeline_config

        config = make_pipeline_config(pipeline_group="api", table_name="events")
        assert config.pipeline_group == "api"
        assert config.table_name == "events"
        assert config.pipeline_name == "api__events"

    def test_explicit_pipeline_name(self):
        from dlt_saga.testing import make_pipeline_config

        config = make_pipeline_config(
            pipeline_group="api",
            pipeline_name="api__custom_name",
            table_name="events",
        )
        assert config.pipeline_name == "api__custom_name"

    def test_custom_schema(self):
        from dlt_saga.testing import make_pipeline_config

        config = make_pipeline_config(schema_name="my_schema")
        assert config.schema_name == "my_schema"
        assert config.config_dict["schema_name"] == "my_schema"

    def test_config_dict_merged(self):
        from dlt_saga.testing import make_pipeline_config

        config = make_pipeline_config(
            config_dict={"api_key": "secret", "endpoint": "http://example.com"}
        )
        assert config.config_dict["api_key"] == "secret"
        assert config.config_dict["endpoint"] == "http://example.com"
        # Framework defaults are still present
        assert "schema_name" in config.config_dict
        assert "pipeline_name" in config.config_dict

    def test_config_dict_overrides_defaults(self):
        from dlt_saga.testing import make_pipeline_config

        # Explicit schema_name in config_dict wins over the positional default
        config = make_pipeline_config(
            schema_name="base",
            config_dict={"schema_name": "override"},
        )
        assert config.config_dict["schema_name"] == "override"

    def test_tags_parsed(self):
        from dlt_saga.testing import make_pipeline_config

        config = make_pipeline_config(tags=["daily", "critical"])
        assert config.has_tag("daily")
        assert config.has_tag("critical")
        assert not config.has_tag("hourly")

    def test_schedule_tag_with_values(self):
        from dlt_saga.testing import make_pipeline_config

        config = make_pipeline_config(tags=[{"hourly": [1, 10]}])
        assert config.has_tag("hourly", 1)
        assert config.has_tag("hourly", 10)
        assert not config.has_tag("hourly", 5)

    def test_write_disposition(self):
        from dlt_saga.testing import make_pipeline_config

        config = make_pipeline_config(write_disposition="merge")
        assert config.config_dict["write_disposition"] == "merge"
        assert config.ingest_enabled is True
        assert config.historize_enabled is False

    def test_historize_write_disposition(self):
        from dlt_saga.testing import make_pipeline_config

        config = make_pipeline_config(write_disposition="append+historize")
        assert config.ingest_enabled is True
        assert config.historize_enabled is True

    def test_disabled_config(self):
        from dlt_saga.testing import make_pipeline_config

        config = make_pipeline_config(enabled=False)
        assert config.enabled is False

    def test_adapter(self):
        from dlt_saga.testing import make_pipeline_config

        config = make_pipeline_config(adapter="dlt_saga.api.genesyscloud")
        assert config.adapter == "dlt_saga.api.genesyscloud"

    def test_identifier_is_synthetic_path(self):
        from dlt_saga.testing import make_pipeline_config

        config = make_pipeline_config(pipeline_group="sheets")
        assert config.identifier.startswith("test/sheets/")


# ---------------------------------------------------------------------------
# make_destination
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestMakeDestination:
    def test_creates_duckdb_destination(self):
        from dlt_saga.destinations.duckdb.destination import DuckDBDestination
        from dlt_saga.testing import make_destination

        dest = make_destination()
        try:
            assert isinstance(dest, DuckDBDestination)
        finally:
            dest.close()

    def test_default_is_in_memory(self):
        from dlt_saga.testing import make_destination

        dest = make_destination()
        try:
            assert dest.config.database_path == ":memory:"
        finally:
            dest.close()

    def test_custom_schema_name(self):
        from dlt_saga.testing import make_destination

        dest = make_destination(schema_name="my_schema")
        try:
            assert dest.config.schema_name == "my_schema"
        finally:
            dest.close()

    def test_unsupported_type_raises(self):
        from dlt_saga.testing import make_destination

        with pytest.raises(ValueError, match="Unsupported test destination type"):
            make_destination(destination_type="bigquery")

    def test_can_execute_sql(self):
        from dlt_saga.testing import make_destination

        dest = make_destination()
        try:
            dest.execute_sql(
                'CREATE SCHEMA IF NOT EXISTS "test"',
                dataset_name="test",
            )
            dest.execute_sql(
                'CREATE TABLE "test"."t" (id INTEGER)',
                dataset_name="test",
            )
            dest.execute_sql('INSERT INTO "test"."t" VALUES (42)', dataset_name="test")
            rows = dest.execute_sql('SELECT id FROM "test"."t"')
            assert len(rows) == 1
            assert rows[0][0] == 42
        finally:
            dest.close()

    def test_separate_instances_are_independent(self):
        """Two :memory: destinations should not share state."""
        from dlt_saga.testing import make_destination

        dest1 = make_destination()
        dest2 = make_destination()
        try:
            dest1.execute_sql(
                'CREATE SCHEMA IF NOT EXISTS "s"; CREATE TABLE "s"."only_in_dest1" (x INT)',
                dataset_name="s",
            )
            result = dest2.execute_sql(
                """
                SELECT count(*) AS cnt
                FROM information_schema.tables
                WHERE table_name = 'only_in_dest1'
                """
            )
            assert result[0][0] == 0
        finally:
            dest1.close()
            dest2.close()


# ---------------------------------------------------------------------------
# run_pipeline_test
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestRunPipelineTest:
    """run_pipeline_test is integration-adjacent but uses only DuckDB."""

    def _make_simple_pipeline(self, rows=None):
        """Return a BasePipeline subclass that yields fixed rows."""
        import dlt

        from dlt_saga.pipelines.base_pipeline import BasePipeline

        _rows = rows or [{"id": 1, "name": "Alice"}, {"id": 2, "name": "Bob"}]

        class SimplePipeline(BasePipeline):
            def extract_data(self):
                @dlt.resource(name=self.table_name)
                def _data():
                    yield from _rows

                return [(_data(), "test data")]

        return SimplePipeline

    def test_successful_pipeline_returns_success(self):
        from dlt_saga.testing import run_pipeline_test

        pipeline_class = self._make_simple_pipeline()
        result = run_pipeline_test(
            pipeline_class,
            config_dict={
                "schema_name": "test",
                "table_name": "people",
                "pipeline_name": "test__people",
            },
        )
        assert result.success is True
        assert result.error is None
        assert result.pipeline_name == "test__people"
        assert result.load_info is not None

    def test_failing_pipeline_returns_failure(self):
        from dlt_saga.pipelines.base_pipeline import BasePipeline
        from dlt_saga.testing import run_pipeline_test

        class BrokenPipeline(BasePipeline):
            def extract_data(self):
                raise RuntimeError("intentional test failure")

        result = run_pipeline_test(
            BrokenPipeline,
            config_dict={
                "schema_name": "test",
                "table_name": "broken",
                "pipeline_name": "test__broken",
            },
        )
        assert result.success is False
        assert "intentional test failure" in result.error
        assert result.load_info is None

    def test_with_pipeline_config(self):
        from dlt_saga.testing import make_pipeline_config, run_pipeline_test

        pipeline_class = self._make_simple_pipeline()
        config = make_pipeline_config(
            pipeline_group="api",
            table_name="items",
        )
        result = run_pipeline_test(pipeline_class, pipeline_config=config)
        assert result.success is True
        assert result.pipeline_name == "api__items"

    def test_config_dict_overrides_pipeline_config(self):
        """config_dict values win over pipeline_config.config_dict values."""
        from dlt_saga.testing import make_pipeline_config, run_pipeline_test

        pipeline_class = self._make_simple_pipeline()
        base_config = make_pipeline_config(
            table_name="original",
            config_dict={"custom_key": "base_value"},
        )
        result = run_pipeline_test(
            pipeline_class,
            config_dict={"custom_key": "overridden"},
            pipeline_config=base_config,
        )
        # Pipeline runs successfully (table_name etc. come from base_config)
        assert result.success is True

    def test_defaults_generated_when_no_config_provided(self):
        from dlt_saga.testing import run_pipeline_test

        pipeline_class = self._make_simple_pipeline()
        # No config_dict, no pipeline_config — should use make_pipeline_config defaults
        result = run_pipeline_test(pipeline_class)
        assert result.success is True
        assert result.pipeline_name == "test_group__test_table"

    def test_load_info_is_list_of_dicts(self):
        from dlt_saga.testing import run_pipeline_test

        pipeline_class = self._make_simple_pipeline()
        result = run_pipeline_test(
            pipeline_class,
            config_dict={
                "schema_name": "test",
                "table_name": "rows",
                "pipeline_name": "test__rows",
            },
        )
        assert result.success
        assert isinstance(result.load_info, list)
        assert len(result.load_info) >= 1
        assert isinstance(result.load_info[0], dict)


# ---------------------------------------------------------------------------
# pytest fixtures (smoke tests — just verify they are importable/callable)
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestFixturesModule:
    def test_fixtures_module_importable(self):
        import dlt_saga.testing.fixtures as fixtures_module

        assert callable(getattr(fixtures_module, "saga_duckdb_destination", None))
        assert callable(getattr(fixtures_module, "saga_clean_context", None))
        assert callable(getattr(fixtures_module, "saga_pipeline_config", None))
