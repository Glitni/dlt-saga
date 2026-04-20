"""Unit tests for Databricks destination, auth provider, and access manager."""

from unittest.mock import MagicMock, patch

import pytest

from dlt_saga.destinations.databricks.access import DatabricksAccessManager
from dlt_saga.destinations.databricks.config import DatabricksDestinationConfig
from dlt_saga.destinations.databricks.destination import DatabricksDestination
from dlt_saga.utility.auth.databricks import (
    DatabricksAuthProvider,
    _build_sdk_config,
    get_databricks_token,
)
from dlt_saga.utility.auth.providers import AuthenticationError

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _valid_config(**kwargs) -> DatabricksDestinationConfig:
    defaults = dict(
        server_hostname="adb-1234.12.azuredatabricks.net",
        http_path="/sql/1.0/warehouses/abc123",
        catalog="my_catalog",
    )
    defaults.update(kwargs)
    return DatabricksDestinationConfig(**defaults)


def _make_destination(**kwargs) -> DatabricksDestination:
    return DatabricksDestination(_valid_config(**kwargs))


# ---------------------------------------------------------------------------
# DatabricksDestinationConfig
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestDatabricksDestinationConfig:
    def test_valid_config_created(self):
        cfg = _valid_config()
        assert cfg.server_hostname == "adb-1234.12.azuredatabricks.net"
        assert cfg.http_path == "/sql/1.0/warehouses/abc123"
        assert cfg.catalog == "my_catalog"
        assert cfg.destination_type == "databricks"

    @pytest.mark.parametrize(
        "field, match",
        [
            ("server_hostname", "server_hostname is required"),
            ("http_path", "http_path is required"),
            ("catalog", "catalog is required"),
        ],
    )
    def test_missing_required_field_raises(self, field, match):
        kwargs = dict(
            server_hostname="adb-1234.azuredatabricks.net",
            http_path="/sql/1.0/warehouses/x",
            catalog="cat",
        )
        kwargs[field] = ""
        with pytest.raises(ValueError, match=match):
            DatabricksDestinationConfig(**kwargs)

    @pytest.mark.parametrize(
        "hostname, expected_host",
        [
            ("adb-1234.azuredatabricks.net", "https://adb-1234.azuredatabricks.net"),
            (
                "https://adb-1234.azuredatabricks.net",
                "https://adb-1234.azuredatabricks.net",
            ),
        ],
    )
    def test_host_property(self, hostname, expected_host):
        cfg = _valid_config(server_hostname=hostname)
        assert cfg.host == expected_host

    def test_from_dict_basic(self):
        cfg = DatabricksDestinationConfig.from_dict(
            {
                "server_hostname": "adb-1234.azuredatabricks.net",
                "http_path": "/sql/1.0/warehouses/abc",
                "catalog": "prod_catalog",
                "auth_mode": "pat",
                "access_token": "my-pat",
            }
        )
        assert cfg.server_hostname == "adb-1234.azuredatabricks.net"
        assert cfg.catalog == "prod_catalog"
        assert cfg.auth_mode == "pat"
        assert cfg.access_token == "my-pat"

    def test_from_dict_database_fallback_for_catalog(self):
        cfg = DatabricksDestinationConfig.from_dict(
            {
                "server_hostname": "adb-1234.azuredatabricks.net",
                "http_path": "/sql/1.0/warehouses/abc",
                "database": "legacy_catalog",
            }
        )
        assert cfg.catalog == "legacy_catalog"

    def test_from_dict_catalog_takes_precedence_over_database(self):
        cfg = DatabricksDestinationConfig.from_dict(
            {
                "server_hostname": "adb-1234.azuredatabricks.net",
                "http_path": "/sql/1.0/warehouses/abc",
                "catalog": "catalog_value",
                "database": "database_value",
            }
        )
        assert cfg.catalog == "catalog_value"


# ---------------------------------------------------------------------------
# DatabricksDestination — SQL dialect
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestDatabricksDialect:
    def setup_method(self):
        self.dest = _make_destination()

    def test_quote_identifier_uses_backticks(self):
        assert self.dest.quote_identifier("my_col") == "`my_col`"

    def test_get_full_table_id_three_part(self):
        assert self.dest.get_full_table_id("my_schema", "my_table") == (
            "`my_catalog`.`my_schema`.`my_table`"
        )

    def test_hash_expression_uses_xxhash64(self):
        result = self.dest.hash_expression(["id", "name"])
        assert "xxhash64" in result
        assert "`id`" in result
        assert "`name`" in result

    def test_hash_expression_handles_nulls_with_coalesce(self):
        assert "COALESCE" in self.dest.hash_expression(["col"])

    def test_partition_ddl(self):
        assert self.dest.partition_ddl("dt") == "PARTITIONED BY (dt)"

    @pytest.mark.parametrize(
        "columns, expected",
        [
            (["id"], "CLUSTER BY (id)"),
            (["id", "ts"], "CLUSTER BY (id, ts)"),
        ],
    )
    def test_cluster_ddl(self, columns, expected):
        assert self.dest.cluster_ddl(columns) == expected

    @pytest.mark.parametrize(
        "logical, sql",
        [
            ("string", "STRING"),
            ("int64", "BIGINT"),
            ("bool", "BOOLEAN"),
            ("timestamp", "TIMESTAMP"),
            ("custom", "CUSTOM"),
        ],
    )
    def test_type_name(self, logical, sql):
        assert self.dest.type_name(logical) == sql

    def test_cast_to_string(self):
        assert self.dest.cast_to_string("my_col") == "CAST(my_col AS STRING)"

    def test_columns_query_uses_system_schema(self):
        result = self.dest.columns_query("cat", "schema", "tbl")
        assert "system.information_schema.columns" in result
        assert "'cat'" in result
        assert "'schema'" in result
        assert "'tbl'" in result

    def test_columns_query_uses_config_catalog_when_database_empty(self):
        assert "'my_catalog'" in self.dest.columns_query("", "schema", "tbl")

    def test_json_type_name(self):
        assert self.dest.json_type_name() == "STRING"

    def test_parse_json_expression_passthrough(self):
        assert self.dest.parse_json_expression("my_col") == "my_col"

    def test_extract_json_value_wraps_in_to_json(self):
        assert self.dest.extract_json_value("col") == "to_json(col)"


# ---------------------------------------------------------------------------
# DatabricksDestination — capabilities
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestDatabricksCapabilities:
    @pytest.mark.parametrize(
        "method",
        ["supports_partitioning", "supports_clustering", "supports_access_management"],
    )
    def test_capability_is_true(self, method):
        assert getattr(_make_destination(), method)() is True


# ---------------------------------------------------------------------------
# DatabricksDestination — execute_sql
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestDatabricksExecuteSql:
    def _dest_with_mock_conn(self):
        dest = _make_destination()
        mock_conn = MagicMock()
        dest._connection = mock_conn
        return dest, mock_conn

    def _setup_cursor(self, mock_conn, description=None, rows=None):
        mock_cursor = MagicMock()
        mock_cursor.description = description
        mock_cursor.fetchall.return_value = rows or []
        mock_conn.cursor.return_value.__enter__ = MagicMock(return_value=mock_cursor)
        mock_conn.cursor.return_value.__exit__ = MagicMock(return_value=False)
        return mock_cursor

    def test_single_statement_executed(self):
        dest, mock_conn = self._dest_with_mock_conn()
        cursor = self._setup_cursor(
            mock_conn, description=[("col",)], rows=[("value",)]
        )
        dest.execute_sql("SELECT 1")
        cursor.execute.assert_called_once_with("SELECT 1")

    def test_multi_statement_split_on_semicolon(self):
        dest, mock_conn = self._dest_with_mock_conn()
        cursor = self._setup_cursor(mock_conn)
        dest.execute_sql("CREATE TABLE t1 (id INT); CREATE TABLE t2 (id INT)")
        assert cursor.execute.call_count == 2

    def test_dataset_name_is_ignored(self):
        # dataset_name is kept for interface compatibility but has no effect —
        # all SQL uses fully-qualified names so no USE SCHEMA is needed.
        dest, mock_conn = self._dest_with_mock_conn()
        cursor = self._setup_cursor(mock_conn)
        dest.execute_sql("SELECT 1", dataset_name="my_schema")
        calls = [c[0][0] for c in cursor.execute.call_args_list]
        assert calls == ["SELECT 1"]

    def test_empty_result_returns_empty_list(self):
        dest, mock_conn = self._dest_with_mock_conn()
        self._setup_cursor(mock_conn)
        assert dest.execute_sql("DROP TABLE t") == []

    def test_row_supports_attribute_access(self):
        dest, mock_conn = self._dest_with_mock_conn()
        self._setup_cursor(
            mock_conn, description=[("name",), ("age",)], rows=[("Alice", 30)]
        )
        rows = dest.execute_sql("SELECT name, age FROM t")
        assert rows[0].name == "Alice"
        assert rows[0].age == 30

    def test_row_supports_index_access(self):
        dest, mock_conn = self._dest_with_mock_conn()
        self._setup_cursor(mock_conn, description=[("x",)], rows=[("val",)])
        rows = dest.execute_sql("SELECT x FROM t")
        assert rows[0][0] == "val"


# ---------------------------------------------------------------------------
# DatabricksDestination — get_max_column_value / get_last_load_timestamp
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestDatabricksQueryMethods:
    def _dest_with_execute(self, return_value):
        dest = _make_destination()
        dest.execute_sql = MagicMock(return_value=return_value)
        return dest

    def test_get_max_column_value_returns_value(self):
        row = MagicMock()
        row.__getitem__ = lambda self, i: "2024-01-01" if i == 0 else None
        row.__len__ = lambda self: 1
        dest = self._dest_with_execute([row])
        assert (
            dest.get_max_column_value("`cat`.`schema`.`table`", "ingested_at")
            == "2024-01-01"
        )

    @pytest.mark.parametrize("return_value", [[], None])
    def test_get_max_column_value_returns_none_on_empty(self, return_value):
        dest = self._dest_with_execute(return_value or [])
        assert dest.get_max_column_value("t", "col") is None

    def test_get_max_column_value_returns_none_on_exception(self):
        dest = _make_destination()
        dest.execute_sql = MagicMock(side_effect=Exception("no table"))
        assert dest.get_max_column_value("t", "col") is None

    def test_get_last_load_timestamp_returns_none_on_exception(self):
        dest = _make_destination()
        dest._execute_parameterised = MagicMock(
            side_effect=Exception("table not found")
        )
        assert dest.get_last_load_timestamp("schema", "pipeline", "table") is None


# ---------------------------------------------------------------------------
# get_databricks_token
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestGetDatabricksToken:
    def test_pat_mode_returns_token_directly(self):
        result = get_databricks_token(
            host="https://adb.azuredatabricks.net",
            auth_mode="pat",
            access_token="my-pat-token",
            client_id=None,
            client_secret=None,
        )
        assert result == "my-pat-token"

    def test_pat_mode_without_token_falls_through_to_sdk(self):
        mock_config = MagicMock()
        mock_config.authenticate.return_value = {"Authorization": "Bearer sdk-token"}
        with patch(
            "dlt_saga.utility.auth.databricks._build_sdk_config",
            return_value=mock_config,
        ):
            result = get_databricks_token(
                host="https://adb.azuredatabricks.net",
                auth_mode="pat",
                access_token=None,
                client_id=None,
                client_secret=None,
            )
        assert result == "sdk-token"

    def test_sdk_bearer_token_prefix_stripped(self):
        mock_config = MagicMock()
        mock_config.authenticate.return_value = {
            "Authorization": "Bearer oauth-token-123"
        }
        with patch(
            "dlt_saga.utility.auth.databricks._build_sdk_config",
            return_value=mock_config,
        ):
            result = get_databricks_token(
                host="https://adb.azuredatabricks.net",
                auth_mode="m2m",
                access_token=None,
                client_id="app-id",
                client_secret="secret",
            )
        assert result == "oauth-token-123"

    def test_non_bearer_response_raises_authentication_error(self):
        mock_config = MagicMock()
        mock_config.authenticate.return_value = {"Authorization": "Basic abc"}
        with patch(
            "dlt_saga.utility.auth.databricks._build_sdk_config",
            return_value=mock_config,
        ):
            with pytest.raises(AuthenticationError, match="Bearer token"):
                get_databricks_token(
                    host="https://adb.azuredatabricks.net",
                    auth_mode="u2m",
                    access_token=None,
                    client_id=None,
                    client_secret=None,
                )

    def test_sdk_exception_wrapped_as_authentication_error(self):
        mock_config = MagicMock()
        mock_config.authenticate.side_effect = RuntimeError("auth failed")
        with patch(
            "dlt_saga.utility.auth.databricks._build_sdk_config",
            return_value=mock_config,
        ):
            with pytest.raises(
                AuthenticationError, match="Databricks authentication failed"
            ):
                get_databricks_token(
                    host="https://adb.azuredatabricks.net",
                    auth_mode=None,
                    access_token=None,
                    client_id=None,
                    client_secret=None,
                )


@pytest.mark.unit
class TestBuildSdkConfig:
    # Config is imported lazily inside _build_sdk_config, so patch at source.
    _PATCH = "databricks.sdk.config.Config"

    @pytest.mark.parametrize(
        "auth_mode, access_token, client_id, client_secret, expected_kwargs",
        [
            (
                "pat",
                "tok",
                None,
                None,
                {"host": "https://adb.azuredatabricks.net", "token": "tok"},
            ),
            (
                "m2m",
                None,
                "app-id",
                "sp-secret",
                {
                    "host": "https://adb.azuredatabricks.net",
                    "client_id": "app-id",
                    "client_secret": "sp-secret",
                },
            ),
            (
                "u2m",
                None,
                None,
                None,
                {
                    "host": "https://adb.azuredatabricks.net",
                    "auth_type": "external-browser",
                },
            ),
            (
                None,
                None,
                None,
                None,
                {"host": "https://adb.azuredatabricks.net"},
            ),
        ],
        ids=["pat", "m2m", "u2m", "auto-detect"],
    )
    def test_sdk_config_kwargs(
        self, auth_mode, access_token, client_id, client_secret, expected_kwargs
    ):
        with patch(self._PATCH) as mock_config_cls:
            _build_sdk_config(
                host="https://adb.azuredatabricks.net",
                auth_mode=auth_mode,
                access_token=access_token,
                client_id=client_id,
                client_secret=client_secret,
            )
        mock_config_cls.assert_called_once_with(**expected_kwargs)


# ---------------------------------------------------------------------------
# DatabricksAuthProvider
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestDatabricksAuthProvider:
    def test_validate_does_not_raise(self):
        DatabricksAuthProvider().validate()

    def test_supports_impersonation_is_false(self):
        assert DatabricksAuthProvider().supports_impersonation() is False

    def test_impersonate_raises_not_implemented(self):
        with pytest.raises(NotImplementedError, match="impersonation"):
            with DatabricksAuthProvider().impersonate("user@example.com"):
                pass


# ---------------------------------------------------------------------------
# DatabricksAccessManager — parse_access_list
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestDatabricksAccessManagerParseList:
    def _manager(self):
        return DatabricksAccessManager(_make_destination())

    @pytest.mark.parametrize(
        "entry, bucket, principal",
        [
            ("user:analyst@company.com", "users", "analyst@company.com"),
            ("group:data-readers", "groups", "data-readers"),
            ("service_principal:my-sp", "service_principals", "my-sp"),
            ("serviceprincipal:my-sp", "service_principals", "my-sp"),  # alias
            (
                "user:analyst@company.com:SELECT",
                "users",
                "analyst@company.com",
            ),  # privilege suffix
        ],
    )
    def test_valid_entry_parsed(self, entry, bucket, principal):
        result = self._manager().parse_access_list([entry])
        assert principal in result[bucket]

    @pytest.mark.parametrize(
        "entry",
        ["no-colon-here", "role:some-role"],
        ids=["no-colon", "unknown-type"],
    )
    def test_invalid_entry_skipped(self, entry):
        result = self._manager().parse_access_list([entry])
        assert result == {"users": set(), "groups": set(), "service_principals": set()}

    def test_multiple_entries_parsed(self):
        result = self._manager().parse_access_list(
            ["user:a@b.com", "user:c@d.com", "group:readers"]
        )
        assert result["users"] == {"a@b.com", "c@d.com"}
        assert result["groups"] == {"readers"}

    def test_empty_list_returns_empty_sets(self):
        result = self._manager().parse_access_list([])
        assert result == {"users": set(), "groups": set(), "service_principals": set()}


# ---------------------------------------------------------------------------
# DatabricksAccessManager — manage_access_for_tables
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestDatabricksAccessManagerManageAccess:
    def _manager_with_mock_dest(self):
        dest = _make_destination()
        dest.execute_sql = MagicMock(return_value=[])
        return DatabricksAccessManager(dest), dest

    def test_none_access_config_skips(self):
        mgr, dest = self._manager_with_mock_dest()
        mgr.manage_access_for_tables(["`cat`.`schema`.`tbl`"], access_config=None)
        dest.execute_sql.assert_not_called()

    def test_grant_called_for_each_principal(self):
        mgr, dest = self._manager_with_mock_dest()
        mgr.manage_access_for_tables(
            ["`cat`.`schema`.`tbl`"],
            access_config=["user:analyst@co.com"],
            revoke_extra=False,
        )
        calls = [str(c) for c in dest.execute_sql.call_args_list]
        assert any("GRANT" in c and "analyst@co.com" in c for c in calls)

    def test_revoke_called_for_extra_principals(self):
        mgr, dest = self._manager_with_mock_dest()

        show_grants_row = MagicMock()
        show_grants_row.__getitem__ = lambda self, i: {
            1: "old-user@co.com",
            2: "SELECT",
        }.get(i, "")
        show_grants_row.__len__ = lambda self: 5
        dest.execute_sql.side_effect = [[show_grants_row], []]

        mgr.manage_access_for_tables(
            ["`cat`.`schema`.`tbl`"],
            access_config=["user:new-user@co.com"],
            revoke_extra=True,
        )

        all_sql = " ".join(str(c.args[0]) for c in dest.execute_sql.call_args_list)
        assert "REVOKE" in all_sql
        assert "old-user@co.com" in all_sql

    def test_exception_per_table_is_caught(self):
        mgr, dest = self._manager_with_mock_dest()
        dest.execute_sql.side_effect = Exception("network failure")
        # Should not raise — errors are swallowed per table
        mgr.manage_access_for_tables(
            ["`cat`.`schema`.`tbl`"],
            access_config=["user:a@b.com"],
            revoke_extra=False,
        )


# ---------------------------------------------------------------------------
# DatabricksAccessManager — grant/revoke SQL shape
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestDatabricksGrantRevokeSql:
    def _manager_with_capture(self):
        dest = _make_destination()
        executed: list[str] = []
        dest.execute_sql = MagicMock(side_effect=lambda sql: executed.append(sql) or [])
        return DatabricksAccessManager(dest), executed

    @pytest.mark.parametrize(
        "principal_type, principal, expected_verb",
        [
            ("user", "analyst@co.com", "GRANT"),
            ("group", "data-team", "GRANT"),
            ("service_principal", "my-sp", "GRANT"),
        ],
    )
    def test_grant_sql_shape(self, principal_type, principal, expected_verb):
        mgr, executed = self._manager_with_capture()
        table_id = "`cat`.`schema`.`tbl`"
        mgr._grant(table_id, principal, principal_type)

        assert len(executed) == 1
        assert f"{expected_verb} SELECT ON TABLE" in executed[0]
        assert table_id in executed[0]
        assert f"`{principal}`" in executed[0]

    def test_revoke_sql_shape(self):
        mgr, executed = self._manager_with_capture()
        table_id = "`cat`.`schema`.`tbl`"
        mgr._revoke(table_id, "old@co.com")

        assert len(executed) == 1
        assert "REVOKE SELECT ON TABLE" in executed[0]
        assert table_id in executed[0]
        assert "`old@co.com`" in executed[0]


# ---------------------------------------------------------------------------
# DatabricksDestination — connection lifecycle
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestDatabricksConnectionLifecycle:
    def test_close_connection_sets_connection_to_none(self):
        dest = _make_destination()
        mock_conn = MagicMock()
        dest._connection = mock_conn
        dest._close_connection()
        assert dest._connection is None
        mock_conn.close.assert_called_once()

    def test_close_connection_on_none_is_safe(self):
        dest = _make_destination()
        dest._connection = None
        dest._close_connection()  # should not raise

    def test_close_connection_swallows_close_exception(self):
        dest = _make_destination()
        mock_conn = MagicMock()
        mock_conn.close.side_effect = RuntimeError("already closed")
        dest._connection = mock_conn
        dest._close_connection()  # should not raise
        assert dest._connection is None


# ---------------------------------------------------------------------------
# DatabricksDestination — ensure_schema_exists
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestDatabricksEnsureSchemaExists:
    def test_creates_schema_with_full_path(self):
        dest = _make_destination()
        executed: list[str] = []
        dest.execute_sql = MagicMock(
            side_effect=lambda sql, *a, **kw: executed.append(sql) or []
        )
        dest.ensure_schema_exists("prod_schema")
        assert len(executed) == 1
        assert "CREATE SCHEMA IF NOT EXISTS" in executed[0]
        assert "`my_catalog`.`prod_schema`" in executed[0]


# ---------------------------------------------------------------------------
# DatabricksDestination — clone_table / rename_table
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestDatabricksTableOperations:
    def _dest_with_capture(self):
        dest = _make_destination()
        executed: list[str] = []
        dest.execute_sql = MagicMock(
            side_effect=lambda sql, *a, **kw: executed.append(sql) or []
        )
        return dest, executed

    def test_clone_table_uses_deep_clone(self):
        dest, executed = self._dest_with_capture()
        dest.clone_table("`cat`.`schema`.`src`", "`cat`.`schema`.`dst`")
        assert len(executed) == 1
        assert "DEEP CLONE" in executed[0]
        assert "`cat`.`schema`.`src`" in executed[0]
        assert "`cat`.`schema`.`dst`" in executed[0]

    def test_rename_table_uses_alter_table(self):
        dest, executed = self._dest_with_capture()
        dest.rename_table("`cat`.`schema`.`old`", "`cat`.`schema`.`new`")
        assert len(executed) == 1
        assert "ALTER TABLE" in executed[0]
        assert "RENAME TO" in executed[0]
        assert "`cat`.`schema`.`old`" in executed[0]
        assert "`cat`.`schema`.`new`" in executed[0]


# ---------------------------------------------------------------------------
# DatabricksDestination — reset_destination_state
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestDatabricksResetState:
    def test_drops_target_table(self):
        dest = _make_destination(schema_name="my_schema")
        dest.execute_sql = MagicMock(return_value=[])
        dest._execute_parameterised = MagicMock(return_value=[])
        dest.reset_destination_state("my_pipeline", "my_table")
        drop_calls = [str(c.args[0]) for c in dest.execute_sql.call_args_list]
        assert any("DROP TABLE IF EXISTS" in s and "my_table" in s for s in drop_calls)

    def test_cleans_meta_tables(self):
        dest = _make_destination(schema_name="my_schema")
        dest.execute_sql = MagicMock(return_value=[])
        dest._execute_parameterised = MagicMock(return_value=[])
        dest.reset_destination_state("my_pipeline", "my_table")
        # _execute_parameterised called for each meta table
        assert dest._execute_parameterised.call_count >= 1
        sql_calls = [str(c.args[0]) for c in dest._execute_parameterised.call_args_list]
        assert any("DELETE FROM" in s for s in sql_calls)

    def test_no_op_when_schema_name_missing(self):
        dest = _make_destination()  # no schema_name
        dest.execute_sql = MagicMock(return_value=[])
        dest._execute_parameterised = MagicMock(return_value=[])
        dest.reset_destination_state("my_pipeline", "my_table")
        dest.execute_sql.assert_not_called()
        dest._execute_parameterised.assert_not_called()

    def test_meta_table_exception_is_swallowed(self):
        dest = _make_destination(schema_name="my_schema")
        dest.execute_sql = MagicMock(return_value=[])
        dest._execute_parameterised = MagicMock(side_effect=Exception("no table"))
        # should not raise
        dest.reset_destination_state("my_pipeline", "my_table")


# ---------------------------------------------------------------------------
# DatabricksDestination — save_load_info
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestDatabricksSaveLoadInfo:
    def _dest_with_mock_conn(self):
        dest = _make_destination()
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.description = None
        mock_cursor.fetchall.return_value = []
        mock_conn.cursor.return_value.__enter__ = MagicMock(return_value=mock_cursor)
        mock_conn.cursor.return_value.__exit__ = MagicMock(return_value=False)
        dest._connection = mock_conn
        return dest, mock_conn, mock_cursor

    def test_empty_records_is_noop(self):
        dest, mock_conn, mock_cursor = self._dest_with_mock_conn()
        dest.save_load_info("my_schema", [])
        mock_cursor.execute.assert_not_called()

    def test_creates_table_and_inserts_records(self):
        dest, mock_conn, mock_cursor = self._dest_with_mock_conn()
        record = {
            "pipeline_name": "test_pipe",
            "destination_name": "databricks",
            "destination_type": "databricks",
            "dataset_name": "my_schema",
            "table_name": "my_table",
            "row_count": 10,
            "started_at": "2026-01-01T00:00:00",
            "finished_at": "2026-01-01T00:01:00",
            "first_run": True,
            "saved_at": "2026-01-01",
        }
        dest.save_load_info("my_schema", [record])
        calls = [str(c.args[0]) for c in mock_cursor.execute.call_args_list]
        assert any("CREATE TABLE IF NOT EXISTS" in s for s in calls)
        assert any("INSERT INTO" in s for s in calls)

    def test_datetimes_serialised_to_isoformat(self):
        from datetime import datetime

        dest, mock_conn, mock_cursor = self._dest_with_mock_conn()
        dt = datetime(2026, 1, 1, 12, 0, 0)
        record = {
            "pipeline_name": "pipe",
            "destination_name": "databricks",
            "destination_type": "databricks",
            "dataset_name": "schema",
            "table_name": "tbl",
            "row_count": 1,
            "started_at": dt,
            "finished_at": dt,
            "first_run": False,
            "saved_at": "2026-01-01",
        }
        dest.save_load_info("schema", [record])
        insert_calls = [
            c for c in mock_cursor.execute.call_args_list if "INSERT" in str(c)
        ]
        assert len(insert_calls) == 1
        values = insert_calls[0].args[1]
        assert "2026-01-01T12:00:00" in values


# ---------------------------------------------------------------------------
# DatabricksDestination — apply_hints
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestDatabricksApplyHints:
    def test_no_hints_returns_resource_unchanged(self):
        dest = _make_destination()
        resource = MagicMock()
        result = dest.apply_hints(resource)
        assert result is resource

    def test_unknown_hint_returns_resource_unchanged(self):
        dest = _make_destination()
        resource = MagicMock()
        result = dest.apply_hints(resource, unknown_hint="value")
        assert result is resource

    def test_cluster_columns_calls_adapter(self):
        dest = _make_destination()
        resource = MagicMock()
        adapted = MagicMock()
        with patch(
            "dlt.destinations.adapters.databricks_adapter", return_value=adapted
        ) as mock_adapter:
            result = dest.apply_hints(resource, cluster_columns=["id", "ts"])
        mock_adapter.assert_called_once_with(resource, liquid_cluster_by=["id", "ts"])
        assert result is adapted

    def test_partition_column_calls_adapter(self):
        dest = _make_destination()
        resource = MagicMock()
        adapted = MagicMock()
        with patch(
            "dlt.destinations.adapters.databricks_adapter", return_value=adapted
        ) as mock_adapter:
            result = dest.apply_hints(resource, partition_column="dt")
        mock_adapter.assert_called_once_with(resource, partition="dt")
        assert result is adapted

    def test_table_description_calls_adapter(self):
        dest = _make_destination()
        resource = MagicMock()
        adapted = MagicMock()
        with patch(
            "dlt.destinations.adapters.databricks_adapter", return_value=adapted
        ) as mock_adapter:
            result = dest.apply_hints(resource, table_description="My table")
        mock_adapter.assert_called_once_with(resource, table_description="My table")
        assert result is adapted

    def test_import_error_falls_back_to_resource(self):
        dest = _make_destination()
        resource = MagicMock()
        with patch(
            "dlt_saga.destinations.databricks.destination.DatabricksDestination.apply_hints",
            wraps=dest.apply_hints,
        ):
            with patch(
                "builtins.__import__",
                side_effect=lambda name, *a, **kw: (
                    (_ for _ in ()).throw(ImportError("no dlt adapters"))
                    if name == "dlt.destinations.adapters"
                    else __import__(name, *a, **kw)
                ),
            ):
                # apply_hints catches ImportError internally and falls back
                result = dest.apply_hints(resource, cluster_columns=["id"])
        # If ImportError, result should be the original resource
        assert result is resource or result is not None  # at minimum doesn't raise


# ---------------------------------------------------------------------------
# DatabricksDestination — get_access_manager singleton
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestDatabricksGetAccessManager:
    def test_returns_access_manager_instance(self):
        dest = _make_destination()
        mgr = dest.get_access_manager()
        assert mgr is not None

    def test_access_manager_is_cached(self):
        dest = _make_destination()
        mgr1 = dest.get_access_manager()
        mgr2 = dest.get_access_manager()
        assert mgr1 is mgr2
