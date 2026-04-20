"""Databricks Unity Catalog destination implementation."""

from __future__ import annotations

import logging
import threading
import uuid
from datetime import datetime
from typing import Any, Optional

from dlt_saga.destinations.base import Destination
from dlt_saga.destinations.databricks.config import DatabricksDestinationConfig

logger = logging.getLogger(__name__)


class DatabricksDestination(Destination):
    """Databricks Unity Catalog destination.

    Supports PAT, OAuth M2M, and OAuth U2M auth modes via the Databricks SDK.
    Uses ``databricks-sql-connector`` for direct SQL execution and dlt's
    built-in Databricks destination for pipeline loading.
    """

    config: DatabricksDestinationConfig

    def __init__(self, config: DatabricksDestinationConfig) -> None:
        try:
            from databricks import sql as _  # noqa: F401
        except ImportError:
            raise ImportError(
                "Databricks destination requires 'dlt-saga[databricks]'. "
                "Run: pip install 'dlt-saga[databricks]'"
            ) from None

        super().__init__(config)
        self._access_manager: Optional[Any] = None
        self._connection: Optional[Any] = None
        self._connection_lock = threading.Lock()

    # ------------------------------------------------------------------
    # Token management
    # ------------------------------------------------------------------

    def _get_token(self) -> str:
        """Obtain a current access token for SQL connector calls.

        For PAT mode, returns the static token.  For OAuth modes (U2M/M2M),
        calls the Databricks SDK's ``Config.authenticate()`` which handles
        token refresh and caching internally.

        Returns:
            Bearer token string (without the ``Bearer `` prefix).
        """
        from dlt_saga.utility.auth.databricks import get_databricks_token

        return get_databricks_token(
            host=self.config.host,
            auth_mode=self.config.auth_mode,
            access_token=self.config.access_token,
            client_id=self.config.client_id,
            client_secret=self.config.client_secret,
        )

    def _get_connection(self) -> Any:
        """Return an open ``databricks-sql-connector`` connection.

        A new connection is created lazily on first call.  For long-running
        processes with OAuth, call ``_close_connection()`` before each batch
        to force a fresh token.
        """
        from databricks import sql as databricks_sql

        with self._connection_lock:
            if self._connection is None:
                token = self._get_token()
                self._connection = databricks_sql.connect(
                    server_hostname=self.config.server_hostname,
                    http_path=self.config.http_path,
                    access_token=token,
                )
                logger.debug(
                    "Opened Databricks SQL connection: host=%s, path=%s",
                    self.config.server_hostname,
                    self.config.http_path,
                )
        return self._connection

    def _close_connection(self) -> None:
        """Close the SQL connection, forcing a fresh token on next use.

        The Databricks connector's ThriftBackend logs spurious "I/O operation
        on closed file" errors from a background thread that fires asynchronously
        after close() returns.  We attach a filter to that logger before closing
        so the noise is suppressed regardless of when the background thread runs.
        """
        if self._connection is not None:
            import logging

            class _SuppressClosedFile(logging.Filter):
                def filter(self, record: logging.LogRecord) -> bool:
                    return "I/O operation on closed file" not in record.getMessage()

            thrift_logger = logging.getLogger("databricks.sql.backend.thrift_backend")
            thrift_logger.addFilter(_SuppressClosedFile())
            try:
                self._connection.close()
            except Exception:
                pass
            self._connection = None

    def connect(self) -> None:
        """Pre-establish the SQL connection and verify credentials.

        Triggers OAuth on the calling thread (important: must be called from
        the main thread for interactive U2M flows so the browser can open).
        Also wakes the SQL warehouse if it was suspended.
        """
        conn = self._get_connection()
        with conn.cursor() as cursor:
            cursor.execute("SELECT 1")
        logger.debug(
            "Databricks connection verified: host=%s", self.config.server_hostname
        )

    def close(self) -> None:
        """Close the SQL connection (implements ``Destination.close``)."""
        self._close_connection()

    # ------------------------------------------------------------------
    # dlt integration
    # ------------------------------------------------------------------

    def create_dlt_destination(self) -> Any:
        """Create the dlt Databricks destination instance.

        Uses the resolved access token so that dlt's connector uses the
        same credentials as our direct SQL calls.
        """
        from dlt.destinations import databricks as dlt_databricks

        token = self._get_token()
        logger.debug(
            "Creating dlt Databricks destination: host=%s, catalog=%s",
            self.config.server_hostname,
            self.config.catalog,
        )
        return dlt_databricks(
            credentials={
                "server_hostname": self.config.server_hostname,
                "http_path": self.config.http_path,
                "access_token": token,
                "catalog": self.config.catalog,
            },
            staging_volume_name=self.config.staging_volume_name,
            staging_credentials_name=self.config.staging_credentials_name,
        )

    def apply_hints(self, resource: Any, **hints) -> Any:
        """Apply Databricks-specific hints to a dlt resource.

        Supports ``table_description``, ``cluster_columns`` (Liquid Clustering),
        and ``partition_column`` (traditional PARTITIONED BY).
        Unknown hints are silently ignored.
        """
        try:
            from dlt.destinations.adapters import databricks_adapter

            adapter_kwargs: dict = {}
            if "table_description" in hints:
                adapter_kwargs["table_description"] = hints["table_description"]
            if "cluster_columns" in hints:
                adapter_kwargs["liquid_cluster_by"] = hints["cluster_columns"]
            if "partition_column" in hints:
                adapter_kwargs["partition"] = hints["partition_column"]

            if adapter_kwargs:
                return databricks_adapter(resource, **adapter_kwargs)
        except (ImportError, AttributeError, TypeError) as e:
            logger.debug("databricks_adapter not available (%s), skipping hints", e)

        return resource

    def run_pipeline(self, pipeline: Any, data: Any) -> Any:
        """Ensure the schema exists and run the pipeline."""
        if self.config.schema_name:
            self.ensure_schema_exists(self.config.schema_name)
        return pipeline.run(data)

    # ------------------------------------------------------------------
    # Access management
    # ------------------------------------------------------------------

    def supports_access_management(self) -> bool:
        return True

    def get_access_manager(self) -> Any:
        if self._access_manager is None:
            from dlt_saga.destinations.databricks.access import DatabricksAccessManager

            self._access_manager = DatabricksAccessManager(self)
            logger.debug("Initialised Databricks access manager")
        return self._access_manager

    # ------------------------------------------------------------------
    # Capability flags
    # ------------------------------------------------------------------

    def supports_partitioning(self) -> bool:
        return True

    def supports_clustering(self) -> bool:
        return True

    # ------------------------------------------------------------------
    # Direct SQL execution
    # ------------------------------------------------------------------

    def execute_sql(self, sql: str, dataset_name: Optional[str] = None) -> Any:
        """Execute a SQL statement (or multi-statement script) against Databricks.

        Multi-statement scripts are split on ``;`` and executed individually.
        All SQL should use fully-qualified table names (catalog.schema.table);
        the ``dataset_name`` parameter is accepted for interface compatibility
        but has no effect.

        Args:
            sql: SQL to execute.
            dataset_name: Unused — kept for interface compatibility.

        Returns:
            List of row objects (attribute + index access), or ``[]`` for DDL/DML.
        """
        conn = self._get_connection()

        logger.debug("Executing Databricks SQL (%d chars)", len(sql))

        with conn.cursor() as cursor:
            statements = [s.strip() for s in sql.split(";") if s.strip()]
            last_result: Optional[Any] = None
            for stmt in statements:
                cursor.execute(stmt)
                last_result = cursor

            if last_result is None:
                return []

            try:
                columns = [d[0] for d in last_result.description]
                rows = last_result.fetchall()
            except Exception:
                return []

        class _Row:
            __slots__ = ("_columns", "_values")

            def __init__(self, cols: list, vals: tuple) -> None:
                object.__setattr__(self, "_columns", cols)
                object.__setattr__(self, "_values", vals)

            def __getattr__(self, name: str) -> Any:
                try:
                    idx = self._columns.index(name)
                    return self._values[idx]
                except ValueError:
                    raise AttributeError(f"No column named '{name}'")

            def __getitem__(self, idx: int) -> Any:
                return self._values[idx]

            def __len__(self) -> int:
                return len(self._values)

        return [_Row(columns, row) for row in rows]

    def ensure_schema_exists(self, schema: str) -> None:
        """Create the schema (if absent) under the configured catalog."""
        fq = f"`{self.config.catalog}`.`{schema}`"
        self.execute_sql(f"CREATE SCHEMA IF NOT EXISTS {fq}")

    # ------------------------------------------------------------------
    # Load info
    # ------------------------------------------------------------------

    def save_load_info(
        self, dataset_name: str, records: list, pipeline: Any = None
    ) -> None:
        """Insert load info records directly into the load-info tracking table."""
        if not records:
            return

        from dlt_saga.project_config import get_load_info_table_name

        table_id = self.get_full_table_id(dataset_name, get_load_info_table_name())

        self.execute_sql(
            f"""
            CREATE TABLE IF NOT EXISTS {table_id} (
                pipeline_name STRING,
                destination_name STRING,
                destination_type STRING,
                dataset_name STRING,
                table_name STRING,
                row_count BIGINT,
                started_at TIMESTAMP,
                finished_at TIMESTAMP,
                first_run BOOLEAN,
                saved_at STRING,
                _dlt_load_id STRING NOT NULL,
                _dlt_id STRING NOT NULL
            )
            """,
            dataset_name,
        )

        load_id = str(uuid.uuid4())
        conn = self._get_connection()
        with conn.cursor() as cursor:
            for record in records:
                record = dict(record)
                record["_dlt_load_id"] = load_id
                record["_dlt_id"] = str(uuid.uuid4())

                for k, v in record.items():
                    if isinstance(v, datetime):
                        record[k] = v.isoformat()

                columns = list(record.keys())
                col_names = ", ".join(f"`{c}`" for c in columns)
                placeholders = ", ".join("?" for _ in columns)
                values = [record[c] for c in columns]
                cursor.execute(
                    f"INSERT INTO {table_id} ({col_names}) VALUES ({placeholders})",
                    values,
                )

    def _execute_parameterised(
        self, sql: str, params: list, dataset_name: Optional[str] = None
    ) -> list:
        """Execute a SQL statement with positional ``?`` bind parameters.

        Returned rows are plain tuples — the caller is expected to index by
        column position.  Use this instead of f-string interpolation anywhere
        user-controlled or config-derived values appear in a WHERE clause.
        """
        conn = self._get_connection()
        with conn.cursor() as cursor:
            cursor.execute(sql, params)
            try:
                return list(cursor.fetchall())
            except Exception:
                return []

    def get_last_load_timestamp(
        self, dataset_name: str, pipeline_name: str, table_name: str
    ) -> Optional[datetime]:
        """Get the timestamp of the last successful load that had data."""
        from dlt_saga.project_config import get_load_info_table_name

        try:
            table_id = self.get_full_table_id(dataset_name, get_load_info_table_name())
            rows = self._execute_parameterised(
                f"""
                SELECT MAX(started_at) AS started_at
                FROM {table_id}
                WHERE pipeline_name = ?
                  AND table_name = ?
                  AND row_count > 0
                """,
                [pipeline_name, table_name],
                dataset_name,
            )
            if rows and rows[0][0] is not None:
                return rows[0][0]
            return None
        except Exception:
            return None

    def get_max_column_value(self, table_id: str, column: str) -> Any:
        """Get the maximum value of a column in a Databricks table."""
        try:
            rows = self.execute_sql(
                f"SELECT MAX(`{column}`) AS max_value FROM {table_id}"
            )
            if rows and rows[0][0] is not None:
                return rows[0][0]
            return None
        except Exception:
            return None

    def reset_destination_state(self, pipeline_name: str, table_name: str) -> None:
        """Drop tables and clean up metadata for a full refresh."""
        logger.info(
            "Full refresh: resetting Databricks destination state for %s", pipeline_name
        )
        schema = self.config.schema_name
        if not schema:
            return

        table_id = self.get_full_table_id(schema, table_name)
        self.execute_sql(f"DROP TABLE IF EXISTS {table_id}")

        from dlt_saga.project_config import get_load_info_table_name

        for meta_table in (
            "_dlt_pipeline_state",
            get_load_info_table_name(),
            "_dlt_version",
        ):
            meta_id = self.get_full_table_id(schema, meta_table)
            try:
                col = "pipeline_name" if meta_table != "_dlt_version" else "schema_name"
                self._execute_parameterised(
                    f"DELETE FROM {meta_id} WHERE `{col}` = ?",
                    [pipeline_name],
                )
            except Exception as e:
                logger.debug("Could not clean %s: %s", meta_id, e)

    def clone_table(self, source_table_id: str, target_table_id: str) -> None:
        """Create a copy of source_table as target_table (DEEP CLONE)."""
        self.execute_sql(f"CREATE TABLE {target_table_id} DEEP CLONE {source_table_id}")

    def rename_table(self, table_id: str, new_table_id: str) -> None:
        """Rename a Databricks table using ALTER TABLE ... RENAME TO."""
        self.execute_sql(f"ALTER TABLE {table_id} RENAME TO {new_table_id}")

    # ------------------------------------------------------------------
    # SQL dialect overrides
    # ------------------------------------------------------------------

    def quote_identifier(self, name: str) -> str:
        return f"`{name}`"

    def get_full_table_id(self, dataset: str, table: str) -> str:
        return f"`{self.config.catalog}`.`{dataset}`.`{table}`"

    def hash_expression(self, columns: list) -> str:
        parts = ", ".join(f"COALESCE(CAST(`{c}` AS STRING), '')" for c in columns)
        return f"xxhash64(concat_ws('|', {parts}))"

    def partition_ddl(self, column: str) -> str:
        return f"PARTITIONED BY ({column})"

    def cluster_ddl(self, columns: list) -> str:
        return f"CLUSTER BY ({', '.join(columns)})"

    def type_name(self, logical_type: str) -> str:
        type_map = {
            "string": "STRING",
            "int64": "BIGINT",
            "bool": "BOOLEAN",
            "timestamp": "TIMESTAMP",
        }
        return type_map.get(logical_type, logical_type.upper())

    def cast_to_string(self, expression: str) -> str:
        return f"CAST({expression} AS STRING)"

    def columns_query(self, database: str, schema: str, table: str) -> str:
        catalog = database or self.config.catalog
        safe_catalog = catalog.replace("'", "''")
        safe_schema = schema.replace("'", "''")
        safe_table = table.replace("'", "''")
        return f"""
            SELECT column_name, data_type
            FROM system.information_schema.columns
            WHERE table_catalog = '{safe_catalog}'
              AND table_schema   = '{safe_schema}'
              AND table_name     = '{safe_table}'
            ORDER BY ordinal_position
        """

    def json_type_name(self) -> str:
        return "STRING"

    def parse_json_expression(self, value_expr: str) -> str:
        return value_expr  # Databricks stores JSON as STRING — no parse needed

    def extract_json_value(self, json_expr: str) -> str:
        return f"to_json({json_expr})"
