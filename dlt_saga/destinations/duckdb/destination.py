"""DuckDB destination implementation.

Provides a local, in-process destination for fast integration testing
and development without requiring cloud credentials.
"""

import logging
import uuid
from datetime import datetime
from typing import Any, Optional

import duckdb

from dlt_saga.destinations.base import Destination
from dlt_saga.destinations.duckdb.config import DuckDBDestinationConfig

logger = logging.getLogger(__name__)


class DuckDBDestination(Destination):
    """DuckDB destination implementation.

    Supports both in-memory databases (for testing) and file-based databases
    (for local development). Uses dlt's built-in DuckDB destination for
    pipeline execution.
    """

    config: DuckDBDestinationConfig

    def __init__(self, config: DuckDBDestinationConfig):
        super().__init__(config)
        self._connection: Optional[duckdb.DuckDBPyConnection] = None

    @property
    def connection(self) -> duckdb.DuckDBPyConnection:
        """Get or create the DuckDB connection.

        Uses a persistent connection for the lifetime of the destination
        to ensure in-memory databases maintain state across queries.
        """
        if self._connection is None:
            self._connection = duckdb.connect(self.config.database_path)
            logger.debug(f"Connected to DuckDB: {self.config.database_path}")
        return self._connection

    def close(self):
        """Close the DuckDB connection."""
        if self._connection is not None:
            self._connection.close()
            self._connection = None

    def create_dlt_destination(self) -> Any:
        """Create dlt DuckDB destination instance."""
        from dlt.destinations import duckdb as dlt_duckdb

        logger.debug(f"Creating dlt DuckDB destination: {self.config.database_path}")
        return dlt_duckdb(credentials=self.connection)

    def apply_hints(self, resource: Any, **hints) -> Any:
        """Apply hints to a dlt resource.

        DuckDB doesn't support partitioning or clustering, so these hints
        are silently ignored. Table descriptions are stored as comments.
        """
        if hints.get("table_description"):
            logger.debug(
                f"Table description for {resource.name}: {hints['table_description']}"
            )
        return resource

    def get_access_manager(self) -> None:
        return None

    def supports_access_management(self) -> bool:
        return False

    def supports_partitioning(self) -> bool:
        return False

    def supports_clustering(self) -> bool:
        return False

    def run_pipeline(self, pipeline: Any, data: Any) -> Any:
        """Run pipeline with DuckDB destination.

        Ensures the schema (dataset) exists before running.
        """
        dataset_name = pipeline.dataset_name
        if dataset_name:
            self.connection.execute(f'CREATE SCHEMA IF NOT EXISTS "{dataset_name}"')
        return pipeline.run(data)

    def save_load_info(
        self, dataset_name: str, records: list[dict], pipeline: Any = None
    ) -> None:
        """Save load info records via direct INSERT into DuckDB."""
        if not records:
            return

        from dlt_saga.project_config import get_load_info_table_name

        conn = self.connection
        table_id = f'"{dataset_name}"."{get_load_info_table_name()}"'

        # Ensure table exists
        conn.execute(f"""
            CREATE TABLE IF NOT EXISTS {table_id} (
                pipeline_name VARCHAR,
                destination_name VARCHAR,
                destination_type VARCHAR,
                dataset_name VARCHAR,
                table_name VARCHAR,
                row_count BIGINT,
                started_at TIMESTAMP,
                finished_at TIMESTAMP,
                first_run BOOLEAN,
                saved_at VARCHAR,
                _dlt_load_id VARCHAR NOT NULL,
                _dlt_id VARCHAR NOT NULL
            )
        """)

        load_id = str(uuid.uuid4())
        for record in records:
            record["_dlt_load_id"] = load_id
            record["_dlt_id"] = str(uuid.uuid4())

            # Serialize datetime objects
            for k, v in record.items():
                if isinstance(v, datetime):
                    record[k] = v.isoformat()

            columns = list(record.keys())
            col_names = ", ".join(f'"{c}"' for c in columns)
            placeholders = ", ".join("?" for _ in columns)
            values = [record[c] for c in columns]

            conn.execute(
                f"INSERT INTO {table_id} ({col_names}) VALUES ({placeholders})",
                values,
            )

    def get_last_load_timestamp(
        self, dataset_name: str, pipeline_name: str, table_name: str
    ) -> Optional[datetime]:
        """Get the timestamp of the last successful load that had data."""
        from dlt_saga.project_config import get_load_info_table_name

        try:
            table_id = f'"{dataset_name}"."{get_load_info_table_name()}"'
            result = self.connection.execute(
                f"""
                SELECT MAX(started_at) as started_at
                FROM {table_id}
                WHERE pipeline_name = ?
                AND table_name = ?
                AND row_count > 0
            """,
                [pipeline_name, table_name],
            ).fetchone()
            if result and result[0] is not None:
                return result[0]
            return None
        except Exception:
            return None

    def get_max_column_value(self, table_id: str, column: str) -> Any:
        """Get the maximum value of a column in a DuckDB table."""
        try:
            result = self.connection.execute(
                f'SELECT MAX("{column}") as max_value FROM {table_id}'
            ).fetchone()
            if result and result[0] is not None:
                return result[0]
            return None
        except Exception:
            return None

    def execute_sql(self, sql: str, dataset_name: Optional[str] = None) -> Any:
        """Execute a SQL statement against DuckDB.

        For multi-statement scripts (separated by ;), executes each
        statement individually and returns the result of the last one.
        """
        conn = self.connection

        if dataset_name:
            conn.execute(f'CREATE SCHEMA IF NOT EXISTS "{dataset_name}"')
            conn.execute(f'SET search_path TO "{dataset_name}"')

        logger.debug(f"Executing SQL ({len(sql)} chars) in dataset={dataset_name}")

        # Split multi-statement scripts and execute individually
        statements = [s.strip() for s in sql.split(";") if s.strip()]
        result = None
        for stmt in statements:
            result = conn.execute(stmt)

        # Return rows that support attribute access for compatibility with BigQuery
        if result is None:
            return []

        try:
            columns = [desc[0] for desc in result.description]
            rows = result.fetchall()
        except Exception:
            return []

        class Row:
            """Row object supporting both attribute and index access."""

            __slots__ = ("_columns", "_values")

            def __init__(self, cols, vals):
                object.__setattr__(self, "_columns", cols)
                object.__setattr__(self, "_values", vals)

            def __getattr__(self, name):
                try:
                    idx = self._columns.index(name)
                    return self._values[idx]
                except ValueError:
                    raise AttributeError(f"No column named '{name}'")

            def __getitem__(self, idx):
                return self._values[idx]

            def __len__(self):
                return len(self._values)

        return [Row(columns, row) for row in rows]

    def reset_destination_state(self, pipeline_name: str, table_name: str) -> None:
        """Reset destination state by dropping tables and metadata."""
        logger.info(f"Full refresh: Resetting destination state for {pipeline_name}")

        conn = self.connection
        dataset = self.config.schema_name

        if not dataset:
            return

        # Drop main table
        self._drop_table_safe(conn, dataset, table_name)

        # Clean up pipeline state
        self._delete_rows_safe(
            conn,
            dataset,
            "_dlt_pipeline_state",
            "pipeline_name = ?",
            [pipeline_name],
        )

        # Clean up load info
        from dlt_saga.project_config import get_load_info_table_name

        self._delete_rows_safe(
            conn,
            dataset,
            get_load_info_table_name(),
            "pipeline_name = ? AND table_name = ?",
            [pipeline_name, table_name],
        )

        # Clean up schema version
        from dlt.common.normalizers.naming.snake_case import NamingConvention

        naming = NamingConvention(max_length=64)
        normalized_schema = naming.normalize_identifier(pipeline_name)

        self._delete_rows_safe(
            conn,
            dataset,
            "_dlt_version",
            "schema_name = ?",
            [normalized_schema],
        )

    @staticmethod
    def _drop_table_safe(
        conn: duckdb.DuckDBPyConnection, schema: str, table: str
    ) -> None:
        """Drop a table if it exists."""
        try:
            conn.execute(f'DROP TABLE IF EXISTS "{schema}"."{table}"')
            logger.debug(f'Dropped table: "{schema}"."{table}"')
        except Exception as e:
            logger.debug(f"Could not drop table {schema}.{table}: {e}")

    @staticmethod
    def _delete_rows_safe(
        conn: duckdb.DuckDBPyConnection,
        schema: str,
        table: str,
        where: str,
        params: list,
    ) -> None:
        """Delete rows from a table, ignoring errors if table doesn't exist."""
        try:
            conn.execute(f'DELETE FROM "{schema}"."{table}" WHERE {where}', params)
        except Exception as e:
            logger.debug(f"Could not clean {schema}.{table}: {e}")

    def supports_transactions(self) -> bool:
        """DuckDB maintains a persistent connection, so BEGIN/COMMIT work across calls."""
        return True

    def clone_table(self, source_table_id: str, target_table_id: str) -> None:
        """Create a full copy of source_table as target_table in DuckDB."""
        sql = f"CREATE TABLE {target_table_id} AS SELECT * FROM {source_table_id}"
        self.execute_sql(sql)

    def rename_table(self, table_id: str, new_table_id: str) -> None:
        """Rename a DuckDB table using ALTER TABLE ... RENAME TO.

        DuckDB's RENAME TO clause takes only the new table name — no schema prefix.
        Extracts the name from the fully-qualified new_table_id.
        """
        # new_table_id format: '"schema"."new_name"' → take last component
        new_name = new_table_id.split(".")[-1].strip('"')
        sql = f"ALTER TABLE {table_id} RENAME TO {self.quote_identifier(new_name)}"
        self.execute_sql(sql)

    # -------------------------------------------------------------------------
    # SQL dialect overrides
    # -------------------------------------------------------------------------

    def quote_identifier(self, name: str) -> str:
        return f'"{name}"'

    def timestamp_n_days_ago(self, days: int) -> str:
        return f"now() - INTERVAL '{days}' DAY"

    def get_full_table_id(self, dataset: str, table: str) -> str:
        return f'"{dataset}"."{table}"'

    def hash_expression(self, columns: list[str]) -> str:
        cast_cols = ", ".join(
            f"COALESCE(CAST(\"{c}\" AS VARCHAR), '')" for c in columns
        )
        return f"md5(concat({cast_cols}))"

    def partition_ddl(self, column: str) -> str:
        return ""  # DuckDB doesn't support partitioning

    def cluster_ddl(self, columns: list[str]) -> str:
        return ""  # DuckDB doesn't support clustering

    def type_name(self, logical_type: str) -> str:
        type_map = {
            "string": "VARCHAR",
            "int64": "BIGINT",
            "bool": "BOOLEAN",
            "timestamp": "TIMESTAMP",
        }
        return type_map.get(logical_type, logical_type.upper())

    def cast_to_string(self, expression: str) -> str:
        return f"CAST({expression} AS VARCHAR)"

    def columns_query(self, database: str, schema: str, table: str) -> str:
        safe_schema = schema.replace("'", "''")
        safe_table = table.replace("'", "''")
        return f"""
            SELECT column_name, data_type
            FROM information_schema.columns
            WHERE table_schema = '{safe_schema}'
            AND table_name = '{safe_table}'
            ORDER BY ordinal_position
        """
