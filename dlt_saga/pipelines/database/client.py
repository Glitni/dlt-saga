"""Database client using ConnectorX for high-performance data extraction."""

import logging
import time
from datetime import datetime, timezone
from typing import Any, Dict, Optional, Tuple
from urllib.parse import quote_plus

import connectorx as cx

from dlt_saga.pipelines.database.config import DatabaseConfig
from dlt_saga.utility.secrets import resolve_secret

logger = logging.getLogger(__name__)

# Placeholder a custom incremental query must contain so the resolved cursor
# can be substituted in (mirrors the API source's placeholder contract).
_INCREMENTAL_VALUE_PLACEHOLDER = "{incremental_value}"


class DatabaseClient:
    """Client for connecting to databases using ConnectorX.

    ConnectorX provides high-performance data extraction using Arrow format,
    with support for parallel reading and multiple database engines.
    """

    # Default ports for common database types
    DEFAULT_PORTS = {
        "postgres": 5432,
        "postgresql": 5432,
        "mysql": 3306,
        "mariadb": 3306,
        "mssql": 1433,
        "oracle": 1521,
        "clickhouse": 9000,
        "redshift": 5439,
        "trino": 8080,
    }

    def __init__(self, config: DatabaseConfig):
        """Initialize database client.

        Args:
            config: DatabaseConfig object with connection details
        """
        self.config = config
        self._connection_string: Optional[str] = None

    def _resolve_credentials(self) -> Tuple[str, str]:
        """Resolves username and password credentials."""
        # Resolve credentials
        username = resolve_secret(self.config.username) if self.config.username else ""
        password = resolve_secret(self.config.password) if self.config.password else ""

        # URL-encode username and password to handle special characters
        if username:
            username = quote_plus(username)
        if password:
            password = quote_plus(password)

        return username, password

    def _build_connection_string(self) -> str:
        """Build connection string from config components.

        Returns:
            Database connection string
        """
        if self.config.connection_string:
            # Use provided connection string, resolving any secrets
            return resolve_secret(self.config.connection_string)

        # Build connection string from components
        db_type = self.config.database_type.lower()

        # Resolve credentials
        username, password = self._resolve_credentials()

        # Get default port if not specified
        port = self.config.port or self.DEFAULT_PORTS.get(db_type)

        # Build base connection string based on database type
        if db_type in ("postgres", "postgresql", "redshift"):
            protocol = "postgresql"
            auth = f"{username}:{password}@" if username else ""
            conn_str = f"{protocol}://{auth}{self.config.host}:{port}/{self.config.source_database}"

        elif db_type in ("mysql", "mariadb", "clickhouse"):
            protocol = "mysql" if db_type in ("mysql", "mariadb") else "clickhouse"
            auth = f"{username}:{password}@" if username else ""
            conn_str = f"{protocol}://{auth}{self.config.host}:{port}/{self.config.source_database}"

        elif db_type == "mssql":
            auth = f"{username}:{password}@" if username else ""
            conn_str = (
                f"mssql://{auth}{self.config.host}:{port}/{self.config.source_database}"
            )

        elif db_type in ("sqlite", "duckdb"):
            # File-based databases
            db_path = self.config.source_database
            conn_str = f"{db_type}://{db_path}"

        elif db_type == "oracle":
            auth = f"{username}:{password}@" if username else ""
            conn_str = f"oracle://{auth}{self.config.host}:{port}/{self.config.source_database}"

        elif db_type == "bigquery":
            # BigQuery uses project ID as database
            conn_str = f"bigquery://{self.config.source_database}"

        elif db_type == "trino":
            auth = f"{username}:{password}@" if username else ""
            catalog = self.config.source_database
            schema = self.config.source_schema or "default"
            conn_str = f"trino://{auth}{self.config.host}:{port}/{catalog}/{schema}"

        else:
            raise ValueError(f"Unsupported database type: {db_type}")

        # Add connection options if provided
        if self.config.connection_options:
            params = "&".join(
                f"{k}={v}" for k, v in self.config.connection_options.items()
            )
            conn_str = f"{conn_str}?{params}"

        logger.debug(f"Built connection string for {db_type} database")
        return conn_str

    def get_connection_string(self) -> str:
        """Get connection string, building it if necessary.

        Returns:
            Connection string for database
        """
        if not self._connection_string:
            self._connection_string = self._build_connection_string()
        return self._connection_string

    def _has_bigquery_credentials(self) -> bool:
        """Check if BigQuery credentials are available for ConnectorX.

        ConnectorX requires a service account key file for BigQuery.
        Returns True if credentials are available, False otherwise.
        """
        import os

        # Check for service account key file
        if os.getenv("GOOGLE_APPLICATION_CREDENTIALS"):
            key_path = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
            if os.path.isfile(key_path):
                return True

        # ConnectorX doesn't support other credential types like ADC/impersonation
        return False

    def _build_query(
        self,
        incremental_value: Optional[Any] = None,
        incremental_column: Optional[str] = None,
    ) -> str:
        """Build SQL query from config.

        Args:
            incremental_value: Value for incremental loading (if applicable)
            incremental_column: Column name for incremental loading (if applicable)

        Returns:
            SQL query string
        """
        if self.config.query:
            return self._build_custom_query(incremental_value, incremental_column)

        # Build query from source_table
        table = self.config.source_table
        if self.config.source_schema:
            table = f"{self.config.source_schema}.{table}"
        if self.config.source_database and isinstance(self.config.source_database, str):
            table = f"{self.config.source_database}.{table}"
        # Quote for databases that need it (e.g., BigQuery backticks)
        if self.config.database_type == "bigquery":
            table = f"`{table}`"

        if incremental_value is not None and incremental_column:
            # Render the watermark by inferred type: numerics unquoted (no implicit
            # string→number cast), everything else as an escaped string literal.
            literal = self._render_sql_literal(incremental_value)
            return f"SELECT * FROM {table} WHERE {incremental_column} > {literal}"

        return f"SELECT * FROM {table}"

    def _build_custom_query(
        self,
        incremental_value: Optional[Any],
        incremental_column: Optional[str],
    ) -> str:
        """Build the query for a user-provided custom ``query`` config.

        The user controls quoting inside their own SQL, so the cursor is
        substituted verbatim into the ``{incremental_value}`` placeholder. When
        incremental is enabled the query MUST contain that placeholder (otherwise
        it would silently re-read everything every run), and a watermark must be
        available (a raw ``{incremental_value}`` sent to the DB is a hard error,
        so first runs require ``initial_value``).
        """
        query = self.config.query
        if not incremental_column:
            # Incremental disabled — return the query as-is.
            return query

        if _INCREMENTAL_VALUE_PLACEHOLDER not in query:
            raise ValueError(
                "incremental is enabled but the custom query has no "
                f"'{_INCREMENTAL_VALUE_PLACEHOLDER}' placeholder — the cursor "
                "cannot be applied and the query would re-read everything every "
                "run. Add a predicate such as:\n"
                "  WHERE updated_at > '{incremental_value}'"
            )

        if incremental_value is None:
            raise ValueError(
                "incremental is enabled with a custom query but no watermark is "
                "available (first run, no prior data). Set 'initial_value' to seed "
                f"the first run's '{_INCREMENTAL_VALUE_PLACEHOLDER}'."
            )

        return query.format(
            incremental_column=incremental_column,
            incremental_value=incremental_value,
        )

    def _render_sql_literal(self, value: Any) -> str:
        """Render a watermark value as a SQL literal for the auto-built WHERE.

        Numerics are emitted unquoted; datetimes are normalised to naive UTC
        (a trailing timezone offset is rejected by some engines, e.g. SQL Server
        DATETIME); everything else is an escaped, single-quoted string.
        """
        if isinstance(value, bool):
            return "TRUE" if value else "FALSE"
        if isinstance(value, (int, float)):
            return str(value)
        if isinstance(value, datetime):
            if value.tzinfo is not None:
                value = value.astimezone(timezone.utc).replace(tzinfo=None)
            text = value.isoformat(sep=" ")
        else:
            text = str(value)
        return "'" + self._escape_sql_string(text) + "'"

    def _escape_sql_string(self, text: str) -> str:
        """Escape single quotes in a string literal for the target dialect.

        BigQuery escapes with a backslash (``\\'``); ANSI dialects (Postgres,
        MySQL, SQL Server, etc.) double the quote (``''``). Mirrors the
        dialect-aware escaping used on the destination side.
        """
        if (
            self.config.database_type
            and self.config.database_type.lower() == "bigquery"
        ):
            return text.replace("\\", "\\\\").replace("'", "\\'")
        return text.replace("'", "''")

    def fetch_data(
        self,
        incremental_value: Optional[Any] = None,
        incremental_column: Optional[str] = None,
        return_type: str = "arrow",
    ) -> Any:
        """Fetch data from database using ConnectorX or BigQuery client.

        Args:
            incremental_value: Value for incremental loading (if applicable)
            incremental_column: Column name for incremental loading (if applicable)
            return_type: Return type - "arrow", "pandas", or "polars" (default: "arrow")

        Returns:
            Arrow table, Pandas DataFrame, or Polars DataFrame depending on return_type
        """
        query = self._build_query(incremental_value, incremental_column)

        logger.debug(f"Executing query: {query}")

        # Special handling for BigQuery - use BigQuery client instead of ConnectorX
        # if service account credentials are not available
        if (
            self.config.database_type
            and self.config.database_type.lower() == "bigquery"
        ):
            if not self._has_bigquery_credentials():
                logger.debug(
                    "No BigQuery service account key found, using BigQuery client with ADC"
                )
                return self._fetch_from_bigquery(query, return_type)
            else:
                logger.debug("BigQuery service account key found, using ConnectorX")

        # DuckDB: use native client (ConnectorX doesn't support DuckDB)
        if self.config.database_type and self.config.database_type.lower() == "duckdb":
            return self._fetch_from_duckdb(query, return_type)

        # For all other databases (and BigQuery with credentials), use ConnectorX
        conn_str = self.get_connection_string()

        # Build kwargs for connectorx
        cx_kwargs: Dict[str, Any] = {
            "return_type": return_type,
        }

        # Add partition options if specified
        if self.config.partition_on:
            cx_kwargs["partition_on"] = self.config.partition_on
            if self.config.partition_num:
                cx_kwargs["partition_num"] = self.config.partition_num

        # Execute query with ConnectorX (with retry for transient connection errors)
        max_retries = self.config.max_retries
        backoff_base = self.config.retry_backoff_base

        for attempt in range(max_retries + 1):
            try:
                data = cx.read_sql(conn_str, query, **cx_kwargs)
                logger.info(f"Fetched {len(data)} rows from database")
                return data
            except Exception as e:
                if attempt < max_retries:
                    wait_seconds = backoff_base ** (attempt + 1)
                    logger.warning(
                        f"Database fetch error (attempt {attempt + 1}/{max_retries + 1}), "
                        f"retrying in {wait_seconds}s: {e}"
                    )
                    time.sleep(wait_seconds)
                    continue

                raise

    def _fetch_from_bigquery(self, query: str, return_type: str = "arrow") -> Any:
        """Fetch data from BigQuery using the BigQuery client (supports ADC/impersonation).

        Retries on transient errors (e.g. EXTERNAL_QUERY connection failures)
        with exponential backoff using config.max_retries and config.retry_backoff_base.

        Args:
            query: SQL query to execute
            return_type: Return type - "arrow", "pandas", or "polars"

        Returns:
            Arrow table, Pandas DataFrame, or Polars DataFrame
        """
        from google.api_core.exceptions import BadRequest, ServerError

        from dlt_saga.utility.cli.context import get_execution_context
        from dlt_saga.utility.gcp.client_pool import bigquery_pool

        # Determine project for query execution (billing project)
        # Priority: profile project > config.database
        context = get_execution_context()
        project_id = context.get_database()
        if not project_id:
            project_id = self.config.source_database
            if not project_id:
                raise ValueError(
                    "No BigQuery project specified. Either configure a profile with a project "
                    "or set 'database' in config."
                )

        # Use pooled BigQuery client
        client = bigquery_pool.get_client(project_id)

        max_retries = self.config.max_retries
        backoff_base = self.config.retry_backoff_base

        for attempt in range(max_retries + 1):
            try:
                # Execute query and get results
                query_job = client.query(query)

                if return_type == "arrow":
                    # Return as Arrow table
                    result = query_job.to_arrow()
                elif return_type == "pandas":
                    # Return as Pandas DataFrame
                    result = query_job.to_dataframe()
                elif return_type == "polars":
                    # Convert Arrow to Polars
                    try:
                        import polars as pl
                    except ImportError as e:
                        raise ValueError(
                            "Polars is not installed. To use return_type='polars', "
                            "install it with: pip install dlt-saga[polars]"
                        ) from e

                    arrow_table = query_job.to_arrow()
                    result = pl.from_arrow(arrow_table)
                else:
                    raise ValueError(f"Unsupported return_type: {return_type}")

                logger.info(f"Fetched {len(result)} rows from BigQuery")
                return result
            except (BadRequest, ServerError) as e:
                # Retry on transient EXTERNAL_QUERY failures (e.g. connection slot exhaustion)
                # and server errors (500, 503)
                is_external_query_error = "EXTERNAL_QUERY" in str(e)
                is_server_error = isinstance(e, ServerError)

                if (
                    is_external_query_error or is_server_error
                ) and attempt < max_retries:
                    wait_seconds = backoff_base ** (attempt + 1)
                    logger.warning(
                        f"Transient BigQuery error (attempt {attempt + 1}/{max_retries + 1}), "
                        f"retrying in {wait_seconds}s: {e}"
                    )
                    time.sleep(wait_seconds)
                    continue

                raise

    def _fetch_from_duckdb(self, query: str, return_type: str = "arrow") -> Any:
        """Fetch data from DuckDB using the native client.

        Args:
            query: SQL query to execute
            return_type: Return type - "arrow", "pandas", or "polars"

        Returns:
            Arrow table, Pandas DataFrame, or Polars DataFrame
        """
        import duckdb

        db_path = self.config.source_database
        conn = duckdb.connect(db_path)

        try:
            result_rel = conn.execute(query)

            if return_type == "arrow":
                import pyarrow as pa

                result = result_rel.fetch_arrow_table()
                # Strip timezone info from timestamp columns to avoid
                # PyArrow tzdata resolution issues on Windows
                new_fields = []
                for field in result.schema:
                    if pa.types.is_timestamp(field.type) and field.type.tz:
                        new_fields.append(
                            field.with_type(pa.timestamp(field.type.unit))
                        )
                    else:
                        new_fields.append(field)
                result = result.cast(pa.schema(new_fields))
            elif return_type == "pandas":
                result = result_rel.fetchdf()
            elif return_type == "polars":
                result = result_rel.pl()
            else:
                raise ValueError(f"Unsupported return_type: {return_type}")

            logger.info(f"Fetched {len(result)} rows from DuckDB")
            return result
        finally:
            conn.close()

    def test_connection(self) -> bool:
        """Test database connection by executing a simple query.

        Returns:
            True if connection successful, False otherwise
        """
        probe = "SELECT 1 as test_column"
        try:
            # Mirror fetch_data's routing so the probe uses the same backend the
            # real fetch will: BigQuery-with-ADC and DuckDB don't go through
            # ConnectorX, so probing them with cx.read_sql would fail spuriously.
            dbtype = (self.config.database_type or "").lower()
            if dbtype == "bigquery" and not self._has_bigquery_credentials():
                self._fetch_from_bigquery(probe, "arrow")
            elif dbtype == "duckdb":
                self._fetch_from_duckdb(probe, "arrow")
            else:
                conn_str = self.get_connection_string()
                cx.read_sql(conn_str, probe, return_type="arrow")
            logger.info("Database connection test successful")
            return True
        except Exception as e:
            logger.error(f"Database connection test failed: {e}")
            return False
