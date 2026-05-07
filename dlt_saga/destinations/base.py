"""Base classes for destination implementations.

This module defines the abstract interfaces that all destination implementations
must follow, enabling support for multiple data warehouses (BigQuery, Snowflake, etc.).
"""

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime
from typing import TYPE_CHECKING, Any, Dict, Optional

if TYPE_CHECKING:
    from dlt_saga.destinations.config import DestinationConfig


# ---------------------------------------------------------------------------
# Native-load contract types
# ---------------------------------------------------------------------------


@dataclass
class DerivedColumn:
    """A framework-managed column injected by NativeLoadPipeline."""

    name: str
    sql_expr: str
    sql_type: str


@dataclass
class NativeLoadSpec:
    """Inputs for a single native-load chunk passed to destination.native_load_chunk."""

    target_dataset: str
    target_table: str
    source_uris: list
    file_type: str
    autodetect_schema: bool
    derived_columns: list
    target_exists: bool
    partition_column: Optional[str]
    cluster_columns: Optional[list]
    format_options: dict
    staging_dataset: str
    chunk_label: str
    write_disposition: str = "append"
    column_hints: Dict[str, str] = field(default_factory=dict)
    # Databricks-specific: external table LOCATION URI (None = managed table)
    target_location: Optional[str] = None
    # Databricks-specific: table format ("delta", "iceberg", "delta_uniform")
    table_format: str = "delta"


@dataclass
class NativeLoadResult:
    """Result returned from destination.native_load_chunk."""

    rows_loaded: int
    job_id: str
    rows_by_uri: dict = field(default_factory=dict)


class Destination(ABC):
    """Abstract base class for all destination implementations.

    Each destination (BigQuery, Snowflake, etc.) implements this interface
    to provide destination-specific functionality while maintaining a consistent API.
    """

    def __init__(self, config: "DestinationConfig"):
        """Initialize destination with configuration.

        Args:
            config: Destination-specific configuration object
        """
        self.config = config

    @abstractmethod
    def create_dlt_destination(self) -> Any:
        """Create the dlt destination instance for this destination type.

        This wraps the dlt library's destination creation (e.g., dlt.destinations.bigquery()).

        Returns:
            dlt destination instance
        """
        pass

    def connect(self) -> None:
        """Pre-establish a connection and verify credentials.

        Called before any query work to ensure authentication happens on the
        main thread (important for interactive OAuth flows such as Databricks
        U2M).  No-op by default; override in destinations with explicit
        connection management.
        """

    def close(self) -> None:
        """Close any open connections held by this destination.

        Called after report/query operations to release resources before
        impersonation contexts are torn down.  No-op by default; destinations
        with explicit connection management (Databricks, DuckDB) override this.
        """

    def create_dlt_staging(self) -> Optional[Any]:
        """Create a dlt staging destination, if required by this destination type.

        Most destinations return None (no external staging needed).  Destinations
        that require a staging area (e.g., Databricks with Azure Blob Storage)
        override this to return a configured dlt filesystem destination.

        Returns:
            dlt staging destination instance, or None.
        """
        return None

    @abstractmethod
    def apply_hints(self, resource: Any, **hints) -> Any:
        """Apply destination-specific hints to a dlt resource.

        Hints can include partitioning, clustering, table descriptions, etc.
        Each destination handles its own supported hint types and should
        silently ignore unknown hints.

        Common hints across destinations:
        - table_description: Description of the table
        - partition_column: Column to partition by (BigQuery, Snowflake)
        - cluster_columns: Columns to cluster by (BigQuery, Snowflake)

        Args:
            resource: dlt resource to apply hints to
            **hints: Destination hints dictionary (destination-agnostic format)

        Returns:
            Resource with applied hints (may be wrapped in destination adapter)
        """
        pass

    @abstractmethod
    def get_access_manager(self) -> Optional["AccessManager"]:
        """Get the access manager for this destination.

        Returns:
            AccessManager instance if destination supports access management, None otherwise
        """
        pass

    @abstractmethod
    def supports_access_management(self) -> bool:
        """Check if this destination supports access management.

        Returns:
            True if destination can manage access permissions
        """
        pass

    @abstractmethod
    def supports_partitioning(self) -> bool:
        """Check if this destination supports table partitioning.

        Returns:
            True if destination supports partitioning
        """
        pass

    @abstractmethod
    def supports_clustering(self) -> bool:
        """Check if this destination supports table clustering.

        Returns:
            True if destination supports clustering
        """
        pass

    def get_client_pool(self) -> Optional[Any]:
        """Get client pool for this destination (if applicable).

        Override this method if the destination uses a client pool.

        Returns:
            Client pool instance or None
        """
        return None

    @classmethod
    def prepare_for_execution(cls, pipeline_configs: list[Any]) -> None:
        """Prepare destination for running multiple pipelines.

        This is called once before executing multiple pipelines, allowing
        destinations to perform upfront setup (e.g., pre-creating schemas/datasets
        to avoid race conditions in parallel execution).

        Base implementation does nothing. Destinations override this for optimization.

        Args:
            pipeline_configs: List of PipelineConfig objects that will be executed
        """
        pass

    def run_pipeline(self, pipeline: Any, data: Any) -> Any:
        """Run pipeline with destination-specific preparation.

        Base implementation just calls pipeline.run().
        Destinations can override this to handle pre-execution setup
        (e.g., ensuring datasets exist to prevent race conditions).

        Args:
            pipeline: dlt Pipeline instance
            data: Data to load

        Returns:
            LoadInfo from pipeline.run()
        """
        return pipeline.run(data)

    def save_load_info(
        self, dataset_name: str, records: list[dict], pipeline: Any = None
    ) -> None:
        """Save load info tracking records to the destination.

        Default implementation uses a full dlt pipeline.run() cycle.
        Destinations should override this with a direct insert for performance.

        Args:
            dataset_name: Target dataset name
            records: List of flat dicts to insert into _saga_load_info
            pipeline: dlt Pipeline instance (used by default implementation)
        """
        if pipeline is None:
            return

        import dlt

        from dlt_saga.project_config import get_load_info_table_name

        load_info_resource = dlt.resource(
            records,
            name=get_load_info_table_name(),
            write_disposition="append",
            columns={
                "pipeline_name": {"data_type": "text"},
                "dataset_name": {"data_type": "text"},
                "destination_name": {"data_type": "text"},
                "destination_type": {"data_type": "text"},
                "started_at": {"data_type": "timestamp"},
                "finished_at": {"data_type": "timestamp"},
                "first_run": {"data_type": "bool"},
                "saved_at": {"data_type": "text"},
                "table_name": {"data_type": "text"},
                "row_count": {"data_type": "bigint"},
            },
        )
        self.run_pipeline(pipeline, load_info_resource)

    def get_last_load_timestamp(
        self, dataset_name: str, pipeline_name: str, table_name: str
    ) -> Optional[datetime]:
        """Get the timestamp of the last successful load that had data.

        Queries _saga_load_info for the most recent load that wrote rows.

        Args:
            dataset_name: Dataset containing _saga_load_info
            pipeline_name: Pipeline identifier
            table_name: Table name to filter for

        Returns:
            datetime of last load with data, or None if no previous loads
        """
        raise NotImplementedError(
            f"{self.__class__.__name__} does not support get_last_load_timestamp"
        )

    def get_max_column_value(self, table_id: str, column: str) -> Any:
        """Get the maximum value of a column in a table.

        Used for incremental loading to determine the cursor position.

        Args:
            table_id: Fully qualified table identifier
            column: Column name to get max value for

        Returns:
            Max value, or None if table doesn't exist or is empty
        """
        raise NotImplementedError(
            f"{self.__class__.__name__} does not support get_max_column_value"
        )

    def execute_sql(self, sql: str, dataset_name: Optional[str] = None) -> Any:
        """Execute a SQL statement against the destination.

        Used by the historize command for running historization queries
        (DDL, DML, and SELECT statements).

        Args:
            sql: SQL statement to execute
            dataset_name: Optional dataset context for the query

        Returns:
            Query result (implementation-specific)

        Raises:
            NotImplementedError: If destination doesn't support direct SQL execution
        """
        raise NotImplementedError(
            f"{self.__class__.__name__} does not support direct SQL execution"
        )

    def supports_transactions(self) -> bool:
        """Return True if destination supports multi-statement transactions.

        When True, the partial refresh runner wraps the DELETE and UPDATE
        rollback phases in explicit BEGIN/COMMIT statements executed via
        separate execute_sql() calls.
        When False, statements are executed individually without transaction
        wrapping. The clone-and-swap pattern provides safety regardless.
        """
        return False

    def clone_table(self, source_table_id: str, target_table_id: str) -> None:
        """Create a writable copy of source_table as target_table.

        BigQuery: CREATE TABLE target CLONE source (zero-copy, copy-on-write).
        Other destinations: CREATE TABLE target AS SELECT * FROM source.

        Args:
            source_table_id: Fully qualified source table identifier.
            target_table_id: Fully qualified target table identifier.
        """
        raise NotImplementedError(
            f"{self.__class__.__name__} does not support clone_table"
        )

    def rename_table(self, table_id: str, new_table_id: str) -> None:
        """Rename a table. Used for the clone-and-swap pattern.

        Args:
            table_id: Fully qualified current table identifier.
            new_table_id: Fully qualified new table identifier.
        """
        raise NotImplementedError(
            f"{self.__class__.__name__} does not support rename_table"
        )

    # -------------------------------------------------------------------------
    # SQL dialect methods — override in subclasses for destination-specific SQL
    # -------------------------------------------------------------------------

    def quote_identifier(self, name: str) -> str:
        """Quote a table or column identifier for this destination.

        Default uses backticks (BigQuery style). Override for other destinations.
        """
        return f"`{name}`"

    def get_full_table_id(self, dataset: str, table: str) -> str:
        """Build a fully qualified table identifier for this destination."""
        raise NotImplementedError(
            f"{self.__class__.__name__} does not implement get_full_table_id"
        )

    def hash_expression(self, columns: list[str]) -> str:
        """Build a SQL expression that hashes multiple columns for change detection.

        Default uses BigQuery's FARM_FINGERPRINT(TO_JSON_STRING(STRUCT(...))).
        Override for other destinations.
        """
        cols = ", ".join(columns)
        return f"FARM_FINGERPRINT(TO_JSON_STRING(STRUCT({cols})))"

    def partition_ddl(self, column: str, col_type: Optional[str] = None) -> str:
        """Return DDL clause for table partitioning, or empty string if unsupported.

        Args:
            column: Partition column name.
            col_type: Optional SQL type hint. BigQuery uses it to choose between
                      ``PARTITION BY DATE(col)`` and ``PARTITION BY col``. Other
                      destinations may ignore it.
        """
        return ""

    def cluster_ddl(self, columns: list[str]) -> str:
        """Return DDL clause for table clustering, or empty string if unsupported."""
        return ""

    def type_name(self, logical_type: str) -> str:
        """Map a logical type name to the destination's SQL type.

        Supported logical types: string, int64, bool, timestamp.
        Default returns BigQuery types.
        """
        type_map = {
            "string": "STRING",
            "int64": "INT64",
            "bool": "BOOL",
            "timestamp": "TIMESTAMP",
        }
        return type_map.get(logical_type, logical_type.upper())

    def cast_to_string(self, expression: str) -> str:
        """Return SQL expression to cast a value to string type."""
        return f"CAST({expression} AS {self.type_name('string')})"

    def columns_query(self, database: str, schema: str, table: str) -> str:
        """Return SQL to discover column names and types from the destination's schema catalog."""
        raise NotImplementedError(
            f"{self.__class__.__name__} does not implement columns_query"
        )

    def ensure_schema_exists(self, schema: str) -> None:
        """Ensure a schema/dataset exists, creating it if necessary.

        Base implementation uses ``CREATE SCHEMA IF NOT EXISTS``.
        Destinations may override for API-based creation or location settings.

        Args:
            schema: Schema/dataset name to ensure exists.
        """
        self.execute_sql(f"CREATE SCHEMA IF NOT EXISTS {self.quote_identifier(schema)}")

    def json_type_name(self) -> str:
        """Return the SQL type name for a JSON column.

        Default returns ``JSON`` (supported by BigQuery, Postgres 14+, etc.).
        Override for destinations with different JSON support.
        """
        return "JSON"

    def parse_json_expression(self, value_expr: str) -> str:
        """Return SQL to parse a string expression into a JSON value.

        Args:
            value_expr: SQL expression that evaluates to a JSON string.

        Default uses ``PARSE_JSON(expr)`` (BigQuery). Override for other
        destinations (e.g. ``expr::jsonb`` for Postgres).
        """
        return f"PARSE_JSON({value_expr})"

    def extract_json_value(self, json_expr: str) -> str:
        """Return SQL to extract a JSON column back to a string.

        Used when reading JSON data and converting back to a dict.
        Default uses ``TO_JSON_STRING(expr)`` (BigQuery). Override for
        destinations where JSON columns are returned as strings already.
        """
        return f"TO_JSON_STRING({json_expr})"

    def current_timestamp_expression(self) -> str:
        """Return SQL expression for the current timestamp.

        Default returns ``CURRENT_TIMESTAMP()``.
        """
        return "CURRENT_TIMESTAMP()"

    def timestamp_n_days_ago(self, days: int) -> str:
        """Return a SQL expression for a timestamp ``days`` days in the past.

        Default uses standard SQL interval arithmetic:
        ``CURRENT_TIMESTAMP() - INTERVAL 'N' DAY``.
        Destinations that require a different dialect (e.g. BigQuery's
        ``TIMESTAMP_SUB``) should override this method.

        Args:
            days: Number of days to subtract from the current timestamp.

        Returns:
            SQL expression string (no trailing semicolon).
        """
        return f"CURRENT_TIMESTAMP() - INTERVAL '{days}' DAY"

    def create_or_replace_view(
        self, schema: str, view_name: str, view_sql: str
    ) -> None:
        """Create or replace a SQL view.

        Base implementation uses ``CREATE OR REPLACE VIEW``.

        Args:
            schema: Schema containing the view.
            view_name: Name of the view.
            view_sql: SELECT statement for the view body.
        """
        full_view_id = self.get_full_table_id(schema, view_name)
        ddl = f"CREATE OR REPLACE VIEW {full_view_id} AS {view_sql}"
        self.execute_sql(ddl, schema)

    def reset_destination_state(self, pipeline_name: str, table_name: str) -> None:
        """Reset destination state by dropping tables and metadata.

        Called before pipeline execution when full_refresh=True.
        Removes all destination-side resources (tables, state, metadata) to enable fresh reload.
        Base implementation does nothing - destinations override to provide cleanup.

        Args:
            pipeline_name: Pipeline name for state cleanup
            table_name: Main table name to drop
        """
        pass

    # -------------------------------------------------------------------------
    # Native-load contract — override on BigQuery and Databricks
    # -------------------------------------------------------------------------

    def supports_native_load(self) -> bool:
        """Return True if destination implements native_load_chunk."""
        return False

    def supported_native_load_uri_schemes(self) -> set:
        """Return the set of URI schemes supported for native_load (e.g. {'gs', 's3'})."""
        return set()

    def native_load_chunk(self, spec: "NativeLoadSpec") -> "NativeLoadResult":
        """Load one chunk of source URIs directly into the warehouse.

        Raises:
            NotImplementedError: if the destination does not support native_load.
        """
        raise NotImplementedError(
            f"{self.__class__.__name__} does not support native_load. "
            "Set adapter: dlt_saga.native_load only on bigquery or databricks destinations."
        )

    def native_load_file_name_expr(self) -> str:
        """Return the SQL expression that yields the source file path for each row.

        BigQuery: ``_FILE_NAME``.
        Databricks: ``_metadata.file_path``.
        """
        raise NotImplementedError(
            f"{self.__class__.__name__} does not implement native_load_file_name_expr"
        )

    def parse_filename_timestamp_expr(
        self, file_name_expr: str, regex_literal: str, format_literal: str
    ) -> str:
        """Return a SQL expression that extracts a TIMESTAMP from a file path string.

        Returns NULL on regex miss — never falls back to CURRENT_TIMESTAMP().

        Args:
            file_name_expr: SQL expression for the file path (e.g. ``_FILE_NAME``).
            regex_literal: Already-escaped regex string with one capture group.
            format_literal: Already-escaped strftime format string.
        """
        raise NotImplementedError(
            f"{self.__class__.__name__} does not implement parse_filename_timestamp_expr"
        )

    def table_exists(self, dataset: str, table: str) -> bool:
        """Return True if the table exists in the given dataset."""
        raise NotImplementedError(
            f"{self.__class__.__name__} does not implement table_exists"
        )

    def drop_table(self, dataset: str, table: str) -> None:
        """Drop a table; no-op if it does not exist."""
        raise NotImplementedError(
            f"{self.__class__.__name__} does not implement drop_table"
        )

    def list_table_columns(self, dataset: str, table: str) -> list:
        """Return [(column_name, sql_type), ...] for the given table."""
        raise NotImplementedError(
            f"{self.__class__.__name__} does not implement list_table_columns"
        )

    def add_column(self, dataset: str, table: str, column: str, type_name: str) -> None:
        """Add a column to an existing table."""
        raise NotImplementedError(
            f"{self.__class__.__name__} does not implement add_column"
        )

    def execute_sql_with_job(self, sql: str, schema: Optional[str] = None) -> tuple:
        """Execute SQL and return (rows, job_id) for traceability."""
        raise NotImplementedError(
            f"{self.__class__.__name__} does not implement execute_sql_with_job"
        )

    def dlt_type_to_native(self, dlt_type: str) -> str:
        """Convert a dlt logical type (e.g. 'timestamp') to the destination's SQL type.

        Falls back to the input uppercased if not recognized, so native types
        like 'INT64' or 'FLOAT64' pass through unchanged.
        """
        return dlt_type.upper()

    def list_tables_by_pattern(self, dataset: str, pattern: str) -> list:
        """Return table names matching a SQL LIKE pattern in the given dataset."""
        return []

    def build_historize_create_table_sql(
        self,
        create_clause: str,
        target_table_id: str,
        select_body: str,
        partition_column: Optional[str],
        cluster_columns: Optional[list],
        table_format: str = "native",
        table_name: str = "",
        schema: str = "",
        source_database: str = "",
        source_schema: str = "",
        source_table: str = "",
    ) -> str:
        """Build the CREATE TABLE DDL for a historize target table.

        Default produces a native CTAS with optional partition/cluster.
        BigQuery and Databricks override this to add format-specific clauses
        (Iceberg OPTIONS, USING DELTA, TBLPROPERTIES, etc.).

        Args:
            create_clause: ``CREATE OR REPLACE TABLE`` or ``CREATE TABLE IF NOT EXISTS``.
            target_table_id: Fully-qualified target table identifier.
            select_body: The ``SELECT ... FROM src WHERE FALSE`` body for CTAS.
            partition_column: Raw partition column name (or None).
            cluster_columns: Raw cluster column list (or None).
            table_format: Resolved table format ("native", "iceberg", "delta", "delta_uniform").
            table_name: Bare target table name (for storage URI construction).
            schema: Bare target schema/dataset name (for storage URI construction).
            source_database: Source database/project (for column-type discovery).
            source_schema: Source schema/dataset (for column-type discovery).
            source_table: Source table name (for column-type discovery).

        Returns:
            Complete DDL string ready for execution.
        """
        partition = self.partition_ddl(partition_column) if partition_column else ""
        cluster = self.cluster_ddl(cluster_columns) if cluster_columns else ""
        parts = [f"{create_clause} {target_table_id}"]
        if partition:
            parts.append(partition)
        if cluster:
            parts.append(cluster)
        parts.extend(["AS", select_body])
        return "\n".join(parts)


class AccessManager(ABC):
    """Abstract base class for destination-specific access management.

    Different destinations have different access control mechanisms:
    - BigQuery: IAM policies at table level
    - Snowflake: GRANT statements for roles
    - Postgres: GRANT statements for users
    """

    @abstractmethod
    def manage_access_for_tables(
        self,
        table_ids: list[str],
        access_config: Optional[list[str]],
        revoke_extra: bool = True,
    ) -> None:
        """Manage access permissions for multiple tables.

        Args:
            table_ids: List of table identifiers (format varies by destination)
            access_config: List of access entries (e.g., ["user:email@example.com"])
                          None means don't manage access, [] means revoke all access
            revoke_extra: Whether to revoke permissions not in access_config
        """
        pass

    @abstractmethod
    def parse_access_list(self, access_list: list[str]) -> dict[str, set[str]]:
        """Parse access list from config into structured format.

        Args:
            access_list: List of access strings from config

        Returns:
            Dict categorizing access by type (users, groups, serviceAccounts, etc.)
        """
        pass
