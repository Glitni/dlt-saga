"""BigQuery destination implementation."""

import logging
from datetime import datetime
from typing import Any, Optional

from dlt_saga.destinations.bigquery.base import BigQueryBaseDestination
from dlt_saga.destinations.bigquery.config import BigQueryDestinationConfig

logger = logging.getLogger(__name__)


class BigQueryDestination(BigQueryBaseDestination):
    """BigQuery destination implementation.

    Supports both native BigQuery tables and BigLake Iceberg tables.
    Table format is controlled by config.table_format:
    - "native": Standard BigQuery tables (default)
    - "iceberg": BigLake Iceberg tables with Parquet storage

    Provides functionality including:
    - dlt BigQuery destination creation
    - Partitioning and clustering hints
    - IAM-based access management
    - Client pooling
    - Iceberg table creation and management
    """

    config: BigQueryDestinationConfig  # Type narrowing for mypy

    def __init__(self, config: BigQueryDestinationConfig):
        """Initialize BigQuery destination.

        Args:
            config: BigQuery-specific configuration
        """
        from dlt_saga.utility.optional_deps import require_optional

        require_optional("google.cloud.bigquery", "BigQuery destination")
        super().__init__(config)
        self._access_manager: Optional[Any] = None
        self._client_pool: Optional[Any] = None

    def _get_bigquery_destination_kwargs(self) -> dict:
        """Get BigQuery destination kwargs.

        For Iceberg tables, adds max_table_nesting=0 to flatten nested structures
        since Iceberg doesn't support JSON type.

        Returns:
            Dictionary of additional kwargs
        """
        if self.config.table_format == "iceberg":
            return {"max_table_nesting": 0}
        return {}

    @staticmethod
    def _normalize_column_name(name: str) -> str:
        """Normalize a column name using dlt's snake_case naming convention.

        Ensures column references in hints (partition_column, cluster_columns)
        match the actual normalized column names that dlt creates in BigQuery.

        Args:
            name: Original column name (e.g. "OrderItem_ID")

        Returns:
            Normalized column name (e.g. "order_item_id")
        """
        from dlt.common.normalizers.naming.snake_case import NamingConvention

        return NamingConvention().normalize_identifier(name)

    def apply_hints(self, resource: Any, **hints) -> Any:
        """Apply hints to a dlt resource.

        For native tables: Uses bigquery_adapter
        For Iceberg tables: Stores hints for use during table creation

        Supported hints:
        - table_description: Table description
        - partition_column: Column to partition by (typically a date/timestamp)
        - cluster_columns: List of columns to cluster by

        Column names in partition_column and cluster_columns are normalized
        to match dlt's snake_case convention (e.g. "OrderItem_ID" -> "order_item_id").
        This may produce harmless dlt "collides with other column" warnings since
        dlt also normalizes the original column names internally.

        Unknown hints are silently ignored, making this method generic
        and compatible with destination-agnostic pipeline code.

        Args:
            resource: dlt resource to apply hints to
            **hints: Destination hints dictionary

        Returns:
            Resource (wrapped with adapter for native, stored hints for Iceberg)
        """
        # Normalize column name references to match dlt's snake_case convention
        if "partition_column" in hints and hints["partition_column"]:
            hints["partition_column"] = self._normalize_column_name(
                hints["partition_column"]
            )
        if "cluster_columns" in hints and hints["cluster_columns"]:
            hints["cluster_columns"] = [
                self._normalize_column_name(c) for c in hints["cluster_columns"]
            ]

        if self.config.table_format == "iceberg":
            return self._apply_iceberg_hints(resource, hints)
        return self._apply_native_hints(resource, hints)

    def _apply_iceberg_hints(self, resource: Any, hints: dict) -> Any:
        """Store hints for Iceberg table creation during run_pipeline."""
        if not hasattr(resource, "_biglake_hints"):
            resource._biglake_hints = {}

        resource._biglake_hints.update(hints)

        if "partition_column" in hints:
            logger.debug(
                f"Partition hint for {resource.name}: {hints['partition_column']}"
            )
        if "cluster_columns" in hints:
            logger.debug(
                f"Cluster hint for {resource.name}: {hints['cluster_columns']}"
            )

        return resource

    @staticmethod
    def _apply_native_hints(resource: Any, hints: dict) -> Any:
        """Apply hints via bigquery_adapter for native BigQuery tables."""
        from dlt.destinations.adapters import bigquery_adapter

        adapter_args = {}

        if "table_description" in hints and hints["table_description"]:
            adapter_args["table_description"] = hints["table_description"]

        if "partition_column" in hints and hints["partition_column"]:
            adapter_args["partition"] = hints["partition_column"]

        if "cluster_columns" in hints and hints["cluster_columns"]:
            adapter_args["cluster"] = hints["cluster_columns"]

        if adapter_args:
            logger.debug(f"Applying BigQuery adapter with args: {adapter_args}")
            return bigquery_adapter(resource, **adapter_args)

        return resource

    def get_client_pool(self) -> Any:
        """Get BigQuery client pool for executing queries.

        Lazily initializes the client pool on first access.

        Returns:
            BigQuery client pool singleton
        """
        if self._client_pool is None:
            from dlt_saga.utility.gcp.client_pool import bigquery_pool

            self._client_pool = bigquery_pool
            logger.debug("Retrieved BigQuery client pool")

        return self._client_pool

    @classmethod
    def prepare_for_execution(cls, pipeline_configs: list[Any]) -> None:
        """Pre-create all unique datasets needed by the pipelines.

        Extends base implementation to also create staging datasets that
        BigQuery uses for merge/replace operations.

        Args:
            pipeline_configs: List of PipelineConfig objects that will be executed
        """
        from dlt_saga.utility.cli.context import get_execution_context

        context = get_execution_context()

        # Collect all unique (project, location, dataset_name, dataset_access) combinations
        datasets_to_create = set()

        for config in pipeline_configs:
            # Get values using same logic as BasePipeline
            project = context.get_database() or config.config_dict.get("gcp_project_id")
            location = context.get_location() or config.config_dict.get(
                "location", "EU"
            )
            # schema_name is already resolved by ConfigSource during discovery
            dataset_name = config.schema_name
            dataset_access = config.config_dict.get("dataset_access")

            if project and dataset_name:
                # Add main dataset
                datasets_to_create.add(
                    (
                        project,
                        location,
                        dataset_name,
                        tuple(dataset_access) if dataset_access else None,
                    )
                )

                # Add staging dataset (DLT creates this for merge/replace operations)
                staging_dataset_name = f"{dataset_name}_staging"
                datasets_to_create.add(
                    (
                        project,
                        location,
                        staging_dataset_name,
                        tuple(dataset_access) if dataset_access else None,
                    )
                )

        # Create all unique datasets
        if datasets_to_create:
            from google.cloud import bigquery

            # Use billing_project for the client (job execution), falling back to data project
            billing_project = None
            if context.profile_target:
                billing_project = context.profile_target.billing_project
            first = next(iter(datasets_to_create))
            client_project = billing_project or first[0]
            client = bigquery.Client(project=client_project, location=first[1])

            for (
                project,
                location,
                dataset_name,
                dataset_access_tuple,
            ) in datasets_to_create:
                dataset_access = (
                    list(dataset_access_tuple) if dataset_access_tuple else None
                )

                # Use static method to sync dataset and access
                cls._sync_dataset_and_access_static(
                    project_id=project,
                    location=location,
                    dataset_name=dataset_name,
                    dataset_access=dataset_access,
                    client=client,
                )
                cls._synced_datasets.add((project, dataset_name))

    def run_pipeline(self, pipeline: Any, data: Any) -> Any:
        """Run pipeline, ensuring datasets exist first to prevent race conditions.

        For native tables: Ensures datasets exist
        For Iceberg tables: Pre-creates minimal Iceberg table structure

        Args:
            pipeline: dlt Pipeline instance
            data: Data to load

        Returns:
            LoadInfo from pipeline.run()
        """
        if self.config.table_format == "iceberg":
            # Ensure dataset exists
            dataset_name = pipeline.dataset_name
            if dataset_name:
                self.sync_dataset_and_access(dataset_name)

            # Check if this is a data table (not a dlt system table)
            table_name = None
            if hasattr(data, "name"):
                table_name = data.name

            from dlt_saga.project_config import get_load_info_table_name

            is_data_table = table_name and not (
                table_name.startswith("_dlt_")
                or table_name == get_load_info_table_name()
            )

            # Pre-create Iceberg table for data tables
            if is_data_table:
                hints = getattr(data, "_biglake_hints", {})

                # Extract primary key columns from dlt resource schema.
                # These must be included in CREATE TABLE since BigQuery Iceberg
                # doesn't allow adding NOT NULL fields via schema evolution.
                primary_key = None
                columns = None
                if hasattr(data, "compute_table_schema"):
                    schema_columns = data.compute_table_schema().get("columns", {})
                    primary_key = [
                        name
                        for name, col in schema_columns.items()
                        if col.get("primary_key")
                    ]
                    if primary_key:
                        columns = schema_columns

                self._create_iceberg_table_if_not_exists(
                    table_name,
                    partition_column=hints.get("partition_column"),
                    cluster_columns=hints.get("cluster_columns"),
                    primary_key=primary_key,
                    columns=columns,
                )

            # Let dlt handle all loading
            return pipeline.run(data)
        else:
            # Native BigQuery tables
            # Sync main dataset and access controls before running pipeline
            # This prevents race conditions in parallel execution
            self.sync_dataset_and_access(pipeline.dataset_name)

            # DLT also creates a staging dataset - sync it too
            staging_dataset_name = f"{pipeline.dataset_name}_staging"
            self.sync_dataset_and_access(staging_dataset_name)

            # Now run pipeline normally - datasets are guaranteed to exist
            return pipeline.run(data)

    def save_load_info(
        self, dataset_name: str, records: list[dict], pipeline: Any = None
    ) -> None:
        """Save load info records via DML INSERT.

        Uses parameterized DML INSERT instead of streaming inserts to avoid
        BigQuery streaming buffer limitations (rows in the streaming buffer
        cannot be targeted by UPDATE/DELETE, which breaks --full-refresh).

        Creates the _saga_load_info table on first use.

        Args:
            dataset_name: Target dataset name
            records: List of flat dicts to insert into _saga_load_info
            pipeline: Unused (kept for interface compatibility)
        """
        from google.cloud import bigquery
        from google.cloud.exceptions import NotFound

        if not records:
            return

        # Serialize datetime/pendulum objects to ISO strings for JSON transport
        from datetime import datetime

        serialized = []
        for record in records:
            row = {}
            for k, v in record.items():
                row[k] = v.isoformat() if isinstance(v, datetime) else v
            serialized.append(row)

        from dlt_saga.project_config import get_load_info_table_name

        table_id = (
            f"{self.config.project_id}.{dataset_name}.{get_load_info_table_name()}"
        )
        client = bigquery.Client(
            project=self.config.job_project_id, location=self.config.location
        )

        try:
            self._insert_load_info_dml(client, table_id, serialized)
        except NotFound:
            # Table doesn't exist yet — create it, then retry
            logger.debug("Load info table not found, creating it")
            ddl = f"""
                CREATE TABLE IF NOT EXISTS `{table_id}` (
                    pipeline_name STRING,
                    destination_name STRING,
                    destination_type STRING,
                    dataset_name STRING,
                    table_name STRING,
                    row_count INT64,
                    started_at TIMESTAMP,
                    finished_at TIMESTAMP,
                    first_run BOOL,
                    saved_at STRING,
                    _dlt_load_id STRING NOT NULL,
                    _dlt_id STRING NOT NULL
                )
            """
            client.query(ddl).result()

            self._insert_load_info_dml(client, table_id, serialized)

    @staticmethod
    def _insert_load_info_dml(client: Any, table_id: str, rows: list[dict]) -> None:
        """Insert load info records via DML query.

        Generates unique _dlt_load_id and _dlt_id values for each row since
        dlt adds these as required NOT NULL columns to the table schema.
        """
        import uuid

        from google.cloud import bigquery

        columns = [
            "pipeline_name",
            "destination_name",
            "destination_type",
            "dataset_name",
            "table_name",
            "row_count",
            "started_at",
            "finished_at",
            "first_run",
            "saved_at",
            "_dlt_load_id",
            "_dlt_id",
        ]
        col_names = ", ".join(columns)
        placeholders = ", ".join(f"@{c}" for c in columns)
        insert_sql = f"INSERT INTO `{table_id}` ({col_names}) VALUES ({placeholders})"

        param_types = {
            "row_count": "INT64",
            "first_run": "BOOL",
            "started_at": "TIMESTAMP",
            "finished_at": "TIMESTAMP",
        }

        load_id = str(uuid.uuid4())
        for row in rows:
            row["_dlt_load_id"] = load_id
            row["_dlt_id"] = str(uuid.uuid4())
            params = [
                bigquery.ScalarQueryParameter(
                    c, param_types.get(c, "STRING"), row.get(c)
                )
                for c in columns
            ]
            job_config = bigquery.QueryJobConfig(query_parameters=params)
            client.query(insert_sql, job_config=job_config).result()

    def get_last_load_timestamp(
        self, dataset_name: str, pipeline_name: str, table_name: str
    ) -> Optional[datetime]:
        """Get the timestamp of the last successful load that had data."""
        from google.cloud import bigquery

        from dlt_saga.project_config import get_load_info_table_name

        try:
            client = bigquery.Client(
                project=self.config.job_project_id, location=self.config.location
            )
            query = f"""
                SELECT MAX(started_at) as started_at
                FROM `{self.config.project_id}.{dataset_name}.{get_load_info_table_name()}`
                WHERE pipeline_name = @pipeline_name
                AND table_name = @table_name
                AND row_count > 0
            """
            job_config = bigquery.QueryJobConfig(
                query_parameters=[
                    bigquery.ScalarQueryParameter(
                        "pipeline_name", "STRING", pipeline_name
                    ),
                    bigquery.ScalarQueryParameter("table_name", "STRING", table_name),
                ]
            )
            results = list(client.query(query, job_config=job_config).result())
            if results and results[0].started_at:
                return results[0].started_at
            return None
        except Exception:
            return None

    def get_max_column_value(self, table_id: str, column: str) -> Any:
        """Get the maximum value of a column in a BigQuery table."""
        from google.cloud import bigquery

        try:
            client = bigquery.Client(
                project=self.config.job_project_id, location=self.config.location
            )
            query = f"SELECT MAX(`{column}`) as max_value FROM `{table_id}`"
            results = list(client.query(query).result())
            if results and results[0].max_value is not None:
                return results[0].max_value
            return None
        except Exception:
            return None

    def execute_sql(self, sql: str, dataset_name: Optional[str] = None) -> Any:
        """Execute a SQL statement against BigQuery.

        Args:
            sql: SQL statement to execute
            dataset_name: Optional default dataset for unqualified table references

        Returns:
            BigQuery RowIterator with query results
        """
        from google.cloud import bigquery

        client = bigquery.Client(
            project=self.config.job_project_id, location=self.config.location
        )

        job_config = bigquery.QueryJobConfig()
        if dataset_name:
            job_config.default_dataset = f"{self.config.project_id}.{dataset_name}"

        logger.debug(f"Executing SQL ({len(sql)} chars) in dataset={dataset_name}")
        result = client.query(sql, job_config=job_config).result(timeout=120)
        return result

    # -------------------------------------------------------------------------
    # SQL dialect overrides
    # -------------------------------------------------------------------------

    def quote_identifier(self, name: str) -> str:
        return f"`{name}`"

    def get_full_table_id(self, dataset: str, table: str) -> str:
        return f"`{self.config.project_id}.{dataset}.{table}`"

    def timestamp_n_days_ago(self, days: int) -> str:
        return f"TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {days} DAY)"

    def hash_expression(self, columns: list[str]) -> str:
        cols = ", ".join(columns)
        return f"FARM_FINGERPRINT(TO_JSON_STRING(STRUCT({cols})))"

    def partition_ddl(self, column: str) -> str:
        return f"PARTITION BY DATE({column})"

    def cluster_ddl(self, columns: list[str]) -> str:
        if not columns:
            return ""
        return f"CLUSTER BY {', '.join(columns)}"

    def type_name(self, logical_type: str) -> str:
        type_map = {
            "string": "STRING",
            "int64": "INT64",
            "bool": "BOOL",
            "timestamp": "TIMESTAMP",
        }
        return type_map.get(logical_type, logical_type.upper())

    def ensure_schema_exists(self, schema: str) -> None:
        """Ensure a BigQuery dataset exists, creating it with the configured location."""
        from google.cloud import bigquery

        client = bigquery.Client(
            project=self.config.job_project_id, location=self.config.location
        )
        dataset_ref = bigquery.DatasetReference(self.config.project_id, schema)
        try:
            client.get_dataset(dataset_ref)
        except Exception:
            dataset = bigquery.Dataset(dataset_ref)
            dataset.location = self.config.location
            client.create_dataset(dataset)

    # supports_transactions() is intentionally not overridden (returns False).
    # BigQuery supports BEGIN TRANSACTION / COMMIT at the SQL level, but
    # execute_sql() creates a separate query job per call, so transaction
    # state does not persist across calls.  The clone-and-swap pattern in
    # the partial-refresh runner protects the live table regardless.

    def clone_table(self, source_table_id: str, target_table_id: str) -> None:
        """Create a zero-copy BigQuery clone of source_table as target_table."""
        sql = f"CREATE TABLE {target_table_id} CLONE {source_table_id}"
        self.execute_sql(sql)

    def rename_table(self, table_id: str, new_table_id: str) -> None:
        """Rename a BigQuery table using ALTER TABLE ... RENAME TO.

        BigQuery's RENAME TO clause takes only the new table name — no project
        or dataset prefix. Extracts the name from the fully-qualified new_table_id.
        """
        new_name = new_table_id.strip("`").split(".")[-1]
        sql = f"ALTER TABLE {table_id} RENAME TO `{new_name}`"
        self.execute_sql(sql)

    def columns_query(self, database: str, schema: str, table: str) -> str:
        safe_table = table.replace("'", "''")
        return f"""
            SELECT column_name, data_type
            FROM `{database}.{schema}.INFORMATION_SCHEMA.COLUMNS`
            WHERE table_name = '{safe_table}'
            ORDER BY ordinal_position
        """

    def reset_destination_state(self, pipeline_name: str, table_name: str) -> None:
        """Reset destination state by dropping tables and metadata.

        For native tables:
        - Main table
        - Staging table
        - Pipeline state metadata (_dlt_pipeline_state)
        - Load info tracking data (_dlt_load_info)
        - Schema version info (_dlt_version) from BOTH main and staging datasets

        For Iceberg tables:
        - BigLake Iceberg table from BigQuery
        - Table data from GCS storage_uri
        - Load info tracking data

        Args:
            pipeline_name: Pipeline name for state cleanup
            table_name: Table name to drop
        """
        from google.cloud import bigquery

        logger.info(f"Full refresh: Resetting destination state for {pipeline_name}")

        staging_dataset = f"{self.config.dataset_name}_staging"

        # Drop main table
        main_table_id = (
            f"{self.config.project_id}.{self.config.dataset_name}.{table_name}"
        )
        self._drop_table(main_table_id, "main table")

        # Drop staging table
        staging_table_id = f"{self.config.project_id}.{staging_dataset}.{table_name}"
        self._drop_table(staging_table_id, "staging table")

        # Iceberg-specific: purge GCS storage
        if self.config.table_format == "iceberg":
            try:
                from google.cloud import storage  # type: ignore[attr-defined]

                storage_uri = self._get_storage_uri(table_name)
                logger.debug(f"Purging Iceberg data from storage_uri: {storage_uri}")

                # Parse storage_uri (gs://bucket/dataset/table/ -> bucket, dataset/table/)
                path_without_scheme = storage_uri[5:]  # Remove "gs://"
                parts = path_without_scheme.split("/", 1)
                bucket_name = parts[0]
                prefix = parts[1] if len(parts) > 1 else ""

                # Delete all objects under this prefix
                storage_client = storage.Client(project=self.config.job_project_id)
                bucket = storage_client.bucket(bucket_name)

                blobs = list(bucket.list_blobs(prefix=prefix))
                if blobs:
                    bucket.delete_blobs(blobs)
                    logger.info(
                        f"Purged {len(blobs)} Iceberg file(s) from GCS for table {table_name}"
                    )
                else:
                    logger.debug(
                        f"No Iceberg files found to purge for table {table_name}"
                    )

            except Exception as e:
                logger.debug(f"Could not purge Iceberg data from GCS: {e}")

        # Common dlt metadata cleanup for all table formats

        # Clean up pipeline state
        try:
            state_table_id = self._get_dlt_table_id("_dlt_pipeline_state")
            self._delete_from_table(
                state_table_id,
                "pipeline_name = @pipeline_name",
                [
                    bigquery.ScalarQueryParameter(
                        "pipeline_name", "STRING", pipeline_name
                    )
                ],
            )
            logger.debug(f"Cleaned up pipeline state for {pipeline_name}")
        except Exception as e:
            logger.debug(f"No pipeline state to clean: {e}")

        # Clean up load info (used for change detection)
        self._cleanup_load_info_table(pipeline_name, table_name)

        # Clean up schema version info (dlt uses this for schema evolution)
        # If not cleaned, dlt may query non-existent tables based on cached schema
        # dlt normalizes schema names: handles length limits, special chars, etc.
        # Schema names are limited to 64 characters and get truncated with hashing
        from dlt.common.normalizers.naming.snake_case import NamingConvention

        # Pass max_length to constructor to enable truncation with hashing
        naming = NamingConvention(max_length=64)
        normalized_schema_name = naming.normalize_identifier(pipeline_name)

        try:
            version_table_id = self._get_dlt_table_id("_dlt_version")
            self._delete_from_table(
                version_table_id,
                "schema_name = @schema_name",
                [
                    bigquery.ScalarQueryParameter(
                        "schema_name", "STRING", normalized_schema_name
                    )
                ],
            )
            logger.debug(
                f"Cleaned up schema version from main dataset for {pipeline_name}"
            )
        except Exception as e:
            logger.debug(f"No schema version in main dataset to clean: {e}")

        # Clean up schema version from STAGING dataset
        # The staging dataset has its own _dlt_version table with schema metadata
        try:
            staging_version_table_id = (
                f"{self.config.project_id}.{staging_dataset}._dlt_version"
            )
            self._delete_from_table(
                staging_version_table_id,
                "schema_name = @schema_name",
                [
                    bigquery.ScalarQueryParameter(
                        "schema_name", "STRING", normalized_schema_name
                    )
                ],
            )
            logger.debug(
                f"Cleaned up schema version from staging dataset for {pipeline_name}"
            )
        except Exception as e:
            logger.debug(f"No schema version in staging dataset to clean: {e}")

    # Iceberg-specific methods

    def _get_storage_uri(self, table_name: str) -> str:
        """Get GCS storage URI for an Iceberg table.

        Args:
            table_name: Table name

        Returns:
            Storage URI in format gs://bucket/base_path/dataset/table/
        """
        storage_path = self.config.storage_path
        if not storage_path.startswith("gs://"):
            raise ValueError(f"storage_path must start with gs://: {storage_path}")

        # Ensure trailing slash
        if not storage_path.endswith("/"):
            storage_path = f"{storage_path}/"

        # Storage URI pattern: {storage_path}{dataset}/{table}/
        # e.g., gs://bucket/dlt/dataset_name/table_name/
        storage_uri = f"{storage_path}{self.config.dataset_name}/{table_name}/"
        return storage_uri

    # Mapping from dlt data types to BigQuery SQL types
    DLT_TO_BIGQUERY_TYPE = {
        "text": "STRING",
        "double": "FLOAT64",
        "bool": "BOOL",
        "timestamp": "TIMESTAMP",
        "bigint": "INT64",
        "date": "DATE",
        "time": "TIME",
        "decimal": "NUMERIC",
        "binary": "BYTES",
        "complex": "STRING",
        "wei": "BIGNUMERIC",
    }

    def _build_create_table_ddl(
        self,
        table_name: str,
        schema: dict,
        partition_column: Optional[str] = None,
        cluster_columns: Optional[list] = None,
    ) -> str:
        """Build CREATE TABLE IF NOT EXISTS DDL for BigLake Iceberg table.

        Args:
            table_name: Table name
            schema: Column definitions dict. Each value can be a dict with
                'data_type' (dlt type) or just a plain dict (defaults to STRING).
            partition_column: Optional partition column
            cluster_columns: Optional clustering columns

        Returns:
            CREATE TABLE DDL string
        """
        table_id = f"{self.config.project_id}.{self.config.dataset_name}.{table_name}"
        storage_uri = self._get_storage_uri(table_name)

        # Build column definitions with proper type mapping
        column_defs = []
        for col_name, col_config in schema.items():
            dlt_type = (
                col_config.get("data_type", "text")
                if isinstance(col_config, dict)
                else "text"
            )
            bq_type = self.DLT_TO_BIGQUERY_TYPE.get(dlt_type, "STRING")
            # Quote column names with backticks to handle reserved keywords
            quoted_col = f"`{col_name}`"
            column_defs.append(
                f"  {quoted_col} {bq_type} NOT NULL"
                if col_config.get("required")
                else f"  {quoted_col} {bq_type}"
            )

        columns_sql = ",\n".join(column_defs)

        # Build DDL
        ddl_parts = [f"CREATE TABLE IF NOT EXISTS `{table_id}` (", columns_sql, ")"]

        # Normalize column names to match dlt's snake_case convention
        from dlt.common.normalizers.naming.snake_case import NamingConvention

        naming = NamingConvention()

        # Add partitioning
        if partition_column:
            normalized = naming.normalize_identifier(partition_column)
            ddl_parts.append(f"PARTITION BY DATE({normalized})")

        # Add clustering
        if cluster_columns:
            normalized_cols = [naming.normalize_identifier(c) for c in cluster_columns]
            cluster_cols = ", ".join(normalized_cols)
            ddl_parts.append(f"CLUSTER BY {cluster_cols}")

        # Add Iceberg OPTIONS
        ddl_parts.extend(
            [
                "WITH CONNECTION DEFAULT",
                "OPTIONS (",
                "  file_format = 'PARQUET',",
                "  table_format = 'ICEBERG',",
                f"  storage_uri = '{storage_uri}'",
                ")",
            ]
        )

        return "\n".join(ddl_parts)

    def _create_iceberg_table_if_not_exists(
        self,
        table_name: str,
        partition_column: Optional[str] = None,
        cluster_columns: Optional[list] = None,
        primary_key: Optional[list] = None,
        columns: Optional[dict] = None,
    ) -> None:
        """Create BigLake Iceberg table if it doesn't exist.

        Creates table with dlt system columns plus any primary key columns
        (which must be included upfront since BigQuery Iceberg doesn't allow
        adding required/NOT NULL fields via schema evolution).

        Args:
            table_name: Table name
            partition_column: Optional partition column
            cluster_columns: Optional clustering columns
            primary_key: Optional list of primary key column names
            columns: Optional column definitions dict from config
        """
        from google.cloud import bigquery
        from google.cloud.exceptions import NotFound

        client = bigquery.Client(
            project=self.config.job_project_id, location=self.config.location
        )

        table_id = f"{self.config.project_id}.{self.config.dataset_name}.{table_name}"

        try:
            # Check if table already exists
            client.get_table(table_id)

            # Table exists - don't recreate it
            logger.debug(f"Table already exists: {table_name}")
            return

        except NotFound:
            pass  # Table doesn't exist, create it

        try:
            # Start with dlt system columns
            schema: dict[str, dict[str, Any]] = {
                "_dlt_load_id": {"data_type": "text"},
                "_dlt_id": {"data_type": "text"},
            }

            # Include primary key columns upfront as NOT NULL.
            # BigQuery Iceberg doesn't allow adding required fields via schema evolution,
            # so they must exist from table creation.
            if primary_key:
                if not columns:
                    raise ValueError(
                        f"Iceberg table '{table_name}' has primary_key={primary_key} but no "
                        f"'columns' configuration. Add all primary key columns to the 'columns' "
                        f"config with data_type to avoid schema evolution errors."
                    )
                for pk_col in primary_key:
                    if pk_col in columns:
                        schema[pk_col] = {**columns[pk_col], "required": True}
                    else:
                        raise ValueError(
                            f"Iceberg table '{table_name}' has primary_key column '{pk_col}' "
                            f"but it's not defined in 'columns'. Add it with a data_type "
                            f'(e.g., columns:\n  {pk_col}:\n    data_type: "text") '
                            f"to the config file. BigQuery Iceberg cannot add NOT NULL fields "
                            f"to existing tables via schema evolution."
                        )

            ddl = self._build_create_table_ddl(
                table_name, schema, partition_column, cluster_columns
            )

            logger.info(f"Creating BigLake Iceberg table: {table_name}")
            logger.debug(f"DDL:\n{ddl}")

            client.query(ddl).result()

            logger.info(
                f"Successfully created BigLake Iceberg table: {table_name}. "
                f"Primary key columns included: {primary_key or 'none'}. "
                f"Remaining columns will be added via schema evolution."
            )

        except Exception as e:
            logger.error(
                f"Failed to create Iceberg table {table_name}: {e}", exc_info=True
            )
            raise
