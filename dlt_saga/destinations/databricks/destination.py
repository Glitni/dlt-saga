"""Databricks Unity Catalog destination implementation."""

from __future__ import annotations

import logging
import threading
import uuid
from datetime import datetime
from typing import TYPE_CHECKING, Any, Dict, Optional

from dlt_saga.destinations.base import Destination
from dlt_saga.destinations.databricks.config import DatabricksDestinationConfig
from dlt_saga.utility.sql import looks_like_missing_table

if TYPE_CHECKING:
    from dlt_saga.destinations.base import MaterializationHints

logger = logging.getLogger(__name__)

# databricks-sql-connector logs a spurious
#   "ThriftBackend.attempt_request: Exception: I/O operation on closed file"
# at ERROR from its thrift teardown path *after* the session has already closed
# and the work has completed successfully — cosmetic noise, not a real failure.
_THRIFT_CLOSED_FILE_NOISE = "I/O operation on closed file"

# Connector logger names across versions: >= 4 nests under `backend`.
_THRIFT_LOGGER_NAMES = (
    "databricks.sql.backend.thrift_backend",
    "databricks.sql.thrift_backend",
)

_thrift_noise_filter_installed = False
_thrift_noise_filter_lock = threading.Lock()


class _SuppressThriftClosedFile(logging.Filter):
    """Drops only the connector's spurious post-close ``attempt_request`` error."""

    def filter(self, record: logging.LogRecord) -> bool:
        return _THRIFT_CLOSED_FILE_NOISE not in record.getMessage()


def _install_thrift_noise_filter() -> None:
    """Attach the suppression filter to the connector's thrift logger(s), once.

    Installed when saga *opens* a Databricks connection rather than only at
    ``close()``: the connector emits the noise from a teardown path that fires
    asynchronously (background thread / GC / interpreter shutdown), and the
    ``saga ingest`` path never calls ``close()`` — it opens a connection (e.g. the
    last-load lookup during the skip-extraction check) and lets it be reclaimed at
    exit. Installing at open time means the filter is present whenever the message
    is eventually emitted. Idempotent and thread-safe.
    """
    global _thrift_noise_filter_installed
    if _thrift_noise_filter_installed:
        return
    with _thrift_noise_filter_lock:
        if _thrift_noise_filter_installed:
            return
        noise_filter = _SuppressThriftClosedFile()
        for name in _THRIFT_LOGGER_NAMES:
            logging.getLogger(name).addFilter(noise_filter)
        _thrift_noise_filter_installed = True


def _resolve_partition_cluster(spec: Any) -> tuple[bool, bool]:
    """Return (has_partition, has_cluster), warning and preferring CLUSTER BY when both are set."""
    has_partition = bool(spec.partition_column)
    has_cluster = bool(spec.cluster_columns)
    if has_partition and has_cluster:
        logger.warning(
            "Both partition_column and cluster_columns are set for %s. "
            "Databricks does not support both on the same table. "
            "Using CLUSTER BY (Liquid Clustering) only.",
            spec.target_table,
        )
        has_partition = False
    return has_partition, has_cluster


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
                # Suppress the connector's spurious post-close thrift error before
                # any connection exists — its teardown can fire long after close()
                # (which the ingest path never calls), so install at open time.
                _install_thrift_noise_filter()
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

        The connector's spurious "I/O operation on closed file" thrift error is
        suppressed by a filter installed at connection-open time (see
        ``_install_thrift_noise_filter``); it is re-asserted here defensively in
        case this instance's connection was set without going through
        ``_get_connection``.
        """
        if self._connection is not None:
            _install_thrift_noise_filter()
            try:
                self._connection.close()
            except Exception as exc:
                logger.debug("Could not close Databricks connection cleanly: %s", exc)
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
        ``partition_column`` (traditional PARTITIONED BY), and ``insert_api``
        (``zerobus`` selects the Databricks Zerobus SDK for append loads;
        ``copy_into`` selects the default COPY INTO loader).
        Unknown hints are silently ignored.

        A failure to apply configured hints (adapter unavailable or a bad kwarg)
        is logged at WARNING — dropping partitioning/clustering silently would
        create the table with the wrong physical layout and no visible signal.
        """
        try:
            from dlt.destinations.adapters import databricks_adapter
        except ImportError as e:
            logger.warning(
                "dlt databricks_adapter is unavailable (%s); Databricks hints "
                "will NOT be applied — the table may be created without the "
                "configured partitioning/clustering/description.",
                e,
            )
            return resource

        # Map saga's generic hint keys to databricks_adapter's actual parameter
        # names. These MUST match dlt's signature — the adapter rejects unknown
        # kwargs with a TypeError, which (before this fix) dropped ALL hints in
        # the call, not just the misnamed one: `table_description`/
        # `liquid_cluster_by` were wrong (the adapter takes `table_comment`/
        # `cluster`), so descriptions and clustering were silently never applied.
        adapter_kwargs: dict = {}
        if "table_description" in hints:
            adapter_kwargs["table_comment"] = hints["table_description"]
        if "cluster_columns" in hints:
            adapter_kwargs["cluster"] = hints["cluster_columns"]
        if "partition_column" in hints:
            adapter_kwargs["partition"] = hints["partition_column"]
        if "insert_api" in hints and hints["insert_api"]:
            adapter_kwargs["insert_api"] = hints["insert_api"]

        if not adapter_kwargs:
            return resource

        try:
            return databricks_adapter(resource, **adapter_kwargs)
        except (AttributeError, TypeError) as e:
            logger.warning(
                "Failed to apply Databricks hints (%s) via databricks_adapter: "
                "%s. The table may be created without the configured "
                "partitioning/clustering/description.",
                ", ".join(sorted(adapter_kwargs)),
                e,
            )
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

    def execute_sql(self, sql: str, schema_name: Optional[str] = None) -> Any:
        """Execute a SQL statement (or multi-statement script) against Databricks.

        Multi-statement scripts are split into individual statements with a
        literal/comment-aware splitter (a plain ``;`` split would break a
        statement whose ``;`` sits inside a string literal — e.g. a table/column
        ``COMMENT`` or a historize-log message — or inside a comment).
        All SQL should use fully-qualified table names (catalog.schema.table);
        the ``schema_name`` parameter is accepted for interface compatibility
        but has no effect.

        Args:
            sql: SQL to execute.
            schema_name: Unused — kept for interface compatibility.

        Returns:
            List of row objects (attribute + index access), or ``[]`` for DDL/DML.
        """
        from dlt_saga.utility.sql import split_sql_statements

        conn = self._get_connection()

        logger.debug("Executing Databricks SQL (%d chars)", len(sql))

        with conn.cursor() as cursor:
            statements = split_sql_statements(sql)
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
        self, schema_name: str, records: list, pipeline: Any = None
    ) -> None:
        """Insert load info records directly into the load-info tracking table."""
        if not records:
            return

        from dlt_saga.project_config import get_load_info_table_name

        table_id = self.get_full_table_id(schema_name, get_load_info_table_name())

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
            schema_name,
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
        self, sql: str, params: list, schema_name: Optional[str] = None
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
        self, schema_name: str, pipeline_name: str, table_name: str
    ) -> Optional[datetime]:
        """Get the timestamp of the last successful load that had data."""
        from dlt_saga.project_config import get_load_info_table_name

        try:
            table_id = self.get_full_table_id(schema_name, get_load_info_table_name())
            rows = self._execute_parameterised(
                f"""
                SELECT MAX(started_at) AS started_at
                FROM {table_id}
                WHERE pipeline_name = ?
                  AND table_name = ?
                  AND row_count > 0
                """,
                [pipeline_name, table_name],
                schema_name,
            )
            if rows and rows[0][0] is not None:
                return rows[0][0]
            return None
        except Exception as e:
            # Missing load-info table = first run → None. Other errors propagate
            # rather than silently re-extracting the entire history.
            if looks_like_missing_table(e):
                return None
            raise

    def get_max_column_value(self, table_id: str, column: str) -> Any:
        """Get the maximum value of a column in a Databricks table."""
        try:
            rows = self.execute_sql(
                f"SELECT MAX(`{column}`) AS max_value FROM {table_id}"
            )
            if rows and rows[0][0] is not None:
                return rows[0][0]
            return None
        except Exception as e:
            # Missing target table = first run → None. Other errors propagate
            # rather than silently re-extracting from scratch / duplicating rows.
            if looks_like_missing_table(e):
                return None
            raise

    def reset_destination_state(self, pipeline_name: str, table_name: str) -> None:
        """Drop tables and clean up metadata for a full refresh."""
        # Debug, not info: callers (full-refresh / destroy) own the user-facing
        # message.
        logger.debug("Resetting Databricks destination state for %s", pipeline_name)
        schema = self.config.schema_name
        if not schema:
            return

        table_id = self.get_full_table_id(schema, table_name)
        self.execute_sql(f"DROP TABLE IF EXISTS {table_id}")

        from dlt_saga.project_config import get_load_info_table_name
        from dlt_saga.utility.naming import normalize_identifier

        # _dlt_version is keyed by the dlt *schema* name, which is the pipeline name
        # normalized by dlt's naming (e.g. "a__b" -> "a_b"). _dlt_pipeline_state and the
        # load-info table are keyed by the raw pipeline_name. Using pipeline_name for
        # _dlt_version leaves its row behind, so dlt thinks the (dropped) table still
        # exists and the next COPY INTO fails. (DuckDB/BigQuery already normalize.)
        normalized_schema = normalize_identifier(pipeline_name, max_length=64)

        for meta_table in (
            "_dlt_pipeline_state",
            get_load_info_table_name(),
            "_dlt_version",
        ):
            meta_id = self.get_full_table_id(schema, meta_table)
            try:
                if meta_table == "_dlt_version":
                    col, value = "schema_name", normalized_schema
                else:
                    col, value = "pipeline_name", pipeline_name
                self._execute_parameterised(
                    f"DELETE FROM {meta_id} WHERE `{col}` = ?",
                    [value],
                )
            except Exception as e:
                logger.debug("Could not clean %s: %s", meta_id, e)

    def build_historize_create_table_sql(
        self,
        create_clause: str,
        target_table_id: str,
        select_body: str,
        hints: "MaterializationHints",
    ) -> str:
        """Build CREATE TABLE DDL for a Databricks historize target table.

        Clause order: CREATE ... USING <fmt> [PARTITIONED BY | CLUSTER BY] [TBLPROPERTIES] AS SELECT.
        - native/delta: USING DELTA, supports PARTITIONED BY and CLUSTER BY.
        - iceberg: USING ICEBERG; cluster_columns raises a clear error.
        - delta_uniform: USING DELTA + TBLPROPERTIES for Iceberg compatibility.

        The ``hints.valid_from_column`` / ``valid_to_column`` / ``is_deleted_column``
        fields are accepted for API parity with the base/BigQuery contract but unused
        here: the Databricks CTAS reads the SCD2 column names straight from
        ``select_body``, which the SQL builder already renders with the configured
        names.
        """
        effective_format = (
            hints.table_format if hints.table_format != "native" else "delta"
        )

        if effective_format == "iceberg" and hints.cluster_columns:
            raise ValueError(
                "Databricks Iceberg tables do not support cluster_columns. "
                "Remove 'cluster_columns' from the historize section of this pipeline "
                "or switch to 'delta_uniform' if you need both Delta and Iceberg compatibility."
            )

        using_clause = (
            "USING DELTA"
            if effective_format in ("delta", "delta_uniform")
            else "USING ICEBERG"
        )

        parts = [f"{create_clause} {target_table_id}", using_clause]

        if hints.partition_column and not hints.cluster_columns:
            parts.append(self.partition_ddl(hints.partition_column))
        if hints.cluster_columns and effective_format != "iceberg":
            parts.append(self.cluster_ddl(hints.cluster_columns))

        if effective_format == "delta_uniform":
            parts.append(
                "TBLPROPERTIES ("
                "'delta.universalFormat.enabledFormats' = 'iceberg', "
                "'delta.enableIcebergCompatV2' = 'true'"
                ")"
            )

        parts.extend(["AS", select_body])
        return "\n".join(parts)

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
        # Spark SQL / Unity Catalog escapes an embedded backtick by doubling it
        # inside a backtick-quoted identifier (backslash is literal here), so a
        # column name containing a backtick can't terminate the identifier.
        return f"`{name.replace('`', '``')}`"

    def get_full_table_id(
        self, schema: str, table: str, database: Optional[str] = None
    ) -> str:
        return f"`{database or self.config.catalog}`.`{schema}`.`{table}`"

    def hash_expression(self, columns: list) -> str:
        # JSON-serialise the value columns (named struct fields) before hashing,
        # for parity with BigQuery. The old concat_ws('|', COALESCE(...,''))
        # silently missed SCD2 changes two ways: COALESCE(...,'') conflated NULL
        # with the empty string, and the '|' separator was ambiguous (values
        # containing '|' could shift across columns and hash identically). With a
        # named-field struct, NULL is distinct from '' (a NULL field is omitted,
        # '' is present) and column boundaries are unambiguous.
        cols = ", ".join(f"`{c}`" for c in columns)
        return f"xxhash64(to_json(struct({cols})))"

    def partition_ddl(self, column: str, col_type: Optional[str] = None) -> str:
        # col_type ignored on Databricks
        return f"PARTITIONED BY ({self.quote_identifier(column)})"

    def cluster_ddl(self, columns: list) -> str:
        cols = ", ".join(self.quote_identifier(c) for c in columns)
        return f"CLUSTER BY ({cols})"

    def partition_cluster_ddl(
        self,
        partition_column: Optional[str],
        cluster_columns: Optional[list],
    ) -> str:
        # Databricks rejects PARTITIONED BY and CLUSTER BY on the same table
        # (SPECIFY_CLUSTER_BY_WITH_PARTITIONED_BY_IS_NOT_ALLOWED); prefer Liquid
        # Clustering when both are requested.
        if partition_column and cluster_columns:
            logger.debug(
                "Partition and cluster columns both requested; Databricks does not "
                "support both on one table — using CLUSTER BY (Liquid Clustering) only."
            )
            partition_column = None
        parts = []
        if partition_column:
            parts.append(self.partition_ddl(partition_column))
        if cluster_columns:
            parts.append(self.cluster_ddl(cluster_columns))
        return "\n".join(parts)

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

    def escape_string_literal(self, value: str) -> str:
        # Spark SQL uses C-style backslash escapes in single-quoted literals
        # (rejects the ANSI '' doubling of the base default; see base helper).
        return self._backslash_escape_string_literal(value)

    def columns_query(self, database: str, schema: str, table: str) -> str:
        catalog = database or self.config.catalog
        safe_catalog = self.escape_string_literal(catalog)
        safe_schema = self.escape_string_literal(schema)
        safe_table = self.escape_string_literal(table)
        return f"""
            SELECT column_name, data_type
            FROM system.information_schema.columns
            WHERE table_catalog = '{safe_catalog}'
              AND table_schema   = '{safe_schema}'
              AND table_name     = '{safe_table}'
            ORDER BY ordinal_position
        """

    # --- Column / table description reconciliation -------------------------

    def supports_description_reconcile(self) -> bool:
        return True

    def get_column_descriptions(self, dataset: str, table: str) -> Dict[str, str]:
        # Catalog-scoped information_schema needs only USE CATALOG / schema
        # access; the account-wide system.information_schema is often admin-gated.
        catalog = self.quote_identifier(self.config.catalog)
        safe_schema = self.escape_string_literal(dataset)
        safe_table = self.escape_string_literal(table)
        rows = self.execute_sql(
            f"SELECT column_name, comment FROM {catalog}.information_schema.columns "
            f"WHERE table_schema = '{safe_schema}' AND table_name = '{safe_table}'"
        )
        return {r.column_name: (r.comment or "") for r in rows}

    def set_column_descriptions(
        self, dataset: str, table: str, descriptions: Dict[str, str]
    ) -> None:
        fqn = self.get_full_table_id(dataset, table)
        for column, description in descriptions.items():
            self.execute_sql(
                f"ALTER TABLE {fqn} ALTER COLUMN {self.quote_identifier(column)} "
                f"COMMENT '{self.escape_string_literal(description)}'"
            )

    def get_table_description(self, dataset: str, table: str) -> Optional[str]:
        catalog = self.quote_identifier(self.config.catalog)
        safe_schema = self.escape_string_literal(dataset)
        safe_table = self.escape_string_literal(table)
        rows = list(
            self.execute_sql(
                f"SELECT comment FROM {catalog}.information_schema.tables "
                f"WHERE table_schema = '{safe_schema}' AND table_name = '{safe_table}'"
            )
        )
        return rows[0].comment if rows else None

    def set_table_description(self, dataset: str, table: str, description: str) -> None:
        fqn = self.get_full_table_id(dataset, table)
        self.execute_sql(
            f"COMMENT ON TABLE {fqn} IS '{self.escape_string_literal(description)}'"
        )

    # --- Clustering reconciliation -----------------------------------------

    def supports_clustering_reconcile(self) -> bool:
        return True

    def get_clustering_columns(self, dataset: str, table: str) -> Optional[list]:
        fqn = self.get_full_table_id(dataset, table)
        try:
            rows = list(self.execute_sql(f"DESCRIBE DETAIL {fqn}"))
        except Exception as exc:
            if looks_like_missing_table(exc):
                return None
            raise
        if not rows:
            return None
        cols = rows[0].clusteringColumns
        return list(cols) if cols else []

    def set_clustering_columns(
        self, dataset: str, table: str, cluster_columns: list
    ) -> None:
        # ALTER ... CLUSTER BY switches the table to Liquid Clustering; a
        # metadata change, existing data reclusters lazily on subsequent writes.
        fqn = self.get_full_table_id(dataset, table)
        cols = ", ".join(self.quote_identifier(c) for c in cluster_columns)
        self.execute_sql(f"ALTER TABLE {fqn} CLUSTER BY ({cols})")

    # -------------------------------------------------------------------------
    # Native-load contract
    # -------------------------------------------------------------------------

    def supports_native_load(self) -> bool:
        return True

    def supported_native_load_uri_schemes(self) -> set:
        return {"gs", "s3", "abfss"}

    def native_load_file_name_expr(self) -> str:
        return "_metadata.file_path"

    def parse_filename_timestamp_expr(
        self, file_name_expr: str, regex_literal: str, format_literal: str
    ) -> str:
        return (
            f"try_to_timestamp("
            f"regexp_extract({file_name_expr}, '{regex_literal}', 1), "
            f"'{format_literal}')"
        )

    def table_exists(self, dataset: str, table: str) -> bool:
        try:
            self.execute_sql(f"DESCRIBE TABLE {self.get_full_table_id(dataset, table)}")
            return True
        except Exception:
            return False

    def drop_table(self, dataset: str, table: str) -> None:
        self.execute_sql(
            f"DROP TABLE IF EXISTS {self.get_full_table_id(dataset, table)}"
        )

    def list_table_columns(self, dataset: str, table: str) -> list:
        rows = self.execute_sql(self.columns_query(self.config.catalog, dataset, table))
        return [(r[0], r[1]) for r in rows]

    def add_column(self, dataset: str, table: str, column: str, type_name: str) -> None:
        table_id = self.get_full_table_id(dataset, table)
        self.execute_sql(
            f"ALTER TABLE {table_id} ADD COLUMNS ({self.quote_identifier(column)} {type_name})"
        )

    def execute_sql_with_job(self, sql: str, schema: Optional[str] = None) -> tuple:
        """Execute SQL and return (rows, job_id) using databricks-sql-connector."""
        conn = self._get_connection()
        with conn.cursor() as cursor:
            cursor.execute(sql)
            try:
                desc = cursor.description or []
                cols = [d[0] for d in desc]
                raw_rows = cursor.fetchall()

                class _R:
                    def __init__(self, c: list, v: tuple) -> None:
                        self._c, self._v = c, v

                    def __getattr__(self, n: str) -> Any:
                        try:
                            return self._v[self._c.index(n)]
                        except ValueError:
                            raise AttributeError(n)

                    def __getitem__(self, i: int) -> Any:
                        return self._v[i]

                rows: list = [_R(cols, r) for r in raw_rows]
            except Exception:
                rows = []
            job_id: str = getattr(cursor, "query_id", "") or ""
        return rows, job_id

    def native_load_chunk(self, spec: "Any") -> "Any":
        """Load one chunk via COPY INTO with mergeSchema."""
        from dlt_saga.destinations.base import NativeLoadResult

        target_id = self.get_full_table_id(spec.target_schema, spec.target_table)

        if not spec.target_exists:
            self._native_load_create_empty_target(spec, target_id)
        else:
            self._native_load_reconcile_schema(spec, target_id)

        sql = self._build_copy_into(spec, target_id)
        rows, job_id = self.execute_sql_with_job(sql, spec.target_schema)
        affected = self._extract_copy_into_affected_rows(rows)
        rows_by_uri = self._native_load_rowcounts(spec, target_id)
        return NativeLoadResult(
            rows_loaded=affected, job_id=job_id, rows_by_uri=rows_by_uri
        )

    def drop_table_external(self, dataset: str, table: str) -> None:
        """DROP TABLE PURGE for external tables — removes catalog entry AND files at LOCATION."""
        table_id = self.get_full_table_id(dataset, table)
        self.execute_sql(f"DROP TABLE IF EXISTS {table_id} PURGE")

    def _native_load_create_empty_target(self, spec: "Any", target_id: str) -> None:
        """CREATE TABLE with framework-managed columns, format DDL, and optional LOCATION.

        Clause order follows Databricks convention:
        USING ... [PARTITIONED BY|CLUSTER BY] ... [LOCATION] ... [TBLPROPERTIES].
        For replace disposition: managed tables use CREATE OR REPLACE TABLE;
        external tables (target_location set) use TRUNCATE TABLE when the table
        already exists (preserves files and Delta time travel) or CREATE TABLE on
        first run.  DROP TABLE PURGE is reserved for --full-refresh only.
        """
        derived_ddl = ", ".join(
            f"{self.quote_identifier(c.name)} {c.sql_type}"
            for c in spec.derived_columns
        )
        table_format = getattr(spec, "table_format", "delta")
        target_location = getattr(spec, "target_location", None)
        is_replace = getattr(spec, "write_disposition", "append") == "replace"

        if is_replace and target_location:
            # External Delta tables: CREATE OR REPLACE TABLE is metadata-only and
            # does not reset the Delta log, so old data would persist.  Instead,
            # TRUNCATE clears the current state without deleting physical files
            # (preserving Delta time travel and storage-retention requirements).
            # On first run the table doesn't exist yet, so fall through to CREATE.
            if self.table_exists(spec.target_schema, spec.target_table):
                self.execute_sql(f"TRUNCATE TABLE {target_id}")
                return
            create_clause = "CREATE TABLE IF NOT EXISTS"
        elif is_replace:
            create_clause = "CREATE OR REPLACE TABLE"
        else:
            create_clause = "CREATE TABLE IF NOT EXISTS"

        if table_format == "iceberg":
            parts = [f"{create_clause} {target_id} ({derived_ddl}) USING ICEBERG"]
            if spec.partition_column:
                parts.append(self.partition_ddl(spec.partition_column))
        else:
            # delta and delta_uniform both use USING DELTA storage
            has_partition, has_cluster = _resolve_partition_cluster(spec)
            parts = [f"{create_clause} {target_id} ({derived_ddl}) USING DELTA"]
            if has_partition:
                parts.append(self.partition_ddl(spec.partition_column))
            if has_cluster:
                parts.append(self.cluster_ddl(spec.cluster_columns))

        if target_location:
            parts.append(f"LOCATION '{self.escape_string_literal(target_location)}'")

        if table_format == "delta_uniform":
            parts.append(
                "TBLPROPERTIES ("
                "'delta.universalFormat.enabledFormats' = 'iceberg', "
                "'delta.enableIcebergCompatV2' = 'true'"
                ")"
            )

        self.execute_sql(" ".join(parts))

    def _native_load_reconcile_schema(self, spec: "Any", target_id: str) -> None:
        """Ensure framework columns exist; let COPY INTO mergeSchema handle data cols."""
        target_cols = self.list_table_columns(spec.target_schema, spec.target_table)
        target_col_map = {n: t for n, t in target_cols}

        for dc in spec.derived_columns:
            if dc.name not in target_col_map:
                self.add_column(
                    spec.target_schema, spec.target_table, dc.name, dc.sql_type
                )
                logger.info(
                    "Added framework column %r to %s", dc.name, spec.target_table
                )

    def _build_copy_into(self, spec: "Any", target_id: str) -> str:
        """Build COPY INTO SQL with derived column SELECT transformation."""
        # Prefer the immediate parent dir so the FILES list uses short basenames.
        # Fall back to the configured source_uri root when URIs span multiple dirs
        # (e.g. cross-partition chunks) — Databricks resolves FILES relative to FROM.
        parents = {uri.rsplit("/", 1)[0] for uri in spec.source_uris}
        if len(parents) == 1:
            source_prefix = next(iter(parents)) + "/"
        elif spec.source_uri_root:
            source_prefix = spec.source_uri_root
        else:
            source_prefix = spec.source_uris[0].rsplit("/", 1)[0] + "/"

        file_format = spec.file_type.upper()
        if file_format == "JSONL":
            file_format = "JSON"

        derived_select = ", ".join(
            f"{c.sql_expr} AS {self.quote_identifier(c.name)}"
            for c in spec.derived_columns
        )
        if file_format == "JSON":
            data_select = "SELECT * EXCEPT (_rescued_data)"
        else:
            data_select = "SELECT *"

        if derived_select:
            select_clause = f"{data_select}, {derived_select}"
        else:
            select_clause = data_select

        where_sql = self.render_filter(getattr(spec, "filters", []) or [])
        where_clause = f" WHERE {where_sql}" if where_sql else ""

        # FILES basenames relative to source_prefix
        file_items = []
        for uri in spec.source_uris:
            rel = (
                uri[len(source_prefix) :]
                if uri.startswith(source_prefix)
                else uri.split("/")[-1]
            )
            file_items.append(f"'{self.escape_string_literal(rel)}'")
        files_list = ", ".join(file_items)

        format_options_str = self._format_databricks_copy_options(spec)

        # COPY INTO is idempotent by default: it tracks already-loaded files and
        # skips them on re-run. That's the desired cross-run dedup only for an
        # incremental append. For `replace` (the target was just truncated/replaced
        # and must be fully reloaded) and for non-incremental appends (no cross-run
        # dedup), that tracking would skip every file and load 0 rows — so force a
        # reload. `replace` is validated as incompatible with incremental, so it
        # always forces regardless.
        is_replace = getattr(spec, "write_disposition", "append") == "replace"
        force = is_replace or not getattr(spec, "incremental", False)
        copy_options = "'mergeSchema' = 'true'"
        if force:
            copy_options += ", 'force' = 'true'"

        return (
            f"COPY INTO {target_id} "
            f"FROM ({select_clause} FROM '{self.escape_string_literal(source_prefix)}'{where_clause}) "
            f"FILEFORMAT = {file_format} "
            f"FILES = ({files_list}) "
            f"FORMAT_OPTIONS ({format_options_str}) "
            f"COPY_OPTIONS ({copy_options})"
        )

    def _format_databricks_copy_options(self, spec: "Any") -> str:
        """Build FORMAT_OPTIONS string for Databricks COPY INTO from spec.format_options."""
        opts = dict(spec.format_options or {})
        parts = []
        if "field_delimiter" in opts:
            delim = self.escape_string_literal(opts["field_delimiter"])
            parts.append(f"'delimiter' = '{delim}'")
        # skip_leading_rows is a count. Databricks 'header' = 'true' only skips a
        # single header row, so it's correct only for 1; for N>1 use 'skipRows'
        # (skips N rows from the start of each file).
        try:
            skip_rows = int(opts.get("skip_leading_rows", 0) or 0)
        except (TypeError, ValueError):
            skip_rows = 0
        if skip_rows == 1:
            parts.append("'header' = 'true'")
        elif skip_rows > 1:
            parts.append(f"'skipRows' = '{skip_rows}'")
        if "encoding" in opts:
            enc = self.escape_string_literal(opts["encoding"])
            parts.append(f"'encoding' = '{enc}'")
        if "quote_character" in opts:
            q = self.escape_string_literal(opts["quote_character"])
            parts.append(f"'quote' = '{q}'")
        if "null_marker" in opts:
            nm = self.escape_string_literal(opts["null_marker"])
            parts.append(f"'nullValue' = '{nm}'")
        return ", ".join(parts) if parts else "'mergeSchema' = 'true'"

    def _extract_copy_into_affected_rows(self, rows: list) -> int:
        """Extract num_inserted_rows from COPY INTO result set."""
        if not rows:
            return 0
        try:
            return int(getattr(rows[0], "num_inserted_rows", None) or 0)
        except Exception as exc:
            logger.debug("Could not read num_inserted_rows by attribute: %s", exc)
            try:
                return int(rows[0][0] or 0)
            except Exception as exc2:
                logger.debug("Could not read num_inserted_rows by index: %s", exc2)
                return 0

    def _native_load_rowcounts(self, spec: "Any", target_id: str) -> dict:
        """Query per-file row counts from the target after COPY INTO."""
        from dlt_saga.pipelines.native_load.pipeline import (
            NativeLoadPipeline as _NLP,
        )

        file_col = _NLP._FILE_NAME_COLUMN
        at_col = _NLP._INGESTED_AT_COLUMN

        has_file_col = any(c.name == file_col for c in spec.derived_columns)
        if not has_file_col:
            return {}

        at_expr = next(
            (c.sql_expr for c in spec.derived_columns if c.name == at_col), None
        )
        if not at_expr:
            return {}

        sql = (
            f"SELECT {self.quote_identifier(file_col)} AS uri, COUNT(*) AS cnt "
            f"FROM {target_id} "
            f"WHERE {self.quote_identifier(at_col)} = {at_expr} "
            f"GROUP BY {self.quote_identifier(file_col)}"
        )
        try:
            rows = self.execute_sql(sql, spec.target_schema)
            return {r.uri: int(r.cnt) for r in rows}
        except Exception as exc:
            logger.warning("Could not derive per-file row counts: %s", exc)
            return {}

    def json_type_name(self) -> str:
        return "STRING"

    def parse_json_expression(self, value_expr: str) -> str:
        return value_expr  # Databricks stores JSON as STRING — no parse needed

    def _render_filter_column(self, spec: Any, resolve: Any) -> str:
        col = resolve(spec.column)
        if not spec.path:
            return col
        # Databricks colon notation works on both STRING (auto-parsed) and
        # VARIANT columns and is faster than get_json_object.
        return col + ":" + ".".join(spec.path)

    def _render_regex_match(self, col_sql: str, pattern: str) -> str:
        return f"{col_sql} RLIKE '{self.escape_string_literal(pattern)}'"
