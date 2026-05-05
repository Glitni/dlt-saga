"""NativeLoadPipeline — cloud-storage → warehouse loader.

Dispatches to destination-specific load mechanics (BigQuery external table →
INSERT, or Databricks COPY INTO) without going through dlt's extract/normalize
pipeline.  Designed for large sources where dlt would be too slow.
"""

import logging
import re
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from typing import Optional

from dlt_saga.pipelines.base_pipeline import BasePipeline
from dlt_saga.pipelines.native_load._sql import esc_sql_literal
from dlt_saga.pipelines.native_load.config import NativeLoadConfig
from dlt_saga.pipelines.native_load.state import NativeLoadStateManager
from dlt_saga.pipelines.native_load.storage import get_storage_client
from dlt_saga.utility.cli.logging import PrefixedLoggerAdapter

logger = logging.getLogger(__name__)


class NativeLoadPipeline(BasePipeline):
    """Generic cloud-storage → warehouse loader.

    Bypasses dlt's extract/normalize machinery entirely.  ``run()`` is fully
    overridden; ``extract_data()`` raises NotImplementedError.
    """

    _INGESTED_AT_COLUMN = "_dlt_ingested_at"
    _FILE_NAME_COLUMN = "_dlt_source_file_name"
    _FILE_DATE_COLUMN = "_dlt_source_file_date"

    def __init__(self, config: dict, log_prefix: Optional[str] = None) -> None:
        # Bypass BasePipeline.__init__ — no dlt.Pipeline needed.
        self.config_dict = config
        self.pipeline_name = config["pipeline_name"]
        self.base_table_name = config.get("base_table_name", self.pipeline_name)
        self.table_name = config.get("table_name", self.base_table_name)
        self.log_prefix = log_prefix
        self.logger = (
            PrefixedLoggerAdapter(logger, log_prefix) if log_prefix else logger
        )

        from dlt_saga.utility.cli.context import get_execution_context

        self.context = get_execution_context()

        # Validate write_disposition before any I/O
        write_disp = config.get("write_disposition", "append") or "append"
        base_disp = write_disp.replace("+historize", "")
        if base_disp not in ("append", "replace"):
            raise ValueError(
                f"native_load adapter only supports write_disposition 'append', 'replace', "
                f"'append+historize', or 'replace+historize', got: {write_disp!r}"
            )

        self.native_config = NativeLoadConfig.from_dict(config)
        self._is_replace: bool = base_disp == "replace"
        self._incremental: bool = self.native_config.incremental

        from dlt_saga.destinations.factory import DestinationFactory

        destination_type = self.context.get_destination_type()
        self.destination = DestinationFactory.create_from_context(
            destination_type, self.context, config
        )

        if not self.destination.supports_native_load():
            raise ValueError(
                f"native_load adapter is not supported on destination {destination_type!r}. "
                "Supported: bigquery, databricks."
            )

        self._validate_uri_scheme_against_destination()
        self._validate_databricks_only_fields()

        # destination_database: used for logging, mirrors what BasePipeline exposes
        self.destination_database = getattr(
            self.destination.config, "database", None
        ) or getattr(self.destination.config, "project_id", "local")

        self._dataset: str = config.get("schema_name") or ""
        if not self._dataset:
            raise ValueError("schema_name must be set in the pipeline config")

        self._staging_dataset: str = (
            self.native_config.staging_dataset or f"{self._dataset}_staging"
        )

        self.state_manager = NativeLoadStateManager(
            destination=self.destination,
            dataset=self._dataset,
        )
        self.storage_client = get_storage_client(
            self.native_config.source_uri,
            billing_project=getattr(self.destination.config, "billing_project_id", None)
            or getattr(self.destination.config, "project_id", None),
            destination=self.destination,
        )

        # Resolve external-table LOCATION (Databricks only; None = managed table)
        self._target_location: Optional[str] = self._resolve_target_location()

        self._phase_timings: dict = {}
        self._target_exists: bool = False
        self._ingest_time_iso: str = datetime.now(timezone.utc).isoformat()
        self._column_hints: dict = self._build_column_hints()

    # ------------------------------------------------------------------
    # Entry points
    # ------------------------------------------------------------------

    def run(self) -> list:
        if self.context.update_access:
            self.logger.warning(
                "update_access is not supported for native_load adapter"
            )
            return []

        try:
            with self._phase("init"):
                self.destination.ensure_schema_exists(self._staging_dataset)
                if self._incremental:
                    self.state_manager.ensure_table_exists()
                else:
                    self.logger.info(
                        "File-level state tracking disabled (incremental=false) — "
                        "every matching file will be loaded every run."
                    )
                if self.context.full_refresh:
                    self._ensure_target_table()
                    self._handle_full_refresh()
                elif self._is_replace:
                    # replace mode: each run rewrites the target fresh; state log unused
                    self._target_exists = False
                else:
                    self._ensure_target_table()

            with self._phase("discover"):
                new_files_by_cursor = self._discover_new_files()

            total_files = sum(len(v) for v in new_files_by_cursor.values())
            if total_files == 0:
                self.logger.info("No new files to load.")
                return [self._build_load_info_entry(loaded=0)]

            batch = self.native_config.load_batch_size
            n_chunks = max(1, (total_files + batch - 1) // batch)
            self.logger.info(
                "Found %d new file(s) across %d cursor group(s); loading in %d chunk(s)",
                total_files,
                len(new_files_by_cursor),
                n_chunks,
            )

            first_run = not self._target_exists

            with self._phase("load"):
                total_rows = self._load_files(new_files_by_cursor)

            with self._phase("finalize"):
                if total_rows > 0:
                    self._write_load_info(total_rows, first_run)

            t = self._phase_timings
            self.logger.info(
                "Loaded %d row(s) from %d file(s) in %.1fs total "
                "(init: %.1fs, discover: %.1fs, load: %.1fs, finalize: %.1fs)",
                total_rows,
                total_files,
                sum(t.values()),
                t.get("init", 0),
                t.get("discover", 0),
                t.get("load", 0),
                t.get("finalize", 0),
            )
            return [self._build_load_info_entry(loaded=total_rows)]

        except Exception as exc:
            if not isinstance(exc, ValueError):
                self.logger.error("Native load failed: %s", exc, exc_info=True)
            raise

    def extract_data(self) -> list:
        raise NotImplementedError(
            "NativeLoadPipeline overrides run() entirely; extract_data() is never called"
        )

    # ------------------------------------------------------------------
    # Init phase helpers
    # ------------------------------------------------------------------

    def _ensure_target_table(self) -> None:
        try:
            if self.destination.table_exists(self._dataset, self.table_name):
                self._target_exists = True
                self.logger.debug("Target table %s already exists", self.table_name)
        except Exception as exc:
            self.logger.debug("Could not check target table existence: %s", exc)

    def _handle_full_refresh(self) -> None:
        self.logger.info(
            "Full refresh: dropping %s and clearing load state", self.table_name
        )
        try:
            # External Delta tables need DROP ... PURGE to also remove files at LOCATION.
            if self._target_location and hasattr(
                self.destination, "drop_table_external"
            ):
                self.destination.drop_table_external(self._dataset, self.table_name)
            else:
                self.destination.drop_table(self._dataset, self.table_name)
        except Exception as exc:
            self.logger.debug("Could not drop target (may not exist): %s", exc)

        self._sweep_orphan_ext_tables()

        try:
            self.state_manager.clear_pipeline_state(self.pipeline_name)
        except Exception as exc:
            self.logger.debug("Could not clear load state: %s", exc)

        self._target_exists = False

    def _sweep_orphan_ext_tables(self) -> None:
        """Drop leftover BigQuery external tables from prior crashes. No-op on Databricks."""
        if not hasattr(self.destination, "list_tables_by_pattern"):
            return
        pattern = f"{self.table_name}__ext_%"
        try:
            names = self.destination.list_tables_by_pattern(
                self._staging_dataset, pattern
            )
        except Exception as exc:
            self.logger.debug("Orphan sweep: could not list staging dataset: %s", exc)
            return
        for name in names:
            try:
                self.destination.drop_table(self._staging_dataset, name)
                self.logger.info("Swept orphan external table %s", name)
            except Exception as exc:
                self.logger.debug("Could not drop orphan %s: %s", name, exc)

    # ------------------------------------------------------------------
    # Discovery
    # ------------------------------------------------------------------

    @property
    def _date_mode(self) -> bool:
        """True when filename_date_regex + filename_date_format are both set."""
        return bool(self.native_config.filename_date_regex)

    def _discover_new_files(self) -> dict:
        if self._date_mode:
            return self._discover_date_mode()
        return self._discover_flat_mode()

    def _discover_flat_mode(self) -> dict:
        """Full prefix listing, deduped against state log when incremental."""
        if self._incremental:
            loaded = self.state_manager.get_loaded_uri_generations(self.pipeline_name)
        else:
            loaded = set()
        new_files = []
        for obj in self.storage_client.list_files(
            self.native_config.source_uri,
            self.native_config.file_pattern,
        ):
            if (obj.full_uri, obj.generation) not in loaded:
                new_files.append(obj)
        return {None: new_files}

    def _discover_date_mode(self) -> dict:
        """Date-based discovery: group files by extracted date value."""
        cursor_re = re.compile(self.native_config.filename_date_regex)
        cursor_fmt = self.native_config.filename_date_format

        if self._incremental:
            last_cursor_str = self.state_manager.get_last_cursor(self.pipeline_name)
            loaded = self.state_manager.get_loaded_uri_generations(
                self.pipeline_name,
                cursor_min=last_cursor_str
                and self._cursor_lookback_str(last_cursor_str, cursor_fmt),
            )
        else:
            last_cursor_str = None
            loaded = set()

        files_by_cursor: dict = {}

        if self._incremental and self.native_config.partition_prefix_pattern:
            # Partition-pruned listing: issue one list_files call per date partition
            # instead of a single recursive scan over the full source root.
            uris_to_scan = self._build_partition_uris(last_cursor_str, cursor_fmt)  # type: ignore[arg-type]
        else:
            uris_to_scan = [
                (
                    self.native_config.source_uri,
                    self._compute_gcs_start_offset(last_cursor_str, cursor_fmt),  # type: ignore[arg-type]
                )
            ]

        for scan_uri, start_offset in uris_to_scan:
            for obj in self.storage_client.list_files(
                scan_uri,
                self.native_config.file_pattern,
                start_offset=start_offset,
            ):
                if (obj.full_uri, obj.generation) in loaded:
                    continue
                # Match cursor on the full path for partition patterns, basename for flat
                match_target = (
                    obj.full_uri
                    if self.native_config.partition_prefix_pattern
                    else obj.full_uri.rsplit("/", 1)[-1]
                )
                m = cursor_re.search(match_target)
                if not m:
                    self.logger.debug(
                        "File %r does not match filename_date_regex, skipping",
                        obj.full_uri,
                    )
                    continue
                cursor_val = m.group(1)
                files_by_cursor.setdefault(cursor_val, []).append(obj)

        # Sort cursor groups chronologically
        sorted_keys = sorted(files_by_cursor.keys())
        return {k: files_by_cursor[k] for k in sorted_keys}

    def _compute_gcs_start_offset(
        self, last_cursor_str: Optional[str], cursor_fmt: str
    ) -> Optional[str]:
        """Compute a GCS-style lexicographic start_offset from the last cursor."""
        if not last_cursor_str:
            return None
        filename_prefix = self._resolve_date_filename_prefix()
        if filename_prefix is None:
            return None
        try:
            last_dt = datetime.strptime(last_cursor_str, cursor_fmt)
            lookback_dt = last_dt - timedelta(
                days=self.native_config.date_lookback_days
            )
            offset_str = lookback_dt.strftime(cursor_fmt)
            uri_path = self.native_config.source_uri
            if uri_path.startswith("gs://"):
                bucket_and_prefix = uri_path[5:]
                _, prefix = (bucket_and_prefix.split("/", 1) + [""])[:2]
                return prefix + filename_prefix + offset_str
        except Exception as exc:
            self.logger.debug("Could not compute date start_offset: %s", exc)
        return None

    def _build_partition_uris(
        self, last_cursor_str: Optional[str], cursor_fmt: str
    ) -> list:
        """Build (uri, None) pairs for each relevant date partition.

        Walks dates from last_cursor - lookback_days to today and formats them
        through partition_prefix_pattern.  Tokens: {year}, {month}, {day}, {hour}.
        When {hour} is present, all 24 hours are emitted per day.
        """
        pattern = self.native_config.partition_prefix_pattern
        source_uri = self.native_config.source_uri
        today = datetime.now(timezone.utc).date()

        if last_cursor_str:
            try:
                last_dt = datetime.strptime(last_cursor_str, cursor_fmt)
                start_date = (
                    last_dt - timedelta(days=self.native_config.date_lookback_days)
                ).date()
            except Exception:
                start_date = today
        else:
            start_date = today

        results = []
        d = start_date
        while d <= today:
            if "{hour}" in pattern:
                for hour in range(24):
                    prefix = (
                        pattern.replace("{year}", f"{d.year:04d}")
                        .replace("{month}", f"{d.month:02d}")
                        .replace("{day}", f"{d.day:02d}")
                        .replace("{hour}", f"{hour:02d}")
                    )
                    results.append((source_uri + prefix, None))
            else:
                prefix = (
                    pattern.replace("{year}", f"{d.year:04d}")
                    .replace("{month}", f"{d.month:02d}")
                    .replace("{day}", f"{d.day:02d}")
                )
                results.append((source_uri + prefix, None))
            d += timedelta(days=1)

        return results

    def _cursor_lookback_str(self, last_cursor: str, fmt: str) -> Optional[str]:
        try:
            dt = datetime.strptime(last_cursor, fmt)
            return (
                dt - timedelta(days=self.native_config.date_lookback_days)
            ).strftime(fmt)
        except Exception:
            return None

    def _resolve_date_filename_prefix(self) -> Optional[str]:
        """Return the literal prefix before the date group in file basenames."""
        if self.native_config.date_filename_prefix is not None:
            return self.native_config.date_filename_prefix

        date_re = re.compile(self.native_config.filename_date_regex)
        blobs = list(
            self.storage_client.list_files(
                self.native_config.source_uri,
                self.native_config.file_pattern,
            )
        )[:50]

        for blob in blobs:
            basename = blob.full_uri.rsplit("/", 1)[-1]
            m = date_re.search(basename)
            if m:
                return basename[: m.start(1)]

        self.logger.warning(
            "Could not auto-detect date_filename_prefix from the first 50 blobs. "
            "Falling back to full listing (no start_offset optimisation)."
        )
        return None

    # ------------------------------------------------------------------
    # Load phase
    # ------------------------------------------------------------------

    def _load_files(self, files_by_cursor: dict) -> int:
        flat: list = [(f, cv) for cv, files in files_by_cursor.items() for f in files]
        batch = self.native_config.load_batch_size

        # Group by parent directory so each COPY INTO chunk has a single source prefix.
        # This avoids the multi-prefix problem on date-partitioned ADLS/GCS sources
        # where a naive slice could span e.g. day=01/ and day=02/ subdirectories.
        from collections import defaultdict

        grouped: dict = defaultdict(list)
        for f, cv in flat:
            parent = f.full_uri.rsplit("/", 1)[0]
            grouped[parent].append((f, cv))

        total_chunks = sum(
            max(1, (len(items) + batch - 1) // batch) for items in grouped.values()
        )
        total_rows = 0
        chunk_num = 0
        for items in grouped.values():
            for i in range(0, len(items), batch):
                chunk_num += 1
                total_rows += self._load_chunk(
                    items[i : i + batch], chunk_num, total_chunks
                )
        return total_rows

    def _load_chunk(self, chunk: list, chunk_num: int, total_chunks: int) -> int:
        from dlt_saga.destinations.base import NativeLoadSpec

        files = [f for f, _ in chunk]
        uris = [f.full_uri for f in files]
        rows_for_log = [(f.full_uri, cv, f.generation) for f, cv in chunk]

        if self._incremental:
            load_ids, started_at = self.state_manager.record_loads_started_bulk(
                self.pipeline_name, rows_for_log
            )
        else:
            load_ids = [None] * len(files)
            started_at = None

        chunk_label = self._format_chunk_label(chunk, chunk_num, total_chunks)
        self.logger.info(chunk_label)

        base_disp = self.native_config.write_disposition.replace("+historize", "")
        spec = NativeLoadSpec(
            target_dataset=self._dataset,
            target_table=self.table_name,
            source_uris=uris,
            file_type=self.native_config.file_type,
            autodetect_schema=self.native_config.autodetect_schema,
            derived_columns=self._derived_columns(),
            target_exists=self._target_exists,
            partition_column=self.config_dict.get("partition_column"),
            cluster_columns=self.config_dict.get("cluster_columns"),
            format_options=self._build_format_options(),
            staging_dataset=self._staging_dataset,
            chunk_label=chunk_label,
            write_disposition=base_disp,
            column_hints=self._column_hints,
            target_location=self._target_location,
            table_format=self.native_config.table_format,
        )
        # Expose source_uri so Databricks COPY INTO can compute basenames
        spec._source_uri = self.native_config.source_uri  # type: ignore[attr-defined]

        try:
            result = self.destination.native_load_chunk(spec)
            self._target_exists = True

            if self._incremental:
                size_by_load_id = {
                    load_id: f.size for f, load_id in zip(files, load_ids)
                }
                loaded_rows_by_load_id = {
                    load_id: int(result.rows_by_uri.get(f.full_uri, 0))
                    for f, load_id in zip(files, load_ids)
                }
                self.state_manager.record_loads_success_bulk(
                    pipeline_name=self.pipeline_name,
                    rows=rows_for_log,
                    load_ids=load_ids,
                    job_id=result.job_id,
                    size_bytes_by_load_id=size_by_load_id,
                    loaded_rows_by_load_id=loaded_rows_by_load_id,
                    started_at=started_at,
                )
            return result.rows_loaded

        except Exception as exc:
            self.logger.error(
                "Native load failed for %d file(s): %s", len(files), exc, exc_info=True
            )
            if self._incremental:
                self.state_manager.record_loads_failed_bulk(
                    pipeline_name=self.pipeline_name,
                    rows=rows_for_log,
                    load_ids=load_ids,
                    error=str(exc)[:4000],
                    started_at=started_at,
                )
            raise

    # ------------------------------------------------------------------
    # Derived columns
    # ------------------------------------------------------------------

    def _derived_columns(self) -> list:
        from dlt_saga.destinations.base import DerivedColumn

        file_name_expr = self.destination.native_load_file_name_expr()
        cols = [
            DerivedColumn(
                name=self._INGESTED_AT_COLUMN,
                sql_expr=f"TIMESTAMP '{esc_sql_literal(self._ingest_time_iso)}'",
                sql_type=self.destination.type_name("timestamp"),
            )
        ]
        if self.native_config.include_file_metadata:
            cols.append(
                DerivedColumn(
                    name=self._FILE_NAME_COLUMN,
                    sql_expr=file_name_expr,
                    sql_type=self.destination.type_name("string"),
                )
            )
            if (
                self.native_config.filename_date_regex
                and self.native_config.filename_date_format
            ):
                regex = esc_sql_literal(self.native_config.filename_date_regex)
                fmt = esc_sql_literal(self.native_config.filename_date_format)
                cols.append(
                    DerivedColumn(
                        name=self._FILE_DATE_COLUMN,
                        sql_expr=self.destination.parse_filename_timestamp_expr(
                            file_name_expr, regex, fmt
                        ),
                        sql_type=self.destination.type_name("timestamp"),
                    )
                )
        return cols

    def _build_column_hints(self) -> dict:
        """Resolve config.columns to {normalized_col_name_lower: bq_sql_type}.

        Keys are normalized via dlt snake_case so both raw and normalized names
        in the config resolve to the same key.  Values are destination SQL types.
        """
        raw_columns = self.native_config.columns
        if not raw_columns:
            return {}

        from dlt.common.normalizers.naming.snake_case import NamingConvention

        conv = NamingConvention()
        hints: dict = {}
        for col_name, col_def in raw_columns.items():
            if not isinstance(col_def, dict):
                continue
            data_type = col_def.get("data_type", "")
            if not data_type:
                continue
            norm_key = conv.normalize_identifier(col_name).lower()
            hints[norm_key] = self.destination.dlt_type_to_native(str(data_type))
        return hints

    def _build_format_options(self) -> dict:
        opts: dict = {}
        if self.native_config.file_type == "csv":
            if self.native_config.csv_separator:
                opts["field_delimiter"] = self.native_config.csv_separator
            if self.native_config.csv_skip_leading_rows:
                opts["skip_leading_rows"] = self.native_config.csv_skip_leading_rows
            if self.native_config.csv_quote_character:
                opts["quote_character"] = self.native_config.csv_quote_character
            if self.native_config.csv_null_marker:
                opts["null_marker"] = self.native_config.csv_null_marker
            if self.native_config.encoding:
                opts["encoding"] = self.native_config.encoding
        if self.native_config.max_bad_records:
            opts["max_bad_records"] = self.native_config.max_bad_records
        if self.native_config.ignore_unknown_values:
            opts["ignore_unknown_values"] = self.native_config.ignore_unknown_values
        return opts

    # ------------------------------------------------------------------
    # Finalize helpers
    # ------------------------------------------------------------------

    def _write_load_info(self, total_rows: int, first_run: bool) -> None:
        started = datetime.fromisoformat(self._ingest_time_iso)
        finished = datetime.now(timezone.utc)
        record = {
            "pipeline_name": self.pipeline_name,
            "destination_name": (
                self.destination.config.__class__.__name__.lower().replace(
                    "destinationconfig", ""
                )
            ),
            "destination_type": self.context.get_destination_type(),
            "dataset_name": self._dataset,
            "table_name": self.table_name,
            "row_count": total_rows,
            "started_at": started,
            "finished_at": finished,
            "first_run": first_run,
            "saved_at": finished.isoformat(),
        }
        try:
            self.destination.save_load_info(self._dataset, [record])
        except Exception as exc:
            self.logger.warning(
                "Failed to write _saga_load_info: %s; "
                "run will not appear in `saga report`",
                exc,
            )

    def _build_load_info_entry(self, loaded: int) -> dict:
        return {
            "pipeline_name": self.pipeline_name,
            "dataset_name": self._dataset,
            "table_name": self.table_name,
            "row_count": loaded,
        }

    # ------------------------------------------------------------------
    # Utility
    # ------------------------------------------------------------------

    def _validate_uri_scheme_against_destination(self) -> None:
        uri = self.native_config.source_uri
        scheme = uri.split("://")[0] if "://" in uri else ""
        supported = self.destination.supported_native_load_uri_schemes()
        if scheme not in supported:
            raise ValueError(
                f"Source URI scheme {scheme!r} is not supported by destination "
                f"{self.context.get_destination_type()!r}. Supported: {supported}"
            )

    def _validate_databricks_only_fields(self) -> None:
        """Raise early for Databricks-only fields set on a non-Databricks destination."""
        is_databricks = self.destination.__class__.__name__ == "DatabricksDestination"
        if self.native_config.target_location and not is_databricks:
            raise ValueError(
                "target_location only applies to Databricks targets. "
                "Remove it from the config or switch to destination_type: databricks."
            )
        if self.native_config.table_format != "delta" and not is_databricks:
            raise ValueError(
                f"table_format={self.native_config.table_format!r} only applies to "
                "Databricks targets. Remove it or switch to destination_type: databricks."
            )
        if is_databricks:
            fmt = self.native_config.table_format
            if fmt == "iceberg" and self.config_dict.get("cluster_columns"):
                raise ValueError(
                    "Liquid Clustering (cluster_columns) is Delta-only. "
                    "Use partition_column for Iceberg, or switch to "
                    "table_format: delta_uniform to keep clustering."
                )

    def _resolve_target_location(self) -> Optional[str]:
        """Resolve the external-table LOCATION for Databricks targets.

        Priority:
        1. Explicit per-pipeline ``target_location`` config field.
        2. Custom ``naming_module.generate_target_location()`` hook
           (mirrors ``generate_schema_name`` / ``generate_table_name``).
        3. ``default_generate_target_location()`` — framework default that
           returns ``<storage_root>/<group>/<table>/`` or ``None`` when no
           ``storage_root`` is configured (caller falls back to managed UC).
        """
        is_databricks = self.destination.__class__.__name__ == "DatabricksDestination"
        if not is_databricks:
            return None

        if self.native_config.target_location:
            return self.native_config.target_location

        storage_root = getattr(self.destination.config, "storage_root", None)

        hook_result = self._invoke_target_location_hook(storage_root)
        if hook_result:
            return hook_result

        from dlt_saga.pipeline_config import default_generate_target_location
        from dlt_saga.utility.naming import get_environment

        return default_generate_target_location(
            self._get_naming_segments(),
            get_environment(),
            storage_root,
            pipeline_group=self.config_dict.get("pipeline_group") or None,
            table_name=self.table_name,
        )

    def _invoke_target_location_hook(
        self, storage_root: Optional[str]
    ) -> Optional[str]:
        """Call ``naming_module.generate_target_location()`` if defined.

        Returns the hook's return value, or ``None`` if no naming module is
        configured, the function is missing, or the hook returns ``None``.
        Errors are caught and logged at debug level so a buggy hook doesn't
        abort the load — the caller falls back to the framework default.
        """
        try:
            from dlt_saga.pipeline_config.naming import load_naming_module
            from dlt_saga.project_config import get_project_config
            from dlt_saga.utility.naming import get_environment

            project_config = get_project_config()
            module = load_naming_module({"naming_module": project_config.naming_module})
            if not module or not hasattr(module, "generate_target_location"):
                return None

            return module.generate_target_location(
                self._get_naming_segments(), get_environment(), storage_root
            )
        except Exception as exc:
            self.logger.debug(
                "naming_module.generate_target_location() raised: %s", exc
            )
            return None

    def _get_naming_segments(self) -> list:
        """Return the identifier segments fed to the naming module.

        For file-based configs, derives ``[group, sub_1, ..., name]`` from
        ``config_path`` (extension stripped). For other config sources that
        don't expose a path, falls back to ``[pipeline_group, base_table_name]``
        — enough to keep the naming defaults working without coupling to a
        specific source identifier shape.
        """
        from pathlib import Path

        config_path = self.config_dict.get("config_path") or ""
        if config_path:
            parts = Path(config_path).parts
            if "configs" in parts:
                idx = parts.index("configs")
                segments = list(parts[idx + 1 :])
            else:
                segments = list(parts)
            if segments:
                last = segments[-1]
                for ext in (".yml", ".yaml"):
                    if last.endswith(ext):
                        segments[-1] = last[: -len(ext)]
                        break
                return segments

        group = self.config_dict.get("pipeline_group") or ""
        base = self.base_table_name or ""
        return [s for s in (group, base) if s]

    @contextmanager
    def _phase(self, name: str):
        import time

        t0 = time.time()
        yield
        self._phase_timings[name] = time.time() - t0

    def _format_chunk_label(
        self, chunk: list, chunk_num: int, total_chunks: int
    ) -> str:
        files = [f for f, _ in chunk]
        cursors = sorted({cv for _, cv in chunk if cv is not None})
        cursor_info = f", dates {cursors[0]}-{cursors[-1]}" if cursors else ""
        total_mb = sum(f.size for f in files) / 1_048_576
        return (
            f"Loading chunk {chunk_num}/{total_chunks}: "
            f"{len(files)} file(s){cursor_info}, {total_mb:.1f} MB"
        )
