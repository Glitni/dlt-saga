# Native Cloud-Storage Loader

The `native_load` adapter ingests large cloud-storage datasets directly into the warehouse, bypassing dlt's extract/normalize machinery entirely. It uses destination-native bulk load mechanics (`LOAD DATA` / external tables on BigQuery, `COPY INTO` on Databricks) for maximum throughput.

## When to use this adapter

Use `native_load` instead of `filesystem` when:

- You have **more than ~1 000 files** per run, OR
- Your total payload exceeds **~1 GB per run**, OR
- Files are already in Parquet/CSV/JSONL in cloud storage and you don't need row-level transforms

For smaller or more complex sources (type coercions, multi-sheet, REST APIs), the standard `filesystem` adapter remains the better choice.

| Adapter | Best for | Throughput model |
|---|---|---|
| `filesystem` | < 1 000 files, < 1 GB, needs row transforms | dlt extract → normalize → load |
| `native_load` | > 1 000 files or > 1 GB, flat copy | Single SQL job per chunk |

---

## Supported destinations and URI schemes

| Destination | Mechanic | Supported URI schemes |
|---|---|---|
| BigQuery | External table → `INSERT INTO` (or CTAS on first run) | `gs://` |
| Databricks | `COPY INTO` with `mergeSchema = true` | `gs://`, `abfss://` |

S3 (`s3://`) is accepted by config validation on Databricks but raises `NotImplementedError` at runtime until a future release.

---

## Quickstart config

**Append with file-level deduplication** (load each file once, skip on re-run):

```yaml
# configs/native_load/my_bucket/orders.yml
adapter: dlt_saga.native_load
tags: [daily]

source_uri: gs://my-bucket/exports/orders/
file_type: parquet

write_disposition: append
incremental: true        # track loaded files; skip already-loaded URIs on re-run
```

**Replace** (rewrite the target table on every run, no state needed):

```yaml
adapter: dlt_saga.native_load
source_uri: gs://my-bucket/snapshots/products/
file_type: parquet
write_disposition: replace
```

Run it:

```bash
saga ingest --select "native_load__my_bucket__orders"
```

---

## Config reference

### Required fields

| Field | Type | Description |
|---|---|---|
| `source_uri` | string | Root URI to list files from. Must end with `/`. |
| `file_type` | string | `parquet`, `csv`, or `jsonl` |

### Discovery

Discovery mode is implied: set both `filename_date_regex` and `filename_date_format` to enable date-based grouping; omit both for flat (full-prefix) listing.

| Field | Default | Description |
|---|---|---|
| `file_pattern` | derived from `file_type` | Glob pattern(s) to filter filenames. Single string or list of strings. |
| `filename_date_regex` | — | Regex with one capture group extracting a date string from the filename. When set (with `filename_date_format`), files are grouped by date and loaded chronologically; `_dlt_source_file_date` is populated. |
| `filename_date_format` | — | strftime format string matching the capture group (e.g. `%Y%m%d`). Must be set if and only if `filename_date_regex` is set. |
| `date_lookback_days` | `2` | How many days before the last loaded date to re-scan for late-arriving files. Requires `incremental: true`; a warning is logged when set without it. |
| `date_filename_prefix` | — | Literal filename prefix before the date group, used to compute GCS `start_offset` for faster listing (auto-detected if omitted). Requires `incremental: true`; a warning is logged when set without it. |
| `partition_prefix_pattern` | — | Date-partition layout under `source_uri` (Databricks + ADLS only, e.g. `year={year}/month={month}/day={day}/`). Lists only partitions in the lookback window instead of scanning the full root. Requires `incremental: true` and `filename_date_regex`; warnings are logged when either is absent. |

### Write disposition and incremental loading

| Field | Default | Description |
|---|---|---|
| `write_disposition` | `append` | Table-level write semantics. `append` inserts into the target; `replace` rewrites the target on every run (`CREATE OR REPLACE TABLE`). Append `+historize` to either value to also run the historize layer. |
| `incremental` | `false` | File-level state tracking. When `true`, loaded `(uri, generation)` pairs are recorded in `_saga_native_load_log` so re-runs skip already-loaded files. Mirrors dlt's opt-in `dlt.sources.incremental`. Not compatible with `write_disposition: replace`. |

> **`append` vs `replace` vs `incremental`:** `write_disposition` controls what happens to the *destination table* each run; `incremental` controls which *source files* are read. A plain `append` without `incremental: true` loads every matching file on every run (duplicates on re-run). Use `incremental: true` to deduplicate across runs. Use `replace` when you want to swap the table contents completely every run.

### Load behaviour

| Field | Default | Description |
|---|---|---|
| `load_batch_size` | `5000` | Maximum URIs per SQL job. |
| `max_bad_records` | `0` | BigQuery only: tolerated malformed rows per job. |
| `ignore_unknown_values` | `false` | BigQuery only: ignore extra JSON keys not in schema. |
| `autodetect_schema` | `true` | Let the destination infer column types from the files. |
| `include_file_metadata` | `true` | Inject `_dlt_source_file_name` and (when `filename_date_regex` is set) `_dlt_source_file_date`. |
| `staging_dataset` | `<target_dataset>_staging` | BigQuery only: dataset for transient external tables. |

### Schema and column types

Column names are always normalized to lowercase snake_case (BigQuery only; Databricks takes names verbatim from source files). New columns from source files are always added automatically via `ALTER TABLE`; type changes always raise an error regardless of any setting.

| Field | Default | Description |
|---|---|---|
| `columns` | `{}` | Explicit type hints. Keys are raw or normalized column names; values are `{data_type: <type>}`. Supported dlt types: `text`, `bigint`, `double`, `bool`, `timestamp`, `date`, `time`, `decimal`. Native BQ types (`STRING`, `INT64`, …) also accepted. |

Example:
```yaml
columns:
  order_date: {data_type: timestamp}
  amount:     {data_type: decimal}
```

### CSV options

Only used when `file_type: csv`.

| Field | Default | Description |
|---|---|---|
| `csv_separator` | — | Field delimiter (e.g. `;`). |
| `encoding` | — | File encoding (e.g. `UTF-8`). |
| `csv_skip_leading_rows` | `0` | Header rows to skip. |
| `csv_quote_character` | — | Quote character override. |
| `csv_null_marker` | — | String to interpret as NULL. |

### Databricks external tables

| Field | Default | Description |
|---|---|---|
| `target_location` | — | Explicit LOCATION for the created Delta/Iceberg table (e.g. `abfss://container@account.dfs.core.windows.net/tables/orders/`). If omitted, Databricks creates a managed table unless a `storage_root` is set in the profile. |
| `table_format` | `delta` | Table format: `delta`, `iceberg`, or `delta_uniform`. See [Choosing a table format](#choosing-a-table-format-databricks). |
| `cluster_columns` | — | Databricks Liquid Clustering columns (e.g. `[id, order_date]`). Not compatible with `table_format: iceberg`. |
| `partition_column` | — | Partition column (BigQuery only; on Databricks use `cluster_columns` instead). |

---

## Historization

`native_load` is fully compatible with saga's historize layer. Set `write_disposition: append+historize` and the `historize:` section as normal — no extra configuration required.

The adapter always populates `_dlt_ingested_at`, which is the column the historize layer uses to discover unprocessed snapshots.

```yaml
adapter: dlt_saga.native_load
tags: [daily]

source_uri: gs://my-bucket/exports/customers/
file_type: parquet
write_disposition: append+historize
primary_key: [customer_id]

historize:
  partition_column: _dlt_valid_from
  cluster_columns: [customer_id]
  track_deletions: true
```

---

## State log

When `incremental: true`, every loaded URI is recorded in `_saga_native_load_log` (configurable via `log_tables.native_load_log` in `saga_project.yml`). A companion `_saga_native_load_log_latest` view shows the most recent run per pipeline.

The state log prevents duplicate loads: a URI that already has a `success` row is skipped on subsequent runs. If a file is overwritten in cloud storage (new generation, same path), its new generation ID triggers a re-load.

When `incremental: false` (the default), the state log is never read or written. Every matching file is loaded on every run — useful when an external system guarantees exactly-once delivery, or when `write_disposition: replace` is used (which rewrites the target table on every run anyway).

---

## Replace mode

`write_disposition: replace` rewrites the target table on every run without needing `--full-refresh`:

```yaml
adapter: dlt_saga.native_load
source_uri: gs://my-bucket/snapshots/
file_type: parquet
write_disposition: replace   # CREATE OR REPLACE TABLE every run
```

BigQuery emits `CREATE OR REPLACE TABLE … AS SELECT …`. Databricks managed tables use `CREATE OR REPLACE TABLE`. Databricks external tables (`target_location` set) use `TRUNCATE TABLE` on existing tables (clears data, keeps files and Delta time travel) or `CREATE TABLE IF NOT EXISTS` on first run. Physical files at LOCATION are never deleted on a routine `replace` run — only `--full-refresh` does that.

`replace` + `incremental: true` is rejected at config validation — replacing the table each run while tracking which files were loaded would silently lose data on re-runs.

---

## Full refresh

```bash
saga ingest --select "native_load__my_bucket__orders" --full-refresh
```

For `append` + `incremental: true` pipelines, this drops the target table, clears state log entries, and sweeps orphaned BigQuery external tables. The next run recreates the table from scratch.

For `append` + `incremental: false` (default), `--full-refresh` drops the target table only — there is no state log to clear.

For `replace` pipelines, `--full-refresh` drops and recreates the table just like for `append` pipelines. Use it when you change `partition_column`, `cluster_columns`, or `table_format` — those DDL changes require a full drop rather than the routine `CREATE OR REPLACE TABLE` / `TRUNCATE TABLE` path.

> **Warning (Databricks external tables):** `--full-refresh` on an external Delta table deletes the files at LOCATION via `DROP TABLE ... PURGE`. Ensure the UC storage credential has DELETE permission, or use manual recovery via the cloud console if needed.

---

## Databricks + ADLS setup

### Unity Catalog external location

The `native_load` adapter lists ADLS files using Databricks SQL `LIST`, so saga itself never touches Azure credentials. Both listing and `COPY INTO` flow through the Databricks SQL warehouse, which authorises against ADLS via the workspace's UC external location credential.

Minimal setup:

```sql
-- 1. Create a storage credential (managed identity preferred)
CREATE STORAGE CREDENTIAL my_adls_cred
  WITH AZURE_MANAGED_IDENTITY = 'my-workspace-identity';

-- 2. Create external location
CREATE EXTERNAL LOCATION my_adls_location
  URL 'abfss://my-container@myaccount.dfs.core.windows.net/'
  WITH (STORAGE CREDENTIAL my_adls_cred);

-- 3. Grant to the saga service principal
GRANT READ FILES, WRITE FILES, CREATE EXTERNAL TABLE
  ON EXTERNAL LOCATION my_adls_location
  TO `saga-principal@example.com`;
```

For external Delta tables with full-refresh PURGE, also grant `DELETE` on the location:

```sql
GRANT DELETE ON EXTERNAL LOCATION my_adls_location TO `saga-principal@example.com`;
```

### Profile: `storage_root`

Set a `storage_root` in your profile to have saga automatically derive `target_location` for every `native_load` pipeline without setting it per-config:

```yaml
# profiles.yml
default:
  outputs:
    prod:
      destination_type: databricks
      storage_root: abfss://tables@myaccount.dfs.core.windows.net/saga/
      # Resolved location per pipeline: <storage_root>/<pipeline_group>/<table>/
```

### Full Databricks + ADLS example

```yaml
# configs/native_load/adls/customer_orders.yml
adapter: dlt_saga.native_load
tags: [daily]

source_uri: abfss://raw@myaccount.dfs.core.windows.net/customer_orders/
file_type: parquet
partition_prefix_pattern: "year=*/month=*/day=*"   # skip scanning whole container
incremental: true                      # required for partition_prefix_pattern

write_disposition: append+historize
primary_key: [order_id]

table_format: delta                    # or iceberg / delta_uniform
cluster_columns: [order_id, order_date]

historize:
  partition_column: _dlt_valid_from
  cluster_columns: [order_id]
  track_deletions: true
```

### Migrating from ADF date-partitioned ADLS staging

If your current ADF pipeline does:

| ADF activity | Equivalent in saga |
|---|---|
| `GetMetadata` on container | Flat listing (automatic, no config needed) |
| `Filter` on date folder | `partition_prefix_pattern: year=*/month=*/day=*` |
| `ForEach` over date folders | Handled internally; chunks by `load_batch_size` |
| `DatabricksNotebook(COPY INTO)` | `adapter: dlt_saga.native_load` + `file_type: parquet` |
| Manual SCD2 notebook | `write_disposition: append+historize` |

Replace the entire ADF pipeline with one YAML file and `saga ingest`.

---

## Choosing a table format (Databricks)

| Format | Use when | Notes |
|---|---|---|
| `delta` (default) | Databricks is the primary engine | Supports Liquid Clustering; best read/write performance on Databricks |
| `iceberg` | External engines need pure Iceberg semantics | Databricks reads and writes; lose Liquid Clustering |
| `delta_uniform` | Databricks writes; external engines (Spark, Trino, Athena) also read | Databricks maintains a parallel Iceberg metadata layer; Liquid Clustering still available |

`cluster_columns` is not compatible with `table_format: iceberg` (saga raises a `ValueError` at startup).

---

## Troubleshooting

| Symptom | Likely cause | Fix |
|---|---|---|
| `NotImplementedError: S3 listing not supported` | `source_uri: s3://...` on Databricks | S3 listing is not yet implemented. Open an issue if you need it. |
| `URI scheme gs:// not supported by this destination` | BigQuery config with `s3://` or `abfss://` | BigQuery only supports `gs://`. |
| BigQuery external table quota errors | Too many tables in staging dataset | Reduce `load_batch_size` to create fewer concurrent tables. |
| `COPY INTO` fails with permission error | UC external location not granted | Verify `GRANT READ FILES` on the external location (see setup above). |
| Full-refresh PURGE fails | Storage credential missing DELETE permission | Grant DELETE on the external location, or use `--full-refresh` with manual file cleanup. |
| Columns not appearing after schema evolution | BigQuery: column name in source doesn't normalize to expected name | Check the snake_case normalization of the source column name (e.g. `OrderID` → `order_id`). New columns are always added automatically. |
| Type change raises error | Source file changed a column's type | Fix upstream data; type changes are never auto-applied. |
| `_dlt_source_file_date` is NULL | `filename_date_regex` didn't match the filename | Check `filename_date_regex` against actual filenames; NULL is correct on miss — no fallback is applied. |
