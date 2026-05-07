# Historize (SCD2)

The historize engine transforms raw snapshot tables into [Slowly Changing Dimension Type 2 (SCD2)](https://en.wikipedia.org/wiki/Slowly_changing_dimension#Type_2:_add_new_row) tables that track the full history of every record.

---

## How it works

A "snapshot" is a point-in-time copy of a source table. Historize processes a sequence of snapshots and computes:

- **When did each version of a record become active?** (`_dlt_valid_from`)
- **When was it superseded?** (`_dlt_valid_to`)
- **Was it deleted?** (`_dlt_is_deleted`)

A row is active when `ts >= _dlt_valid_from AND (ts < _dlt_valid_to OR _dlt_valid_to IS NULL)`.

---

## Output columns

| Column | Description |
|--------|-------------|
| `_dlt_valid_from` | When this version became active (snapshot timestamp) |
| `_dlt_valid_to` | When this version was superseded (`NULL` = current) |
| `_dlt_is_deleted` | `TRUE` on deletion marker rows, `FALSE` on all change rows |

---

## Enabling historize

Set `write_disposition` in the pipeline config:

| Value | Ingest | Historize | Use case |
|-------|--------|-----------|----------|
| `append+historize` | Yes | Yes | Ingest snapshots then historize |
| `historize` | No | Yes | Historize externally-delivered data |

```yaml
# configs/filesystem/snapshots/companies.yml
write_disposition: "append+historize"
primary_key: [company_id]

historize:
  partition_column: "_dlt_valid_from"
  cluster_columns: [company_id]
  track_deletions: true
```

Run with:

```bash
saga run --select "filesystem__snapshots__companies"
# Runs ingest then historize in sequence

# Or separately:
saga ingest --select "filesystem__snapshots__companies"
saga historize --select "filesystem__snapshots__companies"
```

---

## Config reference

| Field | Location | Default | Description |
|-------|----------|---------|-------------|
| `primary_key` | top-level | required | Key columns identifying a unique record |
| `snapshot_date_regex` | top-level | — | Regex to extract snapshot date from file paths (filesystem only) |
| `snapshot_date_format` | top-level | — | `strptime` format for the extracted date string |
| `snapshot_column` | `historize:` | `_dlt_ingested_at` | Column used as the snapshot timestamp |
| `track_columns` | `historize:` | — | Opt-in allowlist: only these columns drive change detection (all columns still appear in output) |
| `ignore_columns` | `historize:` | `[]` | Columns excluded from change detection (still appear in output) |
| `partition_column` | `historize:` | — | Partition the SCD2 output table |
| `cluster_columns` | `historize:` | — | Cluster the SCD2 output table |
| `track_deletions` | `historize:` | `false` | Emit deletion marker rows when a key disappears |
| `table_format` | `historize:` | inherited | Table format for the SCD2 output table. Overrides the profile-level setting. See [Table format](#table-format) |
| `output_schema` | `historize:` | — | Write the historized table to this schema instead of the source schema |
| `output_table` | `historize:` | — | Explicit name for the historized output table (overrides the auto-generated name) |

---

## Deletion tracking

When `track_deletions: true`, a key disappearing from the source produces a **separate deletion marker row**:

- `_dlt_is_deleted = TRUE`
- `_dlt_valid_to = NULL` (open-ended) until the key reappears

This cleanly separates "this version ended" from "this record was deleted." Closed change rows always have `_dlt_is_deleted = FALSE`. When the key reappears, the deletion marker is closed like any other change row.

---

## Snapshot date extraction

For filesystem pipelines where file paths encode the snapshot date (e.g. `snapshots/2024-03-15.parquet`):

```yaml
snapshot_date_regex: "\\d{4}-\\d{2}-\\d{2}"
snapshot_date_format: "%Y-%m-%d"
```

The `_dlt_ingested_at` column is resolved in this order:

1. Date extracted from the file path via `snapshot_date_regex`
2. File modification date
3. Extraction timestamp

---

## Table format

By default, the historized table uses whatever `table_format` is configured on the profile (or `native` if unset). Override it for just the historize layer via a nested `historize:` block in `profiles.yml`:

```yaml
prod:
  type: bigquery
  table_format: native          # ingest tables stay native
  historize:
    table_format: iceberg       # historized tables use BigLake Iceberg
    storage_path: gs://bucket/historized/
```

Or override for a single pipeline:

```yaml
historize:
  table_format: iceberg
```

Resolution chain (first non-null wins):

1. `pipeline.historize.table_format`
2. `pipeline.table_format`
3. `profile.historize.table_format`
4. `profile.table_format`
5. `native`

BigQuery supports `native` and `iceberg` (BigLake managed). Databricks supports `native`/`delta`, `iceberg`, and `delta_uniform`. Combining `iceberg` with `cluster_columns` on Databricks raises a validation error.

Changing `table_format` is treated as a config change — historize detects it via the fingerprint and prompts for `saga historize --full-refresh`.

---

## Incremental vs full refresh

**Incremental** (default): processes only snapshots not yet historized. Uses `LEAD` for within-batch sequencing and `MERGE` to close existing open records.

**Full refresh** (`--full-refresh`): rebuilds the SCD2 table from all raw snapshots. Required after config changes:

```bash
saga historize --full-refresh --select "filesystem__snapshots__companies"
```

Historize detects config changes via a fingerprint stored in `_saga_historize_log` and prompts for a full refresh when `primary_key`, `track_columns`, `ignore_columns`, `track_deletions`, `snapshot_column`, or `table_format` changes.

---

## External data (historize-only)

When data arrives in the destination from an external process (no dlt ingest step):

```yaml
write_disposition: "historize"
primary_key: [order_id]

source_database: "external_source"
source_schema: "deliveries"
source_table: "customer_orders_raw"

historize:
  snapshot_column: "delivery_date"
  track_deletions: true
```

`source_database`, `source_schema`, and `source_table` are top-level fields pointing to the raw source table already present in the destination.

---

## State tracking

Historize state is recorded in `_saga_historize_log` with:

- Which snapshots have been processed (prevents reprocessing)
- A config fingerprint — detects changes requiring a full refresh
- Timing information per historize run

---

## Full example

```yaml
# configs/filesystem/proffdata/companies.yml
write_disposition: "append+historize"
primary_key: [orgnr]

# Snapshot date extraction from file paths
snapshot_date_regex: "\\d{4}-\\d{2}-\\d{2}"
snapshot_date_format: "%Y-%m-%d"

historize:
  # snapshot_column: _dlt_ingested_at  # default
  ignore_columns: [updated_by]
  partition_column: "_dlt_valid_from"
  cluster_columns: [orgnr]
  track_deletions: true
```
