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
| `_dlt_valid_from` | When this version became active (snapshot timestamp). Renameable via `valid_from_column`. |
| `_dlt_valid_to` | When this version was superseded (`NULL` = current). Renameable via `valid_to_column`. |
| `_dlt_is_deleted` | `TRUE` on deletion marker rows, `FALSE` on all change rows. Renameable via `is_deleted_column`. |

The three SCD2 column names default to `_dlt_*` but can be renamed per pipeline — useful when a
downstream convention expects domain-specific names. See [Renaming the SCD2 columns](#renaming-the-scd2-columns).

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
| `partition_column` | `historize:` | `valid_from_column` | Partition the SCD2 output table |
| `cluster_columns` | `historize:` | — | Cluster the SCD2 output table |
| `track_deletions` | `historize:` | `false` | Emit deletion marker rows when a key disappears |
| `merge_key` | `historize:` | — | Subset of `primary_key` that scopes deletion / reappearance detection. See [Scoping deletion detection (`merge_key`)](#scoping-deletion-detection-merge_key) |
| `valid_from_column` | `historize:` | `_dlt_valid_from` | Name of the SCD2 valid-from column in the output table |
| `valid_to_column` | `historize:` | `_dlt_valid_to` | Name of the SCD2 valid-to column in the output table |
| `is_deleted_column` | `historize:` | `_dlt_is_deleted` | Name of the soft-delete marker column in the output table |
| `table_format` | `historize:` | inherited | Table format for the SCD2 output table. Overrides the profile-level setting. See [Table format](#table-format) |
| `output_schema` | `historize:` | — | Write the historized table to this schema instead of the source schema |
| `output_table` | `historize:` | — | Explicit name for the historized output table (overrides the auto-generated name) |
| `filters` | `historize:` | — | Declarative row filter applied only during historize — see [Filtering the source](#filtering-the-source) |

### Renaming the SCD2 columns

By default the validity/deletion columns are named `_dlt_valid_from`, `_dlt_valid_to`, and
`_dlt_is_deleted`. Set `valid_from_column`, `valid_to_column`, and `is_deleted_column` to emit
your own names — for example to match an existing data-warehouse naming convention:

```yaml
write_disposition: "append+historize"
primary_key: [id]

historize:
  track_deletions: true
  valid_from_column: valid_from
  valid_to_column: valid_to
  is_deleted_column: is_deleted
```

The output table then carries `valid_from` / `valid_to` / `is_deleted` instead of the `_dlt_*`
names. `partition_column` follows `valid_from_column` unless set explicitly. All four names are
validated as SQL identifiers. Existing tables are unaffected: the defaults are unchanged, so
pipelines that don't set these keys keep emitting `_dlt_*`. Changing a name after a historized
table exists requires a full refresh (`saga historize --full-refresh`) — it's part of the config
fingerprint, so the framework prompts for it.

> **Consistency caveat.** When you override these names, the historized tables no longer match the
> `_dlt_*` convention dlt's own ingested tables use. That's fine when everything downstream is
> historized, but mixing renamed historized tables with default-named ingested tables in one
> warehouse yields inconsistent column names across sibling tables — pick one convention per
> warehouse.

### Scoping deletion detection (`merge_key`)

By default, historize treats *every* distinct snapshot date in the source as a checkpoint that
keys must be present at — if a key is absent from a snapshot that contains other rows, it's
flagged deleted at that point. That breaks down when the source unions independently-delivered
partitions sharing one snapshot column (per-instance, per-tenant, per-feed): a snapshot date in
one partition is "missing" from sibling partitions even though those partitions just didn't deliver
that day.

`merge_key` scopes deletion and reappearance detection to a subset of `primary_key`. A key in
group `X` is only flagged deleted if it disappears from a snapshot that contains *other rows in
group X*. Sibling-group snapshots no longer drive deletions for `X`. Same idea as dlt's own
`merge_key` in SCD2.

```yaml
write_disposition: "append+historize"
primary_key: [dbinstance, id]

historize:
  snapshot_column: _dlt_source_file_date
  track_deletions: true
  merge_key: [dbinstance]
```

With this setup, `dbinstance = A` skipping a daily snapshot has no effect on `dbinstance = B`'s
keys, and vice versa. A real deletion within a single instance still fires — the marker sits at
the next snapshot date *for that instance*.

Constraints:

- `merge_key` must be a subset of `primary_key`. Scoping by a non-PK column would let two distinct
  keys collide on the scope group and produce inconsistent deletion timing; the validator rejects
  it.
- Changing `merge_key` after the historized table exists is part of the config fingerprint, so
  the framework prompts for a `saga historize --full-refresh`.
- Pairs with `track_deletions`: `merge_key` refines *when* deletions are recorded, not whether
  they're recorded at all.

---

## Filtering the source

`historize.filters:` restricts which source rows enter the SCD2 history. Same schema as the top-level ingest [Row Filters](Configuration#row-filters) (operators `eq`, `ne`, `in`, `not_in`, `is_null`, `is_not_null`, `matches`; optional dotted JSON path; AND-joined), but applied **only** to the historize SQL — independent of any ingest-stage filter.

The classic use case is partitioning one source table into multiple tenant-scoped histories. Each pipeline reads the same source, applies its own filter, and writes to its own output table:

```yaml
# configs/streams/stream_writes_tenant_a.yml
write_disposition: historize
primary_key: [event_id]

source_database: analytics
source_schema: events
source_table: stream_writes_raw

historize:
  snapshot_column: event_ts
  output_table: stream_writes_tenant_a_historized
  track_deletions: true
  filters:
    - column: tenant_id
      value: tenant_a
```

A sibling config with `tenant_b` produces an independent history under `stream_writes_tenant_b_historized`. The historize log keys by pipeline name, so each tenant's snapshot tracking, deletion detection, and partial/full-refresh state are isolated.

**Filter is pushed down everywhere.** The same WHERE clause is applied in every source read the runner makes: snapshot discovery, full-reprocess CTEs (all-snapshots, hashed-rows, key-presence for deletion detection), incremental CTEs (snapshot-filtered hashed reads, deletion candidates), the rollback affected-keys query for partial refresh, and the summary stats query. There is no path through which an out-of-scope row reaches the historized table.

**Composition with ingest `filters:`.** They run independently and stack. For `append+historize`, the top-level `filters:` shapes what enters the raw append table; `historize.filters:` shapes what enters the SCD2 table built from that raw table.

```yaml
write_disposition: append+historize
primary_key: [event_id]

# Ingest-stage filter: keep archived rows out of the raw table entirely
filters:
  - column: status
    op: ne
    value: archived

historize:
  snapshot_column: event_ts
  output_table: stream_writes_tenant_a_historized
  track_deletions: true
  # Historize-stage filter: further restrict to tenant A's history
  filters:
    - column: tenant_id
      value: tenant_a
```

**A subtlety with `track_deletions: true`.** A row whose `tenant_id` mutates between snapshots (e.g. from `tenant_a` to `tenant_b`) registers as a delete in tenant A's history and an insert in tenant B's history. From each tenant's view that's the correct semantic — the row is gone from theirs and appears in the other's — but worth knowing if you rely on `_dlt_is_deleted` markers downstream.

**Config-change detection.** The filter list is part of the historize config fingerprint. Changing `historize.filters:` between runs triggers the same prompt as changing `primary_key` or `ignore_columns` — you'll be told to run `saga historize --full-refresh` so the existing historized table is rebuilt against the new predicate.

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

## Per-layer access

`historize_schema_access` is an overlay on top of `schema_access` that applies only to the historize schema. It exists for the case where the historize schema is distinct from the ingest schema — either because `placement: schema_suffix` is set or because a custom `naming_module` returns a different schema for `layer="historize"` — and you want different grants on each side.

Resolution is union-with-dedup: the historize schema receives `schema_access ∪ historize_schema_access`. When the overlay is unset, the historize schema simply inherits the ingest list (the correct default under `placement: table_suffix`, where both layers share one schema). Reuses the existing `+key:` hierarchical merge in `saga_project.yml`:

```yaml
# saga_project.yml
historize:
  placement: schema_suffix
  schema_suffix: "_historized"

pipelines:
  schema_access:
    - "OWNER:serviceAccount:platform@<project>.iam.gserviceaccount.com"
    - "WRITER:serviceAccount:dlt-run@<project>.iam.gserviceaccount.com"

  filesystem:
    +schema_access:
      - "AUTHORIZED_DATASET:downstream-project.dlt_filesystem"
    +historize_schema_access:
      - "AUTHORIZED_DATASET:downstream-project.dlt_filesystem_historized"
```

The `+`-prefixed forms merge across hierarchy levels (project → group → pipeline). The bare key replaces. Pipeline-level overrides live at the top of a config YAML peer to the existing `historize:` block.

Saga warns at runtime when `historize_schema_access` is declared but the resolved historize schema equals the ingest schema — the overlay would silently mix into the ingest grants. Either consolidate the entries into `schema_access`, or configure `placement: schema_suffix` / a custom `naming_module` to make the schemas distinct.

> **Legacy alias.** The pre-0.4 key `dataset_access:` is still accepted as a silent read-time alias for `schema_access:` at every level. Mixing the legacy and canonical spelling across hierarchy is supported — normalisation runs before the `+key:` merge.

See [Custom Naming](Custom-Naming) for the `layer="historize"` hook that pairs with this overlay.

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
