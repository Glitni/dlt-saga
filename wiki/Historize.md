# Historize (SCD2 / SCD1)

The historize engine transforms raw snapshot tables into [Slowly Changing Dimension Type 2 (SCD2)](https://en.wikipedia.org/wiki/Slowly_changing_dimension#Type_2:_add_new_row) tables that track the full history of every record — or, with `strategy: scd1`, into [Type 1](https://en.wikipedia.org/wiki/Slowly_changing_dimension#Type_1:_overwrite) tables that keep only the latest version of each record. See [SCD1: latest version per key](#scd1-latest-version-per-key). Everything else on this page describes SCD2 unless it says otherwise.

---

## How it works

Historize reads the source as a sequence of deliveries. The `snapshot_column` identifies them: all rows sharing one of its values form one delivery, and the value becomes `_dlt_valid_from` of the versions it brings. What a delivery holds is declared with `delivery`: under `full` (the default) each one is a snapshot, a complete point-in-time copy of the source; under `incremental` it holds only the records sent in it. This page says "snapshot" for one such delivery under either setting, matching the key's name.

From that sequence, historize computes:

- **When did each version of a record become active?** (`_dlt_valid_from`)
- **When was it superseded?** (`_dlt_valid_to`)
- **Was it deleted?** (`_dlt_is_deleted`)

A row is active when `ts >= _dlt_valid_from AND (ts < _dlt_valid_to OR _dlt_valid_to IS NULL)`.

Under `delivery: full`, a record missing from a snapshot was deleted; under `incremental`, it simply wasn't sent — see [Delivery: full vs incremental](#delivery-full-vs-incremental).

---

## Output columns

| Column | Description |
|--------|-------------|
| `_dlt_valid_from` | When this version became active (snapshot timestamp). Renameable via `valid_from_column`. |
| `_dlt_valid_to` | When this version was superseded (`NULL` = current). Renameable via `valid_to_column`. |
| `_dlt_is_deleted` | `TRUE` on deletion marker rows, `FALSE` on all change rows. Renameable via `is_deleted_column`. |

The three SCD2 column names default to `_dlt_*` but can be renamed per pipeline — useful when a
downstream convention expects domain-specific names. See [Renaming the SCD2 columns](#renaming-the-scd2-columns).

When column docs are enabled (`persist_docs.columns`), these three columns also receive canned
descriptions automatically. See [Descriptions & classification](#descriptions--classification).

Every other column of the source table is carried through unchanged, minus the framework's own
system columns (`_dlt_id`, `_dlt_load_id`, ingest metadata).

### Pseudo-columns (ingestion-time partitioned sources)

Catalog **pseudo-columns** — on BigQuery `_PARTITIONTIME` and `_PARTITIONDATE`, exposed by
**ingestion-time partitioned** tables — are not part of a table's schema: they resolve on a direct
scan of their own table, but are returned by neither `SELECT *` nor a projection.

They are never carried into the historized table as data columns. They **are** usable as the
`snapshot_column` (and as `arrival_column`), which is what matters for an ingestion-time partitioned
source where the partition time is the only timestamp available:

```yaml
write_disposition: historize
primary_key: [customer_id]

historize:
  snapshot_column: _PARTITIONTIME
```

Historize reads the value off the source under an internal alias and carries that through its CTEs,
so the SCD2 output is identical to what a real timestamp column produces — `_dlt_valid_from` holds
the partition time. Prefer `_PARTITIONTIME` over `_PARTITIONDATE`: snapshot values are compared
against `TIMESTAMP` literals, and `_PARTITIONTIME` is the `TIMESTAMP`-typed one.

A pseudo-column cannot be a `primary_key` or `merge_key` — those are written into the historized
table, under a name the destination reserves. Configs naming one there are rejected before any query
runs, as are `snapshot_column` / `primary_key` / `merge_key` values the source simply doesn't have.

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

## Delivery: full vs incremental

`delivery` tells historize what each `snapshot_column` value holds. Saga can't tell from the data, so the source's shape is declared:

| `delivery` | Each snapshot holds | A record missing from a snapshot |
|------------|---------------------|-----------------------------------|
| `full` (default) | Every record that exists at that time | Was deleted (a deletion marker with `track_deletions`); if it returns later, it opens a new version even with unchanged values |
| `incremental` | Only the records sent in that delivery: new, changed or re-sent | Means nothing — it simply wasn't sent |

```yaml
# configs/streams/orders.yml — each load carries only the orders that changed
write_disposition: historize
primary_key: [order_id]
source_schema: landing
source_table: order_changes

historize:
  snapshot_column: loaded_at
  delivery: incremental
```

Under `incremental`, SCD2 opens a new version only when a record's values change. Deletion detection, gap-driven reappearance and `merge_key` all read a record's absence, so they don't apply: `track_deletions` defaults to `false` and `track_deletions: true` or a `merge_key` is rejected.

Setting `full` on an incremental source is what to avoid: every record not re-sent in a batch would get a deletion marker, and every unchanged re-send after a gap would open a redundant version.

Each snapshot should hold at most one row per key, under either setting. When one delivery carries several versions of a key under the same `snapshot_column` value, historize keeps one of them, and which one is not defined — give each version its own snapshot value (e.g. a per-row load or event timestamp).

`delivery` is part of the config fingerprint for `scd2`, so changing it requires `--full-refresh`. Under `scd1` it only decides whether `track_deletions` is allowed, which is fingerprinted itself.

---

## SCD1: latest version per key

`strategy: scd1` keeps one row per primary key: the highest-ranked version seen so far. A newer
version overwrites the stored row, so superseded values don't survive in the historized table. That
matters when a newer version exists to *replace* the old one — a correction, or an anonymized
re-delivery that must not leave the original values readable downstream.

```yaml
# configs/streams/events.yml — an append-only event log, one row per version
write_disposition: historize
primary_key: [event_key]
source_schema: landing
source_table: events

historize:
  strategy: scd1
  snapshot_column: ingested_at            # watermark: which rows are new
  order_by: [ingested_at, landing_offset] # ranking: which version wins
```

The output table holds the primary key, the value columns, and `_dlt_valid_from`: when the current
values took effect, i.e. the snapshot where they first appeared without a different version in
between. There is no `_dlt_valid_to`, and `_dlt_is_deleted` only with `track_deletions: true` (see
below). A newly created table is clustered by `[valid_from]` unless `cluster_columns` is set.

**Ranking.** Each run reads the new source rows, adds the stored row of every key they touch, and
keeps the highest-ranked version per key by `order_by` (descending, `NULL` ranks lowest).
`snapshot_column` is appended as the final tiebreaker unless `order_by` already lists it. Because
new versions are ranked *against* the stored one rather than written over it, the result doesn't depend on the order rows land in: a
version ranked below the stored one loses, however late it arrives. Pick `order_by` so the version
that should win always ranks highest. For example, if a re-delivery keeps the original event time,
ranking by event time alone ties and can't tell the two apart. Every `order_by` column must be
carried into the historized table: a value column, or `snapshot_column` itself.

**Change detection.** A winning version whose values equal the stored row's writes nothing, so a
source that re-delivers unchanged records (full extracts, re-sent events) doesn't rewrite them or
move their `_dlt_valid_from`.

**Runs and refreshes.**

- **Incremental runs** merge the snapshots above the watermark. A retried run converges: the rows
  it already applied are unchanged, so it writes nothing for them.
- **Partial refreshes** (`--partial-refresh`, `--historize-from`) re-merge the window in place. On an
  exact tie the source row wins, so a corrected source row takes effect.
- **Full refresh only** can remove a version that has since left the source.
- **Late arrivals:** `detect_late_arrivals: true` re-merges from the earliest late snapshot. Nothing
  is rewound, so the retention guard doesn't apply.

**Deletions** are opt-in with `track_deletions: true`, for sources that deliver full snapshots
(`delivery: full`, the default — see [Delivery: full vs incremental](#delivery-full-vs-incremental)):

```yaml
historize:
  strategy: scd1
  snapshot_column: extract_date
  track_deletions: true
  # delivery: full           # the default; deletions require it
  # merge_key: [tenant_id]   # scopes deletion detection, as under scd2
```

- A key missing from a later snapshot keeps its row, with `_dlt_is_deleted = TRUE`, the values it
  was last seen with, and `_dlt_valid_from` set to the snapshot it disappeared from. Read current
  state with `WHERE NOT _dlt_is_deleted`.
- When it reappears, the row is overwritten with the new values and the flag cleared.
- The row is kept rather than removed so later versions still have something to be ranked against:
  with no stored row, a late, older version would re-insert a deleted key.
- The latest snapshot decides: a deletion happens at a snapshot and has no values for other ranking
  columns, so `order_by` must start with `snapshot_column`. Further columns break ties between rows
  of one key within a snapshot, e.g. `order_by: [extract_date, updated_at]`.
- A key missing from the batch is what a deletion looks like, so each run reads every stored key
  that isn't deleted — the whole historized table, minus deleted keys.
  Only keys in the batch or disappearing from it are ranked and written.

**Not supported with `scd1`:** `track_columns`, `ignore_columns`, a renamed `valid_to_column`; a
`merge_key` or renamed `is_deleted_column` without `track_deletions: true` — these are rejected
rather than ignored. `order_by` is rejected under `scd2`.

**Switching strategy** changes the config fingerprint, so the next run asks for `--full-refresh`.
Going from `scd1` to `scd2` can't recover the versions `scd1` already overwrote. The rebuilt history
starts from what the source still holds.

---

## Config reference

| Field | Location | Default | Description |
|-------|----------|---------|-------------|
| `primary_key` | top-level | required | Key columns identifying a unique record. Must never be NULL in the source — see [NULL primary keys are rejected](#null-primary-keys-are-rejected) |
| `nullable_key_columns` | top-level | — | `primary_key` columns that may be NULL — see [Composite keys with a NULL part](#composite-keys-with-a-null-part) |
| `snapshot_date_regex` | top-level | — | Regex to extract snapshot date from file paths (filesystem only) |
| `snapshot_date_format` | top-level | — | `strptime` format for the extracted date string |
| `strategy` | `historize:` | `scd2` | `scd2` keeps every version; `scd1` keeps only the latest version per key. See [SCD1: latest version per key](#scd1-latest-version-per-key) |
| `order_by` | `historize:` | `[snapshot_column]` | `scd1` only: columns ranking a key's versions, highest wins. With `track_deletions: true` it must start with `snapshot_column` |
| `snapshot_column` | `historize:` | `_dlt_ingested_at` | Column identifying each delivery (a complete snapshot under `delivery: full`); its value becomes `_dlt_valid_from` |
| `track_columns` | `historize:` | — | Opt-in allowlist: only these columns drive change detection (all columns still appear in output) |
| `ignore_columns` | `historize:` | `[]` | Columns excluded from change detection (still appear in output) |
| `exclude_columns` | `historize:` | `[]` | Source columns left out of the historized table, and so out of change detection. Works under both strategies — see [Leaving columns out](#leaving-columns-out) |
| `partition_column` | `historize:` | — | Partition the SCD2 output table. Unset by default — see [Table layout](#table-layout) |
| `cluster_columns` | `historize:` | `[valid_to, valid_from]` at create | Cluster the SCD2 output table. Explicit values are enforced on existing tables; `[]` removes clustering — see [Table layout](#table-layout) |
| `delivery` | `historize:` | `full` | `full`: each snapshot is a complete copy of the source; `incremental`: each holds only the records sent in it. See [Delivery: full vs incremental](#delivery-full-vs-incremental) |
| `track_deletions` | `historize:` | `true` (`scd2`, `full`) | Emit deletion marker rows when a key disappears (`scd1`: opt-in, flags the key's row). Requires `delivery: full` |
| `merge_key` | `historize:` | — | Subset of `primary_key` that scopes deletion / reappearance detection. Requires `delivery: full`; under `scd1` also `track_deletions: true`. See [Scoping deletion detection (`merge_key`)](#scoping-deletion-detection-merge_key) |
| `detect_late_arrivals` | `historize:` | unset | Late snapshots below the watermark: unset = detect + warn, `true` = detect + rewind-and-replay, `false` = skip detection. See [Late-arriving snapshots](#late-arriving-snapshots) |
| `late_arrival_window_days` | `historize:` | — | How many days behind the watermark a late snapshot may be and still replay automatically; older ones are ignored with a warning. Omit for no bound |
| `arrival_column` | `historize:` | `_dlt_ingested_at` | Column holding each row's warehouse arrival time, used for late-arrival detection |
| `valid_from_column` | `historize:` | `_dlt_valid_from` | Name of the SCD2 valid-from column in the output table |
| `valid_to_column` | `historize:` | `_dlt_valid_to` | Name of the SCD2 valid-to column in the output table |
| `is_deleted_column` | `historize:` | `_dlt_is_deleted` | Name of the soft-delete marker column in the output table (`scd1`: only with `track_deletions: true`) |
| `table_format` | `historize:` | inherited | Table format for the SCD2 output table. Overrides the profile-level setting. See [Table format](#table-format) |
| `schema_name` | `historize:` | — | Write the historized table to this schema instead of the source schema (used directly in prod, sandbox-composed in dev). Legacy alias: `output_schema` |
| `table_name` | `historize:` | — | Explicit name for the historized output table (overrides the auto-generated name). Legacy alias: `output_table` |
| `table_suffix` | `historize:` | `_historized` | Suffix appended to the source table name when `table_name` is not set. Legacy alias: `output_table_suffix` |
| `filters` | `historize:` | — | Declarative row filter applied only during historize — see [Filtering the source](#filtering-the-source) |
| `json_columns` | `historize:` | — | Typed columns extracted from a JSON source column (`{column, path, data_type}` per output name) — see [Columns from a JSON source column](#columns-from-a-json-source-column) |
| `description` | `historize:` | inherited | Table description for the historized table (overrides top-level `description`) — see [Descriptions & classification](#descriptions--classification) |
| `classification` | `historize:` | inherited | Table classification for the historized table (overrides top-level `classification`) |
| `columns` | `historize:` | inherited | Per-column description/classification overrides, merged over top-level `columns` (override only the keys you set) |
| `persist_docs` | `historize:` | inherited | Override `persist_docs` (`{table, columns}`) for the historized table only |

### NULL primary keys are rejected

The primary key *is* the SCD2 identity, and SQL equality is never true for NULL. Change detection partitions by the key — where `NULL` groups with `NULL`, so change rows are produced — but the MERGE that closes the previously-open row (`ON t.pk = n.pk`) never matches, and neither do the deletion-marker joins. A NULL-keyed row therefore gains another permanently-open history row on every run, breaking the "exactly one open row per key" guarantee the historized table exists to provide.

There is no correct NULL-safe answer to *which entity is this*, so historize refuses to write instead (a composite key with a part that is meant to be NULL is the exception — see [Composite keys with a NULL part](#composite-keys-with-a-null-part)). Before any work, each run checks the rows it is about to read and fails if any primary-key column is NULL. The failure is recorded like any failed run, so it shows in `saga report`, `--select "state:failed"` and `saga notify`:

```
Primary key column(s) 'company_id' contain NULL in project.dataset.raw_companies (snapshot 2026-01-02). ...
```

An incremental run gets this from the snapshot-discovery query it already runs, so it costs no extra round trip — and the message names the offending snapshot. A full or partial refresh has no earlier scan of the source to fold into, so it runs one `LIMIT 1` probe first.

Three ways out, in order of preference:

1. Fix the source so the key is always populated.
2. Pick a `primary_key` that is always populated.
3. Exclude the rows from historization with a `historize.filters` entry:

```yaml
historize:
  filters:
    - column: company_id
      op: is_not_null
```

Filtered-out rows are never historized, so their NULL keys don't block the run. The check applies the same filter as the source read, so option 3 takes effect immediately.

The check is scoped to what the run reads: a NULL key that lands in an already-processed snapshot is below the watermark and never blocks later incremental runs. And because it runs before the destructive part of a full refresh, a rejected `--full-refresh` leaves the existing historized table untouched.

If earlier runs already wrote duplicate open rows for a NULL key, rebuild with `--full-refresh` once the source is clean.

### Composite keys with a NULL part

Some composite keys have a part that is legitimately absent: a price keyed by `(customer_id, region)` where the row with no region is the customer's default. For such a column, "NULL is one more key value" is a well-defined identity — it is the reading `PARTITION BY` and `DISTINCT` already give it. Declare the column in `nullable_key_columns` and saga treats it that way on both layers:

```yaml
primary_key: [customer_id, region]
nullable_key_columns: [region]
```

- **Ingest** creates `region` nullable. Key columns are otherwise NOT NULL at ingest — dlt marks every `primary_key` and `merge_key` column non-nullable, for `append` as well as `merge` — so without the declaration a row with a NULL key part fails the load.
- **Historize** compares `region` with the destination's null-safe equality (`IS NOT DISTINCT FROM`, `<=>` on Databricks) in every key join: closing the previous version, deletion markers, reappearance, the scd1 merge. `(1, NULL)` versions chain like any other key, can be deleted and reappear, and never collide with `(1, 'eu')`.
- The [NULL-key guard](#null-primary-keys-are-rejected) skips declared columns and still refuses a NULL in any other key column.

`nullable_key_columns` must be a subset of `primary_key`; `saga validate` rejects anything else. It may also be set under `historize:` to apply to the historize layer only, as `primary_key` can. Declaring a column is not a fingerprinted change: rows with a NULL key were refused before, so no existing history depends on it.

Prefer a populated key when the source can provide one. The option exists for keys where NULL carries meaning, not as a way past a source that occasionally drops a key value — those rows are better excluded with a filter, as above.

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
names. The default clustering follows the renamed columns. All three names are
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

### Descriptions & classification

The historized table gets column and table descriptions/classification too — see
[Descriptions & Classification](Configuration#descriptions--classification) for the general model.
Because the historized table is built by saga's own SQL (dlt never sees it), the reconcile is its
*only* source of descriptions; they're applied after each historize run.

By default it inherits the pipeline's top-level `description`, `classification`, and `columns`.
Override any of them for the historized table specifically under `historize:` — `columns` merges
per column, per key (override only what you set, inherit the rest):

```yaml
description: Customer master data          # ingest table
classification: [confidential]
columns:
  email:
    description: "Primary contact email"
    classification: [pii]

historize:
  track_deletions: true
  description: "SCD2 history of customer master data"   # historized table only
  classification: [confidential, historical]
  columns:
    email:
      classification: [pii, historical]     # override classification; inherit the description
```

The SCD2 system columns (`_dlt_valid_from` / `_dlt_valid_to` / `_dlt_is_deleted`, or your renamed
equivalents) receive canned descriptions automatically. All of this is gated by `persist_docs`
(inherited, or overridden via `historize.persist_docs`) — with the default `columns: false`, only
the table description is written; enable `persist_docs.columns` to document columns.

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
  table_name: stream_writes_tenant_a_historized
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
  table_name: stream_writes_tenant_a_historized
  track_deletions: true
  # Historize-stage filter: further restrict to tenant A's history
  filters:
    - column: tenant_id
      value: tenant_a
```

**A subtlety with `track_deletions: true`.** A row whose `tenant_id` mutates between snapshots (e.g. from `tenant_a` to `tenant_b`) registers as a delete in tenant A's history and an insert in tenant B's history. From each tenant's view that's the correct semantic — the row is gone from theirs and appears in the other's — but worth knowing if you rely on `_dlt_is_deleted` markers downstream.

**Config-change detection.** The filter list is part of the historize config fingerprint. Changing `historize.filters:` between runs triggers the same prompt as changing `primary_key` or `snapshot_column` — you'll be told to run `saga historize --full-refresh` so the existing historized table is rebuilt against the new predicate.

---

## Columns from a JSON source column

Streaming landing tables usually carry a few envelope columns plus the event itself as JSON. `historize.json_columns` projects fields out of such a column into real, typed columns of the historized table:

```yaml
# configs/streams/events.yml
write_disposition: historize
primary_key: [landing_key]
source_schema: streams
source_table: events_v1

historize:
  strategy: scd1
  snapshot_column: ingested_at
  order_by: [ingested_at, landing_offset]
  json_columns:
    event_time: { column: payload, path: eventTime, data_type: timestamp }
    user_id: { column: payload, path: user_id }
    tenant: { column: payload, path: meta.tenant }   # dotted string or list of keys
  partition_column: event_time
```

Each entry maps an output column name to:

| Key | Default | Description |
|---|---|---|
| `column` | required | Source column holding the JSON: a JSON-typed column, or a string containing JSON |
| `path` | required | Keys from the root to the value, as a dotted string (`meta.tenant`) or a list (`[meta, tenant]`). Parsed the same way as a filter's `path`; a key that isn't a plain identifier (`content-type`, `zip code`) is quoted for the destination, and a key containing a `.` needs the list form (`[headers, "a.b"]`) |
| `data_type` | `text` | `text`, `bigint`, `double`, `bool`, `timestamp`, `date` or `json` |

**`text` is the value as text, whatever it is**: a scalar unquoted (`hi`, `5`, `true`), an object or array as its JSON text. The other scalar types convert that text, so an object under `bigint` is a failed conversion (`NULL`, warned about — see below), never a silent one. **`json` keeps the value as a JSON document** — object, array or scalar — in the destination's JSON type: `JSON` on BigQuery and DuckDB, `STRING` on Databricks — where it is the same as `text`: an object or array is its JSON text, but a scalar at the path is unquoted (`x`, not `"x"`), so parse the column only where the path leads to a document. Use it to carry a sub-document as its own column, typically with the envelope in [`exclude_columns`](#leaving-columns-out) so that change detection sees only that part of the payload:

```yaml
historize:
  json_columns:
    identity: { column: payload, path: identity, data_type: json }
    event_type: { column: payload, path: type }
  exclude_columns: [payload]     # a volatile field elsewhere in the payload opens no versions
```

**They are added columns.** The derived columns come after the source's own, and the JSON column itself is still carried into the historized table, so fields you haven't extracted stay available in the history. Adding one later is a schema change on the historized table (see below). If the extracted fields are all you need, list the JSON column in [`exclude_columns`](#leaving-columns-out) — the derived columns still read it from the source.

**They behave like source columns.** Use them as `primary_key`, `order_by`, `snapshot_column`, `arrival_column`, `partition_column`, `cluster_columns`, in `historize.filters`, and in `columns` docs/classification. They take part in change detection like any value column. The JSON column changes whenever an extracted field does, so under `scd2` add it to `ignore_columns` if only the extracted fields should open versions. A `json` entry is a value column only: a JSON document can't be compared, ordered or clustered on every destination, so it is refused as a key, `snapshot_column`, `arrival_column`, `order_by`, partition or cluster column, and in a filter without a `path` (`is_null`/`is_not_null` aside) — extract the scalar you need as its own entry, or filter on a path inside it.

**Missing and invalid values are NULL.** A missing path, a JSON `null`, input that isn't valid JSON, or a value that doesn't convert to `data_type` is written as `NULL` — the query never fails on it. Values that are present but don't convert (`"many"` or an object as `bigint`, epoch numbers as `timestamp`) are reported with a warning naming the column, since a payload whose format drifted would otherwise go unnoticed. Timestamps convert from ISO-8601 strings (an offset is honoured); `date` expects `YYYY-MM-DD`.

**Filters on a derived column compare typed.** A path-based filter always compares as text (`value: true` matches the string `'true'`); a filter naming a derived column compares with its declared type. Path filters still suit a condition on a field you don't want as a column.

```yaml
historize:
  json_columns:
    amount: { column: payload, data_type: bigint }
  filters:
    - column: amount         # bigint comparison
      op: in
      value: [50, 100]
    - column: payload        # text comparison, no column added
      path: meta.tenant
      value: t1
```

**Rejected configs.** An output name equal to a column of the source, one historize writes itself (`_dlt_*`, the configured `valid_from_column`/`valid_to_column`/`is_deleted_column`), or one the destination reserves (BigQuery's `_PARTITION…`); a `column` that isn't a column of the source table (derived columns can't read each other); a `json` entry anywhere it would be compared (see above); and a pseudo-column `snapshot_column`/`arrival_column` such as `_PARTITIONTIME` — the source is read through a subquery, which doesn't carry pseudo-columns.

**Adding, removing and changing entries.** An entry is a value column like any other, so adding or removing one is a [schema change](#schema-changes): a new entry is added to the historized table (rows written before it hold `NULL`, nothing is backfilled), and a removed one is kept with its history and `NULL` from then on. Redefining an entry that already exists (its `column`, `path` or `data_type`) prompts for `saga historize --full-refresh`: the stored column would otherwise change meaning mid-history under one name, and a type can't change in place. Removing an entry and re-adding it under the same name with another type is refused the way a source column's type change is, since the column kept its old type on the target. Spelling out `data_type: text` or reordering entries is not a change.

---

## Deletion tracking

When `track_deletions: true`, a key disappearing from the source produces a **separate deletion marker row**. This requires `delivery: full` (the default) — see [Delivery: full vs incremental](#delivery-full-vs-incremental):

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

## Table layout

How the historized table is partitioned and clustered. Both are physical layout, not
semantics: changing them never triggers the config-fingerprint rebuild prompt.

**Defaults.** The table is created unpartitioned and clustered by
`[valid_to_column, valid_from_column]` — the key every SCD2 read filters on:
`_dlt_valid_to IS NULL` for current state, `_dlt_valid_from <= T AND (_dlt_valid_to > T OR
_dlt_valid_to IS NULL)` for as-of. The validity columns don't make good partition keys: the
open rows are spread across every `valid_from` partition, so current-state reads touch all of
them, and on Databricks a timestamp partition key produces one Delta partition per distinct
snapshot value. Destinations without clustering (DuckDB, Databricks Iceberg) get a plain table.

**Explicit settings.** `partition_column` and `cluster_columns` under `historize:` replace the
defaults. `cluster_columns: []` creates an unclustered table.

```yaml
historize:
  cluster_columns: [_dlt_valid_to, _dlt_valid_from, customer_id]  # add the key for per-key lookups
```

**Existing tables.** `CREATE TABLE` applies the layout only when the table is created, so after
each incremental run historize compares the table against the *explicit* settings:

- `cluster_columns` set and different from the table's → clustering is reconciled in place
  (metadata-only; BigQuery and Databricks recluster lazily). `[]` removes it.
- `partition_column` set and different from the table's → a warning. Partitioning can't be
  changed in place; run `saga historize -s "<pipeline>" --full-refresh` to rebuild.
- Unset → the table is left as it is. The defaults apply at create time only, so upgrading never
  changes the layout of a table nobody asked to change.

On Databricks, partitioning and liquid clustering are mutually exclusive: an explicit
`cluster_columns` on a partitioned table warns and points to a full refresh, and when both keys
are set the table is created clustered only.

> **Upgrading from 0.35 or earlier.** Those versions partitioned every historized table by
> `valid_from_column`. Existing tables keep that layout until their next full refresh, which
> rebuilds with the defaults above. To keep the old layout through a full refresh, set
> `partition_column: _dlt_valid_from` explicitly.

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

### Late-arriving snapshots

Incremental discovery is watermark-based: it looks for snapshot values above the last historized one. That assumes the snapshot column advances monotonically for the whole source — which it doesn't when the source unions independently-delivered partitions sharing one snapshot column (the `merge_key` configuration), or whenever a file for an already-processed snapshot date lands late. A snapshot at or below the watermark would never be discovered, and its versions would be silently lost.

Every incremental run therefore also checks for rows whose snapshot value is at or below the watermark but whose **arrival time** (`arrival_column`, default `_dlt_ingested_at`) postdates the previous run's start. What happens on a hit is controlled by `detect_late_arrivals`:

- **unset** (default): the run **warns** — naming the count and earliest late snapshot, and suggesting this key — but historizes nothing extra. Late data never disappears silently, and the history-rewriting replay stays a deliberate choice.
- **`true`**: the run warns, rewinds the historized table to the earliest late snapshot, and replays every snapshot from that boundary — the same idempotent rewind-and-replay mechanism that makes crashed incremental runs safe to retry. History comes out exactly as if every file had arrived on time, including healing any deletion markers the late file's absence had produced.
- **`false`**: detection is skipped entirely (silences the warning; watermark-only discovery).

```yaml
historize:
  snapshot_column: delivery_date
  merge_key: [dbinstance]
  detect_late_arrivals: true
  late_arrival_window_days: 14   # optional: accept files up to 14 days late
```

Notes:

- Detection only makes sense with a custom `snapshot_column`: with the default (`_dlt_ingested_at`), snapshots are stamped at load time and can't arrive late, so detection is skipped and the default configuration pays no extra query.
- The replay rebuilds the rewind window from the raw table, so raw must still contain every historized snapshot in that window. A **retention guard** verifies this before rewinding: if raw has lost part of the window (e.g. `partition_expiration_days` on the raw table), the run warns and skips the replay instead of truncating history — handle it manually with `--historize-from` at a boundary raw still covers, or accept the gap.
- Detection runs for `append+historize` and `historize` (external delivery) sources; `replace`/`merge` sources overwrite prior snapshot rows, so it's skipped there.
- External deliveries without a `_dlt_ingested_at` column can point `arrival_column` at their own load-time column (and should usually add it to `ignore_columns` so a redelivery of unchanged data doesn't register as a change). Without an arrival column, detection is skipped — with `detect_late_arrivals: true` the skip is warned about, so an opted-in pipeline can't quietly do nothing. A [pseudo-column](#pseudo-columns-ingestion-time-partitioned-sources) is accepted here.
- A replay's cost is proportional to how far back the late snapshot sits. `late_arrival_window_days` bounds it: late snapshots further behind the watermark are ignored with a warning instead of replayed, so a stray very old file can't trigger a huge rewind. Per detection run the overhead is one arrival-time query (cheap on raw tables clustered by `_dlt_ingested_at`).

**Full refresh** (`--full-refresh`): rebuilds the SCD2 table from all raw snapshots. Required after config changes:

```bash
saga historize --full-refresh --select "filesystem__snapshots__companies"
```

Historize detects config changes via a fingerprint stored in `_saga_historize_log` and prompts for a full refresh when any of these changes: `primary_key`, `snapshot_column`, `strategy`, `order_by`, `delivery` (`scd2` only), `track_columns`, `ignore_columns`, `exclude_columns`, `track_deletions`, `merge_key`, `filters`, a `json_columns` entry's `column`, `path` or `data_type` (adding or removing an entry is a [schema change](#schema-changes)), `table_format`, or the output column names (`valid_from_column`, `valid_to_column`, `is_deleted_column`).

One kind of edit is exempt: **narrowing change detection**, i.e. adding a column to `ignore_columns` or `exclude_columns`, or dropping one from `track_columns`. Versions already written were real changes at the time and stay correct, and the hash is recomputed on every run, so the edit simply applies from the next run on and is logged at info level. Widening detection again (removing a column from `ignore_columns` or `exclude_columns`, adding one to `track_columns`) still prompts, since history before the edit has no versions for that column.

---

## Schema changes

The historized table is created from the source's columns on the first run. When the source's columns change later, every run that writes into the existing table (incremental, partial refresh, a late-arrival replay) reconciles the target first, before anything is written. A schema change is not a config change: it never prompts for `--full-refresh`.

**Column added.** The column is added to the historized table with the source's type, logged at info level. Rows written before it existed hold `NULL`; nothing is backfilled. A key's next version with a value in the new column opens as usual, since going from unknown to known is a change: under `scd2` every key that arrives with a value gets a new version at that snapshot, under `scd1` its `_dlt_valid_from` moves there. A key that arrives with `NULL` in the new column is unchanged. A column listed in `ignore_columns` (or absent from `track_columns`) is carried like any other untracked column and opens no versions, so a noisy new column is silenced by adding it to `ignore_columns`; that narrows change detection and needs no rebuild.

**Column dropped.** The historized table keeps the column and its history. The source is read as if it still had the column, with `NULL` in it, so a key's next version carries `NULL` instead of a value the source no longer delivers; under `scd1` that is the updated row. Keys the source doesn't re-deliver are untouched. The run warns the first time the column is missing (the log records each run's source columns, so the warning doesn't repeat), since a drop is often unintended upstream. A `--full-refresh` rebuilds from the source's current columns, without the dropped one.

**Column type changed.** The run is refused before any write, naming the column and both types, and points at `--full-refresh`, which rebuilds the history with the new type from what the source still holds. A widening the destination would accept is refused too: it changes the change-detection hash of every stored row.

For sources ingested by saga, only the added case arises: dlt never drops or retypes a raw table's columns (a value that no longer fits its column lands in a variant column such as `price__v_text`, and a column dropped upstream is simply `NULL` from then on). Dropped and retyped columns come from [external deliveries](#external-data-historize-only) whose table someone else maintains, or from [`json_columns`](#columns-from-a-json-source-column) entries you remove or redefine.

### Leaving columns out

By default every source column ends up in the historized table. `exclude_columns` leaves the listed columns out — for a column that belongs in the raw table but not in history: a sensitive field the raw table's retention already bounds, a large payload nobody queries historically, or the JSON column behind your [`json_columns`](#columns-from-a-json-source-column) once the fields you need are extracted.

```yaml
historize:
  exclude_columns: [raw_payload, national_id]
```

An excluded column is out of change detection too — a column that isn't in the output can't be compared — so unlike `ignore_columns` (which keeps the column in the output and only stops it from opening versions) it works under `scd1` as well as `scd2`. It can't name a column the historized table has to carry: `primary_key`/`merge_key`, `snapshot_column`, `order_by`, `partition_column`, `cluster_columns`, a `json_columns` output name, or a column also in `track_columns` or `ignore_columns` (two settings about one column).

**Adding a column to `exclude_columns`** after the historized table exists is forward-only, like a [dropped source column](#schema-changes): the column stays on the table with its history, new versions get `NULL` in it, and no versions open because of the exclusion. It stops the column from being *collected*, not *retained* — the open row of a key keeps its value until that key next changes. To remove the column's values, run `--full-refresh` (rebuilds without it) or drop the column from the historized table by hand; after either, the run has nothing more to do with it. The hand drop is the way to go when a rebuild would lose history: a `replace` source only holds its latest snapshot, and a raw table past partition expiration no longer holds every snapshot.

**Removing a column from `exclude_columns`** prompts for `--full-refresh`, like any widening of change detection.

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
- The source's columns as each run saw them — a dropped column is warned about once, not every run
- Timing information per historize run

---

## Full example

```yaml
# configs/filesystem/vendor/companies.yml
write_disposition: "append+historize"
primary_key: [orgnr]

# Snapshot date extraction from file paths
snapshot_date_regex: "\\d{4}-\\d{2}-\\d{2}"
snapshot_date_format: "%Y-%m-%d"

historize:
  # snapshot_column: _dlt_ingested_at  # default
  ignore_columns: [updated_by]
  cluster_columns: [_dlt_valid_to, _dlt_valid_from, orgnr]
  track_deletions: true
```
