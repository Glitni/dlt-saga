# Performance Tuning

## Parallel Pipeline Execution

Run multiple pipelines concurrently with the `--workers` flag:

```bash
saga ingest --workers 8          # 8 parallel workers
saga ingest --workers 1          # Sequential (debugging)
saga historize --workers 8       # Parallel historization
saga run --workers 8             # Parallel for both phases
```

**Recommended worker counts:**

| Pipelines | Workers |
|-----------|---------|
| 1–3 | `--workers 1` |
| 4–10 | `--workers 4` (default) |
| 10–20 | `--workers 6-8` |
| 20+ | `--workers 8-12` |

## dlt Internal Parallelism

The framework ships sensible defaults (in `dlt_saga/defaults.py`): 4 normalize workers, 2 load workers, etc. These can be overridden in `.dlt/config.toml` or via environment variables:

```toml
# .dlt/config.toml — only add entries to override the defaults
[normalize]
workers = 8                     # Override default of 4

[load]
workers = 4                     # Override default of 2
```

Or via environment variables: `NORMALIZE__WORKERS=8`, `LOAD__WORKERS=4`.

For filesystem sources, normalize workers process multiple files concurrently (3–10x speedup for multi-file workloads).

## Connection Pooling

API client connections are automatically pooled and reused (Google Sheets/Drive API, BigQuery, credentials). This reduces initialization overhead by 10–20%.

Implementation: `utility/gcp/client_pool.py`

## Cloud Run Optimization

Tune worker count based on allocated CPU:

| CPU | Workers |
|-----|---------|
| 1 | `--workers 2-4` |
| 2 | `--workers 4-8` |
| 4 | `--workers 8-12` |

## Faster Dev Runs

There are two distinct levers, for two distinct costs.

### `dev_row_limit` — cap rows (bulk sources)

`dev_row_limit` (set in the profile target) is a **consumer-side** cap: it tells dlt to stop pulling from a resource after N rows.

```yaml
# profiles.yml (dev target)
dev_row_limit: 500
```

It only shortens *extraction* for adapters that **stream** their rows to dlt lazily. For an adapter that fetches everything up front (e.g. a full table query, or a date-window source that loops over many windows before yielding), the fetch has already happened — the limit just caps what gets *written*. It's the right tool for a single bulk table or sheet where you want "the first N rows"; it is **not** the tool for date-windowed / incremental sources.

### `dev:` override block — shrink the source range (windowed / incremental sources)

For date-windowed and incremental pipelines the cost is the **time range**, not the row count — fetching a year of daily windows is slow no matter how few rows you keep. Shrink the range itself with a `dev:` block, whose keys override the top-level config **only when the active environment is `dev`** (and are stripped everywhere else):

```yaml
# configs/api/myservice/events.yml
initial_value: "2020-01-01"          # prod seed, untouched
dev:
  initial_value: "{{ (datetime.now(timezone.utc) - timedelta(days=7)).strftime('%Y-%m-%d') }}"
```

`datetime` / `timedelta` / `timezone` are in scope in templates (see [Configuration](Configuration#templating)), so the seed is a **rolling** date computed at run time — it never goes stale, so the dev dataset doesn't grow unbounded over time.

It works at every level of the config hierarchy (file > folder > project). Since the block is type-agnostic — you write the value — and pipelines in a group tend to share a cursor type (all dates, or all numeric IDs), the natural place for a shared `initial_value` is a **folder (pipeline-group) default**, not a single project-wide value:

```yaml
# saga_project.yml
pipelines:
  api:                                  # applies to all configs/api/** pipelines
    +dev:
      initial_value: "{{ (datetime.now(timezone.utc) - timedelta(days=7)).strftime('%Y-%m-%d') }}"
```

A per-pipeline `dev:` block and a project-wide `pipelines: +dev:` default are also supported; reserve the project-wide form for overrides that are uniform across all groups.

## Backfill with Overrides

For backfilling specific date ranges without modifying pipeline state:

```bash
saga ingest --select "group:api" \
  --start-value-override "2025-01-01" \
  --end-value-override "2025-06-30"
```

## Troubleshooting

| Issue | Solution |
|-------|----------|
| Pipelines timing out | Reduce `--workers` to avoid resource contention |
| Rate limit errors | Reduce `load.workers` in `.dlt/config.toml` or set `LOAD__WORKERS=1` |
| Memory errors | Reduce `normalize.workers` or use `--workers 1` |
| Slow historization | Use `--full-refresh` once, then incremental is faster |
