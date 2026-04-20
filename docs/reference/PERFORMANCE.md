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

## Row Limiting in Dev

Use `dev_row_limit` in pipeline configs to limit rows during development (only applies in dev environment):

```yaml
dev_row_limit: 100    # Only load 100 rows in dev
```

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
