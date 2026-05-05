# Getting Started

This guide takes you from installation to a running pipeline in under 10 minutes.

## Prerequisites

- Python 3.11 or 3.12
- For **BigQuery**: a Google Cloud project with billing enabled and the [`gcloud` CLI](https://cloud.google.com/sdk/docs/install)
- For **Databricks**: a workspace URL and credentials (PAT, service principal, or browser auth)
- For **DuckDB**: nothing extra — ideal for trying out dlt-saga locally without any cloud accounts

---

## 1. Install

Install dlt-saga with the extras matching your destination:

```bash
pip install dlt-saga[bigquery]              # BigQuery
pip install dlt-saga[databricks,azure]      # Databricks on Azure
pip install dlt-saga                        # DuckDB only (no cloud dependencies)
```

---

## 2. Create a project

Run `saga init` in a new directory:

```bash
mkdir my-pipelines && cd my-pipelines
saga init
```

The wizard asks:

1. **Destination type** — `bigquery`, `databricks`, or `duckdb`
2. **Connection details** — GCP project ID, Databricks workspace hostname, etc.
3. **Dev schema name** — the dataset/schema used during development (default: `dlt_dev`)

For DuckDB (the fastest way to try dlt-saga), skip all prompts:

```bash
saga init --no-input
```

This creates a ready-to-run project:

```
my-pipelines/
├── profiles.yml            # Connection settings per environment (dev, prod)
├── saga_project.yml        # Project-level settings (providers, orchestration, hooks)
├── packages.yml            # Register external pipeline packages
├── configs/
│   └── filesystem/
│       └── sample.yml      # A working pipeline that reads data/sample.csv
├── data/
│   └── sample.csv          # Sample data for the example pipeline
└── .dlt/
    └── config.toml         # dlt performance tuning (all commented out)
```

For cloud destinations (BigQuery, Databricks), `saga init` creates a config template instead of a runnable sample — you'll need to fill in your source details.

---

## 3. Authenticate

**BigQuery:**

```bash
gcloud auth application-default login
```

**Databricks:**  
The generated `profiles.yml` has instructions for each auth mode (browser, service principal, or PAT token).

**DuckDB:**  
No authentication needed — skip to step 4.

---

## 4. Verify your setup

Run `saga doctor` to check that everything is configured correctly:

```bash
saga doctor
```

This validates your profiles, project config, pipeline discovery, and destination connectivity. If something is wrong, the output tells you exactly what to fix.

### Optional: set up AI assistance

If you use an AI coding assistant (Claude Code, Cursor, Copilot, etc.), run:

```bash
saga ai-setup
```

This generates `saga_ai_context.md` in your project root with framework patterns and pipeline implementation guidance. Add the one-line instruction it prints to your AI tool's context file — your assistant will then understand dlt-saga's conventions when helping you build custom pipelines.

---

## 5. Explore your project

```bash
saga list                               # All configured pipelines
saga list --resource-type ingest        # Ingest-enabled only
saga list --resource-type historize     # Historize (SCD2) enabled only
```

With DuckDB defaults, you should see `filesystem__sample` in the list. Validate your configs without moving data:

```bash
saga validate                           # Parse and validate all configs
```

---

## 6. Run a pipeline

```bash
# Run the sample pipeline
saga ingest --select "filesystem__sample"

# Run all ingest-enabled pipelines
saga ingest

# Run with more parallelism (default: 4 workers)
saga ingest --workers 8
```

---

## 7. Check the results

**BigQuery:** open the dataset configured in `profiles.yml` (default: `dlt_dev`).  
**Databricks:** open the Unity Catalog schema you configured.  
**DuckDB:** the `local.duckdb` file is created in your project directory. Query it with the `duckdb` CLI or any SQL tool.

Generate a run report:

```bash
saga report --open                      # Opens in browser (last 14 days of history)
```

---

## Next: add a real pipeline

1. Create a YAML config in `configs/<source_type>/<name>.yml`
2. Verify it appears: `saga list`
3. Test in dev: `saga ingest --select "<group>__<name>"`
4. Enable SCD2 historization: set `write_disposition: append+historize` and run `saga historize`

### Minimal example — append from a database table

```yaml
# configs/database/my_db/orders.yml
tags: [daily]

database_type: postgres
host: db.example.com
port: 5432
source_database: mydb
source_table: orders
username: "${DB_USER}"
password: "${DB_PASSWORD}"

write_disposition: append
incremental: true
incremental_key: updated_at
initial_value: "2024-01-01"
```

```bash
saga ingest --select "database__my_db__orders"
```

### With historization (SCD2)

```yaml
# configs/filesystem/snapshots/companies.yml
tags: [daily]

filesystem_type: gs
bucket_name: my-bucket
file_glob: "snapshots/*.parquet"
file_type: parquet

write_disposition: append+historize
primary_key: [company_id]

historize:
  partition_column: _dlt_valid_from
  cluster_columns: [company_id]
  track_deletions: true
```

```bash
saga run --select "filesystem__snapshots__companies"
# Runs ingest then historize in sequence
```

---

## Further reading

- [Configuration](Configuration) — all config fields, source types, historize options
- [Profiles](Profiles) — multiple environments, service account impersonation
- [CLI Reference](CLI-Reference) — full command and flag reference
- [Deployment](Deployment) — Cloud Run, orchestration, production setup
- [Plugin Development](Plugin-Development) — write custom pipeline sources and hooks
