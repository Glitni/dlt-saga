# dlt-saga

Config-driven data ingestion and historization framework, built on [dlt](https://dlthub.com/).

[![PyPI version](https://img.shields.io/pypi/v/dlt-saga.svg)](https://pypi.org/project/dlt-saga/)
[![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](LICENSE)
[![CI](https://github.com/Glitni/dlt-saga/actions/workflows/ci.yml/badge.svg)](https://github.com/Glitni/dlt-saga/actions/workflows/ci.yml)
[![codecov](https://codecov.io/gh/Glitni/dlt-saga/graph/badge.svg)](https://codecov.io/gh/Glitni/dlt-saga)
[![Python](https://img.shields.io/pypi/pyversions/dlt-saga.svg)](https://pypi.org/project/dlt-saga/)

## Why dlt-saga?

[dlt](https://dlthub.com/) is an excellent Python library for building data pipelines.
dlt-saga adds the **operational layer** that teams need to run dlt at scale:

| What you get | How |
|---|---|
| **Zero-code pipelines** | Drop a YAML file in `configs/` — no Python needed for common sources |
| **SCD2 historization** | `write_disposition: append+historize` turns any snapshot table into a full change history with `_dlt_valid_from` / `_dlt_valid_to` |
| **dbt-style selectors** | `saga ingest --select "tag:daily,group:api"` — union, intersection, glob patterns |
| **Multi-environment profiles** | `profiles.yml` with dev/prod targets, service account impersonation, per-environment datasets |
| **Plugin architecture** | Register custom sources and destinations via `packages.yml` or Python entry points — no framework fork needed |
| **Cloud-agnostic** | BigQuery today, Databricks and DuckDB included, more via plugins |

If you are already using dlt directly and finding yourself re-implementing incremental
state management, environment switching, or SCD2 transforms — dlt-saga is the
config layer you are building.

## Architecture

<img src="https://raw.githubusercontent.com/Glitni/dlt-saga/main/docs/images/architecture.png" width="700" alt="Architecture">
```

## Quick Start

```bash
# 1. Install
pip install dlt-saga[bigquery]          # or dlt-saga[databricks,azure], dlt-saga

# 2. Scaffold a new project
mkdir my-pipelines && cd my-pipelines
saga init                               # prompts for destination and credentials

# 3. Authenticate (GCP — skip for DuckDB)
gcloud auth application-default login

# 4. List available pipelines
saga list

# 5. Run a pipeline
saga ingest --select "example__sample"
```

> See the **[Getting Started guide](docs/getting-started.md)** for a full walkthrough, or browse [`example/`](example/) for a minimal runnable setup.

> Local execution is the default. Use `--orchestrate` to fan out to parallel workers (requires `orchestration:` configured in `saga_project.yml`).

## CLI Commands

All commands are subcommands under the `saga` entry point and share common options:
`--select`, `--verbose`, `--profile`, `--target`.

<img src="https://raw.githubusercontent.com/Glitni/dlt-saga/main/docs/images/cli-commands.png" width="800" alt="CLI commands">

### Selectors (dbt-style)

Selectors filter which pipelines to run. They work across all commands.

| Syntax | Meaning | Example |
|--------|---------|---------|
| `name` | Exact pipeline name | `--select google_sheets__my_pipeline` |
| `*glob*` | Glob pattern | `--select "*balance*"` |
| `tag:name` | Filter by tag | `--select "tag:daily"` |
| `group:name` | Filter by source group | `--select "group:google_sheets"` |
| space-separated | UNION (OR) | `--select "tag:daily group:filesystem"` |
| comma-separated | INTERSECTION (AND) | `--select "tag:daily,group:google_sheets"` |

### Common Examples

```bash
# List pipelines
saga list                                        # All enabled pipelines
saga list --resource-type ingest                 # Ingest-enabled only
saga list --resource-type historize              # Historize-enabled only
saga list --select "tag:daily"                   # Filtered by tag

# Ingest
saga ingest --select "tag:daily"
saga ingest --select "group:api" --workers 8
saga ingest --full-refresh --select "my_pipeline"
saga ingest --select "group:api" --start-value-override "2026-01-01"  # Backfill

# Historize (SCD2)
saga historize --select "tag:daily"
saga historize --full-refresh --select "filesystem__*"

# Run (ingest + historize sequentially)
saga run --select "tag:daily"

# Update BigQuery access controls
saga update-access --select "group:google_sheets"

# Target a specific environment
saga ingest --target prod --select "tag:daily"   # production (with impersonation)
```

## Adding a New Pipeline

Create a YAML config file in `configs/<source_type>/` — that's it. The framework auto-discovers configs.

<details>
<summary><b>API</b></summary>

Create a config in `configs/api/<api_name>/` and optionally a custom pipeline implementation in `pipelines/api/<api_name>/`.

The framework uses polymorphic pipeline loading — it resolves implementations from most specific to least specific:

1. `pipelines/api/<api_name>/<endpoint>/pipeline.py` (config-specific)
2. `pipelines/api/<api_name>/pipeline.py` (API-specific)
3. `pipelines/api/pipeline.py` (base API pipeline)

```yaml
# configs/api/myservice/users.yml
base_url: "https://api.example.com"
endpoint: "/users"
auth_type: "bearer"
auth_token: "googlesecretmanager::project::api-token"
response_path: "data"           # JSON path to the records array

write_disposition: "append"
tags: ["daily"]
```

**With pagination** (offset, page, cursor, or next_url):

```yaml
# configs/api/myservice/events.yml
base_url: "https://api.example.com"
endpoint: "/events"
auth_type: "bearer"
auth_token: "googlesecretmanager::project::api-token"
response_path: "data"

pagination:
  type: cursor                  # offset, page, cursor, next_url
  cursor_path: "meta.next_cursor"
  cursor_param: "cursor"
  limit: 100
  limit_param: "per_page"

page_delay: 0.2                 # seconds between requests (rate limiting)

write_disposition: "append"
tags: ["daily"]
```
</details>

<details>
<summary><b>Database (PostgreSQL, MySQL, SQL Server, etc.)</b></summary>

Uses [ConnectorX](https://github.com/sfu-db/connector-x) with Apache Arrow for high-performance extraction.

```yaml
# configs/database/mydb/customers.yml
# Connection (option 1: connection string)
connection_string: "postgresql://user:pass@host:5432/mydb"

# Connection (option 2: individual components)
database_type: "postgres"  # postgres, mysql, mssql, oracle, etc.
host: "db.example.com"
port: 5432
source_database: "mydb"
username: "googlesecretmanager::project::db-user"
password: "googlesecretmanager::project::db-password"

# What to extract
source_table: "customers"
# OR: query: "SELECT * FROM customers WHERE active = true"

# Incremental loading (optional)
incremental: true
incremental_key: "updated_at"
initial_value: "2025-01-01"

write_disposition: "merge"
primary_key: "customer_id"
tags: ["daily"]
```

**Supported databases**: PostgreSQL, MySQL, MariaDB, SQL Server, Oracle, SQLite, Redshift, ClickHouse, BigQuery, Trino
</details>

<details>
<summary><b>Filesystem (GCS, SFTP)</b></summary>

```yaml
# configs/filesystem/mybucket/events.yml
filesystem_type: "gs"  # or "sftp", "file"
bucket_name: "my-bucket"
file_glob: "data/events/*.parquet"
file_type: "parquet"  # csv, json, jsonl, parquet

# CSV-specific
csv_separator: ","

# Incremental loading
incremental: true
incremental_column: "modification_date"
initial_value: "2024-01-01"

write_disposition: "append"
tags: ["hourly"]
```
</details>

<details>
<summary><b>Google Sheets</b></summary>

```yaml
# configs/google_sheets/my_sheet.yml
spreadsheet_id: "YOUR_SPREADSHEET_ID"  # From the URL
sheet_name: "Sheet1"
range: "A:Z"

write_disposition: "replace"
tags: ["daily"]
```

Grant Viewer access to the service account configured in `providers.google_secrets` in `saga_project.yml`.
</details>

<details>
<summary><b>SharePoint</b></summary>

Downloads a file from SharePoint using the app-only OAuth 2.0 flow. Requires `pip install "dlt-saga[azure]"`.

```yaml
# configs/sharepoint/my_report.yml
adapter: dlt_saga.sharepoint

# Authentication (SharePoint app-only OAuth2 form body stored in a secrets provider)
auth_secret: "azurekeyvault::https://my-vault.vault.azure.net::MY-SP-AUTH-SECRET"
tenant_id: "<azure-ad-tenant-id>"

# File location
site_url: "https://contoso.sharepoint.com/sites/MyTeam"
file_path: "/sites/MyTeam/Shared Documents/reports/weekly.xlsx"
file_type: xlsx      # xlsx, csv, json, jsonl

# Excel-specific (optional)
sheet_name: "Data"
header_row: 1

write_disposition: "replace"
tags: ["daily"]
```

The `auth_secret` must resolve to a URL-encoded OAuth2 form body:
```
grant_type=client_credentials&client_id=<app-id>@<tenant-id>&client_secret=<secret>&resource=00000003-0000-0ff1ce00-000000000000/<host>@<tenant-id>
```
</details>

## Write Dispositions and Historize

The `write_disposition` field controls what operations are enabled for a pipeline:

<img src="https://raw.githubusercontent.com/Glitni/dlt-saga/main/docs/images/write-disposition.png" width="500" alt="Write disposition">

| Value | Ingest | Historize | Use Case |
|-------|--------|-----------|----------|
| `append` | Yes | No | Raw event/log data |
| `merge` | Yes | No | Upsert on primary key |
| `replace` | Yes | No | Full refresh each run |
| `append+historize` | Yes | Yes | Snapshot → SCD2 |
| `historize` | No | Yes | External data → SCD2 |

### Historize (SCD2)

Historize transforms raw snapshot data into [Slowly Changing Dimension Type 2](https://en.wikipedia.org/wiki/Slowly_changing_dimension#Type_2:_add_new_row) tables.

**Output columns:**

| Column | Description |
|--------|-------------|
| `_dlt_valid_from` | When this version became active (snapshot timestamp) |
| `_dlt_valid_to` | When this version was superseded (`NULL` = current) |
| `_dlt_is_deleted` | `TRUE` on deletion marker rows, `FALSE` on all change rows |

Validity uses half-open intervals: a row is active when `ts >= _dlt_valid_from AND (ts < _dlt_valid_to OR _dlt_valid_to IS NULL)`.

**Deletion tracking:** When `track_deletions: true`, a key disappearing from the source produces a **separate deletion marker row** (`_dlt_is_deleted = TRUE`) rather than flagging the closed row. The deletion marker has `_dlt_valid_to = NULL` (open-ended) until the key reappears, at which point it is closed like any other row. This cleanly separates "this version ended" from "this record was deleted."

```yaml
# configs/filesystem/proffdata/companies.yml
write_disposition: "append+historize"
primary_key: [orgnr]

# Snapshot date extraction from file paths (filesystem only)
snapshot_date_regex: "\\d{4}-\\d{2}-\\d{2}"
snapshot_date_format: "%Y-%m-%d"

historize:
  # snapshot_column: _dlt_ingested_at  # default
  exclude_columns: [_dlt_source_file_name]
  partition_column: "_dlt_valid_from"
  cluster_columns: [orgnr]
  track_deletions: true
```

Historize runs **incrementally** by default (only new snapshots). Use `--full-refresh` to rebuild from scratch.

## Common Configuration Options

<details>
<summary><b>BigQuery Table Options</b></summary>

```yaml
partition_column: "date"                        # Partition by date column
cluster_columns: ["user_id", "region"]          # Cluster (max 4)
dataset_name: "custom_dataset"                  # Override default dataset
```
</details>

<details>
<summary><b>Tags and Scheduling</b></summary>

```yaml
tags: ["daily", "critical"]
# Run with: saga ingest --select "tag:daily"
```
</details>

<details>
<summary><b>Hierarchical Config Defaults</b></summary>

Use `configs/dlt_project.yml` to set defaults that apply across multiple pipelines:

```yaml
# configs/dlt_project.yml
project:
  tags: ["production"]

  google_sheets:
    +tags: ["sheets"]           # Inherits "production", adds "sheets"
    write_disposition: "replace"
```

Syntax: `+key:` merges with parent, `key:` overrides. See [Configuration Guide](docs/reference/CONFIGURATION.md).
</details>

<details>
<summary><b>Access Control</b></summary>

```yaml
access:
  - "group:analytics-team@org.com"
  - "user:someone@org.com"
```

Apply with `saga update-access`.
</details>

## Local Development

<details>
<summary><b>Environment Variables</b></summary>

- `SAGA_SCHEMA_NAME` — Override the default dataset/schema name for dev targets
- `SAGA_PROFILES_DIR` — Override the directory where `profiles.yml` is searched

Use **profiles** for environment switching — see [Profiles Guide](docs/reference/PROFILES.md).
</details>

<details>
<summary><b>DuckDB (Local Testing)</b></summary>

Use DuckDB as a lightweight local destination — no GCP credentials needed:

```yaml
# profiles.yml
default:
  target: local
  outputs:
    local:
      type: duckdb
      database_path: "./dev.duckdb"
      schema: my_dataset
      environment: dev
```

```bash
saga ingest --target local --select "my_pipeline"
```
</details>

<details>
<summary><b>Managing Dependencies</b></summary>

```bash
# Install all dependencies for development (includes GCP + dev tools)
uv sync --extra dev

# Update lock file
uv lock --upgrade

# Add a new dependency
# 1. Add to pyproject.toml under [project.dependencies] or [project.optional-dependencies]
# 2. Regenerate the lock file and sync
uv lock && uv sync --extra dev
```
</details>

## Community

- [GitHub Issues](https://github.com/Glitni/dlt-saga/issues) — bug reports and feature requests
- [GitHub Discussions](https://github.com/Glitni/dlt-saga/discussions) — questions, ideas, show & tell
- [Contributing guide](CONTRIBUTING.md) — how to get involved
- [dlt community](https://dlthub.com/community) — dlt Slack / Discord

## Further Reading

- **[Getting Started](docs/getting-started.md)** — Full walkthrough: install, init, first pipeline
- **[Architecture](docs/architecture.md)** — Three-layer design, plugin system, execution flow
- **[CLI Reference](docs/reference/CLI.md)** — All commands, flags, and the programmatic API
- **[Configuration Guide](docs/reference/CONFIGURATION.md)** — Hierarchical config, all options reference
- **[Profiles Guide](docs/reference/PROFILES.md)** — Multi-environment setup, service account impersonation
- **[Deployment Guide](docs/reference/DEPLOYMENT.md)** — Orchestration, Cloud Run, worker setup
- **[Performance Tuning](docs/reference/PERFORMANCE.md)** — Parallel execution, worker tuning
- **[Plugin Development Guide](docs/plugin-development-guide.md)** — Custom sources, destinations, hooks

## Project Structure

```
dlt-saga/
├── dlt_saga/              # Main package
│   ├── cli.py            #   CLI entry point (saga command)
│   ├── pipelines/        #   Built-in source implementations
│   │   ├── api/          #     Generic REST API pipeline
│   │   ├── database/     #     Database source (ConnectorX)
│   │   ├── filesystem/   #     Filesystem / GCS source
│   │   ├── google_sheets/#     Google Sheets source
│   │   └── sharepoint/   #     SharePoint source
│   ├── historize/        #   SCD2 historization engine
│   ├── destinations/     #   Destination implementations
│   │   ├── bigquery/     #     BigQuery
│   │   └── duckdb/       #     DuckDB (local development)
│   ├── pipeline_config/  #   Config discovery and parsing
│   ├── schemas/          #   Bundled static schemas (dlt_common.json)
│   └── utility/          #   Shared utilities (CLI, naming, orchestration)
├── example/              # Minimal runnable consumer project (DuckDB)
├── .dlt/                 # dlt runtime config overrides
└── docs/                 # Reference documentation
```
