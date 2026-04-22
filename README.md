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

## Installation

```bash
pip install dlt-saga[bigquery]          # BigQuery
pip install dlt-saga[databricks,azure]  # Databricks on Azure
pip install dlt-saga                    # DuckDB only (no cloud dependencies)
```

## Quick Start

```bash
# 1. Create and scaffold a project
mkdir my-pipelines && cd my-pipelines
saga init                               # prompts for destination and credentials

# 2. Authenticate to your destination (skip for DuckDB)
#    See: wiki/Getting-Started.md

# 3. List available pipelines
saga list

# 4. Run a pipeline
saga ingest --select "example__sample"
```

> See the **[Getting Started guide](wiki/Getting-Started.md)** for a full walkthrough, or browse [`example/`](example/) for a minimal runnable setup.

> Local execution is the default. Use `--orchestrate` to fan out to parallel workers (requires `orchestration:` configured in `saga_project.yml`).

## CLI Commands

All commands are subcommands under the `saga` entry point and share common options:
`--select`, `--verbose`, `--profile`, `--target`.

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

Supported source types out of the box: **API**, **Database** (PostgreSQL, MySQL, SQL Server, and more via ConnectorX), **Filesystem** (GCS, SFTP, local), **Google Sheets**, and **SharePoint**.

See the **[Pipeline Types guide](wiki/Pipeline-Types.md)** for config examples for each source type, and the **[Configuration reference](wiki/Configuration.md)** for all available fields.

## Write Dispositions and Historize

The `write_disposition` field controls what operations are enabled for a pipeline:

| Value | Ingest | Historize | Use Case |
|-------|--------|-----------|----------|
| `append` | Yes | No | Raw event/log data |
| `merge` | Yes | No | Upsert on primary key |
| `replace` | Yes | No | Full refresh each run |
| `append+historize` | Yes | Yes | Snapshot → SCD2 |
| `historize` | No | Yes | External data → SCD2 |

Historize transforms raw snapshot data into [SCD2](https://en.wikipedia.org/wiki/Slowly_changing_dimension#Type_2:_add_new_row) tables with `_dlt_valid_from`, `_dlt_valid_to`, and `_dlt_is_deleted` columns. See the **[Historize guide](wiki/Historize.md)** for the full reference.

## Community

- [GitHub Issues](https://github.com/Glitni/dlt-saga/issues) — bug reports and feature requests
- [GitHub Discussions](https://github.com/Glitni/dlt-saga/discussions) — questions, ideas, show & tell
- [Contributing guide](CONTRIBUTING.md) — how to get involved
- [dlt community](https://dlthub.com/community) — dlt Slack / Discord

## Further Reading

- **[Getting Started](wiki/Getting-Started.md)** — Full walkthrough: install, init, first pipeline
- **[Architecture](wiki/Architecture.md)** — Three-layer design, plugin system, execution flow
- **[Pipeline Types](wiki/Pipeline-Types.md)** — Config reference for API, Database, Filesystem, Sheets, SharePoint
- **[Configuration](wiki/Configuration.md)** — Hierarchical config, all options reference
- **[Profiles](wiki/Profiles.md)** — Multi-environment setup, service account impersonation
- **[Historize (SCD2)](wiki/Historize.md)** — Snapshot tables → slowly changing dimensions
- **[CLI Reference](wiki/CLI-Reference.md)** — All commands, flags, and the programmatic API
- **[Deployment](wiki/Deployment.md)** — Orchestration, Cloud Run, worker setup
- **[Performance](wiki/Performance.md)** — Parallel execution, worker tuning, backfill
- **[Plugin Development](wiki/Plugin-Development.md)** — Custom sources, destinations, hooks

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
├── wiki/                 # Documentation (synced to GitHub wiki)
└── .dlt/                 # dlt runtime config overrides
```
