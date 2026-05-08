# dlt-saga

Config-driven data ingestion and historization framework, built on [dlt](https://dlthub.com/).

[![PyPI version](https://img.shields.io/pypi/v/dlt-saga.svg)](https://pypi.org/project/dlt-saga/)
[![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](https://github.com/Glitni/dlt-saga/blob/main/LICENSE)
[![CI](https://github.com/Glitni/dlt-saga/actions/workflows/ci.yml/badge.svg)](https://github.com/Glitni/dlt-saga/actions/workflows/ci.yml)

```bash
pip install dlt-saga[bigquery]          # BigQuery
pip install dlt-saga[databricks,azure]  # Databricks on Azure
pip install dlt-saga                    # DuckDB only (no cloud dependencies)
```

## Contents

- [Getting Started](Getting-Started) — install, scaffold, first pipeline
- [Architecture](Architecture) — three-layer design, plugin system, execution flow
- [Pipeline Types](Pipeline-Types) — API, Database, Filesystem, Google Sheets, SharePoint config reference
- [Configuration](Configuration) — all config fields, hierarchical defaults, access control
- [Profiles](Profiles) — multi-environment setup, service account impersonation
- [Historize (SCD2)](Historize) — snapshot tables → slowly changing dimensions
- [CLI Reference](CLI-Reference) — all commands, flags, and selectors
- [Deployment](Deployment) — Cloud Run, orchestration, worker setup
- [Orchestration Recipes](Orchestration-Recipes) — Dagster, Airflow, and Prefect integration patterns
- [Performance](Performance) — parallel execution, worker tuning, backfill
- [Plugin Development](Plugin-Development) — custom pipeline sources, destinations, and hooks
