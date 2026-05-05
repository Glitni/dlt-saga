"""saga init — scaffold a new dlt-saga consumer project."""

from __future__ import annotations

from pathlib import Path
from typing import List, Optional

import typer

# Files/dirs that must all be present for a project to be considered initialised.
_REQUIRED_MARKERS = ("profiles.yml", "saga_project.yml", "configs")

_VALID_DESTINATION_TYPES = ("bigquery", "databricks", "duckdb")


# ---------------------------------------------------------------------------
# Detection
# ---------------------------------------------------------------------------


def _check_initialized(target: Path) -> tuple[set[str], set[str]]:
    """Return (present, missing) sets of required project markers."""
    present: set[str] = set()
    missing: set[str] = set()
    for marker in _REQUIRED_MARKERS:
        if (target / marker).exists():
            present.add(marker)
        else:
            missing.add(marker)
    return present, missing


# ---------------------------------------------------------------------------
# File templates
# ---------------------------------------------------------------------------


def _profiles_yml_bigquery(gcp_project: str, location: str, schema_name: str) -> str:
    return f"""\
default:
  target: dev
  outputs:
    dev:
      type: bigquery
      auth_provider: gcp
      database: {gcp_project}
      schema: "{{{{ env_var('SAGA_SCHEMA_NAME', '{schema_name}') }}}}"
      location: {location}
      environment: dev

# Add a prod target here when you're ready to deploy.
# See: https://github.com/Glitni/dlt-saga/wiki/Profiles
"""


def _profiles_yml_databricks(
    server_hostname: str,
    http_path: str,
    catalog: str,
    auth_mode: str,
    schema_name: str,
) -> str:
    if auth_mode == "pat":
        auth_block = """\
      auth_mode: pat
      # Store your PAT as a secret URI rather than plain text:
      # access_token: "azurekeyvault::https://my-vault.vault.azure.net::databricks-pat"
      access_token: "<your-personal-access-token>"
"""
    elif auth_mode == "m2m":
        auth_block = """\
      auth_mode: m2m
      client_id: "{{ env_var('DATABRICKS_CLIENT_ID') }}"
      # Store the secret in Azure Key Vault:
      # client_secret: "azurekeyvault::https://my-vault.vault.azure.net::databricks-sp-secret"
      client_secret: "{{ env_var('DATABRICKS_CLIENT_SECRET') }}"
"""
    else:  # u2m
        auth_block = """\
      auth_mode: u2m
      # Browser login — token cached in ~/.databricks/token-cache.json
"""
    return f"""\
default:
  target: dev
  outputs:
    dev:
      type: databricks
      auth_provider: databricks
{auth_block}      # Workspace URL — find it in your Databricks workspace (Settings → Developer)
      server_hostname: "{server_hostname}"
      # SQL Warehouse HTTP path — find it in the warehouse's Connection Details tab
      http_path: "{http_path}"
      # Unity Catalog name
      catalog: "{catalog}"
      schema: "{{{{ env_var('SAGA_SCHEMA_NAME', '{schema_name}') }}}}"
      environment: dev
      # Staging volume for loading data (required for local execution).
      # Use a shared Unity Catalog volume: "<catalog>.<schema>.<volume_name>"
      # staging_volume_name: "my_catalog.my_schema.ingest_volume"

# Add a prod target here when you're ready to deploy.
# See: https://github.com/Glitni/dlt-saga/wiki/Profiles
"""


def _profiles_yml_duckdb(schema_name: str) -> str:
    return f"""\
default:
  target: dev
  outputs:
    dev:
      type: duckdb
      database_path: local.duckdb
      schema: {schema_name}
      environment: dev
"""


def _saga_project_yml(
    gcp_project: Optional[str], destination_type: str = "bigquery"
) -> str:
    if destination_type == "bigquery":
        project_placeholder = gcp_project or "<your-gcp-project>"
        providers_block = f"""\
# Uncomment to configure a secrets provider (e.g. Google Secret Manager):
# providers:
#   google_secrets:
#     project_id: {project_placeholder}
#     sheets_secret_name: my-sheets-credentials
"""
        orchestration_block = """\
# Uncomment to configure distributed orchestration (Cloud Run):
# orchestration:
#   provider: cloud_run
#   job_name: dlt-saga-ingest-daily
#   schema: dlt_orchestration
"""
    elif destination_type == "databricks":
        providers_block = """\
# Uncomment to configure Azure Key Vault as your secrets provider:
# providers:
#   azurekeyvault:
#     vault_url: https://my-vault.vault.azure.net
"""
        orchestration_block = ""
    else:
        providers_block = ""
        orchestration_block = ""

    # Pipeline adapter mappings — tells dlt-saga which implementation to use
    # for each config directory under configs/.
    if destination_type == "duckdb":
        pipelines_block = """\
# Map config directories to pipeline implementations.
# Each key matches a directory under configs/.
pipelines:
  filesystem:
    adapter: dlt_saga.filesystem
  # api:
  #   adapter: dlt_saga.api
  # database:
  #   adapter: dlt_saga.database
  # google_sheets:
  #   adapter: dlt_saga.google_sheets
"""
    else:
        pipelines_block = """\
# Map config directories to pipeline implementations.
# Each key matches a directory under configs/.
# pipelines:
#   filesystem:
#     adapter: dlt_saga.filesystem
#   api:
#     adapter: dlt_saga.api
#   database:
#     adapter: dlt_saga.database
#   google_sheets:
#     adapter: dlt_saga.google_sheets
"""

    return f"""\
profile: default

config_source:
  type: file
  path: configs

{providers_block}{orchestration_block}{pipelines_block}"""


def _dlt_config_toml() -> str:
    return """\
# dlt configuration overrides
#
# Performance defaults (normalize workers, load workers, etc.) are shipped by
# the dlt-saga package in dlt_saga/defaults.py. You only need entries here to
# override those defaults for local development.
#
# Priority order (highest wins):
#   1. Environment variables (e.g. NORMALIZE__WORKERS=8)
#   2. This file (.dlt/config.toml)
#   3. Package defaults (dlt_saga/defaults.py)
#
# Provider secrets (google_secrets) are configured in saga_project.yml.
# Destination settings (project, location) come from profiles.yml.
"""


def _sample_pipeline_config_runnable() -> str:
    """A working pipeline config that reads a local CSV file."""
    return """\
tags: [daily]
write_disposition: append

filesystem_type: file
bucket_name: data
file_glob: "*.csv"
file_type: csv
"""


def _sample_pipeline_config_template() -> str:
    """A commented-out template showing different source types."""
    return """\
# Pipeline configuration template.
# Copy this file, fill in your source details, then run `saga list`.
#
# tags: [daily]
# write_disposition: append
#
# --- Google Sheets example ---
# adapter: dlt_saga.google_sheets
# spreadsheet_id: "1BxiMVs0XRA5nFMdKvBdBZjgmUUqptlbs74OgVE2upms"
# sheet_name: "Sheet1"
#
# --- Filesystem (GCS) example ---
# adapter: dlt_saga.filesystem
# filesystem_type: gs
# bucket_name: my-bucket
# file_glob: "data/*.parquet"
# file_type: parquet
#
# --- Database example ---
# adapter: dlt_saga.database
# database_type: postgres
# host: db.example.com
# source_table: my_table
"""


def _sample_csv_data() -> str:
    """Sample CSV data for the runnable example pipeline."""
    return """\
id,name,value,updated_at
1,alpha,10,2026-01-01
2,beta,20,2026-01-15
3,gamma,30,2026-02-01
"""


def _gitignore() -> str:
    return """\
# Python
__pycache__/
*.py[cod]
*.egg-info/
dist/
build/

# Virtual environments
.venv/
venv/

# dlt — never commit secrets
.dlt/secrets.toml

# Generated files
schemas/

# Local DuckDB databases
*.duckdb

# OS
.DS_Store
Thumbs.db
"""


def _packages_yml() -> str:
    return """\
# Register local pipeline implementations here.
# Each entry maps a namespace to a Python package directory.
# Example:
# packages:
#   - namespace: local
#     path: ./my_pipelines   # directory containing pipeline.py modules
packages: []
"""


def _readme_md(destination_type: str) -> str:
    if destination_type == "bigquery":
        auth_line = "- Authenticate: `gcloud auth application-default login`\n"
    elif destination_type == "databricks":
        auth_line = (
            "- Authenticate: run `saga ingest` and a browser window will open (u2m).\n"
        )
    else:
        auth_line = ""
    return f"""\
# Data Pipelines

Data ingestion pipelines.

## Quick start

```bash
# List all configured pipelines
saga list

# Run all ingest-enabled pipelines
saga ingest

# Run pipelines matching a selector
saga ingest --select "tag:daily"

# Run ingest then historize
saga run --select "tag:daily"
```

## Setup

1. Edit `profiles.yml` with your destination details.
{auth_line}2. Add pipeline configs under `configs/`.
3. Run `saga list` to verify discovery.

## Configuration

| File | Purpose |
|------|---------|
| `profiles.yml` | Destination targets (dev, prod) |
| `saga_project.yml` | Project settings, providers, orchestration |
| `configs/` | Pipeline configurations (YAML, one per pipeline) |
| `.dlt/config.toml` | dlt performance tuning overrides |

## Selectors

```bash
saga ingest --select "tag:daily"              # by tag
saga ingest --select "group:google_sheets"    # by group
saga ingest --select "*balance*"              # glob pattern
saga ingest --select "tag:daily,group:api"    # AND (intersection)
saga ingest --select "tag:daily tag:weekly"   # OR (union)
```
"""


# ---------------------------------------------------------------------------
# File writing
# ---------------------------------------------------------------------------


def _write_if_absent(
    path: Path, content: str, created: List[str], skipped: List[str]
) -> None:
    """Write *content* to *path* only if it does not already exist."""
    if path.exists():
        skipped.append(str(path))
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")
    created.append(str(path))


def _ensure_dir(directory: Path, created: List[str], skipped: List[str]) -> None:
    """Create *directory* if absent; track result in created/skipped."""
    if directory.exists():
        skipped.append(str(directory) + "/")
        return
    directory.mkdir(parents=True, exist_ok=True)
    created.append(str(directory) + "/")


# ---------------------------------------------------------------------------
# Interactive helpers (each maps to one natural step)
# ---------------------------------------------------------------------------


def _resolve_target(no_input: bool) -> Path:
    """Return the target directory (always the current working directory).

    Run ``saga init`` from the directory you want to initialise.
    """
    return Path.cwd()


def _prompt_config(no_input: bool) -> dict:
    """Prompt for destination settings; return a dict with all resolved values."""
    if no_input:
        return {
            "destination_type": "duckdb",
            "gcp_project": None,
            "location": "EU",
            "schema_name": "dlt_dev",
            "server_hostname": "",
            "http_path": "",
            "catalog": "",
            "auth_mode": "u2m",
        }

    dest_list = "/".join(_VALID_DESTINATION_TYPES)
    destination_type = ""
    while destination_type not in _VALID_DESTINATION_TYPES:
        destination_type = (
            typer.prompt(f"Destination type [{dest_list}]", default="bigquery")
            .strip()
            .lower()
        )
        if destination_type not in _VALID_DESTINATION_TYPES:
            typer.echo(
                f"  Invalid choice '{destination_type}'. "
                f"Please enter one of: {', '.join(_VALID_DESTINATION_TYPES)}"
            )

    gcp_project: Optional[str] = None
    location = "EU"
    server_hostname = ""
    http_path = ""
    catalog = ""
    auth_mode = "u2m"

    if destination_type == "bigquery":
        gcp_project = typer.prompt("GCP project ID").strip()
        location = typer.prompt("BigQuery location", default="EU").strip()

    elif destination_type == "databricks":
        typer.echo(
            "\n  Find server_hostname in your Databricks workspace: "
            "Settings → Developer → SQL Warehouse → Connection Details"
        )
        server_hostname = typer.prompt(
            "Workspace hostname (e.g. adb-1234.azuredatabricks.net)"
        ).strip()
        http_path = typer.prompt(
            "SQL Warehouse HTTP path (e.g. /sql/1.0/warehouses/abc123)"
        ).strip()
        catalog = typer.prompt("Unity Catalog name (e.g. main)").strip()

        auth_choices = ("u2m", "m2m", "pat")
        auth_mode = ""
        while auth_mode not in auth_choices:
            auth_mode = (
                typer.prompt(
                    "Auth mode [u2m (browser) / m2m (service principal) / pat (token)]",
                    default="u2m",
                )
                .strip()
                .lower()
            )
            if auth_mode not in auth_choices:
                typer.echo(
                    f"  Invalid choice '{auth_mode}'. Please enter 'u2m', 'm2m', or 'pat'."
                )

    schema_name = typer.prompt("Dev schema name", default="dlt_dev").strip()
    return {
        "destination_type": destination_type,
        "gcp_project": gcp_project,
        "location": location,
        "schema_name": schema_name,
        "server_hostname": server_hostname,
        "http_path": http_path,
        "catalog": catalog,
        "auth_mode": auth_mode,
    }


def _scaffold(
    target: Path,
    config: dict,
    is_fresh: bool,
) -> tuple[List[str], List[str]]:
    """Write project files; return (created, skipped) path lists."""
    created: List[str] = []
    skipped: List[str] = []

    destination_type = config["destination_type"]
    gcp_project = config.get("gcp_project")
    schema_name = config["schema_name"]

    if destination_type == "bigquery":
        profiles_content = _profiles_yml_bigquery(
            gcp_project, config.get("location", "EU"), schema_name
        )
    elif destination_type == "databricks":
        profiles_content = _profiles_yml_databricks(
            server_hostname=config.get("server_hostname", "<workspace-hostname>"),
            http_path=config.get("http_path", "/sql/1.0/warehouses/<id>"),
            catalog=config.get("catalog", "<catalog>"),
            auth_mode=config.get("auth_mode", "u2m"),
            schema_name=schema_name,
        )
    else:
        profiles_content = _profiles_yml_duckdb(schema_name)

    _write_if_absent(target / "profiles.yml", profiles_content, created, skipped)
    _write_if_absent(
        target / "saga_project.yml",
        _saga_project_yml(gcp_project, destination_type),
        created,
        skipped,
    )
    _ensure_dir(target / "configs", created, skipped)
    _write_if_absent(target / "packages.yml", _packages_yml(), created, skipped)
    _write_if_absent(target / ".gitignore", _gitignore(), created, skipped)

    # Starter files — only on a fresh init (no core markers were present).
    if is_fresh:
        _write_if_absent(
            target / ".dlt" / "config.toml", _dlt_config_toml(), created, skipped
        )

        if destination_type == "duckdb":
            # DuckDB: create a runnable pipeline that reads a local CSV file.
            # Users can `saga list` → `saga ingest` immediately after init.
            _write_if_absent(
                target / "configs" / "filesystem" / "sample.yml",
                _sample_pipeline_config_runnable(),
                created,
                skipped,
            )
            _write_if_absent(
                target / "data" / "sample.csv",
                _sample_csv_data(),
                created,
                skipped,
            )
        else:
            # Cloud destinations: create a commented-out template with a
            # non-.yml extension so it is not picked up by config discovery.
            _write_if_absent(
                target / "configs" / "TEMPLATE.yml.example",
                _sample_pipeline_config_template(),
                created,
                skipped,
            )

        _write_if_absent(
            target / "README.md", _readme_md(destination_type), created, skipped
        )

    # Generate JSON schemas for IDE autocomplete (always, even on partial init)
    schemas_dir = target / "schemas"
    if not schemas_dir.exists():
        try:
            from dlt_saga.utility.generate_schemas import generate_schemas

            generate_schemas(schemas_dir)
            created.append(str(schemas_dir) + "/")
        except Exception as exc:
            import logging as _logging

            _logging.getLogger(__name__).warning(
                "Schema generation failed: %s. IDE autocomplete will be unavailable. "
                "Run 'saga generate-schemas' manually to retry.",
                exc,
            )

    return created, skipped


# ---------------------------------------------------------------------------
# ---------------------------------------------------------------------------
# Next-steps messaging
# ---------------------------------------------------------------------------


def _print_next_steps(destination_type: str, config: dict) -> None:
    """Print post-scaffold next-steps guidance."""
    typer.echo("")
    typer.echo("Done! Next steps:")

    if destination_type == "duckdb":
        typer.echo("  1. Run `saga list` to see the sample pipeline.")
        typer.echo("  2. Run `saga ingest` to load the sample CSV into DuckDB.")
        typer.echo("  3. Add your own pipeline configs under configs/.")
    elif destination_type == "bigquery":
        typer.echo("  1. Authenticate: gcloud auth application-default login")
        typer.echo("  2. Edit profiles.yml with your GCP project details.")
        typer.echo("  3. Add pipeline configs under configs/.")
        typer.echo("  4. Run `saga doctor` to verify your setup.")
        typer.echo("  5. Run `saga list` to see discovered pipelines.")
    elif destination_type == "databricks":
        auth_mode = config.get("auth_mode", "u2m")
        typer.echo("  1. Edit profiles.yml with your workspace details.")
        if auth_mode == "u2m":
            typer.echo("  2. Run `saga doctor` — a browser window will open for login.")
        elif auth_mode == "m2m":
            typer.echo(
                "  2. Set DATABRICKS_CLIENT_ID and DATABRICKS_CLIENT_SECRET env vars."
            )
        else:
            typer.echo("  2. Set your access_token in profiles.yml.")
        typer.echo("  3. Add pipeline configs under configs/.")
        typer.echo("  4. Run `saga list` to see discovered pipelines.")

    typer.echo("")
    typer.echo(
        "Optional: run `saga ai-setup` to generate an AI assistant context file."
    )


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------


def run_init(no_input: bool = False) -> None:
    """Interactive scaffold for a new dlt-saga consumer project."""
    target = _resolve_target(no_input)

    present, missing = _check_initialized(target)
    is_fresh = not present

    if not missing:
        typer.echo(
            f"'{target}' is already fully initialised "
            f"({', '.join(sorted(present))} found). Nothing to do."
        )
        raise typer.Exit()

    if present:
        typer.echo(
            f"Partial initialisation detected in '{target}'.\n"
            f"  Already present : {', '.join(sorted(present))}\n"
            f"  Will create     : {', '.join(sorted(missing))}\n"
        )

    config = _prompt_config(no_input)
    destination_type = config["destination_type"]
    created, skipped = _scaffold(target, config, is_fresh)

    typer.echo("")
    if created:
        typer.echo("Created:")
        for path in created:
            typer.echo(f"  {path}")
    if skipped:
        typer.echo("Skipped (already exist):")
        for path in skipped:
            typer.echo(f"  {path}")

    typer.echo("")
    _print_next_steps(destination_type, config)
