# Plugin Development Guide

This document describes the stable public API for extending `dlt-saga` with
custom pipeline implementations, destinations, and access managers.

The current plugin API version is **1** (`dlt_saga.PLUGIN_API_VERSION`).

---

## Pipeline Plugins

A pipeline plugin extracts data from a source and yields dlt resources.
Every pipeline implementation extends `BasePipeline`.

### Start here — scaffold, then read a built-in

Don't start from a blank file. Two things give you a correct starting point:

1. **Scaffold it:** `saga new adapter <name>` generates a convention-following
   `config.py` + `client.py` + `pipeline.py` (plus a starter config and the
   `packages.yml` entry). `--kind api` instead emits a `BaseApiPipeline` subclass.
   Then `saga new config <name> --adapter <adapter>` scaffolds a pipeline config
   pre-filled with the adapter's fields. Editing generated code is far less
   error-prone than reconstructing the conventions by hand.
2. **Read a built-in as a worked example** — these are the reference
   implementations:
   - `dlt_saga/pipelines/filesystem/` — the `config.py` / `client.py` /
     `pipeline.py` split, end to end.
   - `dlt_saga/pipelines/database/` — incremental loading from the warehouse
     high-water mark (the idempotent pattern below).
   - `dlt_saga/pipelines/api/` — the `BaseApiPipeline` path for REST sources.

When you're done, run `saga lint` (see [below](#linting-your-adapter)) — it flags
the common mistakes before review does.

### Reuse before reinventing

Before writing a custom adapter, check whether a built-in already fits — prefer
using it (config only) or *inheriting* from it over hand-rolling a new source:

| Source | Built-in adapter |
|--------|------------------|
| REST APIs | `dlt_saga.api` (or scaffold `--kind api`) |
| SQL databases | `dlt_saga.database` |
| GCS / S3 / Azure / local / **SFTP** files | `dlt_saga.filesystem` |
| Large file batches (bulk load) | `dlt_saga.native_load` |

To extend one, inherit its config and pipeline (e.g.
`class MyConfig(FilesystemConfig)`, `class MyPipeline(FilesystemPipeline)`) and
override only what differs. Reach for a from-scratch `BasePipeline` only for a
source none of the built-ins cover.

### Anatomy: `config.py` / `client.py` / `pipeline.py`

A custom source lives in its own package directory with three files (the layout
the scaffolder emits and every built-in follows):

| File | Owns |
|------|------|
| `config.py` | A dataclass extending `BaseConfig`. Declares the source's fields, types, defaults, and validation (`__post_init__`). |
| `client.py` | Connection + I/O + credential resolution. All the "talk to the source" logic, so it can be tested in isolation. |
| `pipeline.py` | A `BasePipeline` subclass — orchestration only: resolve the incremental cursor, call the client, return resources. |

REST API adapters can skip `client.py` — `BaseApiPipeline` owns the HTTP layer.

### Minimum Viable Pipeline

```python
from typing import Any, Dict, List, Tuple
from dlt_saga.pipelines.base_pipeline import BasePipeline


class ApiPipeline(BasePipeline):
    """Custom API pipeline implementation."""

    def __init__(self, config: Dict[str, Any], log_prefix: str = None):
        super().__init__(config, log_prefix)
        # Parse source-specific config from self.config_dict
        self.api_url = config.get("api_url")

    def extract_data(self) -> List[Tuple[Any, str]]:
        """Extract data from source. Must return list of (resource, description) tuples."""
        import dlt
        import requests

        response = requests.get(self.api_url)
        data = response.json()

        resource = dlt.resource(
            data,
            name=self.base_table_name,
            write_disposition=self.target_writer.config.write_disposition,
            primary_key=self.target_writer.config.primary_key,
        )

        return [(resource, f"Data from {self.api_url}")]
```

### Public API — BasePipeline

These are the attributes and methods available to pipeline implementations.

#### Attributes (set by `__init__`)

| Attribute | Type | Description |
|-----------|------|-------------|
| `self.config_dict` | `dict` | Raw pipeline configuration from YAML |
| `self.base_table_name` | `str` | Table name from config path (e.g., `livewrapped__stats`) |
| `self.table_name` | `str` | Environment-aware table name |
| `self.pipeline_name` | `str` | Full pipeline name (e.g., `api__livewrapped__stats`) |
| `self.pipeline` | `dlt.Pipeline` | The dlt pipeline instance |
| `self.destination` | `Destination` | The destination implementation |
| `self.target_writer` | `TargetWriter` | Handles write disposition, merge keys, dedup |
| `self.logger` | `Logger` | Logger with optional prefix |
| `self.log_prefix` | `str \| None` | Prefix string for log messages |

#### Methods to Override

| Method | Required | Description |
|--------|----------|-------------|
| `extract_data()` | **Yes** | Return `List[Tuple[resource, description]]` — the resources to load |

#### Methods Available to Subclasses

| Method | Description |
|--------|-------------|
| `run()` | Main execution method — calls `extract_data()`, processes resources, manages timing and access. Rarely overridden. |
| `filter_excluded_columns(record, exclude_columns)` | Remove columns from a dict record. Useful in `extract_data()`. |

#### Internal Methods (do not override)

Methods prefixed with `_` are internal implementation details. They may
change between versions. Key ones:

- `_create_pipeline()` — creates the dlt pipeline
- `_inject_ingested_at(resource)` — adds `_dlt_ingested_at` for append pipelines
- `_apply_row_limit(resource)` — applies dev row limit
- `_process_resource_data(resource, description)` — processes a single resource through the load pipeline
- `_build_destination_hints(description)` — builds partitioning/clustering hints
- `_manage_table_access(table_ids)` — applies access controls

### Config Factory Pattern

If your pipeline type has a specific configuration dataclass, use the
factory method pattern inherited from `BaseApiPipeline`:

```python
from dataclasses import dataclass, field, fields
from dlt_saga.pipelines.api.config import ApiConfig


@dataclass
class MyApiConfig(ApiConfig):
    """Configuration for my custom API."""
    api_url: str = field(default="", metadata={"description": "API base URL", "required": True})
    page_size: int = field(default=100)

    def __post_init__(self):
        super().__post_init__()
        if not self.api_url.startswith("https://"):
            raise ValueError("api_url must be set and use HTTPS")
```

### Pipeline YAML Config

```yaml
# configs/api/myservice/data.yml
tags: [daily]
write_disposition: append
api_url: "https://api.example.com/data"
page_size: 50
auth_type: bearer
auth_token: "env_secret::API_TOKEN"
```

### Idempotent incremental loading

Resume from what's actually in the warehouse, not from the calendar. Look up the
maximum value already loaded for the cursor column and fall back to
`initial_value` only on the first run:

```python
def _incremental_start(self):
    if not self.source_config.incremental:
        return None
    table_id = f"{self.destination_database}.{self.pipeline.dataset_name}.{self.table_name}"
    return (
        self.destination.get_max_column_value(table_id, self.source_config.incremental_column)
        or self.source_config.initial_value
    )
```

This makes the pipeline **self-healing**: a missed or failed run is caught up on
the next run, with no gaps and no duplicates. Never hardcode "load yesterday"
(`datetime.now() - timedelta(days=1)`) — that silently drops data whenever a run
is skipped. See `dlt_saga/pipelines/database/` for the full pattern; `saga lint`
flags the hardcoded-window anti-pattern.

### Secrets and credentials

Any string config field accepts a plain value or a secret URI
(`googlesecretmanager::…` / `azurekeyvault::…` / `env_secret::VAR`) interchangeably;
non-secret environment variables can also be injected in any field with `{{ env_var('VAR') }}`
templating. So **name credential fields by what they hold, never by
secrecy**. `api_token` and `password` are good; `auth_secret` / `*_plaintext`
are not (the name guarantees nothing, and `saga lint` flags it).

Type credential fields as `SecretStr`, coerce them in `__post_init__`, and
resolve them lazily at use:

```python
from dlt_saga.utility.secrets.secret_str import SecretStr, coerce_secret

@dataclass
class MyConfig(BaseConfig):
    api_token: Optional[SecretStr] = field(default=None, metadata={"description": "..."})

    def __post_init__(self):
        super().__post_init__()
        self.api_token = coerce_secret(self.api_token)
```

```python
# in client.py, at request time:
from dlt_saga.utility.secrets import resolve_secret
token = resolve_secret(self.config.api_token)   # handles plain values and secret URIs
```

### Standard config vocabulary

Reuse the inherited field names instead of inventing new ones — configs stay
consistent across adapters, and `saga lint` flags near-misses (`primary_keys` →
`primary_key`, `incremental_col` → `incremental_column`). The standard fields:

| Field | Purpose |
|-------|---------|
| `incremental`, `incremental_column`, `initial_value` | Incremental loading (from `BaseConfig`) |
| `tags`, `enabled`, `task_group` | Selection / scheduling / execution (from `BaseConfig`) |
| `write_disposition` | `append` / `merge` / `replace` / `…+historize` |
| `primary_key`, `merge_key`, `merge_strategy` | Merge / SCD2 behaviour |
| `partition_column`, `cluster_columns` | Destination hints |
| `historize` | SCD2 historization block |

Add source-specific fields freely (`base_url`, `spreadsheet_id`, …) — just don't
re-spell a standard concept under a new name.

### Validate config in `__post_init__`

Keep source-identifying values (URLs, table names) with an empty default and
validate them in `__post_init__`, so they must be set in the YAML rather than
buried as class defaults. Raise `ValueError` for configuration mistakes (the CLI
renders these as clean, actionable errors — no traceback):

```python
def __post_init__(self):
    super().__post_init__()              # always call super first
    self.api_token = coerce_secret(self.api_token)
    if not self.base_url:
        raise ValueError("base_url is required")
```

### Registering via Entry Points

The recommended way to ship a pipeline plugin as a Python package is via the
`dlt_saga.pipelines` entry point group. Once registered, users can reference
your namespace in `adapter:` without any `packages.yml` entry — just
`pip install your-package`.

**EP name** = namespace prefix used in `adapter:` values  
**EP value** = base Python module path (no `:attribute`). The module is never
imported at startup — only when a pipeline of that namespace is first resolved.

In your plugin package's `pyproject.toml`:

```toml
[project.entry-points."dlt_saga.pipelines"]
acme = "acme_saga_pipelines.pipelines"
```

After `pip install acme-saga-pipelines`, a config with:

```yaml
adapter: acme.api.orders
```

resolves to `acme_saga_pipelines.pipelines.api.orders.pipeline` and
auto-discovers the `BasePipeline` subclass inside it.

Use `saga info` to confirm the namespace is registered, and `saga doctor` to
verify the base module is importable.

### Registering a local package (`packages.yml`)

For adapters that live in your own project (not a separately-installed package),
register the directory in `packages.yml` at the project root:

```yaml
packages:
  - namespace: local        # prefix used in adapter: values
    path: ./pipelines        # directory holding <group>/<name>/pipeline.py modules
```

A config with `adapter: local.api.my_source` then resolves to
`pipelines/api/my_source/pipeline.py`. `saga new adapter` creates this entry for
you (and reuses an existing namespace if the directory is already registered).

### Linting your adapter

`saga lint` statically checks your adapters for the conventions on this page —
secret-by-name fields, config names that diverge from the standard vocabulary,
and non-idempotent "load yesterday" date math — and exits non-zero on any
finding, so it can gate CI:

```bash
saga lint            # your project's own adapters
saga lint --all      # also built-in and installed third-party adapters
```

By default it lints only adapters whose source lives inside your project (the
ones you can actually change); built-ins and pip/entry-point installs are
skipped unless you pass `--all`.

### Anti-patterns to avoid

| Don't | Do |
|-------|----|
| Bury source values (URLs, table names) as class defaults, leaving the YAML empty | Empty default + validate in `__post_init__`; set real values in the config |
| Invent a name for a standard concept (`overlap_days`, `incremental_col`) | Reuse the standard vocabulary (`incremental_column`, `primary_key`, …) |
| Name a field `*_secret` / `*_plaintext` | Name by content (`api_token`, `password`); any field accepts a secret URI |
| Hardcode "load yesterday" / `datetime.now() - timedelta(...)` | Resume from `get_max_column_value(...) or initial_value` |
| Read `os.environ` directly or hardcode credentials | `SecretStr` + `coerce_secret()` in config, `resolve_secret()` at use |
| `print()` for output | `self.logger` / `logging.getLogger(__name__)` |
| Swallow config errors in a broad `except` | Raise `ValueError` with an actionable message |

### Contribution checklist

Before opening a PR that adds an adapter:

- [ ] Scaffolded with `saga new adapter` (or follows the same `config` / `client` / `pipeline` layout)
- [ ] Reuses a built-in / inherits where one fits, rather than reimplementing it
- [ ] Credentials are `SecretStr`, resolved via `resolve_secret`, named by content
- [ ] Incremental loading resumes from the warehouse high-water mark (no hardcoded window)
- [ ] Config fields reuse the standard vocabulary; required ones validated in `__post_init__`
- [ ] `saga lint` passes; tests added (see [Testing Your Plugin](#testing-your-plugin))

---

## Multi-destination Configuration

Pipeline plugin code is destination-agnostic — the same `BasePipeline` subclass works with BigQuery, Databricks, and DuckDB without modification. The destination is selected via `profiles.yml`; the pipeline config can include optional partitioning and clustering hints that each destination translates into its own DDL.

### BigQuery

```yaml
# profiles.yml — dev target
default:
  outputs:
    dev:
      destination_type: bigquery
      project: your-dev-project
      location: EU
      dataset: dlt_dev
```

```yaml
# configs/api/myservice/data.yml
adapter: acme.api.myservice
tags: [daily]
write_disposition: append
api_key: "googlesecretmanager::your-project::myservice-api-key"

partition_column: _dlt_ingested_at
cluster_columns: [account_id]
```

### Databricks (Unity Catalog)

```yaml
# profiles.yml — databricks target
default:
  outputs:
    databricks_dev:
      destination_type: databricks
      server_hostname: adb-1234567890.12.azuredatabricks.net
      http_path: /sql/1.0/warehouses/abc123
      catalog: main
      dataset: dlt_dev
      auth_mode: u2m          # browser auth for local dev
```

For production (service principal):

```yaml
    databricks_prod:
      destination_type: databricks
      server_hostname: adb-1234567890.12.azuredatabricks.net
      http_path: /sql/1.0/warehouses/abc123
      catalog: main
      auth_mode: m2m
      client_id: "azurekeyvault::https://my-vault.vault.azure.net::databricks-client-id"
      client_secret: "azurekeyvault::https://my-vault.vault.azure.net::databricks-client-secret"
```

The same pipeline config works unchanged — partition and cluster hints are translated to Databricks SQL (`PARTITIONED BY` / `CLUSTER BY`):

```yaml
# configs/api/myservice/data.yml
adapter: acme.api.myservice
tags: [daily]
write_disposition: append
api_key: "azurekeyvault::https://my-vault.vault.azure.net::myservice-api-key"

partition_column: _dlt_ingested_at
cluster_columns: [account_id]
```

### Access Control

BigQuery uses `access:` lists with IAM principals; Databricks uses Unity Catalog GRANT/REVOKE with `user:`, `group:`, or `service_principal:` prefixes:

```yaml
# BigQuery
access:
  - "group:analysts@example.com"
  - "user:alice@example.com"

# Databricks (same field, different principal syntax)
access:
  - "group:analysts"
  - "service_principal:my-sp-app-id"
```

Apply with `saga update-access`.

### Secret References

All string config fields support secret reference prefixes — the same config works across providers:

| Prefix | Provider |
|--------|----------|
| `googlesecretmanager::project::secret-name` | GCP Secret Manager |
| `azurekeyvault::https://vault-name.vault.azure.net::secret-name` | Azure Key Vault |
| `env_secret::VAR` | Secret from an environment variable (resolved at runtime) |

---

## Destination Plugins

A destination plugin tells `dlt-saga` how to write data to a specific data
warehouse. Every destination extends `Destination`.

### Public API — Destination

#### Abstract Methods (must override)

| Method | Description |
|--------|-------------|
| `create_dlt_destination()` | Return a dlt destination instance (e.g., `dlt.destinations.bigquery()`) |
| `apply_hints(resource, **hints)` | Apply destination-specific hints (partitioning, clustering) |
| `get_access_manager()` | Return `AccessManager` or `None` |
| `supports_access_management()` | Return `bool` |
| `supports_partitioning()` | Return `bool` |
| `supports_clustering()` | Return `bool` |

#### Optional Overrides

| Method | Default | Description |
|--------|---------|-------------|
| `get_client_pool()` | `None` | Return a client pool for connection reuse |
| `prepare_for_execution(pipeline_configs)` | no-op | One-time setup before parallel execution (e.g., pre-create schemas) |
| `run_pipeline(pipeline, data)` | `pipeline.run(data)` | Wrap pipeline execution (e.g., ensure dataset exists) |
| `save_load_info(dataset, records, pipeline)` | dlt resource append | Direct insert for performance |
| `get_last_load_timestamp(dataset, pipeline, table)` | raises | Query last successful load time |
| `get_max_column_value(table_id, column)` | raises | Query max column value for incremental loading |
| `execute_sql(sql, dataset_name)` | raises | Execute arbitrary SQL (used by historize) |
| `reset_destination_state(pipeline_name, table_name)` | no-op | Clean up tables/metadata for full refresh |

#### SQL Dialect Methods

Override these if your destination uses different SQL syntax:

| Method | Default | Description |
|--------|---------|-------------|
| `quote_identifier(name)` | `` `name` `` | Quote identifiers |
| `get_full_table_id(dataset, table)` | raises | Build fully qualified table reference |
| `hash_expression(columns)` | `FARM_FINGERPRINT(...)` | Change detection hash |
| `partition_ddl(column)` | `""` | Partition DDL clause |
| `cluster_ddl(columns)` | `""` | Cluster DDL clause |
| `type_name(logical_type)` | BigQuery types | Map logical types to SQL types |
| `cast_to_string(expression)` | `CAST(... AS STRING)` | Cast expression to string |
| `columns_query(database, schema, table)` | raises | Schema introspection SQL |

### Destination Config

Each destination needs a config dataclass that extends `DestinationConfig`:

```python
from dataclasses import dataclass
from dlt_saga.destinations.config import DestinationConfig


@dataclass
class SnowflakeDestinationConfig(DestinationConfig):
    destination_type: str = "snowflake"
    account: str = ""
    warehouse: str = ""
    database: str = ""
    schema: str = ""
    role: str = ""

    @classmethod
    def from_context(cls, context, config_dict):
        return cls(
            account=context.get("account"),
            warehouse=context.get("warehouse"),
            # ...
        )

    @classmethod
    def from_dict(cls, data):
        return cls(**{k: v for k, v in data.items() if k in ...})
```

### Registration

Register your destination in the factory at startup (e.g., in your package's
`__init__.py`):

```python
from dlt_saga.destinations.factory import DestinationFactory

DestinationFactory.register("snowflake", SnowflakeDestination, SnowflakeDestinationConfig)
```

Entry point registration for destinations is not yet supported — destinations
are substantial implementations (full SQL dialect, auth, access management) and
are expected to be contributed to the core package rather than shipped as
third-party plugins.

---

## Access Manager Plugins

For destinations that support table-level access control:

```python
from dlt_saga.destinations.base import AccessManager


class SnowflakeAccessManager(AccessManager):
    def manage_access_for_tables(self, table_ids, access_config, revoke_extra=True):
        # Implement GRANT/REVOKE logic
        pass

    def parse_access_list(self, access_list):
        # Parse ["role:analyst", "role:data_engineer"] into structured format
        pass
```

---

## Lifecycle Hooks

Hooks let you react to pipeline lifecycle events without subclassing
`BasePipeline`. They are the recommended way to add observability, alerting,
or metrics to any pipeline run.

### Available Events

| Event | When | `ctx.result` | `ctx.error` |
|-------|------|-------------|-------------|
| `on_pipeline_start` | Before execution begins | `None` | `None` |
| `on_pipeline_complete` | After successful completion | load_info / run_result dict | `None` |
| `on_pipeline_error` | After failure | `None` | `Exception` |

### Writing a Hook

A hook is a plain callable that accepts a `HookContext`:

```python
# mypackage/hooks.py
from dlt_saga.hooks import HookContext


def on_failure(ctx: HookContext) -> None:
    """Send a Slack alert when any pipeline fails."""
    send_slack(
        channel="#data-alerts",
        text=f"Pipeline `{ctx.pipeline_name}` failed ({ctx.command}): {ctx.error}",
    )


# Limit to specific events (default: all events)
on_failure.saga_hook_events = ["on_pipeline_error"]
```

`HookContext` fields:

| Field | Type | Description |
|-------|------|-------------|
| `pipeline_name` | `str` | Fully-qualified name (e.g. `google_sheets__budget`) |
| `config` | `PipelineConfig` | Full pipeline config object |
| `command` | `str` | `"ingest"` or `"historize"` |
| `result` | `Any \| None` | load_info dict (ingest) or run_result dict (historize) on success |
| `error` | `Exception \| None` | Exception raised on failure |

### Registering Hooks

**Via `saga_project.yml`** — per-event lists of `'module:callable'` strings:

```yaml
hooks:
  on_pipeline_error:
    - mypackage.hooks:on_failure
  on_pipeline_complete:
    - mypackage.hooks:emit_metrics
```

**Via entry points** — for hooks shipped inside an installable package:

```toml
[project.entry-points."dlt_saga.hooks"]
my_hook = "mypackage.hooks:on_failure"
```

> **Thread safety:** Hooks fire from the same worker thread as the pipeline,
> so they must be thread-safe. Keep hooks fast — long-blocking work should be
> offloaded to a background thread or async queue.

---

## Testing Your Plugin

`dlt_saga.testing` provides factory functions and pytest fixtures for testing pipeline implementations against an in-memory DuckDB destination — no cloud credentials needed.

### Quick start: `run_pipeline_test`

The simplest way to test a `BasePipeline` subclass end-to-end:

```python
import dlt
import pytest
from dlt_saga.pipelines.base_pipeline import BasePipeline
from dlt_saga.testing import run_pipeline_test


class MyPipeline(BasePipeline):
    def extract_data(self):
        @dlt.resource(name=self.table_name)
        def _rows():
            yield {"id": 1, "name": "Alice"}
            yield {"id": 2, "name": "Bob"}

        return [(_rows(), "people")]


def test_pipeline_runs():
    result = run_pipeline_test(
        MyPipeline,
        config_dict={
            "schema_name": "test",
            "table_name": "people",
            "pipeline_name": "test__people",
            "write_disposition": "append",
            # Your source-specific config:
            "api_url": "https://api.example.com/test",
        },
    )
    assert result.success
    assert result.load_info is not None


def test_pipeline_failure_is_captured():
    class BrokenPipeline(BasePipeline):
        def extract_data(self):
            raise RuntimeError("connection refused")

    result = run_pipeline_test(
        BrokenPipeline,
        config_dict={"schema_name": "test", "table_name": "t", "pipeline_name": "test__t"},
    )
    assert not result.success
    assert "connection refused" in result.error
```

`run_pipeline_test` returns a `PipelineResult` with `.success`, `.error`, and `.load_info` (list of dlt load-info dicts on success).

### Mocking external API calls

Wrap your HTTP client calls with `unittest.mock.patch` or `responses`:

```python
from unittest.mock import patch
from dlt_saga.testing import run_pipeline_test


def test_pipeline_with_mock_api():
    fake_response = [{"id": 1, "value": 42}]

    with patch("mypackage.pipeline.requests.get") as mock_get:
        mock_get.return_value.json.return_value = fake_response
        mock_get.return_value.status_code = 200

        result = run_pipeline_test(
            MyApiPipeline,
            config_dict={
                "schema_name": "test",
                "table_name": "records",
                "pipeline_name": "test__records",
                "api_url": "https://api.example.com/records",
            },
        )

    assert result.success
```

### Building `PipelineConfig` objects for unit tests

Use `make_pipeline_config` when testing logic that takes a `PipelineConfig` — selectors, discovery, hook dispatch — without running a full pipeline:

```python
from dlt_saga.testing import make_pipeline_config


def test_selector_logic():
    config = make_pipeline_config(
        pipeline_group="api",
        table_name="events",
        tags=["daily", "critical"],
        write_disposition="append+historize",
        config_dict={"api_key": "test"},
    )
    assert config.pipeline_name == "api__events"
    assert config.has_tag("daily")
    assert config.ingest_enabled
    assert config.historize_enabled
    assert config.config_dict["api_key"] == "test"
```

### pytest fixtures

Register the built-in fixtures in your `conftest.py`:

```python
# conftest.py
pytest_plugins = ["dlt_saga.testing.fixtures"]
```

| Fixture | Type | Description |
|---------|------|-------------|
| `saga_duckdb_destination` | `DuckDBDestination` | Fresh in-memory DuckDB destination, closed automatically after each test |
| `saga_clean_context` | — | Resets the global execution context after each test to prevent state leakage |
| `saga_pipeline_config` | factory function | `make_pipeline_config` bound as a fixture, call it with any keyword arguments |

```python
def test_destination_sql(saga_duckdb_destination):
    saga_duckdb_destination.execute_sql(
        'CREATE SCHEMA IF NOT EXISTS "test"',
        dataset_name="test",
    )
    saga_duckdb_destination.execute_sql(
        'CREATE TABLE "test"."t" (id INTEGER)',
        dataset_name="test",
    )
    saga_duckdb_destination.execute_sql(
        'INSERT INTO "test"."t" VALUES (42)',
        dataset_name="test",
    )
    rows = saga_duckdb_destination.execute_sql('SELECT id FROM "test"."t"')
    assert rows[0][0] == 42


def test_tag_filtering(saga_pipeline_config):
    config = saga_pipeline_config(
        pipeline_group="api",
        table_name="events",
        tags=["daily"],
    )
    assert config.has_tag("daily")
    assert not config.has_tag("hourly")
```

### Suggested test layout

```
my-plugin/
├── mypackage/
│   ├── pipeline.py          # BasePipeline subclass
│   └── config.py            # Config dataclass
└── tests/
    ├── conftest.py           # pytest_plugins = ["dlt_saga.testing.fixtures"]
    ├── test_pipeline.py      # run_pipeline_test end-to-end tests
    └── test_config.py        # make_pipeline_config unit tests
```

### CI configuration

```yaml
# .github/workflows/ci.yml
- name: Run tests
  run: pytest -m "not integration" --tb=short
  # integration tests that hit a real warehouse go in a separate job
  # with credentials injected as secrets
```

---

## Version Compatibility

```python
import dlt_saga

# Check plugin API version at import time
assert dlt_saga.PLUGIN_API_VERSION >= 1, "Requires dlt-saga plugin API v1+"
```

The plugin API version increments when:
- Abstract methods are added/removed on `BasePipeline` or `Destination`
- Method signatures change in a way that breaks existing implementations
- Constructor contracts change

It does **not** increment for:
- New optional methods with defaults
- New internal (`_`-prefixed) methods
- Bug fixes in base implementations
