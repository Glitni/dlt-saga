# Custom Naming

`dlt-saga` derives every schema name, table name, and external-table storage URI from a small set of overridable hooks. The defaults cover most projects, but when you need to encode org-specific conventions — per-environment dataset prefixes, per-tenant subfolders, distinct raw/historized layouts inside a BigLake bucket — point `naming_module` in `saga_project.yml` at a Python module that defines one or more of the hooks below.

## When to override

You probably **don't** need a custom naming module. The framework defaults cover:

- Prod schema = `dlt_<pipeline_group>`, dev schema = the developer's schema (`SAGA_SCHEMA_NAME` or the profile's `schema`).
- Prod table = `<sub_folders>__<base_name>`, dev table = `<pipeline_group>__<...>__<base_name>`.
- Storage URI = `<storage_path>/<schema>/<table>/` for BigLake Iceberg / Databricks external tables.

Reach for a custom naming module when one of these isn't enough:

| Need                                                                 | Hook                          |
| -------------------------------------------------------------------- | ----------------------------- |
| Distinct dataset names per layer (`dlt_<group>_raw` vs `_historized`) | `generate_schema_name`        |
| Per-tenant or env-prefixed subfolders inside a single storage bucket | `generate_target_location`    |
| A table-name convention the segment-based default doesn't capture    | `generate_table_name`         |

Each hook is optional — return only the dispatches you care about, the rest fall back to the framework defaults.

## The segments contract

Every hook receives an ordered `segments: list[str]` that identifies the pipeline config:

- `segments[0]` is the **pipeline group** (the top-level folder under `configs/`, e.g. `google_sheets`).
- `segments[-1]` is the **base name** (the YAML filename without extension).
- Anything in between adds organisational depth.

For `configs/google_sheets/asm/salgsmal.yml`, segments are `["google_sheets", "asm", "salgsmal"]`. Other config sources (database-backed, SharePoint-backed, etc.) produce their own segments from whatever identifier shape they expose — the hooks don't care where segments came from.

## The `layer` keyword

Every hook accepts a `layer` keyword — `"ingest"` (default) or `"historize"`. The framework calls the same hook twice, once per layer, so a custom module can return distinct values per layer without inspecting any other context. The framework defaults ignore `layer` (they return the same shape either way); your custom module opts in to layer-aware behaviour by branching on the keyword.

This is the canonical extension point for the common ask: *"my historized data should live in a different dataset / under a different bucket prefix than my raw data."*

## Wiring it up

1. Create a Python module (any importable path — a single file at the project root works, e.g. `naming.py`).
2. Define one or more of `generate_schema_name`, `generate_table_name`, `generate_target_location`. Missing hooks fall back to the framework defaults.
3. Point `saga_project.yml` at the module:

```yaml
naming_module: my_project.naming  # or just `naming` if the file sits next to saga_project.yml
```

If your custom module imports the framework defaults to wrap them, do so from `dlt_saga.pipeline_config`:

```python
from dlt_saga.pipeline_config import (
    default_generate_schema_name,
    default_generate_table_name,
    default_generate_target_location,
)
```

The defaults accept the same parameters you'll use in your custom hook — pass them straight through when you want default behaviour for a particular branch.

## Hook reference

### `generate_schema_name`

```python
def generate_schema_name(
    segments: list[str],
    environment: str,
    default_schema: str,
    *,
    layer: str = "ingest",
    custom_schema_name: str | None = None,
) -> str: ...
```

Returns the schema/dataset name a pipeline writes to. Called once per layer when the historize factory builds its target — return a value distinct from the ingest side to land historized data in a separate schema.

- `environment` — `"prod"` or `"dev"`. Lets you split prod/dev conventions.
- `default_schema` — the dev-side schema the user configured (via profile or `SAGA_SCHEMA_NAME`). Useful as the dev fallback.
- `layer` — `"ingest"` or `"historize"`.
- `custom_schema_name` — the config's explicit `schema_name:` override, or `None`. The default composes it **per environment**: used directly in prod, and namespaced under the developer's sandbox in dev (`{default_schema}_{custom_schema_name}`) so a shared config keeps each developer isolated. Compose it differently, or ignore it, as you see fit. A hook written against the older signature (no `custom_schema_name` parameter) keeps the legacy behavior — an explicit `schema_name:` is used verbatim in both environments — until it adopts the parameter.

### `generate_table_name`

```python
def generate_table_name(
    segments: list[str],
    environment: str,
    *,
    layer: str = "ingest",
) -> str: ...
```

Returns the table name. The framework default produces dev-prefixed names (`<group>__<base>` in dev, bare `<base>` in prod). Override when your downstream layer (dbt, BI tooling) needs a different convention.

When you return a layer-specific table name (e.g. appending `_historized` for `layer="historize"`), the historize factory adopts it as the historize-side override — that wins over `HistorizeProjectConfig.table_suffix`.

### `generate_target_location`

```python
def generate_target_location(
    segments: list[str],
    environment: str,
    default_storage_root: str | None,
    *,
    layer: str = "ingest",
    schema: str | None = None,
    table: str | None = None,
) -> str | None: ...
```

Returns the external-table storage URI for table formats that need one (BigLake Iceberg on BigQuery; Delta / Iceberg / DeltaUniform on Databricks via `native_load`).

- `default_storage_root` — the profile's `storage_path` (BigQuery) or `storage_root` (Databricks). Returning `None` from the hook tells the destination "use a managed table" (Databricks only — BigQuery requires an Iceberg URI when `table_format: iceberg`).
- `schema` / `table` — the already-resolved warehouse names for this layer. Use these to compose the URI; segments aren't always populated (the BigQuery Iceberg URI builder doesn't carry them).
- `layer` — `"ingest"` or `"historize"`.

## Worked examples

### 1. Split raw vs historized at the top folder of the BigLake bucket

The simplest case has no custom code at all — set `historize.storage_path` on the profile and you're done:

```yaml
# profiles.yml
default:
  outputs:
    prod:
      type: bigquery
      table_format: iceberg
      storage_path: gs://bucket/dlt/raw
      historize:
        storage_path: gs://bucket/dlt/historized
```

URIs land at:

- Ingest: `gs://bucket/dlt/raw/<dataset>/<table>/`
- Historize: `gs://bucket/dlt/historized/<dataset>/<table>_historized/`

A custom `generate_target_location` is only needed when the shape inside each bucket needs to differ — e.g. per-tenant subfolders, environment-prefixed paths.

### 2. Per-environment subfolder in the URI

```python
# naming.py
from dlt_saga.pipeline_config import default_generate_target_location


def generate_target_location(
    segments, environment, default_storage_root,
    *, layer="ingest", schema=None, table=None,
):
    # Inject the environment as a top-level folder inside the bucket.
    if not default_storage_root:
        return None
    base = default_storage_root.rstrip("/")
    return f"{base}/{environment}/{schema}/{table}/"
```

```yaml
# saga_project.yml
naming_module: naming
```

URIs become `<storage_path>/<env>/<dataset>/<table>/`.

### 3. Distinct dataset names per layer

```python
# naming.py
def generate_schema_name(segments, environment, default_schema, *, layer="ingest"):
    if environment != "prod":
        return default_schema  # dev keeps a single shared schema
    group = segments[0] if segments else "default"
    suffix = "_raw" if layer == "ingest" else "_historized"
    return f"dlt_{group}{suffix}"
```

In prod this produces:

- Ingest schema: `dlt_<group>_raw`
- Historize schema: `dlt_<group>_historized`

Combine with [`historize_schema_access`](Historize#per-layer-access) to grant the historized schemas to a broader audience than the raw side.

### 4. Per-tenant URI subfolder driven by pipeline-config metadata

When a single source ingests into per-tenant tables and you want each tenant's files in its own GCS prefix, the hook can derive the tenant from segments (or wrap `default_generate_target_location` to inject a known segment):

```python
# naming.py
from dlt_saga.pipeline_config import default_generate_target_location


def generate_target_location(
    segments, environment, default_storage_root,
    *, layer="ingest", schema=None, table=None,
):
    if not default_storage_root:
        return None
    # configs/api/<tenant>/<source>.yml → segments[1] is the tenant
    tenant = segments[1] if len(segments) >= 2 else "shared"
    base = default_storage_root.rstrip("/")
    return f"{base}/{tenant}/{schema}/{table}/"
```

### 5. Mixing custom and default — fall back when you don't care

A hook can dispatch to the framework default for unknown cases:

```python
from dlt_saga.pipeline_config import default_generate_schema_name


def generate_schema_name(segments, environment, default_schema, *, layer="ingest"):
    if layer == "historize" and environment == "prod":
        group = segments[0] if segments else "default"
        return f"dlt_{group}_historized"
    return default_generate_schema_name(segments, environment, default_schema, layer=layer)
```

## Avoiding target collisions

Each **enabled** pipeline config must map to exactly one destination table — a project-wide invariant. The default naming scheme guarantees this structurally (a per-group schema in prod, group-prefixed table names in dev), but a custom `naming_module` can break it — e.g. a `generate_table_name` that returns the same name for several configs, or an explicit `schema_name:` combined with a shared table name. Two configs pointed at one table is corruption *by construction*, independent of schedule: each is an independent writer that overwrites and interleaves the other's data. An hourly pipeline pointed at a monthly pipeline's table clobbers it every hour and the monthly run stomps back once a month — they never share an execution and the table is still wrong.

dlt-saga guards against this, and detection is **project-wide** — it resolves every enabled config's target, not just the ones a given run selects:

- **`saga ingest` / `saga historize` / `saga run`** (and the orchestrator plan) **fail before touching the warehouse** when a *selected* pipeline shares a target with any enabled config — naming the co-claimant even if it isn't in this run. So deploying a new pipeline onto an existing table's identity is caught even when you run the new one alone. Every member of a collision group fails until you disambiguate (the framework can't know which config rightfully owns the table); unrelated pipelines in the same run proceed. Resolution is pinned to the environment the run targets.
- **`saga doctor`** runs the same check read-only, resolving in the current environment **and** prod. That flags latent prod collisions from a dev run *and* dev-only collisions a custom `naming_module` can create (default naming can't collide dev-only — dev table names are group-prefixed and path-unique). Detection stays project-wide even under `--select`, so a CI run over just the changed configs still surfaces what they collide with. Each collision is labelled with the environment(s) it manifests in.

The check covers both the ingest target and the resolved historized target (including two `write_disposition: historize` pipelines with no ingest target). If a run stops with "multiple pipelines resolve to the same destination table", give each pipeline a distinct target (via `schema_name` / `table_name` / your `naming_module`), or consolidate them into a single pipeline that reads from all sources.

## Backwards compatibility

The `layer` keyword and the `schema`/`table` kwargs on `generate_target_location` were added in 0.4.0. Custom modules written against earlier signatures continue to work — the framework introspects the hook's signature and drops keyword arguments the function doesn't accept. You can opt in to the new keywords incrementally, one hook at a time.

## See also

- [Profiles](Profiles) — where `storage_path` and the `historize.storage_path` override live.
- [Historize (SCD2)](Historize) — for `historize_schema_access`, `historize.output_schema`, and the `placement` strategy that the naming hooks compose with.
- [Configuration](Configuration) — hierarchical pipeline config and the `+key:` merge syntax.
