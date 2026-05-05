# Project Context

dlt-saga is a config-driven data ingestion and historization framework built on top of [dlt](https://dlthub.com/). It adds a declarative pipeline configuration layer, SCD2 historization, multi-cloud destination support, and a `saga` CLI — so teams can run and orchestrate pipelines without writing boilerplate dlt code.

## Architecture Overview

### Three-Layer Architecture

1. **Source Layer** (`pipelines/`)
   - Source-specific implementations (google_sheets, filesystem, api, database, etc.)
   - Each source has: `config.py`, `client.py`, `pipeline.py`, `__init__.py`
   - Inherits from `BasePipeline` and `BaseClient`

2. **Destination Layer** (`destinations/`)
   - Destination-specific implementations (BigQuery, Databricks, DuckDB)
   - Uses factory pattern for instantiation
   - Each destination has: `destination.py`, optionally `access.py` (for IAM/GRANT management)
   - Abstract base classes in `destinations/base.py`
   - Capability-based design (`supports_partitioning`, `supports_clustering`, etc.)

3. **Configuration Layer** (`pipeline_config/`)
   - Discovers pipeline configs from various sources (files, potentially databases)
   - Currently: YAML files in `configs/` directory
   - Supports dbt-style hierarchical configuration
   - Environment-aware naming (prod vs dev)

### Key Features

**Profiles System** (`utility/cli/profiles.py`)
- dbt-style `profiles.yml` configuration
- Multi-environment support (dev, prod, staging, etc.)
- Service account impersonation for production
- Target selection via `--target` parameter

**Selector System** (`utility/cli/selectors.py`)
- dbt-style selector syntax for filtering pipelines
- Supports: pipeline names, glob patterns, tag selectors, group selectors
- UNION (OR): space-separated or multiple `--select` flags
- INTERSECTION (AND): comma-separated selectors
- Examples:
  - `--select "tag:daily"` - select by tag
  - `--select "group:google_sheets"` - select by group
  - `--select "*sales*"` - glob pattern
  - `--select "tag:daily,group:google_sheets"` - intersection (AND)
  - `--select "tag:daily group:filesystem"` - union (OR)

**Execution Modes**
1. Local execution: Run pipelines sequentially or in parallel
2. Orchestrator mode: Create execution plan and trigger remote workers (e.g. Cloud Run Jobs)
3. Worker mode: Execute assigned tasks from an execution plan

**Environment-Aware Naming** (`utility/naming.py`)
- Production: Separate dataset per pipeline group (`dlt_google_sheets`, `dlt_filesystem`)
- Development: Developer-specific dataset (from `SAGA_SCHEMA_NAME` or profile)
- Table names: Include pipeline group prefix in dev, exclude in prod

**Polymorphic Pipeline Loading** (`pipelines/registry.py`)
- Namespace-based resolution via `adapter` field in configs
- Class auto-discovery: finds the `BasePipeline` subclass defined in a `pipeline.py` module
- Resolution order:
  1. Explicit `adapter` → namespace lookup → import module → auto-discover class
  2. Folder-structure fallback (when `adapter` omitted): config path → module path
  3. Base implementation for `pipeline_group`
- Built-in namespace `dlt_saga` → `dlt_saga.pipelines` (always registered)
- Bare names (no namespace prefix) resolve via default namespace: `api.myservice` = `dlt_saga.api.myservice`
- Any class name works — no naming convention enforced
- Lazy-loads external packages from `packages.yml` before first resolution

**Pipeline Packages** (`packages.yml`, `dlt_saga/packages.py`)
- External pipeline implementations registered via `packages.yml` at repo root
- Each package entry: `namespace`, `path` (Python package)
- Local path packages: parent directory added to `sys.path`, directory name becomes importable module
- Example: `adapter: local.api.my_source` → `my_pipelines/api/my_source/pipeline.py`

**Granular Timing Tracking**
- Tracks: initialization, extraction, load, finalization phases
- Reports timing in format: `68.3s total (init: 0.2s, extract: 25.7s, load: 32.4s, finalize: 10.0s)`

## Currently Implemented

### Sources
- **google_sheets**: Extract data from Google Sheets with change detection
- **filesystem**: Extract files from GCS/local filesystem with incremental loading
- **database**: Extract from SQL databases via dlt's `sql_database` source
- **api**: Base HTTP API pipeline with pagination support
- **sharepoint**: Extract files from SharePoint

### Bulk loaders
- **native_load** (`adapter: dlt_saga.native_load`): Bypasses dlt extract/normalize and loads Parquet/CSV/JSONL directly into the warehouse via destination-native bulk mechanics (BigQuery external tables → INSERT, Databricks COPY INTO). Use for >1 000 files or >1 GB per run. Supports `append`, `replace`, `append+historize`, and `replace+historize` write dispositions. File-level dedup across runs requires `incremental: true` (opt-in, mirrors dlt's split between write_disposition and `dlt.sources.incremental`). `replace` rewrites the target table on every run (no state needed). Fully compatible with the historize layer — no extra config needed. State tracked in `_saga_native_load_log` (only when `incremental: true`). Supports BigQuery (`gs://`) and Databricks (`gs://`, `abfss://`). Column names always normalized to snake_case (BigQuery); explicit type hints via `columns:`; date-partition filtering via `partition_prefix_pattern` (requires `incremental: true`). Databricks supports external Delta/Iceberg/DeltaUniform tables via `table_format` + `target_location`.

### Destinations
- **bigquery**: Full implementation with partitioning, clustering, IAM management
- **databricks**: Full implementation with Unity Catalog, volume staging, DEEP CLONE
- **duckdb**: Local development and integration testing

### Deployment Targets
- Local development (DuckDB or any destination)
- GCP Cloud Run (orchestration via `saga plan` / `saga worker`)
- Any container-based environment via `StdoutProvider`

## CLI Usage

All commands are subcommands under a single `saga` entry point.

### List Command
```bash
saga list                                           # All enabled pipelines
saga list --resource-type ingest                    # Ingest-enabled only
saga list --resource-type historize                 # Historize-enabled only
saga list --select "tag:daily"                      # With selector
saga list --select "tag:daily" --resource-type ingest
```

### Ingest Command
```bash
saga ingest                                         # All ingest-enabled
saga ingest --select "tag:daily"                    # By tag
saga ingest --select "group:google_sheets"          # By group
saga ingest --select "*sales*"                      # Glob pattern

# Selectors
saga ingest --select "tag:daily group:google_sheets"    # UNION (OR)
saga ingest --select "tag:daily,group:google_sheets"    # INTERSECTION (AND)

# Parallel execution
saga ingest --workers 8
saga ingest --select "group:google_sheets" --workers 1  # Sequential

# Profiles
saga ingest --target prod
saga ingest --profile myprofile --target staging

# Orchestration
saga ingest --orchestrate
```

### Historize Command
```bash
saga historize --select "tag:daily"                 # Incremental (default)
saga historize --select "filesystem__reports__*"    # Specific pipeline
saga historize --full-refresh --select "..."        # Full rebuild
saga historize --verbose --select "..." --target dev # Debug mode
```

### Run Command (Combined)
```bash
saga run --select "tag:daily"                       # Ingest then historize
saga run --workers 8 --select "tag:daily"           # Parallel workers
saga run --full-refresh --select "..."              # Full refresh (prompts separately for ingest/historize)
```

### Update Access Command
```bash
saga update-access                                  # All tables
saga update-access --select "group:google_sheets"   # Specific group
saga update-access --target prod                    # In production
```

### Report Command
```bash
saga report                                         # Default report (30 days)
saga report --days 7 --output weekly.html           # Last 7 days
saga report --select "tag:daily" --target prod      # Filtered by selector
saga report --select "group:google_sheets" -o gs.html  # Single group
```

### AI Setup Command
```bash
saga ai-setup                                       # Generate saga_ai_context.md
```

Writes `saga_ai_context.md` to the project root with framework patterns and guidance for AI coding assistants. Content hash is embedded; `saga doctor` warns if outdated after a package upgrade.

### Configuration Files

**profiles.yml** (repo root, with `.dlt/profiles.yml` as legacy fallback)
```yaml
default:
  target: dev
  outputs:
    dev:
      project: <gcp-project>
      location: EU
      environment: dev
      dataset: dlt_dev
      destination_type: bigquery

    prod:
      project: <gcp-project>
      location: EU
      environment: prod
      destination_type: bigquery
      run_as: sa@<project>.iam.gserviceaccount.com
```

**Pipeline packages** (`packages.yml`)
```yaml
packages:
  - namespace: local
    path: ./my_pipelines          # Python package with pipeline.py modules
```

**Pipeline config** (`configs/<pipeline_group>/<name>.yml`)
```yaml
tags: [daily, critical]
adapter: dlt_saga.api.myservice   # optional: explicit implementation binding

# Source config (type-specific)
spreadsheet_id: "..."
sheet_name: "Data"

# Target config
write_disposition: "merge"
merge_strategy: "scd2"
partition_column: "date"
cluster_columns: ["id"]
```

**Pipeline config with ingest + historization** (`configs/<pipeline_group>/<name>.yml`)
```yaml
tags: [daily]

# write_disposition controls what runs:
#   "append"           → ingest only
#   "append+historize"  → ingest AND historize
#   "historize"         → historize only (external data)
write_disposition: "append+historize"
primary_key: [id]

# Historize config
historize:
  # snapshot_column: _dlt_ingested_at   # default
  # primary_key: inherited from top level
  ignore_columns: [updated_by]
  partition_column: "_dlt_valid_from"
  cluster_columns: [id]
  track_deletions: true
```

**External delivery (historize-only, no ingest)**
```yaml
tags: [daily]
write_disposition: "historize"
primary_key: [order_id]

# Source location (top-level, shared between ingest and historize)
source_database: "<source>"
source_schema: "external_deliveries"
source_table: "customer_orders_raw"

historize:
  snapshot_column: "delivery_date"
  track_deletions: true
```

## Development Setup

- Python 3.11+
- Install: `uv sync --extra dev`
- Run tests: `uv run pytest -m unit` / `uv run pytest -m integration`
- Lint: `uv run ruff check .` / `uv run ruff format --check .`
- Type check: `uv run mypy dlt_saga/`
- Cloud authentication is destination-specific: BigQuery uses ADC (`gcloud auth application-default login`) or service account impersonation; Databricks uses OAuth or access token; DuckDB requires no credentials

# Coding Rules

## Code Style
- Follow the project formatter. Do not change file style.
- Use logging instead of print. Include error context.
- Add type hints and docstrings for public functions.
- Keep functions small; prefer composition over long scripts.
- Write safe defaults. Handle timeouts and retries where external calls exist.

## Error Handling
- Distinguish between configuration errors (ValueError, no traceback) and unexpected errors (full traceback).
- Provide clear, actionable error messages that guide users to the fix.
- Use `logger.error()` for user-facing errors, `logger.debug()` for internal diagnostics.
- Don't duplicate error logging — log once at the appropriate level.

## Architecture Patterns

### Source Implementations (`pipelines/`)
- Each source type gets its own directory: `pipelines/<source_type>/`
- Required files: `config.py`, `client.py`, `pipeline.py`, `__init__.py`
- Inherit from `BasePipeline` and `BaseClient`
- Config class uses dataclass with type hints
- Class auto-discovered by registry: any `BasePipeline` subclass in `pipeline.py` works
- Bind via `adapter` in config (e.g., `adapter: dlt_saga.api.myservice`)
- External implementations registered via `packages.yml` with a namespace + path

### Destination Implementations (`destinations/`)
- Each destination type gets its own directory: `destinations/<dest_type>/`
- Required files: `destination.py`, optionally `access.py` for IAM/GRANT management
- Inherit from `Destination` and optionally `AccessManager` abstract classes
- Register in `destinations/factory.py` via `DestinationFactory.register()`
- Use capability-based design: `supports_partitioning()`, `supports_clustering()`, etc.
- SQL dialect methods on `Destination`: `quote_identifier()`, `get_full_table_id()`, `hash_expression()`, `partition_ddl()`, `cluster_ddl()`, `type_name()`, `cast_to_string()`, `columns_query()`
- Query methods: `get_max_column_value()`, `get_last_load_timestamp()`, `execute_sql()`

### Configuration Discovery (`pipeline_config/`)
- Config sources inherit from `ConfigSource` abstract class
- Must implement `discover()` and `get_config()` methods
- Return `PipelineConfig` dataclass instances
- Support environment-aware field interpolation (e.g., `${ENV_VAR}`)
- `write_disposition` controls what commands run: `append+historize` enables both, `historize` enables historize-only
- `PipelineConfig.ingest_enabled` — derived from `write_disposition` (True when base is append/merge/replace)
- `PipelineConfig.historize_enabled` — derived from `write_disposition` (True when contains "historize")
- `PipelineConfig.dlt_write_disposition` — strips `+historize` suffix for dlt
- `enabled: false` disables both ingest and historize (master switch)

### Historize Layer (`historize/`)
- `config.py`: `HistorizeConfig` dataclass with `from_dict()` factory — source location fields (`source_database`, `source_schema`, `source_table`) are top-level, not nested under `historize:`
- `state.py`: `HistorizeStateManager` — manages `_saga_historize_log` table (lazy creation), snapshot discovery, config fingerprint for change detection
- `sql.py`: `HistorizeSqlBuilder` — generates full-reprocess and incremental SQL; deletions are separate marker rows; incremental uses LEAD for within-batch sequencing and MERGE to close existing open records
- `runner.py`: `HistorizeRunner` — orchestrates discovery → mode selection → SQL execution → logging, with phase-level timing (init/setup/execute)
- Coexists with dlt's built-in SCD2 (`merge_strategy: scd2`) — no conflict
- `_dlt_ingested_at` column injected by `BasePipeline._inject_ingested_at()` for ALL append pipelines (handles both dict rows and PyArrow tables)
- `_dlt_ingested_at` 3-tier resolution: (1) regex from file path, (2) file modification date, (3) extraction timestamp
- `_dlt_is_deleted` boolean column: deletions produce a **separate marker row** (`_dlt_is_deleted = TRUE`, `_dlt_valid_to = NULL`) rather than flagging the closed row; closed change rows always have `_dlt_is_deleted = FALSE`; deletion markers are closed (`_dlt_valid_to` set) when the key reappears
- File metadata (`_dlt_source_file_name`, `_dlt_source_modification_date`) auto-injected for append-mode filesystem pipelines only (not for merge/SCD2 to avoid false change detection)
- `snapshot_date_regex` + `snapshot_date_format`: top-level config fields for extracting snapshot dates from file paths
- Config fingerprint stored in log — detects changes to `primary_key`, `track_columns`, `ignore_columns`, `track_deletions`, `snapshot_column` and prompts for `saga historize --full-refresh`

### CLI and UX
- Single `saga` entry point with subcommands: `list`, `ingest`, `historize`, `run`, `update-access`, `report`
- Use dbt-style conventions where applicable (profiles, selectors, targets)
- `saga list` with `--resource-type` filter for filtered listing
- Use `--verbose` / `-v` for debug logging (affects all loggers via `logging_manager.set_level()`)
- Default behavior should be sensible (e.g., `saga ingest` with no args runs all ingest-enabled pipelines)

## Naming Conventions

### Environment-Aware Naming
- Production: `dlt_<pipeline_group>` datasets, no prefix on table names
- Development: `dlt_dev` (or custom) dataset, `<pipeline_group>__<table_name>` table names
- Pipeline names: Always `<pipeline_group>__<base_table_name>` (consistent across environments)

### File Naming
- Config files: `configs/<pipeline_group>/<subfolder>/<name>.yml`
- Source implementations: `pipelines/<source_type>/pipeline.py`
- Destination implementations: `destinations/<dest_type>/destination.py`

### Variable Naming
- Use descriptive names: `selected_configs`, not `sc`
- Prefix private methods with `_`: `_select_single()`
- Use snake_case for functions/methods, PascalCase for classes

## Configuration

### Profiles (`profiles.yml`)
- Search path: `SAGA_PROFILES_DIR` env var → `./profiles.yml` (repo root) → `.dlt/profiles.yml` (legacy)
- Structure: `profile_name` → `target` → `outputs` → `target_name` → settings
- Required fields per target: `project`, `location`, `environment`, `destination_type`
- Optional fields: `dataset`, `run_as`
- Use environment variable interpolation: `{{ env_var('VAR', 'default') }}`

### Project Config (`saga_project.yml`)
- `config_source:` — where pipeline configs are discovered (type, path)
- `providers:` — provider credentials (e.g. `google_secrets.project_id`, `google_secrets.sheets_secret_name`)
- `pipelines:` — pipeline-level settings (dataset_access, adapter, etc.)

### dlt Performance Defaults (`dlt_saga/defaults.py`)
- Shipped as lowest-priority `DictionaryProvider` via `apply_dlt_defaults()`
- Overridden by user's `.dlt/config.toml` or environment variables
- Includes: normalize workers, load workers, retry settings, parquet config
- `.dlt/config.toml` should only contain user overrides, not framework defaults

### Pipeline Configs (`configs/<type>/<name>.yml`)
- Optional: `enabled: true/false` (master switch, default: true)
- Optional but recommended: `tags: [tag1, tag2]`
- Source-specific config (varies by pipeline type)
- Target config: `write_disposition`, `merge_strategy`, `partition_column`, `cluster_columns`, etc.
- `write_disposition` controls what runs: `append` (ingest only), `append+historize` (both), `historize` (historize-only)
- Historize config: `historize:` section with `snapshot_column`, `track_columns`, `ignore_columns`, `track_deletions`, etc.
- Snapshot date extraction: `snapshot_date_regex` + `snapshot_date_format` (top-level, filesystem only)
- Hierarchical inheritance via `dlt_project.yml` (use `+key:` for merge, `key:` for override)

## Testing
- Create unit tests for core logic (selectors, config parsing, etc.)
- Use mock objects to avoid external dependencies
- Test files: `test_<module>.py` in `tests/unit/` or `tests/integration/`
- Mark tests: `@pytest.mark.unit` or `@pytest.mark.integration`
- Integration tests use DuckDB — no cloud credentials required
- Verify edge cases: empty inputs, invalid selectors, race conditions

## Logging Best Practices
- Use module-level logger: `logger = logging_manager.get_logger(__name__)`
- Log levels:
  - DEBUG: Implementation details, state transitions
  - INFO: User-facing progress updates, successful operations
  - WARNING: Recoverable issues, degraded functionality
  - ERROR: Operation failures, configuration errors
- Include context in log messages: identifiers, counts, timing
- Format timing as: `{duration:.1f}s` (one decimal place)

## Security
- Never include secrets in code or examples.
- Use environment variables or placeholders like `${API_KEY}` or `<API_KEY>`
- Identity impersonation: Always use the `AuthProvider.impersonate()` context manager
- Credentials: Load from environment, profiles, or cloud metadata service — never hardcode

## Performance Considerations
- Use parallel execution where safe (independent pipelines)
- Implement incremental loading where applicable (track state in `_saga_load_info`)
- Proactively prevent race conditions (e.g., dataset creation in BigQuery)
- Use connection pooling for databases
- Profile and log timing for major phases (init, extract, load, finalize)

## dlt Integration
- Use dlt's native sources where available (e.g., `sql_database` for SQL sources)
- Apply hints via adapters: `bigquery_adapter()`, not manual SQL
- Leverage dlt's write dispositions: `append`, `merge`, `replace`
- Use dlt's incremental loading: `dlt.sources.incremental`

## Future-Proofing
- Design for extensibility: new source types, destination types, config sources
- Use abstract base classes and factory patterns
- Keep configuration separate from implementation
- Version control all config changes
