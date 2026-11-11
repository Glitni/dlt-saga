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
- Supports: pipeline names, glob patterns, tag selectors, group selectors, state selectors
- UNION (OR): space-separated or multiple `--select` flags
- INTERSECTION (AND): comma-separated selectors
- Examples:
  - `--select "tag:daily"` - select by tag
  - `--select "group:google_sheets"` - select by group
  - `--select "*sales*"` - glob pattern
  - `--select "tag:daily,group:google_sheets"` - intersection (AND)
  - `--select "tag:daily group:filesystem"` - union (OR)
  - `--select "state:new"` - un-ingested / un-historized pipelines
  - `--select "state:failed"` - last recorded run failed
- EXCLUSION: `--exclude` on every command that has `--select`, same grammar, applied after selection
  (`PipelineSelector.select(..., exclude=)`) — exclusion wins; without `--select` it subtracts from
  every candidate. It warns when it matches no candidate at all (a typo would let the pipeline
  meant to be skipped run), not when it merely misses the selection. A `state:` token in it needs
  the resolver like one in `--select` (`selection_needs_state(select, exclude)`). Run records
  (`select_criteria`) and `RunContext.exclude` carry it (`format_selection`)
- Prefix set is closed (`tag:`, `group:`, `state:`): an unknown prefix raises `SelectorSyntaxError`
  rather than falling through to the name/glob match, where a typo was indistinguishable from a
  valid selector matching nothing (a scheduled run would do nothing and report success). Values
  behind `tag:`/`group:` stay open-ended and still just warn when they match nothing

**State Selectors** (`utility/cli/pipeline_state.py`)
- `state:new` = the target table doesn't exist yet; `state:failed` = the pipeline's most recent
  outcome in `_saga_execution_plans` is `failed`/`abandoned` (30-day lookback, latest row per
  `(pipeline_type, table_name)` — the stored `pipeline_identifier` is a config path and differs
  between a local checkout and a worker container)
- The only selectors that read warehouse state, so they're scoped to the target being run (new in
  dev ≠ new in prod) and need credentials — `build_state_resolver()` returns None when the
  selection has no `state:` token, keeping every other selection offline. `saga validate` passes
  `layer=None` and refuses them
- Newness is judged per **layer**: the ingest target for `saga ingest`, the historized target
  (`historize.factory.resolve_historize_target`) for `saga historize`, either enabled layer for a
  command spanning both. `saga run` selects once per phase, so a pipeline whose raw table exists
  but whose historized table doesn't gets historized without being re-ingested (local and
  `--orchestrate` agree — `_run_orchestrate` re-selects per layer off one shared resolver)
- Newness comes from **table existence** (`Destination.list_tables`, one listing per schema),
  not `_saga_load_info`: a zero-row load records nothing there and a historize-only pipeline never
  writes to it, so both would look permanently new. `list_tables` must raise on anything but a
  missing schema — an empty listing means "select every pipeline here", so a swallowed permission
  error would silently turn `state:new` into a full re-ingest
- Selection must run inside the impersonation scope (`discover_and_select_with_auth` for CLI
  commands not already inside one; impersonation does not nest)

**Execution Modes**
1. Local execution: Run pipelines sequentially or in parallel
2. Orchestrator mode: Create execution plan and trigger remote workers (e.g. Cloud Run Jobs)
3. Worker mode: Execute assigned tasks from an execution plan

**Task-group concurrency** (`utility/task_groups.py`)
- `task_group` co-locates pipelines in one orchestrated task; `task_concurrency` caps how many of a group's pipelines run at once (for sources limiting concurrent requests), in local runs (`Session`) and worker mode alike, for ingest and historize
- Scheduled as `cap` lanes draining the group's queue (`submit_with_group_caps`), so waiting members never hold a pool slot other pipelines could use
- Members must agree on the value (unset counts as a value), checked project-wide like target collisions: `TaskConcurrencyError` (a `ValueError`) at run time in `Session`/`run_orchestrator_mode`, errors in `saga validate`. Only coordinates within one saga process

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
- Example: `adapter: local.api.my_source` → `pipelines/api/my_source/pipeline.py`
- Scaffold new adapters with `saga new adapter <name>` (`new_adapter_command.py`) — generates a convention-following config.py + pipeline.py + starter config and registers the package in `packages.yml` (default dir `./pipelines`, namespace `local`)

**Lifecycle Hooks & Notifiers** (`hooks/`)
- Events: `on_pipeline_start`, `on_pipeline_complete`, `on_pipeline_error` (per pipeline, in the worker thread) and `on_run_complete` (once per command, main thread, carrying a `RunContext`)
- The three per-pipeline events fire from both `Session` and `run_worker_mode` (through `hooks/registry.py`'s `fire_pipeline_*`, which also loads hooks on first fire — a worker builds no `Session`, so nothing else would register them). `on_run_complete` fires from `Session` only: no worker container has a view of a fan-out run, so use `saga notify` for the per-run summary
- `saga run` is two `Session` calls but one command, so `hooks/run_scope.py` coalesces them into a single `on_run_complete`
- Hooks fire only in `hooks.environments` (`HooksConfig`, parsed with the project config: required when hooks are listed; installed entry-point hooks are skipped with a warning while it's unset, since whether one is installed depends on the venv). `saga_project.yml` is shared by every target, so the gate lives in `HookRegistry` (`register(..., environments=)`), matched against the environment the event carries — `RunContext.environment` / `HookContext.environment`, both resolved by `utility/naming.environment_of` (target's `environment`, else `SAGA_ENVIRONMENT`, else `dev`) when the context is built rather than when the event fires, because `saga run` fires after its phases' contexts have closed. Handlers registered in code without it fire everywhere
- Built-in Slack notifier (`hooks/notifiers/slack.py`): `notifications.slack` configures `saga notify`, the notification path. Posting when a command finishes is opt-in through `notifications.slack.on_run_complete` (`environments` required — independent of `hooks.environments` — and `notify_on`); without it nothing is registered. Top-level `notify_on` is a config error pointing to `on_run_complete`; `per_pipeline` (anywhere) is a config error saying it was removed. Webhook as a secret URI (never `{{ env_var() }}`, which bakes it into the execution plan). `notifications.slack.mentions` works at project level and on a pipeline's own config; `notifications.slack.enabled: false` on a pipeline's config mutes it for Slack only — dropped from both digests (failing, recovered and the attempt totals; a run/sweep whose only failures are muted posts nothing), executions still claimed, `saga report` unaffected. The sink applies its own keys: `SweepContext` stays sink-agnostic, `SlackNotifier._without_muted` filters, and `_dispatch` returns the sweep outcome (`quiet` when every failure is muted). `saga notify` reads per-pipeline settings from the *current* discovered config matched by `(pipeline_type, table_name)` (`apply_current_configs`, enabled ∪ disabled sets), falling back to the plan row's `config_json` — which is frozen at plan time and empty for local runs. **Reminder cadence** (`notifications.slack.repeat_after_hours`, project level with per-pipeline override, unset = every sweep reports as before; `saga notify` only): a failure that was already failing at the last sweep is a *reminder*, posted once per interval counted from the streak start and otherwise dropped from the digest; new failures and recoveries always post. Nothing is stored for it: the sweep query reads every terminal row in the window (`reportable` flag + `config_json` only for reportable rows), `_derive_state` gives each outcome `previously_failing` (status of its latest *claimed* row), `streak_started_at`/`streak_attempts` (failed rows since its last completed one), and `read_last_sweep` reads `MAX(swept_at)` from `_saga_notify_log` into `SweepContext.last_swept_at`; `SlackNotifier._reminder_due` does the arithmetic (`floor((now-start)/interval) > max(floor((last-start)/interval), 0)`), missing facts → due. A pipeline failing at the last sweep and passing now is a `recovered` outcome even with no failure in this sweep (previously never reported). **Routing** (`notifications.slack.channel` on a pipeline, string or list; token transport only): the complete set of channels the pipeline posts to, *replacing* the project channel (naming the project channel folds it back into the project digest). `SlackNotifier._route_sweep`/`_route_results` build one digest per distinct channel (`build_sweep_payloads` → `[(channel|None, payload)]`, `_send(payload, channel=)`); the project digest keeps sweep totals minus routed-away outcomes, a routed digest counts only its own; project `mentions` only in the project digest, pipeline mentions on the line everywhere; any channel failing → sweep undelivered (duplicate beats lost). Under a webhook, routing is ignored with one warning; `saga validate` (`_validate_notifications`, `PIPELINE_SLACK_KEYS`) makes it an error and warns on unknown keys in a pipeline's `notifications.slack` (catches `+channel`: the `+` merge works only on top-level keys and one level into a dict)

**Granular Timing Tracking**
- Tracks: setup (constructor + destination/dlt orchestration overhead), extraction, normalize, load, finalization phases
- Reports timing in format: `68.3s total (setup: 15.2s, extract: 25.7s, normalize: 10.2s, load: 32.4s, finalize: 10.0s)`. `extract`/`normalize`/`load` are dlt trace phase timings (subsets of wall-clock); `setup` is the reconciling remainder (constructor + destination client auth, dataset creation, dlt state sync, staging) so the breakdown sums to total.

## Currently Implemented

### Sources
- **google_sheets**: Extract data from Google Sheets with change detection
- **filesystem**: Extract files from GCS/S3/Azure/SFTP/Google Drive/local filesystem with incremental loading. `filesystem_type: gdrive` uses dlt's built-in Drive fsspec (`bucket_name` = folder ID), or with `file_id` fetches one file by ID (`FilesystemClient._file_source` → `_gdrive_file_resource`, lazy-download `FileItemDict` pages; all readers go through `_file_source`, never `readers()`); credentials resolve via `google_api_pool` (`credentials` — dict/JSON string/key path, secret URIs resolved by value — else ADC; `drive.readonly`) and are wrapped as dlt `GcpServiceAccountCredentials`. UTF-8 CSV on `merge`/`replace` (no `csv_include_file_metadata`) is read by saga's own DuckDB reader (`_iter_csv_duckdb`, dict rows through a connection cursor), not dlt's `read_csv_duckdb`, whose relation `fetchmany()` is gone in DuckDB 2.0; append and non-UTF-8 CSV go through pandas. Saga's own code runs on DuckDB 2.0 (on a relation use `fetchall()`/`to_arrow_table()`/`df()`/`pl()`, or query through the connection with `conn.execute(...)`: 2.0 removed `fetchone()`/`fetchmany()`/`execute()` from relations and `fetch_arrow_table()`/`pl()` from connections), but `duckdb<2.0.0` stays capped until dlt supports 2.0 — dlt 1.31's `read_csv_duckdb` and DuckDB Arrow reads still use removed APIs
- **database**: Extract from SQL databases via dlt's `sql_database` source
- **api**: Base HTTP API pipeline with pagination support. Transport is dlt's retrying `requests` `Client` (`dlt.sources.helpers.requests`), built per pipeline from `max_retries`/`timeout`/`retry_backoff_base` with `_is_transient` as the sole retry predicate (so every retry is logged). `_make_request` = the one configured JSON call; `_send_request(method, url, **requests_kwargs)` = the same transport for multi-step adapters (no implicit auth — credentials only go where an adapter attaches them), raising `ApiRequestError`
- **sharepoint**: Extract files from SharePoint

### Bulk loaders
- **native_load** (`adapter: dlt_saga.native_load`): Bypasses dlt extract/normalize and loads Parquet/CSV/JSONL directly into the warehouse via destination-native bulk mechanics (BigQuery external tables → INSERT, Databricks COPY INTO; on BigQuery, incremental and cross-cloud appends stage through a transient transfer table (`_transfer_table`: expires after 24h, same-cloud under the sweepable `__ext_` prefix) so per-file `loaded_rows`, recorded on incremental runs only, are counted from the chunk's rows rather than a scan of the target). Use for >1 000 files or >1 GB per run. Supports `append`, `replace`, `append+historize`, and `replace+historize` write dispositions. File-level dedup across runs requires `incremental: true` (opt-in, mirrors dlt's split between write_disposition and `dlt.sources.incremental`). `replace` rewrites the target table on every run (no state needed). Fully compatible with the historize layer — no extra config needed. State tracked in `_saga_native_load_log` (only when `incremental: true`); the log table and its `_latest` view are created once, gated on `table_exists`, never re-issued per run — every pipeline in a schema shares them and BigQuery rate-limits table updates per table, so a task group fanning out spent up to a minute per pipeline in the client's job-retry loop on the view rewrite alone. Supports BigQuery (`gs://`, `s3://`) and Databricks (`gs://`, `abfss://`). **S3 on BigQuery** goes through a BigQuery Omni cross-cloud connection: set `source_connection` (`<omni-region>.<id>`, e.g. `aws-eu-west-1.my-conn`) plus AWS listing credentials (`aws_access_key_id`/`aws_secret_access_key` as secret URIs, `aws_region`); the external table read runs in the Omni region, then a cross-cloud CTAS materializes a destination-region transfer table from which the real target is created/appended **in-region** (a single statement can't span the Omni + destination regions), so partitioning/clustering on the target work normally. Requires S3↔BigQuery region colocation (EU S3 via `aws-eu-west-1` → EU BigQuery). Large loads auto-split by cumulative file size (`load_batch_bytes`) to stay under the 60 GiB cross-cloud cap. Chunks are balanced (files spread evenly over the fewest chunks `load_batch_size`/`load_batch_bytes` allow; when loading in parallel the count is raised to a multiple of the concurrency, floored at `_MIN_PARALLEL_CHUNK_FILES` (100) or `load_batch_size` if smaller — `load_batch_size` is always the upper bound); when a run creates/replaces the target, chunk 1 runs alone first — a 10-file seed chunk for self-describing formats (`SELF_DESCRIBING_FILE_TYPES`) or an explicit external schema, since later chunks add missing columns — and otherwise every chunk runs in parallel; up to `load_concurrency` (default 4) at a time where the destination opts in (`Destination.supports_concurrent_native_load` — BigQuery yes, with column reconciliation serialized per target; Databricks no, one shared SQL connection). Partition listings run in parallel when the storage client sets `supports_concurrent_listing` (GCS, S3; not ADLS, which lists through the Databricks connection). No client-side timeouts on BigQuery job waits: giving up on a running INSERT records its files failed while the job still commits. An INSERT BigQuery aborted over a concurrent update is retried (`_run_insert`, only when the job is DONE with that error — never on a transport error, after which it may have committed). `Destination.native_load_max_files_per_chunk` caps `load_batch_size` at pipeline build (Databricks: 1000, the COPY INTO `FILES` limit). `file_pattern` is matched against each file's path relative to `source_uri` with fsspec glob semantics (`*` stays within one path segment, `**` recurses) — identical to the `filesystem` adapter's `file_glob`, so configs migrate between the two without silently widening the input set; a non-recursive pattern also prunes the GCS/S3 listing server-side, and a zero-match run probes the subfolders and warns with the `**/` form to use (`pipelines/native_load/storage/matching.py`). Column names always normalized to snake_case (BigQuery); explicit type hints via `columns:`; date-partition filtering via `partition_prefix_pattern` (requires `incremental: true`). **Headerless CSV/TSV** (e.g. Snowplow enriched): set `autodetect_schema: false` + an ordered `columns:` block — it becomes the positional external-table schema (all read as STRING, then SAFE_CAST to the declared types), since autodetect mis-types headerless columns non-deterministically. Databricks supports external Delta/Iceberg/DeltaUniform tables via `table_format` + `target_location`.

### Destinations
- **bigquery**: Full implementation with partitioning, clustering, IAM management. Saga's own clients come from `utility/gcp/client_pool.py`, which gives every request a socket timeout (`BIGQUERY_REQUEST_TIMEOUT_SECONDS`; the library default is none, so a silently dead connection blocked a thread for good and a worker waiting on it never finished). That bounds one HTTP request, not a job — `result()` waits stay unbounded on purpose. Every BigQuery job a saga process issues is labelled: the destination-neutral job context (`utility/job_labels.py`: `app`, `command` from the log context — the CLI subcommand process-wide, `ingest`/`historize` inside a pipeline — `pipeline`, `environment`, `execution_id`/`task_index`, plus `user_job_labels()` = the profile target's `job_labels` with `SAGA_JOB_LABELS` merged over it, which is how the orchestrator forwards its resolved labels to Cloud Run workers) formatted by `utility/gcp/job_labels.py` as `app=dlt-saga` + `saga-*` keys with BigQuery's value cleaning. `job_labels` is a `BigQueryDestinationConfig` profile field; each destination that tags jobs implements `DestinationConfig.validate_job_labels` (base returns None = unsupported, which `saga validate` warns about). Pooled clients resolve the labels per `query` call from the log context (so a pool thread moving to its next pipeline relabels; native_load's chunk pool copies the context into its threads); dlt's client is opened on dlt's own pool threads with no context, so `create_dlt_destination` captures the labels on the pipeline's thread and sets them as the native client's default job configs, which the library merges into every job dlt issues. Datasets saga creates are labelled too (`utility/gcp/dataset_labels.py`: `app=dlt-saga` as a default under the profile target's `dataset_labels`, a separate key from `job_labels` — `app` is overridable there, reserved on jobs; `saga-*` reserved on both), for storage-cost attribution — at creation only, at all three create sites (`_create_dataset_with_access`, `ensure_schema_exists`, `_ensure_dataset`); an existing dataset is never relabelled, and one dlt creates itself (the fallback when saga's pre-create fails) gets none
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
saga historize --select "group:finance" --exclude finance__legacy  # all but one

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
saga report -o gs://bucket/r.html --summary --stale-after-minutes 90  # + r.json
```

`--summary` (opt-in) writes a compact machine-readable status summary beside the report (`.html` → `.json`, same upload path). It counts every pipeline but lists only `needs_attention` (failing or overdue — non-empty always means a problem now), `recovered` (with `recovered_at`, so a consumer lets it fade) and `no_runs` (informational; list names match their state and `counts` key), so its size tracks problems, not pipeline count. `overdue_at` per entry and top-level `next_overdue_at` (earliest among pipelines not in `needs_attention`) are absolute, so consumers check overdue live without re-implementing the rule. Verdicts per pipeline (`ok`/`failing`/`recovered`/`no_runs`/`disabled`/`unknown`, failure streak, last success, `is_overdue` against the usual gap — the 90th-percentile gap between attempts, not the median, which makes a pipeline run twice a morning "overdue" every afternoon) are computed once in `report/status.py` from the execution-plan rows the collector already reads — no extra query — and the HTML dashboard's health headline reads the same verdicts, so page and summary can't disagree. Failed statuses are `pipeline_state.FAILED_STATUSES` (`abandoned` counts), keyed by `(pipeline_type, table_name)` (`collector.pipeline_key`). Plan rows store no pipeline name: the collector resolves `pipeline_name`/`schema_name` from every discovered config by that key (the plans table holds runs outside the selection too) — never rebuilds it as `<group>__<table>`, which doubles the group under dev naming; an unconfigured pipeline keeps its `table_name`. An unreadable plans table (anything but missing) sets `complete: false` and every pipeline `unknown` — never `ok`.

### Notify Command
```bash
saga notify                                  # sweep: everything finished and unreported
saga notify --since-days 1 --target prod
saga notify --execution-id "$EXECUTION_ID"   # chained straight after a run
saga notify --dry-run                        # preview; sends nothing, claims nothing
```

Reports run outcomes to the configured notifier by **reading recorded state**, not by hooking into a run. Every lifecycle hook fires from `Session`, and `run_worker_mode` executes pipelines without going through it — so on a fan-out deployment hooks post nothing, and a notification assembled inside one worker could only describe that worker's slice. `notify.py` reads `_saga_execution_plans` + `_saga_executions`, which every invocation style already writes (local, `saga plan` + workers, `--orchestrate`).

- Selection is on **unreported** (`notified_at IS NULL` on `_saga_executions`), not recent: a late sweep misses nothing, overlapping schedules duplicate nothing. `--since-days` (default 7) is only a bound against reporting a large backlog.
- Pipelines are identified by `(pipeline_type, table_name)` — the same key `state:` selectors use. `pipeline_identifier` is a config path that differs between a local checkout and a worker container, and `record_local_run` stores an empty `config_json`.
- Scoped by **environment**, never target *name*: `target` stores whatever string the caller passed, and two targets can describe one warehouse differing only in `run_as`.
- An execution with non-terminal rows is skipped as in-flight, but only for 24h (`PLANS_STALE_HOURS`); past that a dangling task is a crash, not work in progress, or it would be excluded until `saga maintenance` ran.
- Quiet when nothing failed. A pipeline that failed and has since recovered is still reported, or one failing a run in five would never surface.
- Every real sweep appends a row to `_saga_notify_log` (`record_sweep`, outcome `quiet`/`delivered`/`undelivered`) so a notifier that stopped running is detectable as a query — silence alone can't distinguish it from all-green. `--execution-id` and `--dry-run` don't write; a write failure warns rather than failing the command.

### AI Setup Command
```bash
saga ai-setup                                       # Generate saga_ai_context.md
```

Writes `saga_ai_context.md` to the project root with framework patterns and guidance for AI coding assistants. Content hash is embedded; `saga doctor` warns if outdated after a package upgrade.

### Generate Schemas Command
```bash
saga generate-schemas                               # Generate schemas + link configs
saga generate-schemas --no-link                     # Only write schema files (CI-safe)
saga generate-schemas -o my_schemas/                # Custom output directory
saga generate-schemas --sync                        # Also delete stale *.json in the output dir
```

Generates JSON schemas for all pipeline config classes (built-in + `packages.yml`) plus `saga_project.yml`, `profiles.yml`, `packages.yml`. By default it then **links** every discovered config file to its adapter's schema by writing/updating a `# yaml-language-server: $schema=...` modeline at the top of the file. Linking is per-file (resolves each config's effective `adapter`, including inheritance from `saga_project.yml`/folder defaults), editor-agnostic, and idempotent — safe to run repeatedly or as a pre-commit hook. Generating before linking guarantees brand-new pipelines have a schema before any link is written. Use `--no-link` for generate-only runs (e.g. CI verifying schemas, or `git diff --exit-code` to catch unlinked/stale configs). Adapter→schema resolution and the linker live in `utility/generate_schemas.py` (`schema_filename_for_adapter`) and `utility/link_schemas.py`. Filenames come from one function, `schema_filename_for_config_class(cls)`, which generator and linker share: snake_cased class name minus `Config`, prefixed `<namespace>__` for any non-`dlt_saga` namespace — two packages each defining e.g. `GoogleAdManagerConfig` would otherwise write the same file, last one silently winning, with both adapters' configs linked to it. A filename two distinct classes still map to is an `[ERROR]` (exit 1), never an overwrite. `--sync` deletes `*.json` in the output dir not produced by this run, and is skipped when any schema failed (or a failed module's last good schema would be deleted).

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
    path: ./pipelines             # Python package with pipeline.py modules
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
  cluster_columns: [_dlt_valid_to, _dlt_valid_from, id]  # default: the two validity columns
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
- Jinja2 templating is applied to config values at load time (see `utility/templating.py`): `{{ env_var('VAR') }}`, `{{ env_var('VAR', 'default') }}`, plus filters/nested calls, e.g. `{{ env_var('GCP_DATASET') | replace('-', '_') }}`. Sandboxed, rendered before the hierarchical merge across all three surfaces (`profiles.yml`, `saga_project.yml`, pipeline configs). Missing var with no default → `""`. Use `{% raw %}…{% endraw %}` to keep a literal `{{`. Secret URIs (`googlesecretmanager::`, `azurekeyvault::`) are plain strings, untouched by rendering and resolved later at runtime.
- `write_disposition` controls what commands run: `append+historize` enables both, `historize` enables historize-only
- **Config error vs run failure**: classified by *phase*, not exception type. A `ValueError` while preparing a pipeline — adapter resolution + construction (`execute_pipeline` re-raises it as `PipelineConfigError`), the historize factory, `HistorizeRunner._validate_config` — means the run never started: `config_error=True`, not recorded (no run record, no historize log entry, no destination opened for it). Anything raised by the run itself is recorded, `ValueError` included (`ApiRequestError`, decode errors, NULL-PK / missing-table / config-changed checks) — just without a traceback. Worker mode records every failure
- **Row guard** (`pipelines/row_guard.py`): `min_rows: N` abandons a load carrying fewer rows, *before* anything is written — dlt emits a load job for a zero-row `replace`, so the truncate/swap runs and empties the target behind a green run; scd2 retires keys absent from a partial batch. The count comes from dlt's normalize step (destinations route the run through `Destination.execute_dlt_run`), and the rejected package is discarded or the next run would load it. `MinRowsNotMetError` is a recorded run failure, logged without a traceback. `native_load` bypasses dlt, so it counts the source files instead (BigQuery only)
- `PipelineConfig.ingest_enabled` — derived from `write_disposition` (True when base is append/merge/replace)
- `PipelineConfig.historize_enabled` — derived from `write_disposition` (True when contains "historize")
- `PipelineConfig.dlt_write_disposition` — strips `+historize` suffix for dlt
- `enabled: false` disables both ingest and historize (master switch)
- **Target-collision guard** (`utility/collisions.py`): a config must map 1:1 to a destination table. Default naming guarantees this; overrides (explicit `schema_name:`, a custom `naming_module`) can break it. `check_target_collisions()` groups selected configs by resolved `(schema_name, normalize_identifier(table_name))` — ingest layer — and by the resolved historized target (via `historize.factory.resolve_historize_target`, extracted so the runner and the guard agree) — and raises `TargetCollisionError` (a `ValueError`) before any warehouse work. Enforced at run time in `Session._run_ingest`/`_run_historize`/`_run_both` and the orchestrator `run_orchestrator_mode`; surfaced offline over the whole project by `saga validate` (`validate.validate_pipeline_configs` → `_validate_collisions`, dev ∪ prod).

### Column & Table Documentation (`persist_docs`)
- Config surface (`pipelines/target/config.py`): table-level `description` + `classification` (list of labels), and per-column `columns.<col>.{description, classification}`. `classification` is data governance and is **distinct from top-level `tags`** (pipeline selection) — tags never reach the warehouse, classification never affects selection.
- `classification` is encoded into the description as a single-line, sorted, sentinel block `[saga:classification=…]` (helpers in `utility/column_docs.py`), so it rides dlt's native description support on every destination — no native tag/policy-tag dependency.
- `persist_docs: {table, columns}` gates writes; defaults `table: true, columns: false` (per-column writes are O(columns) and a per-run cost on destinations without batch column-comment DDL, e.g. Databricks). Opt in per project via `+persist_docs: {columns: true}` in `saga_project.yml`. Normalized via `PersistDocs.from_value` (accepts bool / dict / None).
- **Reconcile** (`Destination.reconcile_descriptions()` in `destinations/base.py`): reads current descriptions, diffs, writes only changes — idempotent (unchanged config = one metadata read, no writes). dlt writes descriptions only when a column is *created*, so the reconcile is what propagates edits to existing tables, covers DuckDB (dlt emits none), and documents historized tables (built by saga SQL — dlt never sees them). Invoked in `BasePipeline._reconcile_descriptions` (ingest finalize) and `HistorizeRunner._reconcile_descriptions`; both best-effort.
- Per-destination: BigQuery **overrides** `reconcile_descriptions` for one `get_table` + one `update_table` (schema + table description patched together); Databricks/DuckDB implement the `get_/set_column_descriptions` + table primitives and use the base orchestrator. Desired strings reuse `TargetConfig.get_column_description_map()` / `resolve_table_description()`, so they're byte-identical to what dlt wrote at create (guarantees idempotency). Config keys matched to stored columns via `utility/naming.normalize_identifier` (dlt snake_case).

### Historize Layer (`historize/`)
- `config.py`: `HistorizeConfig` dataclass with `from_dict()` factory — source location fields (`source_database`, `source_schema`, `source_table`) are top-level, not nested under `historize:`
- `state.py`: `HistorizeStateManager` — manages `_saga_historize_log` table (lazy creation), snapshot discovery, config fingerprint for change detection. Incremental discovery is watermark-based plus **late-arrival detection** (`find_late_arrivals`): rows with `snapshot <= watermark` but `arrival_column` (default `_dlt_ingested_at`) newer than the last run's start trigger a rewind-and-replay from the earliest late snapshot (reuses the re-entrancy rollback prefix; baseline re-anchored below the boundary). Tri-state `historize.detect_late_arrivals`: unset (default) = detect + warn (suggests the key), `true` = detect + rewind-and-replay, `false` = skip detection. Gated to append/historize dispositions (replace/merge raws don't retain history), skipped when `snapshot_column == arrival_column` (monotonic by construction — the default config pays nothing) or the column is absent. A retention guard (`_target_snapshots_missing_from_source`) refuses the rewind when raw no longer contains every historized snapshot in the replay window (partition expiration) — warn + watermark-only fallback instead of truncating history. `late_arrival_window_days` bounds accepted lateness (older late snapshots are warned about, never auto-replayed)
- `sql.py`: `HistorizeSqlBuilder` — generates full-reprocess and incremental SQL; deletions are separate marker rows; incremental uses LEAD for within-batch sequencing and MERGE to close existing open records. A snapshot batch is scoped by range (`snapshot_range_filter`), never an inlined list of every value — statement size must not scale with backlog size, since a snapshot column stamped per row rather than per run reaches tens of thousands of distinct values and overran BigQuery's 1 MB query-length limit. The same helper bounds the two post-run stats queries in `runner.py`
- `runner.py`: `HistorizeRunner` — orchestrates discovery → mode selection → SQL execution → logging, with phase-level timing (init/setup/execute)
- **Strategy** (`historize.strategy`): `scd2` (default) or `scd1` — latest version per key, output = PK + value columns + `valid_from` (+ `is_deleted` with `track_deletions`). `HistorizeSqlBuilder.build_scd1_sql` is one script for every mode (full/incremental/partial/late-arrival replay differ only in source scope; full reprocess skips the stored side and INSERTs): batch rows + the stored row of each touched key are ranked by `order_by` (default `[snapshot_column]`, snapshot appended as tiebreaker unless listed; NULLS LAST; batch wins exact ties), and the winner is written unless the stored row is in its stretch of identical values (value hash; a key's outranked batch rows within one snapshot are dropped first, or they'd split that stretch) — so an unchanged key is never rewritten and `valid_from` = start of that stretch. Ranking against the stored row makes it order-independent and re-entrant: no rollback prefix, no clone-and-swap for partial refresh, no rewind/retention guard for late arrivals. `order_by` columns must be carried into the target (`_guard_order_by`). `track_columns`/`ignore_columns`/renamed `valid_to_column` are rejected under scd1, as are `merge_key`/renamed `is_deleted_column` without deletions (`track_deletions` is `Optional`, resolved per strategy and delivery in `__post_init__`). scd1 deletions (opt-in `track_deletions: true`; needs `delivery: full`, and `order_by` must lead with the snapshot column — further columns only break ties within a snapshot): `_scd1_deletion_ctes` stamps every non-deleted stored key at `SCD1_BASELINE_SNAPSHOT` ("present before this batch"), finds disappearances against the per-merge_key snapshot sequence, and adds each as a candidate version (last-seen values, `is_deleted` TRUE) to the same ranking — one the key doesn't come back from within the batch ranks above everything (`SCD1_FINAL_DELETION_RANK`, via the `_saga_rank_at` column), or a stored row dated inside a refreshed/replayed window would outrank it — the deleted flag counts toward change detection, and the row is kept (soft) so a late older version can't re-insert a deleted key. With deletions every non-deleted stored key is read as baseline, but only keys in the batch or disappearing from it are ranked (`touched_keys`). The disappearance rule itself has one definition, `_disappearance_ctes`, shared by scd2 full/incremental and scd1; `order_by` is rejected under scd2. `strategy`/`order_by` are fingerprinted, and `decode_fingerprint` fills `strategy: scd2` into pre-existing fingerprints so a switch still demands `--full-refresh`
- **Delivery** (`historize.delivery`): `full` (default) = each snapshot value is a complete copy of the source, so absence means deletion/gap; `incremental` = each holds only the records sent in it, so absence means nothing. Saga can't infer it — it's declared. Under `incremental`, `HistorizeSqlBuilder._snapshot_sequence_ctes` / `_gap_detection_sql` emit nothing (no distinct-snapshot scan, no gap-driven reappearance), `track_deletions` resolves to `false`, and `track_deletions: true` / `merge_key` are rejected (`_validate_delivery_options`). Fingerprinted; `decode_fingerprint` fills `delivery: full` into older fingerprints
- **JSON columns** (`historize.json_columns`: output name → `{column, path, data_type}`, `JsonColumn` in `config.py`): when set, every source read goes through one derived relation (`sql.build_source_relation`: `(SELECT *, <expr> AS name … FROM <table>)`), held as `source_relation` on the runner and SQL builder and passed as the state manager's `source_table_id`; `source_table_id` on the runner/builder stays the identity for logs, state rows and messages. Unset → the relation is the table id, so SQL is unchanged. The relation is bound in `_execute_run` right after `_guard_source_columns` (`_bind_source_relation` → `HistorizeSqlBuilder.set_source_relation`), not in `__init__`: an entry's expression depends on the catalog type of the column it reads (BigQuery's `JSON_QUERY` returns the input's type, so a `JSON` column and a `STRING` holding JSON need different SQL), and `__init__` hasn't read the catalog. Derived columns are additive (the JSON column is still carried) and count as source columns everywhere (`_source_columns()` = `_table_columns()` + derived). `data_type` is `text` (the value as text: a scalar unquoted, an object/array as its JSON text), a scalar type converting that text, or `json` (the value kept as a document in `dlt_type_to_native("json")`: BigQuery/DuckDB `JSON`, Databricks `STRING`). The destination primitives (`json_extract_text` / `json_extract_document`, both taking the column's catalog type; no portable default, like `hash_expression`) are separate from `json_extract_scalar`, which path-based filters keep using (`JSON_VALUE`, NULL for a document). `text` never drops a document, so a scalar type pointed at an object fails its cast and is warned about rather than silently NULL; `text`/`json` get no `cast_checks`. A `json` entry (`JsonColumn.is_document`) is a value column only — refused as PK (`_validate_key_columns`, in `validate()` because the PK may be inherited), snapshot/arrival/order_by/partition/cluster column and in a path-less filter other than `is_null`/`is_not_null` (`_validate_json_document_columns`), since BigQuery `JSON` has no equality or clustering. Values that are present but don't convert are warned about, folded into incremental discovery (`cast_checks`) or, on full/partial refreshes, one aggregate probe (`build_failed_cast_probe_sql`). Rejected: pseudo-column `snapshot_column`/`arrival_column` (a subquery drops pseudo-columns), output names shadowing source/system/destination-reserved columns, `column` not a physical source column. Schema evolution treats them as source columns: an added entry goes through `_reconcile_target_columns` (`_source_column_type` gives a derived column `dlt_type_to_native(data_type)`, the type the CTAS created it with), a removed one is read as a typed NULL like a dropped column and is recorded in the log's `source_columns` so it warns once, and `_guard_column_types` compares a derived column's declared type with the target's catalog type through `Destination.normalize_type_name` (DuckDB aliases `TIMESTAMP WITH TIME ZONE` → `TIMESTAMPTZ`), so an entry removed and re-added under its name with another type is refused like a retyped source column. The failed-cast probe is bounded by `--historize-from` (`build_failed_cast_probe_sql(snapshot_from=)`)
- Coexists with dlt's built-in SCD2 (`merge_strategy: scd2`) — no conflict
- `_dlt_ingested_at` column injected by `BasePipeline._inject_ingested_at()` for ALL `append`, `replace`, and plain `merge` pipelines (handles both dict rows and PyArrow tables); excluded only for `scd2` (which versions by row-content hash — a per-run timestamp would open a spurious version every run). On `merge` it's a last-upserted-per-key stamp: merge is upsert-by-key with no content comparison, so each row touched this run carries the run timestamp and untouched keys keep their prior value (bare/delete-insert/upsert/insert-only all qualify; only scd2 is gated off via `merge_strategy`). This makes `replace+historize` work — each `replace` run overwrites the staging table with one snapshot stamped at run time. Auto-clustering by `_dlt_ingested_at` is append-only (a replace table holds a single ~constant timestamp; a merge table's stamps are scattered per key). Caveat: a `--full-refresh` historize over a `replace` source rebuilds from only the latest snapshot — history accrues going forward, run by run.
- `_dlt_ingested_at` 3-tier resolution: (1) regex from file path, (2) file modification date, (3) extraction timestamp
- `_dlt_is_deleted` boolean column: deletions produce a **separate marker row** (`_dlt_is_deleted = TRUE`, `_dlt_valid_to = NULL`) rather than flagging the closed row; closed change rows always have `_dlt_is_deleted = FALSE`; deletion markers are closed (`_dlt_valid_to` set) when the key reappears
- File metadata (`_dlt_source_file_name`, `_dlt_source_modification_date`) auto-injected for append-mode filesystem pipelines only (not for merge/SCD2 to avoid false change detection)
- `snapshot_date_regex` + `snapshot_date_format`: top-level config fields for extracting snapshot dates from file paths
- **Excluding columns** (`historize.exclude_columns`): source columns left out of the historized table and the hash (`select_value_columns` / `_get_hash_columns`), under both strategies — unlike `ignore_columns`, which keeps the column in the output. Rejected for anything the target must carry (PK in `validate()`, the rest in `_validate_exclude_columns`: snapshot, `order_by`, partition/cluster, `track_columns`/`ignore_columns` overlap, `json_columns` output names; a `json_columns` *source* column may be excluded). A `DETECTION_KEYS` member: adding is `narrowed` (forward-only), removing blocks. On an existing target an excluded column falls into `_reconcile_target_columns`'s absent set like a source-dropped column (kept with history, read as typed NULL) but without the drop warning and out of the hash, so excluding opens no versions — stops collecting, not retaining; `--full-refresh` or a hand `DROP COLUMN` removes it
- Config fingerprint stored in log (`HistorizeStateManager.compute_fingerprint`) — detects changes to `primary_key`, `snapshot_column`, `strategy`, `order_by`, `delivery` (scd2 only), `track_columns`, `ignore_columns`, `exclude_columns`, `track_deletions`, `merge_key`, `filters`, `json_columns` (per output name → `[column, path, data_type]`; `_json_column_redefined` blocks only on an entry present on both sides with another definition — the stored column would change meaning under one name — while an entry on one side only is schema evolution; `FingerprintChanges.describe` lists mapping keys per differing entry), `table_format` and the output column names, and prompts for `saga historize --full-refresh`. `fingerprint_changes()` splits differing keys into `blocking` and `narrowed`: a `track_columns`/`ignore_columns` edit under which the hashed column set can only have shrunk (`_detection_narrowed`, decided from config alone — unset `track_columns` means "every value column") is forward-only, logged at info by `_check_config_changes` and never refused, since stored versions stay correct and the hash is recomputed from the open row each run; widening still blocks. `config_changed()` is the blocking-only boolean
- **Schema evolution** (`HistorizeRunner._reconcile_target_columns`, via `_prepare_existing_target`): every non-full run reconciles the existing target against the source's columns before writing — source schema drift is not a config change and never trips the fingerprint prompt. Added column → `Destination.add_column` with the source's catalog type (earlier rows NULL; unknown → known opens a version, decision made deliberately — no per-column state, matches the data). Dropped column → kept on the target with its history and read from the source as `CAST(NULL AS <target type>)` through `HistorizeSqlBuilder.set_absent_source_columns` / `_source_cols_sql` (the only two source-side column lists: scd2 `source_rows`, scd1 `batch`), so the hash sees the NULL and both strategies, incremental, partial (reconciled before the clone; the staging builder takes the main builder's `absent_source_columns`) and late-arrival replay get it from one mechanism; no subquery, so pseudo-column snapshots keep working. Symmetric with add, and identical to what a dlt-ingested raw already produces when a column is dropped upstream (dlt never drops/retypes columns — type conflicts become `col__v_text` variant columns — so drop/retype only arise on `write_disposition: historize` external tables). Type change → `ValueError` before any write, comparing catalog spellings from the same `columns_query` (the target was CTAS'd from the source); widening is refused too, since it changes the string-serialised hash of every row. **Nested fields** (`historize/column_types.py`): when spellings differ and `Destination.supports_nested_evolution()` (BigQuery, DuckDB; not Databricks), both are parsed (`parse_type`: BigQuery `STRUCT<a T>`/`ARRAY<>`, DuckDB `STRUCT(a T)[]`, Databricks `struct<a:t>`) and `reconcile_types` splits the difference into `added`/`removed` dotted paths with a `merged` type (target fields in target order, new ones appended) or a `conflict` (a retyped nested scalar, a struct where a scalar was) — the conflict is refused with its path, the rest is drift: `_reconcile_nested_fields` widens the target via `Destination.widen_column_type` (BigQuery: schema patch through the API keeping existing `SchemaField`s; DuckDB: `ALTER COLUMN … SET DATA TYPE … USING`) and sets `HistorizeSqlBuilder.set_reshaped_source_columns` so `_source_cols_sql` reads the column through `Destination.reshape_expression` (BigQuery `STRUCT(x.a AS a, CAST(NULL AS T) AS b)` with every field name backtick-quoted / ordered `ARRAY(SELECT … FROM UNNEST … WITH OFFSET)`, DuckDB `struct_pack`/`list_transform`), which puts fields in the target's order and NULL-fills removed ones — safe because the hash is computed at run time on both sides and serialises a struct field by field, so a widened struct whose new field is NULL on both sides hashes the same (unlike scalar widening). Key columns are never reconciled this way. The log's `source_columns` records nested paths (`col.field`) so a removed field warns once. Unparseable spellings fall back to the spelling comparison. Each log row records the source's column names (`source_columns`, added lazily to pre-existing log tables on the first failed INSERT — `write_log_entry` retries once after `_add_missing_log_columns`; never detected by error text, DuckDB's missing-column error matches `looks_like_missing_table`), so a dropped column warns the first run it is missing and logs debug after. Databricks `columns_query` returns `full_data_type` (`decimal(12,2)`, not `DECIMAL`) because `add_column` recreates the column from it
- **Table format**: historized table format resolved via 5-level chain: `pipeline.historize.table_format` → `pipeline.table_format` → `profile.historize.table_format` → `profile.table_format` → `"native"`. BigQuery supports `native` and `iceberg` (BigLake). Databricks supports `native` (=delta), `delta`, `iceberg`, `delta_uniform`. BigLake Iceberg requires `storage_path`. Profile-level `historize.table_format` / `historize.storage_path` keys override ingest-layer values for historized tables only.
- **Historize placement** (`saga_project.yml` → `historize.placement`): `table_suffix` (default) keeps historized table in source dataset with `_historized` name suffix; `schema_suffix` moves it to a parallel dataset `<source_dataset>_historized` with unchanged table name. Resolved via `resolve_historized_target()` in `naming.py`. `placement` is effectively write-once — changing it after first run orphans existing tables. Per-pipeline `historize.output_dataset` / `historize.output_table` always override the project-level strategy.
- **Doc reconcile**: after each successful run `HistorizeRunner._reconcile_descriptions()` documents the historized table (its only source of descriptions — dlt never touches it). Inherits the pipeline's top-level `description`/`classification`/`columns`; override for the historized table via `historize.{description, classification, columns, persist_docs}` (`columns` merges per column, per key). SCD2 system columns (`valid_from`/`valid_to`/`is_deleted`) get canned descriptions. Gated by `persist_docs` (see Column & Table Documentation above).

### CLI and UX
- Single `saga` entry point with subcommands: `list`, `ingest`, `historize`, `run`, `update-access`, `report`, `notify`, `maintenance`, `validate`, `doctor`
- Use dbt-style conventions where applicable (profiles, selectors, targets)
- `saga list` with `--resource-type` filter for filtered listing
- Use `--verbose` / `-v` for debug logging (`setup_logging()` in `utility/cli/common.py`, also enabled by `SAGA_DEBUG_LOGGING=true`). It widens only what reaches the **terminal** — the file handler in `logs/` is always at DEBUG, so a non-verbose run still captures full detail
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
- Use environment variable interpolation: `{{ env_var('VAR', 'default') }}` (full Jinja2 — filters and nested calls supported, e.g. `{{ env_var('GCP_DATASET') | replace('-', '_') }}`)

### Project Config (`saga_project.yml`)
- `config_source:` — where pipeline configs are discovered (type, path)
- `providers:` — provider credentials (e.g. `google_secrets.project_id`; `google_secrets.sheets_secret_name` is deprecated in favour of `credentials` on google_sheets pipelines)
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
- Hierarchical inheritance via the `pipelines:` section of `saga_project.yml` (use `+key:` for merge, `key:` for override)

## Testing
- Create unit tests for core logic (selectors, config parsing, etc.)
- Use mock objects to avoid external dependencies
- Test files: `test_<module>.py` in `tests/unit/` or `tests/integration/`
- Mark tests: `@pytest.mark.unit` or `@pytest.mark.integration`
- Integration tests use DuckDB — no cloud credentials required
- Verify edge cases: empty inputs, invalid selectors, race conditions

## Logging Best Practices
- Use module-level logger: `logger = logging.getLogger(__name__)` and nothing else — handlers, formatters and filters are attached once by `configure_cli_logging()` (`utility/cli/logging.py`). Use `PrefixedLoggerAdapter` when a pipeline needs a `[1/N]` prefix.
- Log levels:
  - DEBUG: Implementation details, state transitions
  - INFO: User-facing progress updates, successful operations
  - WARNING: Recoverable issues, degraded functionality
  - ERROR: Operation failures, configuration errors
- Include context in log messages: identifiers, counts, timing
- Format timing as: `{duration:.1f}s` (one decimal place)
- `extra=FILE_ONLY` (`utility/cli/logging.py`) sends a record to the log file but not the console — for text the terminal already shows another way. The end-of-run failure summary uses it: it's echoed to stderr (immune to hosts silencing loggers) and logged `FILE_ONLY`, so the log file says why a run failed even for failures deliberately not logged inline. Under the JSON console format the summary is plain log records instead (text in the stream would break a collector)
- Console format is `text` or `json` (`SAGA_LOG_FORMAT`, or the app-level `saga --log-format json <command>` set in `_app_callback` via `set_console_format()` — not per subcommand like `--verbose`, since it shapes the whole process's output and means nothing to interactive commands); the file handler is always text. JSON records are `time`/`severity`/`message`/`logger` plus the **log context**: `log_context(pipeline=, command=)` is bound around each pipeline's execution in `Session` and `run_modes` (in the worker thread, so dlt's and `requests`' records inside the run carry it too — a ContextVar, reset on exit so a pool thread doesn't leak it into its next task); `bind_log_context(execution_id=, task_index=)` is process-wide, set in worker/orchestrator/`saga plan`, and `_app_callback` binds `command=` to the CLI subcommand process-wide (the per-pipeline value wins inside a run). `_LogContextFilter` stamps both onto every record; only the JSON formatter emits them
- dlt's own `dlt` logger (`propagate=False`, own stderr handler, re-created on every runtime init reusing the first handler it finds) is routed through saga's root handlers by `_DltLogBridge`, installed in `configure_cli_logging()` — so dlt records get saga's format, redaction, noise filters and the log file, and never appear as bare text inside the JSON stream

## Security
- Never include secrets in code or examples.
- Use placeholders like `<API_KEY>` in examples; in configs reference secrets via a secret URI (`env_secret::API_KEY`, `googlesecretmanager::…`, `azurekeyvault::…`) — not `{{ env_var() }}`, which renders at load time and would bake the value into the execution plan
- Redaction is a log-handler filter, so text that leaves the process any other way must call `redact()` itself — before truncating, or a secret cut in half no longer matches. Failure messages do so at `PipelineResult.error`, `_format_run_error`, and the plans-table writes (`ExecutionPlanManager`), which is what the Slack notifier posts from; a new outbound channel (stderr echo, warehouse column, notifier) needs the same
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
- dlt recognises a pandas/polars/pyarrow item by reading `sys.modules` (`dlt.common.libs`), which bypasses the import lock: a thread midway through a lazy `import pandas` (filesystem CSV reader, database client return types, any adapter) leaves a half-built module another pipeline's extract reads as `partially initialized module 'pandas'`. `utility/dataframe_libs.preload_dataframe_libs()` imports all three on the launching thread before every ingest pool (`Session._execute_pipelines_tracked`, worker `_execute_worker_ingest`); lazy imports inside pipelines are fine, they're no-ops after it

## Future-Proofing
- Design for extensibility: new source types, destination types, config sources
- Use abstract base classes and factory patterns
- Keep configuration separate from implementation
- Version control all config changes
