# CLI Reference

All commands share common options: `--select`, `--verbose`, `--profile`, `--target`. See [Logging & debugging](#logging--debugging) for the on-disk debug log written for every local run.

---

## saga list

List configured pipelines.

```bash
saga list [OPTIONS]
```

| Option | Description |
|--------|-------------|
| `-s, --select TEXT` | Selector expression(s) |
| `--resource-type TEXT` | Filter by `ingest`, `historize`, or `all` (default: `all`) |
| `--pipelines` | List available pipeline implementations instead of configs |
| `-v, --verbose` | Enable debug logging |
| `--profile TEXT` | Profile name from `profiles.yml` (default: `default`) |
| `--target TEXT` | Target within profile (e.g., `dev`, `prod`) |

---

## saga validate

Validate pipeline configs without executing anything.

```bash
saga validate [OPTIONS]
```

| Option | Description |
|--------|-------------|
| `-s, --select TEXT` | Selector expression(s) |
| `-v, --verbose` | Enable debug logging |
| `--profile TEXT` | Profile name (default: `default`) |
| `--target TEXT` | Target within profile |

---

## saga ingest

Extract and load data.

```bash
saga ingest [OPTIONS]
```

| Option | Default | Description |
|--------|---------|-------------|
| `-s, --select TEXT` | all ingest-enabled | Selector expression(s) |
| `-w, --workers INT` | `4` | Number of parallel workers |
| `--orchestrate / --no-orchestrate` | off | Distribute via orchestration provider |
| `--full-refresh` | off | Drop state and tables, reload from scratch |
| `-f, --force` | off | Execute even if source hasn't changed |
| `--start-value-override TEXT` | — | Override incremental start value (backfill) |
| `--end-value-override TEXT` | — | Override incremental end value |
| `-v, --verbose` | off | Enable debug logging |
| `--profile TEXT` | `default` | Profile name |
| `--target TEXT` | — | Target within profile |

---

## saga historize

Build SCD2 tables from raw snapshot data.

```bash
saga historize [OPTIONS]
```

| Option | Default | Description |
|--------|---------|-------------|
| `-s, --select TEXT` | all historize-enabled | Selector expression(s) |
| `-w, --workers INT` | `4` | Number of parallel workers |
| `--orchestrate / --no-orchestrate` | off | Distribute via orchestration provider |
| `--full-refresh` | off | Rebuild SCD2 table from all raw snapshots |
| `--partial-refresh` | off | Rebuild from earliest raw snapshot, preserving older SCD2 records (GDPR-safe) |
| `--historize-from TEXT` | — | Reprocess from this date onwards (`YYYY-MM-DD` or `YYYY-MM-DD HH:MM:SS`). Older records preserved |
| `-f, --force` | off | Re-process even if no new snapshots detected |
| `-y, --yes` | off | Skip confirmation prompts |
| `-v, --verbose` | off | Enable debug logging |
| `--profile TEXT` | `default` | Profile name |
| `--target TEXT` | — | Target within profile |

---

## saga run

Run ingest then historize sequentially for the same set of pipelines.

```bash
saga run [OPTIONS]
```

| Option | Default | Description |
|--------|---------|-------------|
| `-s, --select TEXT` | all enabled | Selector expression(s) |
| `-w, --workers INT` | `4` | Number of parallel workers |
| `--orchestrate / --no-orchestrate` | off | Distribute via orchestration provider |
| `--full-refresh` | off | Full refresh for both ingest and historize |
| `--partial-refresh` | off | Partial refresh for historize phase only |
| `--historize-from TEXT` | — | Reprocess historize from this date onwards |
| `-f, --force` | off | Force ingest execution |
| `--start-value-override TEXT` | — | Override incremental start value |
| `--end-value-override TEXT` | — | Override incremental end value |
| `-y, --yes` | off | Skip confirmation prompts |
| `-v, --verbose` | off | Enable debug logging |
| `--profile TEXT` | `default` | Profile name |
| `--target TEXT` | — | Target within profile |

> When `--full-refresh` is set and an ingest fails, historize is skipped for that pipeline.

---

## saga destroy

Remove a pipeline's warehouse footprint (tables + state) **without reloading** — the teardown counterpart to `--full-refresh` (which drops *and* rebuilds). Run it to decommission a pipeline before deleting or disabling its config, so no stale tables or state are left behind.

```bash
saga destroy [OPTIONS]
```

| Option | Default | Description |
|--------|---------|-------------|
| `-s, --select TEXT` | all matching | Selector expression(s) |
| `--resource-type TEXT` | `all` | Which layer to tear down: `ingest`, `historize`, or `all` |
| `--dry-run` | off | Show exactly what would be dropped without deleting anything |
| `-w, --workers INT` | `4` | Number of parallel workers |
| `-y, --yes` | off | Skip the confirmation prompt |
| `-v, --verbose` | off | Enable debug logging |
| `--profile TEXT` | `default` | Profile name |
| `--target TEXT` | — | Target within profile |

```bash
saga destroy --select "group:google_sheets" --dry-run        # always preview first
saga destroy --select "filesystem__old_report__*"            # drop ingest + historized
saga destroy --select "tag:deprecated" --resource-type historize
```

**What gets dropped is decided by state, not by the config's current `write_disposition`.** For each selected pipeline, destroy removes only the tables saga's own state records *this* pipeline as having created:

- **Ingest** — the table(s) recorded in `_saga_load_info`, dropped together with dlt's pipeline state, staging table, and load-info rows (the same teardown `--full-refresh` performs before reloading).
- **Historize** — dropped only when `_saga_historize_log` records a run for this pipeline, along with the pipeline's log entries.

Because ownership comes from state, a table sitting at a config's derived name that this pipeline never created (a coincidental name match, a manually-created table, another pipeline's output) is **never** dropped — and a disposition change is handled correctly (an `append+historize` config later narrowed to `append` still has its historize-log entry, so its now-orphaned historized table is cleaned up).

Notes:

- Selects **disabled** configs too (`enabled: false`) — tearing a pipeline down after disabling it is the expected path. Run destroy *before* deleting the config file, since it derives the footprint from the config.
- Always `--dry-run` first. The confirmation prompt is shown otherwise (skipped by `-y/--yes` or in Cloud Run); `--dry-run` needs no confirmation since it deletes nothing.
- Teardown only touches the destination — it never contacts the source, so a pipeline whose source is already gone can still be destroyed.

---

## saga update-access

Update destination access controls (e.g., BigQuery IAM) without running pipelines.

```bash
saga update-access [OPTIONS]
```

| Option | Default | Description |
|--------|---------|-------------|
| `-s, --select TEXT` | all ingest-enabled | Selector expression(s) |
| `-w, --workers INT` | `4` | Number of parallel workers |
| `--dry-run` | off | Preview the access changes that would be applied without calling BigQuery's `update_dataset` / `set_iam_policy`. Log output mirrors a real run, prefixed with `[DRY RUN]` and using `would grant` / `would revoke` verbs. The run-end summary reports `would apply N grant(s), M revoke(s)` instead of `applied …`. |
| `-v, --verbose` | off | Enable debug logging |
| `--profile TEXT` | `default` | Profile name |
| `--target TEXT` | — | Target within profile |

---

## saga init

Scaffold a new dlt-saga consumer project.

```bash
saga init [OPTIONS]
```

| Option | Description |
|--------|-------------|
| `--no-input` | Skip all prompts; use defaults (DuckDB, schema `dlt_dev`, current directory) |

See [Getting Started](Getting-Started) for a full walkthrough.

---

## saga plan

Create an execution plan for external orchestrators (does not trigger workers).

```bash
saga plan [OPTIONS]
```

| Option | Default | Description |
|--------|---------|-------------|
| `-s, --select TEXT` | all | Selector expression(s) |
| `-c, --command TEXT` | `ingest` | Command workers will run: `ingest`, `historize`, or `run` |
| `--dry-run` | off | Preview task assignments without persisting |
| `--compact` | off | Emit JSON on a single line instead of pretty-printed. Pair with automation harnesses that read Cloud Logging entry-by-entry, where a multi-line block can be split into separate ingestion entries and arrive interleaved. |
| `-v, --verbose` | off | Enable debug logging |
| `--profile TEXT` | `default` | Profile name |
| `--target TEXT` | — | Target within profile |

---

## saga worker

Execute a single task from an execution plan. Typically run inside a container.

```bash
saga worker [OPTIONS]
```

| Option | Env var fallback | Description |
|--------|-----------------|-------------|
| `--execution-id TEXT` | `SAGA_EXECUTION_ID` | Execution plan ID |
| `--task-index INT` | `CLOUD_RUN_TASK_INDEX` → `SAGA_TASK_INDEX` | Task index to execute |
| `-c, --command TEXT` | `SAGA_WORKER_COMMAND` (default: `ingest`) | Command to run |
| `-w, --workers INT` | `SAGA_WORKER_CONCURRENCY` → `orchestration.worker_concurrency` (default: `4`) | Cap on parallel pipelines per task |
| `--full-refresh` | — | Drop state/tables and reload from scratch |
| `-f, --force` | — | Force execution |
| `--start-value-override TEXT` | — | Override incremental start value |
| `--end-value-override TEXT` | — | Override incremental end value |
| `-v, --verbose` | — | Enable debug logging |
| `--profile TEXT` | — | Profile name (default: `default`) |
| `--target TEXT` | — | Target within profile |

---

## saga report

Generate a standalone HTML run report.

```bash
saga report [OPTIONS]
```

| Option | Default | Description |
|--------|---------|-------------|
| `-s, --select TEXT` | all | Selector expression(s) |
| `-o, --output TEXT` | `saga_report.html` | Output path: local file or remote URI (`gs://`, `s3://`, `az://`) |
| `-d, --days INT` | `14` | Days of run history to include |
| `--open / --no-open` | open | Open in browser after generating (local output only) |
| `-v, --verbose` | off | Enable debug logging |
| `--profile TEXT` | `default` | Profile name |
| `--target TEXT` | — | Target within profile |

The **Executions** tab shows one row per run recorded in `_saga_executions` (both orchestrated and local), including its command, selector, and — when the run was a backfill — the `--start-value-override` / `--end-value-override` window.

---

## saga info

Show runtime environment: installed versions, registered plugins, active destinations, config paths.

```bash
saga info [OPTIONS]
```

| Option | Description |
|--------|-------------|
| `-v, --verbose` | Enable debug logging |

---

## saga doctor

Validate configuration and diagnose the environment (read-only) — similar to `dbt debug`. Checks the active dlt-saga build, profiles, project config, pipeline discovery (with resolved schema), destination connectivity, and that registered pipeline plugins are importable. Exits with code 1 if a check fails.

Also reports **clustering drift** on saga's internal log tables: tables created by older versions of saga were never clustered and stay that way until reconciled. Drift is advisory (it never fails `doctor`) and points you to `saga maintenance`.

```bash
saga doctor [OPTIONS]
```

| Option | Description |
|--------|-------------|
| `-v, --verbose` | Enable debug logging |
| `--profile TEXT` | Profile to use from profiles.yml |
| `--target TEXT` | Target within profile |
| `-s, --select TEXT` | Scope the config check and print each matched pipeline's resolved `project.schema.table` |

---

## saga maintenance

Reconcile the physical layout of saga's internal bookkeeping tables (the native_load log, historize log, and execution-plan log). Internal log tables created by older versions of saga were never clustered, so reads over a large log scan more than they need to — this command applies the current clustering to those tables across the project's schemas. It is a one-time migration that becomes a no-op once every table is up to date.

It only updates table metadata (no data is rewritten); the warehouse reclusters in the background, so it is safe to run against live tables. DuckDB has nothing to reconcile.

Run `saga doctor` first to see which tables have drifted.

```bash
saga maintenance [OPTIONS]
```

| Option | Description |
|--------|-------------|
| `-v, --verbose` | Enable debug logging |
| `--profile TEXT` | Profile to use from profiles.yml |
| `--target TEXT` | Target within profile |
| `-s, --select TEXT` | Scope which pipeline schemas are swept |
| `--dry-run` | Report what would change without writing anything |

```bash
saga maintenance --dry-run                     # Preview
saga maintenance                               # Apply across all schemas
saga maintenance --select "group:filesystem"   # Scope to one group's schema
```

---

## saga ai-setup

Generate an AI context file for the current project.

Writes `saga_ai_context.md` to the current directory. This file contains framework patterns, pipeline implementation guidance, and config reference that AI coding assistants (Claude Code, Cursor, Copilot, Windsurf, etc.) can use when helping you build custom pipelines.

Add the following to your AI assistant's context file (location varies by tool):

```
When working with dlt-saga pipelines, pipeline configs, or the saga CLI,
read ./saga_ai_context.md for framework patterns and guidance.
```

| Tool | Context file |
|------|-------------|
| Claude Code | `.claude/CLAUDE.md` |
| Cursor | `.cursorrules` |
| GitHub Copilot | `.github/copilot-instructions.md` |
| Windsurf | `.windsurfrules` |

The generated file is version-stamped. `saga doctor` will warn if it becomes outdated after a package upgrade.

```bash
saga ai-setup [OPTIONS]
```

No options.

---

## saga generate-schemas

Generate JSON schemas for pipeline config files and project files. Enables IDE autocomplete and inline validation.

```bash
saga generate-schemas [OPTIONS]
```

| Option | Default | Description |
|--------|---------|-------------|
| `-o, --output-dir TEXT` | `schemas` | Directory to write schemas to |

---

## Logging & debugging

Every local `saga` invocation writes a full DEBUG transcript to `logs/saga-<timestamp>-<pid>.log` while keeping the terminal at INFO. If a run fails, the file already contains the detail — no need to re-run with `--verbose` to reproduce the failure. `--verbose` only widens what reaches the terminal; the file is always DEBUG.

File logging is **off** in Cloud Run, in distributed worker mode (`SAGA_WORKER_MODE=true`), and during tests — those environments either capture stdout already or run on ephemeral storage.

| Variable | Default | Description |
|----------|---------|-------------|
| `SAGA_LOG_FILE` | (auto) | Set to `0`/`false` to disable file logging, or to `1`/`true` to force-enable it (e.g. inside a container with a mounted volume) |
| `SAGA_LOG_DIR` | `./logs` | Directory for log files |
| `SAGA_LOG_RETENTION` | `10` | Number of recent log files to keep; older files are pruned on each run |
| `SAGA_DEBUG_LOGGING` | off | Set to `true` to flip the terminal to DEBUG without passing `--verbose` |

---

## Selectors

All commands that accept `--select` use dbt-style selector syntax:

| Syntax | Meaning | Example |
|--------|---------|---------|
| `name` | Exact pipeline name | `--select google_sheets__budget` |
| `*glob*` | Glob pattern | `--select "*balance*"` |
| `tag:name` | Filter by tag | `--select "tag:daily"` |
| `group:name` | Filter by source group | `--select "group:google_sheets"` |
| space-separated | UNION (OR) | `--select "tag:daily group:filesystem"` |
| comma-separated | INTERSECTION (AND) | `--select "tag:daily,group:google_sheets"` |

Multiple `--select` flags are also combined with UNION:

```bash
saga ingest --select "tag:daily" --select "tag:critical"
```

---

## Programmatic API

Use `dlt_saga.Session` when you need to run pipelines from Python code (Airflow operators, notebooks, scripts):

```python
import dlt_saga

session = dlt_saga.Session(target="prod")

# Discover pipelines
configs = session.discover(select=["tag:daily"])

# Run phases individually
ingest_result = session.ingest(select=["tag:daily"], workers=8)
hist_result   = session.historize(select=["tag:daily"])

# Or both at once
result = session.run(select=["tag:daily"], workers=4)

if result.has_failures:
    for failure in result.failures:
        print(f"{failure.pipeline_name}: {failure.error}")
    raise RuntimeError(f"{result.failed} pipeline(s) failed")

# Reconcile internal-table clustering (equivalent of `saga maintenance`)
counts = session.maintenance(dry_run=True)   # {"absent": …, "unchanged": …, "reconciled": …}
```

`session.maintenance()` returns a status-count dict (not a `SessionResult`), since it operates on internal tables rather than pipelines. `session.update_access(...)` mirrors `saga update-access`.

**`SessionResult` attributes:**

| Attribute | Type | Description |
|-----------|------|-------------|
| `.succeeded` | `int` | Number of successful pipelines |
| `.failed` | `int` | Number of failed pipelines |
| `.has_failures` | `bool` | Whether any pipeline failed |
| `.failures` | `list[PipelineResult]` | Only the failed results |
| `.pipeline_results` | `list[PipelineResult]` | All results |

**`PipelineResult` attributes:** `.pipeline_name`, `.success`, `.error`, `.load_info`.
