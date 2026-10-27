# CLI Reference

All commands share common options: `--select`, `--exclude`, `--verbose`, `--profile`, `--target`. See [Logging & debugging](#logging--debugging) for the on-disk debug log written for every local run.

---

## saga list

List configured pipelines.

```bash
saga list [OPTIONS]
```

| Option | Description |
|--------|-------------|
| `-s, --select TEXT` | Selector expression(s) |
| `--exclude TEXT` | Remove pipelines matching these selectors — see [Excluding pipelines](#excluding-pipelines) |
| `--resource-type TEXT` | Filter by `ingest`, `historize`, or `all` (default: `all`) |
| `--pipelines` | List available pipeline implementations instead of configs |
| `-v, --verbose` | Enable debug logging |
| `--profile TEXT` | Profile name from `profiles.yml` (default: `default`) |
| `--target TEXT` | Target within profile (e.g., `dev`, `prod`) |

---

## saga validate

Validate pipeline configs without executing anything — the **config-correctness gate** (similar to `dbt parse`). Fully offline: no warehouse connection or credentials, so it's the right check to run in CI on every pull request. Exits with code 1 if any config has an error (or no configs match the selectors).

Per pipeline it checks: `write_disposition` is a known value, the `adapter` resolves to an implementation, the source config fields instantiate, and the historize block is valid. Across the selection it also runs two **project-wide** checks:

- **Target collisions** — two pipelines resolving to the same destination table (an error; a config must map to exactly one table). Detection is project-wide, so a selected pipeline colliding with a non-selected enabled one is still flagged, naming both. Resolved in the current environment **and** prod, so a dev run catches latent prod-only collisions too. See [Custom Naming → Avoiding target collisions](Custom-Naming#avoiding-target-collisions).
- **Deprecated config keys** — an advisory scan of the raw YAML of the selected configs, `saga_project.yml`, and `profiles.yml` for legacy alias keys (e.g. `output_table` → `table_name`, `dataset_access` → `schema_access`). These still resolve via read-time aliases, so they're reported as **warnings** and **never fail** the command — just a nudge to rename. This is the one check that reads the un-normalized files, since aliases are rewritten at load time.

Environment/connectivity health (does the profile resolve, can we reach the destination) is a separate concern — see [`saga doctor`](#saga-doctor).

```bash
saga validate [OPTIONS]
```

| Option | Description |
|--------|-------------|
| `-s, --select TEXT` | Selector expression(s) |
| `--exclude TEXT` | Remove pipelines matching these selectors — see [Excluding pipelines](#excluding-pipelines) |
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
| `--exclude TEXT` | none | Remove pipelines matching these selectors — see [Excluding pipelines](#excluding-pipelines) |
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
| `--exclude TEXT` | none | Remove pipelines matching these selectors — see [Excluding pipelines](#excluding-pipelines) |
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
| `--exclude TEXT` | none | Remove pipelines matching these selectors — see [Excluding pipelines](#excluding-pipelines) |
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
| `--exclude TEXT` | none | Remove pipelines matching these selectors — see [Excluding pipelines](#excluding-pipelines) |
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
| `--exclude TEXT` | none | Remove pipelines matching these selectors — see [Excluding pipelines](#excluding-pipelines) |
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
| `--exclude TEXT` | none | Remove pipelines matching these selectors — see [Excluding pipelines](#excluding-pipelines) |
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
| `--exclude TEXT` | none | Remove pipelines matching these selectors — see [Excluding pipelines](#excluding-pipelines) |
| `-o, --output TEXT` | `saga_report.html` | Output path: local file or remote URI (`gs://`, `s3://`, `az://`) |
| `-d, --days INT` | `14` | Days of run history to include |
| `--open / --no-open` | open | Open in browser after generating (local output only) |
| `--summary` | off | Also write a machine-readable [status summary](#status-summary) beside the report |
| `--stale-after-minutes INT` | `1440` | How long consumers should treat the summary as current |
| `-v, --verbose` | off | Enable debug logging |
| `--profile TEXT` | `default` | Profile name |
| `--target TEXT` | — | Target within profile |

The **Executions** tab shows one row per run recorded in `_saga_executions` (both orchestrated and local), including its command, selector, and — when the run was a backfill — the `--start-value-override` / `--end-value-override` window.

The Dashboard's *Longest execution per day* chart shows how long the biggest run of each day took (first task start → last task completion), so a schedule that is getting slower is visible at a glance; the tooltip names the execution's command and selector, and clicking a bar opens that day's executions. Only orchestrated executions have a measured duration — a local run records its outcomes when it finishes, not per-task timings — and backfills are left out as one-off windows.

The Dashboard's health headline counts a pipeline as failing when its most recent attempt failed or was abandoned (a worker that never reported back) — the same judgement as [`state:failed`](#selectors) and [`saga notify`](#saga-notify).

### Status summary

`--summary` writes a small JSON file beside the report: the `--output` path with `.html` replaced by `.json` (`gs://bucket/reports/saga.html` → `gs://bucket/reports/saga.json`). A status page, uptime check, deploy gate or script can then ask *"is ingestion healthy, and has anything stopped running?"* without parsing the HTML. It goes through the same upload path, so whoever can read the report can read the summary. The HTML report's own health headline reads the same verdicts, so the two never disagree.

Every pipeline in the selection is counted in `counts`, but only three kinds are listed: `needs_attention` (failing first, then overdue — a non-empty list always means a problem *now*), `recovered` (failed in the window, fine now; most recent recovery first, so judge each by `recovered_at` and let it fade) and `no_runs` (enabled, but no attempt in the window — config that has quietly stopped being used: worth knowing, not urgent). `recovered` and `no_runs` are named after the state they hold, and match their entries in `counts`. A recovered pipeline that has since gone overdue is listed in `needs_attention`, not `recovered`. Healthy and disabled pipelines are counted only, so the file grows with the number of problems rather than the number of pipelines. Per-pipeline detail for healthy ones stays in the HTML report. The file is written compact; pipe it through `jq` to read it.

```bash
saga report --output gs://my-bucket/reports/saga.html --summary --stale-after-minutes 90
```

```json
{
  "schema_version": 1,
  "generated_at": "2026-10-15T07:15:02+00:00",
  "stale_after_minutes": 90,
  "environment": "prod",
  "project": "my-project",
  "window_days": 14,
  "report_file": "saga.html",
  "complete": true,
  "last_attempt_at": "2026-10-15T07:00:03+00:00",
  "next_overdue_at": "2026-10-15T08:30:01+00:00",
  "counts": {"pipelines": 42, "ok": 36, "failing": 1, "recovered": 1, "no_runs": 1,
             "disabled": 3, "unknown": 0, "overdue": 0},
  "needs_attention": [
    {
      "pipeline_name": "api__orders",
      "pipeline_group": "api",
      "table_name": "orders",
      "state": "failing",
      "attempts": 24,
      "failures": 6,
      "consecutive_failures": 6,
      "failing_since": "2026-10-15T01:00:04+00:00",
      "recovered_at": null,
      "last_attempt_at": "2026-10-15T07:00:03+00:00",
      "last_success_at": "2026-10-15T00:00:05+00:00",
      "last_error": "first line of the latest error",
      "usual_gap_minutes": 60,
      "overdue_at": "2026-10-15T08:30:03+00:00",
      "is_overdue": false
    }
  ],
  "recovered": [],
  "no_runs": []
}
```

**About the summary**

| Field | Meaning |
|-------|---------|
| `stale_after_minutes` | When a consumer should consider the summary itself out of date. saga cannot know how often you regenerate the report, so set it to match your schedule — e.g. `90` for an hourly report, `1560` for a daily one. Default `1440` |
| `complete` | `false` when saga could not read the execution history at all (e.g. a permission error) — not "some numbers are partial". No pipeline could be judged: every enabled one is counted as `unknown` and nothing is listed. Show the summary as unavailable, not as healthy |
| `last_attempt_at` | The most recent attempt at any pipeline — "ingestion last ran 12 minutes ago" |
| `next_overdue_at` | The earliest `overdue_at` among pipelines *not* in `needs_attention`. A pipeline that is fine when the summary is written but stops running afterwards isn't in `needs_attention`, so past this moment the summary may be missing a newly overdue pipeline, however fresh `stale_after_minutes` says it is. `null` when no such pipeline has a usual gap |
| `report_file` | File name of the HTML report beside it, for linking relative to the summary's URL |

**Per listed pipeline** (the same fields in all three lists)

| Field | Meaning |
|-------|---------|
| `state` | `failing` — the latest attempt failed or was abandoned · `recovered` — failed in the window, latest attempt succeeded · `no_runs` — enabled, no attempt in the window. `counts` also covers `ok`, `disabled`, and `unknown` (only ever when `complete` is `false`) |
| `consecutive_failures`, `failing_since` | The current failure streak: failed attempts since the last success, and when the first of them started. Bounded by `--days` |
| `recovered_at` | For `recovered`: the first success after the last failure. Unlike `last_success_at` it doesn't move on every run, so a consumer can show "recovered 2h ago" and let it fade — the pipeline otherwise stays `recovered` for the whole window |
| `last_error` | First line of the latest failure's error, truncated to 300 characters. The full message is in the report |
| `usual_gap_minutes` | The longest the pipeline normally goes without an attempt, in whole minutes: the 90th percentile of the gaps between its attempts in the window. A percentile rather than the median, so a pipeline run twice a morning an hour apart learns its ~23h overnight gap rather than the hour. `null` with fewer than four attempts in the window |
| `overdue_at` | When the pipeline becomes overdue unless it is attempted again: 1.5 usual gaps after its last attempt. Absolute, so a consumer can compare it against its own clock while a page stays open, without knowing the rule. A long gap rarer than one in ten (an hourly job that skips weekends) still reads as overdue across it |
| `is_overdue` | Whether `overdue_at` had passed when the summary was generated — the pipeline that silently stopped being run, which failure alerts can't catch |

Verdicts come from `_saga_execution_plans`, which every way of running saga writes — local runs, `saga plan` + workers, `--orchestrate`. `schema_version` changes whenever a consumer would notice: a renamed or removed key, or a changed meaning.

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

Check environment health and destination connectivity (read-only) — similar to `dbt debug`: *"is my environment set up so I can run?"*. Checks the active dlt-saga build, profiles, project config, pipeline discovery (with resolved schema), destination connectivity, and that registered pipeline plugins are importable. Exits with code 1 if a check fails.

`doctor` is the **environment** counterpart to [`saga validate`](#saga-validate)'s **config-correctness** checks — the two split on whether a warehouse connection is needed. Config correctness (write_disposition, adapters, source fields, historize config, **target collisions**, **deprecated keys**) is offline and lives in `saga validate`; `doctor` requires credentials and proves you can actually reach the configured destination. Run `validate` in CI on every PR; run `doctor` when setting up an environment or when a run won't connect.

Use `--select` to print the fully resolved `project.schema.table` for specific pipelines without running them — handy for confirming a profile's `env_var()` schema resolved as expected, or which build is active after switching between an editable install and the pinned release. When scoped with `--select`, it also points you at `saga maintenance --dry-run` for internal-table upkeep (clustering + log-growth cleanup). `doctor` itself does **not** scan those tables — measuring clustering drift and reclaimable rows is deferred to the maintenance preview, so `doctor` stays a fast connectivity health check.

```bash
saga doctor [OPTIONS]
```

| Option | Description |
|--------|-------------|
| `-v, --verbose` | Enable debug logging |
| `--profile TEXT` | Profile to use from profiles.yml |
| `--target TEXT` | Target within profile |
| `-s, --select TEXT` | Scope the config check and print each matched pipeline's resolved `project.schema.table` |
| `--exclude TEXT` | Remove pipelines matching these selectors — see [Excluding pipelines](#excluding-pipelines) |

---

## saga maintenance

Reconcile and compact saga's internal bookkeeping tables (the native_load log, historize log, and execution-plan log). Runs three passes across the project's schemas, all by default:

- **Clustering reconcile** — internal log tables created by older versions of saga were never clustered, so reads over a large log scan more than they need to. This applies the current clustering to those tables. It only updates table metadata (no data is rewritten); the warehouse reclusters in the background, so it is safe on live tables. DuckDB has nothing to reconcile.
- **Log compaction** — the status logs (native_load, execution-plan) accumulate superseded rows. This collapses each log to the latest row per key, deleting only strictly-superseded *earlier* rows plus dangling stale `started` rows (native_load orphans). It is lossless: every read of these logs already collapses to the latest row per key, so no observable state changes. The historize log is not compacted (it only ever writes terminal rows).
- **Stale-task reconcile** — a crashed or never-scheduled orchestration task leaves a dangling `running`/`pending` row in the execution-plan log forever. This relabels ones older than the stale cutoff to a terminal `abandoned` status (`failed` stays reserved for genuine worker-reported failures). It's an in-place status update, not a delete.

All three passes are one-time-style migrations that become cheap no-ops once tables are up to date.

```bash
saga maintenance [OPTIONS]
```

| Option | Description |
|--------|-------------|
| `-v, --verbose` | Enable debug logging |
| `--profile TEXT` | Profile to use from profiles.yml |
| `--target TEXT` | Target within profile |
| `-s, --select TEXT` | Scope which pipeline schemas are swept |
| `--exclude TEXT` | Remove pipelines matching these selectors — see [Excluding pipelines](#excluding-pipelines) |
| `--dry-run` | Report what would change without writing anything |

```bash
saga maintenance --dry-run                     # Preview
saga maintenance                               # Apply across all schemas
saga maintenance --select "group:filesystem"   # Scope to one group's schema
```

---

## saga notify

Report run outcomes to the configured [notifiers](Configuration#slack-notifications) by reading what saga recorded, rather than hooking into a run.

That distinction is the point. Every lifecycle hook fires from `Session`, and fan-out workers execute pipelines without going through it — so on an orchestrated deployment hooks post nothing, and a notification assembled *inside* one worker could only describe that worker's slice anyway. `saga notify` reads the execution-plan tables, which every way of running saga already writes:

| How saga ran | Recorded | Covered by `saga notify` |
|---|---|---|
| `saga run` / `ingest` / `historize` locally | ✅ | ✅ |
| `saga plan` + worker fan-out | ✅ | ✅ |
| `saga run --orchestrate` | ✅ | ✅ |

```bash
saga notify [OPTIONS]
```

| Option | Description |
|--------|-------------|
| `-v, --verbose` | Enable debug logging |
| `--profile TEXT` | Profile to use from profiles.yml |
| `--target TEXT` | Target within profile. Selects the connection; the sweep is scoped to that target's **environment**, not its name |
| `--execution-id TEXT` | Report exactly this execution instead of sweeping |
| `--since-days INT` | How far back to sweep (default: 7) |
| `--force` | Report even executions already marked as reported |
| `--dry-run` | Build and log the digest without sending or marking anything |

```bash
saga notify                                  # sweep, default 7 days
saga notify --since-days 1 --target prod
saga notify --execution-id "$EXECUTION_ID"   # chained straight after a run
saga notify --dry-run                        # preview; sends nothing
```

### Two ways to use it

**Scheduled** — the general case. Run it on its own cadence (cron, Cloud Scheduler, an Airflow DAG) and it reports every execution that has finished and not yet been reported. No integration with whatever ran the pipelines is required.

**Chained** — pass `--execution-id` immediately after a run, when waiting for the next sweep is too slow. An orchestrator that already generates the id for [`saga plan`](#saga-plan) can hand the same one here rather than parsing it back out of container logs.

Both go through the same code, so the digest is identical.

### Scope

A sweep covers one orchestration schema in one database — that is the connection you make, so most of the scoping is implicit. On top of it, executions are filtered by the active target's **environment**.

Not by target *name*. Two targets can describe the same warehouse and differ only in how you authenticate — same `type`, `database` and `location`, one adding `run_as` impersonation — and their executions belong in the same digest. The stored `target` column is not usable either: the orchestrator records the raw `--target` string (`None` when omitted) while a local run records the resolved profile name, so two callers against one warehouse disagree.

Executions with no recorded environment are included rather than dropped — a missing field is not evidence that a run belongs elsewhere.

### Selection is on *unreported*, not *recent*

Executions are claimed via a `notified_at` column on `_saga_executions`, so:

- a **late** sweep misses nothing,
- **overlapping** schedules duplicate nothing,
- a run **straddling** a window boundary is reported once, when it finishes.

`--since-days` is only a bound, which is why its exact value barely matters. It stops a first run (or a long outage) from reporting a large backlog — this command reports recent runs that may need handling, not history that no longer does.

> **First run after upgrading.** Nothing has been marked as reported yet, so the first sweep covers everything inside the window. Run it once with a short `--since-days` (or `--dry-run` first) before scheduling it.

An execution with any task still `pending` or `running` is skipped and picked up by a later sweep, so a half-finished run is never reported as though it were done. That judgement is age-bounded: after 24 hours a dangling task is read as a crash rather than as work in progress, and the execution is reported. Otherwise it would wait for [`saga maintenance`](#saga-maintenance) to relabel the row `abandoned`, and would be dropped from every sweep until that ran.

### What gets reported

Pipelines lead, executions are context — the first question is "what is broken", not "which run broke":

```
:x: 2 pipeline(s) failing · 3 runs in the last 7d · `prod`

failing:
    • `shop__orders` — failing, 3 of 3 attempts — min_rows=1 not met. …  <@U012ABCDEF>
    • `crm__accounts` — failing, 1 of 3 attempts — 404 Client Error: Not Found …

recovered:
    • `shop__events` (1 of 4 attempts failed, latest attempt OK)
44 of 46 pipeline attempts succeeded
```

- **The denominator is attempts, not runs.** Consecutive executions may select different pipelines, so "failed in 1 of 3 runs" would imply a pipeline succeeded in two that never attempted it.
- **Persistence is kept** because *failed 3 of 3 attempts* and *failed once* are the difference between an outage and a blip.
- **Recovered pipelines are reported too**, as a separate line. A pipeline that fails one run in five and always recovers before the next sweep would otherwise never be reported at all — invisible precisely because it keeps fixing itself.
- **Per-pipeline mentions still work.** The execution-plan row stores each pipeline's config, so `notifications.slack.mentions` resolves even though no single process ran every pipeline.
- **Nothing failed, nothing posted.** A clean sweep is silent.

### Knowing the notifier ran

Because a clean sweep is silent, silence alone cannot tell "nothing failed" from "the notifier stopped running" — a removed schedule, expired credentials, a broken image. So every sweep appends one row to `_saga_notify_log` in the orchestration schema, whether or not it posted anything:

| Column | Meaning |
|--------|---------|
| `swept_at` | When the sweep ran |
| `environment` | Environment it was scoped to |
| `since_days` | The window it looked back over |
| `executions`, `total_attempts`, `failed_attempts` | What it considered |
| `failing_pipelines`, `recovered_pipelines` | What it found |
| `outcome` | `quiet` (nothing to report), `delivered`, or `undelivered` (a digest was due but no notifier accepted it) |

Point whatever monitoring you already have at it — no chat traffic is added. A monitor has to tell three states apart, and only one of them is healthy:

| State | What the monitor sees | Means |
|-------|-----------------------|-------|
| **Absent** | The query fails: the table does not exist | Never swept — the table is created by the first sweep |
| **Empty** | `MAX(swept_at)` is `NULL` | Never swept for this environment |
| **Stale** | An old `MAX(swept_at)` | Stopped sweeping |

Absent and empty are the *worst* cases — a sweep that was never scheduled — so they must alert, not pass. A threshold written the obvious way, `MAX(swept_at) < now - 2h`, does neither: comparing `NULL` is not true, so it stays green on an empty table. Ask for a verdict instead, and treat a query error as a failed check:

```sql
-- One row, always. overdue is TRUE when the last sweep is too old, or when
-- there has never been one. A "table not found" error means the same thing.
SELECT
  MAX(swept_at) AS last_sweep,
  COALESCE(MAX(swept_at) < CURRENT_TIMESTAMP - INTERVAL 2 HOUR, TRUE) AS overdue
FROM <orchestration_schema>._saga_notify_log
WHERE environment = 'prod';

-- A notifier that runs but cannot reach its channel
SELECT * FROM <orchestration_schema>._saga_notify_log
WHERE outcome = 'undelivered' ORDER BY swept_at DESC;
```

Set the interval to a couple of sweep intervals, so one late sweep is not an alert.

Only real sweeps write a row. `--execution-id` (a single report chained after one run) and `--dry-run` do not, and a sweep that fails to read the execution tables writes nothing — the missing row is what should alert.

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
| `--link / --no-link` | `--link` | Add/update the `# yaml-language-server: $schema=...` modeline in each config file. Use `--no-link` to only write schema files (e.g. in CI) |
| `--sync` | off | Delete every other `.json` file in the output directory, such as schemas left behind by a removed adapter or a renamed config class. Skipped if any schema fails to generate |

Schema files are named after the config class: `ApiConfig` → `api_config.json`. Classes from a package namespace (`packages.yml` or an entry point) are prefixed with the namespace, e.g. `local__my_source_config.json`, so packages can't overwrite each other's schemas. Two classes that would still share a file (same class name in one namespace) fail the command instead of one silently replacing the other.

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
| `state:new` | Target table doesn't exist yet | `--select "state:new"` |
| `state:failed` | Last recorded run failed | `--select "state:failed"` |
| space-separated | UNION (OR) | `--select "tag:daily group:filesystem"` |
| comma-separated | INTERSECTION (AND) | `--select "tag:daily,group:google_sheets"` |

Multiple `--select` flags are also combined with UNION:

```bash
saga ingest --select "tag:daily" --select "tag:critical"
```

The prefix set is closed: `tag:`, `group:`, `state:`. An unrecognized one is
rejected as a typo rather than treated as a pipeline name, so a mistyped
selector fails the command instead of silently selecting nothing. Values behind
`tag:` and `group:` are open-ended and still just match nothing when unknown.

```bash
saga list --select "tags:daily"   # error: Unknown selector 'tags:daily'
saga list --select "tag:nosuch"   # warning: matched no pipelines
```

### Excluding pipelines

`--exclude` takes selectors in the same syntax and removes whatever they match
from the selection. Exclusion always wins: a pipeline matched by both `--select`
and `--exclude` does not run. Without `--select`, it subtracts from every
pipeline the command would otherwise run.

```bash
saga historize --select "group:finance" --exclude finance__legacy_ledger
saga run --select "tag:daily" --exclude "tag:slow group:sharepoint"   # UNION
saga ingest --exclude "group:api,tag:slow"                            # all but slow api ones
saga ingest --select "tag:daily" --exclude "state:failed"             # skip what broke last run
```

An exclude that matches no pipeline at all warns, since a typo there lets the
pipeline you meant to skip run anyway. An exclude that matches pipelines that
just weren't in the selection is fine and stays quiet. `state:` selectors work
in `--exclude` too, with the same credential requirement (see below). Run
records and notifications show the exclusion next to the selection.

### State selectors

`state:new` and `state:failed` filter on what has already happened in the
warehouse, so unlike the other selectors they read from the destination — and
are answered for the target you are running against. A pipeline can be new in
`dev` and not in `prod`.

```bash
saga list --select "state:new"                   # Preview what is un-ingested
saga run --select "state:new"                    # Onboard newly added pipelines
saga ingest --select "state:failed"              # Retry what broke
saga ingest --select "tag:daily,state:new"       # Intersect like any selector
```

**`state:new`** matches a pipeline whose **target table does not exist yet**,
judged per layer: the ingest target for `saga ingest`, the historized target for
`saga historize`, either one for a command spanning both. So `saga run --select
"state:new"` ingests a brand-new pipeline and, for one whose raw table exists
but whose historized table doesn't, historizes it without re-ingesting.

Two consequences worth knowing:

- A pipeline that keeps failing before it creates its table stays `state:new`,
  so a scheduled `--select "state:new"` retries it every run.
- `saga historize --select "state:new"` on a pipeline that has never been
  ingested fails on the missing source table — use `saga run` for onboarding, so
  ingest creates the raw table first.

**`state:failed`** matches a pipeline whose most recent outcome in the
execution-plan log (`_saga_execution_plans`) is a failure, looking back 30 days.
It is layer-agnostic: a local `saga run` records one outcome per pipeline for
both phases, so the log cannot attribute a failure to ingest or historize.
Crashed runs that `saga maintenance` relabelled to `abandoned` count as failed;
pipelines currently `pending` or `running` do not.

Both selectors need credentials — `saga list --select "state:new"`, normally
offline, connects to the destination and impersonates the target's `run_as`
identity. `saga validate` is offline by design and rejects them.

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
hist_result = session.historize(select=["tag:daily"])

# Or both at once; every selecting method also takes exclude=
result = session.run(select=["tag:daily"], exclude=["tag:slow"], workers=4)

if result.has_failures:
    for failure in result.failures:
        print(f"{failure.pipeline_name}: {failure.error}")
    raise RuntimeError(f"{result.failed} pipeline(s) failed")

# Reconcile + compact internal tables (equivalent of `saga maintenance`)
counts = session.maintenance(dry_run=True)
# {"clustering": {"absent": …, "unchanged": …, "reconciled": …},
#  "compaction": {"absent": …, "collapsed": …, "orphaned": …},
#  "reconcile":  {"abandoned": …}}
```

`session.maintenance()` returns per-pass status-count dicts (not a `SessionResult`), since it operates on internal tables rather than pipelines. `session.update_access(...)` mirrors `saga update-access`.

**`SessionResult` attributes:**

| Attribute | Type | Description |
|-----------|------|-------------|
| `.succeeded` | `int` | Number of successful pipelines |
| `.failed` | `int` | Number of failed pipelines |
| `.has_failures` | `bool` | Whether any pipeline failed |
| `.failures` | `list[PipelineResult]` | Only the failed results |
| `.pipeline_results` | `list[PipelineResult]` | All results |

**`PipelineResult` attributes:** `.pipeline_name`, `.success`, `.error`, `.load_info`.
