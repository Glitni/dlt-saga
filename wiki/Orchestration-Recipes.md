# Orchestration Recipes

dlt-saga ships its own orchestration layer (`saga ingest --orchestrate`, `saga plan` / `saga worker`) for running pipelines on Cloud Run or other container backends — see [Deployment](Deployment) for that path.

If you already run a workflow tool — **Dagster, Airflow, Prefect** — you usually want it to own scheduling, retries, partitioning, and observability. dlt-saga doesn't ship integration packages for these tools, but the `Session` API exposes everything an external orchestrator needs in a few lines of glue code.

This page collects copy-pasteable recipes for the three most common orchestrators.

> **When external orchestrators take over.** Under Dagster/Airflow/Prefect the framework's own `--orchestrate` flag and the `orchestration:` block in `saga_project.yml` are not used — the external tool decides what to run, when, and where. Profiles, config discovery, hooks, and historize all continue to work unchanged.

---

## Building blocks

All three recipes lean on the same `Session` API:

```python
import dlt_saga

session = dlt_saga.Session(target="prod")          # picks up profiles.yml
configs = session.discover(select=["tag:daily"])   # List[PipelineConfig]

result = session.ingest(select=["sales__orders"], workers=1)
# result.has_failures, result.failed, result.failures: List[PipelineResult]
```

What you get on each `PipelineConfig`:

| Field | Use |
|---|---|
| `pipeline_name` | Stable identifier (e.g. `google_sheets__sales`). Pass to `select=` to run a single pipeline. |
| `pipeline_group` | Source group (e.g. `google_sheets`). Useful for UI grouping. |
| `ingest_enabled` / `historize_enabled` | Whether ingest / historize should run for this config. |
| `get_tag_names()` | Tag names as a `List[str]` (tags are `ScheduleTag` objects, not bare strings). |

What you get on each result:

| Field | Use |
|---|---|
| `result.pipeline_results: List[PipelineResult]` | Per-pipeline outcome. |
| `result.has_failures: bool` | Quick check for the orchestrator. |
| `r.success`, `r.error`, `r.load_info` | Details for a single pipeline. |

### Granularity

You have two natural levels of granularity:

1. **Coarse — one task per `saga` run.** The simplest pattern: a single task calls `session.ingest(select=[...])` for many pipelines and fails as a unit. Easiest to wire up; less retry granularity.
2. **Fine — one asset/task per pipeline.** Drives `Session` once per `PipelineConfig`. Much better visibility and per-pipeline retries; recommended once you have more than a handful of pipelines.

The recipes below use the fine-grained pattern. Drop to coarse by replacing the per-config loop with a single `session.ingest(select=["tag:daily"])` call.

---

## Dagster

Wrap `Session` in `build_definitions()` and surface each pipeline as one or two assets (ingest + historize as separate assets with an explicit dependency between them).

```python
# orchestration/dagster_defs.py
from dagster import asset, AssetExecutionContext, AssetKey, Definitions

import dlt_saga
from dlt_saga.pipeline_config import PipelineConfig


def build_definitions(target: str = "prod") -> Definitions:
    session = dlt_saga.Session(target=target)
    configs = session.discover()

    assets = []
    for cfg in configs:
        assets.extend(_assets_for_config(session, cfg))
    return Definitions(assets=assets)


def _assets_for_config(session: dlt_saga.Session, cfg: PipelineConfig):
    ingest_key = AssetKey([cfg.pipeline_name, "ingest"])
    historize_key = AssetKey([cfg.pipeline_name, "historize"])
    out = []

    if cfg.ingest_enabled:
        @asset(
            key=ingest_key,
            group_name=cfg.pipeline_group,
            tags={t: "" for t in cfg.get_tag_names()},
        )
        def _ingest(context: AssetExecutionContext, _cfg=cfg, _session=session):
            result = _session.ingest(select=[_cfg.pipeline_name], workers=1)
            if result.has_failures:
                raise RuntimeError(result.failures[0].error)
            context.add_output_metadata({"pipeline": _cfg.pipeline_name})

        out.append(_ingest)

    if cfg.historize_enabled:
        deps = [ingest_key] if cfg.ingest_enabled else []

        @asset(
            key=historize_key,
            group_name=cfg.pipeline_group,
            deps=deps,
        )
        def _historize(context: AssetExecutionContext, _cfg=cfg, _session=session):
            result = _session.historize(select=[_cfg.pipeline_name], workers=1)
            if result.has_failures:
                raise RuntimeError(result.failures[0].error)

        out.append(_historize)

    return out
```

Wire it into your Dagster repo:

```python
# orchestration/definitions.py
from .dagster_defs import build_definitions

defs = build_definitions(target="prod")
```

**Why not `@dlt_assets`?** Dagster's `dagster-dlt` package auto-generates assets from raw dlt sources with native metadata (row counts, schema). It bypasses the dlt-saga `Session` / config / historize layer entirely — config discovery, packages, and historize all have to be re-implemented in Dagster terms. Wrapping `Session` keeps everything dlt-saga already does and gives Dagster a clean asset boundary on top.

---

## Airflow

Two patterns work well; pick based on whether you want pipelines running inside the Airflow worker or in their own container.

### Pattern A — `PythonOperator` (in-worker)

Best when your Airflow workers can install `dlt-saga` and reach the destination directly.

```python
# dags/dlt_saga_dag.py
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

import dlt_saga


def _run_ingest(pipeline_name: str, target: str):
    session = dlt_saga.Session(target=target)
    result = session.ingest(select=[pipeline_name], workers=1)
    if result.has_failures:
        raise RuntimeError(result.failures[0].error)


def _run_historize(pipeline_name: str, target: str):
    session = dlt_saga.Session(target=target)
    result = session.historize(select=[pipeline_name], workers=1)
    if result.has_failures:
        raise RuntimeError(result.failures[0].error)


def build_dag(target: str = "prod") -> DAG:
    session = dlt_saga.Session(target=target)
    configs = session.discover()

    with DAG(
        dag_id="dlt_saga_daily",
        start_date=datetime(2026, 1, 1),
        schedule="@daily",
        catchup=False,
        default_args={"retries": 2, "retry_delay": timedelta(minutes=5)},
        tags=["dlt-saga"],
    ) as dag:
        for cfg in configs:
            ingest_task = None
            if cfg.ingest_enabled:
                ingest_task = PythonOperator(
                    task_id=f"{cfg.pipeline_name}__ingest",
                    python_callable=_run_ingest,
                    op_kwargs={"pipeline_name": cfg.pipeline_name, "target": target},
                )
            if cfg.historize_enabled:
                historize_task = PythonOperator(
                    task_id=f"{cfg.pipeline_name}__historize",
                    python_callable=_run_historize,
                    op_kwargs={"pipeline_name": cfg.pipeline_name, "target": target},
                )
                if ingest_task is not None:
                    ingest_task >> historize_task
    return dag


dag = build_dag()
```

### Pattern B — `KubernetesPodOperator` / `BashOperator` (per-pipeline container)

Best when you want each pipeline to run in its own container — typically for isolation, custom dependencies, or to avoid loading `dlt-saga` into the Airflow scheduler.

```python
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator

KubernetesPodOperator(
    task_id="sales__orders__ingest",
    image="ghcr.io/your-org/dlt-saga-runner:latest",
    cmds=["saga", "ingest"],
    arguments=["--select", "sales__orders", "--target", "prod"],
    env_vars={"SAGA_PROFILES_DIR": "/etc/dlt-saga"},
)
```

Pair it with `saga plan` / `saga worker` (see [Deployment](Deployment#saga-plan-and-saga-worker)) when you want Airflow to fan out workers against a pre-computed execution plan.

---

## Prefect

`@flow` and `@task` map cleanly onto `Session.ingest` / `Session.historize`.

```python
# flows/dlt_saga_flow.py
from prefect import flow, task

import dlt_saga


@task(retries=2)
def run_ingest(pipeline_name: str, target: str) -> None:
    session = dlt_saga.Session(target=target)
    result = session.ingest(select=[pipeline_name], workers=1)
    if result.has_failures:
        raise RuntimeError(result.failures[0].error)


@task(retries=1)
def run_historize(pipeline_name: str, target: str) -> None:
    session = dlt_saga.Session(target=target)
    result = session.historize(select=[pipeline_name], workers=1)
    if result.has_failures:
        raise RuntimeError(result.failures[0].error)


@flow(name="dlt-saga-daily")
def daily(target: str = "prod") -> None:
    session = dlt_saga.Session(target=target)
    configs = session.discover(select=["tag:daily"])

    for cfg in configs:
        ingest_future = None
        if cfg.ingest_enabled:
            ingest_future = run_ingest.submit(cfg.pipeline_name, target)
        if cfg.historize_enabled:
            wait_for = [ingest_future] if ingest_future else None
            run_historize.submit(cfg.pipeline_name, target, wait_for=wait_for)


if __name__ == "__main__":
    daily()
```

Deploy via `prefect deploy` or `flow.serve(...)` — Prefect handles scheduling, retries, and observability.

---

## Tag-based scheduling

The most useful pattern, once you have more than a handful of pipelines, is **one schedule per tag** — a `daily` schedule that picks up `tag:daily` pipelines, an `hourly` one for `tag:hourly`, and so on. Tags live in the YAML and are exposed on every `PipelineConfig`, so each orchestrator can drive its scheduler off them.

> Tag values (e.g. `hourly: [1, 10]`) work too — `cfg.has_tag("hourly", 10)` filters down to pipelines that should run at 10am. In practice it's cleaner to let the external scheduler own the cron and use bare tag names for filtering.

### Dagster — one `ScheduleDefinition` per tag

The Dagster recipe already attaches each pipeline's tag names to its assets, so `AssetSelection.tag(...)` does the filtering for you:

```python
from dagster import AssetSelection, Definitions, ScheduleDefinition, define_asset_job

from .dagster_defs import build_definitions

base = build_definitions(target="prod")

daily_job = define_asset_job("daily", selection=AssetSelection.tag("daily", ""))
hourly_job = define_asset_job("hourly", selection=AssetSelection.tag("hourly", ""))

defs = Definitions(
    assets=base.assets,
    jobs=[daily_job, hourly_job],
    schedules=[
        ScheduleDefinition(job=daily_job, cron_schedule="0 6 * * *"),
        ScheduleDefinition(job=hourly_job, cron_schedule="0 * * * *"),
    ],
)
```

### Airflow — one DAG per tag

Build a DAG factory keyed on the tag, and let each DAG own its own schedule:

```python
def build_dag_for_tag(tag: str, schedule: str, target: str = "prod") -> DAG:
    session = dlt_saga.Session(target=target)
    configs = session.discover(select=[f"tag:{tag}"])

    with DAG(
        dag_id=f"dlt_saga_{tag}",
        start_date=datetime(2026, 1, 1),
        schedule=schedule,
        catchup=False,
        tags=["dlt-saga", tag],
    ) as dag:
        for cfg in configs:
            # ... same per-pipeline task graph as before ...
            pass
    return dag


daily_dag = build_dag_for_tag("daily", schedule="@daily")
hourly_dag = build_dag_for_tag("hourly", schedule="@hourly")
```

### Prefect — one parameterised flow, multiple deployments

A single flow is enough — give it a `tag` parameter and create one deployment per schedule:

```python
@flow
def dlt_saga_by_tag(tag: str, target: str = "prod") -> None:
    session = dlt_saga.Session(target=target)
    configs = session.discover(select=[f"tag:{tag}"])
    for cfg in configs:
        # ... same per-pipeline submit/wait_for as before ...
        pass


# Two deployments, two schedules — same flow, different parameters
# dlt_saga_by_tag.deploy(name="daily",  cron="0 6 * * *", parameters={"tag": "daily"})
# dlt_saga_by_tag.deploy(name="hourly", cron="0 * * * *", parameters={"tag": "hourly"})
```

---

## Tips and trade-offs

- **Always pass `workers=1`** when the orchestrator is already parallelising across pipelines. Letting both layers fan out leads to surprising contention and double-counted concurrency.
- **One `Session` per process is enough** — `Session.__init__` validates credentials and applies dlt defaults. For Dagster, share a single `Session` across asset definitions; for Airflow `PythonOperator`, recreate per-task (each task runs in its own process anyway).
- **Selectors still work**: any selector that works on the CLI (`tag:daily`, `group:google_sheets`, `*sales*`) is valid as a list element passed to `select=[...]`. Use this to keep DAGs/repos focused.
- **Failures**: every recipe re-raises on `result.has_failures` so the orchestrator marks the task failed. If you'd rather log and continue (e.g. for low-priority pipelines), inspect `result.failures` and decide per-pipeline.
- **Hooks still fire**: lifecycle hooks (`ON_PIPELINE_START`, `ON_PIPELINE_COMPLETE`, `ON_PIPELINE_ERROR`) run inside `Session.ingest` / `Session.historize` regardless of who's calling — your alerting and reporting hooks keep working unchanged.

---

## See also

- [Deployment](Deployment) — Cloud Run, the built-in `--orchestrate` mode, and `saga plan` / `saga worker`
- [CLI Reference](CLI-Reference) — selector syntax (`tag:`, `group:`, glob patterns)
- [Profiles](Profiles) — multi-environment setup; pass `target=` to `Session()` to pick one
