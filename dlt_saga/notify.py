"""Report run outcomes by reading recorded state, on a schedule of its own.

Backs the ``saga notify`` command.

Why this reads state rather than hooking into a run
---------------------------------------------------
Every lifecycle hook fires from :class:`~dlt_saga.session.Session`, and saga's
worker mode executes pipelines without going through it — so on a fan-out
deployment nothing fires at all, and a notification assembled inside one worker
could only ever describe that worker's slice anyway.

Every way of running saga already records the same two tables, which makes a
state-reading notifier work everywhere without integrating with any
orchestrator:

===========================================  ==========================  =================
How saga ran                                 Recorded by                 is_orchestrated
===========================================  ==========================  =================
``saga run`` / ``ingest`` / ``historize``    ``record_local_run``        ``FALSE``
``saga plan`` + worker fan-out               ``create_execution_plan``   ``TRUE``
``saga run --orchestrate``                   same                        ``TRUE``
===========================================  ==========================  =================

``reconcile_stale_tasks`` drives dangling rows to ``abandoned``, so even a
container that was OOM-killed reaches a terminal state this can see.

The predicate is "unreported", not "recent"
-------------------------------------------
Selection is on ``notified_at IS NULL``, with the time window only a bound. A
late scheduler misses nothing, overlapping schedules duplicate nothing, and a
run straddling a boundary is reported once — when it finishes. The window only
stops a first run (or a long outage) from reporting a large backlog: this
reports recent runs that may need handling, not history that no longer does.

Every sweep leaves a trace
--------------------------
A clean sweep posts nothing, so silence cannot tell "nothing failed" from "the
notifier stopped running" — and the second is the silence this command exists
to remove, moved up one layer. Each completed sweep therefore appends one row to
``_saga_notify_log``, so whether the notifier *ran* is a query something
other than the notifier can answer, without adding chat traffic.

Per-pipeline settings come from the current config
--------------------------------------------------
A notifier reads a pipeline's own ``notifications`` block — who to mention,
whether to post about it at all. The plan row stores a copy of the config, but
it is the config *as planned*: frozen when the execution was created, and empty
for a local run (``record_local_run``). So each outcome is matched to the
pipeline's current config by the same ``(pipeline_type, table_name)`` key the
report and ``state:`` selectors use (:func:`apply_current_configs`), and the
stored copy only stands in for a pipeline no longer configured. Muting a
pipeline therefore applies to the failures already recorded, not just to runs
planned after the change.

A pipeline's previous state is derived, not stored
----------------------------------------------------
A notifier that reminds about an ongoing failure every N hours rather than
every sweep needs to know whether a failure is new or continuing, and since
when. Both are reconstructed from rows this sweep already reads: the window
query returns every terminal row in the window, claimed ones included, and a
pipeline's state at the last sweep that saw it is the status of its most recent
claimed row (:func:`_derive_state`). Claimed means "a sweep considered it",
which stays true for a failure the notifier chose not to repeat, so no
per-pipeline marker is needed. The streak start is the first failed row after
the pipeline's last completed one; the previous sweep's time is the newest row
of the notify log. The reminder itself is the notifier's arithmetic over those
three facts — see ``hooks/notifiers/slack.py``. Within the window only: a
streak older than ``--since-days`` reads as starting at the window's edge.

The logic here is warehouse-facing and CLI-agnostic (no typer); the command in
``cli.py`` is a thin wrapper.
"""

import logging
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any, Dict, Iterable, Iterator, List, Optional

if TYPE_CHECKING:
    from dlt_saga.pipeline_config.base_config import PipelineConfig

logger = logging.getLogger(__name__)

# Terminal statuses an execution's pipelines must all have reached before it is
# reported. Anything else means the run is still in flight and belongs to a
# later sweep.
_TERMINAL = ("completed", "failed", "abandoned")
_FAILED = ("failed", "abandoned")

# An execution younger than this with a non-terminal row is genuinely still
# running and belongs to a later sweep. An older one crashed: its dangling rows
# only become `abandoned` once `saga maintenance` runs, and waiting for that
# would drop the execution from every sweep in the meantime. Matches
# PLANS_STALE_HOURS, which is the same judgement made by the reconciler.
_IN_FLIGHT_HOURS = 24

DEFAULT_SINCE_DAYS = 7

# Executions claimed per UPDATE. The statement inlines one literal per id,
# and statement size must not scale with how much a sweep covered — the same
# reason historize batches snapshots by range rather than by value list.
CLAIM_BATCH_SIZE = 1000

# How a sweep ended, as recorded in the notify log.
SWEEP_QUIET = "quiet"  # nothing to report
SWEEP_DELIVERED = "delivered"  # a digest went out
SWEEP_UNDELIVERED = "undelivered"  # a digest was due but nothing accepted it


@dataclass
class PipelineOutcome:
    """One pipeline's outcome aggregated across every execution in the sweep.

    Attributes:
        pipeline_name: Fully-qualified pipeline name.
        table_name: Destination table name.
        attempts: How many executions actually ran this pipeline. The
            denominator is attempts rather than executions, because consecutive
            runs may select different pipelines — "failed in 1 of 3 runs" would
            otherwise imply it succeeded in two that never attempted it.
        failures: How many of those attempts failed or were abandoned.
        currently_failing: Whether the most recent attempt failed.
        latest_error: Error from the most recent failed attempt.
        config_dict: The pipeline's config, from which a notifier reads its
            per-pipeline settings (``notifications.slack.mentions``, ``enabled``).
            The current config when the pipeline is still configured
            (:func:`apply_current_configs`), else the copy the plan row stores —
            available even though no single process ran every pipeline.
        pipeline_type: The pipeline group as recorded on the plan row; with
            ``table_name`` it identifies the pipeline across executions.
        previously_failing: The pipeline's state at the last sweep that saw it:
            ``True`` if its most recent claimed row failed, ``False`` if it
            completed, ``None`` when no earlier sweep saw it in the window. A
            failure with ``True`` is ongoing rather than new; a success with
            ``True`` is a recovery even when this sweep holds no failure.
        streak_started_at: When the current run of failures began — the first
            failed row after the pipeline's last completed one, claimed rows
            included. ``None`` when the pipeline is not failing or the rows
            carry no timestamp.
        streak_attempts: Failed attempts in that run, claimed rows included.
    """

    pipeline_name: str
    table_name: str
    attempts: int = 0
    failures: int = 0
    currently_failing: bool = False
    latest_error: Optional[str] = None
    config_dict: Dict[str, Any] = field(default_factory=dict)
    pipeline_type: str = ""
    previously_failing: Optional[bool] = None
    streak_started_at: Optional[datetime] = None
    streak_attempts: int = 0

    @property
    def recovered(self) -> bool:
        """Was failing — in this sweep or at the last one — and now passes."""
        if self.currently_failing:
            return False
        return self.failures > 0 or self.previously_failing is True

    @property
    def failed_every_attempt(self) -> bool:
        return self.attempts > 0 and self.failures == self.attempts


@dataclass
class SweepContext:
    """Everything one ``saga notify`` sweep found.

    Attributes:
        environment: Environment the sweep was scoped to.
        since_days: How far back the sweep looked.
        execution_ids: Executions covered, oldest first.
        outcomes: Per-pipeline aggregates across those executions.
        command: Which command ran, when every execution agrees on one.
        total_attempts: Pipeline attempts across every execution in the sweep,
            including pipelines that never failed. ``outcomes`` holds only the
            ones that did, so these totals are counted separately — otherwise
            the digest's "N of M succeeded" line would silently use the number
            of *failing* pipelines as its denominator and read as far more
            systemic than it is.
        failed_attempts: How many of those attempts failed.
        swept_at: When this sweep ran (UTC).
        last_swept_at: When the previous real sweep for this environment ran,
            from the notify log; ``None`` when there is none to be found. With
            ``swept_at`` and each outcome's ``streak_started_at`` it is what a
            notifier needs to remind about an ongoing failure on a cadence
            instead of every sweep.
    """

    environment: Optional[str] = None
    since_days: int = DEFAULT_SINCE_DAYS
    execution_ids: List[str] = field(default_factory=list)
    outcomes: List[PipelineOutcome] = field(default_factory=list)
    command: Optional[str] = None
    total_attempts: int = 0
    failed_attempts: int = 0
    swept_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    last_swept_at: Optional[datetime] = None

    @property
    def failing(self) -> List[PipelineOutcome]:
        """Pipelines whose most recent attempt failed — what needs attention."""
        return [o for o in self.outcomes if o.currently_failing]

    @property
    def recovered(self) -> List[PipelineOutcome]:
        """Pipelines that failed in the window but have since succeeded."""
        return [o for o in self.outcomes if o.recovered]

    @property
    def has_anything_to_report(self) -> bool:
        """Whether this sweep is worth posting.

        A sweep where nothing failed says nothing. One where everything failed
        and then recovered still posts: a pipeline failing one run in five and
        recovering before the next sweep would otherwise never be reported at
        all, invisible precisely because it keeps fixing itself.
        """
        return bool(self.failing or self.recovered)


def _pipeline_key(row: Any) -> tuple:
    """Identify a pipeline stably across every execution in the sweep.

    ``(pipeline_type, table_name)``, which is what the ``state:`` selectors
    already key on (``pipeline_state.py``) and for the same reason: the stored
    ``pipeline_identifier`` is a config *path* that differs between a local
    checkout and a worker container, and ``record_local_run`` stores an empty
    ``config_json`` so there is no ``pipeline_name`` to fall back on either.

    Keying on the path produced two entries for one pipeline — the orchestrated
    runs under ``database__asp__customer_state`` and the local ones under
    ``configs/database/asp/customer_state.yml`` — each with its own partial
    attempt count.
    """
    from dlt_saga.report.collector import pipeline_key

    return pipeline_key(
        str(getattr(row, "pipeline_type", "") or ""), str(row.table_name or "")
    )


def _display_name(row: Any, config: Dict[str, Any]) -> str:
    """A human-readable name for a pipeline.

    Display only — identity is :func:`_pipeline_key`. Prefers the stored
    ``pipeline_name``, which orchestrated rows carry. A pipeline whose rows are
    all local has none (``record_local_run`` stores an empty ``config_json``),
    so it falls back to ``<group>__<table>`` — the same derivation
    ``report/collector.py`` uses, so a pipeline is named identically in the
    digest and in ``saga report``.
    """
    name = config.get("pipeline_name")
    if name:
        return str(name)
    group = str(getattr(row, "pipeline_type", "") or "")
    table = str(row.table_name or "")
    return f"{group}__{table}" if group else table


def _as_dict(value: Any) -> Dict[str, Any]:
    """Coerce a stored ``config_json`` cell to a dict.

    Destinations hand it back as a dict (BigQuery JSON) or a string.
    """
    if isinstance(value, dict):
        return value
    if isinstance(value, str) and value:
        import json

        try:
            parsed = json.loads(value)
            return parsed if isinstance(parsed, dict) else {}
        except ValueError:
            return {}
    return {}


def collect_unreported(
    manager: Any,
    *,
    environment: Optional[str] = None,
    since_days: int = DEFAULT_SINCE_DAYS,
    execution_id: Optional[str] = None,
    force: bool = False,
) -> SweepContext:
    """Gather the outcomes of executions that finished and were never reported.

    Args:
        manager: An :class:`ExecutionPlanManager` for the orchestration schema.
        environment: Environment to scope to (``prod``, ``dev``).

            Not the target *name*. Two targets can describe the same warehouse
            and differ only in how you authenticate to it — same ``type``,
            ``database`` and ``location``, one with ``run_as`` impersonation —
            and their executions must be swept together. Nor is the stored
            ``target`` column usable: the orchestrator records the raw
            ``--target`` string (``None`` when omitted) while a local run
            records the resolved profile name, so two callers against one
            warehouse disagree.

            The rest of a target's definition is already implied by the
            connection — the sweep reads one schema in one database — so
            environment is the only part that discriminates. Rows with no
            recorded environment are included rather than dropped.
        since_days: How far back to look, defaulting to :data:`DEFAULT_SINCE_DAYS`.
        execution_id: Report exactly this execution instead of sweeping,
            ignoring both the window and whether it was already reported.
        force: Include executions already marked as reported. Applies to the
            sweep as well, so ``--force`` on its own re-reports the window.

    Returns:
        A :class:`SweepContext`, empty when there is nothing to report.

    Raises:
        Exception: Query failures propagate, unlike ``report/collector.py``
            which classifies them with ``looks_like_missing_table`` and carries
            on. A partial report is still useful; a notifier that quietly
            reports nothing because it could not read the table is precisely
            the silence this command exists to remove, so it fails loudly.
    """
    d = manager.destination

    # Every terminal row in the window is read, not only the ones to report:
    # the claimed ones are what tells a continuing failure from a new one
    # (see "A pipeline's previous state is derived, not stored" above). Which
    # rows are *reported* is still decided by the SQL predicate, returned per
    # row as `reportable`, and the stored config — the one wide column — is
    # only fetched for those.
    cutoff = d.timestamp_n_days_ago(since_days)
    where = f"e.created_at >= {cutoff}"
    if execution_id:
        reportable = f"e.execution_id = '{d.escape_string_literal(execution_id)}'"
        where = f"({reportable} OR {where})"
    elif force:
        reportable = "TRUE"
    else:
        reportable = "e.notified_at IS NULL"
    if environment:
        # NULL is included deliberately: a row that never recorded an
        # environment is not evidence that it belongs to a different one, and
        # excluding it would drop it from every sweep forever.
        env = d.escape_string_literal(environment)
        where += f" AND (e.environment = '{env}' OR e.environment IS NULL)"

    last_swept_at = read_last_sweep(manager, environment)

    rows = list(
        d.execute_sql(
            f"""
            SELECT
                p.execution_id      AS execution_id,
                p.pipeline_identifier AS pipeline_identifier,
                p.pipeline_type     AS pipeline_type,
                p.table_name        AS table_name,
                p.status            AS status,
                p.error_message     AS error_message,
                CASE WHEN {reportable} THEN p.config_json ELSE NULL END
                                    AS config_json,
                p.log_timestamp     AS log_timestamp,
                e.command           AS command,
                e.environment       AS environment,
                e.created_at        AS created_at,
                ({reportable})      AS reportable
            FROM {manager.plans_view_id} AS p
            JOIN {manager.executions_table_id} AS e
              ON p.execution_id = e.execution_id
            WHERE {where}
            ORDER BY p.log_timestamp
            """,
            manager.schema,
        )
    )

    rows = _drop_in_flight(rows)

    ctx = _aggregate(
        [r for r in rows if getattr(r, "reportable", True)],
        history=rows,
        environment=environment,
        since_days=since_days,
    )
    ctx.last_swept_at = last_swept_at
    return ctx


def read_last_sweep(manager: Any, environment: Optional[str]) -> Optional[datetime]:
    """When the previous real sweep for *environment* ran, from the notify log.

    ``None`` when the log does not exist yet or holds no sweep for the
    environment — and, deliberately, when it cannot be read: a notifier that
    cannot tell when it last ran falls back to reporting every ongoing failure,
    which is the noisy direction to be wrong in, not the silent one.
    """
    from dlt_saga.project_config import get_notify_log_table_name
    from dlt_saga.utility.sql import looks_like_missing_table

    d = manager.destination
    table_id = d.get_full_table_id(manager.schema, get_notify_log_table_name())
    where = ""
    if environment:
        env = d.escape_string_literal(environment)
        where = f"WHERE environment = '{env}' OR environment IS NULL"
    try:
        rows = list(
            d.execute_sql(
                f"SELECT MAX(swept_at) AS last_swept_at FROM {table_id} {where}",
                manager.schema,
            )
        )
    except Exception as exc:
        if not looks_like_missing_table(exc):
            logger.warning(
                "Could not read the previous sweep time from the notify log; "
                "ongoing failures will be reported as if due: %s",
                exc,
            )
        return None
    if not rows:
        return None
    return _as_utc(getattr(rows[0], "last_swept_at", None))


def _drop_in_flight(rows: List[Any]) -> List[Any]:
    """Drop executions that have not finished yet.

    "Not finished" is deliberately age-bounded. A non-terminal row on a recent
    execution means the run is still going, and reporting it would be wrong. The
    same row on a day-old execution means a task crashed — those only become
    ``abandoned`` when ``saga maintenance`` runs the stale reconciler, and
    treating them as in-flight until then would drop the execution from every
    sweep indefinitely, which is the silent loss this notifier exists to avoid.
    """
    from datetime import datetime, timedelta, timezone

    cutoff = datetime.now(timezone.utc) - timedelta(hours=_IN_FLIGHT_HOURS)
    unfinished: set = set()
    for row in rows:
        if (row.status or "").lower() in _TERMINAL:
            continue
        created = _as_utc(getattr(row, "created_at", None))
        if created is None or created >= cutoff:
            unfinished.add(row.execution_id)
    return [r for r in rows if r.execution_id not in unfinished]


def _as_utc(value: Any) -> Optional[datetime]:
    """Coerce a destination timestamp cell to an aware UTC datetime, or None."""
    if not isinstance(value, datetime):
        return None
    return value if value.tzinfo else value.replace(tzinfo=timezone.utc)


def _derive_state(history: List[Any]) -> Dict[tuple, tuple]:
    """Per pipeline, its state at the last sweep and the current failure streak.

    Walks every terminal row in the window in recorded order. The state at the
    last sweep is the status of the most recent *claimed* row (``None`` when
    no earlier sweep saw the pipeline). The streak is the run of failed rows
    since the pipeline's last completed one, claimed or not: its start is the
    first such row's ``created_at`` and its length the number of them. A
    pipeline whose latest row completed has no streak.

    Returns:
        ``{key: (previously_failing, streak_started_at, streak_attempts)}``.
    """
    previous: Dict[tuple, Optional[bool]] = {}
    streak_start: Dict[tuple, Optional[datetime]] = {}
    streak_attempts: Dict[tuple, int] = {}
    for row in history:
        key = _pipeline_key(row)
        failed = (row.status or "").lower() in _FAILED
        if getattr(row, "notified_at", None) is not None:
            previous[key] = failed
        if failed:
            if streak_attempts.get(key, 0) == 0:
                streak_start[key] = _as_utc(getattr(row, "created_at", None))
            streak_attempts[key] = streak_attempts.get(key, 0) + 1
        else:
            streak_start[key] = None
            streak_attempts[key] = 0
    return {
        key: (previous.get(key), streak_start.get(key), streak_attempts.get(key, 0))
        for key in set(previous) | set(streak_attempts)
    }


def _aggregate(
    rows: List[Any],
    *,
    environment: Optional[str],
    since_days: int,
    history: Optional[List[Any]] = None,
) -> SweepContext:
    """Fold per-execution plan rows into one outcome per pipeline.

    Rows arrive oldest-first, so the last one seen for a pipeline is its most
    recent attempt — which decides ``currently_failing`` and the error shown.

    Args:
        rows: The rows to report.
        history: Every terminal row in the window, *rows* included, from
            which each pipeline's previous state and failure streak are
            derived (:func:`_derive_state`). Defaults to *rows*.
    """
    state = _derive_state(rows if history is None else history)
    outcomes: Dict[tuple, PipelineOutcome] = {}
    execution_ids: List[str] = []
    commands = set()
    total_attempts = 0
    failed_attempts = 0

    for row in rows:
        if row.execution_id not in execution_ids:
            execution_ids.append(row.execution_id)
        if getattr(row, "command", None):
            commands.add(row.command)

        config = _as_dict(getattr(row, "config_json", None))
        key = _pipeline_key(row)
        outcome = outcomes.get(key)
        if outcome is None:
            previously_failing, streak_started_at, streak_attempts = state.get(
                key, (None, None, 0)
            )
            outcome = PipelineOutcome(
                pipeline_name=_display_name(row, config),
                table_name=row.table_name or "",
                pipeline_type=key[0],
                previously_failing=previously_failing,
                streak_started_at=streak_started_at,
                streak_attempts=streak_attempts,
            )
            outcomes[key] = outcome
        elif config.get("pipeline_name"):
            # A later row carried the real name where an earlier one (a local
            # run, with no stored config) could only offer its config path.
            outcome.pipeline_name = str(config["pipeline_name"])

        failed = (row.status or "").lower() in _FAILED
        total_attempts += 1
        failed_attempts += 1 if failed else 0
        outcome.attempts += 1
        outcome.currently_failing = failed
        if config:
            outcome.config_dict = config
        if failed:
            outcome.failures += 1
            outcome.latest_error = row.error_message or "failed"

    return SweepContext(
        environment=environment,
        since_days=since_days,
        execution_ids=execution_ids,
        # A pipeline with only successes here still matters when the last
        # sweep saw it failing: that is a recovery, and the only way one that
        # recovered between sweeps is ever reported.
        outcomes=[o for o in outcomes.values() if o.failures or o.previously_failing],
        command=commands.pop() if len(commands) == 1 else None,
        total_attempts=total_attempts,
        failed_attempts=failed_attempts,
    )


def apply_current_configs(
    ctx: SweepContext, config_sets: Iterable[Dict[str, List["PipelineConfig"]]]
) -> None:
    """Give each outcome its pipeline's current config where one is configured.

    The plan row's copy is the config as planned — frozen at plan time, and
    empty for a local run — so a notifier reading per-pipeline settings from it
    would miss a change made since, and miss local runs entirely. Matched by
    the same key as the report's :func:`~dlt_saga.report.collector.pipeline_key`;
    a pipeline that is no longer configured keeps the stored copy.

    Args:
        ctx: The sweep to update in place.
        config_sets: Discovered configs by pipeline group, as
            ``ConfigSource.discover()`` returns them — pass both the enabled
            and the disabled set, since a disabled pipeline can still have
            recorded failures to report.
    """
    from dlt_saga.report.collector import pipeline_key

    by_key = {
        pipeline_key(c.pipeline_group, c.table_name): c
        for configs in config_sets
        for config_list in configs.values()
        for c in config_list
    }
    for outcome in ctx.outcomes:
        config = by_key.get(pipeline_key(outcome.pipeline_type, outcome.table_name))
        if config is None:
            continue
        outcome.config_dict = {
            **config.config_dict,
            "pipeline_name": config.pipeline_name,
        }
        outcome.pipeline_name = config.pipeline_name


def mark_reported(manager: Any, execution_ids: List[str]) -> int:
    """Claim executions as reported so no later sweep repeats them.

    A single guarded ``UPDATE`` per call: the ``notified_at IS NULL`` predicate
    makes concurrent sweeps idempotent rather than racing.

    Returns:
        Number of executions claimed.
    """
    if not execution_ids:
        return 0
    d = manager.destination
    claimed = 0
    for batch in _batched(execution_ids, CLAIM_BATCH_SIZE):
        ids = ", ".join(f"'{d.escape_string_literal(e)}'" for e in batch)
        try:
            d.execute_sql(
                f"UPDATE {manager.executions_table_id} "
                f"SET notified_at = {d.current_timestamp_expression()} "
                f"WHERE execution_id IN ({ids}) AND notified_at IS NULL",
                manager.schema,
            )
            claimed += len(batch)
        except Exception as exc:
            # Not fatal, but it means the next sweep repeats this digest —
            # better a duplicate message than a lost one. The same is true of a
            # batch that fails after an earlier one committed.
            logger.warning(
                "Could not mark %d execution(s) as reported; the next sweep "
                "will repeat them: %s",
                len(batch),
                exc,
            )
    return claimed


def record_sweep(manager: Any, ctx: SweepContext, outcome: str) -> bool:
    """Append one row to the notify log, so a silent notifier is detectable.

    A quiet sweep and a sweep that never ran look the same in the channel.
    This row is what tells them apart: "no sweep in the last N intervals"
    becomes a query any external monitor can run.

    Optimistic, like the sweep's own read: the table is created only when the
    insert fails. Best-effort beyond that — by the time this runs the digest
    has already gone out, and failing the command would not bring it back. A
    row that could not be written reads as a missed sweep to whatever watches
    the table, which is the safe direction to be wrong in.

    Args:
        manager: An :class:`ExecutionPlanManager` for the orchestration schema.
        ctx: What the sweep found.
        outcome: One of :data:`SWEEP_QUIET`, :data:`SWEEP_DELIVERED`,
            :data:`SWEEP_UNDELIVERED`.

    Returns:
        Whether the row was written.
    """
    from dlt_saga.project_config import get_notify_log_table_name

    d = manager.destination
    table_name = get_notify_log_table_name()
    table_id = d.get_full_table_id(manager.schema, table_name)
    environment = (
        f"'{d.escape_string_literal(ctx.environment)}'" if ctx.environment else "NULL"
    )
    insert_sql = f"""
        INSERT INTO {table_id}
        (swept_at, environment, since_days, executions, total_attempts,
         failed_attempts, failing_pipelines, recovered_pipelines, outcome)
        VALUES (
            {d.current_timestamp_expression()},
            {environment},
            {int(ctx.since_days)},
            {len(ctx.execution_ids)},
            {ctx.total_attempts},
            {ctx.failed_attempts},
            {len(ctx.failing)},
            {len(ctx.recovered)},
            '{d.escape_string_literal(outcome)}'
        )
    """

    def _warn(error: Exception) -> bool:
        logger.warning(
            "Could not record this sweep in %s; a monitor watching it will "
            "read this sweep as missed: %s",
            table_name,
            error,
        )
        return False

    try:
        d.execute_sql(insert_sql, manager.schema)
        return True
    except Exception as insert_error:
        logger.debug("Notify log insert failed (%s); creating the table", insert_error)
        try:
            _ensure_notify_log(manager, table_id)
        except Exception as init_error:
            # Report the insert error, not this one: a notifier without DDL
            # rights fails here as a matter of course, and "cannot CREATE
            # TABLE" would send you after the wrong permission.
            logger.debug("Notify log creation also failed: %s", init_error)
            return _warn(insert_error)

    # A second insert failure is not "the table was missing", so it is the
    # error worth showing.
    try:
        d.execute_sql(insert_sql, manager.schema)
        return True
    except Exception as exc:
        return _warn(exc)


def _ensure_notify_log(manager: Any, table_id: str) -> None:
    """Create the notify log if it does not exist yet."""
    d = manager.destination

    def t(logical_type: str) -> str:
        return d.type_name(logical_type)

    d.ensure_schema_exists(manager.schema)
    d.execute_sql(
        f"""
        CREATE TABLE IF NOT EXISTS {table_id} (
            swept_at {t("timestamp")} NOT NULL,
            environment {t("string")},
            since_days {t("int64")} NOT NULL,
            executions {t("int64")} NOT NULL,
            total_attempts {t("int64")} NOT NULL,
            failed_attempts {t("int64")} NOT NULL,
            failing_pipelines {t("int64")} NOT NULL,
            recovered_pipelines {t("int64")} NOT NULL,
            outcome {t("string")} NOT NULL
        )
        """,
        manager.schema,
    )


def _batched(items: List[str], size: int) -> Iterator[List[str]]:
    """Yield *items* in chunks of at most *size*."""
    for start in range(0, len(items), size):
        yield items[start : start + size]


def render_text_digest(ctx: SweepContext) -> str:
    """Render a sweep as plain text, independent of any notifier.

    Used by ``--dry-run`` when nothing is configured to send to, so the digest
    can be previewed before a webhook exists.
    """
    runs = len(ctx.execution_ids)
    scope = f" · {ctx.environment}" if ctx.environment else ""
    lines = [
        f"{len(ctx.failing)} failing, {len(ctx.recovered)} recovered · "
        f"{runs} run(s) in the last {ctx.since_days}d{scope}"
    ]
    if ctx.failing:
        lines.append("  failing:")
        for outcome in ctx.failing:
            lines.append(
                f"     - {outcome.pipeline_name} "
                f"({outcome.failures} of {outcome.attempts} attempts failed) "
                f"- {outcome.latest_error or 'failed'}"
            )
    if ctx.recovered:
        lines.append("  recovered:")
        for outcome in ctx.recovered:
            lines.append(
                f"     - {outcome.pipeline_name} "
                f"({outcome.failures} of {outcome.attempts} attempts failed, "
                "latest attempt OK)"
            )
    if ctx.total_attempts:
        succeeded = ctx.total_attempts - ctx.failed_attempts
        lines.append(f"  {succeeded} of {ctx.total_attempts} attempts succeeded")
    return "\n".join(lines)


def run_notify(
    *,
    environment: Optional[str] = None,
    since_days: int = DEFAULT_SINCE_DAYS,
    execution_id: Optional[str] = None,
    force: bool = False,
    dry_run: bool = False,
    config_sets: Optional[Iterable[Dict[str, List["PipelineConfig"]]]] = None,
) -> SweepContext:
    """Collect unreported outcomes, post a digest, and claim what was reported.

    Args:
        environment: Environment to scope to; resolved from the active
            profile by the caller rather than taken from ``--target``.
        since_days: How far back to sweep.
        execution_id: Report exactly this execution instead of sweeping.
        force: Report even executions already marked as reported.
        dry_run: Build and log the digest without sending or claiming anything.
        config_sets: The project's discovered configs, enabled and disabled,
            so per-pipeline notification settings are read from the current
            config rather than the copy frozen on the plan row
            (:func:`apply_current_configs`). ``None`` leaves the stored copies.

    Returns:
        The :class:`SweepContext` that was (or would have been) reported.
    """
    from dlt_saga.destinations.factory import DestinationFactory
    from dlt_saga.utility.cli.context import get_execution_context
    from dlt_saga.utility.naming import get_execution_plan_schema
    from dlt_saga.utility.orchestration.execution_plan import ExecutionPlanManager

    context = get_execution_context()
    destination = DestinationFactory.create_from_context(
        context.get_destination_type(), context, {"schema_name": ""}
    )
    manager = ExecutionPlanManager(
        destination=destination, schema=get_execution_plan_schema()
    )

    def _collect() -> SweepContext:
        return collect_unreported(
            manager,
            environment=environment,
            since_days=since_days,
            execution_id=execution_id,
            force=force,
        )

    # Optimistic, as in `record_local_run`: a sweep is a reader, but
    # `ensure_table_exists()` is the write path's bootstrap — two CREATE IF NOT
    # EXISTS, the column checks and a view replace, each its own BigQuery job at
    # roughly a second, against the single job the sweep itself needs. Nothing a
    # read requires is missing in the steady state, so pay for the bootstrap only
    # when the read actually fails: a schema that has never been written to, or a
    # deployment whose `_saga_executions` predates `notified_at`, is exactly what
    # that failure looks like.
    try:
        ctx = _collect()
    except Exception as read_error:
        logger.debug("Sweep query failed (%s); initialising the tables", read_error)
        try:
            manager.ensure_table_exists()
        except Exception as init_error:
            # Report the read error, not this one. A notifier granted read-only
            # access fails here as a matter of course, and "cannot CREATE
            # TABLE" would send you after the wrong permission entirely.
            logger.debug("Table initialisation also failed: %s", init_error)
            raise read_error
        # A second read failure is not "the tables were missing", so it
        # propagates: a notifier that quietly reported nothing because it could
        # not read would be the silence this command exists to remove.
        ctx = _collect()

    if config_sets is not None:
        apply_current_configs(ctx, config_sets)

    scope = _describe_scope(
        len(ctx.execution_ids),
        since_days=since_days,
        execution_id=execution_id,
        force=force,
    )

    # Only a real sweep moves the marker. A single-execution report is chained
    # after one run and says nothing about whether the schedule is alive, and
    # a dry run is a preview.
    record = not (execution_id or dry_run)

    if not ctx.has_anything_to_report:
        logger.info("Nothing to report: no failures across %s", scope)
        if record:
            record_sweep(manager, ctx, SWEEP_QUIET)
        return ctx

    logger.info(
        "Reporting %d failing and %d recovered pipeline(s) across %s",
        len(ctx.failing),
        len(ctx.recovered),
        scope,
    )

    outcome = _dispatch(ctx, dry_run=dry_run)
    if dry_run:
        logger.info("Dry run - nothing sent, nothing marked as reported")
        return ctx

    _settle(manager, ctx, outcome, record=record)
    return ctx


def _settle(manager: Any, ctx: SweepContext, outcome: str, *, record: bool) -> None:
    """Claim and log the sweep according to how the notifier ended it.

    Delivered and quiet both claim the executions. A quiet outcome here means
    every failure belongs to a pipeline that opted out of the channel: the
    notifier has said all it will about them, and leaving them unreported would
    resurface them the day the mute is lifted, as a backlog nobody asked about.
    Undelivered claims nothing — that would turn one outage into a permanently
    lost alert, since the executions would never re-enter a sweep.
    """
    if outcome == SWEEP_UNDELIVERED:
        logger.warning(
            "Nothing was delivered, so %d execution(s) stay unreported and the "
            "next sweep will retry them.",
            len(ctx.execution_ids),
        )
    else:
        if outcome == SWEEP_QUIET:
            logger.info(
                "Nothing to post: every failing pipeline is muted for the notifier"
            )
        mark_reported(manager, ctx.execution_ids)
    if record:
        record_sweep(manager, ctx, outcome)


def _describe_scope(
    count: int,
    *,
    since_days: int,
    execution_id: Optional[str],
    force: bool,
) -> str:
    """Say what the count counted.

    A bare "across 1 execution(s)" is read as the size of the window, when a
    sweep in fact only ever looks at the *unreported* part of it — so the run
    right after a ``--force`` sweep, which claimed everything, looks as though
    the window itself had collapsed.
    """
    if execution_id:
        return f"execution '{execution_id}'"
    if force:
        return (
            f"{count} execution(s) in the last {since_days}d "
            "(--force: already-reported ones included)"
        )
    return f"{count} unreported execution(s) in the last {since_days}d"


def _dispatch(ctx: SweepContext, dry_run: bool = False) -> str:
    """Hand the sweep to every configured notifier.

    Returns how the sweep ended: :data:`SWEEP_DELIVERED` when a digest went
    out, :data:`SWEEP_QUIET` when the notifier found nothing left to post once
    its muted pipelines were removed, :data:`SWEEP_UNDELIVERED` when a digest
    was due and nothing accepted it. The caller claims executions as reported
    on the strength of this, so a failure must be visible here rather than
    swallowed — otherwise an outage loses the digest for good.

    Under ``dry_run`` each notifier renders its message and it is logged instead
    of sent — a preview that showed only a count would not let you check what
    is about to reach a channel, which is the whole point of previewing.

    Best-effort per notifier: an alerting channel that breaks must not stop the
    others, and must not fail the command.
    """
    from dlt_saga.project_config import get_project_config

    notifications = getattr(get_project_config(), "notifications", None)
    if notifications is None or not getattr(notifications, "slack", None):
        logger.warning(
            "Nothing to notify through: no notifications.slack configured in "
            "saga_project.yml"
        )
        if dry_run:
            logger.info("Digest that would be sent:\n%s", render_text_digest(ctx))
        # Nothing was delivered, so nothing may be claimed: a project that runs
        # the sweep before configuring a notifier would otherwise burn through
        # its backlog silently, and those executions never come back.
        return SWEEP_UNDELIVERED

    from dlt_saga.hooks.notifiers.slack import SlackNotifier, render_payload_text

    notifier = SlackNotifier(notifications.slack)
    try:
        if dry_run:
            payloads = notifier.build_sweep_payloads(ctx)
            if not payloads:
                logger.info(
                    "Nothing would be sent: every failing pipeline is muted "
                    "(notifications.slack.enabled: false) or not yet due for a "
                    "reminder (repeat_after_hours)"
                )
            for channel, payload in payloads:
                logger.info(
                    "Slack digest that would be sent to %s:\n%s",
                    channel or notifications.slack.channel or "the webhook's channel",
                    render_payload_text(payload),
                )
            return SWEEP_UNDELIVERED
        delivered = notifier.on_sweep_complete(ctx)
    except Exception:
        logger.warning("Slack notifier failed on the sweep digest", exc_info=True)
        return SWEEP_UNDELIVERED
    if delivered is None:
        return SWEEP_QUIET
    return SWEEP_DELIVERED if delivered else SWEEP_UNDELIVERED
