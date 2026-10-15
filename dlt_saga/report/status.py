"""Per-pipeline status verdicts derived from recorded run history.

Backs the status summary that ``saga report --summary`` writes beside the HTML
report, and the report dashboard's own health headline — one derivation, so
the page and the summary cannot disagree about which pipelines are failing.

Verdicts, not history
---------------------
A consumer of the summary (a status page, an uptime check, a deploy gate) wants
"failing since X, N attempts", not run rows to interpret. Deciding what
"failing" means here keeps every consumer on the definition saga itself uses:
the failed statuses are the ones ``state:failed`` selects on, so an abandoned
task counts as a failure, and pipelines are keyed by ``(pipeline_type,
table_name)`` for the same reason the ``state:`` selectors and ``saga notify``
key on it.

The input is the execution-plan rows the report already collects. Every way of
running saga writes them — local runs, ``saga plan`` with workers, and
``--orchestrate`` — and they are the only record of failed attempts, since
``_saga_load_info`` logs successes only.

"Has it stopped?"
-----------------
Failure-based alerting cannot see a pipeline that silently stops being run: no
run, no failure, nothing to report. Each pipeline therefore carries its usual
gap — the longest it normally goes without an attempt — and is overdue once
:data:`OVERDUE_FACTOR` usual gaps have passed since its last attempt. No
schedule needs to be known.

The usual gap is a high percentile of the gaps between recent attempts, not
their median. The question is how long a pipeline normally goes *without* a
run, and the median answers how long it usually goes between two: a pipeline
run twice a morning, an hour apart, has a median gap of an hour and would be
"overdue" every afternoon. The percentile still leaves out an occasional
outage, and learns a weekday-only schedule's weekend — unless runs after a
long gap are rarer than :data:`USUAL_GAP_QUANTILE` admits, as for an hourly
weekday job, which then reads as overdue across the weekend.

The logic here is CLI-agnostic; ``generator.py`` serializes it.
"""

import statistics
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple

from dlt_saga.report.collector import PipelineInfo, ReportData

#: Version of the summary contract. Bump on any change a consumer would notice:
#: a renamed or removed key, or a changed meaning.
STATUS_SCHEMA_VERSION = 1

#: Default for how long the summary stays current, in minutes. Only whoever
#: schedules the report knows its cadence, so ``saga report`` takes it as an
#: option; the default suits a daily report.
DEFAULT_STALE_AFTER_MINUTES = 1440

#: A pipeline is overdue once this many usual gaps have passed since its last
#: attempt.
OVERDUE_FACTOR = 1.5

#: Percentile of the gaps between attempts taken as the usual gap.
USUAL_GAP_QUANTILE = 0.9

#: Gaps between attempts needed before a usual gap is trusted. With fewer, one
#: manual rerun would set it.
MIN_GAPS = 3

#: Longest error excerpt carried per pipeline. The full message is in the report.
ERROR_MAX_CHARS = 300

STATE_OK = "ok"
STATE_FAILING = "failing"
STATE_RECOVERED = "recovered"
STATE_NO_RUNS = "no_runs"
STATE_DISABLED = "disabled"
STATE_UNKNOWN = "unknown"


@dataclass
class PipelineStatus:
    """One pipeline's verdict over the report window.

    Attributes:
        pipeline_name: Fully-qualified pipeline name.
        pipeline_group: Pipeline group.
        table_name: Destination table name.
        state: One of ``ok`` (latest attempt succeeded, none failed in the
            window), ``failing`` (latest attempt failed), ``recovered`` (failed
            in the window, latest attempt succeeded), ``no_runs`` (enabled, no
            attempt in the window), ``disabled``, or ``unknown`` (run history
            could not be read).
        attempts: Finished attempts in the window.
        failures: How many of those failed or were abandoned.
        consecutive_failures: Failed attempts since the last success.
        failing_since: Start of the first attempt in the current failure streak.
            Bounded by the window: a streak older than it starts at its edge.
        recovered_at: For a ``recovered`` pipeline, start of the first success
            after its last failure. Unlike ``last_success_at`` it does not move
            on every run, so a consumer can let a recovery fade.
        last_attempt_at: Start of the most recent attempt, finished or in flight.
        last_success_at: Start of the most recent successful attempt.
        last_error: First line of the most recent failure's error, truncated.
        usual_gap_minutes: The longest the pipeline normally goes without an
            attempt (:data:`USUAL_GAP_QUANTILE` of its gaps), in whole minutes,
            or None with too few attempts to judge.
        overdue_at: When the pipeline becomes overdue unless attempted again:
            :data:`OVERDUE_FACTOR` usual gaps after its last attempt. Absolute,
            so a consumer can check it against its own clock without knowing
            the rule. None when there is no usual gap.
        is_overdue: Whether ``overdue_at`` had passed when the summary was
            generated. None when there is no usual gap.
    """

    pipeline_name: str
    pipeline_group: str
    table_name: str
    state: str
    attempts: int = 0
    failures: int = 0
    consecutive_failures: int = 0
    failing_since: Optional[datetime] = None
    recovered_at: Optional[datetime] = None
    last_attempt_at: Optional[datetime] = None
    last_success_at: Optional[datetime] = None
    last_error: Optional[str] = None
    usual_gap_minutes: Optional[int] = None
    overdue_at: Optional[datetime] = None
    is_overdue: Optional[bool] = None

    @property
    def needs_attention(self) -> bool:
        """Whether something is wrong with this pipeline right now.

        Failing or overdue only, so a non-empty ``needs_attention`` always means
        a current problem. ``recovered`` and ``no_runs`` are worth knowing but
        not urgent, and are listed apart; ``unknown`` only arises when no
        history could be read, which ``complete`` says once.
        """
        return bool(self.is_overdue) or self.state == STATE_FAILING


@dataclass
class StatusSummary:
    """Every pipeline's verdict, plus what the verdicts were judged from.

    Attributes:
        generated_at: When the verdicts were computed (UTC).
        environment: Environment the report ran against.
        project: Database/project identifier.
        window_days: How far back attempts were read.
        complete: False when the run history could not be read. Every enabled
            pipeline is then ``unknown`` — an unreadable history must never
            read as "nothing failed".
        pipelines: One verdict per pipeline in the report's selection.
    """

    generated_at: datetime
    environment: str = ""
    project: str = ""
    window_days: int = 0
    complete: bool = True
    pipelines: List[PipelineStatus] = field(default_factory=list)

    def counts(self) -> Dict[str, int]:
        """Pipelines per state, plus the total and how many are overdue."""
        result = {
            state: 0
            for state in (
                STATE_OK,
                STATE_FAILING,
                STATE_RECOVERED,
                STATE_NO_RUNS,
                STATE_DISABLED,
                STATE_UNKNOWN,
            )
        }
        for p in self.pipelines:
            result[p.state] += 1
        result["pipelines"] = len(self.pipelines)
        result["overdue"] = sum(1 for p in self.pipelines if p.is_overdue)
        return result

    @property
    def last_attempt_at(self) -> Optional[datetime]:
        """The most recent attempt at any pipeline: is ingestion running at all?"""
        return max(
            (p.last_attempt_at for p in self.pipelines if p.last_attempt_at),
            default=None,
        )

    @property
    def next_overdue_at(self) -> Optional[datetime]:
        """The earliest ``overdue_at`` among pipelines not needing attention.

        A pipeline that is fine when the summary is written but stops running
        afterwards is not in ``needs_attention``. Past this moment the summary
        may be missing such a pipeline, however fresh ``stale_after_minutes``
        says it is.
        """
        return min(
            (
                p.overdue_at
                for p in self.pipelines
                if p.overdue_at and not p.needs_attention
            ),
            default=None,
        )


@dataclass
class _Attempt:
    """One execution's attempt at a pipeline."""

    at: datetime
    finished: bool
    failed: bool
    error: Optional[str]


def _as_utc(value: Optional[datetime]) -> Optional[datetime]:
    """Coerce a possibly-naive warehouse timestamp to aware UTC."""
    if value is None:
        return None
    return value if value.tzinfo else value.replace(tzinfo=timezone.utc)


def _key(pipeline_group: str, table_name: str) -> Tuple[str, str]:
    from dlt_saga.utility.naming import normalize_identifier

    return (pipeline_group or "", normalize_identifier(table_name or ""))


def _first_line(message: Optional[str]) -> Optional[str]:
    """The first non-empty line of an error, truncated to :data:`ERROR_MAX_CHARS`."""
    if not message:
        return None
    line = next((ln.strip() for ln in message.splitlines() if ln.strip()), "")
    if len(line) > ERROR_MAX_CHARS:
        line = line[: ERROR_MAX_CHARS - 1] + "…"
    return line or None


def _attempts_by_pipeline(
    data: ReportData, now: datetime
) -> Dict[Tuple[str, str], List[_Attempt]]:
    """Fold the report's plan rows into attempts per pipeline, oldest first.

    One attempt per execution: a task that ran a pipeline's ingest and
    historize phases separately would otherwise count twice, seconds apart,
    and pull its usual gap towards zero. The attempt failed if any of its rows did.

    An unfinished row counts towards *when* a pipeline was last attempted — a
    long run in progress has not stopped — but not towards its verdict. Only
    while it is younger than ``PLANS_STALE_HOURS``: past that it is a crash the
    reconciler has yet to relabel, and treating it as in flight would hide a
    pipeline that stopped behind its own dangling row.
    """
    from dlt_saga.utility.cli.pipeline_state import FAILED_STATUSES
    from dlt_saga.utility.orchestration.execution_plan import (
        PLANS_STALE_HOURS,
        PLANS_TERMINAL_STATUSES,
    )

    in_flight_cutoff = now - timedelta(hours=PLANS_STALE_HOURS)
    grouped: Dict[Tuple[str, str], Dict[str, _Attempt]] = {}

    for run in data.orchestration_runs:
        at = _as_utc(run.started_at or run.log_timestamp)
        if at is None:
            continue
        status = (run.status or "").lower()
        finished = status in PLANS_TERMINAL_STATUSES
        if not finished and at < in_flight_cutoff:
            continue
        failed = status in FAILED_STATUSES

        per_execution = grouped.setdefault(_key(run.pipeline_group, run.table_name), {})
        attempt = per_execution.get(run.execution_id)
        if attempt is None:
            per_execution[run.execution_id] = _Attempt(
                at=at,
                finished=finished,
                failed=failed,
                error=run.error_message if failed else None,
            )
            continue
        attempt.at = min(attempt.at, at)
        attempt.finished = attempt.finished and finished
        if failed and not attempt.failed:
            attempt.failed = True
            attempt.error = run.error_message

    return {
        key: sorted(per_execution.values(), key=lambda a: a.at)
        for key, per_execution in grouped.items()
    }


def _usual_gap(attempts: List[_Attempt]) -> Optional[timedelta]:
    """The :data:`USUAL_GAP_QUANTILE` gap between attempts.

    None with fewer than :data:`MIN_GAPS` gaps. Interpolated within the observed
    gaps, so it never exceeds the longest one.
    """
    gaps = [
        (later.at - earlier.at).total_seconds()
        for earlier, later in zip(attempts, attempts[1:])
    ]
    if len(gaps) < MIN_GAPS:
        return None
    cuts = statistics.quantiles(gaps, n=100, method="inclusive")
    usual = cuts[round(USUAL_GAP_QUANTILE * 100) - 1]
    return timedelta(seconds=usual) if usual > 0 else None


def _judge(
    info: PipelineInfo, attempts: List[_Attempt], now: datetime
) -> PipelineStatus:
    """Turn one enabled pipeline's attempts into a verdict."""
    status = PipelineStatus(
        pipeline_name=info.pipeline_name,
        pipeline_group=info.pipeline_group,
        table_name=info.table_name,
        state=STATE_NO_RUNS,
    )
    if attempts:
        status.last_attempt_at = attempts[-1].at

    for attempt in (a for a in attempts if a.finished):
        status.attempts += 1
        if attempt.failed:
            status.failures += 1
            status.consecutive_failures += 1
            status.failing_since = status.failing_since or attempt.at
            status.last_error = _first_line(attempt.error) or "failed"
        else:
            if status.consecutive_failures:
                status.recovered_at = attempt.at
            status.consecutive_failures = 0
            status.failing_since = None
            status.last_success_at = attempt.at

    if status.consecutive_failures:
        status.state = STATE_FAILING
        status.recovered_at = None
    elif status.failures:
        status.state = STATE_RECOVERED
    elif status.attempts:
        status.state = STATE_OK

    usual_gap = _usual_gap(attempts)
    if usual_gap is not None and status.last_attempt_at is not None:
        status.usual_gap_minutes = round(usual_gap.total_seconds() / 60)
        status.overdue_at = status.last_attempt_at + usual_gap * OVERDUE_FACTOR
        status.is_overdue = now > status.overdue_at
    return status


def compute_status(data: ReportData, now: Optional[datetime] = None) -> StatusSummary:
    """Judge every pipeline in the report's selection.

    Args:
        data: Collected report data; its ``orchestration_runs`` are the input,
            and its ``disabled_pipelines`` are reported as ``disabled``.
        now: Reference time for overdue checks. Defaults to the current time.

    Returns:
        A :class:`StatusSummary` with one verdict per pipeline.
    """
    now = _as_utc(now) or datetime.now(timezone.utc)
    summary = StatusSummary(
        generated_at=now,
        environment=data.environment,
        project=data.project,
        window_days=data.days,
        complete=data.orchestration_complete,
    )

    attempts = _attempts_by_pipeline(data, now) if summary.complete else {}
    for info in data.pipelines:
        if summary.complete:
            pipeline_attempts = attempts.get(_key(info.pipeline_group, info.table_name))
            summary.pipelines.append(_judge(info, pipeline_attempts or [], now))
        else:
            summary.pipelines.append(
                PipelineStatus(
                    pipeline_name=info.pipeline_name,
                    pipeline_group=info.pipeline_group,
                    table_name=info.table_name,
                    state=STATE_UNKNOWN,
                )
            )
    for info in data.disabled_pipelines:
        summary.pipelines.append(
            PipelineStatus(
                pipeline_name=info.pipeline_name,
                pipeline_group=info.pipeline_group,
                table_name=info.table_name,
                state=STATE_DISABLED,
            )
        )
    return summary


def summary_to_dict(
    summary: StatusSummary,
    *,
    stale_after_minutes: int,
    report_file: Optional[str],
) -> Dict[str, Any]:
    """Serialize a summary to the published JSON contract.

    Every pipeline is counted in ``counts``, but only three kinds are listed:
    ``needs_attention`` (failing or overdue: a problem now), ``recovered``
    (failed in the window, fine now, most recent recovery first) and
    ``no_runs`` (enabled, no attempt in the window). Each list but
    ``needs_attention`` is named after the state it holds, as in ``counts``.
    The file grows with the number of problems rather than the number of
    pipelines; per-pipeline detail for healthy ones is what the HTML report is
    for.

    Args:
        summary: The verdicts to publish.
        stale_after_minutes: How long after ``generated_at`` a consumer should
            consider the summary out of date.
        report_file: File name of the HTML report it sits beside, so a consumer
            can link to it relative to the summary's own location.

    Returns:
        A JSON-serializable dict, versioned by ``schema_version``.
    """
    return {
        "schema_version": STATUS_SCHEMA_VERSION,
        "generated_at": _dt(summary.generated_at),
        "stale_after_minutes": stale_after_minutes,
        "environment": summary.environment,
        "project": summary.project,
        "window_days": summary.window_days,
        "report_file": report_file,
        "complete": summary.complete,
        "last_attempt_at": _dt(summary.last_attempt_at),
        "next_overdue_at": _dt(summary.next_overdue_at),
        "counts": summary.counts(),
        "needs_attention": [
            _entry(p)
            for p in sorted(
                (p for p in summary.pipelines if p.needs_attention), key=_urgency
            )
        ],
        "recovered": [
            _entry(p)
            for p in sorted(
                _calm(summary, STATE_RECOVERED),
                key=lambda p: p.recovered_at or summary.generated_at,
                reverse=True,
            )
        ],
        "no_runs": [
            _entry(p)
            for p in sorted(
                _calm(summary, STATE_NO_RUNS), key=lambda p: p.pipeline_name
            )
        ],
    }


def _calm(summary: StatusSummary, state: str) -> List[PipelineStatus]:
    """Pipelines in *state* that are not already listed as needing attention."""
    return [p for p in summary.pipelines if p.state == state and not p.needs_attention]


def _dt(value: Optional[datetime]) -> Optional[str]:
    return value.isoformat() if value else None


def _entry(p: PipelineStatus) -> Dict[str, Any]:
    """One listed pipeline, as published."""
    return {
        "pipeline_name": p.pipeline_name,
        "pipeline_group": p.pipeline_group,
        "table_name": p.table_name,
        "state": p.state,
        "attempts": p.attempts,
        "failures": p.failures,
        "consecutive_failures": p.consecutive_failures,
        "failing_since": _dt(p.failing_since),
        "recovered_at": _dt(p.recovered_at),
        "last_attempt_at": _dt(p.last_attempt_at),
        "last_success_at": _dt(p.last_success_at),
        "last_error": p.last_error,
        "usual_gap_minutes": p.usual_gap_minutes,
        "overdue_at": _dt(p.overdue_at),
        "is_overdue": p.is_overdue,
    }


def _urgency(status: PipelineStatus) -> Tuple[int, str]:
    """Sort key for ``needs_attention``: failing before overdue."""
    return (0 if status.state == STATE_FAILING else 1), status.pipeline_name
