"""Unit tests for per-pipeline status verdicts (report/status.py)."""

import json
from datetime import datetime, timedelta, timezone

import pytest

from dlt_saga.report.collector import OrchestrationRun, PipelineInfo, ReportData
from dlt_saga.report.status import (
    ERROR_MAX_CHARS,
    STATE_DISABLED,
    STATE_FAILING,
    STATE_NO_RUNS,
    STATE_OK,
    STATE_RECOVERED,
    STATE_UNKNOWN,
    STATUS_SCHEMA_VERSION,
    compute_status,
    summary_to_dict,
)

NOW = datetime(2026, 5, 10, 12, 0, 0, tzinfo=timezone.utc)


def _pipeline(table="sample", group="filesystem", enabled=True) -> PipelineInfo:
    return PipelineInfo(
        pipeline_name=f"{group}__{table}",
        pipeline_group=group,
        tags=[],
        write_disposition="append",
        ingest_enabled=True,
        historize_enabled=False,
        enabled=enabled,
        table_name=table,
        schema_name=f"dlt_{group}",
    )


def _run(
    hours_ago: float,
    status: str = "completed",
    execution_id=None,
    table="sample",
    group="filesystem",
    error=None,
    naive=False,
) -> OrchestrationRun:
    at = NOW - timedelta(hours=hours_ago)
    if naive:
        at = at.replace(tzinfo=None)
    return OrchestrationRun(
        execution_id=execution_id or f"exec-{hours_ago}",
        task_index=0,
        pipeline_group=group,
        pipeline_name=f"{group}__{table}",
        table_name=table,
        status=status,
        log_timestamp=at,
        started_at=at,
        completed_at=at,
        error_message=error,
    )


def _data(runs, pipelines=None, **kwargs) -> ReportData:
    return ReportData(
        generated_at=NOW,
        environment="prod",
        project="proj",
        days=14,
        pipelines=pipelines if pipelines is not None else [_pipeline()],
        orchestration_runs=runs,
        **kwargs,
    )


def _only(data: ReportData):
    (status,) = compute_status(data, now=NOW).pipelines
    return status


@pytest.mark.unit
class TestStates:
    def test_all_successes_is_ok(self):
        s = _only(_data([_run(3), _run(2), _run(1)]))
        assert s.state == STATE_OK
        assert s.attempts == 3
        assert s.failures == 0
        assert s.last_success_at == NOW - timedelta(hours=1)

    def test_latest_failure_is_failing_with_streak(self):
        s = _only(
            _data(
                [
                    _run(4),
                    _run(3, "failed", error="first\nsecond"),
                    _run(2, "failed", error="boom: table gone\ntraceback..."),
                ]
            )
        )
        assert s.state == STATE_FAILING
        assert s.consecutive_failures == 2
        assert s.failing_since == NOW - timedelta(hours=3)
        assert s.last_success_at == NOW - timedelta(hours=4)
        assert s.last_error == "boom: table gone"

    def test_failure_then_success_is_recovered(self):
        s = _only(_data([_run(3, "failed", error="x"), _run(2)]))
        assert s.state == STATE_RECOVERED
        assert s.consecutive_failures == 0
        assert s.failing_since is None
        assert s.failures == 1
        assert s.last_error == "x"

    def test_abandoned_counts_as_failed(self):
        s = _only(_data([_run(2), _run(1, "abandoned")]))
        assert s.state == STATE_FAILING
        assert s.last_error == "failed"

    def test_no_attempts_is_no_runs(self):
        s = _only(_data([]))
        assert s.state == STATE_NO_RUNS
        assert s.last_attempt_at is None
        assert s.is_overdue is None

    def test_disabled_pipelines_are_listed_as_disabled(self):
        summary = compute_status(
            _data([], disabled_pipelines=[_pipeline("off", enabled=False)]),
            now=NOW,
        )
        states = {p.pipeline_name: p.state for p in summary.pipelines}
        assert states == {
            "filesystem__sample": STATE_NO_RUNS,
            "filesystem__off": STATE_DISABLED,
        }

    def test_unreadable_history_is_unknown_not_ok(self):
        summary = compute_status(
            _data([_run(1)], orchestration_complete=False), now=NOW
        )
        assert summary.complete is False
        assert [p.state for p in summary.pipelines] == [STATE_UNKNOWN]

    def test_runs_of_unselected_pipelines_are_ignored(self):
        summary = compute_status(_data([_run(1, table="other")]), now=NOW)
        assert [p.state for p in summary.pipelines] == [STATE_NO_RUNS]

    def test_table_name_matched_case_insensitively(self):
        s = _only(_data([_run(1, table="Sample")]))
        assert s.state == STATE_OK

    def test_naive_warehouse_timestamps_are_utc(self):
        s = _only(_data([_run(1, naive=True)]))
        assert s.last_attempt_at == NOW - timedelta(hours=1)

    def test_long_error_is_truncated(self):
        s = _only(_data([_run(1, "failed", error="x" * 1000)]))
        assert len(s.last_error) == ERROR_MAX_CHARS


@pytest.mark.unit
class TestAttemptFolding:
    def test_one_attempt_per_execution_failed_if_any_row_failed(self):
        s = _only(
            _data(
                [
                    _run(1.0, execution_id="e1"),
                    _run(0.9, "failed", execution_id="e1", error="historize"),
                ]
            )
        )
        assert s.attempts == 1
        assert s.state == STATE_FAILING
        assert s.last_error == "historize"

    def test_recent_in_flight_row_counts_for_timing_not_state(self):
        s = _only(_data([_run(5), _run(0.5, "running")]))
        assert s.state == STATE_OK
        assert s.attempts == 1
        assert s.last_attempt_at == NOW - timedelta(hours=0.5)

    def test_stale_in_flight_row_is_ignored(self):
        s = _only(_data([_run(48), _run(30, "running")]))
        assert s.attempts == 1
        assert s.last_attempt_at == NOW - timedelta(hours=48)


@pytest.mark.unit
class TestOverdue:
    def test_hourly_pipeline_on_schedule_is_not_overdue(self):
        s = _only(_data([_run(h) for h in (4, 3, 2, 1)]))
        assert s.usual_gap_minutes == 60.0
        assert s.is_overdue is False

    def test_hourly_pipeline_that_stopped_is_overdue(self):
        s = _only(_data([_run(h) for h in (8, 7, 6, 5)]))
        assert s.is_overdue is True

    def test_too_few_intervals_leaves_overdue_unknown(self):
        s = _only(_data([_run(h) for h in (30, 20, 10)]))
        assert s.usual_gap_minutes is None
        assert s.is_overdue is None

    def test_an_occasional_outage_does_not_set_the_usual_gap(self):
        hours = [h for h in range(60, 0, -1) if not 30 < h < 40]
        s = _only(_data([_run(h) for h in hours]))
        assert s.usual_gap_minutes == 60.0

    def test_back_to_back_runs_learn_the_long_gap(self):
        """Twice a morning, an hour apart: a median gap of an hour would make
        every one of these overdue by the afternoon."""
        hours = [h for day in range(10) for h in (day * 24 + 7, day * 24 + 6)]
        s = _only(_data([_run(h) for h in sorted(hours, reverse=True)]))
        assert s.usual_gap_minutes == pytest.approx(23 * 60, abs=1)
        assert s.is_overdue is False

    def test_burst_schedule_that_stopped_is_overdue(self):
        hours = [day * 24 + offset for day in range(2, 10) for offset in (0, 0.1, 0.2)]
        s = _only(_data([_run(h) for h in sorted(hours, reverse=True)]))
        assert s.is_overdue is True


@pytest.mark.unit
class TestSummaryToDict:
    def test_contract_shape(self):
        summary = compute_status(
            _data([_run(2), _run(1, "failed", error="boom")]), now=NOW
        )
        payload = summary_to_dict(
            summary, stale_after_minutes=90, report_file="saga_report.html"
        )
        json.dumps(payload)  # serializable
        assert payload["schema_version"] == STATUS_SCHEMA_VERSION
        assert payload["generated_at"] == NOW.isoformat()
        assert payload["stale_after_minutes"] == 90
        assert payload["report_file"] == "saga_report.html"
        assert payload["complete"] is True
        assert payload["window_days"] == 14
        assert payload["counts"]["pipelines"] == 1
        assert payload["counts"][STATE_FAILING] == 1
        (pipeline,) = payload["needs_attention"]
        assert pipeline["state"] == STATE_FAILING
        assert pipeline["last_error"] == "boom"
        assert pipeline["failing_since"] == (NOW - timedelta(hours=1)).isoformat()


@pytest.mark.unit
class TestNeedsAttention:
    """Only pipelines needing attention are listed; every one is counted."""

    def _payload(self, runs, pipelines, **kwargs):
        summary = compute_status(_data(runs, pipelines=pipelines, **kwargs), now=NOW)
        return summary_to_dict(summary, stale_after_minutes=60, report_file=None)

    def test_healthy_and_disabled_pipelines_are_counted_not_listed(self):
        payload = self._payload(
            [_run(1, table="healthy")],
            [_pipeline("healthy")],
            disabled_pipelines=[_pipeline("off", enabled=False)],
        )
        assert payload["needs_attention"] == []
        assert payload["counts"]["ok"] == 1
        assert payload["counts"]["disabled"] == 1
        assert payload["counts"]["pipelines"] == 2

    def test_listed_most_urgent_first(self):
        hourly_then_stopped = [_run(h, table="stopped") for h in (9, 8, 7, 6)]
        payload = self._payload(
            [
                _run(2, "failed", table="recovered"),
                _run(1, table="recovered"),
                *hourly_then_stopped,
                _run(1, "failed", table="failing"),
                _run(1, table="healthy"),
            ],
            [
                _pipeline(name)
                for name in ("healthy", "recovered", "never", "stopped", "failing")
            ],
        )
        assert [p["table_name"] for p in payload["needs_attention"]] == [
            "failing",
            "stopped",
        ]
        assert [p["table_name"] for p in payload["recovered"]] == ["recovered"]
        assert [p["table_name"] for p in payload["no_runs"]] == ["never"]

    def test_unreadable_history_lists_nothing(self):
        payload = self._payload([_run(1)], [_pipeline()], orchestration_complete=False)
        assert payload["complete"] is False
        assert payload["needs_attention"] == []
        assert payload["counts"]["unknown"] == 1


@pytest.mark.unit
class TestRecoveredAt:
    def test_is_the_first_success_after_the_last_failure(self):
        s = _only(
            _data(
                [
                    _run(6, "failed"),
                    _run(5),
                    _run(4, "failed"),
                    _run(3, "failed"),
                    _run(2),
                    _run(1),
                ]
            )
        )
        assert s.state == STATE_RECOVERED
        assert s.recovered_at == NOW - timedelta(hours=2)
        assert s.last_success_at == NOW - timedelta(hours=1)

    def test_is_unset_while_failing(self):
        s = _only(_data([_run(3, "failed"), _run(2), _run(1, "failed")]))
        assert s.state == STATE_FAILING
        assert s.recovered_at is None


@pytest.mark.unit
class TestOverdueAt:
    def test_is_absolute_so_consumers_can_check_it_live(self):
        s = _only(_data([_run(h) for h in (4, 3, 2, 1)]))
        assert s.usual_gap_minutes == 60
        assert isinstance(s.usual_gap_minutes, int)
        assert s.overdue_at == NOW - timedelta(hours=1) + timedelta(minutes=90)
        assert s.is_overdue is False

    def test_next_overdue_at_is_the_earliest_unlisted_one(self):
        """An unlisted pipeline that stops after generation is not in the file;
        next_overdue_at is when the summary may start missing it."""
        runs = [
            *[_run(h, table="hourly") for h in (4, 3, 2, 1)],
            *[_run(h * 24 + 1, table="daily") for h in (4, 3, 2, 1)],
            *[_run(h, "failed", table="failing") for h in (4, 3, 2, 1)],
        ]
        summary = compute_status(
            _data(
                runs, pipelines=[_pipeline(t) for t in ("hourly", "daily", "failing")]
            ),
            now=NOW,
        )
        hourly = NOW - timedelta(hours=1) + timedelta(minutes=90)
        assert summary.next_overdue_at == hourly
        payload = summary_to_dict(summary, stale_after_minutes=60, report_file=None)
        assert payload["next_overdue_at"] == hourly.isoformat()
        assert payload["last_attempt_at"] == (NOW - timedelta(hours=1)).isoformat()

    def test_nothing_to_judge_leaves_both_unset(self):
        summary = compute_status(_data([]), now=NOW)
        assert summary.next_overdue_at is None
        assert summary.last_attempt_at is None


@pytest.mark.unit
class TestRecoveredList:
    """A recovery is not a current problem, so it never keeps
    ``needs_attention`` non-empty for the rest of the window."""

    def _payload(self, runs, tables):
        summary = compute_status(
            _data(runs, pipelines=[_pipeline(t) for t in tables]), now=NOW
        )
        return summary_to_dict(summary, stale_after_minutes=60, report_file=None)

    def test_recovered_is_not_attention(self):
        payload = self._payload(
            [_run(300, "failed", table="old"), _run(299, table="old")], ["old"]
        )
        assert payload["needs_attention"] == []
        assert [p["table_name"] for p in payload["recovered"]] == ["old"]

    def test_most_recent_recovery_first(self):
        payload = self._payload(
            [
                _run(50, "failed", table="earlier"),
                _run(49, table="earlier"),
                _run(5, "failed", table="later"),
                _run(4, table="later"),
            ],
            ["earlier", "later"],
        )
        assert [p["table_name"] for p in payload["recovered"]] == ["later", "earlier"]

    def test_recovered_but_overdue_needs_attention(self):
        runs = [_run(h, table="r") for h in (10, 9, 8)]
        runs.insert(0, _run(11, "failed", table="r"))
        payload = self._payload(runs, ["r"])
        assert [p["table_name"] for p in payload["needs_attention"]] == ["r"]
        assert payload["recovered"] == []
