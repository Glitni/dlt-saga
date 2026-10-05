"""`saga notify` — the state-reading sweep.

The notifier reads what saga recorded rather than hooking into a run, because
every lifecycle hook fires from `Session` and worker mode bypasses it. These
pin the decisions that make the digest trustworthy: what counts as an attempt,
what "currently failing" means across several executions, and that an execution
is neither reported twice nor silently dropped.
"""

import logging
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from dlt_saga.notify import (
    DEFAULT_SINCE_DAYS,
    PipelineOutcome,
    SweepContext,
    collect_unreported,
    mark_reported,
)

# A Windows-recorded config path. Built from parts so no editing step can
# mangle its separators.
_WINDOWS_PATH = chr(92).join(["configs", "database", "asp", "customer_state.yml"])


def _row(
    execution_id,
    pipeline,
    status,
    *,
    error=None,
    config=None,
    command="run",
    environment="prod",
    created_at=None,
):
    """One plan-view row as the destination hands it back."""
    return SimpleNamespace(
        execution_id=execution_id,
        pipeline_identifier=pipeline,
        pipeline_type=pipeline.split("__")[0],
        table_name=pipeline.split("__")[-1],
        status=status,
        error_message=error,
        config_json=config if config is not None else {"pipeline_name": pipeline},
        log_timestamp=f"{execution_id}:{pipeline}",
        command=command,
        environment=environment,
        created_at=created_at,
    )


def _manager(rows, excluded=0):
    """An ExecutionPlanManager double that returns *rows* for the sweep query."""
    manager = MagicMock()
    manager.schema = "dlt_orchestration"
    manager.plans_view_id = "proj.ds.plans_view"
    manager.executions_table_id = "proj.ds.executions"
    d = manager.destination
    d.escape_string_literal.side_effect = lambda s: str(s).replace("'", "''")
    d.timestamp_n_days_ago.side_effect = lambda n: f"CURRENT_TIMESTAMP() - {n} DAY"
    d.current_timestamp_expression.return_value = "CURRENT_TIMESTAMP()"

    def execute(sql, schema=None, **kwargs):
        if "COUNT(*)" in sql:
            return [SimpleNamespace(n=excluded)]
        return rows

    d.execute_sql.side_effect = execute
    return manager


@pytest.mark.unit
class TestAggregation:
    def test_clean_sweep_reports_nothing(self):
        ctx = collect_unreported(_manager([_row("e1", "shop__orders", "completed")]))

        assert ctx.outcomes == []
        assert not ctx.has_anything_to_report

    def test_a_failure_is_reported(self):
        ctx = collect_unreported(
            _manager([_row("e1", "shop__orders", "failed", error="boom")])
        )

        assert [o.pipeline_name for o in ctx.failing] == ["shop__orders"]
        assert ctx.failing[0].latest_error == "boom"
        assert ctx.has_anything_to_report

    def test_abandoned_counts_as_failed(self):
        """A worker that was OOM-killed reaches `abandoned`, not `failed`."""
        ctx = collect_unreported(_manager([_row("e1", "shop__orders", "abandoned")]))

        assert len(ctx.failing) == 1

    def test_latest_attempt_decides_current_state(self):
        """Rows arrive oldest-first, so the last one seen is the newest."""
        ctx = collect_unreported(
            _manager(
                [
                    _row("e1", "shop__orders", "failed", error="first"),
                    _row("e2", "shop__orders", "completed"),
                ]
            )
        )

        assert ctx.failing == []
        assert [o.pipeline_name for o in ctx.recovered] == ["shop__orders"]

    def test_recovered_then_failed_again_is_currently_failing(self):
        ctx = collect_unreported(
            _manager(
                [
                    _row("e1", "shop__orders", "failed", error="first"),
                    _row("e2", "shop__orders", "completed"),
                    _row("e3", "shop__orders", "failed", error="latest"),
                ]
            )
        )

        assert len(ctx.failing) == 1
        assert ctx.recovered == []
        assert ctx.failing[0].latest_error == "latest"
        assert ctx.failing[0].failures == 2
        assert ctx.failing[0].attempts == 3

    def test_latest_error_wins_when_errors_differ(self):
        ctx = collect_unreported(
            _manager(
                [
                    _row("e1", "shop__orders", "failed", error="timeout"),
                    _row("e2", "shop__orders", "failed", error="404 not found"),
                ]
            )
        )

        assert ctx.failing[0].latest_error == "404 not found"


@pytest.mark.unit
class TestAttemptsNotRuns:
    """The denominator is attempts, not executions in the window.

    Consecutive sweeps may cover executions with different selections, so
    "failed in 1 of 3 runs" would imply a pipeline succeeded in two runs that
    never attempted it.
    """

    def test_denominator_counts_only_executions_that_ran_it(self):
        rows = [
            # e1 selected both pipelines; e2 and e3 selected only `events`.
            _row("e1", "shop__orders", "failed", error="boom"),
            _row("e1", "shop__events", "completed"),
            _row("e2", "shop__events", "completed"),
            _row("e3", "shop__events", "completed"),
        ]

        ctx = collect_unreported(_manager(rows))

        orders = ctx.failing[0]
        assert orders.attempts == 1, "counted executions that never ran it"
        assert orders.failures == 1
        assert orders.failed_every_attempt

    def test_failed_every_attempt_distinguishes_outage_from_blip(self):
        persistent = [
            _row(f"e{i}", "shop__orders", "failed", error="boom") for i in (1, 2, 3)
        ]
        blip = [
            _row("e1", "crm__accounts", "failed", error="boom"),
            _row("e2", "crm__accounts", "completed"),
            _row("e3", "crm__accounts", "failed", error="boom"),
        ]

        ctx = collect_unreported(_manager(persistent + blip))
        by_name = {o.pipeline_name: o for o in ctx.outcomes}

        assert by_name["shop__orders"].failed_every_attempt
        assert not by_name["crm__accounts"].failed_every_attempt
        assert by_name["crm__accounts"].failures == 2
        assert by_name["crm__accounts"].attempts == 3


@pytest.mark.unit
class TestInFlightExecutions:
    def test_unfinished_execution_is_left_for_a_later_sweep(self):
        """Reporting a half-finished run would be wrong and would also claim it."""
        rows = [
            _row("e1", "shop__orders", "failed", error="boom"),
            _row("e1", "shop__events", "running"),
        ]

        ctx = collect_unreported(_manager(rows))

        assert ctx.execution_ids == []
        assert not ctx.has_anything_to_report

    def test_finished_executions_are_unaffected_by_an_in_flight_one(self):
        rows = [
            _row("e1", "shop__orders", "failed", error="boom"),
            _row("e2", "shop__orders", "pending"),
        ]

        ctx = collect_unreported(_manager(rows))

        assert ctx.execution_ids == ["e1"]
        assert len(ctx.failing) == 1


@pytest.mark.unit
class TestScoping:
    def test_sweep_selects_on_unreported(self):
        manager = _manager([])

        collect_unreported(manager)

        sql = manager.destination.execute_sql.call_args_list[-1][0][0]
        assert "notified_at IS NULL" in sql

    def test_sweep_is_bounded_by_the_window(self):
        manager = _manager([])

        collect_unreported(manager, since_days=3)

        sql = manager.destination.execute_sql.call_args_list[-1][0][0]
        assert "CURRENT_TIMESTAMP() - 3 DAY" in sql

    def test_default_window_is_generous(self):
        """A narrow window loses alerts across a scheduler outage."""
        assert DEFAULT_SINCE_DAYS == 7

    def test_environment_scopes_the_sweep(self):
        """A digest mixing environments invites misreading."""
        manager = _manager([])

        collect_unreported(manager, environment="prod")

        sql = manager.destination.execute_sql.call_args_list[-1][0][0]
        assert "e.environment = 'prod'" in sql

    def test_rows_without_an_environment_are_included(self):
        """Regression: an over-tight scope silently dropped nearly everything.

        A row that never recorded an environment is not evidence that it
        belongs to a different one, and excluding it would drop it from every
        sweep forever.
        """
        manager = _manager([])

        collect_unreported(manager, environment="prod")

        sql = manager.destination.execute_sql.call_args_list[-1][0][0]
        assert "e.environment IS NULL" in sql

    def test_target_name_is_never_used_for_scoping(self):
        """Two targets can describe one warehouse and differ only in `run_as`;
        the stored `target` column also disagrees between callers.
        """
        manager = _manager([])

        collect_unreported(manager, environment="prod")

        sql = manager.destination.execute_sql.call_args_list[-1][0][0]
        assert "e.target" not in sql

    def test_execution_id_ignores_both_window_and_reported_state(self):
        """The chained case: report this run now, whatever else is true."""
        manager = _manager([])

        collect_unreported(manager, execution_id="abc-123")

        sql = manager.destination.execute_sql.call_args_list[-1][0][0]
        assert "e.execution_id = 'abc-123'" in sql
        assert "notified_at IS NULL" not in sql


@pytest.mark.unit
class TestMarkReported:
    def test_claims_the_reported_executions(self):
        manager = _manager([])

        assert mark_reported(manager, ["e1", "e2"]) == 2

        sql = manager.destination.execute_sql.call_args[0][0]
        assert "SET notified_at" in sql
        assert "'e1', 'e2'" in sql

    def test_claim_is_guarded_so_concurrent_sweeps_do_not_race(self):
        manager = _manager([])

        mark_reported(manager, ["e1"])

        sql = manager.destination.execute_sql.call_args[0][0]
        assert "notified_at IS NULL" in sql

    def test_a_large_claim_is_split_across_statements(self):
        """Statement size must not scale with how much the sweep covered.

        The UPDATE inlines one literal per id, and a destination's query-length
        limit is finite — the same reason historize batches snapshots by range
        rather than by value list.
        """
        from dlt_saga.notify import CLAIM_BATCH_SIZE

        manager = _manager([])
        ids = [f"e{i}" for i in range(CLAIM_BATCH_SIZE * 2 + 1)]

        assert mark_reported(manager, ids) == len(ids)

        statements = manager.destination.execute_sql.call_args_list
        assert len(statements) == 3
        for call in statements:
            assert call[0][0].count("'e") <= CLAIM_BATCH_SIZE

    def test_a_failed_batch_does_not_discard_the_others(self):
        """A later batch failing must not un-claim what already committed, nor
        stop the remaining ones from being attempted.
        """
        from dlt_saga.notify import CLAIM_BATCH_SIZE

        manager = _manager([])
        calls = {"n": 0}

        def flaky(sql, schema=None, **kwargs):
            calls["n"] += 1
            if calls["n"] == 2:
                raise RuntimeError("denied")
            return []

        manager.destination.execute_sql.side_effect = flaky

        claimed = mark_reported(manager, [f"e{i}" for i in range(CLAIM_BATCH_SIZE * 3)])

        assert calls["n"] == 3, "a failed batch stopped the remaining ones"
        assert claimed == CLAIM_BATCH_SIZE * 2

    def test_nothing_to_claim_issues_no_statement(self):
        manager = _manager([])

        assert mark_reported(manager, []) == 0
        manager.destination.execute_sql.assert_not_called()

    def test_failed_claim_warns_rather_than_raising(self, caplog):
        """A duplicate digest next sweep beats losing the run entirely."""
        manager = _manager([])
        manager.destination.execute_sql.side_effect = RuntimeError("denied")

        with caplog.at_level(logging.WARNING):
            assert mark_reported(manager, ["e1"]) == 0

        assert "the next sweep will repeat them" in caplog.text


@pytest.mark.unit
class TestReportingThreshold:
    def _ctx(self, **kwargs):
        return SweepContext(**kwargs)

    def test_nothing_failed_is_quiet(self):
        assert not self._ctx().has_anything_to_report

    def test_currently_failing_reports(self):
        ctx = self._ctx(
            outcomes=[
                PipelineOutcome(
                    "p", "p", attempts=1, failures=1, currently_failing=True
                )
            ]
        )
        assert ctx.has_anything_to_report

    def test_recovered_only_still_reports(self):
        """A pipeline that fails one run in five and always recovers before the
        next sweep would otherwise never be reported at all.
        """
        ctx = self._ctx(
            outcomes=[
                PipelineOutcome(
                    "p", "p", attempts=5, failures=1, currently_failing=False
                )
            ]
        )

        assert ctx.recovered
        assert ctx.failing == []
        assert ctx.has_anything_to_report


@pytest.mark.unit
class TestConfigCarriedThrough:
    def test_stored_config_is_kept_for_mention_routing(self):
        """No single process ran every pipeline, but the plan row stores its config."""
        config = {
            "pipeline_name": "shop__orders",
            "notifications": {"slack": {"mentions": ["<@U9>"]}},
        }
        ctx = collect_unreported(
            _manager([_row("e1", "shop__orders", "failed", config=config)])
        )

        assert ctx.failing[0].config_dict == config

    def test_json_string_config_is_parsed(self):
        """Destinations hand the column back as a dict or a string."""
        import json

        config = {"pipeline_name": "shop__orders", "tags": ["daily"]}
        ctx = collect_unreported(
            _manager([_row("e1", "shop__orders", "failed", config=json.dumps(config))])
        )

        assert ctx.failing[0].config_dict["tags"] == ["daily"]

    def test_unparseable_config_does_not_break_the_sweep(self):
        ctx = collect_unreported(
            _manager([_row("e1", "shop__orders", "failed", config="{not json")])
        )

        assert len(ctx.failing) == 1
        assert ctx.failing[0].config_dict == {}


@pytest.mark.unit
class TestForce:
    """`--force` re-reports what a previous sweep already claimed."""

    def test_force_drops_the_unreported_predicate(self):
        manager = _manager([])

        collect_unreported(manager, force=True)

        sql = manager.destination.execute_sql.call_args_list[-1][0][0]
        assert "notified_at IS NULL" not in sql
        # …but the window still applies, so it can't re-report all of history.
        assert "CURRENT_TIMESTAMP() -" in sql

    def test_force_without_an_execution_id_still_sweeps(self):
        """Regression: `--force` alone once fell through to the unreported-only
        query and found nothing, making the flag silently useless.
        """
        ctx = collect_unreported(
            _manager([_row("e1", "shop__orders", "failed", error="boom")]),
            force=True,
        )

        assert len(ctx.failing) == 1

    def test_unforced_sweep_keeps_the_predicate(self):
        manager = _manager([])

        collect_unreported(manager, force=False)

        sql = manager.destination.execute_sql.call_args_list[-1][0][0]
        assert "notified_at IS NULL" in sql


@pytest.mark.unit
class TestScaleCounts:
    """The "N of M attempts succeeded" line exists to say whether a failure is
    isolated or systemic, so its denominator must cover every pipeline — not
    just the ones that failed.
    """

    def test_totals_include_pipelines_that_never_failed(self):
        rows = [_row("e1", "shop__orders", "failed", error="boom")]
        rows += [_row("e1", f"clean__p{i}", "completed") for i in range(45)]

        ctx = collect_unreported(_manager(rows))

        assert ctx.total_attempts == 46, "denominator counted only failures"
        assert ctx.failed_attempts == 1
        # …while the reported outcomes stay limited to what actually failed.
        assert len(ctx.outcomes) == 1

    def test_totals_span_executions(self):
        rows = [
            _row("e1", "shop__orders", "failed", error="boom"),
            _row("e1", "shop__events", "completed"),
            _row("e2", "shop__orders", "completed"),
            _row("e2", "shop__events", "completed"),
        ]

        ctx = collect_unreported(_manager(rows))

        assert ctx.total_attempts == 4
        assert ctx.failed_attempts == 1


@pytest.mark.unit
class TestInFlightIsAgeBounded:
    """A non-terminal row means different things at different ages.

    On a recent execution it means the run is still going. On a day-old one it
    means a task crashed — and those only become `abandoned` once
    `saga maintenance` runs the stale reconciler. Treating them as in-flight
    until then would drop the execution from every sweep indefinitely.
    """

    def _aged(self, hours):
        from datetime import datetime, timedelta, timezone

        return datetime.now(timezone.utc) - timedelta(hours=hours)

    def test_recent_unfinished_execution_waits_for_a_later_sweep(self):
        rows = [
            _row(
                "e1", "shop__orders", "failed", error="boom", created_at=self._aged(1)
            ),
            _row("e1", "shop__events", "running", created_at=self._aged(1)),
        ]

        ctx = collect_unreported(_manager(rows))

        assert ctx.execution_ids == []

    def test_old_dangling_execution_is_reported(self):
        """Regression: these were excluded forever unless maintenance ran."""
        rows = [
            _row(
                "e1", "shop__orders", "failed", error="boom", created_at=self._aged(48)
            ),
            _row("e1", "shop__events", "running", created_at=self._aged(48)),
        ]

        ctx = collect_unreported(_manager(rows))

        assert ctx.execution_ids == ["e1"]
        assert len(ctx.failing) == 1

    def test_unknown_age_is_treated_as_in_flight(self):
        """Without a timestamp the safe reading is 'still running' — reporting a
        half-finished run would also claim it, so it could never be revisited.
        """
        rows = [
            _row("e1", "shop__orders", "failed", error="boom", created_at=None),
            _row("e1", "shop__events", "running", created_at=None),
        ]

        ctx = collect_unreported(_manager(rows))

        assert ctx.execution_ids == []

    def test_naive_timestamps_are_handled(self):
        """Destinations differ on whether they hand back tz-aware datetimes."""
        from datetime import datetime, timedelta, timezone

        naive = (datetime.now(timezone.utc) - timedelta(hours=48)).replace(tzinfo=None)
        rows = [
            _row("e1", "shop__orders", "failed", error="boom", created_at=naive),
            _row("e1", "shop__events", "running", created_at=naive),
        ]

        ctx = collect_unreported(_manager(rows))

        assert ctx.execution_ids == ["e1"]


@pytest.mark.unit
class TestPipelineIdentity:
    """Identity is `(pipeline_type, table_name)` — what the `state:` selectors
    already key on, and for the same reason: `pipeline_identifier` is a config
    path that differs between a local checkout and a worker container, and
    `record_local_run` stores an empty `config_json` so there is no
    `pipeline_name` to fall back on either.
    """

    def _local(self, execution_id, path, status, error=None):
        """A local run: config path, no stored config."""
        row = _row(execution_id, "ignored", status, error=error)
        row.pipeline_identifier = path
        row.pipeline_type = "database"
        row.table_name = "customer_state"
        row.config_json = {}
        return row

    def _orchestrated(self, execution_id, status, error=None):
        """An orchestrated run: stored config carrying the pipeline name."""
        row = _row(execution_id, "ignored", status, error=error)
        row.pipeline_identifier = "configs/database/asp/customer_state.yml"
        row.pipeline_type = "database"
        row.table_name = "customer_state"
        row.config_json = {"pipeline_name": "database__asp__customer_state"}
        return row

    def test_local_and_orchestrated_runs_are_one_pipeline(self):
        """Observed in production: one pipeline reported twice, each with a
        partial attempt count — 2 of 335 orchestrated and 1 of 2 local.
        """
        rows = [
            self._orchestrated("e1", "failed", "boom"),
            self._orchestrated("e2", "completed"),
            self._local("e3", _WINDOWS_PATH, "failed", "boom"),
        ]

        ctx = collect_unreported(_manager(rows))

        assert len(ctx.outcomes) == 1, "one pipeline was reported as several"
        assert ctx.outcomes[0].attempts == 3
        assert ctx.outcomes[0].failures == 2

    def test_the_real_name_wins_over_a_config_path(self):
        """A group whose first row is local would otherwise be labelled by path
        even once an orchestrated row supplies the name.
        """
        rows = [
            self._local("e1", "configs/database/asp/customer_state.yml", "failed", "x"),
            self._orchestrated("e2", "failed", "boom"),
        ]

        ctx = collect_unreported(_manager(rows))

        assert ctx.outcomes[0].pipeline_name == "database__asp__customer_state"

    def test_local_only_pipeline_is_named_like_saga_report(self):
        """With no orchestrated row to supply a name, the label is derived the
        way `report/collector.py` derives it — so a pipeline reads the same in
        the digest and in `saga report`, rather than appearing as a file path.
        """
        rows = [self._local("e1", _WINDOWS_PATH, "failed", "x")]

        ctx = collect_unreported(_manager(rows))

        assert ctx.outcomes[0].pipeline_name == "database__customer_state"

    def test_different_tables_stay_separate(self):
        rows = [self._local("e1", "configs/database/a.yml", "failed", "x")]
        other = self._local("e1", "configs/database/b.yml", "failed", "x")
        other.table_name = "other_table"
        rows.append(other)

        ctx = collect_unreported(_manager(rows))

        assert len(ctx.outcomes) == 2

    def test_same_table_in_different_groups_stays_separate(self):
        """The one-config-one-table invariant is per schema, not global."""
        rows = [self._local("e1", "configs/database/x.yml", "failed", "x")]
        other = self._local("e1", "configs/api/x.yml", "failed", "x")
        other.pipeline_type = "api"
        rows.append(other)

        ctx = collect_unreported(_manager(rows))

        assert len(ctx.outcomes) == 2


@pytest.mark.unit
class TestTextDigest:
    """The plain renderer backs `--dry-run` when nothing is configured to send
    to, so a digest can be previewed before a webhook exists.
    """

    def _ctx(self, outcomes, **kw):
        defaults = {
            "environment": "prod",
            "since_days": 14,
            "execution_ids": ["e1", "e2"],
            "outcomes": outcomes,
            "total_attempts": 46,
            "failed_attempts": 3,
        }
        defaults.update(kw)
        return SweepContext(**defaults)

    def test_failure_counts_are_shown_not_just_the_state(self):
        from dlt_saga.notify import render_text_digest

        text = render_text_digest(
            self._ctx(
                [
                    PipelineOutcome(
                        "db__customer_state",
                        "customer_state",
                        attempts=23,
                        failures=3,
                        currently_failing=False,
                    )
                ]
            )
        )

        assert "3 of 23 attempts failed" in text
        assert "db__customer_state" in text

    def test_failing_pipelines_carry_their_error(self):
        from dlt_saga.notify import render_text_digest

        text = render_text_digest(
            self._ctx(
                [
                    PipelineOutcome(
                        "db__orders",
                        "orders",
                        attempts=3,
                        failures=3,
                        currently_failing=True,
                        latest_error="timeout",
                    )
                ]
            )
        )

        assert "failing:" in text
        assert "- db__orders (3 of 3 attempts failed) - timeout" in text

    def test_groups_are_nested_under_a_heading(self):
        """Flat prefixed lines are hard to scan once there are more than a few."""
        from dlt_saga.notify import render_text_digest

        text = render_text_digest(
            self._ctx(
                [
                    PipelineOutcome(
                        "db__a", "a", attempts=2, failures=2, currently_failing=True
                    ),
                    PipelineOutcome(
                        "db__b", "b", attempts=4, failures=1, currently_failing=False
                    ),
                ]
            )
        )

        lines = text.splitlines()
        assert "  failing:" in lines
        assert "  recovered:" in lines
        assert any(ln.strip().startswith("- db__a") for ln in lines)
        assert any(ln.strip().startswith("- db__b") for ln in lines)

    def test_no_backlog_line(self):
        """The notifier is about recent runs that may need handling, not about
        catching up on history.
        """
        from dlt_saga.notify import render_text_digest

        text = render_text_digest(self._ctx([]))

        assert "outside" not in text
        assert "not reported" not in text

    def test_scale_and_window_are_stated(self):
        from dlt_saga.notify import render_text_digest

        text = render_text_digest(self._ctx([]))

        assert "2 run(s) in the last 14d" in text
        assert "43 of 46 attempts succeeded" in text
        assert "prod" in text


@pytest.mark.unit
class TestScopeDescription:
    """The execution count in the log has to say what it counted.

    Observed in production: a `--force` sweep claimed 479 executions, and the
    ordinary sweep that followed reported "no failures across 1 execution(s) in
    the last 14d" — which reads as the window having shrunk to one run, rather
    than one execution being all that was left unreported.
    """

    def test_a_sweep_says_the_count_is_of_unreported_executions(self):
        from dlt_saga.notify import _describe_scope

        scope = _describe_scope(1, since_days=14, execution_id=None, force=False)

        assert scope == "1 unreported execution(s) in the last 14d"

    def test_force_says_claimed_executions_are_included(self):
        """`--force` drops the unreported predicate, so calling the count
        "unreported" there would be the opposite of true.
        """
        from dlt_saga.notify import _describe_scope

        scope = _describe_scope(479, since_days=14, execution_id=None, force=True)

        assert "479 execution(s) in the last 14d" in scope
        assert "unreported" not in scope
        assert "--force" in scope

    def test_an_execution_id_names_it_instead_of_counting(self):
        """Neither the window nor the claim applies to `--execution-id`."""
        from dlt_saga.notify import _describe_scope

        scope = _describe_scope(1, since_days=14, execution_id="run-7", force=False)

        assert scope == "execution 'run-7'"
        assert "14d" not in scope
