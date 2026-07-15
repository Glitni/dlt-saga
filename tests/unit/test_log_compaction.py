"""Unit tests for internal-log compaction.

Covers the compactable-log registry, the destination-agnostic collapse / orphan
SQL builders, and the ``compact_log`` / ``run_log_compaction`` orchestration
against a fake destination (no warehouse). Real SQL is exercised against DuckDB
in tests/integration/test_log_compaction_duckdb.py.
"""

import pytest

from dlt_saga.maintenance import (
    _abandon_set_clause,
    _collapse_where,
    _orphan_where,
    _scalar,
    _stale_task_where,
    compact_log,
    reconcile_stale_tasks,
    run_log_compaction,
    run_stale_task_reconcile,
)
from dlt_saga.utility.internal_tables import resolve_compactable_logs


def _native_spec(schema="dlt_a"):
    logs = resolve_compactable_logs([schema], None)
    return next(log for log in logs if log.label == "native_load log")


def _plans_spec(schema="dlt_orch"):
    logs = resolve_compactable_logs([], schema)
    return next(log for log in logs if log.label == "execution-plan log")


class _FakeDest:
    """Records DELETEs and answers COUNT queries from a preset queue."""

    def __init__(self, counts=None, exists=True):
        self._counts = list(counts or [])
        self._exists = exists
        self.deletes: list[str] = []
        self.updates: list[str] = []

    def table_exists(self, schema, table):
        return self._exists

    def get_full_table_id(self, schema, table):
        return f"{schema}.{table}"

    def escape_string_literal(self, value):
        return value.replace("'", "\\'")

    def timestamp_n_days_ago(self, days):
        return f"now() - INTERVAL '{days}' DAY"

    def execute_sql(self, sql, schema=None):
        head = sql.strip().upper()
        if head.startswith("SELECT COUNT"):
            n = self._counts.pop(0) if self._counts else 0
            return [(n,)]
        if head.startswith("DELETE"):
            self.deletes.append(sql)
            return []
        if head.startswith("UPDATE"):
            self.updates.append(sql)
            return []
        return []


@pytest.mark.unit
class TestResolveCompactableLogs:
    def test_native_log_per_schema_with_orphan_rule(self):
        logs = resolve_compactable_logs(["dlt_a", "dlt_b"], None)
        native = [log for log in logs if log.label == "native_load log"]
        assert {log.schema for log in native} == {"dlt_a", "dlt_b"}
        assert native[0].key_columns == ["pipeline_name", "source_uri", "generation"]
        assert native[0].terminal_statuses == ["success", "failed"]
        assert native[0].orphan is not None
        assert native[0].orphan.status == "started"
        # native_load deletes orphans; it does not relabel them.
        assert native[0].reconcile is None

    def test_execution_plan_log_added_under_orchestration_schema(self):
        logs = resolve_compactable_logs(["dlt_a"], "dlt_orch")
        plans = [log for log in logs if log.label == "execution-plan log"]
        assert len(plans) == 1
        assert plans[0].schema == "dlt_orch"
        assert plans[0].key_columns == [
            "execution_id",
            "task_index",
            "pipeline_identifier",
        ]
        assert plans[0].terminal_statuses == ["completed", "failed", "abandoned"]
        # No orphan *deletion* for the plans log — dangling rows are relabelled,
        # not deleted (see reconcile below).
        assert plans[0].orphan is None
        # It carries a reconcile spec instead (relabel stale danglers).
        assert plans[0].reconcile is not None
        assert plans[0].reconcile.nonterminal_statuses == ["pending", "running"]
        assert plans[0].reconcile.abandoned_status == "abandoned"

    def test_historize_log_is_never_compacted(self):
        # The historize log only ever writes terminal rows (one per snapshot
        # run), so it has no superseded rows to collapse and must be absent.
        logs = resolve_compactable_logs(["dlt_a"], "dlt_orch")
        assert all(log.label != "historize log" for log in logs)

    def test_empty_and_duplicate_schemas_deduped(self):
        logs = resolve_compactable_logs(["dlt_a", "dlt_a", "", None], None)
        assert {log.schema for log in logs} == {"dlt_a"}


@pytest.mark.unit
class TestCollapseWhere:
    def test_native_collapse_predicate_shape(self):
        spec = _native_spec()
        where = _collapse_where(_FakeDest(), spec, "dlt_a.tbl")
        # Only non-terminal rows are deletable.
        assert "R.status NOT IN ('success', 'failed')" in where
        # Superseded == a strictly-later peer for the same key.
        assert "EXISTS (SELECT 1 FROM dlt_a.tbl AS S" in where
        assert (
            "COALESCE(S.finished_at, S.started_at) > "
            "COALESCE(R.finished_at, R.started_at)" in where
        )
        # NULL-safe key equality (generation is nullable).
        assert (
            "(S.generation = R.generation OR "
            "(S.generation IS NULL AND R.generation IS NULL))" in where
        )
        # Tie-break: a terminal row wins at equal order value (a started row and
        # its success can share a timestamp on a low-resolution clock).
        assert (
            "COALESCE(S.finished_at, S.started_at) = "
            "COALESCE(R.finished_at, R.started_at) "
            "AND S.status IN ('success', 'failed')" in where
        )

    def test_plans_collapse_uses_log_timestamp_ordering(self):
        spec = _plans_spec()
        where = _collapse_where(_FakeDest(), spec, "dlt_orch.plans")
        assert "R.status NOT IN ('completed', 'failed', 'abandoned')" in where
        assert "S.log_timestamp > R.log_timestamp" in where
        assert (
            "S.log_timestamp = R.log_timestamp "
            "AND S.status IN ('completed', 'failed', 'abandoned')" in where
        )


@pytest.mark.unit
class TestOrphanWhere:
    def test_native_orphan_predicate_shape(self):
        spec = _native_spec()
        where = _orphan_where(_FakeDest(), spec, "dlt_a.tbl")
        assert "R.status = 'started'" in where
        assert "R.started_at < now() - INTERVAL '1' DAY" in where
        # Must be the latest row (no strictly-later peer)...
        assert "NOT EXISTS (SELECT 1 FROM dlt_a.tbl AS S" in where
        # ...and the key must never have had a terminal row.
        assert "S.status IN ('success', 'failed')" in where


@pytest.mark.unit
class TestScalar:
    def test_index_access(self):
        assert _scalar([(7,)]) == 7

    def test_attribute_access(self):
        class Row:
            n = 4

        assert _scalar([Row()]) == 4

    def test_empty(self):
        assert _scalar([]) == 0

    def test_null_coerced_to_zero(self):
        assert _scalar([(None,)]) == 0


@pytest.mark.unit
class TestCompactLog:
    def test_absent_table_returns_none(self):
        spec = _native_spec()
        dest = _FakeDest(exists=False)
        assert compact_log(dest, spec, dry_run=False) is None

    def test_counts_and_deletes_collapse_and_orphan(self):
        spec = _native_spec()
        dest = _FakeDest(counts=[3, 2])  # collapse count, orphan count
        result = compact_log(dest, spec, dry_run=False)
        assert result == {"collapsed": 3, "orphaned": 2}
        assert len(dest.deletes) == 2  # collapse + orphan DELETEs issued

    def test_dry_run_counts_without_deleting(self):
        spec = _native_spec()
        dest = _FakeDest(counts=[3, 2])
        result = compact_log(dest, spec, dry_run=True)
        assert result == {"collapsed": 3, "orphaned": 2}
        assert dest.deletes == []

    def test_zero_matches_issues_no_delete(self):
        spec = _native_spec()
        dest = _FakeDest(counts=[0, 0])
        result = compact_log(dest, spec, dry_run=False)
        assert result == {"collapsed": 0, "orphaned": 0}
        assert dest.deletes == []

    def test_plans_log_has_no_orphan_pass(self):
        spec = _plans_spec()
        dest = _FakeDest(counts=[4])  # only the collapse count is consumed
        result = compact_log(dest, spec, dry_run=False)
        assert result == {"collapsed": 4, "orphaned": 0}
        assert len(dest.deletes) == 1


@pytest.mark.unit
class TestRunLogCompaction:
    def test_aggregates_and_counts_absent(self):
        native = _native_spec("dlt_a")
        plans = _plans_spec("dlt_orch")
        # native present (collapse 2, orphan 1); plans absent.
        dest = _FakeDest(counts=[2, 1])
        dest._exists_by_call = None

        class _Dest(_FakeDest):
            def __init__(self):
                super().__init__(counts=[2, 1])
                self._seen = 0

            def table_exists(self, schema, table):
                self._seen += 1
                return self._seen == 1  # first (native) exists, second absent

        d = _Dest()
        totals = run_log_compaction(d, [native, plans], dry_run=False)
        assert totals == {"absent": 1, "collapsed": 2, "orphaned": 1}


@pytest.mark.unit
class TestStaleTaskWhere:
    def test_targets_latest_stale_nonterminal_row(self):
        spec = _plans_spec()
        where = _stale_task_where(_FakeDest(), spec, "dlt_orch.plans")
        assert "R.status IN ('pending', 'running')" in where
        # Stale: the row's own order value is older than the cutoff.
        assert "R.log_timestamp < now() - INTERVAL '1' DAY" in where
        # Latest: nothing supersedes it.
        assert "NOT EXISTS (SELECT 1 FROM dlt_orch.plans AS S" in where


@pytest.mark.unit
class TestAbandonSetClause:
    def test_sets_abandoned_with_per_status_reason(self):
        spec = _plans_spec()
        set_clause = _abandon_set_clause(_FakeDest(), spec)
        assert "status = 'abandoned'" in set_clause
        # CASE reads the pre-update status; each maps to an attributed reason.
        assert "CASE R.status" in set_clause
        assert "WHEN 'running' THEN '[saga maintenance]" in set_clause
        assert "WHEN 'pending' THEN '[saga maintenance]" in set_clause
        assert "presumed crashed" in set_clause
        assert "never started" in set_clause
        # Cutoff hours interpolated into the reason text.
        assert "24h" in set_clause
        # Preserves any existing message for unexpected statuses.
        assert "ELSE R.error_message END" in set_clause


@pytest.mark.unit
class TestReconcileStaleTasks:
    def test_absent_table_is_noop(self):
        spec = _plans_spec()
        dest = _FakeDest(exists=False)
        assert reconcile_stale_tasks(dest, spec, dry_run=False) == 0
        assert dest.updates == []

    def test_counts_and_updates(self):
        spec = _plans_spec()
        dest = _FakeDest(counts=[7])
        assert reconcile_stale_tasks(dest, spec, dry_run=False) == 7
        assert len(dest.updates) == 1

    def test_dry_run_counts_without_updating(self):
        spec = _plans_spec()
        dest = _FakeDest(counts=[7])
        assert reconcile_stale_tasks(dest, spec, dry_run=True) == 7
        assert dest.updates == []

    def test_zero_matches_issues_no_update(self):
        spec = _plans_spec()
        dest = _FakeDest(counts=[0])
        assert reconcile_stale_tasks(dest, spec, dry_run=False) == 0
        assert dest.updates == []


@pytest.mark.unit
class TestRunStaleTaskReconcile:
    def test_only_logs_with_reconcile_spec_are_touched(self):
        native = _native_spec("dlt_a")  # orphan rule, no reconcile
        plans = _plans_spec("dlt_orch")  # reconcile spec

        class _Dest(_FakeDest):
            def __init__(self):
                super().__init__(counts=[5])

        d = _Dest()
        totals = run_stale_task_reconcile(d, [native, plans], dry_run=False)
        # native is skipped (no reconcile); only the plans UPDATE runs.
        assert totals == {"abandoned": 5}
        assert len(d.updates) == 1
