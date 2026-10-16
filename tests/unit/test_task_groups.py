"""Unit tests for per-task_group concurrency caps (``task_concurrency``)."""

import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from functools import partial
from types import SimpleNamespace

import pytest

from dlt_saga.utility.task_groups import (
    TaskConcurrencyError,
    check_task_concurrency,
    parallel_slots,
    submit_with_group_caps,
    task_concurrency_problems,
)

pytestmark = pytest.mark.unit


def _config(name, task_group=None, task_concurrency=None):
    config_dict = {}
    if task_group is not None:
        config_dict["task_group"] = task_group
    if task_concurrency is not None:
        config_dict["task_concurrency"] = task_concurrency
    return SimpleNamespace(pipeline_name=name, config_dict=config_dict)


class _FakeSource:
    def __init__(self, configs):
        self._configs = configs

    def discover(self):
        return {"group": self._configs}, {}


class _FailingSource:
    def discover(self):
        raise RuntimeError("boom")


class TestProblems:
    def test_agreeing_group_is_clean(self):
        configs = [_config("a", "g", 1), _config("b", "g", 1), _config("c")]
        assert task_concurrency_problems(configs) == {}

    def test_group_without_cap_is_clean(self):
        assert task_concurrency_problems([_config("a", "g"), _config("b", "g")]) == {}

    @pytest.mark.parametrize("value", [0, -1, "1", 1.5, True])
    def test_invalid_value(self, value):
        problems = task_concurrency_problems([_config("a", "g", value)])
        assert "integer >= 1" in problems["a"][0]

    def test_cap_without_group(self):
        problems = task_concurrency_problems([_config("a", task_concurrency=1)])
        assert "task_group is not" in problems["a"][0]

    def test_disagreeing_values_flag_every_member(self):
        configs = [_config("a", "g", 1), _config("b", "g", 2)]
        problems = task_concurrency_problems(configs)
        assert set(problems) == {"a", "b"}
        assert "a: 1, b: 2" in problems["a"][0]

    def test_unset_member_disagrees_with_set_member(self):
        problems = task_concurrency_problems([_config("a", "g", 1), _config("b", "g")])
        assert "b: unset" in problems["a"][0]

    def test_disagreement_with_unselected_member_is_found_project_wide(self):
        selected = _config("a", "g", 1)
        source = _FakeSource([selected, _config("b", "g", 2)])
        problems = task_concurrency_problems([selected], source)
        assert set(problems) == {"a"}

    def test_unrelated_project_disagreement_does_not_touch_selection(self):
        selected = _config("a", "g", 1)
        source = _FakeSource([selected, _config("x", "h", 1), _config("y", "h", 2)])
        assert task_concurrency_problems([selected], source) == {}

    def test_discovery_failure_falls_back_to_selection(self):
        configs = [_config("a", "g", 1), _config("b", "g", 2)]
        assert set(task_concurrency_problems(configs, _FailingSource())) == {"a", "b"}

    def test_check_raises_config_error(self):
        with pytest.raises(TaskConcurrencyError) as exc_info:
            check_task_concurrency([_config("a", "g", 1), _config("b", "g", 2)])
        assert isinstance(exc_info.value, ValueError)

    def test_check_passes_when_clean(self):
        check_task_concurrency([_config("a", "g", 1), _config("b", "g", 1)])


class TestParallelSlots:
    def test_uncapped_configs_each_take_a_slot(self):
        assert parallel_slots([_config("a"), _config("b", "g"), _config("c", "g")]) == 3

    def test_capped_group_takes_at_most_its_cap(self):
        configs = [_config(n, "g", 2) for n in "abcd"] + [_config("e")]
        assert parallel_slots(configs) == 3

    def test_cap_larger_than_group(self):
        assert parallel_slots([_config("a", "g", 5), _config("b", "g", 5)]) == 2


def _run(configs, workers, duration=0.05):
    """Run every config through submit_with_group_caps, tracking peak overlap."""
    lock = threading.Lock()
    active = {}
    peak = {}

    def job(config):
        key = config.config_dict.get("task_group") or "__all__"
        with lock:
            active[key] = active.get(key, 0) + 1
            peak[key] = max(peak.get(key, 0), active[key])
            active["__total__"] = active.get("__total__", 0) + 1
            peak["__total__"] = max(peak.get("__total__", 0), active["__total__"])
        time.sleep(duration)
        with lock:
            active[key] -= 1
            active["__total__"] -= 1
        return config.pipeline_name

    with ThreadPoolExecutor(max_workers=workers) as executor:
        futures = submit_with_group_caps(
            executor, [(c, partial(job, c)) for c in configs]
        )
        results = {futures[f].pipeline_name: f.result() for f in as_completed(futures)}
    return results, peak


class TestSubmitWithGroupCaps:
    def test_capped_group_never_exceeds_cap(self):
        configs = [_config(f"g{i}", "g", 1) for i in range(4)]
        results, peak = _run(configs, workers=4)
        assert set(results) == {c.pipeline_name for c in configs}
        assert peak["g"] == 1

    def test_cap_of_two(self):
        configs = [_config(f"g{i}", "g", 2) for i in range(6)]
        _, peak = _run(configs, workers=8)
        assert peak["g"] == 2

    def test_waiting_group_members_do_not_hold_pool_slots(self):
        # Two workers, a serial group submitted ahead of two free pipelines. If
        # waiting members blocked pool threads, the free pipelines would queue
        # behind the whole group; with lanes they finish while it's still going.
        configs = [_config(f"g{i}", "g", 1) for i in range(4)]
        configs += [_config("free1"), _config("free2")]
        finished = []

        def job(config):
            time.sleep(0.05)
            finished.append(config.pipeline_name)

        with ThreadPoolExecutor(max_workers=2) as executor:
            futures = submit_with_group_caps(
                executor, [(c, partial(job, c)) for c in configs]
            )
            for future in as_completed(futures):
                future.result()

        assert finished[-1] == "g3"
        assert finished.index("free2") < finished.index("g3")

    def test_uncapped_group_runs_in_parallel(self):
        configs = [_config(f"g{i}", "g") for i in range(4)]
        _, peak = _run(configs, workers=4)
        assert peak["g"] > 1

    def test_failure_is_reported_on_its_own_future_and_group_continues(self):
        configs = [_config(n, "g", 1) for n in ("a", "b", "c")]

        def job(config):
            if config.pipeline_name == "b":
                raise RuntimeError("b failed")
            return config.pipeline_name

        with ThreadPoolExecutor(max_workers=2) as executor:
            futures = submit_with_group_caps(
                executor, [(c, partial(job, c)) for c in configs]
            )
            outcomes = {}
            for future in as_completed(futures):
                name = futures[future].pipeline_name
                outcomes[name] = future.exception() or future.result()

        assert outcomes["a"] == "a"
        assert outcomes["c"] == "c"
        assert str(outcomes["b"]) == "b failed"
