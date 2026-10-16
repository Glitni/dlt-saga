"""Per-``task_group`` concurrency caps (``task_concurrency``).

``task_group`` runs a set of pipelines together as one unit — in orchestrated
runs, one Cloud Run task. ``task_concurrency`` caps how many of a group's
pipelines run at the same time, for sources that limit concurrent requests
across what are otherwise independent pipelines. The cap holds in every
execution mode: local runs, and the worker that executes the group's task.

The cap is a property of the group, but it's set per pipeline (usually once,
through folder defaults in ``saga_project.yml``). Members that disagree are a
configuration error rather than resolved to the smallest value, which would hide
the mistake. Like the target-collision guard, agreement is checked
project-wide: a selected pipeline is refused when its group disagrees anywhere
in the project, even if the disagreeing member isn't part of this run.

Scheduling doesn't block pool threads: a capped group runs as ``cap`` lanes,
each draining the group's queue in order, so a waiting group member never holds
a slot another pipeline could use.
"""

from collections import deque
from concurrent.futures import Executor, Future
from typing import Callable, Deque, Dict, List, Optional, Tuple, TypeVar

from dlt_saga.pipeline_config import ConfigSource, PipelineConfig
from dlt_saga.utility.collisions import discover_enabled_configs

_T = TypeVar("_T")

_UNSET = "unset"


class TaskConcurrencyError(ValueError):
    """Raised when ``task_concurrency`` is invalid or a group's members disagree.

    A configuration error (subclasses ``ValueError``): callers render it as a
    user-facing message without a traceback.
    """


def _task_group(config: PipelineConfig) -> Optional[str]:
    return config.config_dict.get("task_group") or None


def _raw_concurrency(config: PipelineConfig) -> object:
    return config.config_dict.get("task_concurrency")


def _display(value: object) -> object:
    return _UNSET if value is None else value


def _is_valid_concurrency(value: object) -> bool:
    return isinstance(value, int) and not isinstance(value, bool) and value >= 1


def _config_problems(config: PipelineConfig) -> List[str]:
    """Problems visible on one config alone."""
    value = _raw_concurrency(config)
    if value is None:
        return []
    if not _is_valid_concurrency(value):
        return [f"task_concurrency must be an integer >= 1, got {value!r}"]
    if _task_group(config) is None:
        return [
            "task_concurrency is set but task_group is not — the cap applies "
            "to the pipelines sharing a task_group"
        ]
    return []


def _group_problems(configs: List[PipelineConfig]) -> Dict[str, List[str]]:
    """Disagreement within each task_group, keyed by member pipeline name."""
    members: Dict[str, List[PipelineConfig]] = {}
    for config in configs:
        group = _task_group(config)
        if group is not None:
            members.setdefault(group, []).append(config)

    problems: Dict[str, List[str]] = {}
    for group, group_configs in members.items():
        values = {repr(_raw_concurrency(c)) for c in group_configs}
        if len(values) <= 1:
            continue
        listing = ", ".join(
            f"{c.pipeline_name}: {_display(_raw_concurrency(c))}"
            for c in sorted(group_configs, key=lambda c: c.pipeline_name)
        )
        message = (
            f"task_group '{group}' members disagree on task_concurrency "
            f"({listing}) — set one value for the whole group"
        )
        for config in group_configs:
            problems.setdefault(config.pipeline_name, []).append(message)
    return problems


def task_concurrency_problems(
    selected_configs: List[PipelineConfig],
    source: Optional[ConfigSource] = None,
) -> Dict[str, List[str]]:
    """Problems touching ``selected_configs``, keyed by pipeline name.

    Per-config problems (an invalid value, a cap without a group) are reported
    for the selection. Group disagreement is detected across every enabled
    config in the project (falling back to the selection when ``source`` is
    None or discovery fails) and reported for the selected members.
    """
    problems: Dict[str, List[str]] = {}
    for config in selected_configs:
        config_problems = _config_problems(config)
        if config_problems:
            problems[config.pipeline_name] = config_problems

    project_configs = discover_enabled_configs(source) if source else None
    if project_configs is None:
        project_configs = list(selected_configs)

    selected_names = {config.pipeline_name for config in selected_configs}
    for name, messages in _group_problems(project_configs).items():
        if name in selected_names:
            problems.setdefault(name, []).extend(messages)
    return problems


def check_task_concurrency(
    configs: List[PipelineConfig],
    source: Optional[ConfigSource] = None,
) -> None:
    """Raise :class:`TaskConcurrencyError` if any selected config has a problem.

    Run before any pipeline executes, alongside the target-collision guard.
    """
    problems = task_concurrency_problems(configs, source)
    if not problems:
        return
    lines = sorted({m for messages in problems.values() for m in messages})
    raise TaskConcurrencyError(
        "Invalid task_concurrency configuration:\n"
        + "\n".join(f"  - {line}" for line in lines)
    )


def _cap(config: PipelineConfig) -> Optional[int]:
    """The config's cap, or None when it runs unconstrained."""
    value = _raw_concurrency(config)
    if _task_group(config) is None or not _is_valid_concurrency(value):
        return None
    return value  # type: ignore[return-value]


def parallel_slots(configs: List[PipelineConfig]) -> int:
    """Most pipelines from ``configs`` that can run at once under the caps."""
    uncapped = 0
    capped: Dict[str, Tuple[int, int]] = {}
    for config in configs:
        cap = _cap(config)
        if cap is None:
            uncapped += 1
        else:
            group = _task_group(config)
            assert group is not None
            _, count = capped.get(group, (cap, 0))
            capped[group] = (cap, count + 1)
    return uncapped + sum(min(cap, count) for cap, count in capped.values())


def submit_with_group_caps(
    executor: Executor,
    jobs: List[Tuple[PipelineConfig, Callable[[], _T]]],
) -> Dict["Future[_T]", PipelineConfig]:
    """Submit each job, honoring its ``task_group``'s ``task_concurrency``.

    Uncapped jobs are submitted directly. A capped group is submitted as
    ``min(cap, members)`` lanes that drain the group's queue in order. Every job
    gets its own future either way, so callers consume the result with
    ``as_completed`` exactly as they would plain ``executor.submit`` futures.

    Callers validate with :func:`check_task_concurrency` first; a group is keyed
    on its first member's cap.
    """
    futures: Dict["Future[_T]", PipelineConfig] = {}
    queues: Dict[str, Deque[Tuple[Callable[[], _T], "Future[_T]"]]] = {}
    caps: Dict[str, int] = {}
    order: List[object] = []  # direct jobs and group names, in first-seen order

    for config, job in jobs:
        cap = _cap(config)
        group = _task_group(config)
        if cap is None or group is None:
            order.append((config, job))
            continue
        if group not in queues:
            queues[group] = deque()
            caps[group] = cap
            order.append(group)
        future: "Future[_T]" = Future()
        queues[group].append((job, future))
        futures[future] = config

    # Queues are complete before any lane starts — a lane that found its queue
    # empty would exit and strand members appended after it.
    for item in order:
        if isinstance(item, str):
            queue = queues[item]
            for _ in range(min(caps[item], len(queue))):
                executor.submit(_drain, queue)
        else:
            config, job = item  # type: ignore[misc]
            futures[executor.submit(job)] = config
    return futures


def _drain(queue: Deque[Tuple[Callable[[], _T], "Future[_T]"]]) -> None:
    """Run queued jobs one at a time, resolving each job's future."""
    while True:
        try:
            job, future = queue.popleft()
        except IndexError:
            return
        if not future.set_running_or_notify_cancel():
            continue
        try:
            result = job()
        except Exception as exc:
            future.set_exception(exc)
        except BaseException as exc:
            # Interrupted: fail this job and cancel the rest so callers waiting
            # on the futures don't hang, then let the interrupt propagate.
            future.set_exception(exc)
            while queue:
                queue.popleft()[1].cancel()
            raise
        else:
            future.set_result(result)
