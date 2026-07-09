"""saga lint — static checks that flag adapter & config anti-patterns.

The scaffolder (`saga new adapter`) and the AI-authoring context steer *new* code
toward the framework conventions; this catches divergence in code that already
exists (older adapters, hand edits, AI output that drifted). Checks are
high-signal and conservative — a linter that cries wolf gets ignored.

Extensible: a check is a callable ``(AdapterTarget) -> Iterable[Finding]`` listed
in ``CHECKS``. Add one and it runs over every discovered adapter.
"""

from __future__ import annotations

import ast
import dataclasses
import difflib
import importlib
from dataclasses import dataclass
from pathlib import Path
from typing import Callable, Iterable, List, Optional

SEVERITY_WARNING = "warning"
SEVERITY_ERROR = "error"

# Directory segments that mark installed (non-editable) code.
_INSTALLED_DIR_MARKERS = {"site-packages", "dist-packages"}

# Suffixes/names that describe secrecy rather than content.
_SECRET_NAME_SUFFIXES = ("_secret", "_plaintext")
_SECRET_NAME_EXACT = {"secret", "plaintext"}

# Similarity threshold for "did you mean a standard field?". Tuned empirically to
# catch abbreviations/pluralisations of standard fields (incremental_col→
# incremental_column 0.91, primary_keys→primary_key 0.96, snapshot_col 0.89)
# while clearing legitimately distinct fields that merely share a prefix
# (partition_on 0.86, partition_num 0.83, project_id 0.83 — all real, kept).
_NEAR_MISS_CUTOFF = 0.88


@dataclass
class Finding:
    """One lint result for an adapter."""

    adapter: str
    code: str
    message: str
    severity: str = SEVERITY_WARNING
    location: Optional[str] = None  # field name or file:line
    suggestion: Optional[str] = None


@dataclass
class AdapterTarget:
    """An adapter to lint: its config dataclass and pipeline source file."""

    adapter: str
    source: str  # "built-in" | "external" | "local" | ...
    config_class: Optional[type]
    pipeline_file: Optional[Path]


# ---------------------------------------------------------------------------
# Standard vocabulary
# ---------------------------------------------------------------------------


def standard_field_names() -> set:
    """Names of the blessed config fields (BaseConfig + TargetConfig + Historize)."""
    from dlt_saga.historize.config import HistorizeConfig
    from dlt_saga.pipelines.base_config import BaseConfig
    from dlt_saga.pipelines.target.config import TargetConfig

    names: set = set()
    for cls in (BaseConfig, TargetConfig, HistorizeConfig):
        names.update(f.name for f in dataclasses.fields(cls))
    return names


def _config_field_names(config_class: type) -> List[str]:
    return [
        f.name for f in dataclasses.fields(config_class) if not f.name.startswith("_")
    ]


# ---------------------------------------------------------------------------
# Checks
# ---------------------------------------------------------------------------


def check_secret_field_names(target: AdapterTarget) -> Iterable[Finding]:
    """Flag fields named by secrecy (``*_secret``) rather than by content."""
    if target.config_class is None:
        return
    for name in _config_field_names(target.config_class):
        if name.endswith(_SECRET_NAME_SUFFIXES) or name in _SECRET_NAME_EXACT:
            yield Finding(
                adapter=target.adapter,
                code="secret-naming",
                message=(
                    f"Config field '{name}' is named by secrecy. Any field accepts a "
                    "secret URI (googlesecretmanager::…/azurekeyvault::…/env_secret::…) just "
                    "as well as a plain value, so the name carries no guarantee — "
                    "name it by what it holds and type it as SecretStr."
                ),
                location=name,
                suggestion="rename to its content, e.g. api_token / password / connection_string",
            )


def check_field_name_near_miss(target: AdapterTarget) -> Iterable[Finding]:
    """Flag a config field that closely resembles a standard one (likely a
    divergence from the shared vocabulary)."""
    if target.config_class is None:
        return
    standard = standard_field_names()
    for name in _config_field_names(target.config_class):
        if name in standard:
            continue
        match = difflib.get_close_matches(name, standard, n=1, cutoff=_NEAR_MISS_CUTOFF)
        if match:
            yield Finding(
                adapter=target.adapter,
                code="field-name",
                message=(
                    f"Config field '{name}' closely matches the standard field "
                    f"'{match[0]}'. Reuse the standard vocabulary when you mean the same "
                    "concept so configs stay consistent across adapters."
                ),
                location=name,
                suggestion=f"did you mean '{match[0]}'?",
            )


def _has_call_to(tree: ast.AST, attr_names: set) -> Optional[int]:
    """Line number of the first call to ``<x>.<attr>()`` for attr in *attr_names*."""
    for node in ast.walk(tree):
        if (
            isinstance(node, ast.Call)
            and isinstance(node.func, ast.Attribute)
            and node.func.attr in attr_names
        ):
            return node.lineno
    return None


def _references_timedelta(tree: ast.AST) -> bool:
    for node in ast.walk(tree):
        if isinstance(node, ast.Name) and node.id == "timedelta":
            return True
        if isinstance(node, ast.Attribute) and node.attr == "timedelta":
            return True
    return False


def check_hardcoded_window(target: AdapterTarget) -> Iterable[Finding]:
    """Heuristic: flag a pipeline that derives a date window from "now" with no
    warehouse watermark — i.e. "always load yesterday", which can't catch up
    after a missed or failed run."""
    pf = target.pipeline_file
    if pf is None or not pf.exists():
        return
    try:
        src = pf.read_text(encoding="utf-8")
        tree = ast.parse(src)
    except (OSError, SyntaxError):
        return

    now_line = _has_call_to(tree, {"now", "today", "utcnow"})
    if now_line is None or not _references_timedelta(tree):
        return

    # Resuming from the warehouse high-water mark is the idempotent pattern; if
    # any of these appear, assume the date math is a fallback, not the cursor.
    watermark_markers = ("get_max_column_value", "initial_value", "get_last_load")
    if any(marker in src for marker in watermark_markers):
        return

    yield Finding(
        adapter=target.adapter,
        code="hardcoded-window",
        message=(
            "Pipeline derives a date window from the current date (datetime.now/"
            "date.today + timedelta) with no warehouse watermark "
            "(get_max_column_value / initial_value). This loads a fixed window (e.g. "
            "'yesterday') and won't catch up after a missed or failed run — resume "
            "from the high-water mark instead. (Heuristic — ignore if intentional.)"
        ),
        location=f"{pf}:{now_line}",
        suggestion="seed from get_max_column_value(...) or initial_value",
    )


CHECKS: List[Callable[[AdapterTarget], Iterable[Finding]]] = [
    check_secret_field_names,
    check_field_name_near_miss,
    check_hardcoded_window,
]


# ---------------------------------------------------------------------------
# Target collection + runner
# ---------------------------------------------------------------------------


def is_project_owned(pipeline_file: Optional[Path], project_root: Path) -> bool:
    """True when *pipeline_file* is editable code inside this project.

    The reliable signal for "the user can fix this" is location, not how the
    adapter was registered: built-ins and installed third-party adapters live in
    site-packages, and a packages.yml entry can point at a cloned repo outside
    the project — all of which we skip. Only source under the project root (and
    not in an installed-packages dir) counts as owned.
    """
    if pipeline_file is None:
        return False
    try:
        resolved = pipeline_file.resolve()
    except OSError:
        return False
    if _INSTALLED_DIR_MARKERS & {part.lower() for part in resolved.parts}:
        return False
    try:
        resolved.relative_to(project_root.resolve())
        return True
    except ValueError:
        return False


def collect_targets(include_all: bool = False) -> List[AdapterTarget]:
    """Build a lint target per adapter.

    By default only the project's own (editable, in-project) adapters are
    returned — built-in and installed third-party adapters can't be changed by
    the user. Pass ``include_all=True`` to lint everything.
    """
    from dlt_saga.pipelines.registry import discover_implementations
    from dlt_saga.utility.generate_schemas import config_class_for_adapter
    from dlt_saga.utility.project_root import find_project_root

    project_root = find_project_root()
    targets: List[AdapterTarget] = []
    for impl in discover_implementations():
        adapter = impl["adapter"]

        pipeline_file: Optional[Path] = None
        try:
            module = importlib.import_module(impl["module"])
            if getattr(module, "__file__", None):
                pipeline_file = Path(module.__file__)
        except Exception:
            pipeline_file = None

        if not include_all and not is_project_owned(pipeline_file, project_root):
            continue

        try:
            config_class = config_class_for_adapter(adapter)
        except Exception:
            config_class = None

        targets.append(
            AdapterTarget(
                adapter, impl.get("source", "external"), config_class, pipeline_file
            )
        )
    return targets


def lint_target(target: AdapterTarget) -> List[Finding]:
    """Run every registered check against a single target."""
    findings: List[Finding] = []
    for check in CHECKS:
        findings.extend(check(target))
    return findings


def run_lint(
    targets: Optional[List[AdapterTarget]] = None, include_all: bool = False
) -> List[Finding]:
    """Run all checks over all targets (discovering them if not supplied)."""
    if targets is None:
        targets = collect_targets(include_all=include_all)
    findings: List[Finding] = []
    for target in targets:
        findings.extend(lint_target(target))
    return findings
