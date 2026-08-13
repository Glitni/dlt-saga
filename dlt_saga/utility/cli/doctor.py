"""Health-check helpers for the ``saga doctor`` command.

Each ``_doctor_*`` function performs one read-only check of the *environment*
(profile, project config, pipeline discovery with resolved schema, destination
connectivity, plugin imports) and reports its result through an
``emit(symbol, label, detail)`` callback so the command body stays a thin
orchestrator. Checks never mutate state; the connectivity probe opens and
immediately closes a connection.

Config *correctness* (write_disposition, adapters, source fields, historize
config, target collisions, deprecated keys) is offline and lives in
``dlt_saga.validate`` behind ``saga validate`` — doctor is the connectivity /
environment counterpart (dbt debug to validate's dbt parse).
"""

from pathlib import Path
from typing import TYPE_CHECKING, Callable, List, Optional

import typer

from dlt_saga.utility.cli.common import (
    discover_and_select_configs,
    execute_with_impersonation,
    flatten_configs,
    load_profile_config,
    setup_execution_context,
)
from dlt_saga.utility.cli.context import get_execution_context
from dlt_saga.utility.project_root import find_project_root

if TYPE_CHECKING:
    from dlt_saga.utility.cli.context import ExecutionContext
    from dlt_saga.utility.cli.profiles import ProfileTarget


def _doctor_check(label: str, module_path: str) -> bool:
    """Try to import *module_path* and print OK / FAIL. Returns True on success."""
    import importlib

    try:
        importlib.import_module(module_path)
        typer.echo(f"  OK    {label}")
        return True
    except ImportError as e:
        typer.echo(f"  FAIL  {label}: {e}")
        return False


def _doctor_check_profile(
    profile: Optional[str],
    target: Optional[str],
    verbose: bool,
    emit: Callable[..., None],
) -> tuple:
    """Load and validate profile. Returns (profile_target, context, dest_type) or raises Exit."""
    import traceback

    from dlt_saga.utility.cli.common import resolve_profile_name

    try:
        resolved_profile = resolve_profile_name(profile)
        profile_target = load_profile_config(profile, target)
        setup_execution_context(profile_target)
        context = get_execution_context()
        dest_type = context.get_destination_type() or "unknown"
        env = context.get_environment() or "unknown"
        active_target = target or (profile_target.name if profile_target else "dev")
        # Surface the resolved dev schema. context.get_schema() is the profile
        # target's rendered `schema` (dbt-style `env_var()` already applied); in
        # dev that IS the schema pipelines land in, so an empty/None value here
        # is exactly what silently falls back to `dlt_dev` at run time. In prod
        # the schema is per-group, so only show this when the profile sets one.
        schema = context.get_schema()
        schema_detail = f", schema={schema}" if schema else ""
        emit(
            "✓",
            "profiles.yml",
            f"[{resolved_profile} → {active_target}, {dest_type}, {env}{schema_detail}]",
        )
        return profile_target, context, dest_type
    except Exception as e:
        emit("✗", "profiles.yml", str(e) if verbose else str(e).splitlines()[0])
        if verbose:
            typer.echo(traceback.format_exc())
        typer.echo("")
        typer.echo("Cannot continue without a valid profile.")
        raise typer.Exit(1)


def _doctor_check_project(verbose: bool, emit: Callable[..., None]) -> bool:
    """Validate saga_project.yml. Returns True on success."""
    import traceback

    from dlt_saga.project_config import get_project_config

    try:
        get_project_config()
        emit("✓", "saga_project.yml")
        return True
    except Exception as e:
        emit("✗", "saga_project.yml", str(e) if verbose else str(e).splitlines()[0])
        if verbose:
            typer.echo(traceback.format_exc())
        return False


def _describe_project_lookalike(loc_label: str, stem: str, ext: str) -> str:
    """Explain every way a project-defaults look-alike differs from the canonical
    ``saga_project.yml`` at the project root, so the warning points straight at
    the fix.
    """
    issues = []
    if stem.startswith("dlt"):
        issues.append("old docs name (rename to saga_project)")
    if "-" in stem:
        issues.append("hyphen instead of underscore")
    if ext != ".yml":
        issues.append(f"{ext} instead of .yml")
    if loc_label:
        issues.append("in configs/ instead of the project root")
    return ", ".join(issues) or "misplaced"


def _doctor_check_stray_project_config(emit: Callable[..., None]) -> None:
    """Warn when a project-defaults file looks misplaced or misnamed.

    Project defaults are read from ``saga_project.yml`` at the project root
    (``find_project_root()``; see :func:`dlt_saga.project_config.get_project_config`).
    A file named ``dlt_project.yml`` (the name the docs used before #459), a
    ``saga_project.yml`` left inside ``configs/`` instead of the root, or a
    near-miss spelling at the root is **never read**: the hierarchical merge
    short-circuits on an empty project config (``FilePipelineConfig._resolve_config``)
    and every shared default (``tags``, ``schema_access``, ``adapter``,
    ``write_disposition``) is silently dropped while pipelines still run green.

    Emitted as a warning (``!``), never a failure — these are all valid files to
    have lying around, and ``doctor`` should not exit non-zero on a project that
    works. Stays silent when the canonical file is present (a shadowed look-alike
    is harmless) to avoid noise.
    """
    root = find_project_root()
    canonical = root / "saga_project.yml"

    # Every look-alike is the correct/old base name crossed with a separator or
    # extension typo and the two directories people drop it in. Enumerating the
    # matrix (rather than a hand-listed set) closes the whole class of near-miss
    # — `dlt-project.yml`, `saga_project.yaml`, `configs/saga-project.yml`, … —
    # instead of whichever variants someone happened to think of. Only the
    # canonical file is read; anything else here is silently ignored.
    stems = ["saga_project", "saga-project", "dlt_project", "dlt-project"]
    exts = [".yml", ".yaml"]
    locations = [("", root), ("configs/", root / "configs")]

    stray = []
    for loc_label, loc_dir in locations:
        for stem in stems:
            for ext in exts:
                path = loc_dir / f"{stem}{ext}"
                if path == canonical or not path.exists():
                    continue
                stray.append((path, _describe_project_lookalike(loc_label, stem, ext)))

    if canonical.exists():
        # The correct file is being read; any look-alike is inert. Keep quiet.
        return

    if stray:
        found = "; ".join(f"{path} ({why})" for path, why in stray)
        emit(
            "!",
            "Project defaults",
            f"{canonical} is missing, so shared defaults (tags, schema_access, "
            f"adapter, write_disposition) are silently ignored. Found {found} — "
            "rename/move it to that path",
        )
        return

    # No project file and no look-alike. Only worth flagging when the config tree
    # has more than one pipeline group — that combination is far more often an
    # oversight than a deliberate choice to forgo shared defaults.
    configs_dir = root / "configs"
    if configs_dir.is_dir():
        groups = [d for d in configs_dir.iterdir() if d.is_dir()]
        if len(groups) > 1:
            emit(
                "!",
                "Project defaults",
                f"no saga_project.yml at {root}, but {len(groups)} pipeline "
                "groups in configs/ — shared defaults have nowhere to live "
                f"(create {canonical})",
            )


def _doctor_emit_version(emit: Callable[..., None]) -> None:
    """Report the active dlt-saga version and whether it's an editable/local
    checkout or an installed (PyPI) build.

    This is the fast answer to "which dlt-saga am I actually running?" — the
    exact question that bites when switching between a local editable install
    (``uv pip install -e``) and the pinned release (``uv sync``).
    """
    from importlib.metadata import PackageNotFoundError, version

    import dlt_saga

    try:
        ver = version("dlt-saga")
    except PackageNotFoundError:
        ver = "unknown"
    location = Path(dlt_saga.__file__).resolve().parent
    # An editable/local checkout lives outside site-packages; a released build
    # is unpacked into site-packages. Good enough to tell the two apart.
    kind = (
        "editable/local" if "site-packages" not in location.as_posix() else "installed"
    )
    emit("✓", f"dlt-saga {ver}", f"[{kind}] {location}")


def _doctor_check_configs(
    select: Optional[List[str]],
    context: "ExecutionContext",
    verbose: bool,
    emit: Callable[..., None],
) -> dict:
    """Discover pipeline configs and report their resolved destination targets.

    Prints the distinct schema(s) the selected pipelines resolve to — the
    single most useful value for catching a mis-resolved schema (e.g. a profile
    that fell back to ``dlt_dev`` because ``env_var()`` was empty at load time)
    without having to run ``saga ingest``. When a ``--select`` is given (or in
    ``--verbose``), each pipeline's full ``project.schema.table`` is listed too.

    Returns the selected configs (empty dict on failure).
    """
    import traceback

    try:
        selected, _ = discover_and_select_configs(select)
        total = sum(len(v) for v in selected.values())
        flat = flatten_configs(selected) if selected else []
        schemas = sorted({c.schema_name for c in flat if c.schema_name})
        schema_detail = f" → schema(s): {', '.join(schemas)}" if schemas else ""
        emit(
            "✓",
            "Pipeline configs",
            f"{total} pipeline(s) in {len(selected)} group(s){schema_detail}",
        )

        # Per-pipeline resolved target: useful when zooming in on one pipeline
        # (or debugging naming), noisy for a whole project — so gate on --select
        # or --verbose. Cap the list but report how many were hidden (never a
        # silent truncation).
        if flat and (select or verbose):
            project = context.get_database()
            limit = len(flat) if verbose else 20
            for c in flat[:limit]:
                target = ".".join(
                    p for p in (project, c.schema_name, c.table_name) if p
                )
                typer.echo(f"        {c.pipeline_name} → {target}")
            hidden = len(flat) - limit
            if hidden > 0:
                typer.echo(f"        … and {hidden} more (use --verbose to list all)")
        return selected
    except Exception as e:
        emit("✗", "Pipeline configs", str(e) if verbose else str(e).splitlines()[0])
        if verbose:
            typer.echo(traceback.format_exc())
        return {}


def _doctor_check_destination(
    dest_type: str,
    context: "ExecutionContext",
    selected_configs: dict,
    profile_target: Optional["ProfileTarget"],
    verbose: bool,
    emit: Callable[..., None],
) -> bool:
    """Verify destination connectivity, then point at internal-table maintenance.

    Connects (inside any impersonation setup) to prove connectivity, then emits a
    static pointer to ``saga maintenance --dry-run``. Internal-table state
    (clustering drift, log growth) is deliberately *not* probed here: clustering
    rarely drifts (only after an upgrade) and measuring reclaimable rows is a
    costly self-join, so both are deferred to the maintenance preview to keep
    doctor a fast health check.
    """
    import traceback

    from dlt_saga.utility.cli.common import build_destination_from_configs

    try:
        destination = build_destination_from_configs(
            dest_type, context, selected_configs
        )
    except Exception as e:
        emit("✗", f"Connection ({dest_type})", _doctor_error_detail(e, verbose))
        if verbose:
            typer.echo(traceback.format_exc())
        return False

    def _probe() -> None:
        destination.connect()
        destination.close()

    try:
        execute_with_impersonation(profile_target, _probe)
    except Exception as e:
        emit("✗", f"Connection ({dest_type})", _doctor_error_detail(e, verbose))
        if verbose:
            typer.echo(traceback.format_exc())
        return False

    emit("✓", f"Connection ({dest_type})")
    if selected_configs:
        emit(
            "→",
            "Internal-table maintenance",
            "run `saga maintenance --dry-run` to preview clustering + "
            "log-growth cleanup",
        )
    return True


def _doctor_error_detail(exc: Exception, verbose: bool) -> str:
    """Full message in verbose mode, first line otherwise."""
    return str(exc) if verbose else str(exc).splitlines()[0]
