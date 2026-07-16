"""Health-check helpers for the ``saga doctor`` command.

Each ``_doctor_*`` function performs one read-only check (profile, project
config, pipeline discovery, target collisions, deprecated config keys,
destination connectivity, plugin imports) and reports its result through an
``emit(symbol, label, detail)`` callback so the command body stays a thin
orchestrator. Checks never mutate state; the connectivity probe opens and
immediately closes a connection.

Extracted from ``cli.py`` (module-boundary cleanup); behavior unchanged. The
``doctor`` command itself stays in ``cli.py`` and calls these helpers.
"""

import os
from pathlib import Path
from typing import TYPE_CHECKING, Callable, List, Optional

import typer

from dlt_saga.utility.cli.common import (
    discover_and_select_configs,
    execute_with_impersonation,
    flatten_configs,
    get_config_source,
    load_profile_config,
    setup_execution_context,
)
from dlt_saga.utility.cli.context import get_execution_context

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


def _doctor_check_collisions(
    selected_configs: dict,
    verbose: bool,
    emit: Callable[..., None],
) -> bool:
    """Flag pipelines that resolve to the same destination table.

    Read-only counterpart to the run-time guard. Detection is project-wide
    (:func:`collisions_for_selection`), so ``saga doctor --select <one>`` still
    finds a collision with an unselected pipeline — the CI pattern of running
    doctor over just the changed configs surfaces what they collide with. The
    default (whole-project) run reports every collision. Reuses the configs
    already discovered by :func:`_doctor_check_configs`, so it costs no extra
    discovery.
    """
    from dlt_saga.utility.collisions import collisions_for_selection
    from dlt_saga.utility.naming import get_environment

    flat = flatten_configs(selected_configs) if selected_configs else []
    if not flat:
        return True

    # doctor is the comprehensive audit: check the current environment and prod
    # (deduped), so a dev run flags both dev-only collisions and latent prod
    # ones before they ship.
    current_env = get_environment()
    environments = [current_env] if current_env == "prod" else [current_env, "prod"]

    try:
        collisions = collisions_for_selection(
            flat,
            get_config_source(),
            environments=environments,
            check_ingest=True,
            check_historize=True,
        )
    except Exception as e:
        # Never let the diagnosis itself crash the health check.
        emit("!", "Target collisions", f"could not check: {e}")
        return True

    if not collisions:
        emit(
            "✓",
            "Target collisions",
            "none — each pipeline maps to its own table",
        )
        return True

    total = sum(len(c.pipelines) for c in collisions)
    emit(
        "✗",
        "Target collisions",
        f"{len(collisions)} shared target(s) across {total} pipeline(s)",
    )
    for collision in collisions:
        target = ".".join(p for p in (collision.schema, collision.display_table) if p)
        typer.echo(
            f"        {target} ({collision.layer}, {collision.env_label}) "
            f"← {', '.join(collision.pipelines)}"
        )
    return False


def _doctor_legacy_key_scan_paths(selected_configs: dict) -> List[str]:
    """Raw YAML files to scan for deprecated keys, de-duplicated in stable order:
    the selected config files plus ``saga_project.yml`` and ``profiles.yml``.
    """
    from dlt_saga.utility.cli.profiles import get_profiles_config

    paths: List[str] = []
    for config in flatten_configs(selected_configs) if selected_configs else []:
        config_path = config.config_dict.get("config_path")
        if config_path:
            paths.append(config_path)

    project_path = getattr(get_config_source(), "project_config_path", None)
    if project_path:
        paths.append(str(project_path))
    try:
        profiles_path = get_profiles_config().profiles_path
        if profiles_path:
            paths.append(str(profiles_path))
    except Exception:
        # Profiles are validated by their own check; a missing/broken one here
        # isn't this advisory's concern.
        pass

    return list(dict.fromkeys(paths))


def _doctor_scan_legacy_keys(paths: List[str]) -> dict:
    """Map ``path -> [(legacy, canonical), ...]`` for files with deprecated keys."""
    from dlt_saga.pipeline_config.compat import find_legacy_keys
    from dlt_saga.utility.yaml_io import load_yaml

    findings: dict = {}
    for path in paths:
        if not os.path.exists(path):
            continue
        try:
            raw = load_yaml(path)
        except Exception:
            continue  # a malformed file is surfaced by the config/profile checks
        legacy = find_legacy_keys(raw)
        if legacy:
            findings[path] = legacy
    return findings


def _doctor_check_legacy_keys(
    selected_configs: dict,
    verbose: bool,
    emit: Callable[..., None],
) -> bool:
    """Advise on deprecated config-key aliases (read-only, non-fatal).

    Legacy keys still resolve at load time (see ``pipeline_config/compat.py``),
    so this never fails the health check — it only nudges migration. Because
    normalization happens at load, the discovered configs no longer carry the
    legacy keys, so it scans the **raw** YAML (see
    :func:`_doctor_legacy_key_scan_paths`).
    """
    findings = _doctor_scan_legacy_keys(_doctor_legacy_key_scan_paths(selected_configs))

    if not findings:
        emit("✓", "Config keys", "no deprecated keys")
        return True

    total = sum(len(pairs) for pairs in findings.values())
    emit(
        "!",
        "Config keys",
        f"{total} deprecated key(s) in {len(findings)} file(s) — "
        "still work via aliases; rename when convenient",
    )
    for path, pairs in findings.items():
        renames = ", ".join(f"{legacy} → {canonical}" for legacy, canonical in pairs)
        typer.echo(f"        {path}: {renames}")
    # Advisory only — deprecated-but-working keys don't fail the health check.
    return True


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
