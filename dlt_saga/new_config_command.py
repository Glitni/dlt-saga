"""saga new config — scaffold a pipeline config YAML for an existing adapter.

Standing up a new adapter is rare; adding a config for an existing one is the
everyday task — and the spot where authors most often produce empty YAML because
they don't know which fields the adapter accepts. This introspects the adapter's
config dataclass (the same metadata that drives ``saga generate-schemas``) and
emits a config pre-populated with the available fields: required ones active,
optional ones commented with their descriptions, plus a schema modeline.
"""

from __future__ import annotations

import typing
from dataclasses import MISSING, fields
from pathlib import Path
from typing import List, Optional, Tuple

import typer

from dlt_saga.new_adapter_command import _normalize_name, _prompt_group, _prompt_name

# Meta fields handled explicitly (adapter/tags) or rarely set in a per-pipeline
# config — kept out of the introspected field listing to reduce noise.
_SKIP_FIELDS = {"adapter", "tags", "enabled"}


# ---------------------------------------------------------------------------
# Interactive prompts
# ---------------------------------------------------------------------------


def available_adapters() -> List[Tuple[str, str]]:
    """Return (adapter, source) pairs for every discovered adapter, sorted.

    Includes built-in adapters and any registered via packages.yml, so the prompt
    can show the user what they can actually pick.
    """
    from dlt_saga.pipelines.registry import discover_implementations

    try:
        impls = discover_implementations()
    except Exception:
        return []
    return [(impl["adapter"], impl.get("source", "external")) for impl in impls]


def _prompt_adapter(adapter: Optional[str], no_input: bool) -> str:
    if adapter is not None:
        return adapter
    if no_input:
        raise ValueError("--adapter is required with --no-input (e.g. dlt_saga.api).")

    choices = available_adapters()
    if not choices:
        return typer.prompt(
            "Adapter (e.g. dlt_saga.api, dlt_saga.database, local.api.my_source)"
        )

    typer.echo("Available adapters:")
    for idx, (adapter_str, source) in enumerate(choices, 1):
        typer.echo(f"  {idx:>2}. {adapter_str}  ({source})")
    typer.echo("  Pick a number, or type a custom adapter (e.g. local.api.my_source).")
    raw = typer.prompt("Adapter").strip()
    if raw.isdigit() and 1 <= int(raw) <= len(choices):
        return choices[int(raw) - 1][0]
    return raw


def resolve_config_inputs(
    name: Optional[str],
    group: Optional[str],
    adapter: Optional[str],
    no_input: bool,
) -> Tuple[str, str, str]:
    """Resolve (name, group, adapter), prompting for any left unset.

    Kept separate from run_new_config so the core stays non-interactive (and
    unit-testable without a TTY).
    """
    name = _normalize_name(
        _prompt_name(name, no_input, "Config name (snake_case, e.g. orders)")
    )
    adapter = _prompt_adapter(adapter, no_input)
    group = _normalize_name(_prompt_group(group, no_input))
    return name, group, adapter


# ---------------------------------------------------------------------------
# Field introspection → YAML
# ---------------------------------------------------------------------------


def _is_required(field) -> bool:
    """A field is required if flagged in metadata, or has no default at all."""
    if field.metadata.get("required"):
        return True
    return field.default is MISSING and field.default_factory is MISSING


def _to_yaml_scalar(value) -> str:
    """Render a Python default as a YAML scalar (strings quoted so values like
    ``none``/``yes`` aren't parsed as null/bool)."""
    from enum import Enum

    if isinstance(value, Enum):
        return f'"{value.value}"'
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, (int, float)):
        return str(value)
    return f'"{value}"'


def _placeholder(field) -> str:
    """A YAML placeholder value for a field.

    Prefers the field's actual default (most useful for optional fields), then
    an enum's first value, then a type-appropriate empty value.
    """
    if field.default is not MISSING and field.default is not None:
        return _to_yaml_scalar(field.default)

    enum = field.metadata.get("enum")
    if enum:
        return f'"{enum[0]}"'

    tp = field.type
    origin = typing.get_origin(tp)
    args = typing.get_args(tp)
    if origin is typing.Union:  # Optional[X] / Union[...]
        non_none = [a for a in args if a is not type(None)]
        if non_none:
            tp = non_none[0]
            origin = typing.get_origin(tp)

    if origin is list:
        return "[]"
    if origin is dict:
        return "{}"
    if tp is bool:
        return "false"
    if tp in (int, float):
        return "0"
    return '""'


def _short_desc(desc: Optional[str], limit: int = 90) -> str:
    """First sentence of a field description, capped — full text stays available
    via the schema modeline on hover. Keeps the inline annotation to one line."""
    if not desc:
        return ""
    first = desc.split(". ", 1)[0].strip().rstrip(".")
    if len(first) > limit:
        first = first[: limit - 1].rstrip() + "..."
    return first


def _field_line(field, commented: bool) -> str:
    """Render one config line: ``key: value  # short description``.

    The description sits inline (not on its own ``#`` line) so a disabled setting
    is never confused with prose — the ``key: value`` token is right beside it.
    """
    prefix = "# " if commented else ""
    line = f"{prefix}{field.name}: {_placeholder(field)}"
    desc = _short_desc(field.metadata.get("description"))
    if desc:
        line += f"  # {desc}"
    return line


def _classify_fields(config_class: type) -> Tuple[list, list]:
    """Split a config dataclass's fields into (required, optional), skipping
    private and meta fields."""
    required: list = []
    optional: list = []
    for field in fields(config_class):
        if field.name.startswith("_") or field.name in _SKIP_FIELDS:
            continue
        (required if _is_required(field) else optional).append(field)
    return required, optional


def render_config_yaml(adapter: str, config_class: type) -> str:
    """Render a starter config YAML for *adapter*, driven by its config class."""
    required, optional = _classify_fields(config_class)
    lines: List[str] = [
        f"adapter: {adapter}",
        "tags: [daily]",
        "",
        "# What runs: append = ingest only. append+historize also builds SCD2 history.",
        "write_disposition: append",
        "",
    ]

    if required:
        lines.append("# --- Required source settings ---")
        for field in required:
            lines.append(_field_line(field, commented=False))
        lines.append("")

    if optional:
        lines.append("# --- Optional source settings (uncomment what you need) ---")
        lines.append("# (full descriptions show on hover via the schema modeline)")
        for field in optional:
            lines.append(_field_line(field, commented=True))
        lines.append("")

    # Curated load-behaviour options (these live on TargetConfig, merged at
    # runtime, so they aren't on the source config class above).
    lines += [
        "# --- Common load options (uncomment as needed) ---",
        "# primary_key: [id]",
        "# merge_strategy: scd2          # use with write_disposition: merge",
        "# partition_column: <date_column>",
        "# cluster_columns: [<column>]",
    ]
    return "\n".join(lines) + "\n"


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------


def config_file_path(target: Path, group: str, name: str) -> Path:
    return target / "configs" / group / f"{name}.yml"


def run_new_config(
    name: str,
    adapter: str,
    group: str = "api",
    schema_dir: str = "schemas",
    target: Optional[Path] = None,
) -> Path:
    """Scaffold a pipeline config for *adapter*. Returns the written file path."""
    from dlt_saga.utility.generate_schemas import config_class_for_adapter

    name = _normalize_name(name)
    group = _normalize_name(group)
    target = target or Path.cwd()

    config_path = config_file_path(target, group, name)
    if config_path.exists():
        raise ValueError(
            f"Config '{config_path}' already exists. Choose a different name or "
            "--group, or edit the existing file."
        )

    config_class = config_class_for_adapter(adapter, group, str(config_path))
    if config_class is None:
        raise ValueError(
            f"Could not resolve a config class for adapter '{adapter}'. Check the "
            "adapter string (e.g. dlt_saga.api) and that its package is registered "
            "in packages.yml."
        )

    config_path.parent.mkdir(parents=True, exist_ok=True)
    config_path.write_text(render_config_yaml(adapter, config_class), encoding="utf-8")
    typer.echo(f"Created: {config_path}")

    _link_schema(config_path, adapter, group, schema_dir, target)
    _print_next_steps(name, group, config_path)
    return config_path


def _link_schema(
    config_path: Path, adapter: str, group: str, schema_dir: str, target: Path
) -> None:
    """Add the yaml-language-server modeline if the schema file already exists."""
    from dlt_saga.utility.generate_schemas import schema_filename_for_adapter
    from dlt_saga.utility.link_schemas import apply_modeline

    schema_filename = schema_filename_for_adapter(adapter, group, str(config_path))
    schema_full = target / schema_dir / schema_filename if schema_filename else None
    if schema_full and schema_full.exists():
        apply_modeline(str(config_path), str(target / schema_dir), schema_filename)
        typer.echo(f"Linked schema: {schema_filename}")
    else:
        typer.echo(
            "Note: run `saga generate-schemas` to create + link the JSON schema "
            "for editor validation."
        )


def _print_next_steps(name: str, group: str, config_path: Path) -> None:
    typer.echo("")
    typer.echo("Done! Next steps:")
    typer.echo(f"  1. Fill in the required fields in {config_path}.")
    typer.echo(f'  2. Run `saga list --select "{name}"` to confirm discovery.')
