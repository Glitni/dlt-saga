"""saga ai-setup — generate AI context file for the current project."""

from __future__ import annotations

import hashlib
import logging
import re
from datetime import datetime, timezone
from importlib.metadata import version as pkg_version
from importlib.resources import files as pkg_files
from pathlib import Path

logger = logging.getLogger(__name__)

# The generated file name in the user's project root.
AI_CONTEXT_FILENAME = "saga_ai_context.md"

# Embedded in the generated file to detect content changes across versions.
_TEMPLATE_HASH_PREFIX = "<!-- template-hash: "
_TEMPLATE_HASH_PATTERN = re.compile(r"<!-- template-hash: ([a-f0-9]+) -->")


def _get_saga_version() -> str:
    """Return the installed dlt-saga version, or 'unknown'."""
    try:
        return pkg_version("dlt-saga")
    except Exception:
        return "unknown"


def _read_template() -> str:
    """Read the AI context template shipped with the package."""
    template_path = pkg_files("dlt_saga") / "templates" / "ai_context.md"
    return template_path.read_text(encoding="utf-8")


def _template_hash(template: str) -> str:
    """Return a short hash of the raw template (before rendering)."""
    return hashlib.sha256(template.encode("utf-8")).hexdigest()[:12]


def _render_template(template: str, version: str) -> str:
    """Fill in version, date, and template hash placeholders."""
    now = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    content_hash = _template_hash(template)
    rendered = template.replace("{version}", version).replace("{date}", now)
    # Append hash as an HTML comment at the end of the generated file.
    rendered += f"\n{_TEMPLATE_HASH_PREFIX}{content_hash} -->\n"
    return rendered


def _get_generated_hash(project_dir: Path) -> str | None:
    """Extract the template hash from an existing generated file, or None."""
    context_file = project_dir / AI_CONTEXT_FILENAME
    if not context_file.exists():
        return None
    try:
        content = context_file.read_text(encoding="utf-8")
        match = _TEMPLATE_HASH_PATTERN.search(content)
        if match:
            return match.group(1)
    except Exception:
        pass
    return None


def check_staleness(project_dir: Path) -> str | None:
    """Check if the AI context file is outdated.

    Compares the template hash embedded in the generated file against the
    current template shipped with the package. Only warns if the template
    content actually changed — version-only bumps are silent.

    Returns a warning message if stale, or None if up-to-date / not present.
    """
    generated_hash = _get_generated_hash(project_dir)
    if generated_hash is None:
        return None
    try:
        current_hash = _template_hash(_read_template())
    except Exception:
        return None
    if generated_hash != current_hash:
        current_version = _get_saga_version()
        return (
            f"saga_ai_context.md is outdated (template changed in v{current_version}). "
            f"Run `saga ai-setup` to update."
        )
    return None


def run_ai_setup(project_dir: Path | None = None) -> None:
    """Generate the AI context file and print setup instructions."""
    import typer

    if project_dir is None:
        project_dir = Path.cwd()

    version = _get_saga_version()
    template = _read_template()
    content = _render_template(template, version)

    output_path = project_dir / AI_CONTEXT_FILENAME
    output_path.write_text(content, encoding="utf-8")

    typer.echo(f"Generated {AI_CONTEXT_FILENAME} (dlt-saga v{version})")
    typer.echo("")
    typer.echo("Add one of the following to your AI assistant's context file:")
    typer.echo("")
    typer.echo("  Claude Code (.claude/CLAUDE.md):")
    typer.echo(
        "    When working with dlt-saga pipelines, pipeline configs, or the saga CLI,"
    )
    typer.echo("    read saga_ai_context.md for framework patterns and guidance.")
    typer.echo("")
    typer.echo("  Cursor (.cursorrules):")
    typer.echo(
        "    When working with dlt-saga pipelines, pipeline configs, or the saga CLI,"
    )
    typer.echo("    read saga_ai_context.md for framework patterns and guidance.")
    typer.echo("")
    typer.echo("  GitHub Copilot (.github/copilot-instructions.md):")
    typer.echo(
        "    When working with dlt-saga pipelines, pipeline configs, or the saga CLI,"
    )
    typer.echo("    read saga_ai_context.md for framework patterns and guidance.")
    typer.echo("")
    typer.echo(
        "The instruction is the same for all tools — only the file location differs."
    )
    typer.echo(
        "Re-run `saga ai-setup` after upgrading dlt-saga to keep the context current."
    )
