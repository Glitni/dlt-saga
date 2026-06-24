"""saga new adapter — scaffold a custom pipeline adapter from a known-good template.

The generated files bake in the conventions a first-time author would otherwise
have to rediscover (and usually gets wrong): the real ``extract_data()`` contract,
idempotent incremental loading via the warehouse high-water mark, secret-URI-aware
credential fields, and reuse of the inherited ``BaseConfig`` vocabulary. Starting
from correct is the cheapest guardrail we have.
"""

from __future__ import annotations

import keyword
from pathlib import Path
from typing import List, Optional, Tuple

import typer
import yaml

_VALID_KINDS = ("generic", "api")


# ---------------------------------------------------------------------------
# Naming helpers
# ---------------------------------------------------------------------------


def _normalize_name(raw: str) -> str:
    """Normalise an adapter/group name to a snake_case Python identifier."""
    name = raw.strip().replace("-", "_").lower()
    if not name:
        raise ValueError("Name must not be empty.")
    if not name.isidentifier() or keyword.iskeyword(name):
        raise ValueError(
            f"'{raw}' is not a valid name. Use letters, digits and underscores "
            "(must be a valid Python identifier), e.g. 'my_service'."
        )
    return name


def _pascal_case(name: str) -> str:
    """Convert snake_case to PascalCase (my_service -> MyService)."""
    return "".join(part.capitalize() for part in name.split("_") if part)


# ---------------------------------------------------------------------------
# Interactive prompts (each resolves one value, prompting only when unset)
# ---------------------------------------------------------------------------


def _prompt_name(
    name: Optional[str],
    no_input: bool,
    label: str = "Adapter name (snake_case, e.g. my_service)",
) -> str:
    if name is not None:
        return name
    if no_input:
        raise ValueError(
            "A name is required: pass it as an argument or drop --no-input to be "
            "prompted."
        )
    return typer.prompt(label)


def _prompt_kind(kind: Optional[str], no_input: bool) -> str:
    if kind is not None:
        return kind
    if no_input:
        return "generic"
    typer.echo(
        "Adapter kind:\n"
        "  generic - any source; you implement extract_data() (BasePipeline)\n"
        "  api     - REST API; inherits requests/auth/pagination (BaseApiPipeline)"
    )
    choice = ""
    while choice not in _VALID_KINDS:
        choice = typer.prompt("Kind [generic/api]", default="generic").strip().lower()
        if choice not in _VALID_KINDS:
            typer.echo(f"  Invalid choice. Enter one of: {', '.join(_VALID_KINDS)}")
    return choice


def _prompt_group(group: Optional[str], no_input: bool) -> str:
    if group is not None:
        return group
    if no_input:
        return "api"
    return typer.prompt(
        "Pipeline group / config subfolder (e.g. api, crm, ads)", default="api"
    )


def resolve_inputs(
    name: Optional[str],
    group: Optional[str],
    kind: Optional[str],
    no_input: bool,
) -> Tuple[str, str, str]:
    """Resolve (name, group, kind), prompting for any left unset.

    Kept separate from run_new_adapter so the scaffolding core stays
    non-interactive (and unit-testable without a TTY). Names are normalised here
    so the caller (e.g. a confirmation prompt) sees the final values.
    """
    name = _normalize_name(_prompt_name(name, no_input))
    kind = _prompt_kind(kind, no_input)
    group = _normalize_name(_prompt_group(group, no_input))
    return name, group, kind


# ---------------------------------------------------------------------------
# File templates
# ---------------------------------------------------------------------------


def _config_py_generic(name: str, class_name: str) -> str:
    return f'''\
"""Configuration for the {name} source."""

from dataclasses import dataclass, field
from typing import Optional

from dlt_saga.pipelines.base_config import BaseConfig
from dlt_saga.utility.secrets.secret_str import SecretStr, coerce_secret


@dataclass
class {class_name}Config(BaseConfig):
    """Configuration for the {name} source.

    Credentials accept a plain value, an environment variable (``${{ENV_VAR}}``)
    or a secret URI (``googlesecretmanager::project::secret``) interchangeably.
    Name fields by *what they hold* (``credential``, ``password``), never by
    whether the value is secret — avoid ``*_secret`` names, since secrecy is a
    property of the value, not the field.
    """

    # --- Connection ------------------------------------------------------
    # One required, source-identifying value (a URL, host, path or DSN). Keep the
    # default empty and validate it below so it MUST be set in the pipeline YAML —
    # never bury a real source location as a class default.
    source_uri: str = field(
        default="",
        metadata={{
            "description": "Where to read from: URL, host, path, or DSN",
            "required": True,
        }},
    )

    # --- Credentials -----------------------------------------------------
    # Type secrets as SecretStr and coerce them in __post_init__. The value may be
    # plain, ${{ENV_VAR}}, or a secret URI — resolve it lazily in client.py.
    credential: Optional[SecretStr] = field(
        default=None,
        metadata={{
            "description": "Access credential (plain value, ${{ENV_VAR}}, or secret URI)"
        }},
    )

    # --- Source-specific fields ------------------------------------------
    # Add the fields YOUR source needs and validate them in __post_init__.
    #
    # First check whether a built-in adapter already fits — prefer reusing or
    # inheriting from one over reimplementing it here:
    #   * REST APIs                     -> dlt_saga.api  (or scaffold --kind api)
    #   * SQL databases                 -> dlt_saga.database
    #   * GCS / S3 / Azure / local /    -> dlt_saga.filesystem
    #     SFTP files
    # To extend one, inherit its config instead of this BaseConfig — e.g.
    # `class {class_name}Config(FilesystemConfig)` — and inherit its pipeline,
    # overriding only what differs. Reach for this generic template only for a
    # source none of those cover. Example of adding your own field:
    #
    #   region: str = field(
    #       default="", metadata={{"description": "...", "required": True}}
    #   )

    # Incremental loading uses the INHERITED BaseConfig fields:
    #   incremental, incremental_column, initial_value
    # Reuse them — don't invent new names (e.g. overlap_days) for a concept the
    # standard vocabulary already covers. The watermark lookup that makes loads
    # idempotent lives in pipeline.py.

    def __post_init__(self) -> None:
        # Always call super() first so BaseConfig validation (tags, incremental)
        # runs, then coerce/validate this source's own fields.
        super().__post_init__()
        self.credential = coerce_secret(self.credential)

        if not self.source_uri:
            raise ValueError("source_uri is required for {name}")
'''


def _client_py_generic(name: str, class_name: str) -> str:
    return f'''\
"""Client for the {name} source — owns the connection and data fetching."""

import logging
from typing import Any, Dict, List, Optional

from .config import {class_name}Config

logger = logging.getLogger(__name__)


class {class_name}Client:
    """Talks to the {name} source.

    Keep all I/O and credential resolution here so the pipeline stays
    orchestration-only — this mirrors the built-in adapters (database, filesystem,
    sftp). Resolve secrets lazily at call time so secret URIs and ${{ENV_VAR}}
    both work::

        from dlt_saga.utility.secrets import resolve_secret
        secret = resolve_secret(self.config.credential)
    """

    def __init__(self, config: {class_name}Config):
        self.config = config

    def fetch(self, incremental_value: Optional[str] = None) -> List[Dict[str, Any]]:
        """Fetch records from the source, honouring ``incremental_value`` when set.

        Push ``incremental_value`` into your query/filter (e.g.
        ``updated_at > incremental_value``) so only new rows come back. Return a
        list of dict rows (or yield them).
        """
        raise NotImplementedError(
            "Implement {class_name}Client.fetch(): connect to {name} and return rows."
        )
'''


def _pipeline_py_generic(name: str, class_name: str, adapter: str) -> str:
    return f'''\
"""Pipeline for the {name} source."""

import logging
from dataclasses import fields as dataclass_fields
from typing import Any, Dict, List, Optional, Tuple

import dlt

from dlt_saga.pipelines.base_pipeline import BasePipeline

from .client import {class_name}Client
from .config import {class_name}Config

logger = logging.getLogger(__name__)


class {class_name}Pipeline(BasePipeline):
    """Extract data from {name}.

    The registry auto-discovers any BasePipeline subclass in this module, bound
    via ``adapter: {adapter}``. Orchestration lives here; connection + I/O live in
    client.py.
    """

    def __init__(self, config: Dict[str, Any], log_prefix: Optional[str] = None):
        # Parse only the keys this config understands (includes inherited
        # BaseConfig/Target fields); the rest are handled by the base pipeline.
        field_names = {{f.name for f in dataclass_fields({class_name}Config)}}
        self.source_config = {class_name}Config(
            **{{k: v for k, v in config.items() if k in field_names}}
        )
        self.client = {class_name}Client(self.source_config)
        super().__init__(config, log_prefix)

    def extract_data(self) -> List[Tuple[Any, str]]:
        """Return a list of ``(dlt.resource, description)`` tuples.

        This is the one required method. ``run()`` (on BasePipeline) iterates the
        returned resources, applies the write disposition and destination hints,
        and records load state in ``_saga_load_info``.
        """
        incremental_value = self._resolve_incremental_start()

        rows = self.client.fetch(incremental_value)
        resource = dlt.resource(rows, name=self.table_name, write_disposition="auto")

        return [(resource, self.source_config.source_uri)]

    def _resolve_incremental_start(self) -> Optional[str]:
        """Resolve the incremental cursor idempotently.

        Resume from the warehouse high-water mark (the max value already loaded),
        falling back to ``initial_value`` only on the first run. Looking the
        cursor up from the target table — instead of hardcoding "yesterday" —
        makes the pipeline self-healing: a missed or failed run is caught up on
        the next run with no gaps and no duplicates.
        """
        if not self.source_config.incremental:
            return None

        column = self.source_config.incremental_column
        table_id = (
            f"{{self.destination_database}}."
            f"{{self.pipeline.dataset_name}}.{{self.table_name}}"
        )
        start = (
            self.destination.get_max_column_value(table_id, column)
            or self.source_config.initial_value
        )
        if start:
            self.logger.info(f"Incremental load from {{column}} > {{start}}")
        return start
'''


def _config_py_api(name: str, class_name: str) -> str:
    return f'''\
"""Configuration for the {name} API source."""

from dataclasses import dataclass

from dlt_saga.pipelines.api.config import ApiConfig


@dataclass
class {class_name}Config(ApiConfig):
    """Configuration for the {name} API source.

    Inherits every REST field from ApiConfig: ``base_url``, ``endpoint``,
    ``auth_type``, ``auth_token`` (secret-URI aware), ``pagination``,
    ``response_path``, plus the incremental fields from BaseConfig.

    Add source-specific fields here only when the generic API config can't
    express them. Override ``__post_init__`` (calling ``super().__post_init__()``
    first) to validate them. If you add nothing, you may not need a custom config
    or pipeline at all — the built-in ``dlt_saga.api`` adapter handles many REST
    sources from YAML alone.
    """
'''


def _pipeline_py_api(name: str, class_name: str, adapter: str) -> str:
    return f'''\
"""Pipeline for the {name} API source."""

import logging
from dataclasses import fields as dataclass_fields
from typing import Any, Dict

from dlt_saga.pipelines.api.base import BaseApiPipeline

from .config import {class_name}Config

logger = logging.getLogger(__name__)


class {class_name}Pipeline(BaseApiPipeline):
    """Extract data from the {name} API.

    Inherits HTTP requests, retries/backoff, auth, JSON extraction and
    pagination from BaseApiPipeline. Bound via ``adapter: {adapter}``.
    """

    def _create_api_config(self, config_dict: Dict[str, Any]) -> {class_name}Config:
        """Build this pipeline's config object (overrides ApiConfig factory)."""
        field_names = {{f.name for f in dataclass_fields({class_name}Config)}}
        return {class_name}Config(
            **{{k: v for k, v in config_dict.items() if k in field_names}}
        )

    # Override hooks, lowest-impact first — for most REST APIs you need NONE of
    # these: configuring auth + pagination + response_path in the pipeline YAML
    # is enough, and the base class handles the rest.
    #
    #   fetch_data(self) -> list
    #       Return the list of record dicts for a single request. Override this
    #       when the records need custom assembly. The base class still wraps
    #       your result into a dlt.resource and applies pagination.
    #
    #   _make_request(self) / _extract_data_from_response(self, data)
    #       Finer control over the HTTP call or response unwrapping.
    #
    # Do NOT override extract_data() here: BaseApiPipeline already implements it
    # (pagination + dlt.resource + description). Overriding it means
    # reimplementing pagination yourself.
    #
    # def fetch_data(self) -> list:
    #     from dlt_saga.utility.secrets import resolve_secret
    #     token = resolve_secret(self.api_config.auth_token)
    #     ...
    #     return records
'''


def _starter_config_yml_generic(name: str, adapter: str) -> str:
    return f"""\
adapter: {adapter}
tags: [daily]

# What runs: "append" = ingest only. Use "append+historize" to also build SCD2
# history. See the write_disposition table in saga_ai_context.md.
write_disposition: append

# Where to read from — set the real value HERE, not as a default in config.py.
source_uri: "https://example.com/or/host/or/path"

# Add whatever source-specific fields you declared in config.py (e.g. endpoint,
# hostname, source_table). Credentials: never commit a plain secret — point at a
# secret manager or env var; any field accepts a secret URI:
# credential: "googlesecretmanager::my-project::{name}-credential"
# credential: "${{MY_CREDENTIAL}}"

# Incremental loading (optional). The pipeline resumes from the warehouse
# high-water mark and seeds from initial_value on the first run — so it catches
# up automatically after a missed or failed run.
incremental: false
# incremental_column: updated_at
# initial_value: "2024-01-01"
"""


def _starter_config_yml_api(name: str, adapter: str) -> str:
    return f"""\
adapter: {adapter}
tags: [daily]
write_disposition: append

base_url: "https://api.example.com"
endpoint: "/v1/records"
response_path: "data"

# Auth: any field accepts a plain value, ${{ENV_VAR}} or a secret URI.
auth_type: bearer
auth_token: "${{MY_API_TOKEN}}"
# auth_token: "googlesecretmanager::my-project::{name}-token"

# Pagination (optional) — omit for single-request endpoints.
# pagination:
#   type: offset
#   limit: 100

# Incremental loading (optional): resumes from the high-water mark, seeds from
# initial_value on the first run.
incremental: false
# incremental_column: updated_at
# initial_value: "2024-01-01"
"""


# ---------------------------------------------------------------------------
# File writing
# ---------------------------------------------------------------------------


def _write_if_absent(
    path: Path, content: str, created: List[str], skipped: List[str]
) -> None:
    if path.exists():
        skipped.append(str(path))
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")
    created.append(str(path))


def _ensure_package_init(
    directory: Path, created: List[str], skipped: List[str]
) -> None:
    """Ensure *directory* is an importable package (has __init__.py)."""
    _write_if_absent(directory / "__init__.py", "", created, skipped)


def _norm_path(path: object) -> str:
    """Normalise a packages.yml path for comparison (``./pipelines`` == ``pipelines``)."""
    text = str(path or "").strip()
    if text.startswith("./"):
        text = text[2:]
    return text.rstrip("/")


def _load_packages(target: Path) -> List[dict]:
    """Return the list of package entries from packages.yml (empty if none)."""
    packages_file = target / "packages.yml"
    if not packages_file.exists():
        return []
    loaded = yaml.safe_load(packages_file.read_text(encoding="utf-8"))
    if not isinstance(loaded, dict):
        return []
    return [e for e in (loaded.get("packages") or []) if isinstance(e, dict)]


def effective_namespace(target: Path, namespace: str, pkg_dir: str) -> str:
    """Return the namespace the adapter will actually use.

    If *pkg_dir* is already registered in packages.yml under some namespace, that
    existing namespace wins — we never register the same directory twice under a
    different name. Otherwise the requested *namespace* is used.
    """
    for entry in _load_packages(target):
        if _norm_path(entry.get("path")) == _norm_path(pkg_dir):
            return entry.get("namespace") or namespace
    return namespace


def _register_package(
    target: Path,
    namespace: str,
    pkg_dir: str,
    updated: List[str],
    skipped: List[str],
) -> str:
    """Ensure ``packages.yml`` maps a namespace to *pkg_dir*; return that namespace.

    Reuses an existing registration for the same directory (no duplicate entry).
    Rewrites packages.yml when a new entry is needed — this normalises the file
    (comments in an existing packages.yml are not preserved), reported to the user.
    """
    packages_file = target / "packages.yml"
    entries = _load_packages(target)

    # Already registered under some namespace → reuse it, don't add a duplicate.
    for entry in entries:
        if _norm_path(entry.get("path")) == _norm_path(pkg_dir):
            existing_ns = entry.get("namespace") or namespace
            skipped.append(
                f"{packages_file} (path './{pkg_dir}' already registered "
                f"as namespace '{existing_ns}')"
            )
            return existing_ns

    # The requested namespace is taken by a *different* path → genuine conflict.
    for entry in entries:
        if entry.get("namespace") == namespace:
            raise ValueError(
                f"Namespace '{namespace}' is already registered in packages.yml "
                f"for path '{entry.get('path')}'. Pass --namespace to pick another, "
                "or --path to reuse that package directory."
            )

    entries.append({"namespace": namespace, "path": f"./{pkg_dir}"})
    header = (
        "# Registers local pipeline implementations. Each entry maps a namespace\n"
        "# to a Python package directory containing pipeline.py modules.\n"
    )
    packages_file.parent.mkdir(parents=True, exist_ok=True)
    packages_file.write_text(
        header + yaml.safe_dump({"packages": entries}, sort_keys=False),
        encoding="utf-8",
    )
    updated.append(str(packages_file))
    return namespace


# ---------------------------------------------------------------------------
# Collision detection
# ---------------------------------------------------------------------------


def existing_adapter_files(
    target: Path, pkg_dir: str, group: str, name: str
) -> List[Path]:
    """Return the meaningful adapter files that already exist at the target.

    Keyed on config.py / pipeline.py (the files that define the adapter) so a
    stray empty __init__.py doesn't count as a collision.
    """
    adapter_dir = target / pkg_dir / group / name
    return [
        p
        for p in (adapter_dir / "config.py", adapter_dir / "pipeline.py")
        if p.exists()
    ]


# ---------------------------------------------------------------------------
# Scaffold
# ---------------------------------------------------------------------------


def _scaffold(
    target: Path,
    name: str,
    group: str,
    kind: str,
    pkg_dir: str,
    namespace: str,
) -> Tuple[List[str], List[str], List[str], str]:
    """Write adapter files. Returns (created, skipped, updated, adapter)."""
    created: List[str] = []
    skipped: List[str] = []
    updated: List[str] = []

    # Register first: the effective namespace (which may reuse an existing
    # registration for this directory) determines the adapter string baked into
    # the generated files.
    effective_ns = _register_package(target, namespace, pkg_dir, updated, skipped)
    class_name = _pascal_case(name)
    adapter = f"{effective_ns}.{group}.{name}"

    # Package skeleton: <pkg_dir>/<group>/<name>/ with __init__.py at each level.
    base = target / pkg_dir
    group_dir = base / group
    adapter_dir = group_dir / name
    _ensure_package_init(base, created, skipped)
    _ensure_package_init(group_dir, created, skipped)
    _ensure_package_init(adapter_dir, created, skipped)

    if kind == "api":
        # API sources need no client.py — BaseApiPipeline owns the HTTP layer.
        files = {
            "config.py": _config_py_api(name, class_name),
            "pipeline.py": _pipeline_py_api(name, class_name, adapter),
        }
        starter_yml = _starter_config_yml_api(name, adapter)
    else:
        # Generic sources split connection/I/O into client.py (the house pattern
        # used by the built-in database/filesystem/sftp adapters).
        files = {
            "config.py": _config_py_generic(name, class_name),
            "client.py": _client_py_generic(name, class_name),
            "pipeline.py": _pipeline_py_generic(name, class_name, adapter),
        }
        starter_yml = _starter_config_yml_generic(name, adapter)

    for filename, content in files.items():
        _write_if_absent(adapter_dir / filename, content, created, skipped)

    # Starter pipeline config so the adapter is immediately discoverable.
    _write_if_absent(
        target / "configs" / group / f"{name}.yml", starter_yml, created, skipped
    )

    return created, skipped, updated, adapter


def _print_next_steps(name: str, group: str, adapter: str, kind: str) -> None:
    class_name = _pascal_case(name)
    typer.echo("")
    typer.echo("Done! Next steps:")
    if kind == "api":
        typer.echo(
            f"  1. Configure auth + pagination + response_path in "
            f"configs/{group}/{name}.yml (or override fetch_data() in pipeline.py)."
        )
    else:
        typer.echo(
            f"  1. Implement {class_name}Client.fetch() in {group}/{name}/client.py "
            "(replace the NotImplementedError stub)."
        )
    typer.echo(f"  2. Fill in real source values in configs/{group}/{name}.yml.")
    typer.echo(
        "  3. Run `saga generate-schemas` to create + link the JSON schema "
        "for editor validation."
    )
    typer.echo(f'  4. Run `saga list --select "{name}"` to confirm discovery.')


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------


def run_new_adapter(
    name: str,
    group: str = "api",
    kind: str = "generic",
    pkg_dir: str = "pipelines",
    namespace: str = "local",
    target: Optional[Path] = None,
) -> str:
    """Scaffold a new pipeline adapter. Returns the resolved adapter string."""
    if kind not in _VALID_KINDS:
        raise ValueError(
            f"Invalid kind '{kind}'. Choose one of: {', '.join(_VALID_KINDS)}."
        )

    name = _normalize_name(name)
    group = _normalize_name(group)
    target = target or Path.cwd()

    # Never silently skip an existing adapter — that reads as success when
    # nothing was written. Refuse and point the user at their options.
    existing = existing_adapter_files(target, pkg_dir, group, name)
    if existing:
        ns = effective_namespace(target, namespace, pkg_dir)
        raise ValueError(
            f"Adapter '{ns}.{group}.{name}' already exists "
            f"({existing[0]}). Choose a different name or --group, or edit the "
            "existing files directly."
        )

    created, skipped, updated, adapter = _scaffold(
        target, name, group, kind, pkg_dir, namespace
    )

    typer.echo("")
    if created:
        typer.echo("Created:")
        for path in created:
            typer.echo(f"  {path}")
    if updated:
        typer.echo("Updated:")
        for path in updated:
            typer.echo(f"  {path}")
    if skipped:
        typer.echo("Skipped (already exist):")
        for path in skipped:
            typer.echo(f"  {path}")

    _print_next_steps(name, group, adapter, kind)
    return adapter
