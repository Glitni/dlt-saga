"""Default name and location generation for pipeline configs.

These functions implement the framework's default rules for deriving
schema names, table names, and external-table locations from a pipeline
config identifier. They are the canonical defaults used when no custom
``naming_module`` is configured in ``saga_project.yml`` — and the same
implementations users typically copy or wrap when they do define one.

Identifier contract — segments
------------------------------

The naming functions operate on an ordered list of strings — *segments* —
that uniquely identify a pipeline config. The contract is:

- ``segments[0]`` is the **pipeline group** (top-level partition of the
  config space, e.g. ``"google_sheets"``).
- ``segments[-1]`` is the **base name** (leaf identifier of the config).
- Anything in between adds organisational depth.

For the file-based config source, segments are the relative path parts with
the file extension stripped — e.g. ``["google_sheets", "asm", "salgsmal"]``
for ``configs/google_sheets/asm/salgsmal.yml``. Other config sources
(database-backed, SharePoint-backed, …) produce their own segments from
whatever identifier shape they use; the naming functions don't care.

Layer keyword
-------------

Every hook accepts ``layer`` — ``"ingest"`` (default) or ``"historize"``.
The framework calls the same hook for both layers so a custom naming
module can produce distinct shapes per layer (e.g. ``dlt_<group>_raw``
for ingest, ``dlt_<group>_historized`` for historize). The default
implementations ignore ``layer`` and return the same value either way.

Custom naming module
--------------------

Users override any subset of these defaults by pointing
``naming_module: <importable.module>`` in ``saga_project.yml`` at a Python
module that defines one or more of:

- ``generate_schema_name(segments, environment, default_schema, *, layer="ingest", custom_schema_name=None) -> str``
- ``generate_table_name(segments, environment, *, layer="ingest", custom_table_name=None) -> str``
- ``generate_target_location(segments, environment, default_storage_root, *, layer="ingest", schema=None, table=None) -> Optional[str]``

Missing functions fall back to the defaults below. Modules whose hook
signature predates ``layer`` are still supported — calls retry without
the keyword. Users typically import these defaults inside their custom
module and wrap them with their own logic — see the docstrings on each
function for examples.
"""

import importlib
import inspect
import logging
from typing import Any, Callable, Dict, List, Optional

from dlt_saga.utility.naming import normalize_identifier

logger = logging.getLogger(__name__)

# Cached custom naming module (None = not loaded, False = no custom module)
_naming_module: Any = None


def load_naming_module(project_config: Dict[str, Any]) -> Any:
    """Load the custom naming module declared in ``saga_project.yml``.

    Result is cached on first call. Missing optional functions are logged at
    debug level only — all three hooks are genuinely optional and fall back
    to the framework defaults in this module.

    Args:
        project_config: Parsed ``saga_project.yml`` contents.

    Returns:
        The imported module, or ``False`` if no custom module is configured.
    """
    global _naming_module
    if _naming_module is not None:
        return _naming_module

    try:
        module_name = project_config.get("naming_module")
        if not module_name:
            _naming_module = False
            return _naming_module

        _naming_module = importlib.import_module(module_name)
        logger.debug(f"Loaded custom naming module: {module_name}")

        for fn_name in (
            "generate_schema_name",
            "generate_table_name",
            "generate_target_location",
        ):
            if not hasattr(_naming_module, fn_name):
                logger.debug(
                    f"Custom naming module '{module_name}' does not define "
                    f"{fn_name}(); using framework default for that function"
                )

        return _naming_module

    except Exception as e:
        logger.warning(f"Failed to load custom naming module: {e}, using defaults")
        _naming_module = False
        return _naming_module


def call_hook(func: Callable, /, *args: Any, **kwargs: Any) -> Any:
    """Invoke a naming-module hook, tolerating older signatures.

    Hook contracts grow over time — ``layer`` and the URI ``dataset``/``table``
    kwargs were added after the initial release. To keep custom modules
    written against earlier signatures working, drop kwargs the hook
    doesn't accept (anything not in its signature and not absorbed by a
    ``**kwargs`` catch-all) before calling. Positional args pass through
    unchanged.
    """
    try:
        sig = inspect.signature(func)
    except (TypeError, ValueError):
        return func(*args, **kwargs)

    params = sig.parameters
    accepts_kwargs = any(
        p.kind is inspect.Parameter.VAR_KEYWORD for p in params.values()
    )
    if accepts_kwargs:
        return func(*args, **kwargs)

    filtered = {k: v for k, v in kwargs.items() if k in params}
    return func(*args, **filtered)


def hook_accepts_kwarg(func: Callable, name: str) -> bool:
    """Whether a naming hook accepts keyword ``name`` (by name or via ``**kwargs``).

    Used to tell a hook written against the current signature from a legacy one,
    so a newly added keyword (e.g. ``custom_schema_name``) can be routed to hooks
    that understand it and handled by a compatibility path for those that don't —
    rather than being silently dropped by :func:`call_hook`. Assumes acceptance
    when the signature can't be introspected (built-ins, C callables).
    """
    try:
        sig = inspect.signature(func)
    except (TypeError, ValueError):
        return True
    params = sig.parameters
    if any(p.kind is inspect.Parameter.VAR_KEYWORD for p in params.values()):
        return True
    return name in params


def default_generate_schema_name(
    segments: List[str],
    environment: str,
    default_schema: str,
    *,
    layer: str = "ingest",
    custom_schema_name: Optional[str] = None,
) -> str:
    """Default schema name generation from config identifier segments.

    Without a ``custom_schema_name`` override:

    - Prod: ``dlt_{segments[0]}`` (e.g. ``dlt_google_sheets``), with the group
      segment run through dlt's snake_case normalization so the schema matches
      dlt's actual dataset (``configs/MyAPI/…`` → ``dlt_my_api``).
    - Dev: ``default_schema`` (from profile or ``SAGA_SCHEMA_NAME``).

    With a ``custom_schema_name`` override (a config's ``schema_name:``), the
    override is composed **per environment** — the dbt ``generate_schema_name``
    pattern:

    - Prod: the override is used directly (``schema_name: analytics`` →
      ``analytics``; it fully replaces the ``dlt_<group>`` default).
    - Dev: the override is composed with the developer's sandbox as
      ``{default_schema}_{custom_schema_name}`` (e.g. developer sandbox
      ``dlt_user`` → ``dlt_user_analytics``).
      A literal override in dev would place every developer's data in the same
      schema — since configs are shared in git, that breaks sandbox isolation —
      so placement is namespaced under each developer's sandbox.

    The default ignores ``layer``: ingest and historize share one dataset.
    Override in a custom naming module to produce distinct shapes per
    layer (e.g. ``dlt_<group>_raw`` vs ``dlt_<group>_historized``) or to change
    how ``custom_schema_name`` composes in dev.

    Args:
        segments: Ordered identifier segments. ``segments[0]`` is the group.
            Empty list falls back to ``"default"``.
        environment: Current environment (``"prod"`` or ``"dev"``).
        default_schema: Dev schema name from profile/env var.
        layer: ``"ingest"`` (default) or ``"historize"``. Ignored here.
        custom_schema_name: Explicit ``schema_name:`` override, or ``None``.
    """
    del layer  # default behavior is layer-agnostic
    if environment == "prod":
        if custom_schema_name:
            return custom_schema_name
        # Normalize the group through dlt's snake_case convention so the schema
        # matches dlt's actual (case-sensitive) dataset: dlt_MyAPI → dlt_my_api.
        first_segment = normalize_identifier(segments[0]) if segments else "default"
        return f"dlt_{first_segment}"
    if custom_schema_name:
        return f"{default_schema}_{custom_schema_name}"
    return default_schema


def default_generate_table_name(
    segments: List[str],
    environment: str,
    *,
    layer: str = "ingest",
    custom_table_name: Optional[str] = None,
) -> str:
    """Default table name generation from config identifier segments.

    Without a ``custom_table_name`` override:

    - Prod: ``segments[1:]`` joined with ``__`` (or ``segments[0]`` when there's
      only one segment), e.g. ``"reports__monthly"``.
    - Dev: ``"{segments[0]}__{prod_name}"``, e.g. ``"google_sheets__reports__monthly"``.

    With a ``custom_table_name`` override (a config's ``table_name:``), the
    override replaces the derived base name and is composed per environment,
    mirroring ``schema_name``:

    - Prod: the override is used directly (``table_name: orders`` → ``orders``).
    - Dev: the override is group-prefixed (``google_sheets__orders``) so it
      stays disambiguated within the shared dev schema.

    Every segment (group and inner) is run through dlt's snake_case
    normalization so the names are safe SQL identifiers and match what dlt
    creates (e.g. ``My-API`` / ``MyAPI`` → ``my_api``).

    The default ignores ``layer``: the historized table's name is derived
    by ``HistorizeConfig.table_suffix`` (or ``placement: schema_suffix``)
    rather than by re-running this hook. A custom naming module that
    returns a distinct name for ``layer="historize"`` is honoured by the
    historize factory and overrides the suffix-based default.

    Args:
        segments: Ordered identifier segments. Empty list returns
            ``"default_data"``.
        environment: Current environment (``"prod"`` or ``"dev"``).
        layer: ``"ingest"`` (default) or ``"historize"``. Ignored here.
        custom_table_name: Explicit ``table_name:`` override, or ``None``.
    """
    del layer  # default behavior is layer-agnostic
    if not segments:
        return custom_table_name or "default_data"

    first_segment = normalize_identifier(segments[0])

    if custom_table_name:
        base_name = custom_table_name
    elif len(segments) == 1:
        base_name = normalize_identifier(segments[0])
    elif len(segments) == 2:
        base_name = normalize_identifier(segments[1])
    else:
        base_name = "__".join(normalize_identifier(s) for s in segments[1:])

    if environment == "prod":
        return base_name
    return f"{first_segment}__{base_name}"


def resolve_table_name_with_leaf(
    segments: List[str],
    leaf: str,
    environment: str,
    project_config: Optional[Dict[str, Any]] = None,
    *,
    layer: str = "ingest",
) -> str:
    """Run the canonical table-name generator with the leaf segment replaced.

    Honours a custom ``naming_module.generate_table_name`` when one is configured.
    """
    new_segments = list(segments[:-1]) + [leaf] if segments else [leaf]
    module = load_naming_module(project_config) if project_config else None
    if module and hasattr(module, "generate_table_name"):
        return call_hook(
            module.generate_table_name, new_segments, environment, layer=layer
        )
    return default_generate_table_name(new_segments, environment, layer=layer)


def default_generate_target_location(
    segments: List[str],
    environment: str,
    default_storage_root: Optional[str],
    pipeline_group: Optional[str] = None,
    table_name: Optional[str] = None,
    *,
    layer: str = "ingest",
    schema: Optional[str] = None,
    table: Optional[str] = None,
) -> Optional[str]:
    """Default external-table LOCATION generation.

    Returns ``<default_storage_root>/<schema_or_group>/<table>/`` or
    ``None`` when no ``default_storage_root`` is configured (caller should
    then fall back to a managed table).

    Resolution for the path components:

    - **Folder segment**: ``schema`` when provided (covers the BigQuery
      / BigLake call sites that don't carry naming segments), otherwise
      ``pipeline_group`` when provided, otherwise ``segments[0]``.
    - **Table segment**: ``table`` when provided, otherwise ``table_name``,
      otherwise ``default_generate_table_name(segments, environment)``.

    The default ignores ``layer``: it returns the same shape for ingest
    and historize. Custom naming modules can use ``layer`` to inject a
    layer-specific subfolder (e.g. ``…/raw/<schema>/<table>/`` vs
    ``…/historized/<schema>/<table>/``).

    Args:
        segments: Ordered identifier segments. ``segments[0]`` is the group.
            May be empty when the caller doesn't carry segments (e.g. the
            BigQuery Iceberg URI builder, which only knows schema / table).
        environment: Current environment (``"prod"`` or ``"dev"``).
        default_storage_root: Profile's ``storage_root`` / ``storage_path``
            (e.g. ``"gs://bucket/lake/"``) or ``None``.
        pipeline_group: Optional pre-resolved pipeline group; defaults to
            ``segments[0]``.
        table_name: Optional pre-resolved table name; defaults to
            ``default_generate_table_name(segments, environment)``.
        layer: ``"ingest"`` (default) or ``"historize"``. Ignored here.
        schema: Resolved warehouse schema name. Used as the folder
            segment when set — takes precedence over ``pipeline_group``
            / ``segments[0]``.
        table: Resolved warehouse table name. Used as the table segment
            when set — takes precedence over ``table_name``.

    Returns:
        Full external-table URI, or ``None`` to indicate "use a managed table".
    """
    del layer  # default behavior is layer-agnostic
    if not default_storage_root:
        return None
    folder = schema or pipeline_group or (segments[0] if segments else "default")
    leaf = table or table_name
    if leaf is None:
        leaf = default_generate_table_name(segments, environment)
    return f"{default_storage_root.rstrip('/')}/{folder}/{leaf}/"
