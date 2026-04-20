"""Pipeline class registry with namespace-based resolution.

Resolves ``adapter`` values to concrete pipeline classes.

Resolution order:
1. If ``adapter`` is set:
   a. Split first segment as namespace (e.g., ``dlt_saga`` from ``dlt_saga.api.genesyscloud``).
   b. Look up namespace in registry → get base module path.
   c. Append remaining segments + ``.pipeline`` → import the module.
   d. Auto-discover the class that extends ``BasePipeline``.
   e. If no namespace matches, treat the whole value as a bare name
      and resolve under the default namespace (backwards compat).
2. If ``adapter`` is not set:
   a. Folder-structure fallback using ``config_path`` (unchanged from before).
"""

import importlib
import logging
import threading
import types
from typing import Dict, Optional, Type

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Namespace registry
# ---------------------------------------------------------------------------
# Maps namespace prefixes to base module paths.
# Built-in namespace is always registered.  External namespaces are loaded
# from packages.yml via dlt_saga.packages.load_packages().

_NAMESPACE_REGISTRY: Dict[str, str] = {
    "dlt_saga": "dlt_saga.pipelines",
}

# Tracks where each namespace was registered from (for saga info / saga doctor).
_NAMESPACE_SOURCES: Dict[str, str] = {
    "dlt_saga": "built-in",
}

_DEFAULT_NAMESPACE = "dlt_saga"

_ep_loaded = False
_ep_lock = threading.Lock()


def register_namespace(
    namespace: str, base_module: str, source: str = "packages.yml"
) -> None:
    """Register a pipeline namespace.

    Args:
        namespace: Short prefix used in adapter (e.g., 'local')
        base_module: Python module path the namespace maps to
        source: Where this registration came from ('built-in', 'packages.yml', 'entry-point')
    """
    _NAMESPACE_REGISTRY[namespace] = base_module
    _NAMESPACE_SOURCES[namespace] = source


# ---------------------------------------------------------------------------
# Class auto-discovery
# ---------------------------------------------------------------------------


def _find_pipeline_class(module: types.ModuleType) -> Type:
    """Find the Pipeline class in *module* by ``BasePipeline`` inheritance.

    Only considers classes **defined** in the module (``__module__`` check)
    so that imported parent classes are excluded.  This naturally finds the
    leaf class even with multi-level inheritance.
    """
    from dlt_saga.pipelines.base_pipeline import BasePipeline

    candidates = [
        obj
        for name, obj in vars(module).items()
        if isinstance(obj, type)
        and issubclass(obj, BasePipeline)
        and obj is not BasePipeline
        and obj.__module__ == module.__name__
    ]
    if len(candidates) == 1:
        return candidates[0]
    if len(candidates) == 0:
        raise ImportError(f"No BasePipeline subclass found in {module.__name__}")
    raise ImportError(
        f"Multiple BasePipeline subclasses in {module.__name__}: "
        f"{[c.__name__ for c in candidates]}. Cannot auto-resolve."
    )


# ---------------------------------------------------------------------------
# Module resolution helpers
# ---------------------------------------------------------------------------


def _resolve_via_namespace(adapter: str) -> Optional[str]:
    """Resolve *adapter* to a full module path using the namespace registry.

    Returns the full module path, or ``None`` if no namespace matched.
    """
    parts = adapter.split(".", 1)
    namespace = parts[0]

    if namespace in _NAMESPACE_REGISTRY:
        base = _NAMESPACE_REGISTRY[namespace]
        if len(parts) > 1:
            return f"{base}.{parts[1]}.pipeline"
        # Namespace only, no sub-path – resolve to base pipeline
        return f"{base}.pipeline"

    return None


def _try_import(module_path: str, label: str) -> Optional[Type]:
    """Try to import *module_path* and auto-discover the Pipeline class."""
    try:
        module = importlib.import_module(module_path)
        cls = _find_pipeline_class(module)
        logger.debug(f"[Registry] {label}: found {cls.__name__} in {module_path}")
        return cls
    except (ImportError, AttributeError) as e:
        logger.debug(f"[Registry] {label}: {module_path} failed – {e}")
        return None


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def _load_entry_point_pipelines() -> None:
    """Load pipeline namespaces from installed entry points (idempotent).

    Each entry point in the ``dlt_saga.pipelines`` group registers a namespace:
    - EP name  = namespace prefix (e.g., ``acme``)
    - EP value = base Python module path (e.g., ``acme_saga_pipelines.pipelines``)

    The module is NOT imported here — only the string value is used so that
    ``saga list`` remains fast even when plugin packages have heavy dependencies.
    The actual import happens on first pipeline resolution.
    """
    global _ep_loaded
    # Fast path: already loaded (no lock needed, _ep_loaded only transitions False→True)
    if _ep_loaded:
        return
    with _ep_lock:
        # Double-checked locking: re-test inside the lock
        if _ep_loaded:
            return

        try:
            from importlib.metadata import entry_points

            eps = entry_points(group="dlt_saga.pipelines")
            for ep in eps:
                # Use ep.value directly — never call ep.load() here.
                # Strip :attribute if someone accidentally includes one.
                base_module = ep.value.split(":")[0]
                register_namespace(ep.name, base_module, source="entry-point")
                logger.debug(
                    "[Registry] entry-point namespace '%s' → '%s'", ep.name, base_module
                )
        except Exception as e:
            logger.debug("[Registry] Entry point loading failed: %s", e)

        # Mark as loaded only after all registrations are complete.
        _ep_loaded = True


def _ensure_packages_loaded() -> None:
    """Lazily load external packages from packages.yml and entry points (idempotent)."""
    from dlt_saga.packages import load_packages

    load_packages()
    _load_entry_point_pipelines()


def get_pipeline_class(
    pipeline_group: str,
    config_path: Optional[str] = None,
    adapter: Optional[str] = None,
) -> Type:
    """Get pipeline class for the specified pipeline.

    Resolution order:
    1. Explicit *adapter* → namespace-based resolution.
    2. Folder-structure fallback (if *config_path* given).
    3. Base implementation for *pipeline_group*.

    Args:
        pipeline_group: Pipeline group (e.g., ``api``, ``filesystem``).
            Used only for folder-structure fallback and base resolution.
        config_path: Optional path to config file (for folder fallback).
        adapter: Optional explicit implementation identifier
            (e.g., ``dlt_saga.api.genesyscloud``).

    Returns:
        Pipeline class (a subclass of ``BasePipeline``).
    """
    _ensure_packages_loaded()
    logger.debug(
        f"[Registry] Resolving: adapter={adapter!r}, "
        f"pipeline_group={pipeline_group!r}, config_path={config_path!r}"
    )

    # ------------------------------------------------------------------
    # 1. Explicit adapter
    # ------------------------------------------------------------------
    if adapter:
        # a. Try namespace-based resolution
        module_path = _resolve_via_namespace(adapter)
        if module_path:
            try:
                module = importlib.import_module(module_path)
                cls = _find_pipeline_class(module)
                logger.debug(
                    f"[Registry] namespace: found {cls.__name__} in {module_path}"
                )
                return cls
            except ImportError as e:
                raise ImportError(
                    f"adapter '{adapter}' resolved to module '{module_path}' "
                    f"but could not be imported: {e}"
                ) from e
            except Exception as e:
                raise ImportError(
                    f"adapter '{adapter}' resolved to module '{module_path}' "
                    f"but no BasePipeline subclass was found: {e}"
                ) from e

        # b. No namespace match → treat as bare name under default namespace
        default_base = _NAMESPACE_REGISTRY[_DEFAULT_NAMESPACE]
        bare_path = f"{default_base}.{adapter}.pipeline"
        cls = _try_import(bare_path, "bare (default namespace)")
        if cls:
            return cls

        raise ImportError(
            f"adapter '{adapter}' could not be resolved. "
            f"Tried namespace lookup and bare name under '{_DEFAULT_NAMESPACE}'. "
            f"Available namespaces: {list(_NAMESPACE_REGISTRY.keys())}"
        )

    # ------------------------------------------------------------------
    # 2. Folder-structure fallback (unchanged logic)
    # ------------------------------------------------------------------
    if config_path:
        cls = _resolve_from_config_path(pipeline_group, config_path)
        if cls:
            return cls

    # ------------------------------------------------------------------
    # 3. Base implementation
    # ------------------------------------------------------------------
    base_module = f"dlt_saga.pipelines.{pipeline_group}.pipeline"
    cls = _try_import(base_module, "base")
    if cls:
        return cls

    raise ImportError(f"Could not find pipeline module for group '{pipeline_group}'")


def _resolve_from_config_path(pipeline_group: str, config_path: str) -> Optional[Type]:
    """Try to resolve a pipeline class from the config file path.

    Walks the config path to find progressively less specific implementations:
    1. Config-filename-based: ``pipelines/<group>/<subdir>/<filename>/pipeline.py``
    2. Subdirectory-based: ``pipelines/<group>/<subdir>/pipeline.py``
    """
    from pathlib import Path

    try:
        normalized = config_path.replace("\\", "/")
        path_obj = Path(normalized)
        if "configs" not in path_obj.parts:
            return None

        configs_idx = path_obj.parts.index("configs")

        # Need at least a subdirectory after pipeline_group
        if configs_idx + 2 >= len(path_obj.parts):
            return None

        nested_dir = path_obj.parts[configs_idx + 2]

        # Try config-filename-based (most specific)
        if configs_idx + 3 < len(path_obj.parts):
            filename = path_obj.stem
            module_path = (
                f"dlt_saga.pipelines.{pipeline_group}.{nested_dir}.{filename}.pipeline"
            )
            cls = _try_import(module_path, "config-filename")
            if cls:
                return cls

        # Try subdirectory-based
        module_path = f"dlt_saga.pipelines.{pipeline_group}.{nested_dir}.pipeline"
        cls = _try_import(module_path, "subdirectory")
        if cls:
            return cls

    except (ValueError, IndexError):
        pass

    return None


# ---------------------------------------------------------------------------
# Discovery (for saga list --implementations)
# ---------------------------------------------------------------------------


def discover_implementations() -> list[dict]:
    """Discover all available pipeline implementations.

    Scans built-in and external pipeline packages and returns metadata for each.

    Returns:
        List of dicts with keys: namespace, path, module, class_name, source.
    """
    import pkgutil

    _ensure_packages_loaded()

    results = []

    for namespace, base_module in _NAMESPACE_REGISTRY.items():
        try:
            base = importlib.import_module(base_module)
        except ImportError:
            continue

        if not hasattr(base, "__path__"):
            continue

        # Walk all sub-packages looking for pipeline.py modules
        for importer, modname, ispkg in pkgutil.walk_packages(
            base.__path__, prefix=f"{base_module}."
        ):
            if not modname.endswith(".pipeline"):
                continue
            # Skip if it's a sub-module of a pipeline module
            # (e.g., pipeline.something — shouldn't exist but guard)
            try:
                mod = importlib.import_module(modname)
                cls = _find_pipeline_class(mod)
            except (ImportError, AttributeError):
                continue

            # Build the adapter value from the module path
            # e.g., dlt_saga.pipelines.api.genesyscloud.pipeline → api.genesyscloud
            relative = modname.removeprefix(f"{base_module}.")
            impl_path = relative.removesuffix(".pipeline")
            full_impl = f"{namespace}.{impl_path}" if impl_path else namespace

            results.append(
                {
                    "namespace": namespace,
                    "path": impl_path or "(base)",
                    "adapter": full_impl,
                    "module": modname,
                    "class_name": cls.__name__,
                    "source": _NAMESPACE_SOURCES.get(namespace, "external"),
                }
            )

    return sorted(results, key=lambda r: (r["namespace"], r["path"]))
