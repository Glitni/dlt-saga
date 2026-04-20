"""Package loader for external pipeline implementations.

Reads ``packages.yml`` from the project root and registers pipeline namespaces
for ``adapter`` resolution in the registry.

Usage::

    # packages.yml
    packages:
      - path: ./my_pipelines      # local Python package with pipeline.py modules
        namespace: local           # prefix used in adapter values

Then in pipeline configs::

    adapter: local.api.my_custom   # resolves via the 'local' namespace
"""

import logging
import sys
import threading
from dataclasses import dataclass, field
from pathlib import Path
from typing import List, Optional

import yaml

logger = logging.getLogger(__name__)

_loaded = False
_lock = threading.Lock()


@dataclass
class PackageEntry:
    """A single package entry in packages.yml."""

    namespace: str = field(
        metadata={
            "description": (
                "Short prefix used in adapter values (e.g., 'local', 'team_b'). "
                "Must not contain dots."
            ),
            "pattern": "^[a-zA-Z_][a-zA-Z0-9_]*$",
        },
    )
    path: str = field(
        metadata={
            "description": (
                "Path to a Python package containing pipeline.py modules "
                "(relative to project root or absolute)"
            ),
        },
    )


@dataclass
class PackagesConfig:
    """Top-level structure of packages.yml."""

    packages: List[PackageEntry] = field(
        default_factory=list,
        metadata={
            "description": "List of external pipeline packages to register",
        },
    )

    @classmethod
    def from_dict(cls, data: dict) -> "PackagesConfig":
        """Create a PackagesConfig from parsed YAML data.

        Args:
            data: Parsed YAML content

        Returns:
            PackagesConfig instance
        """
        raw_packages = data.get("packages", [])
        if not isinstance(raw_packages, list):
            logger.warning("packages.yml: 'packages' must be a list")
            return cls(packages=[])

        entries = []
        for idx, pkg in enumerate(raw_packages):
            if not isinstance(pkg, dict):
                logger.warning("packages.yml: entry %d is not a dict, skipping", idx)
                continue
            entries.append(
                PackageEntry(
                    namespace=pkg.get("namespace", ""),
                    path=pkg.get("path", ""),
                )
            )
        return cls(packages=entries)


def load_packages(project_root: Optional[Path] = None) -> None:
    """Load ``packages.yml`` and register all declared packages.

    This function is idempotent and thread-safe — calling it multiple times
    from concurrent workers is safe.

    Args:
        project_root: Root directory containing ``packages.yml``.
            If *None*, auto-detected by searching for ``saga_project.yml``
            or ``packages.yml`` from the current working directory upward.
    """
    global _loaded
    # Fast path: already loaded (no lock needed, _loaded only transitions False→True)
    if _loaded:
        return
    with _lock:
        # Double-checked locking: re-test inside the lock
        if _loaded:
            return

        if project_root is None:
            project_root = _find_project_root()

        packages_path = project_root / "packages.yml"
        if not packages_path.exists():
            logger.debug("No packages.yml found at %s", packages_path)
            _loaded = True
            return

        try:
            with open(packages_path) as f:
                data = yaml.safe_load(f) or {}
        except Exception as e:
            logger.warning("Failed to load packages.yml: %s", e)
            _loaded = True
            return

        config = PackagesConfig.from_dict(data)

        for idx, entry in enumerate(config.packages):
            try:
                _register_package(entry, project_root)
            except ValueError as e:
                logger.warning("packages.yml: entry %d skipped: %s", idx, e)

        if config.packages:
            logger.debug("Loaded %d package(s) from packages.yml", len(config.packages))

        # Mark as loaded only after all registrations are complete so that
        # concurrent threads cannot observe a partially-registered state.
        _loaded = True


def _register_package(entry: PackageEntry, project_root: Path) -> None:
    """Register a single package entry from ``packages.yml``."""
    from dlt_saga.pipelines.registry import register_namespace

    if not entry.namespace:
        raise ValueError("Package entry must have a 'namespace' field")
    if not entry.path:
        raise ValueError(f"Package '{entry.namespace}' must have a 'path' field")

    # Validate namespace format
    if "." in entry.namespace:
        raise ValueError(
            f"Package namespace '{entry.namespace}' must not contain dots. "
            "Use a simple identifier like 'local' or 'team_b'."
        )

    # Resolve path relative to project root
    abs_path = (project_root / entry.path).resolve()
    if not abs_path.is_dir():
        raise ValueError(f"Package '{entry.namespace}' path does not exist: {abs_path}")

    # Add parent directory to sys.path so the package is importable
    parent = str(abs_path.parent)
    if parent not in sys.path:
        sys.path.insert(0, parent)

    # The importable base module is the directory name
    base_module = abs_path.name
    register_namespace(entry.namespace, base_module)
    logger.info(
        "Registered package: %s → %s (from %s)",
        entry.namespace,
        base_module,
        abs_path,
    )


def _find_project_root() -> Path:
    """Find the project root by walking up from CWD.

    Looks for ``saga_project.yml`` or ``packages.yml`` as markers.
    Falls back to CWD if no marker found.
    """
    cwd = Path.cwd()
    for directory in [cwd, *cwd.parents]:
        if (directory / "saga_project.yml").exists():
            return directory
        if (directory / "packages.yml").exists():
            return directory
    return cwd


def reset() -> None:
    """Reset loader state. Intended for testing only."""
    global _loaded
    _loaded = False
