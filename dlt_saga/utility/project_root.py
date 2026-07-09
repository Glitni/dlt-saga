"""Locate the saga project root.

Kept in a dependency-free leaf module so both ``packages`` and
``project_config`` can share it without forming an import cycle.
"""

from pathlib import Path


def find_project_root() -> Path:
    """Find the project root by walking up from the current working directory.

    Looks for ``saga_project.yml`` or ``packages.yml`` as markers. Falls back to
    the current working directory if no marker is found.
    """
    cwd = Path.cwd()
    for directory in [cwd, *cwd.parents]:
        if (directory / "saga_project.yml").exists():
            return directory
        if (directory / "packages.yml").exists():
            return directory
    return cwd
