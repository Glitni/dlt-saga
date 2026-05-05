"""Optional dependency helpers.

Provides clear error messages when optional packages are missing,
guiding users to the correct ``pip install dlt-saga[extra]`` command.

The mapping from import names to extras is derived at runtime from the
installed ``dlt-saga`` package metadata — no hardcoded list to maintain.
"""

import logging
import re
from typing import Dict, Optional

logger = logging.getLogger(__name__)

_DIST_PACKAGE = "dlt-saga"

# Cached mapping: {top_level_import_name: extra}
_import_to_extra: Optional[Dict[str, str]] = None


def _build_import_to_extra() -> Dict[str, str]:
    """Build {top_level_import_name: extra} from installed package metadata.

    1. Parse ``Requires-Dist`` entries to find ``{distribution_name: extra}``.
    2. For each distribution, read ``top_level.txt`` to get the actual
       top-level import name(s).  Falls back to normalising the
       distribution name (``-`` → ``_``) when the file is absent.
    """
    from importlib.metadata import distribution, requires

    # Step 1: {dist_name_lower: extra}
    dist_to_extra: Dict[str, str] = {}
    for req in requires(_DIST_PACKAGE) or []:
        dist_match = re.match(r"([a-zA-Z0-9_.-]+)", req.split("[")[0].split(";")[0])
        extra_match = re.search(r'extra\s*==\s*"([^"]+)"', req)
        if dist_match and extra_match:
            dist_to_extra[dist_match.group(1).lower()] = extra_match.group(1)

    # Step 2: resolve dist names → top-level import names
    result: Dict[str, str] = {}
    for dist_name, extra in dist_to_extra.items():
        try:
            dist = distribution(dist_name)
            top_level_txt = dist.read_text("top_level.txt")
            if top_level_txt:
                for pkg in top_level_txt.strip().split():
                    result[pkg] = extra
                continue
        except Exception as exc:
            logger.debug(
                "Could not read top_level.txt for %s; "
                "falling back to normalised dist name. Reason: %s",
                dist_name,
                exc,
            )
        # Fallback: normalise distribution name as import name
        result[dist_name.replace("-", "_")] = extra

    return result


def _find_extra(package: str) -> Optional[str]:
    """Find the pip extra that provides *package*.

    Matches the top-level component of *package* (e.g.
    ``google.cloud.bigquery`` → ``google``) against the extras declared
    in the ``dlt-saga`` package metadata.
    """
    global _import_to_extra
    if _import_to_extra is None:
        try:
            _import_to_extra = _build_import_to_extra()
        except Exception as exc:
            logger.debug(
                "Could not build import-to-extra mapping; "
                "extras hints will be unavailable. Reason: %s",
                exc,
            )
            _import_to_extra = {}

    top_level = package.split(".")[0]
    return _import_to_extra.get(top_level)


def require_optional(package: str, purpose: str) -> None:
    """Import *package* or raise a helpful error.

    Call this at the top of functions that depend on optional packages.
    The error message tells the user exactly which extra to install.

    Args:
        package: Dotted module name to import (e.g. ``"googleapiclient"``).
        purpose: Short description of what needs the package
                 (e.g. ``"Google Sheets pipelines"``).

    Raises:
        ImportError: With a message pointing to the correct pip extra.

    Example::

        require_optional("googleapiclient", "Google Sheets pipelines")
    """
    try:
        __import__(package)
    except ImportError:
        extra = _find_extra(package)
        if extra:
            install_hint = f'pip install "dlt-saga[{extra}]"'
        else:
            install_hint = f"pip install {package}"

        raise ImportError(
            f"{purpose} requires the '{package}' package. "
            f"Install it with: {install_hint}"
        ) from None
