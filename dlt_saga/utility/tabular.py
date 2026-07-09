"""Helpers for parsing tabular sources (CSV, spreadsheets) into rows."""

import logging
from typing import Dict, List, Sequence

logger = logging.getLogger(__name__)


def dedupe_headers(headers: Sequence[object], *, source: str = "") -> List[str]:
    """Return column headers with duplicate names disambiguated by a suffix.

    Row parsers that build dicts keyed on the header row — ``csv.DictReader``,
    ``dict(zip(headers, row))``, ``dict(zip_longest(...))`` — silently keep only
    the last value when two columns share a name, dropping every earlier column's
    data. This renames repeats to ``<name>_2``, ``<name>_3`` … (the first
    occurrence is kept as-is) so all columns survive, and logs a warning naming
    the collisions rather than dropping data silently.

    Args:
        headers: The raw header cells (coerced to ``str``).
        source: Optional human-readable source label included in the warning
            (e.g. ``"sheet 'Data'"``, ``"SharePoint CSV"``).

    Returns:
        A list of header names of the same length as ``headers``, with no
        duplicates.
    """
    counts: Dict[str, int] = {}
    used: set[str] = set()
    result: List[str] = []
    collisions: List[str] = []

    for header in headers:
        name = str(header)
        if name not in used:
            used.add(name)
            counts[name] = 1
            result.append(name)
            continue

        collisions.append(name)
        n = counts.get(name, 1) + 1
        candidate = f"{name}_{n}"
        # A suffixed name could itself collide with an existing column (raw or
        # already generated); keep bumping the counter until we land on a free
        # name.
        while candidate in used:
            n += 1
            candidate = f"{name}_{n}"
        counts[name] = n
        used.add(candidate)
        # Register the generated name too, so a later raw header equal to it
        # gets its own suffix instead of raising.
        counts[candidate] = 1
        result.append(candidate)

    if collisions:
        where = f" in {source}" if source else ""
        logger.warning(
            "Duplicate column header(s) %s%s renamed with a numeric suffix to "
            "avoid silently dropping data; consider fixing the source's header row.",
            sorted(set(collisions)),
            where,
        )

    return result
