"""Pipeline selector parsing and filtering logic.

This module implements dbt-style selector syntax for filtering pipeline configurations:
- Direct pipeline names: "google_sheets__data"
- Glob patterns: "google_sheets__*", "*balance*"
- Tag selectors: "tag:daily", "tag:critical", "tag:hourly:10" (with schedule value)
- Group selectors: "group:google_sheets"
- State selectors: "state:new", "state:failed" (see below)

Tag selector syntax:
- tag:hourly        - Match configs that should run at the CURRENT hour
- tag:daily         - Match configs that should run on the CURRENT day of month / weekday
- tag:hourly:10     - Match configs that run at hour 10 (explicit override)
- tag:daily:2       - Match configs that run on day 2 of month (explicit override)
- tag:daily:monday  - Match configs that run on Mondays (explicit weekday override)
- tag:critical      - Match any config with "critical" tag (non-schedule tags)

Schedule-aware behavior:
- "hourly" and "daily" tags automatically use current time when no value specified
- A config with "hourly: [1, 10]" only matches "tag:hourly" when current hour is 1 or 10
- A config with "hourly" (no values) matches "tag:hourly" at any hour
- A config with "daily: [2, 28]" only matches "tag:daily" on the 2nd or 28th
- A config with "daily: [monday]" only matches "tag:daily" on Mondays
- A config with "daily: [2, monday]" matches on the 2nd OR on Mondays
- A config with "hourly: [monday]" matches "tag:hourly" any hour on Mondays
- A config with "hourly: [{monday: [6]}, 9]" matches Mon@6am OR every day@9am
- Other tags (critical, api, etc.) match regardless of time

Schedule values in config files:
    tags:
      - daily              # Runs every day
      - hourly: [1, 10]    # Runs at 1am and 10am
      - daily:
        - 2
        - 28               # Runs on 2nd and 28th of month
      - daily:
        - 2
        - monday           # Runs on 2nd and every Monday
      - hourly:
        - monday: [6]      # Runs Mondays at 6am
        - tuesday: [6]     # AND Tuesdays at 6am
        - 9                # AND every day at 9am

State selectors:
- state:new     - Pipelines whose target table does not exist yet
- state:failed  - Pipelines whose most recent recorded run failed

Unlike every other selector these read warehouse state rather than config, so
they require a :class:`~dlt_saga.utility.cli.pipeline_state.PipelineStateResolver`
(passed as ``state``) and are scoped to the target being run — a pipeline can be
new in dev and not in prod. "new" is judged per layer: the ingest target for
``saga ingest``, the historized target for ``saga historize``, either one for a
command spanning both. See that module for the semantics and their rationale.

An unrecognized prefix (``tags:daily``, ``owner:me``) is a syntax error rather
than a selector that matches nothing — see :class:`SelectorSyntaxError`. A
recognized prefix validates its own value: an unknown ``state:`` keyword raises
``StateSelectorError``, while ``tag:`` and ``group:`` take open-ended values and
simply match nothing.

Selector combinations:
- Space-separated: UNION (OR) - "tag:daily group:google_sheets"
- Comma-separated: INTERSECTION (AND) - "tag:daily,group:google_sheets"
- State composes like any other: "tag:daily,state:new" is daily AND new
"""

import fnmatch
import logging
import re
from datetime import datetime, timezone
from typing import Dict, List, Optional

from dlt_saga.pipeline_config.base_config import PipelineConfig, normalize_weekday
from dlt_saga.utility.cli.pipeline_state import (
    STATE_KEYWORDS,
    PipelineStateResolver,
    StateSelectorError,
)

# Tags that have schedule semantics and should use current time
SCHEDULE_AWARE_TAGS = {"hourly", "daily"}


class SelectorSyntaxError(ValueError):
    """A selector that isn't valid syntax (rather than one matching nothing).

    A configuration error (subclasses ``ValueError``), so callers render it as
    a message without a traceback.
    """


logger = logging.getLogger(__name__)


class PipelineSelector:
    """Parses and applies dbt-style selectors to filter pipeline configurations."""

    def __init__(
        self,
        all_configs: Dict[str, List[PipelineConfig]],
        state: Optional[PipelineStateResolver] = None,
    ):
        """Initialize selector with all available configs.

        Args:
            all_configs: Dictionary of configs organized by pipeline type
            state: Resolver backing the ``state:`` selectors. Required only
                when the selection uses one; build it with
                :func:`~dlt_saga.utility.cli.pipeline_state.build_state_resolver`,
                which returns None for a selection that needs no warehouse read.
        """
        self.all_configs = all_configs
        self.state = state
        # Flatten for easier searching
        self.flat_configs: List[PipelineConfig] = []
        for configs in all_configs.values():
            self.flat_configs.extend(configs)

    def select(
        self,
        selectors: Optional[List[str]] = None,
        warn_on_no_match: bool = True,
    ) -> Dict[str, List[PipelineConfig]]:
        """Apply selectors to filter configs.

        Args:
            selectors: List of selector strings (space-separated = UNION, comma-separated = INTERSECTION)
                      None or empty list returns all configs
            warn_on_no_match: Emit a warning for selectors that match nothing.
                      Set False when selecting against a subset (e.g. the
                      disabled-config probe), where a non-match is expected and
                      the warning would contradict a successful enabled match.

        Returns:
            Filtered configs organized by pipeline type

        Examples:
            select(None) -> all configs
            select(["google_sheets__data"]) -> exact match
            select(["*balance*"]) -> glob pattern
            select(["tag:daily"]) -> configs with daily tag
            select(["group:google_sheets"]) -> all google_sheets configs
            select(["tag:daily", "group:google_sheets"]) -> UNION (configs with daily tag OR google_sheets group)
            select(["tag:daily group:google_sheets"]) -> UNION (space-separated in single string)
            select(["tag:daily,group:google_sheets"]) -> INTERSECTION (google_sheets configs with daily tag)
        """
        # Default: return all configs if no selectors
        if not selectors:
            return self.all_configs

        # Collect all selected configs using UNION logic
        # Use dict to deduplicate by identifier (since PipelineConfig is not hashable)
        selected_dict = {}

        for selector_group in selectors:
            # Collapse whitespace around commas first, so a comma followed by a
            # space reads as one intersection token rather than being torn apart
            # by the whitespace (UNION) split below: "tag:daily, group:x" then
            # behaves like "tag:daily,group:x" (AND), not "tag:daily," OR "group:x".
            normalized = re.sub(r"\s*,\s*", ",", selector_group)

            # Split by spaces to handle UNION within a single --select argument
            # Example: --select "tag:daily group:google_sheets" -> ["tag:daily", "group:google_sheets"]
            space_separated = normalized.split()

            for selector in space_separated:
                # Handle INTERSECTION logic (comma-separated within a selector).
                # Drop empty tokens so a trailing/doubled comma doesn't intersect
                # against an empty selector (which matches nothing) and silently
                # narrow the whole group to zero.
                if "," in selector:
                    parts = [p for p in selector.split(",") if p]
                    if not parts:
                        continue
                    matched = self._select_intersection(parts)
                else:
                    matched = self._select_single(selector)

                # Surface selectors that match nothing (dbt does the same) so a
                # typo'd tag/group/name doesn't silently narrow the run to zero.
                if not matched and warn_on_no_match:
                    logger.warning(
                        "Selector '%s' did not match any pipelines.", selector
                    )

                # Add to dict using identifier as key for deduplication
                for config in matched:
                    selected_dict[config.identifier] = config

        # Convert back to dict organized by group
        return self._organize_by_group(list(selected_dict.values()))

    def _select_single(self, selector: str) -> List[PipelineConfig]:
        """Apply a single selector to get matching configs.

        Args:
            selector: Single selector string (e.g., "tag:daily", "group:google_sheets", "google_sheets__*")

        Returns:
            List of matching configs
        """
        selector = selector.strip()

        # State selector: state:new or state:failed
        if selector.startswith("state:"):
            return self._select_state(selector[6:])

        # Tag selector: tag:daily or tag:hourly:10 or tag:daily:monday
        if selector.startswith("tag:"):
            return self._select_tag(selector[4:])

        # Group selector: group:google_sheets
        if selector.startswith("group:"):
            pipeline_group = selector.split(":", 1)[1]
            return self.all_configs.get(pipeline_group, [])

        # The prefix set above is closed, so anything else carrying a ':' is a
        # mistyped prefix rather than a name — pipeline names and glob patterns
        # are built from config paths and never contain one. Raise instead of
        # falling through to the name match, where an unknown prefix would be
        # indistinguishable from a valid selector that matched nothing, and a
        # scheduled run would quietly do nothing and report success.
        if ":" in selector:
            supported = ["tag:<name>", "group:<name>"] + [
                f"state:{keyword}" for keyword in STATE_KEYWORDS
            ]
            raise SelectorSyntaxError(
                f"Unknown selector '{selector}'. Supported prefixes: "
                f"{', '.join(supported)}. A pipeline name or glob pattern "
                f"cannot contain ':'."
            )

        # Pipeline name (exact or glob pattern)
        # Try exact match first
        exact_matches = [c for c in self.flat_configs if c.pipeline_name == selector]
        if exact_matches:
            return exact_matches

        # Try table_name match
        exact_table_matches = [c for c in self.flat_configs if c.table_name == selector]
        if exact_table_matches:
            return exact_table_matches

        # Try glob pattern matching on pipeline_name
        if "*" in selector or "?" in selector:
            return [
                c
                for c in self.flat_configs
                if fnmatch.fnmatch(c.pipeline_name, selector)
            ]

        # No matches found for this selector
        return []

    def _select_tag(self, tag_part: str) -> List[PipelineConfig]:
        """Apply the part of a ``tag:`` selector after the prefix.

        Args:
            tag_part: e.g. ``"daily"``, ``"hourly:10"``, ``"daily:monday"``.

        Returns:
            List of matching configs
        """
        # Check if there's an explicit schedule value (e.g., "hourly:10", "daily:monday")
        if ":" in tag_part:
            tag_name, value_str = tag_part.split(":", 1)
            # Try int first, then weekday name
            try:
                tag_value = int(value_str)
                # Explicit int value: use exact=True
                return [
                    c
                    for c in self.flat_configs
                    if c.has_tag(tag_name, tag_value, exact=True)
                ]
            except ValueError:
                weekday = normalize_weekday(value_str)
                if weekday is not None:
                    # Explicit weekday: use exact=True
                    return [
                        c
                        for c in self.flat_configs
                        if c.has_tag(tag_name, exact=True, query_weekday=weekday)
                    ]
                logger.warning(
                    f"Invalid tag value '{value_str}', must be integer or weekday name"
                )
                return []
        else:
            # No explicit value - check if this is a schedule-aware tag
            tag_name = tag_part
            if tag_name in SCHEDULE_AWARE_TAGS:
                # Use current time for schedule-aware tags
                # exact=False: include configs with no values (run always)
                tag_value, weekday = self._get_current_schedule_value(tag_name)
                logger.debug(
                    f"Schedule-aware tag '{tag_name}' using current value: "
                    f"{tag_value}" + (f" ({weekday})" if weekday else "")
                )
                return [
                    c
                    for c in self.flat_configs
                    if c.has_tag(
                        tag_name, tag_value, exact=False, query_weekday=weekday
                    )
                ]
            else:
                # Non-schedule tag - match any config with this tag
                return [c for c in self.flat_configs if c.has_tag(tag_name)]

    def _select_state(self, keyword: str) -> List[PipelineConfig]:
        """Apply a ``state:`` selector against warehouse state.

        Args:
            keyword: The part after ``state:`` (e.g. ``"new"``).

        Returns:
            Matching configs.

        Raises:
            StateSelectorError: Unknown keyword, or no resolver available. Both
                are raised rather than matching nothing: ``state:`` is a closed
                vocabulary, so a typo is a mistake to surface — silently
                selecting zero pipelines would let a scheduled run do nothing
                and look successful.
        """
        if keyword not in STATE_KEYWORDS:
            raise StateSelectorError(
                f"Unknown state selector 'state:{keyword}'. "
                f"Supported: {', '.join(f'state:{k}' for k in STATE_KEYWORDS)}."
            )
        if self.state is None:
            raise StateSelectorError(
                f"Selector 'state:{keyword}' needs warehouse state, but none "
                f"was provided to the selector. This is a wiring bug — build a "
                f"resolver with build_state_resolver()."
            )
        if keyword == "new":
            return [c for c in self.flat_configs if self.state.is_new(c)]
        return [c for c in self.flat_configs if self.state.last_run_failed(c)]

    def _select_intersection(self, selectors: List[str]) -> List[PipelineConfig]:
        """Apply multiple selectors with INTERSECTION (AND) logic.

        Args:
            selectors: List of selector strings that must ALL match

        Returns:
            List of configs matching ALL selectors
        """
        if not selectors:
            return []

        # Start with first selector's results as a dict (keyed by identifier)
        result_dict = {c.identifier: c for c in self._select_single(selectors[0])}

        # Intersect with remaining selectors
        for selector in selectors[1:]:
            matched_dict = {c.identifier: c for c in self._select_single(selector)}
            # Keep only configs that are in both dicts
            result_dict = {
                ident: config
                for ident, config in result_dict.items()
                if ident in matched_dict
            }

        return list(result_dict.values())

    def _get_current_schedule_value(self, tag_name: str) -> tuple[int, Optional[str]]:
        """Get the current schedule value for a schedule-aware tag.

        Uses UTC time for consistency across environments (Cloud Run, local dev, etc.).

        Args:
            tag_name: Name of the schedule-aware tag ("hourly" or "daily")

        Returns:
            Tuple of (numeric_value, weekday_name_or_none):
            - "hourly": (current_hour 0-23, current_weekday e.g. "monday")
              Weekday is included so per-weekday hourly bindings
              (e.g. `hourly: [{monday: [6]}]`) and bare-weekday entries
              (e.g. `hourly: [monday]`) can be matched.
            - "daily": (current_day_of_month 1-31, current_weekday e.g. "monday")
        """
        now = datetime.now(timezone.utc)
        if tag_name == "hourly":
            return (now.hour, now.strftime("%A").lower())
        elif tag_name == "daily":
            return (now.day, now.strftime("%A").lower())
        else:
            raise ValueError(f"Unknown schedule-aware tag: {tag_name}")

    def _organize_by_group(
        self, configs: List[PipelineConfig]
    ) -> Dict[str, List[PipelineConfig]]:
        """Organize flat list of configs into dict by pipeline group.

        Args:
            configs: Flat list of pipeline configs

        Returns:
            Dictionary organized by pipeline_group
        """
        organized: Dict[str, List[PipelineConfig]] = {}
        for config in configs:
            if config.pipeline_group not in organized:
                organized[config.pipeline_group] = []
            organized[config.pipeline_group].append(config)
        return organized


def format_config_list(
    configs: Dict[str, List[PipelineConfig]],
    disabled_configs: Optional[Dict[str, List[PipelineConfig]]] = None,
) -> str:
    """Format configs as a readable list.

    Args:
        configs: Enabled configs organized by type
        disabled_configs: Optional disabled configs to show

    Returns:
        Formatted string for display
    """
    lines = ["Available pipelines:\n"]

    # Show enabled configs by group
    total_enabled = 0
    for group in sorted(configs.keys()):
        pipeline_configs = configs[group]
        total_enabled += len(pipeline_configs)

        lines.append(f"{group} ({len(pipeline_configs)} pipelines):")
        for config in sorted(pipeline_configs, key=lambda c: c.pipeline_name):
            tag_str = (
                f" [tags: {', '.join(str(t) for t in config.tags)}]"
                if config.tags
                else ""
            )
            lines.append(f"  - {config.pipeline_name}{tag_str}")
        lines.append("")  # Blank line between types

    lines.append(
        f"Total: {total_enabled} enabled pipeline{'s' if total_enabled != 1 else ''}"
    )

    # Show disabled configs if any
    if disabled_configs:
        total_disabled = sum(len(cfgs) for cfgs in disabled_configs.values())
        if total_disabled > 0:
            lines.append(f"\nDisabled ({total_disabled}):")
            for group in sorted(disabled_configs.keys()):
                for config in sorted(
                    disabled_configs[group], key=lambda c: c.pipeline_name
                ):
                    lines.append(f"  - {config.pipeline_name} [disabled]")

    return "\n".join(lines)
