"""Pipeline selector parsing and filtering logic.

This module implements dbt-style selector syntax for filtering pipeline configurations:
- Direct pipeline names: "google_sheets__data"
- Glob patterns: "google_sheets__*", "*balance*"
- Tag selectors: "tag:daily", "tag:critical", "tag:hourly:10" (with schedule value)
- Group selectors: "group:google_sheets"

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

Selector combinations:
- Space-separated: UNION (OR) - "tag:daily type:google_sheets"
- Comma-separated: INTERSECTION (AND) - "tag:daily,type:google_sheets"
"""

import fnmatch
import logging
from datetime import datetime, timezone
from typing import Dict, List, Optional

from dlt_saga.pipeline_config.base_config import PipelineConfig, normalize_weekday

# Tags that have schedule semantics and should use current time
SCHEDULE_AWARE_TAGS = {"hourly", "daily"}

logger = logging.getLogger(__name__)


class PipelineSelector:
    """Parses and applies dbt-style selectors to filter pipeline configurations."""

    def __init__(self, all_configs: Dict[str, List[PipelineConfig]]):
        """Initialize selector with all available configs.

        Args:
            all_configs: Dictionary of configs organized by pipeline type
        """
        self.all_configs = all_configs
        # Flatten for easier searching
        self.flat_configs: List[PipelineConfig] = []
        for configs in all_configs.values():
            self.flat_configs.extend(configs)

    def select(
        self, selectors: Optional[List[str]] = None
    ) -> Dict[str, List[PipelineConfig]]:
        """Apply selectors to filter configs.

        Args:
            selectors: List of selector strings (space-separated = UNION, comma-separated = INTERSECTION)
                      None or empty list returns all configs

        Returns:
            Filtered configs organized by pipeline type

        Examples:
            select(None) -> all configs
            select(["google_sheets__data"]) -> exact match
            select(["*balance*"]) -> glob pattern
            select(["tag:daily"]) -> configs with daily tag
            select(["type:google_sheets"]) -> all google_sheets configs
            select(["tag:daily", "type:google_sheets"]) -> UNION (configs with daily tag OR google_sheets type)
            select(["tag:daily type:google_sheets"]) -> UNION (space-separated in single string)
            select(["tag:daily,type:google_sheets"]) -> INTERSECTION (google_sheets configs with daily tag)
        """
        # Default: return all configs if no selectors
        if not selectors:
            return self.all_configs

        # Collect all selected configs using UNION logic
        # Use dict to deduplicate by identifier (since PipelineConfig is not hashable)
        selected_dict = {}

        for selector_group in selectors:
            # First, split by spaces to handle UNION within a single --select argument
            # Example: --select "tag:daily type:google_sheets" -> ["tag:daily", "type:google_sheets"]
            space_separated = selector_group.split()

            for selector in space_separated:
                # Handle INTERSECTION logic (comma-separated within a selector)
                if "," in selector:
                    matched = self._select_intersection(selector.split(","))
                else:
                    matched = self._select_single(selector)

                # Add to dict using identifier as key for deduplication
                for config in matched:
                    selected_dict[config.identifier] = config

        # Convert back to dict organized by group
        return self._organize_by_group(list(selected_dict.values()))

    def _select_single(self, selector: str) -> List[PipelineConfig]:
        """Apply a single selector to get matching configs.

        Args:
            selector: Single selector string (e.g., "tag:daily", "type:google_sheets", "google_sheets__*")

        Returns:
            List of matching configs
        """
        selector = selector.strip()

        # Tag selector: tag:daily or tag:hourly:10 or tag:daily:monday
        if selector.startswith("tag:"):
            tag_part = selector[4:]
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

        # Group selector: group:google_sheets
        if selector.startswith("group:"):
            pipeline_group = selector.split(":", 1)[1]
            return self.all_configs.get(pipeline_group, [])

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
            - "hourly": (current_hour 0-23, None)
            - "daily": (current_day_of_month 1-31, current_weekday e.g. "monday")
        """
        now = datetime.now(timezone.utc)
        if tag_name == "hourly":
            return (now.hour, None)
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
