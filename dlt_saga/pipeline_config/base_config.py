"""Base configuration abstractions for pipeline configs.

This module defines the abstract interfaces and data models for managing
pipeline configurations from any source.
"""

import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple, Union

logger = logging.getLogger(__name__)

# Canonical weekday names (lowercase)
WEEKDAY_NAMES = {
    "monday",
    "tuesday",
    "wednesday",
    "thursday",
    "friday",
    "saturday",
    "sunday",
}

# Abbreviation mappings to canonical names
WEEKDAY_ABBREVS = {
    "mon": "monday",
    "tue": "tuesday",
    "wed": "wednesday",
    "thu": "thursday",
    "fri": "friday",
    "sat": "saturday",
    "sun": "sunday",
}


def normalize_weekday(value: str) -> Optional[str]:
    """Normalize a weekday string to canonical lowercase form.

    Accepts full names (case-insensitive) and common abbreviations.

    Args:
        value: Weekday string (e.g., "Monday", "mon", "FRIDAY")

    Returns:
        Canonical lowercase weekday name, or None if not a valid weekday.

    Examples:
        normalize_weekday("Monday") -> "monday"
        normalize_weekday("mon") -> "monday"
        normalize_weekday("FRIDAY") -> "friday"
        normalize_weekday("xyz") -> None
    """
    lower = value.strip().lower()
    if lower in WEEKDAY_NAMES:
        return lower
    return WEEKDAY_ABBREVS.get(lower)


@dataclass
class ScheduleTag:
    """Represents a tag with optional schedule values.

    Tags can be simple (e.g., "daily") or have specific values (e.g., "hourly: [1, 10]").
    Daily tags support both day-of-month numbers and weekday names. Hourly tags additionally
    support per-weekday hour bindings via dict entries.

    Examples:
        - Simple tag: ScheduleTag("daily") - runs every day
        - With values: ScheduleTag("hourly", [1, 10]) - runs at 1am and 10am every day
        - With values: ScheduleTag("daily", [2, 28]) - runs on 2nd and 28th of month
        - With weekdays: ScheduleTag("daily", ["monday"]) - runs every Monday
        - Mixed: ScheduleTag("daily", [2, "monday"]) - runs on 2nd and every Monday
        - Bare weekday in hourly: ScheduleTag("hourly", ["monday"]) - runs every hour on Mondays
        - Per-weekday hours: ScheduleTag("hourly", [{"monday": [6]}, 9])
          runs Mon@6 OR every day@9

    YAML formats supported:
        tags:
          - daily              # Simple tag
          - hourly: [1, 10]    # Tag with specific values
          - daily:
            - 2
            - 28
          - daily:
            - 2
            - monday           # Weekday name
          - hourly:
            - monday: [6]      # Per-weekday: Monday at 6am
            - tuesday: [6]     # Per-weekday: Tuesday at 6am
            - 9                # Plus every day at 9am
    """

    name: str
    # Each list element is one of:
    #   int               -> value (hour/day) every weekday
    #   str               -> weekday name (any value on that weekday)
    #   Dict[str, [int]]  -> {weekday: [values]} - weekday/value pairs
    # None means "all" (matches every value).
    values: Optional[List[Union[int, str, Dict[str, List[int]]]]] = None

    def matches(
        self,
        query_name: str,
        query_value: Optional[int] = None,
        exact: bool = False,
        query_weekday: Optional[str] = None,
    ) -> bool:
        """Check if this tag matches a query.

        Args:
            query_name: Tag name to match (e.g., "daily", "hourly")
            query_value: Optional specific value to match (e.g., 10 for 10am, 2 for 2nd day)
            exact: If True, only match tags that explicitly have the value in their list.
                   If False, tags with no values (run always) also match.
            query_weekday: Optional weekday name to match (e.g., "monday")

        Returns:
            True if the tag matches the query

        Examples:
            tag = ScheduleTag("hourly", [1, 10])
            tag.matches("hourly")           # True - matches tag name
            tag.matches("hourly", 10)       # True - runs at hour 10
            tag.matches("hourly", 10, exact=True)  # True - explicitly has 10
            tag.matches("hourly", 5)        # False - doesn't run at hour 5
            tag.matches("daily")            # False - wrong tag name

            tag = ScheduleTag("hourly")     # No values = all hours
            tag.matches("hourly", 10)       # True - runs every hour (auto-detect)
            tag.matches("hourly", 10, exact=True)  # False - no explicit values

            tag = ScheduleTag("daily", [2, "monday"])
            tag.matches("daily", 2)                          # True - day 2 of month
            tag.matches("daily", 15, query_weekday="monday") # True - it's a Monday
            tag.matches("daily", 15, query_weekday="friday") # False - not day 15, not Monday

            tag = ScheduleTag("hourly", [{"monday": [6]}, 9])
            tag.matches("hourly", 9)                          # True - every day @ 9
            tag.matches("hourly", 6, query_weekday="monday")  # True - Mon @ 6
            tag.matches("hourly", 6, query_weekday="tuesday") # False - Tue @ 6 not declared
        """
        if self.name != query_name:
            return False
        if query_value is None and query_weekday is None:
            return True  # Just checking tag name
        if self.values is None:
            # Tag has no specific values (runs every hour/day)
            # exact=True: only match if explicitly listed (so False here)
            # exact=False: match any value since we run always
            return not exact
        return any(
            _entry_matches(entry, query_value, query_weekday) for entry in self.values
        )

    def __str__(self) -> str:
        """String representation for display."""
        if self.values is None:
            return self.name
        return f"{self.name}:{','.join(_format_value(v) for v in self.values)}"

    def __eq__(self, other: object) -> bool:
        """Equality check - supports comparison with string for backwards compatibility."""
        if isinstance(other, ScheduleTag):
            return self.name == other.name and self.values == other.values
        if isinstance(other, str):
            # Allow comparison with simple string tags for backwards compatibility
            return self.name == other and self.values is None
        return False

    def __hash__(self) -> int:
        """Hash for use in sets/dicts."""
        if self.values is None:
            return hash((self.name, None))
        return hash((self.name, tuple(_hashable_value(v) for v in self.values)))


def _entry_matches(
    entry: Union[int, str, Dict[str, List[int]]],
    query_value: Optional[int],
    query_weekday: Optional[str],
) -> bool:
    """Check whether a single value-list entry matches a query.

    - dict {weekday: [values]}: requires matching weekday; if query_weekday is
      None (e.g. explicit "tag:hourly:6" selector), match on value alone.
    - str (bare weekday): matches when query_weekday equals the entry.
    - int (bare value): matches when query_value equals the entry.
    """
    if isinstance(entry, dict):
        weekday_key, value_list = next(iter(entry.items()))
        if query_weekday is not None and query_weekday != weekday_key:
            return False
        if query_value is None:
            return query_weekday is not None  # tag-name-only against weekday
        return query_value in value_list
    if isinstance(entry, str):
        return query_weekday is not None and query_weekday == entry
    return query_value is not None and query_value == entry


def _format_value(v: Union[int, str, Dict[str, List[int]]]) -> str:
    """Render a single ScheduleTag value entry for display."""
    if isinstance(v, dict):
        key, vals = next(iter(v.items()))
        return f"{key}:{','.join(str(x) for x in vals)}"
    return str(v)


def _hashable_value(v: Union[int, str, Dict[str, List[int]]]) -> Any:
    """Convert a value entry to a hashable form."""
    if isinstance(v, dict):
        key, vals = next(iter(v.items()))
        return (key, tuple(vals))
    return v


def parse_tag(tag_data: Union[str, Dict[str, Any]]) -> ScheduleTag:
    """Parse a tag from YAML format into a ScheduleTag object.

    String values in tag lists are validated as weekday names and normalized
    to canonical lowercase form (e.g., "Mon" -> "monday"). List entries may
    also be single-key dicts `{weekday: [values]}` expressing per-weekday
    value bindings (e.g. `hourly: [{monday: [6]}]` for Monday@6am).

    Args:
        tag_data: Either a string ("daily") or dict ({"hourly": [1, 10]})

    Returns:
        ScheduleTag instance

    Examples:
        parse_tag("daily") -> ScheduleTag("daily")
        parse_tag({"hourly": [1, 10]}) -> ScheduleTag("hourly", [1, 10])
        parse_tag({"daily": [2, "monday"]}) -> ScheduleTag("daily", [2, "monday"])
        parse_tag({"hourly": [{"monday": [6]}, 9]})
            -> ScheduleTag("hourly", [{"monday": [6]}, 9])
    """
    if isinstance(tag_data, str):
        return ScheduleTag(name=tag_data)
    elif isinstance(tag_data, dict):
        if len(tag_data) != 1:
            raise ValueError(f"Tag dict must have exactly one key, got: {tag_data}")
        name, values = next(iter(tag_data.items()))
        if not isinstance(values, list):
            values = [values]
        normalized: List[Union[int, str, Dict[str, List[int]]]] = [
            _normalize_value_entry(v, name) for v in values
        ]
        return ScheduleTag(name=name, values=normalized)
    else:
        raise ValueError(f"Invalid tag format: {tag_data}")


def _normalize_value_entry(
    entry: Any, tag_name: str
) -> Union[int, str, Dict[str, List[int]]]:
    """Normalize a single entry in a tag's values list.

    Bare strings are normalized as weekday names. Bare ints pass through.
    Dict entries must be `{weekday: int}` or `{weekday: [int, ...]}` and
    are normalized to `{canonical_weekday: [int, ...]}`.
    """
    if isinstance(entry, bool):
        # bool is a subclass of int in Python — reject explicitly
        raise ValueError(f"Invalid value in tag '{tag_name}': {entry!r}")
    if isinstance(entry, int):
        return entry
    if isinstance(entry, str):
        return _normalize_weekday_str(entry, tag_name)
    if isinstance(entry, dict):
        return _normalize_per_weekday_dict(entry, tag_name)
    raise ValueError(f"Invalid value in tag '{tag_name}': {entry!r}")


def _normalize_weekday_str(value: str, tag_name: str) -> str:
    weekday = normalize_weekday(value)
    if weekday is None:
        raise ValueError(
            f"Invalid weekday name '{value}' in tag '{tag_name}'. "
            f"Valid weekdays: {', '.join(sorted(WEEKDAY_NAMES))}"
        )
    return weekday


def _normalize_per_weekday_dict(
    entry: Dict[Any, Any], tag_name: str
) -> Dict[str, List[int]]:
    if len(entry) != 1:
        raise ValueError(
            f"Per-weekday entry in tag '{tag_name}' must have exactly one "
            f"weekday key, got: {entry}"
        )
    weekday_raw, sub_values = next(iter(entry.items()))
    if not isinstance(weekday_raw, str):
        raise ValueError(
            f"Per-weekday entry key in tag '{tag_name}' must be a weekday "
            f"name, got: {weekday_raw!r}"
        )
    weekday = _normalize_weekday_str(weekday_raw, tag_name)
    if not isinstance(sub_values, list):
        sub_values = [sub_values]
    sub_ints: List[int] = []
    for sv in sub_values:
        if isinstance(sv, bool) or not isinstance(sv, int):
            raise ValueError(
                f"Per-weekday value in tag '{tag_name}' under '{weekday}' "
                f"must be an integer, got: {sv!r}"
            )
        sub_ints.append(sv)
    return {weekday: sub_ints}


def parse_tags(
    tags_data: List[Union[str, Dict[str, Any]]],
) -> List[ScheduleTag]:
    """Parse a list of tags from YAML format.

    Args:
        tags_data: List of tag data (strings or dicts)

    Returns:
        List of ScheduleTag instances
    """
    return [parse_tag(t) for t in tags_data]


@dataclass
class PipelineConfig:
    """Unified pipeline configuration model.

    Represents a single pipeline configuration regardless of source
    (file, SharePoint, database, etc.).
    """

    # Core identity
    pipeline_group: str  # 'google_sheets', 'filesystem', etc.
    pipeline_name: (
        str  # Stable identifier across environments, computed by ConfigSource
    )
    table_name: str  # Environment-aware table name (includes pipeline type prefix in dev, excludes in prod)
    identifier: str  # Unique identifier (file path, SharePoint ID, database key, etc.)

    # Configuration
    config_dict: Dict[str, Any]  # Full configuration dictionary
    enabled: bool  # Whether pipeline is enabled
    tags: List[ScheduleTag]  # Tags for filtering (supports schedule values)

    # Implementation
    adapter: Optional[str] = None  # e.g., 'dlt_saga.api.genesyscloud'

    # Naming (resolved by ConfigSource during discovery)
    schema_name: str = ""  # Environment-aware schema/dataset name

    # Metadata
    source_type: str = (
        "file"  # Type of config source ('file', 'sharepoint', 'database', etc.)
    )

    def has_tag(
        self,
        tag_name: str,
        tag_value: Optional[int] = None,
        exact: bool = False,
        query_weekday: Optional[str] = None,
    ) -> bool:
        """Check if this config has a matching tag.

        Args:
            tag_name: Name of the tag to match (e.g., "daily", "hourly")
            tag_value: Optional specific value (e.g., 10 for 10am, 2 for 2nd day)
            exact: If True, only match tags that explicitly have the value.
                   If False, tags with no values (run always) also match.
            query_weekday: Optional weekday name to match (e.g., "monday")

        Returns:
            True if config has a matching tag

        Examples:
            config.has_tag("daily")              # True if has "daily" tag
            config.has_tag("hourly", 10)         # True if runs at 10am (including "hourly" with no values)
            config.has_tag("hourly", 10, exact=True)  # True only if explicitly has 10 in values
            config.has_tag("daily", 15, query_weekday="monday")  # True if runs on 15th or Mondays
        """
        return any(
            tag.matches(tag_name, tag_value, exact, query_weekday) for tag in self.tags
        )

    def get_tag_names(self) -> List[str]:
        """Get list of tag names (without values) for display purposes."""
        return [tag.name for tag in self.tags]

    @property
    def raw_write_disposition(self) -> str:
        """The write_disposition as specified in config (may include +historize suffix)."""
        return self.config_dict.get("write_disposition", "append")

    @property
    def dlt_write_disposition(self) -> str:
        """Write disposition to pass to dlt (strips +historize suffix).

        Examples:
            "append+historize" → "append"
            "merge+historize" → "merge"
            "historize" → "historize" (no ingest, historize-only)
            "append" → "append"
        """
        wd = self.raw_write_disposition
        if "+" in wd:
            return wd.split("+")[0]
        return wd

    @property
    def ingest_enabled(self) -> bool:
        """Whether ingest command should run this pipeline.

        True when write_disposition has an ingest component (append, merge, replace).
        False when write_disposition is "historize" only (external data delivery).
        """
        base = self.dlt_write_disposition
        return base in ("append", "merge", "replace")

    @property
    def historize_enabled(self) -> bool:
        """Whether historize command should run this pipeline.

        True when write_disposition contains "historize" (e.g., "append+historize", "historize").
        """
        return "historize" in self.raw_write_disposition


class ConfigSource(ABC):
    """Abstract base class for pipeline configuration sources.

    Implementations provide different ways to discover and manage pipeline configs:
    - FilePipelineConfig: YAML files in configs/ directory
    - SharePointPipelineConfig: SharePoint list items
    - DatabasePipelineConfig: Database table rows
    - etc.
    """

    @abstractmethod
    def discover(
        self,
    ) -> Tuple[Dict[str, List[PipelineConfig]], Dict[str, List[PipelineConfig]]]:
        """Discover all pipeline configurations.

        Returns:
            Tuple of (enabled_configs, disabled_configs), both organized by pipeline_group:
            {
                'google_sheets': [PipelineConfig(...), ...],
                'filesystem': [PipelineConfig(...), ...],
            }
        """
        pass

    @abstractmethod
    def get_config(self, identifier: str) -> Optional[PipelineConfig]:
        """Get a specific configuration by its identifier.

        Args:
            identifier: Unique identifier for the config (e.g., file path, ID)

        Returns:
            PipelineConfig if found, None otherwise
        """
        pass

    def filter_by_group(
        self, configs: Dict[str, List[PipelineConfig]], pipeline_group: str
    ) -> Dict[str, List[PipelineConfig]]:
        """Filter configs to only include specific pipeline group.

        Args:
            configs: Configs organized by pipeline group
            pipeline_group: Group to filter for (e.g., 'google_sheets')

        Returns:
            Filtered configs dictionary with only the specified group
        """
        if pipeline_group not in configs:
            return {}
        return {pipeline_group: configs[pipeline_group]}

    def filter_by_tags(
        self, configs: Dict[str, List[PipelineConfig]], tags: List[str]
    ) -> Dict[str, List[PipelineConfig]]:
        """Filter configs to only include those with ALL specified tags.

        Args:
            configs: Configs organized by pipeline group
            tags: List of tag names - configs must have ALL of them

        Returns:
            Filtered configs dictionary
        """
        if not tags:
            return configs

        filtered = {}
        for group, pipeline_configs in configs.items():
            matching_configs = [
                config
                for config in pipeline_configs
                if all(config.has_tag(tag) for tag in tags)
            ]
            if matching_configs:
                filtered[group] = matching_configs
        return filtered

    def warn_disabled_matches(
        self,
        disabled_configs: Dict[str, List[PipelineConfig]],
        pipeline_group: Optional[str] = None,
        tags: Optional[List[str]] = None,
    ):
        """Warn about disabled configs that match filter criteria.

        Args:
            disabled_configs: Disabled configs organized by pipeline group
            pipeline_group: Optional group filter
            tags: Optional tag filters
        """
        # Apply the same filters to disabled configs
        matching_disabled = disabled_configs

        if pipeline_group:
            matching_disabled = self.filter_by_group(matching_disabled, pipeline_group)

        if tags:
            matching_disabled = self.filter_by_tags(matching_disabled, tags)

        # Warn about each matching disabled config
        for group, pipeline_configs in matching_disabled.items():
            for config in pipeline_configs:
                logger.warning(f"The source {config.table_name} is disabled")

    def get_configs(
        self,
        pipeline_group: Optional[str] = None,
        tags: Optional[List[str]] = None,
    ) -> Dict[str, List[PipelineConfig]]:
        """Get filtered pipeline configurations with automatic discovery and filtering.

        This is the primary method to use from CLI - it handles all the complexity of:
        1. Discovering all configs
        2. Filtering by group and tags
        3. Warning about disabled matches
        4. Logging appropriate errors

        Args:
            pipeline_group: Optional pipeline group to filter for (e.g., 'google_sheets')
            tags: Optional list of tags - configs must have ALL of them

        Returns:
            Dictionary of enabled PipelineConfigs organized by pipeline_group.
            Returns empty dict if no configs found (with appropriate error logging).
        """
        # Discover all configs
        enabled_configs, disabled_configs = self.discover()

        if not enabled_configs:
            logger.error("No valid configurations found")
            return {}

        # Apply filters
        if pipeline_group:
            enabled_configs = self.filter_by_group(enabled_configs, pipeline_group)
            if not enabled_configs:
                logger.error(f"No configurations found for group: {pipeline_group}")
                return {}

        if tags:
            enabled_configs = self.filter_by_tags(enabled_configs, tags)
            if not enabled_configs:
                logger.error(
                    f"No configurations found matching all tags: {', '.join(tags)}"
                )
                return {}

        # Warn about disabled configs that match the filter criteria
        self.warn_disabled_matches(disabled_configs, pipeline_group, tags)

        return enabled_configs

    @staticmethod
    def prepare_for_execution(pipeline_config: PipelineConfig) -> Dict[str, Any]:
        """Prepare a PipelineConfig for execution by adding environment-aware fields.

        This method enriches the config_dict with runtime fields that depend on the environment:
        - schema_name: Already resolved by ConfigSource, carried forward
        - table_name: Environment-aware table name (with/without pipeline group prefix)
        - pipeline_name: Pipeline name (always includes pipeline group prefix)
        - initial_value: Overridden for incremental models in dev (if profile has override)

        Args:
            pipeline_config: PipelineConfig to prepare

        Returns:
            Dictionary ready to pass to pipeline class constructor
        """
        config_dict = pipeline_config.config_dict.copy()

        # schema_name is already resolved by ConfigSource during discovery
        config_dict["schema_name"] = pipeline_config.schema_name

        # Add environment-aware table name (includes pipeline group prefix in dev, excludes in prod)
        config_dict["table_name"] = pipeline_config.table_name

        # Pipeline name always includes pipeline group prefix (consistent across environments)
        config_dict["pipeline_name"] = pipeline_config.pipeline_name

        # Apply overrides from execution context
        from dlt_saga.utility.cli.context import get_execution_context

        context = get_execution_context()

        # start/end_value_override from CLI for backfilling
        if context.start_value_override:
            config_dict["start_value_override"] = context.start_value_override
        if context.end_value_override:
            config_dict["end_value_override"] = context.end_value_override

        return config_dict
