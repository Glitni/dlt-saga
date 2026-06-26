"""Configuration for date-window incremental API pipelines."""

from dataclasses import dataclass, field
from typing import Optional

from dlt_saga.pipelines.api.config import ApiConfig

_WINDOW_END_VALUES = ("today", "yesterday")
_ON_FIRST_RUN_VALUES = ("error", "today", "yesterday")


@dataclass
class DateWindowApiConfig(ApiConfig):
    """Configuration for a REST API ingested as an incremental date window.

    Adds the fields needed to resume from the warehouse high-water mark, re-fetch
    a small overlap to catch late-arriving or corrected rows, and load a
    ``[start, end]`` date window — the idempotent, self-healing pattern that
    avoids a hardcoded "yesterday" which can't recover from a missed run.

    Inherited from :class:`ApiConfig` / ``BaseConfig`` and reused as-is:
    ``incremental_column`` (the warehouse column whose max seeds the cursor),
    ``initial_value`` (first-run seed), ``start_value_override`` /
    ``end_value_override`` (CLI/config backfill). ``incremental`` defaults to
    ``True`` here since that is the whole point of this adapter.

    For the common case the pipeline needs no Python at all: point ``adapter`` at
    ``dlt_saga.api.date_window`` and set ``start_param`` / ``end_param`` so the
    resolved window is placed into the request.
    """

    # Date-window pipelines are incremental by definition.
    incremental: bool = field(
        default=True,
        metadata={
            "description": "Enable incremental date-window loading. Defaults to True for this adapter."
        },
    )

    overlap: int = field(
        default=1,
        metadata={
            "description": (
                "Number of already-loaded days to re-fetch on each run, inclusive of "
                "the last loaded day (overlap=1 reloads just the last day). Catches "
                "late-arriving or corrected data. Set to 0 to resume strictly after "
                "the watermark with no re-fetch."
            )
        },
    )

    timezone: str = field(
        default="UTC",
        metadata={
            "description": "IANA timezone used to compute 'today'/'yesterday' (e.g. 'Europe/Oslo'). Must match how the API and the warehouse interpret dates."
        },
    )

    window_end: str = field(
        default="today",
        metadata={
            "description": "Upper bound of the window: 'today' (include the current, possibly partial, day) or 'yesterday' (only complete days).",
            "enum": list(_WINDOW_END_VALUES),
        },
    )

    on_first_run: str = field(
        default="error",
        metadata={
            "description": (
                "Behaviour on the first run when no watermark exists and no "
                "initial_value is set: 'error' (fail loud — recommended, forces an "
                "explicit start), 'today', or 'yesterday'."
            ),
            "enum": list(_ON_FIRST_RUN_VALUES),
        },
    )

    # --- default request wiring (no Python needed for the common case) ---------
    start_param: Optional[str] = field(
        default=None,
        metadata={
            "description": "Query-parameter name that receives the window start (e.g. 'from', 'startDate'). Required unless a subclass overrides _fetch_window()."
        },
    )

    end_param: Optional[str] = field(
        default=None,
        metadata={
            "description": "Query-parameter name that receives the window end (e.g. 'to', 'endDate'). Optional if the API only needs a start."
        },
    )

    date_format: str = field(
        default="%Y-%m-%d",
        metadata={
            "description": "strftime format used to render window dates into the request (e.g. '%Y-%m-%d', '%Y-%m-%dT%H:%M:%SZ')."
        },
    )

    end_param_inclusive: bool = field(
        default=True,
        metadata={
            "description": "Whether end_param is inclusive of the last day (True) or an exclusive upper bound (False, sends end + 1 day)."
        },
    )

    per_period_requests: bool = field(
        default=False,
        metadata={
            "description": (
                "Issue one request per day across the window (for APIs that only "
                "accept a single date). When False, the whole window is sent in one "
                "request (paginated if 'pagination' is configured)."
            )
        },
    )

    def __post_init__(self):
        """Validate configuration after initialization."""
        super().__post_init__()

        if self.overlap < 0:
            raise ValueError(f"overlap must be >= 0, got {self.overlap}")
        if self.window_end not in _WINDOW_END_VALUES:
            raise ValueError(
                f"window_end must be one of {_WINDOW_END_VALUES}, got '{self.window_end}'"
            )
        if self.on_first_run not in _ON_FIRST_RUN_VALUES:
            raise ValueError(
                f"on_first_run must be one of {_ON_FIRST_RUN_VALUES}, got '{self.on_first_run}'"
            )
