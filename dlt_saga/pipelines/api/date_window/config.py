"""Configuration for date-window incremental API pipelines."""

from dataclasses import dataclass, field
from typing import Optional

from dlt_saga.pipelines.api.config import ApiConfig
from dlt_saga.pipelines.date_window import DateWindowConfig


@dataclass
class DateWindowApiConfig(DateWindowConfig, ApiConfig):
    """Configuration for a REST API ingested as an incremental date window.

    Combines the transport-agnostic window fields from :class:`DateWindowConfig`
    (``overlap``, ``timezone``, ``window_end``, ``on_first_run``, plus the
    inherited ``incremental_column`` / ``initial_value`` / overrides) with the
    REST fields from :class:`ApiConfig`, and adds the request wiring that places
    the resolved window into the HTTP request.

    For the common case the pipeline needs no Python at all: point ``adapter`` at
    ``dlt_saga.api.date_window`` and set ``start_param`` / ``end_param`` so the
    resolved window is sent with the request.
    """

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
