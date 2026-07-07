"""Transport-agnostic date-window incremental loading.

The window bookkeeping that makes a date-range ingest idempotent — resume from
the warehouse high-water mark, re-fetch a small overlap, seed the first run,
honour backfill overrides — has nothing to do with *how* rows are fetched. This
module provides both halves of that, transport-free:

- :class:`DateWindowConfig` — the config fields the resolver reads.
- :class:`DateWindowResolver` — a mixin that resolves the ``[start, end]`` window.

Both are reused by REST API pipelines (``dlt_saga.pipelines.api.date_window``,
over ``BaseApiPipeline``) and can be mixed into custom-client pipelines (a plain
``BasePipeline`` with its own SDK/HTTP/DB client).
"""

import logging
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta
from typing import Any, Iterator, Optional, Tuple
from zoneinfo import ZoneInfo

from dlt_saga.pipelines.base_config import BaseConfig
from dlt_saga.pipelines.watermark import read_destination_watermark

logger = logging.getLogger(__name__)

_WINDOW_END_VALUES = ("today", "yesterday")
_ON_FIRST_RUN_VALUES = ("error", "today", "yesterday")


@dataclass
class DateWindowConfig(BaseConfig):
    """Transport-agnostic configuration for date-window incremental loading.

    Carries the fields :class:`DateWindowResolver` needs — independent of how rows
    are fetched. Combine it with a transport config to build a concrete config
    (e.g. ``DateWindowApiConfig(DateWindowConfig, ApiConfig)``), or inherit it
    directly for a custom-client pipeline. ``incremental_column`` / ``initial_value``
    / ``start_value_override`` / ``end_value_override`` are inherited from
    ``BaseConfig``; ``incremental`` defaults to ``True`` since that is the point.

    Note on write disposition: ``overlap`` deliberately re-fetches already-loaded
    days to catch late/corrected rows, so each run re-emits those rows. Pair the
    date-window adapter with ``merge`` (delete-insert on the date column),
    ``scd2``, or ``+historize`` so the re-fetched rows reconcile instead of
    accumulating. With plain ``append`` the overlap days duplicate on every run.
    """

    incremental: bool = field(
        default=True,
        metadata={
            "description": "Enable incremental date-window loading. Defaults to True."
        },
    )

    overlap: int = field(
        default=1,
        metadata={
            "description": (
                "Number of already-loaded days to re-fetch on each run, to catch "
                "late-arriving or corrected data: 0 = none (resume the day after the "
                "watermark); 1 = re-fetch the watermark day itself (inclusive, the "
                "default); N = re-fetch the watermark day plus the N-1 days before it."
            )
        },
    )

    timezone: str = field(
        default="UTC",
        metadata={
            "description": "IANA timezone used to compute 'today'/'yesterday' (e.g. 'Europe/Oslo'). Must match how the source and the warehouse interpret dates."
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
                "initial_value is set: 'error' (fail loud — recommended), 'today', "
                "or 'yesterday'."
            ),
            "enum": list(_ON_FIRST_RUN_VALUES),
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

        # overlap=0 resumes the day *after* the watermark. With window_end=today
        # the watermark day was only partially loaded (up to the previous run),
        # so rows arriving later that same day are never picked up — silent data
        # loss. overlap=0 is only safe with window_end=yesterday (complete days).
        if self.incremental and self.overlap == 0 and self.window_end == "today":
            logger.warning(
                "overlap=0 with window_end='today' can silently drop rows: the "
                "watermark day is only partially loaded, and resuming the day "
                "after it skips rows that arrive later the same day. Use overlap "
                ">= 1 to re-fetch the watermark day, or window_end='yesterday' to "
                "only load complete days."
            )


class DateWindowResolver:
    """Mixin: resolve an idempotent ``[start, end]`` date window from the warehouse.

    Mix into any ``BasePipeline`` subclass and call :meth:`resolve_window` /
    :meth:`iter_days` from ``extract_data``/``fetch_data``. The host must provide:

    - a :attr:`window_config` property returning an object that carries the
      date-window fields (``overlap``, ``timezone``, ``window_end``,
      ``on_first_run``, ``incremental``, ``incremental_column``, ``initial_value``,
      ``start_value_override``, ``end_value_override``) — e.g.
      :class:`DateWindowApiConfig`;
    - the standard ``BasePipeline`` attributes ``destination``,
      ``destination_database``, ``pipeline``, ``table_name`` and ``logger``.

    Granularity is days, covering the overwhelming majority of date-windowed
    sources. For a different period (e.g. ISO weeks) override
    :meth:`resolve_window` / :meth:`iter_days`.
    """

    # Provided by the host BasePipeline (declared for type-checkers; the mixin
    # never sets them).
    destination: Any
    destination_database: str
    pipeline: Any
    table_name: str
    logger: Any

    @property
    def window_config(self) -> Any:
        raise NotImplementedError(
            "DateWindowResolver hosts must provide a `window_config` property "
            "returning the pipeline's date-window config."
        )

    def _get_watermark(self) -> Optional[Any]:
        """Return ``MAX(incremental_column)`` from the destination, or ``None``.

        ``None`` means the table doesn't exist yet or is empty — i.e. the first
        run. The destination is the source of truth for the high-water mark, so a
        missed or failed run is caught up on the next run with no gaps.

        The destination read distinguishes a missing table (returns ``None``,
        first run) from an infrastructure error (raises). We do NOT swallow the
        latter: treating a permission/network error as "first run" would silently
        re-scan the entire date range on every failed run.
        """
        return read_destination_watermark(
            self.destination,
            self.destination_database,
            self.pipeline.dataset_name,
            self.table_name,
            self.window_config.incremental_column,
        )

    def _watermark_date(self) -> Optional[date]:
        """The watermark coerced to a ``date`` (``None`` on the first run)."""
        raw = self._get_watermark()
        if raw is None:
            return None
        return self._to_date(raw)

    def resolve_window(self) -> Tuple[date, date]:
        """Resolve the ``[start, end]`` date window to load this run.

        Precedence (highest first):

        1. ``start_value_override`` / ``end_value_override`` — explicit backfill.
        2. ``incremental = false`` — full load of ``[initial_value, end]`` every run.
        3. Warehouse watermark minus ``overlap`` — the normal incremental path.
        4. ``initial_value`` — the first-run seed.
        5. ``on_first_run`` policy (``error`` / ``today`` / ``yesterday``).

        ``end`` is today (or yesterday, per ``window_end``) in the configured
        timezone, unless an explicit ``end_value_override`` is given.
        """
        cfg = self.window_config
        today = datetime.now(ZoneInfo(cfg.timezone)).date()
        end = today if cfg.window_end == "today" else today - timedelta(days=1)

        # 1. Explicit backfill override (wins even when incremental is off).
        if cfg.start_value_override:
            start = self._to_date(cfg.start_value_override)
            if cfg.end_value_override:
                end = self._to_date(cfg.end_value_override)
            self.logger.info(f"Backfill window: {start} -> {end}")
            return start, end

        # 2. Non-incremental: full load of the whole range, no state. Like a
        #    non-incremental load anywhere else in the framework, but a date-window
        #    source needs a lower bound, so initial_value is required.
        if not cfg.incremental:
            if not cfg.initial_value:
                raise ValueError(
                    f"incremental is false for '{self.table_name}', which reloads the "
                    f"full range every run — set initial_value to the start of that "
                    f"range, or set incremental: true to resume from the watermark."
                )
            start = self._to_date(cfg.initial_value)
            self.logger.info(f"Full (non-incremental) load: {start} -> {end}")
            return start, end

        # 3. Resume from the warehouse watermark, re-fetching `overlap` days.
        watermark = self._watermark_date()
        if watermark is not None:
            # overlap = number of already-loaded days to re-fetch:
            #   0 -> none, resume the day after the watermark
            #   1 -> re-fetch the watermark day (inclusive)  [default]
            #   N -> re-fetch the watermark day + N-1 days before it
            start = watermark - timedelta(days=cfg.overlap - 1)
            self.logger.info(
                f"Incremental window: {start} -> {end} "
                f"(watermark {watermark}, overlap {cfg.overlap})"
            )
            return start, end

        # 4. First run — seed from initial_value.
        if cfg.initial_value:
            start = self._to_date(cfg.initial_value)
            self.logger.info(f"First run from initial_value: {start} -> {end}")
            return start, end

        # 5. First run with no seed — apply the configured policy.
        if cfg.on_first_run == "today":
            self.logger.info(f"First run (no initial_value): loading {today}")
            return today, end
        if cfg.on_first_run == "yesterday":
            yesterday = today - timedelta(days=1)
            self.logger.info(f"First run (no initial_value): loading {yesterday}")
            return yesterday, end
        raise ValueError(
            f"No watermark in the destination and no initial_value configured for "
            f"'{self.table_name}'. Set initial_value (e.g. '2024-01-01') to seed the "
            f"first run, or set on_first_run to 'today'/'yesterday'."
        )

    def iter_days(self, start: date, end: date) -> Iterator[date]:
        """Yield each day from ``start`` to ``end`` inclusive."""
        current = start
        while current <= end:
            yield current
            current += timedelta(days=1)

    @staticmethod
    def _to_date(value: Any) -> date:
        """Coerce a date/datetime/ISO-string watermark or config value to a ``date``."""
        if isinstance(value, datetime):
            return value.date()
        if isinstance(value, date):
            return value
        text = str(value).strip()
        try:
            return datetime.fromisoformat(text).date()
        except ValueError:
            return date.fromisoformat(text[:10])
