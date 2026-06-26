"""Date-window incremental pipeline for REST API sources.

Most incremental API ingests share one shape:

1. Resume from the warehouse high-water mark (``MAX(incremental_column)``).
2. Re-fetch a small ``overlap`` of already-loaded days to catch late-arriving or
   corrected rows.
3. Load the resulting ``[start, end]`` date window — never a hardcoded
   "yesterday" that can't recover from a missed or failed run.

``DateWindowApiPipeline`` implements exactly that, so an adapter gets idempotent,
self-healing incremental loading with little or no code:

- **Config only** — point ``adapter`` at ``dlt_saga.api.date_window`` and set
  ``incremental_column`` + ``start_param``/``end_param``. The resolved window is
  placed into the request automatically (paginated if ``pagination`` is set).
- **One request per day** — set ``per_period_requests: true`` for APIs that only
  accept a single date.
- **Custom request shape** — subclass and override :meth:`_fetch_window` (e.g. an
  API that takes the window in a POST body rather than query params). The window
  resolution, watermark lookup and day iteration are still reused.

Granularity is days, which covers the overwhelming majority of date-windowed
APIs. For a different period (e.g. ISO weeks) subclass and override
:meth:`resolve_window` / :meth:`iter_days` / :meth:`_render_date`.
"""

import time
from dataclasses import fields
from datetime import date, datetime, timedelta
from typing import Any, Iterator, List, Optional, Tuple
from zoneinfo import ZoneInfo

import dlt

from dlt_saga.pipelines.api.base import BaseApiPipeline
from dlt_saga.pipelines.api.date_window.config import DateWindowApiConfig


class DateWindowApiPipeline(BaseApiPipeline):
    """Incremental REST API pipeline driven by a resolved ``[start, end]`` date window.

    See the module docstring for the loading model and the three usage tiers.
    """

    def _create_api_config(self, config_dict: dict) -> DateWindowApiConfig:
        config_field_names = {f.name for f in fields(DateWindowApiConfig)}
        return DateWindowApiConfig(
            **{k: v for k, v in config_dict.items() if k in config_field_names}
        )

    @property
    def window_config(self) -> DateWindowApiConfig:
        """The pipeline's config, typed as :class:`DateWindowApiConfig`."""
        return self.api_config  # type: ignore[return-value]

    # ------------------------------------------------------------------
    # Watermark
    # ------------------------------------------------------------------

    def _get_watermark(self) -> Optional[Any]:
        """Return ``MAX(incremental_column)`` from the destination, or ``None``.

        ``None`` means the table doesn't exist yet or is empty — i.e. the first
        run. The destination is the source of truth for the high-water mark, so a
        missed or failed run is caught up on the next run with no gaps.
        """
        column = self.window_config.incremental_column
        if not column:
            return None
        table_id = (
            f"{self.destination_database}.{self.pipeline.dataset_name}."
            f"{self.table_name}"
        )
        try:
            return self.destination.get_max_column_value(table_id, column)
        except Exception as e:  # pragma: no cover - destination already guards
            self.logger.debug(f"No watermark for {table_id} (likely first run): {e}")
            return None

    def _watermark_date(self) -> Optional[date]:
        """The watermark coerced to a ``date`` (``None`` on the first run)."""
        raw = self._get_watermark()
        if raw is None:
            return None
        return self._to_date(raw)

    # ------------------------------------------------------------------
    # Window resolution
    # ------------------------------------------------------------------

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
        #    API needs a lower bound, so initial_value is required.
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
            # overlap = number of already-loaded days to re-fetch: 1 reloads just
            # the watermark day, 2 reloads it plus the day before, 0 resumes the
            # day after (no re-fetch).
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

    # ------------------------------------------------------------------
    # Fetch
    # ------------------------------------------------------------------

    def extract_data(self) -> List[Tuple[Any, str]]:
        """Resolve the window, fetch it, and wrap the records in a dlt resource.

        Overrides the base ``extract_data`` (rather than ``fetch_data``) so the
        window is always resolved first — including when ``pagination`` is
        configured, which the base would otherwise dispatch before any hook runs.
        """
        start, end = self.resolve_window()

        if start > end:
            self.logger.info(
                f"Window start {start} is after end {end} — nothing to load this run."
            )
            data: List[Any] = []
        else:
            data = self._fetch_window(start, end)

        resource = dlt.resource(
            data,
            name=self.table_name,
            max_table_nesting=self.window_config.max_table_nesting,
        )
        description = (
            f"Data from {self.window_config.base_url}{self.window_config.endpoint} "
            f"[{start} -> {end}]"
        )
        return [(resource, description)]

    def _fetch_window(self, start: date, end: date) -> List[Any]:
        """Fetch all records in the ``[start, end]`` window. The main override point.

        Default behaviour places the window into ``query_params`` via
        ``start_param`` / ``end_param`` and fetches it — one request per day when
        ``per_period_requests`` is set, otherwise the whole window in a single
        request (paginated if ``pagination`` is configured).

        Override this for APIs that take the window somewhere other than query
        params (e.g. a JSON report body). ``resolve_window``, ``iter_days`` and
        ``_get_watermark`` remain available to the override.
        """
        cfg = self.window_config
        if not cfg.start_param and not cfg.end_param:
            raise ValueError(
                "DateWindowApiPipeline needs start_param and/or end_param to place "
                "the resolved window into the request, or a subclass that overrides "
                "_fetch_window(). Set them in config, e.g.:\n"
                "  start_param: from\n"
                "  end_param: to"
            )

        if cfg.per_period_requests:
            records: List[Any] = []
            for day in self.iter_days(start, end):
                self._inject_window_params(day, day)
                records.extend(self._fetch_once())
                if cfg.page_delay:
                    time.sleep(cfg.page_delay)
            return records

        self._inject_window_params(start, end)
        return self._fetch_once()

    def _fetch_once(self) -> List[Any]:
        """Fetch the request currently described by ``query_params``.

        Uses the base pagination loop when ``pagination`` is configured, otherwise
        a single request.
        """
        if self.window_config.pagination:
            return list(self._fetch_all_pages())
        return self.fetch_data()

    def _inject_window_params(self, start: date, end: date) -> None:
        """Place the window bounds into ``query_params`` (preserving other params)."""
        cfg = self.window_config
        params = dict(cfg.query_params or {})
        if cfg.start_param:
            params[cfg.start_param] = self._render_date(start)
        if cfg.end_param:
            end_value = end if cfg.end_param_inclusive else end + timedelta(days=1)
            params[cfg.end_param] = self._render_date(end_value)
        cfg.query_params = params

    def _render_date(self, value: date) -> str:
        """Render a window date into the request using ``date_format``."""
        return value.strftime(self.window_config.date_format)

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

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
