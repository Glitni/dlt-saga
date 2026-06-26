"""Date-window incremental pipeline for REST API sources.

Most incremental API ingests share one shape:

1. Resume from the warehouse high-water mark (``MAX(incremental_column)``).
2. Re-fetch a small ``overlap`` of already-loaded days to catch late-arriving or
   corrected rows.
3. Load the resulting ``[start, end]`` date window — never a hardcoded
   "yesterday" that can't recover from a missed or failed run.

``DateWindowApiPipeline`` combines the transport-agnostic
:class:`DateWindowResolver` (which does the window bookkeeping) with
``BaseApiPipeline`` (which does HTTP), so an adapter gets idempotent,
self-healing incremental loading with little or no code:

- **Config only** — point ``adapter`` at ``dlt_saga.api.date_window`` and set
  ``incremental_column`` + ``start_param``/``end_param``. The resolved window is
  placed into the request automatically (paginated if ``pagination`` is set).
- **One request per day** — set ``per_period_requests: true`` for APIs that only
  accept a single date.
- **Custom request shape** — subclass and override :meth:`_fetch_window` (e.g. an
  API that takes the window in a POST body). The window resolution, watermark
  lookup and day iteration are still reused.

A custom-client (non-HTTP) pipeline can reuse the same window logic without this
class by mixing :class:`DateWindowResolver` into a plain ``BasePipeline``.
"""

import time
from dataclasses import fields
from datetime import date, timedelta
from typing import Any, List, Tuple

import dlt

from dlt_saga.pipelines.api.base import BaseApiPipeline
from dlt_saga.pipelines.api.date_window.config import DateWindowApiConfig
from dlt_saga.pipelines.date_window import DateWindowResolver


class DateWindowApiPipeline(DateWindowResolver, BaseApiPipeline):
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
