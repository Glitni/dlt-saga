"""Import the dataframe libraries dlt sniffs for before pipeline threads start.

dlt recognises a pandas, polars or pyarrow item (``dlt.common.libs``) by
looking the library up in ``sys.modules`` and reading a class off it, instead
of importing it. That lookup bypasses the import lock: Python registers a
module in ``sys.modules`` before its body runs, so while one thread is midway
through ``import pandas`` another thread's extract sees the half-built module
and fails with ``partially initialized module 'pandas' has no attribute
'DataFrame'``. Saga and adapters import these libraries where they're used
(the filesystem CSV reader, the database client's return types), which is
inside a worker thread, so a cold interpreter running two pipelines in
parallel can hit it on the very first run. Importing them once on the
launching thread before the pool starts makes every later lazy import a
no-op.
"""

import importlib
import logging
import time

logger = logging.getLogger(__name__)

DATAFRAME_LIBS = ("pyarrow", "pandas", "polars")


def preload_dataframe_libs() -> None:
    """Import every library in :data:`DATAFRAME_LIBS` on the calling thread.

    Call before starting a thread pool that runs ingest pipelines. Idempotent:
    an already-imported library costs a dictionary lookup. A library that is
    not installed is skipped, since pipelines that never produce its items
    don't need it.
    """
    started = time.perf_counter()
    for name in DATAFRAME_LIBS:
        try:
            importlib.import_module(name)
        except ImportError as exc:
            logger.debug("Dataframe library %s not importable, skipped: %s", name, exc)
    logger.debug(
        "Dataframe libraries preloaded in %.1fs", time.perf_counter() - started
    )
