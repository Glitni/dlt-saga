"""dlt-saga: a config-driven data ingestion framework built on dlt."""

# Stable plugin API version. Increment when the contract for
# BasePipeline, Destination, or AccessManager changes in a way
# that existing plugins must adapt to.
PLUGIN_API_VERSION = 1

__all__ = [
    "PLUGIN_API_VERSION",
    "AuthenticationError",
    "Session",
    "SessionResult",
    "PipelineResult",
    "HookContext",
    "get_hook_registry",
]


def __getattr__(name: str):
    """Lazy-load public API symbols on first access.

    Avoids eagerly importing the full module graph (DestinationFactory,
    BigQuery SDK, etc.) at ``import dlt_saga`` time. This matters because
    dlt's normalize step forks worker processes that inherit the parent's
    memory — eager imports would bloat every worker.
    """
    if name == "AuthenticationError":
        from dlt_saga.utility.auth.providers import AuthenticationError

        globals()["AuthenticationError"] = AuthenticationError
        return AuthenticationError
    if name in ("Session", "SessionResult", "PipelineResult"):
        from dlt_saga.session import PipelineResult, Session, SessionResult

        globals().update(
            Session=Session,
            SessionResult=SessionResult,
            PipelineResult=PipelineResult,
        )
        return globals()[name]
    if name in ("HookContext", "get_hook_registry"):
        from dlt_saga.hooks.registry import HookContext, get_hook_registry

        globals().update(HookContext=HookContext, get_hook_registry=get_hook_registry)
        return globals()[name]
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
