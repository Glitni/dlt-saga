"""Built-in outbound notifiers.

Notifiers are ordinary lifecycle hooks (see :mod:`dlt_saga.hooks`) that ship
with the framework, so a project gets alerting from a few lines of
*saga_project.yml* instead of a hand-written hook module. Configuring a channel
under ``notifications:`` registers the matching notifier automatically.

The re-exports resolve on first attribute access (PEP 562) rather than at
package-import time: ``dlt_saga.hooks.loader`` imports the submodules directly,
so an eager re-export here would put this package in a module-lock cycle with
it — the same hazard ``dlt_saga.hooks`` and ``dlt_saga.utility.secrets`` avoid.
"""

from typing import TYPE_CHECKING

from dlt_saga.utility.lazy import lazy_exports

if TYPE_CHECKING:  # pragma: no cover - for type checkers only, never at runtime
    from .slack import SlackNotifier, register_slack_notifier

_EXPORTS = {
    "SlackNotifier": "slack",
    "register_slack_notifier": "slack",
}

__all__ = ["SlackNotifier", "register_slack_notifier"]

__getattr__, __dir__ = lazy_exports(__name__, _EXPORTS, globals())
