"""historize must not expose --force.

Unlike ingest, historize has no proxy-based change detection to override: it keys
on actual snapshot values in the data, so "force even if nothing changed" has no
meaning. Rebuild needs are covered by --full-refresh / --partial-refresh.

Introspects the resolved click command options rather than parsing rendered help
text (which Rich styles with ANSI codes and wraps, splitting flags apart).
"""

import pytest
from typer.main import get_command

from dlt_saga.cli import app


def _option_flags(command_name: str) -> set:
    command = get_command(app).commands[command_name]
    flags: set = set()
    for param in command.params:
        flags.update(getattr(param, "opts", []))
    return flags


@pytest.mark.unit
class TestHistorizeNoForce:
    def test_historize_has_no_force_option(self):
        assert "--force" not in _option_flags("historize")
        assert "-f" not in _option_flags("historize")

    def test_ingest_still_has_force_option(self):
        assert "--force" in _option_flags("ingest")
