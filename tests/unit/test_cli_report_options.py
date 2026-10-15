"""`saga report` status-summary options.

Introspects resolved click options rather than asserting on rendered help text.
"""

import pytest
from typer.main import get_command

from dlt_saga.cli import app


def _params(command_name: str) -> dict:
    command = get_command(app).commands[command_name]
    return {opt: param for param in command.params for opt in param.opts}


@pytest.mark.unit
class TestReportSummaryOptions:
    def test_summary_is_opt_in(self):
        params = _params("report")
        assert "--summary" in params
        assert params["--summary"].default is False

    def test_stale_after_minutes_has_no_cli_default(self):
        # None defers to DEFAULT_STALE_AFTER_MINUTES in Session.report, so the
        # default lives in one place.
        params = _params("report")
        assert params["--stale-after-minutes"].default is None
