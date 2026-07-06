"""historize must not expose --force.

Unlike ingest, historize has no proxy-based change detection to override: it keys
on actual snapshot values in the data, so "force even if nothing changed" has no
meaning. Rebuild needs are covered by --full-refresh / --partial-refresh.
"""

import pytest
from typer.testing import CliRunner

from dlt_saga.cli import app

runner = CliRunner()


@pytest.mark.unit
class TestHistorizeNoForce:
    def test_historize_rejects_force(self):
        result = runner.invoke(app, ["historize", "--force"])
        assert result.exit_code != 0
        assert "--force" in result.output  # "No such option: --force"

    def test_historize_help_omits_force(self):
        result = runner.invoke(app, ["historize", "--help"])
        assert result.exit_code == 0
        assert "--force" not in result.output

    def test_ingest_still_has_force(self):
        result = runner.invoke(app, ["ingest", "--help"])
        assert result.exit_code == 0
        assert "--force" in result.output
