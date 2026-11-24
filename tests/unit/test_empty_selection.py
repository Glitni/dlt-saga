"""A selection that matches no pipeline is a successful no-op, not a failure."""

from unittest.mock import MagicMock

import pytest

from dlt_saga.cli import _run_phases
from dlt_saga.utility.cli.run_modes import run_orchestrator_mode


@pytest.mark.unit
def test_run_with_empty_selection_runs_no_phase():
    session = MagicMock()
    session.discover.return_value = []

    _run_phases(
        session,
        ["state:failed"],
        None,
        workers=1,
        force=False,
        full_refresh=False,
        partial_refresh=False,
        historize_from=None,
        start_value_override=None,
        end_value_override=None,
        in_cloud_run=False,
        yes=True,
    )

    session.ingest.assert_not_called()
    session.historize.assert_not_called()


@pytest.mark.unit
def test_orchestrator_with_no_configs_triggers_nothing():
    provider = MagicMock()

    run_orchestrator_mode({}, provider=provider)

    provider.trigger.assert_not_called()
    assert not provider.method_calls
