"""CLI confirmation-flag handling.

`--yes` must skip the full-refresh confirmation prompts (otherwise CI/cron hangs
on stdin), and `--partial-refresh` / `--historize-from` must be rejected under
`--orchestrate` (the refresh scope isn't propagated to workers, so the banner
would otherwise imply a partial refresh that never happens).

Introspects resolved click options / calls the confirm helpers directly rather
than asserting on rendered help text.
"""

from unittest.mock import patch

import pytest
import typer
from typer.main import get_command

from dlt_saga.cli import (
    _confirm_full_refresh,
    _confirm_historize_full_refresh,
    _confirm_run_full_refresh,
    _validate_historize_flags,
    app,
)


def _option_flags(command_name: str) -> set:
    command = get_command(app).commands[command_name]
    flags: set = set()
    for param in command.params:
        flags.update(getattr(param, "opts", []))
    return flags


@pytest.mark.unit
class TestIngestHasYes:
    def test_ingest_exposes_yes(self):
        flags = _option_flags("ingest")
        assert "--yes" in flags
        assert "-y" in flags


@pytest.mark.unit
class TestYesSkipsFullRefreshPrompt:
    def test_confirm_full_refresh_skips_prompt_with_yes(self):
        with patch("dlt_saga.cli.typer.confirm") as confirm:
            _confirm_full_refresh(full_refresh=True, in_cloud_run=False, yes=True)
            confirm.assert_not_called()

    def test_confirm_full_refresh_prompts_without_yes(self):
        with patch("dlt_saga.cli.typer.confirm", return_value=True) as confirm:
            _confirm_full_refresh(full_refresh=True, in_cloud_run=False, yes=False)
            confirm.assert_called_once()

    def test_confirm_historize_full_refresh_skips_prompt_with_yes(self):
        with patch("dlt_saga.cli.typer.confirm") as confirm:
            _confirm_historize_full_refresh(
                full_refresh=True, in_cloud_run=False, yes=True
            )
            confirm.assert_not_called()

    def test_confirm_run_full_refresh_refreshes_both_with_yes(self):
        with patch("dlt_saga.cli.typer.confirm") as confirm:
            result = _confirm_run_full_refresh(
                full_refresh=True,
                in_cloud_run=False,
                has_ingest=True,
                has_historize=True,
                yes=True,
            )
            confirm.assert_not_called()
            assert result == (True, True)


@pytest.mark.unit
class TestPartialRefreshRejectedUnderOrchestrate:
    def test_partial_refresh_with_orchestrate_exits(self):
        with pytest.raises(typer.Exit):
            _validate_historize_flags(
                full_refresh=False,
                partial_refresh=True,
                historize_from=None,
                orchestrate=True,
            )

    def test_historize_from_with_orchestrate_exits(self):
        with pytest.raises(typer.Exit):
            _validate_historize_flags(
                full_refresh=False,
                partial_refresh=False,
                historize_from="2026-01-01",
                orchestrate=True,
            )

    def test_partial_refresh_allowed_without_orchestrate(self):
        # No raise when not orchestrating.
        _validate_historize_flags(
            full_refresh=False,
            partial_refresh=True,
            historize_from=None,
            orchestrate=False,
        )
