"""CLI confirmation-flag handling.

`--yes` must skip the full-refresh confirmation prompts (otherwise CI/cron hangs
on stdin), and `--partial-refresh` / `--historize-from` must be rejected under
`--orchestrate` (the refresh scope isn't propagated to workers, so the banner
would otherwise imply a partial refresh that never happens).

Introspects resolved click options / calls the confirm helpers directly rather
than asserting on rendered help text.
"""

import logging
from unittest.mock import patch

import pytest
import typer
from typer.main import get_command

from dlt_saga.cli import _validate_historize_flags, app
from dlt_saga.utility.cli.prompts import (
    _confirm_destroy,
    _confirm_destructive,
    _confirm_full_refresh,
    _confirm_historize_full_refresh,
    _confirm_partial_refresh,
    _confirm_run_full_refresh,
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
        with patch("dlt_saga.utility.cli.prompts.typer.confirm") as confirm:
            _confirm_full_refresh(full_refresh=True, in_cloud_run=False, yes=True)
            confirm.assert_not_called()

    def test_confirm_full_refresh_prompts_without_yes(self):
        with patch(
            "dlt_saga.utility.cli.prompts.typer.confirm", return_value=True
        ) as confirm:
            _confirm_full_refresh(full_refresh=True, in_cloud_run=False, yes=False)
            confirm.assert_called_once()

    def test_confirm_historize_full_refresh_skips_prompt_with_yes(self):
        with patch("dlt_saga.utility.cli.prompts.typer.confirm") as confirm:
            _confirm_historize_full_refresh(
                full_refresh=True, in_cloud_run=False, yes=True
            )
            confirm.assert_not_called()

    def test_confirm_run_full_refresh_refreshes_both_with_yes(self):
        with patch("dlt_saga.utility.cli.prompts.typer.confirm") as confirm:
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
class TestConfirmDestructiveHelper:
    """The shared banner+prompt helper the confirm functions delegate to."""

    def test_skips_prompt_with_yes(self):
        with patch("dlt_saga.utility.cli.prompts.typer.confirm") as confirm:
            _confirm_destructive("T", "body", in_cloud_run=False, yes=True)
            confirm.assert_not_called()

    def test_skips_prompt_in_cloud_run(self):
        with patch("dlt_saga.utility.cli.prompts.typer.confirm") as confirm:
            _confirm_destructive("T", "body", in_cloud_run=True, yes=False)
            confirm.assert_not_called()

    def test_exits_on_decline(self):
        with patch("dlt_saga.utility.cli.prompts.typer.confirm", return_value=False):
            with pytest.raises(typer.Exit):
                _confirm_destructive("T", "body", in_cloud_run=False, yes=False)

    def test_proceeds_on_confirm(self):
        with patch(
            "dlt_saga.utility.cli.prompts.typer.confirm", return_value=True
        ) as confirm:
            _confirm_destructive("T", "body", in_cloud_run=False, yes=False)
            confirm.assert_called_once()


@pytest.mark.unit
class TestDelegatingConfirms:
    """The single-prompt confirms honor --yes and still warn."""

    def test_destroy_skips_prompt_with_yes(self):
        with patch("dlt_saga.utility.cli.prompts.typer.confirm") as confirm:
            _confirm_destroy(
                dry_run=False, resource_type="all", in_cloud_run=False, yes=True
            )
            confirm.assert_not_called()

    def test_destroy_dry_run_never_prompts(self):
        with patch("dlt_saga.utility.cli.prompts.typer.confirm") as confirm:
            _confirm_destroy(
                dry_run=True, resource_type="all", in_cloud_run=False, yes=False
            )
            confirm.assert_not_called()

    def test_partial_refresh_prompts_without_yes(self):
        with patch(
            "dlt_saga.utility.cli.prompts.typer.confirm", return_value=True
        ) as confirm:
            _confirm_partial_refresh(
                partial_refresh=True,
                historize_from=None,
                in_cloud_run=False,
                yes=False,
            )
            confirm.assert_called_once()

    def test_partial_refresh_with_yes_skips_prompt_but_still_warns(self, caplog):
        # Behaviour change from the refactor: the banner is now shown under
        # --yes (previously it returned before warning), consistent with the
        # other destructive confirms. Assert a WARNING was emitted, not its text.
        with patch("dlt_saga.utility.cli.prompts.typer.confirm") as confirm:
            with caplog.at_level(
                logging.WARNING, logger="dlt_saga.utility.cli.prompts"
            ):
                _confirm_partial_refresh(
                    partial_refresh=True,
                    historize_from=None,
                    in_cloud_run=False,
                    yes=True,
                )
            confirm.assert_not_called()
        assert any(r.levelno == logging.WARNING for r in caplog.records)


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
