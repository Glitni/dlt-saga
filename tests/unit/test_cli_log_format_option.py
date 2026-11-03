"""`--log-format` is one app-level option (`saga --log-format json ingest`),
not repeated on every subcommand: it shapes the whole process's console
output and means nothing to interactive commands like `doctor` or `lint`.

Introspects resolved click options rather than asserting on rendered help text.
"""

import pytest
from typer.main import get_command
from typer.testing import CliRunner

from dlt_saga.cli import app
from dlt_saga.utility.cli import logging as cli_logging


def _param(command, flag):
    for param in command.params:
        if flag in getattr(param, "opts", []):
            return param
    return None


@pytest.mark.unit
class TestLogFormatOption:
    def test_lives_on_the_app_not_on_subcommands(self):
        group = get_command(app)
        assert _param(group, "--log-format") is not None
        for name, command in group.commands.items():
            assert _param(command, "--log-format") is None, name

    def test_choices_are_text_and_json_and_default_keeps_startup_choice(self):
        param = _param(get_command(app), "--log-format")
        assert set(param.type.choices) == {"text", "json"}
        assert param.default is None

    def test_sets_console_format_before_the_command_runs(self, monkeypatch):
        monkeypatch.setattr(cli_logging, "_console_format", "text")
        monkeypatch.setattr(cli_logging, "_console_handler", None)
        # The callback alone (invoke_without_command): --version would exit
        # eagerly during parsing, before the callback body runs.
        result = CliRunner().invoke(app, ["--log-format", "json"])
        assert result.exit_code == 0, result.output
        assert cli_logging.get_console_format() == "json"
