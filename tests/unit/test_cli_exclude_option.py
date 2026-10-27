"""`--exclude` is offered wherever `--select` is.

Introspects resolved click options rather than asserting on rendered help text.
"""

import pytest
from typer.main import get_command

from dlt_saga.cli import app


def _commands_with_select() -> dict:
    commands = get_command(app).commands
    return {
        name: {opt for param in command.params for opt in param.opts}
        for name, command in commands.items()
        if any("--select" in param.opts for param in command.params)
    }


@pytest.mark.unit
def test_every_selecting_command_accepts_exclude():
    commands = _commands_with_select()
    assert commands, "expected commands with --select"
    missing = sorted(name for name, opts in commands.items() if "--exclude" not in opts)
    assert missing == []


@pytest.mark.unit
def test_exclude_is_repeatable_and_unset_by_default():
    command = get_command(app).commands["ingest"]
    (param,) = [p for p in command.params if "--exclude" in p.opts]
    assert param.multiple
    assert param.default is None
