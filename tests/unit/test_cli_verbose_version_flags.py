"""`-v` must mean `--verbose` consistently across all subcommands, and must not
collide with version.

Previously `-v` was overloaded: the top-level callback bound it to `--version`,
`lint` bound it to `--verbose`, and every other subcommand had `--verbose` with
no short flag at all. Now `-v` is `--verbose` on every subcommand and version
uses `-V`/`--version`.

Introspects resolved click options rather than asserting on rendered help text.
"""

import pytest
from typer.main import get_command

from dlt_saga.cli import app


def _flags(command) -> set:
    flags: set = set()
    for param in command.params:
        flags.update(getattr(param, "opts", []))
    return flags


@pytest.mark.unit
class TestVerboseShortFlag:
    def test_every_command_with_verbose_also_has_v(self):
        group = get_command(app)
        for name, command in group.commands.items():
            flags = _flags(command)
            if "--verbose" in flags:
                assert "-v" in flags, f"{name} has --verbose but not -v"

    def test_representative_commands_expose_verbose_short_and_long(self):
        group = get_command(app)
        for name in ("ingest", "historize", "run", "lint"):
            flags = _flags(group.commands[name])
            assert "--verbose" in flags and "-v" in flags, name

    def test_v_only_ever_binds_verbose_on_subcommands(self):
        group = get_command(app)
        for name, command in group.commands.items():
            for param in command.params:
                if "-v" in getattr(param, "opts", []):
                    assert param.name == "verbose", (
                        f"{name}: -v is bound to {param.name!r}, not verbose"
                    )


@pytest.mark.unit
class TestVersionFlag:
    def test_version_uses_capital_v_not_lowercase(self):
        top_flags: set = set()
        for param in get_command(app).params:
            top_flags.update(getattr(param, "opts", []))
        assert "--version" in top_flags
        assert "-V" in top_flags
        assert "-v" not in top_flags
