"""``ensure_utf8_streams`` must make CLI output survive a non-UTF-8 stream.

Regression for the ``saga doctor`` ``UnicodeEncodeError``: redirected stdout on
a Western-European Windows code page (cp1252) has no ``✓``/``→``, so a bare
``typer.echo("✓")`` aborted the command. The entry point now reconfigures the
streams to UTF-8 before any command runs.
"""

import io

import pytest

from dlt_saga.utility.cli.logging import ensure_utf8_streams


@pytest.mark.unit
class TestEnsureUtf8Streams:
    def test_reconfigures_cp1252_stream_to_utf8(self, monkeypatch):
        # A cp1252 text stream can't encode ✓ — exactly the redirected-output trap.
        cp1252 = io.TextIOWrapper(io.BytesIO(), encoding="cp1252")
        with pytest.raises(UnicodeEncodeError):
            cp1252.write("✓")

        monkeypatch.setattr("sys.stdout", cp1252)
        ensure_utf8_streams()

        assert cp1252.encoding == "utf-8"
        # Now the glyph the doctor emits first goes through without raising.
        cp1252.write("✓ → ✗")

    def test_replace_errors_on_encoding_that_cannot_take_glyph(self, monkeypatch):
        # errors="replace" is the safety net: even if a stream stays on a codec
        # that lacks the glyph, writing must not raise (it degrades, not crashes).
        stream = io.TextIOWrapper(io.BytesIO(), encoding="utf-8")
        monkeypatch.setattr("sys.stderr", stream)
        ensure_utf8_streams()
        assert stream.errors == "replace"

    def test_skips_stream_without_reconfigure(self, monkeypatch):
        # A capture shim without .reconfigure must be skipped, not crash startup.
        class _Bare:
            pass

        monkeypatch.setattr("sys.stdout", _Bare())
        ensure_utf8_streams()  # no raise

    def test_swallows_reconfigure_failure(self, monkeypatch):
        class _Boom:
            def reconfigure(self, **kwargs):
                raise ValueError("detached")

        monkeypatch.setattr("sys.stdout", _Boom())
        ensure_utf8_streams()  # no raise
