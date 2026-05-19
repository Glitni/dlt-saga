"""Unit tests for the CLI logging configuration (file + console handlers)."""

import logging
import os

import pytest

from dlt_saga.utility.cli import logging as cli_logging


@pytest.fixture(autouse=True)
def _isolate_logging(monkeypatch, tmp_path):
    """Reset root logger state and force a clean log dir for each test."""
    root = logging.getLogger()
    saved_handlers = list(root.handlers)
    saved_level = root.level

    # Reset module-level state captured by configure_cli_logging.
    cli_logging._log_file_path = None
    cli_logging._console_handler = None

    # Clear any environment that would steer the gating logic.
    for var in (
        "SAGA_LOG_FILE",
        "SAGA_LOG_DIR",
        "SAGA_LOG_RETENTION",
        "SAGA_WORKER_MODE",
        "K_SERVICE",
        "CLOUD_RUN_JOB",
    ):
        monkeypatch.delenv(var, raising=False)

    # Anchor the default log dir in tmp so we don't pollute the repo.
    monkeypatch.setenv("SAGA_LOG_DIR", str(tmp_path / "logs"))

    yield

    for h in list(root.handlers):
        root.removeHandler(h)
    for h in saved_handlers:
        root.addHandler(h)
    root.setLevel(saved_level)
    cli_logging._log_file_path = None
    cli_logging._console_handler = None


def _force_enable(monkeypatch):
    """Make configure_cli_logging act as if invoked from a real terminal session."""
    monkeypatch.setenv("SAGA_LOG_FILE", "1")


class TestFileHandlerLifecycle:
    def test_creates_log_file_when_enabled(self, monkeypatch, tmp_path):
        _force_enable(monkeypatch)
        cli_logging.configure_cli_logging()

        path = cli_logging.get_log_file_path()
        assert path is not None
        assert path.exists()
        assert path.parent == tmp_path / "logs"
        assert path.name.startswith("saga-")
        assert path.suffix == ".log"

    def test_file_handler_captures_debug_records(self, monkeypatch):
        _force_enable(monkeypatch)
        cli_logging.configure_cli_logging()

        logger = logging.getLogger("dlt_saga.test.file_handler")
        logger.debug("debug-from-test-marker")
        for h in logging.getLogger().handlers:
            h.flush()

        path = cli_logging.get_log_file_path()
        assert path is not None
        contents = path.read_text(encoding="utf-8")
        assert "debug-from-test-marker" in contents

    def test_console_stays_at_info_by_default(self, monkeypatch):
        _force_enable(monkeypatch)
        cli_logging.configure_cli_logging()

        assert cli_logging._console_handler is not None
        assert cli_logging._console_handler.level == logging.INFO
        # Root must permit DEBUG so the file handler still receives detail.
        assert logging.getLogger().level == logging.DEBUG

    def test_set_console_verbose_widens_terminal_only(self, monkeypatch):
        _force_enable(monkeypatch)
        cli_logging.configure_cli_logging()

        cli_logging.set_console_verbose(True)
        assert cli_logging._console_handler.level == logging.DEBUG
        # The file handler stays at DEBUG either way.
        file_handlers = [
            h
            for h in logging.getLogger().handlers
            if isinstance(h, logging.FileHandler)
        ]
        assert len(file_handlers) == 1
        assert file_handlers[0].level == logging.DEBUG

        cli_logging.set_console_verbose(False)
        assert cli_logging._console_handler.level == logging.INFO


class TestFileLoggingGating:
    def test_disabled_in_cloud_run(self, monkeypatch):
        monkeypatch.setenv("K_SERVICE", "saga-service")
        cli_logging.configure_cli_logging()

        assert cli_logging.get_log_file_path() is None
        assert not any(
            isinstance(h, logging.FileHandler) for h in logging.getLogger().handlers
        )

    def test_disabled_in_worker_mode(self, monkeypatch):
        monkeypatch.setenv("SAGA_WORKER_MODE", "true")
        cli_logging.configure_cli_logging()

        assert cli_logging.get_log_file_path() is None
        assert not any(
            isinstance(h, logging.FileHandler) for h in logging.getLogger().handlers
        )

    def test_disabled_when_env_opts_out(self, monkeypatch):
        monkeypatch.setenv("SAGA_LOG_FILE", "0")
        cli_logging.configure_cli_logging()

        assert cli_logging.get_log_file_path() is None

    def test_explicit_opt_in_overrides_pytest_autodetect(self, monkeypatch):
        # Without SAGA_LOG_FILE the gating would skip file logging because
        # pytest is in sys.modules — but an explicit opt-in must win.
        _force_enable(monkeypatch)
        cli_logging.configure_cli_logging()
        assert cli_logging.get_log_file_path() is not None


class TestRetention:
    def test_keeps_only_n_most_recent_files(self, monkeypatch, tmp_path):
        log_dir = tmp_path / "logs"
        log_dir.mkdir()

        # Create 15 stale files with monotonically increasing mtimes.
        for i in range(15):
            path = log_dir / f"saga-old-{i:02d}.log"
            path.write_text("stale")
            os.utime(path, (1_700_000_000 + i, 1_700_000_000 + i))

        cli_logging._cleanup_old_log_files(log_dir, retention=5)

        remaining = sorted(log_dir.glob("saga-*.log"))
        assert len(remaining) == 5
        # The 5 newest (highest mtime) should survive.
        assert {p.name for p in remaining} == {
            f"saga-old-{i:02d}.log" for i in range(10, 15)
        }

    def test_retention_via_env_var(self, monkeypatch, tmp_path):
        _force_enable(monkeypatch)
        log_dir = tmp_path / "logs"
        log_dir.mkdir()
        # Pre-seed three stale files.
        for i in range(3):
            (log_dir / f"saga-old-{i}.log").write_text("stale")

        monkeypatch.setenv("SAGA_LOG_RETENTION", "1")
        monkeypatch.setenv("SAGA_LOG_DIR", str(log_dir))

        cli_logging.configure_cli_logging()

        # 1 retained = the new file just created by configure_cli_logging.
        remaining = list(log_dir.glob("saga-*.log"))
        assert len(remaining) == 1
        assert remaining[0] == cli_logging.get_log_file_path()


class TestIdempotency:
    def test_repeat_calls_do_not_stack_handlers(self, monkeypatch):
        _force_enable(monkeypatch)
        cli_logging.configure_cli_logging()
        cli_logging.configure_cli_logging()
        cli_logging.configure_cli_logging()

        handlers = logging.getLogger().handlers
        # Exactly one StreamHandler (console) and one FileHandler.
        stream_handlers = [
            h
            for h in handlers
            if isinstance(h, logging.StreamHandler)
            and not isinstance(h, logging.FileHandler)
        ]
        file_handlers = [h for h in handlers if isinstance(h, logging.FileHandler)]
        assert len(stream_handlers) == 1
        assert len(file_handlers) == 1
