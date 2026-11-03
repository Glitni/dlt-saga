"""Unit tests for the CLI logging configuration (file + console handlers)."""

import io
import json
import logging
import os
from concurrent.futures import ThreadPoolExecutor
from unittest.mock import patch

import pytest

from dlt_saga.utility.cli import logging as cli_logging

pytestmark = pytest.mark.unit


@pytest.fixture(autouse=True)
def _isolate_logging(monkeypatch, tmp_path):
    """Reset root logger state and force a clean log dir for each test."""
    root = logging.getLogger()
    saved_handlers = list(root.handlers)
    saved_level = root.level

    # Reset module-level state captured by configure_cli_logging.
    cli_logging._log_file_path = None
    cli_logging._console_handler = None
    cli_logging._console_format = "text"
    cli_logging._process_context.clear()

    dlt_logger = logging.getLogger("dlt")
    saved_dlt_handlers = list(dlt_logger.handlers)
    saved_dlt_propagate = dlt_logger.propagate

    # Clear any environment that would steer the gating logic.
    for var in (
        "SAGA_LOG_FILE",
        "SAGA_LOG_DIR",
        "SAGA_LOG_RETENTION",
        "SAGA_LOG_FORMAT",
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
    cli_logging._console_format = "text"
    cli_logging._process_context.clear()
    for h in list(dlt_logger.handlers):
        dlt_logger.removeHandler(h)
    for h in saved_dlt_handlers:
        dlt_logger.addHandler(h)
    dlt_logger.propagate = saved_dlt_propagate


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

    def test_file_only_record_skips_the_console(self, monkeypatch, capsys):
        _force_enable(monkeypatch)
        cli_logging.configure_cli_logging()

        logger = logging.getLogger("dlt_saga.test.file_only")
        logger.error("file-only-marker", extra=cli_logging.FILE_ONLY)
        logger.error("both-marker")
        for h in logging.getLogger().handlers:
            h.flush()

        console = capsys.readouterr().err
        assert "both-marker" in console
        assert "file-only-marker" not in console
        contents = cli_logging.get_log_file_path().read_text(encoding="utf-8")
        assert "file-only-marker" in contents
        assert "both-marker" in contents


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

    def test_current_file_survives_an_mtime_tie(self, tmp_path):
        """Coarse filesystem timestamps can tie this run's file with files
        written just before it; it must survive regardless of listing order."""
        log_dir = tmp_path / "logs"
        log_dir.mkdir()
        paths = [log_dir / f"saga-{name}.log" for name in ("a", "b", "c", "z")]
        for path in paths:
            path.write_text("x")
            os.utime(path, (1_700_000_000, 1_700_000_000))

        for current in paths:
            cli_logging._cleanup_old_log_files(log_dir, retention=1, current=current)
            assert list(log_dir.glob("saga-*.log")) == [current]
            for path in paths:
                path.write_text("x")
                os.utime(path, (1_700_000_000, 1_700_000_000))

    def test_retention_counts_the_current_file(self, tmp_path):
        log_dir = tmp_path / "logs"
        log_dir.mkdir()
        for i in range(5):
            path = log_dir / f"saga-old-{i}.log"
            path.write_text("stale")
            os.utime(path, (1_700_000_000 + i, 1_700_000_000 + i))
        current = log_dir / "saga-current.log"
        current.write_text("x")
        os.utime(current, (1_600_000_000, 1_600_000_000))  # oldest mtime

        cli_logging._cleanup_old_log_files(log_dir, retention=3, current=current)

        assert {p.name for p in log_dir.glob("saga-*.log")} == {
            "saga-current.log",
            "saga-old-4.log",
            "saga-old-3.log",
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


@pytest.mark.unit
class TestColoredFormatter:
    def test_does_not_leak_color_into_shared_record(self):
        # Handlers share one LogRecord: the console's color codes must not reach
        # the file handler (or any other handler) formatting it afterwards
        record = logging.LogRecord(
            "dlt_saga.test", logging.WARNING, __file__, 1, "careful", None, None
        )
        colored = cli_logging._ColoredFormatter("%(levelname)s %(message)s")

        assert colored.format(record) != "WARNING careful"
        assert record.levelname == "WARNING"
        assert logging.Formatter("%(levelname)s").format(record) == "WARNING"


_StreamHandler = logging.StreamHandler


def _capture_console() -> io.StringIO:
    """Point the console handler at a buffer and return it."""
    buf = io.StringIO()
    assert cli_logging._console_handler is not None
    cli_logging._console_handler.setStream(buf)
    return buf


def _records(buf: io.StringIO) -> list:
    return [json.loads(line) for line in buf.getvalue().splitlines() if line]


class TestJsonFormat:
    def test_env_var_selects_one_json_object_per_line(self, monkeypatch):
        monkeypatch.setenv("SAGA_LOG_FORMAT", "json")
        cli_logging.configure_cli_logging()
        buf = _capture_console()

        logging.getLogger("dlt_saga.test").info("hello %s", "world")
        logging.getLogger("dlt_saga.test").warning("careful")

        first, second = _records(buf)
        assert first["message"] == "hello world"
        assert first["severity"] == "INFO"
        assert first["logger"] == "dlt_saga.test"
        assert first["time"].endswith("Z")
        assert second["severity"] == "WARNING"
        assert cli_logging.get_console_format() == "json"

    def test_env_var_is_case_insensitive(self, monkeypatch):
        monkeypatch.setenv("SAGA_LOG_FORMAT", "JSON")
        cli_logging.configure_cli_logging()
        assert cli_logging.get_console_format() == "json"

    def test_unknown_env_value_falls_back_to_text_and_warns(self, monkeypatch):
        monkeypatch.setenv("SAGA_LOG_FORMAT", "yaml")
        buf = io.StringIO()
        # The warning must go through the console handler being configured,
        # not Python's last-resort handler: capture its stream at creation.
        with patch.object(logging, "StreamHandler", lambda: _StreamHandler(buf)):
            cli_logging.configure_cli_logging()

        logging.getLogger("dlt_saga.test").info("plain")

        assert cli_logging.get_console_format() == "text"
        lines = buf.getvalue().splitlines()
        assert "Unknown SAGA_LOG_FORMAT 'yaml'" in lines[0]
        assert "WARNING" in lines[0].split(" - ")[1]  # colored on a terminal
        assert lines[1].endswith("INFO - plain")

    def test_default_is_text(self):
        cli_logging.configure_cli_logging()
        assert cli_logging.get_console_format() == "text"
        assert not isinstance(
            cli_logging._console_handler.formatter, cli_logging._JsonFormatter
        )

    def test_set_console_format_switches_the_live_handler(self):
        cli_logging.configure_cli_logging()
        buf = _capture_console()

        cli_logging.set_console_format("json")
        logging.getLogger("dlt_saga.test").info("structured")
        cli_logging.set_console_format("text")
        logging.getLogger("dlt_saga.test").info("unstructured")

        lines = buf.getvalue().splitlines()
        assert json.loads(lines[0])["message"] == "structured"
        assert lines[1].endswith("INFO - unstructured")

    def test_set_console_format_rejects_unknown(self):
        cli_logging.configure_cli_logging()
        with pytest.raises(ValueError):
            cli_logging.set_console_format("xml")

    def test_file_handler_stays_text(self, monkeypatch):
        _force_enable(monkeypatch)
        monkeypatch.setenv("SAGA_LOG_FORMAT", "json")
        cli_logging.configure_cli_logging()
        _capture_console()

        logging.getLogger("dlt_saga.test").info("to both")
        for h in logging.getLogger().handlers:
            h.flush()

        content = cli_logging.get_log_file_path().read_text(encoding="utf-8")
        assert "INFO     dlt_saga.test to both" in content

    def test_exception_is_a_field_not_extra_lines(self, monkeypatch):
        monkeypatch.setenv("SAGA_LOG_FORMAT", "json")
        cli_logging.configure_cli_logging()
        buf = _capture_console()

        try:
            raise ValueError("boom")
        except ValueError:
            logging.getLogger("dlt_saga.test").error("failed", exc_info=True)

        (record,) = _records(buf)
        assert record["message"] == "failed"
        assert "ValueError: boom" in record["exception"]

    def test_non_ascii_message_is_kept_verbatim(self, monkeypatch):
        monkeypatch.setenv("SAGA_LOG_FORMAT", "json")
        cli_logging.configure_cli_logging()
        buf = _capture_console()

        logging.getLogger("dlt_saga.test").info("✓ ferdig")

        assert "✓ ferdig" in buf.getvalue()
        assert _records(buf)[0]["message"] == "✓ ferdig"


class TestLogContext:
    def test_pipeline_context_is_stamped_only_inside_the_block(self, monkeypatch):
        monkeypatch.setenv("SAGA_LOG_FORMAT", "json")
        cli_logging.configure_cli_logging()
        buf = _capture_console()
        log = logging.getLogger("dlt_saga.test")

        log.info("before")
        with cli_logging.log_context(pipeline="group__table", command="ingest"):
            log.info("inside")
        log.info("after")

        before, inside, after = _records(buf)
        assert "pipeline" not in before and "pipeline" not in after
        assert inside["pipeline"] == "group__table"
        assert inside["command"] == "ingest"

    def test_process_context_applies_everywhere_and_none_clears(self, monkeypatch):
        monkeypatch.setenv("SAGA_LOG_FORMAT", "json")
        cli_logging.configure_cli_logging()
        buf = _capture_console()
        log = logging.getLogger("dlt_saga.test")

        cli_logging.bind_log_context(execution_id="abc", task_index=3)
        with cli_logging.log_context(pipeline="p"):
            log.info("both")
        cli_logging.bind_log_context(execution_id=None)
        log.info("cleared")

        both, cleared = _records(buf)
        assert both["execution_id"] == "abc"
        assert both["task_index"] == "3"
        assert both["pipeline"] == "p"
        assert "execution_id" not in cleared
        assert cleared["task_index"] == "3"

    def test_context_is_per_thread(self, monkeypatch):
        monkeypatch.setenv("SAGA_LOG_FORMAT", "json")
        cli_logging.configure_cli_logging()
        buf = _capture_console()
        log = logging.getLogger("dlt_saga.test")

        def run(name: str) -> None:
            with cli_logging.log_context(pipeline=name):
                log.info("from %s", name)

        with ThreadPoolExecutor(max_workers=2) as pool:
            list(pool.map(run, ["a", "b", "c", "d"]))
        # A pool thread must not carry the last task's context into later work.
        with ThreadPoolExecutor(max_workers=2) as pool:
            list(pool.map(lambda _: log.info("bare"), range(2)))

        records = _records(buf)
        named = {r["message"]: r["pipeline"] for r in records if "pipeline" in r}
        assert named == {f"from {n}": n for n in "abcd"}
        assert all("pipeline" not in r for r in records if r["message"] == "bare")

    def test_text_format_ignores_context(self):
        cli_logging.configure_cli_logging()
        buf = _capture_console()

        with cli_logging.log_context(pipeline="p"):
            logging.getLogger("dlt_saga.test").info("plain")

        assert buf.getvalue().strip().endswith("INFO - plain")


class TestDltLogBridge:
    """dlt's own records must come out through saga's handlers, once."""

    def test_dlt_record_reaches_console_once_before_dlt_initialises(self, monkeypatch):
        monkeypatch.setenv("SAGA_LOG_FORMAT", "json")
        cli_logging.configure_cli_logging()
        buf = _capture_console()

        logging.getLogger("dlt").warning("from dlt")

        (record,) = _records(buf)
        assert record["logger"] == "dlt"

    def test_bridge_survives_dlt_creating_its_logger(self, monkeypatch):
        from dlt.common.logger import _create_logger

        monkeypatch.setenv("SAGA_LOG_FORMAT", "json")
        cli_logging.configure_cli_logging()
        buf = _capture_console()

        # What dlt does on runtime init: reuse the first handler on the logger,
        # set its own formatter, stop propagation.
        for _ in range(2):
            _create_logger("dlt", "WARNING", "{message}", "dlt", {})
        with cli_logging.log_context(pipeline="p"):
            logging.getLogger("dlt").warning("after init")

        (record,) = _records(buf)
        assert record["message"] == "after init"
        assert record["pipeline"] == "p"
        assert [type(h) for h in logging.getLogger("dlt").handlers] == [
            cli_logging._DltLogBridge
        ]

    def test_dlt_record_reaches_console_and_log_file_in_text_mode(self, monkeypatch):
        _force_enable(monkeypatch)
        cli_logging.configure_cli_logging()
        buf = _capture_console()

        logging.getLogger("dlt").warning("dlt says")
        for h in logging.getLogger().handlers:
            h.flush()

        assert "dlt says" in buf.getvalue()
        assert "dlt says" in cli_logging.get_log_file_path().read_text(encoding="utf-8")
