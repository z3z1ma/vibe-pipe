"""
Tests for the logging system.
"""

import io
import json
import logging
import sys
from contextlib import redirect_stderr, redirect_stdout

import pytest

from vibe_piper.monitoring.logging import (
    ColoredFormatter,
    JSONFormatter,
    LogLevel,
    StructuredLogger,
    configure_logging,
    get_logger,
    log_execution,
)


def test_log_level_values() -> None:
    """Test that log levels have correct values."""
    assert LogLevel.TRACE.value == 5
    assert LogLevel.DEBUG.value == 10
    assert LogLevel.INFO.value == 20
    assert LogLevel.WARNING.value == 30
    assert LogLevel.ERROR.value == 40
    assert LogLevel.CRITICAL.value == 50


def test_json_formatter_basic() -> None:
    """Test JSON formatter with basic log record."""
    formatter = JSONFormatter(include_extra_fields=False)

    record = logging.LogRecord(
        name="test_logger",
        level=logging.INFO,
        pathname="test.py",
        lineno=42,
        msg="Test message",
        args=(),
    )

    result = formatter.format(record)
    log_dict = json.loads(result)

    assert log_dict["level"] == "INFO"
    assert log_dict["logger"] == "test_logger"
    assert log_dict["message"] == "Test message"
    assert "timestamp" in log_dict


def test_json_formatter_with_exception() -> None:
    """Test JSON formatter with exception info."""
    formatter = JSONFormatter()

    try:
        raise ValueError("Test exception")
    except Exception as e:
        exc_info = sys.exc_info()

        record = logging.makeLogRecord(
            dict(
                name="test_logger",
                level=logging.ERROR,
                pathname="test.py",
                lineno=42,
                msg="Error occurred",
                args=(),
                exc_info=exc_info,
            )
        )

        result = formatter.format(record)
        log_dict = json.loads(result)

        assert "exception" in log_dict
        assert "ValueError" in log_dict["exception"]


def test_json_formatter_with_extra_fields() -> None:
    """Test JSON formatter includes extra fields."""
    formatter = JSONFormatter(include_extra_fields=True)

    record = logging.LogRecord(
        name="test_logger",
        level=logging.INFO,
        pathname="test.py",
        lineno=42,
        msg="Test message",
        args=(),
    )

    # Add custom extra field
    record.custom_field = "custom_value"

    result = formatter.format(record)
    log_dict = json.loads(result)

    assert log_dict["custom_field"] == "custom_value"


def test_colored_formatter_basic() -> None:
    """Test colored formatter formats messages correctly."""
    formatter = ColoredFormatter()

    record = logging.LogRecord(
        name="test_logger",
        level=logging.INFO,
        pathname="test.py",
        lineno=42,
        msg="Test message",
        args=(),
    )

    result = formatter.format(record)

    assert "INFO" in result
    assert "Test message" in result
    assert "test_logger" in result


def test_structured_logger_initialization() -> None:
    """Test structured logger initialization."""
    logger_instance = StructuredLogger(
        name="test_logger",
        level=LogLevel.INFO,
        context={"module": "test_module"},
    )

    assert logger_instance.name == "test_logger"
    assert logger_instance.level == LogLevel.INFO
    assert logger_instance.context == {"module": "test_module"}


def test_structured_logger_log_methods() -> None:
    """Test structured logger logging methods."""
    logger_instance = StructuredLogger(name="test_logger")

    # Capture output
    with io.StringIO() as buf:
        handler = logging.StreamHandler(buf)
        logger_instance._logger.addHandler(handler)

        logger_instance.info("Test info message")
        logger_instance.warning("Test warning message")
        logger_instance.error("Test error message")

        output = buf.getvalue()
        assert "Test info message" in output
        assert "Test warning message" in output
        assert "Test error message" in output


def test_structured_logger_with_context() -> None:
    """Test structured logger includes context in logs."""
    logger_instance = StructuredLogger(
        name="test_logger",
        context={"pipeline_id": "test_pipeline", "run_id": "test_run"},
    )

    with io.StringIO() as buf:
        handler = logging.StreamHandler(buf)
        handler.setFormatter(JSONFormatter())
        logger_instance._logger.addHandler(handler)

        logger_instance.info("Test message", extra_context={"asset_name": "test_asset"})

        output = buf.getvalue()
        log_dict = json.loads(output.strip())

        assert log_dict["message"] == "Test message"
        assert "context" in log_dict
        assert log_dict["context"]["pipeline_id"] == "test_pipeline"
        assert log_dict["context"]["run_id"] == "test_run"
        assert log_dict["context"]["asset_name"] == "test_asset"


def test_get_logger() -> None:
    """Test getting a structured logger instance."""
    logger_instance = get_logger("test_module")

    assert isinstance(logger_instance, StructuredLogger)
    assert logger_instance.name == "test_module"


def test_get_logger_with_context() -> None:
    """Test getting a logger with default context."""
    logger_instance = get_logger("test_module", context={"environment": "test"})

    assert logger_instance.context == {"environment": "test"}


def test_log_execution_context_manager_success() -> None:
    """Test log_execution context manager on success."""
    import time

    with io.StringIO() as buf:
        handler = logging.StreamHandler(buf)
        handler.setFormatter(JSONFormatter())
        logging.getLogger().handlers.clear()
        logging.getLogger().addHandler(handler)
        logging.getLogger().setLevel(logging.INFO)

        with log_execution("test_pipeline", "test_run"):
            time.sleep(0.01)  # Small delay

        output = buf.getvalue()
        log_lines = [json.loads(line) for line in output.strip().split("\n") if line]

        # Should have start and success log entries
        assert len(log_lines) == 2
        start_log = log_lines[0]
        success_log = log_lines[1]

        assert start_log["context"]["event"] == "execution_start"
        assert success_log["context"]["event"] == "execution_success"
        assert "duration_ms" in success_log["context"]


def test_log_execution_context_manager_failure() -> None:
    """Test log_execution context manager on failure."""
    import time

    with io.StringIO() as buf:
        handler = logging.StreamHandler(buf)
        handler.setFormatter(JSONFormatter())
        logging.getLogger().handlers.clear()
        logging.getLogger().addHandler(handler)
        logging.getLogger().setLevel(logging.ERROR)

        with pytest.raises(ValueError, match="Test error"):
            with log_execution("test_pipeline", "test_run"):
                time.sleep(0.01)
                raise ValueError("Test error")

        output = buf.getvalue()
        log_lines = [json.loads(line) for line in output.strip().split("\n") if line]

        # Should have start and failure log entries
        assert len(log_lines) == 2
        start_log = log_lines[0]
        failure_log = log_lines[1]

        assert start_log["context"]["event"] == "execution_start"
        assert failure_log["context"]["event"] == "execution_failure"
        assert "exception" in failure_log


def test_configure_logging_colored() -> None:
    """Test configuring logging with colored format."""
    # Save original handlers
    root_logger = logging.getLogger()
    original_handlers = root_logger.handlers[:]

    configure_logging(level=LogLevel.INFO, format_type="colored")

    # Should have at least a console handler
    assert len(root_logger.handlers) > 0

    # Restore original handlers
    root_logger.handlers.clear()
    for handler in original_handlers:
        root_logger.addHandler(handler)


def test_configure_logging_json() -> None:
    """Test configuring logging with JSON format."""
    root_logger = logging.getLogger()
    original_handlers = root_logger.handlers[:]

    configure_logging(level=LogLevel.DEBUG, format_type="json")

    # Should have at least a console handler with JSON formatter
    assert len(root_logger.handlers) > 0

    # Restore original handlers
    root_logger.handlers.clear()
    for handler in original_handlers:
        root_logger.addHandler(handler)


def test_configure_logging_with_file() -> None:
    """Test configuring logging with file output."""
    import tempfile

    root_logger = logging.getLogger()
    original_handlers = root_logger.handlers[:]

    with tempfile.NamedTemporaryFile(mode="w", delete=False, suffix=".log") as f:
        log_file = f.name

    try:
        configure_logging(level=LogLevel.INFO, format_type="json", log_file=log_file)

        # Should have both console and file handlers
        assert len(root_logger.handlers) >= 2

    finally:
        import os

        # Clean up
        root_logger.handlers.clear()
        for handler in original_handlers:
            root_logger.addHandler(handler)
        if os.path.exists(log_file):
            os.remove(log_file)
