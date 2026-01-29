"""
Structured logging framework for Vibe Piper.

This module provides a structured logging system that:
- Supports JSON-formatted logs for machine parsing
- Provides context-aware logging
- Includes execution tracing
- Supports custom log levels and formatting
"""

import json
import logging
import sys
import time
from collections.abc import Mapping
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any

# =============================================================================
# Logger
# =============================================================================

logger = logging.getLogger(__name__)

# =============================================================================
# Log Levels
# =============================================================================


class LogLevel(Enum):
    """Custom log levels for Vibe Piper."""

    TRACE = 5  # More detailed than DEBUG
    DEBUG = 10  # Standard DEBUG level
    INFO = 20  # Standard INFO level
    WARNING = 30  # Standard WARNING level
    ERROR = 40  # Standard ERROR level
    CRITICAL = 50  # Standard CRITICAL level


# Add TRACE level to logging
logging.addLevelName(LogLevel.TRACE.value, "TRACE")


def _trace_method(self: logging.Logger, msg: str, *args: Any, **kwargs: Any) -> None:
    """Custom TRACE log method."""
    if self.isEnabledFor(LogLevel.TRACE.value):
        self._log(LogLevel.TRACE.value, msg, args, **kwargs)


logging.Logger.trace = _trace_method  # type: ignore[attr-defined]

# =============================================================================
# Log Formatters
# =============================================================================


class JSONFormatter(logging.Formatter):
    """
    JSON formatter for structured logging.

    Outputs log records as JSON objects with consistent structure,
    making them easy to parse and analyze with log aggregation tools.
    """

    def __init__(
        self,
        include_extra_fields: bool = True,
        timestamp_format: str = "%Y-%m-%dT%H:%M:%S.%fZ",
    ) -> None:
        """
        Initialize the JSON formatter.

        Args:
            include_extra_fields: Whether to include extra fields in the output
            timestamp_format: Format string for timestamps
        """
        super().__init__()
        self.include_extra_fields = include_extra_fields
        self.timestamp_format = timestamp_format

    def format(self, record: logging.LogRecord) -> str:
        """
        Format the log record as JSON.

        Args:
            record: The log record to format

        Returns:
            JSON-formatted log entry
        """
        log_entry: dict[str, Any] = {
            "timestamp": datetime.utcnow().strftime(self.timestamp_format),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }

        # Add exception info if present
        if record.exc_info:
            log_entry["exception"] = self.formatException(record.exc_info)

        # Add stack trace if present
        if record.stack_info:
            log_entry["stack_trace"] = self.formatStack(record.stack_info)

        # Add standard fields
        if record.pathname:
            log_entry["file"] = record.pathname
        if record.lineno:
            log_entry["line"] = record.lineno
        if record.funcName:
            log_entry["function"] = record.funcName

        # Add extra fields if enabled
        if self.include_extra_fields:
            for key, value in record.__dict__.items():
                if key not in {
                    "name",
                    "msg",
                    "args",
                    "levelname",
                    "levelno",
                    "pathname",
                    "filename",
                    "module",
                    "exc_info",
                    "exc_text",
                    "stack_info",
                    "lineno",
                    "funcName",
                    "created",
                    "msecs",
                    "relativeCreated",
                    "thread",
                    "threadName",
                    "processName",
                    "process",
                    "message",
                    "asctime",
                }:
                    log_entry[key] = value

        return json.dumps(log_entry, default=str, ensure_ascii=False)


class ColoredFormatter(logging.Formatter):
    """
    Colored formatter for console output.

    Provides color-coded log levels for better readability in terminals.
    """

    # ANSI color codes
    COLORS = {
        "TRACE": "\033[37m",  # White
        "DEBUG": "\033[36m",  # Cyan
        "INFO": "\033[32m",  # Green
        "WARNING": "\033[33m",  # Yellow
        "ERROR": "\033[31m",  # Red
        "CRITICAL": "\033[35m",  # Magenta
    }
    RESET = "\033[0m"

    def __init__(self, fmt: str = "%(asctime)s - %(levelname)s - %(name)s - %(message)s") -> None:
        """
        Initialize the colored formatter.

        Args:
            fmt: Format string for log messages
        """
        super().__init__(fmt=fmt, datefmt="%Y-%m-%d %H:%M:%S")

    def format(self, record: logging.LogRecord) -> str:
        """
        Format the log record with colors.

        Args:
            record: The log record to format

        Returns:
            Color-formatted log entry
        """
        levelname = record.levelname
        if levelname in self.COLORS:
            record.levelname = f"{self.COLORS[levelname]}{levelname}{self.RESET}"

        result = super().format(record)

        # Reset levelname for next record
        record.levelname = levelname

        return result


# =============================================================================
# Structured Logger
# =============================================================================


@dataclass
class StructuredLogger:
    """
    Wrapper around Python's logging module for structured logging.

    Provides convenient methods for logging with context and
    automatically adding pipeline/execution metadata.

    Attributes:
        name: Logger name (typically __name__)
        level: Log level threshold
        context: Default context to include in all log messages
    """

    name: str
    level: LogLevel = LogLevel.INFO
    context: Mapping[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        """Initialize the underlying logger."""
        self._logger = logging.getLogger(self.name)
        self._logger.setLevel(self.level.value)

    def _log_with_context(
        self,
        level: int,
        msg: str,
        extra_context: Mapping[str, Any] | None = None,
        *args: Any,
        **kwargs: Any,
    ) -> None:
        """
        Log a message with context.

        Args:
            level: Log level
            msg: Log message
            extra_context: Additional context to include
            *args: Additional arguments for logging
            **kwargs: Additional keyword arguments for logging
        """
        # Merge default context with extra context
        merged_context = {**self.context}
        if extra_context:
            merged_context.update(extra_context)

        # Add merged context as extra
        kwargs["extra"] = {"context": merged_context}

        self._logger.log(level, msg, *args, **kwargs)

    def trace(
        self,
        msg: str,
        extra_context: Mapping[str, Any] | None = None,
        *args: Any,
        **kwargs: Any,
    ) -> None:
        """
        Log a TRACE message.

        Args:
            msg: Log message
            extra_context: Additional context to include
            *args: Additional arguments
            **kwargs: Additional keyword arguments
        """
        self._log_with_context(LogLevel.TRACE.value, msg, extra_context, *args, **kwargs)

    def debug(
        self,
        msg: str,
        extra_context: Mapping[str, Any] | None = None,
        *args: Any,
        **kwargs: Any,
    ) -> None:
        """
        Log a DEBUG message.

        Args:
            msg: Log message
            extra_context: Additional context to include
            *args: Additional arguments
            **kwargs: Additional keyword arguments
        """
        self._log_with_context(logging.DEBUG, msg, extra_context, *args, **kwargs)

    def info(
        self,
        msg: str,
        extra_context: Mapping[str, Any] | None = None,
        *args: Any,
        **kwargs: Any,
    ) -> None:
        """
        Log an INFO message.

        Args:
            msg: Log message
            extra_context: Additional context to include
            *args: Additional arguments
            **kwargs: Additional keyword arguments
        """
        self._log_with_context(logging.INFO, msg, extra_context, *args, **kwargs)

    def warning(
        self,
        msg: str,
        extra_context: Mapping[str, Any] | None = None,
        *args: Any,
        **kwargs: Any,
    ) -> None:
        """
        Log a WARNING message.

        Args:
            msg: Log message
            extra_context: Additional context to include
            *args: Additional arguments
            **kwargs: Additional keyword arguments
        """
        self._log_with_context(logging.WARNING, msg, extra_context, *args, **kwargs)

    def error(
        self,
        msg: str,
        extra_context: Mapping[str, Any] | None = None,
        *args: Any,
        **kwargs: Any,
    ) -> None:
        """
        Log an ERROR message.

        Args:
            msg: Log message
            extra_context: Additional context to include
            *args: Additional arguments
            **kwargs: Additional keyword arguments
        """
        self._log_with_context(logging.ERROR, msg, extra_context, *args, **kwargs)

    def critical(
        self,
        msg: str,
        extra_context: Mapping[str, Any] | None = None,
        *args: Any,
        **kwargs: Any,
    ) -> None:
        """
        Log a CRITICAL message.

        Args:
            msg: Log message
            extra_context: Additional context to include
            *args: Additional arguments
            **kwargs: Additional keyword arguments
        """
        self._log_with_context(logging.CRITICAL, msg, extra_context, *args, **kwargs)

    def exception(
        self,
        msg: str,
        extra_context: Mapping[str, Any] | None = None,
        *args: Any,
        **kwargs: Any,
    ) -> None:
        """
        Log an ERROR message with exception info.

        Args:
            msg: Log message
            extra_context: Additional context to include
            *args: Additional arguments
            **kwargs: Additional keyword arguments
        """
        kwargs["exc_info"] = True
        self.error(msg, extra_context, *args, **kwargs)


# =============================================================================
# Execution Logging
# =============================================================================


@contextmanager
def log_execution(
    pipeline_id: str,
    run_id: str,
    logger_instance: logging.Logger | StructuredLogger | None = None,
    level: LogLevel = LogLevel.INFO,
) -> Any:
    """
    Context manager for logging execution of a pipeline or asset.

    Automatically logs start and completion of execution with timing.

    Args:
        pipeline_id: Pipeline identifier
        run_id: Run identifier
        logger_instance: Logger to use (defaults to module logger)
        level: Log level for messages

    Yields:
        None

    Example:
        Use as a context manager::

            with log_execution("my_pipeline", "run_123"):
                result = execute_pipeline()
    """
    if logger_instance is None:
        logger_instance = logger
    elif isinstance(logger_instance, StructuredLogger):
        logger_instance = logger_instance._logger

    start_time = time.time()
    timestamp = datetime.utcnow().isoformat()

    # Log execution start
    log_msg = f"Starting execution: pipeline={pipeline_id} run={run_id}"
    extra_context = {
        "pipeline_id": pipeline_id,
        "run_id": run_id,
        "event": "execution_start",
        "timestamp": timestamp,
    }

    logger_instance.log(level.value, log_msg, extra={"context": extra_context})

    try:
        yield
        # Log successful completion
        duration_ms = (time.time() - start_time) * 1000
        success_msg = (
            f"Execution completed: pipeline={pipeline_id} run={run_id} duration={duration_ms:.2f}ms"
        )
        success_context = {
            **extra_context,
            "event": "execution_success",
            "duration_ms": duration_ms,
        }
        logger_instance.log(level.value, success_msg, extra={"context": success_context})

    except Exception as e:
        # Log failure
        duration_ms = (time.time() - start_time) * 1000
        error_msg = (
            f"Execution failed: pipeline={pipeline_id} run={run_id} "
            f"error={str(e)} duration={duration_ms:.2f}ms"
        )
        error_context = {
            **extra_context,
            "event": "execution_failure",
            "duration_ms": duration_ms,
            "error_type": type(e).__name__,
        }
        logger_instance.log(
            logging.ERROR,
            error_msg,
            extra={"context": error_context},
            exc_info=True,
        )
        raise


# =============================================================================
# Logging Configuration
# =============================================================================


def configure_logging(
    level: LogLevel = LogLevel.INFO,
    format_type: str = "colored",  # "colored", "json", "simple"
    log_file: str | None = None,
    include_timestamps: bool = True,
) -> None:
    """
    Configure's root logger for Vibe Piper.

    Args:
        level: Minimum log level to capture
        format_type: Type of formatter to use ("colored", "json", "simple")
        log_file: Optional file path to write logs to
        include_timestamps: Whether to include timestamps in simple format

    Example:
        Configure JSON logging to a file::

            configure_logging(
                level=LogLevel.DEBUG,
                format_type="json",
                log_file="pipeline.log"
            )
    """
    # Remove any existing handlers
    root_logger = logging.getLogger()
    root_logger.handlers.clear()

    # Set log level
    root_logger.setLevel(level.value)

    # Add console handler
    if format_type == "json":
        json_formatter = JSONFormatter()
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setFormatter(json_formatter)
        root_logger.addHandler(console_handler)
    elif format_type == "colored":
        colored_formatter = ColoredFormatter()
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setFormatter(colored_formatter)
        root_logger.addHandler(console_handler)
    else:  # simple
        if include_timestamps:
            fmt = "%(asctime)s - %(levelname)s - %(name)s - %(message)s"
        else:
            fmt = "%(levelname)s - %(name)s - %(message)s"
        simple_formatter = logging.Formatter(fmt)
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setFormatter(simple_formatter)
        root_logger.addHandler(console_handler)

    # Add file handler if specified
    if log_file:
        file_handler = logging.FileHandler(log_file)
        # Always use JSON format for files
        file_formatter = JSONFormatter()
        file_handler.setFormatter(file_formatter)
        root_logger.addHandler(file_handler)

    # Log configuration
    logger.info(
        f"Logging configured: level={level.name} format={format_type} "
        f"log_file={log_file or 'stdout'}"
    )


def get_logger(name: str, context: Mapping[str, Any] | None = None) -> StructuredLogger:
    """
    Get a structured logger instance.

    Args:
        name: Logger name (typically __name__)
        context: Default context to include in all messages

    Returns:
        StructuredLogger instance

    Example:
        Get a logger for your module::

            logger = get_logger(__name__, {"module": "my_module"})
    """
    return StructuredLogger(name=name, context=context or {})
