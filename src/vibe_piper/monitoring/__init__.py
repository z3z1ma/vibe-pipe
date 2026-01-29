"""
Monitoring and observability module for Vibe Piper.

This module provides comprehensive monitoring capabilities including:
- Metrics collection (execution time, record counts, custom metrics)
- Structured logging framework
- Error aggregation and tracking
- Health checks
- Performance profiling hooks
"""

from vibe_piper.monitoring.errors import (
    ErrorAggregator,
    ErrorCategory,
    ErrorRecord,
    ErrorSeverity,
)
from vibe_piper.monitoring.health import HealthChecker, HealthStatus
from vibe_piper.monitoring.logging import (
    LogLevel,
    StructuredLogger,
    configure_logging,
    get_logger,
    log_execution,
)
from vibe_piper.monitoring.metrics import (
    MetricsCollector,
    MetricsSnapshot,
    MetricType,
)
from vibe_piper.monitoring.profiling import Profiler, profile_execution

__all__ = [
    # Metrics
    "MetricsCollector",
    "MetricsSnapshot",
    "MetricType",
    # Logging
    "LogLevel",
    "StructuredLogger",
    "configure_logging",
    "get_logger",
    "log_execution",
    # Health
    "HealthChecker",
    "HealthStatus",
    # Errors
    "ErrorAggregator",
    "ErrorRecord",
    "ErrorSeverity",
    "ErrorCategory",
    # Profiling
    "Profiler",
    "profile_execution",
]
