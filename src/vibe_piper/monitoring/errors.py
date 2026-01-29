"""
Error aggregation and tracking system for Vibe Piper.

This module provides error aggregation capabilities for:
- Collecting and categorizing errors
- Tracking error frequency
- Identifying error patterns
- Providing error summaries
"""

import logging
import threading
from collections import defaultdict
from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum, auto
from typing import Any
from uuid import uuid4

# =============================================================================
# Logger
# =============================================================================

logger = logging.getLogger(__name__)

# =============================================================================
# Error Severity
# =============================================================================


class ErrorSeverity(Enum):
    """Severity levels for errors."""

    LOW = auto()  # Non-critical, can be ignored
    MEDIUM = auto()  # Needs attention but not critical
    HIGH = auto()  # Critical, needs immediate attention
    CRITICAL = auto()  # System-breaking, emergency


# =============================================================================
# Error Categories
# =============================================================================


class ErrorCategory(Enum):
    """Categories of errors for grouping and analysis."""

    VALIDATION = auto()  # Data validation errors
    CONNECTION = auto()  # Database/connection errors
    TRANSFORMATION = auto()  # Data transformation errors
    IO = auto()  # File I/O errors
    TIMEOUT = auto()  # Timeout errors
    SYSTEM = auto()  # System/infrastructure errors
    UNKNOWN = auto()  # Uncategorized errors


# =============================================================================
# Error Record
# =============================================================================


@dataclass(frozen=True)
class ErrorRecord:
    """
    A record of an error that occurred during execution.

    Attributes:
        error_id: Unique identifier for this error
        error_type: Type of exception
        error_message: The error message
        severity: Severity level of the error
        category: Category of the error
        asset_name: Optional name of the asset that caused the error
        pipeline_id: Optional pipeline identifier
        run_id: Optional run identifier
        timestamp: When the error occurred
        stack_trace: Optional stack trace
        context: Additional context about the error
        count: Number of times this error occurred (for aggregation)
        first_seen: When this error was first seen
        last_seen: When this error was last seen
    """

    error_id: str
    error_type: str
    error_message: str
    severity: ErrorSeverity
    category: ErrorCategory
    asset_name: str | None = None
    pipeline_id: str | None = None
    run_id: str | None = None
    timestamp: datetime = field(default_factory=datetime.utcnow)
    stack_trace: str | None = None
    context: Mapping[str, Any] = field(default_factory=dict)
    count: int = 1
    first_seen: datetime | None = None
    last_seen: datetime | None = None

    def __post_init__(self) -> None:
        """Set first_seen and last_seen if not provided."""
        if self.first_seen is None:
            object.__setattr__(self, "first_seen", self.timestamp)
        if self.last_seen is None:
            object.__setattr__(self, "last_seen", self.timestamp)

    def increment_count(self) -> "ErrorRecord":
        """
        Increment the count and update last_seen timestamp.

        Returns:
            New ErrorRecord with incremented count
        """
        return ErrorRecord(
            error_id=self.error_id,
            error_type=self.error_type,
            error_message=self.error_message,
            severity=self.severity,
            category=self.category,
            asset_name=self.asset_name,
            pipeline_id=self.pipeline_id,
            run_id=self.run_id,
            timestamp=self.timestamp,
            stack_trace=self.stack_trace,
            context=self.context,
            count=self.count + 1,
            first_seen=self.first_seen,
            last_seen=datetime.utcnow(),
        )


# =============================================================================
# Error Aggregator
# =============================================================================


class ErrorAggregator:
    """
    Aggregates and tracks errors across pipeline executions.

    This class provides a mechanism for collecting, categorizing,
    and analyzing errors that occur during pipeline execution.

    Attributes:
        max_errors: Maximum number of error records to keep in memory
        aggregation_window: Time window for aggregating similar errors
        enabled: Whether error aggregation is enabled

    Example:
        Collect and aggregate errors::

            aggregator = ErrorAggregator()

            try:
                execute_pipeline()
            except Exception as e:
                aggregator.add_error(
                    error_type=type(e).__name__,
                    error_message=str(e),
                    severity=ErrorSeverity.HIGH,
                    asset_name="my_asset"
                )

            summary = aggregator.get_summary()
    """

    def __init__(
        self,
        max_errors: int = 10000,
        aggregation_window: timedelta = timedelta(hours=1),
        enabled: bool = True,
    ) -> None:
        """
        Initialize error aggregator.

        Args:
            max_errors: Maximum number of error records to keep
            aggregation_window: Time window for aggregating similar errors
            enabled: Whether error aggregation is enabled
        """
        self.max_errors = max_errors
        self.aggregation_window = aggregation_window
        self.enabled = enabled
        self._errors: dict[str, ErrorRecord] = {}
        self._lock = threading.Lock()

    def add_error(
        self,
        error_type: str,
        error_message: str,
        severity: ErrorSeverity,
        category: ErrorCategory = ErrorCategory.UNKNOWN,
        asset_name: str | None = None,
        pipeline_id: str | None = None,
        run_id: str | None = None,
        stack_trace: str | None = None,
        context: Mapping[str, Any] | None = None,
    ) -> ErrorRecord:
        """
        Add an error to the aggregator.

        Args:
            error_type: Type of exception
            error_message: The error message
            severity: Severity level of the error
            category: Category of the error
            asset_name: Optional name of the asset that caused the error
            pipeline_id: Optional pipeline identifier
            run_id: Optional run identifier
            stack_trace: Optional stack trace
            context: Additional context about the error

        Returns:
            ErrorRecord that was added or updated
        """
        if not self.enabled:
            # Create a record but don't store it
            return ErrorRecord(
                error_id=str(uuid4()),
                error_type=error_type,
                error_message=error_message,
                severity=severity,
                category=category,
                asset_name=asset_name,
                pipeline_id=pipeline_id,
                run_id=run_id,
                stack_trace=stack_trace,
                context=context or {},
            )

        # Create a unique key for aggregation
        error_key = self._create_error_key(error_type, error_message, asset_name)

        error_record = ErrorRecord(
            error_id=str(uuid4()),
            error_type=error_type,
            error_message=error_message,
            severity=severity,
            category=category,
            asset_name=asset_name,
            pipeline_id=pipeline_id,
            run_id=run_id,
            stack_trace=stack_trace,
            context=context or {},
        )

        with self._lock:
            # Check if this error already exists and is within aggregation window
            if error_key in self._errors:
                existing = self._errors[error_key]
                last_seen = existing.last_seen or datetime.utcnow()
                time_diff = datetime.utcnow() - last_seen

                if time_diff < self.aggregation_window:
                    # Aggregate with existing error
                    updated = existing.increment_count()
                    self._errors[error_key] = updated
                    logger.debug(f"Aggregated error: {error_type} (count: {updated.count})")
                    return updated

            # Add new error
            self._errors[error_key] = error_record

            # Prune old errors if over limit
            if len(self._errors) > self.max_errors:
                self._prune_old_errors()

            logger.debug(f"Added error: {error_type}")
            return error_record

    def _create_error_key(self, error_type: str, error_message: str, asset_name: str | None) -> str:
        """
        Create a unique key for error aggregation.

        Args:
            error_type: Type of exception
            error_message: The error message
            asset_name: Optional name of the asset

        Returns:
            Unique key string
        """
        # Normalize error message for aggregation
        normalized_message = error_message.split("\n")[0][:200]  # First line, truncated
        return f"{error_type}:{asset_name or 'global'}:{normalized_message}"

    def _prune_old_errors(self) -> None:
        """Remove errors outside the aggregation window."""
        now = datetime.utcnow()
        to_remove = []

        for key, error_record in self._errors.items():
            last_seen = error_record.last_seen or now
            if (now - last_seen) > self.aggregation_window:
                to_remove.append(key)

        for key in to_remove:
            del self._errors[key]

        logger.debug(f"Pruned {len(to_remove)} old errors")

    def get_error(self, error_id: str) -> ErrorRecord | None:
        """
        Get a specific error by ID.

        Args:
            error_id: Error ID to look up

        Returns:
            ErrorRecord if found, None otherwise
        """
        with self._lock:
            for error_record in self._errors.values():
                if error_record.error_id == error_id:
                    return error_record
        return None

    def get_errors_by_severity(self, severity: ErrorSeverity) -> list[ErrorRecord]:
        """
        Get all errors of a specific severity level.

        Args:
            severity: Severity level to filter by

        Returns:
            List of ErrorRecord objects
        """
        with self._lock:
            return [e for e in self._errors.values() if e.severity == severity]

    def get_errors_by_category(self, category: ErrorCategory) -> list[ErrorRecord]:
        """
        Get all errors of a specific category.

        Args:
            category: Category to filter by

        Returns:
            List of ErrorRecord objects
        """
        with self._lock:
            return [e for e in self._errors.values() if e.category == category]

    def get_errors_by_asset(self, asset_name: str) -> list[ErrorRecord]:
        """
        Get all errors for a specific asset.

        Args:
            asset_name: Name of the asset

        Returns:
            List of ErrorRecord objects
        """
        with self._lock:
            return [e for e in self._errors.values() if e.asset_name == asset_name]

    def get_summary(self) -> dict[str, Any]:
        """
        Get a summary of all aggregated errors.

        Returns:
            Dictionary with error statistics and breakdowns
        """
        with self._lock:
            if not self._errors:
                return {
                    "total_errors": 0,
                    "by_severity": {},
                    "by_category": {},
                    "by_asset": {},
                    "top_errors": [],
                }

            errors = list(self._errors.values())

            # Count by severity
            by_severity: dict[str, int] = defaultdict(int)
            for error in errors:
                by_severity[error.severity.name] += error.count

            # Count by category
            by_category: dict[str, int] = defaultdict(int)
            for error in errors:
                by_category[error.category.name] += error.count

            # Count by asset
            by_asset: dict[str, int] = defaultdict(int)
            for error in errors:
                if error.asset_name:
                    by_asset[error.asset_name] += error.count

            # Top errors by frequency
            top_errors = sorted(errors, key=lambda e: e.count, reverse=True)[:10]

            return {
                "total_errors": sum(e.count for e in errors),
                "unique_errors": len(errors),
                "by_severity": dict(by_severity),
                "by_category": dict(by_category),
                "by_asset": dict(by_asset),
                "top_errors": [
                    {
                        "error_id": e.error_id,
                        "error_type": e.error_type,
                        "error_message": e.error_message,
                        "count": e.count,
                        "severity": e.severity.name,
                        "category": e.category.name,
                        "asset_name": e.asset_name,
                        "first_seen": e.first_seen.isoformat() if e.first_seen else None,
                        "last_seen": e.last_seen.isoformat() if e.last_seen else None,
                    }
                    for e in top_errors
                ],
            }

    def clear(self, older_than: timedelta | None = None) -> None:
        """
        Clear errors from the aggregator.

        Args:
            older_than: If provided, only clears errors older than this duration
        """
        with self._lock:
            if older_than:
                now = datetime.utcnow()
                to_remove = [
                    key
                    for key, error in self._errors.items()
                    if (now - (error.last_seen or now)) > older_than
                ]
                for key in to_remove:
                    del self._errors[key]
                logger.debug(f"Cleared {len(to_remove)} old errors")
            else:
                self._errors.clear()
                logger.debug("Cleared all errors")

    def __len__(self) -> int:
        """Return the number of unique errors."""
        with self._lock:
            return len(self._errors)

    def get_critical_errors(self) -> list[ErrorRecord]:
        """Get all critical severity errors."""
        return self.get_errors_by_severity(ErrorSeverity.CRITICAL)

    def get_high_severity_errors(self) -> list[ErrorRecord]:
        """Get all high severity errors."""
        return self.get_errors_by_severity(ErrorSeverity.HIGH)

    def has_critical_errors(self) -> bool:
        """Check if there are any critical errors."""
        return len(self.get_critical_errors()) > 0
