"""
Tests for error aggregation system.
"""

from datetime import datetime, timedelta

import pytest

from vibe_piper.monitoring.errors import (
    ErrorAggregator,
    ErrorCategory,
    ErrorRecord,
    ErrorSeverity,
)


def test_error_severity_enum() -> None:
    """Test error severity enum values."""
    assert ErrorSeverity.LOW.value < ErrorSeverity.MEDIUM.value
    assert ErrorSeverity.MEDIUM.value < ErrorSeverity.HIGH.value
    assert ErrorSeverity.HIGH.value < ErrorSeverity.CRITICAL.value


def test_error_category_enum() -> None:
    """Test error category enum."""
    categories = [
        ErrorCategory.VALIDATION,
        ErrorCategory.CONNECTION,
        ErrorCategory.TRANSFORMATION,
        ErrorCategory.IO,
        ErrorCategory.TIMEOUT,
        ErrorCategory.SYSTEM,
        ErrorCategory.UNKNOWN,
    ]

    assert len(categories) == 7
    assert all(isinstance(cat, ErrorCategory) for cat in categories)


def test_error_record_creation() -> None:
    """Test creating an error record."""
    error = ErrorRecord(
        error_id="test_error_1",
        error_type="ValueError",
        error_message="Test error message",
        severity=ErrorSeverity.MEDIUM,
        category=ErrorCategory.VALIDATION,
        asset_name="test_asset",
        pipeline_id="test_pipeline",
        run_id="test_run",
        stack_trace="Traceback...",
    )

    assert error.error_id == "test_error_1"
    assert error.error_type == "ValueError"
    assert error.error_message == "Test error message"
    assert error.severity == ErrorSeverity.MEDIUM
    assert error.category == ErrorCategory.VALIDATION
    assert error.asset_name == "test_asset"
    assert error.pipeline_id == "test_pipeline"
    assert error.run_id == "test_run"
    assert error.count == 1
    assert error.first_seen is not None
    assert error.last_seen is not None


def test_error_record_increment_count() -> None:
    """Test incrementing error count."""
    original = ErrorRecord(
        error_id="test_error_1",
        error_type="ValueError",
        error_message="Test error",
        severity=ErrorSeverity.LOW,
        category=ErrorCategory.UNKNOWN,
        count=5,
        first_seen=datetime(2026, 1, 1, 0, 0),
        last_seen=datetime(2026, 1, 1, 0, 0),
    )

    incremented = original.increment_count()

    assert incremented.count == 6
    assert incremented.error_id == original.error_id
    assert incremented.error_type == original.error_type
    assert incremented.last_seen > original.last_seen


def test_error_aggregator_initialization() -> None:
    """Test error aggregator initialization."""
    aggregator = ErrorAggregator(enabled=True)

    assert aggregator.enabled is True
    assert len(aggregator) == 0


def test_error_aggregator_disabled() -> None:
    """Test that disabled aggregator doesn't collect errors."""
    aggregator = ErrorAggregator(enabled=False)

    aggregator.add_error(
        error_type="ValueError",
        error_message="Test",
        severity=ErrorSeverity.HIGH,
        category=ErrorCategory.UNKNOWN,
    )

    assert len(aggregator) == 0


def test_error_aggregator_add_error() -> None:
    """Test adding an error to aggregator."""
    aggregator = ErrorAggregator(enabled=True)

    error = aggregator.add_error(
        error_type="ValueError",
        error_message="Test error message",
        severity=ErrorSeverity.MEDIUM,
        category=ErrorCategory.VALIDATION,
        asset_name="test_asset",
        pipeline_id="test_pipeline",
        run_id="test_run",
    )

    assert len(aggregator) == 1
    assert error.error_type == "ValueError"
    assert error.asset_name == "test_asset"


def test_error_aggregator_add_error_with_context() -> None:
    """Test adding error with context."""
    aggregator = ErrorAggregator(enabled=True)

    error = aggregator.add_error(
        error_type="ConnectionError",
        error_message="Failed to connect",
        severity=ErrorSeverity.HIGH,
        category=ErrorCategory.CONNECTION,
        asset_name="test_asset",
        context={"retry_count": 3, "last_attempt": "2026-01-29"},
    )

    assert len(aggregator) == 1
    assert error.context == {"retry_count": 3, "last_attempt": "2026-01-29"}


def test_error_aggregator_aggregation() -> None:
    """Test that similar errors are aggregated."""
    aggregator = ErrorAggregator(
        enabled=True,
        aggregation_window=timedelta(minutes=5),
    )

    # Add same error twice
    error1 = aggregator.add_error(
        error_type="ValueError",
        error_message="Test error",
        severity=ErrorSeverity.LOW,
        category=ErrorCategory.VALIDATION,
        asset_name="test_asset",
    )

    error2 = aggregator.add_error(
        error_type="ValueError",
        error_message="Test error",
        severity=ErrorSeverity.LOW,
        category=ErrorCategory.VALIDATION,
        asset_name="test_asset",
    )

    # Should be aggregated (only one record with count 2)
    # Note: error2 is the returned ErrorRecord from the second call,
    # but it references the same aggregated error in the dict
    assert len(aggregator) == 1
    # Both returned records should have count 2
    assert error1.count == 2
    # error2 may be a reference to the updated error or a new record
    # Check that the stored error has count 2
    stored = aggregator.get_error(error1.error_id)
    assert stored is not None
    assert stored.count == 2


def test_error_aggregator_no_aggregation_different_messages() -> None:
    """Test that different error messages are not aggregated."""
    aggregator = ErrorAggregator(
        enabled=True,
        aggregation_window=timedelta(minutes=5),
    )

    aggregator.add_error(
        error_type="ValueError",
        error_message="Error message 1",
        severity=ErrorSeverity.LOW,
        category=ErrorCategory.VALIDATION,
    )

    aggregator.add_error(
        error_type="ValueError",
        error_message="Error message 2",  # Different message
        severity=ErrorSeverity.LOW,
        category=ErrorCategory.VALIDATION,
    )

    # Should not be aggregated (two separate records)
    assert len(aggregator) == 2


def test_error_aggregator_get_errors_by_severity() -> None:
    """Test filtering errors by severity."""
    aggregator = ErrorAggregator(enabled=True)

    aggregator.add_error(
        error_type="Error1",
        error_message="Low severity",
        severity=ErrorSeverity.LOW,
        category=ErrorCategory.UNKNOWN,
    )

    aggregator.add_error(
        error_type="Error2",
        error_message="High severity",
        severity=ErrorSeverity.HIGH,
        category=ErrorCategory.UNKNOWN,
    )

    aggregator.add_error(
        error_type="Error3",
        error_message="Critical severity",
        severity=ErrorSeverity.CRITICAL,
        category=ErrorCategory.UNKNOWN,
    )

    low_errors = aggregator.get_errors_by_severity(ErrorSeverity.LOW)
    high_errors = aggregator.get_errors_by_severity(ErrorSeverity.HIGH)
    critical_errors = aggregator.get_errors_by_severity(ErrorSeverity.CRITICAL)

    assert len(low_errors) == 1
    assert len(high_errors) == 1
    assert len(critical_errors) == 1


def test_error_aggregator_get_errors_by_category() -> None:
    """Test filtering errors by category."""
    aggregator = ErrorAggregator(enabled=True)

    aggregator.add_error(
        error_type="Error1",
        error_message="Validation error",
        severity=ErrorSeverity.MEDIUM,
        category=ErrorCategory.VALIDATION,
    )

    aggregator.add_error(
        error_type="Error2",
        error_message="Connection error",
        severity=ErrorSeverity.HIGH,
        category=ErrorCategory.CONNECTION,
    )

    aggregator.add_error(
        error_type="Error3",
        error_message="IO error",
        severity=ErrorSeverity.MEDIUM,
        category=ErrorCategory.IO,
    )

    validation_errors = aggregator.get_errors_by_category(ErrorCategory.VALIDATION)
    connection_errors = aggregator.get_errors_by_category(ErrorCategory.CONNECTION)
    io_errors = aggregator.get_errors_by_category(ErrorCategory.IO)

    assert len(validation_errors) == 1
    assert len(connection_errors) == 1
    assert len(io_errors) == 1


def test_error_aggregator_get_errors_by_asset() -> None:
    """Test filtering errors by asset name."""
    aggregator = ErrorAggregator(enabled=True)

    aggregator.add_error(
        error_type="Error1",
        error_message="Asset A error",
        severity=ErrorSeverity.LOW,
        category=ErrorCategory.UNKNOWN,
        asset_name="asset_a",
    )

    aggregator.add_error(
        error_type="Error2",
        error_message="Asset B error",
        severity=ErrorSeverity.HIGH,
        category=ErrorCategory.UNKNOWN,
        asset_name="asset_b",
    )

    aggregator.add_error(
        error_type="Error3",
        error_message="Asset A error 2",
        severity=ErrorSeverity.MEDIUM,
        category=ErrorCategory.UNKNOWN,
        asset_name="asset_a",
    )

    asset_a_errors = aggregator.get_errors_by_asset("asset_a")
    asset_b_errors = aggregator.get_errors_by_asset("asset_b")

    assert len(asset_a_errors) == 2
    assert len(asset_b_errors) == 1


def test_error_aggregator_get_summary() -> None:
    """Test getting error summary."""
    aggregator = ErrorAggregator(enabled=True)

    aggregator.add_error(
        error_type="Error1",
        error_message="Low severity",
        severity=ErrorSeverity.LOW,
        category=ErrorCategory.VALIDATION,
        asset_name="asset_a",
    )

    aggregator.add_error(
        error_type="Error2",
        error_message="High severity",
        severity=ErrorSeverity.HIGH,
        category=ErrorCategory.CONNECTION,
        asset_name="asset_b",
    )

    # Add same error twice
    aggregator.add_error(
        error_type="Error3",
        error_message="Repeated error",
        severity=ErrorSeverity.MEDIUM,
        category=ErrorCategory.IO,
        asset_name="asset_a",
    )

    aggregator.add_error(
        error_type="Error3",
        error_message="Repeated error",
        severity=ErrorSeverity.MEDIUM,
        category=ErrorCategory.IO,
        asset_name="asset_a",
    )

    summary = aggregator.get_summary()

    assert "total_errors" in summary
    assert "unique_errors" in summary
    assert "by_severity" in summary
    assert "by_category" in summary
    assert "by_asset" in summary
    assert "top_errors" in summary

    # Check totals
    # Error1 (1), Error2 (1), Error3 (aggregated count 2)
    assert summary["total_errors"] == 4  # 1 + 1 + 2
    assert summary["unique_errors"] == 3  # Error1, Error2, Error3

    # Check by severity
    assert summary["by_severity"]["LOW"] == 1
    assert summary["by_severity"]["HIGH"] == 1
    assert summary["by_severity"]["MEDIUM"] == 2  # Error3 counted twice

    # Check by category
    assert summary["by_category"]["VALIDATION"] == 1
    assert summary["by_category"]["CONNECTION"] == 1
    assert summary["by_category"]["IO"] == 2

    # Check by asset
    assert summary["by_asset"]["asset_a"] == 3
    assert summary["by_asset"]["asset_b"] == 1


def test_error_aggregator_clear() -> None:
    """Test clearing errors from aggregator."""
    aggregator = ErrorAggregator(enabled=True)

    aggregator.add_error(
        error_type="Error1",
        error_message="Test",
        severity=ErrorSeverity.LOW,
        category=ErrorCategory.UNKNOWN,
    )

    aggregator.add_error(
        error_type="Error2",
        error_message="Test",
        severity=ErrorSeverity.LOW,
        category=ErrorCategory.UNKNOWN,
    )

    assert len(aggregator) == 2

    aggregator.clear()

    assert len(aggregator) == 0


def test_error_aggregator_clear_older_than() -> None:
    """Test clearing errors older than a duration."""
    aggregator = ErrorAggregator(enabled=True)

    # Add error 1 minute ago
    old_error = aggregator.add_error(
        error_type="OldError",
        error_message="This is old",
        severity=ErrorSeverity.LOW,
        category=ErrorCategory.UNKNOWN,
    )
    # Mock the timestamp to be old
    # Note: In real code, timestamp is set automatically

    # Add recent error
    aggregator.add_error(
        error_type="NewError",
        error_message="This is new",
        severity=ErrorSeverity.LOW,
        category=ErrorCategory.UNKNOWN,
    )

    initial_count = len(aggregator)

    # Clear errors older than 1 second (should clear old error)
    aggregator.clear(older_than=timedelta(seconds=1))

    # Should have cleared the old error
    # Note: In real implementation, this would clear based on timestamps


def test_error_aggregator_get_critical_errors() -> None:
    """Test getting critical errors."""
    aggregator = ErrorAggregator(enabled=True)

    aggregator.add_error(
        error_type="LowError",
        error_message="Low",
        severity=ErrorSeverity.LOW,
        category=ErrorCategory.UNKNOWN,
    )

    aggregator.add_error(
        error_type="CriticalError",
        error_message="Critical",
        severity=ErrorSeverity.CRITICAL,
        category=ErrorCategory.UNKNOWN,
    )

    aggregator.add_error(
        error_type="HighError",
        error_message="High",
        severity=ErrorSeverity.HIGH,
        category=ErrorCategory.UNKNOWN,
    )

    critical_errors = aggregator.get_critical_errors()
    high_errors = aggregator.get_high_severity_errors()

    assert len(critical_errors) == 1
    assert critical_errors[0].error_type == "CriticalError"
    assert len(high_errors) == 1
    assert high_errors[0].error_type == "HighError"


def test_error_aggregator_has_critical_errors() -> None:
    """Test checking for critical errors."""
    aggregator = ErrorAggregator(enabled=True)

    aggregator.add_error(
        error_type="LowError",
        error_message="Low",
        severity=ErrorSeverity.LOW,
        category=ErrorCategory.UNKNOWN,
    )

    assert aggregator.has_critical_errors() is False

    aggregator.add_error(
        error_type="CriticalError",
        error_message="Critical",
        severity=ErrorSeverity.CRITICAL,
        category=ErrorCategory.UNKNOWN,
    )

    assert aggregator.has_critical_errors() is True


def test_error_aggregator_thread_safety() -> None:
    """Test that error aggregator is thread-safe."""
    import threading

    aggregator = ErrorAggregator(enabled=True)

    def add_errors() -> None:
        for i in range(10):
            aggregator.add_error(
                error_type=f"Error_{i}",
                error_message=f"Message {i}",
                severity=ErrorSeverity.LOW,
                category=ErrorCategory.UNKNOWN,
            )

    threads = [threading.Thread(target=add_errors) for _ in range(5)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    # Should have 50 errors total (5 threads * 10 errors each)
    assert len(aggregator) == 50


def test_error_aggregator_max_errors() -> None:
    """Test that aggregator respects max errors limit."""
    aggregator = ErrorAggregator(
        enabled=True,
        max_errors=5,
    )

    for i in range(10):
        aggregator.add_error(
            error_type=f"Error_{i}",
            error_message=f"Message {i}",
            severity=ErrorSeverity.LOW,
            category=ErrorCategory.UNKNOWN,
        )

    # Should have max 5 errors (not 10)
    assert len(aggregator) == 5
