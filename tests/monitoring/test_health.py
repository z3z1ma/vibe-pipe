"""
Tests for health check system.
"""

from datetime import datetime, timedelta

import pytest

from vibe_piper.monitoring.health import (
    HealthChecker,
    HealthStatus,
    create_disk_space_check,
    create_memory_check,
)


def test_health_check_result_creation() -> None:
    """Test creating a health check result."""
    result = HealthChecker.HealthCheckResult(
        check_name="test_check",
        status=HealthStatus.HEALTHY,
        message="All systems operational",
        details={"test_key": "test_value"},
    )

    assert result.check_name == "test_check"
    assert result.status == HealthStatus.HEALTHY
    assert result.message == "All systems operational"
    assert result.is_healthy() is True
    assert result.is_unhealthy() is False


def test_health_check_result_degraded() -> None:
    """Test health check result with degraded status."""
    result = HealthChecker.HealthCheckResult(
        check_name="test_check",
        status=HealthStatus.DEGRADED,
        message="System is degraded",
    )

    assert result.is_healthy() is False
    assert result.is_degraded() is True
    assert result.is_unhealthy() is False


def test_health_check_result_unhealthy() -> None:
    """Test health check result with unhealthy status."""
    result = HealthChecker.HealthCheckResult(
        check_name="test_check",
        status=HealthStatus.UNHEALTHY,
        message="System is unhealthy",
    )

    assert result.is_healthy() is False
    assert result.is_unhealthy() is True


def test_health_checker_initialization() -> None:
    """Test health checker initialization."""
    checker = HealthChecker(
        check_timeout=5.0,
        strict_mode=False,
    )

    assert checker.check_timeout == 5.0
    assert checker.strict_mode is False
    assert len(checker.list_checks()) == 0


def test_health_checker_register_check() -> None:
    """Test registering a health check."""
    checker = HealthChecker()

    def check_fn() -> HealthChecker.HealthCheckResult:
        return HealthChecker.HealthCheckResult(
            check_name="test",
            status=HealthStatus.HEALTHY,
            message="OK",
        )

    checker.register_check("test_check", check_fn)

    assert "test_check" in checker.list_checks()


def test_health_checker_duplicate_check() -> None:
    """Test that registering duplicate check raises error."""
    checker = HealthChecker()

    def check_fn() -> HealthChecker.HealthCheckResult:
        return HealthChecker.HealthCheckResult(
            check_name="test",
            status=HealthStatus.HEALTHY,
            message="OK",
        )

    checker.register_check("test_check", check_fn)

    with pytest.raises(ValueError, match="already registered"):
        checker.register_check("test_check", check_fn)


def test_health_checker_unregister_check() -> None:
    """Test unregistering a health check."""
    checker = HealthChecker()

    def check_fn() -> HealthChecker.HealthCheckResult:
        return HealthChecker.HealthCheckResult(
            check_name="test",
            status=HealthStatus.HEALTHY,
            message="OK",
        )

    checker.register_check("test_check", check_fn)
    assert "test_check" in checker.list_checks()

    checker.unregister_check("test_check")
    assert "test_check" not in checker.list_checks()


def test_health_checker_run_check() -> None:
    """Test running a specific health check."""
    checker = HealthChecker()

    def check_fn() -> HealthChecker.HealthCheckResult:
        return HealthChecker.HealthCheckResult(
            check_name="test",
            status=HealthStatus.HEALTHY,
            message="OK",
        )

    checker.register_check("test_check", check_fn)

    result = checker.run_check("test_check")

    assert result.check_name == "test_check"
    assert result.status == HealthStatus.HEALTHY


def test_health_checker_run_all_checks() -> None:
    """Test running all registered health checks."""
    checker = HealthChecker()

    def check_fn_healthy() -> HealthChecker.HealthCheckResult:
        return HealthChecker.HealthCheckResult(
            check_name="healthy_check",
            status=HealthStatus.HEALTHY,
            message="OK",
        )

    def check_fn_unhealthy() -> HealthChecker.HealthCheckResult:
        return HealthChecker.HealthCheckResult(
            check_name="unhealthy_check",
            status=HealthStatus.UNHEALTHY,
            message="Failed",
        )

    checker.register_check("check1", check_fn_healthy)
    checker.register_check("check2", check_fn_unhealthy)

    results = checker.run_all_checks()

    assert len(results) == 2
    assert results["check1"].status == HealthStatus.HEALTHY
    assert results["check2"].status == HealthStatus.UNHEALTHY


def test_health_checker_get_overall_health() -> None:
    """Test getting overall health status."""
    # Test with all healthy checks
    checker_healthy = HealthChecker()

    def check_healthy() -> HealthChecker.HealthCheckResult:
        return HealthChecker.HealthCheckResult(
            check_name="check",
            status=HealthStatus.HEALTHY,
            message="OK",
        )

    checker_healthy.register_check("check1", check_healthy)
    checker_healthy.register_check("check2", check_healthy)

    assert checker_healthy.get_overall_health() == HealthStatus.HEALTHY

    # Test with unhealthy checks (non-strict mode)
    checker_degraded = HealthChecker(strict_mode=False)

    def check_unhealthy() -> HealthChecker.HealthCheckResult:
        return HealthChecker.HealthCheckResult(
            check_name="check",
            status=HealthStatus.UNHEALTHY,
            message="Failed",
        )

    checker_degraded.register_check("check1", check_unhealthy)

    assert checker_degraded.get_overall_health() == HealthStatus.DEGRADED

    # Test with unhealthy checks (strict mode)
    checker_strict = HealthChecker(strict_mode=True)
    checker_strict.register_check("check1", check_unhealthy)

    assert checker_strict.get_overall_health() == HealthStatus.UNHEALTHY


def test_health_checker_get_healthy_checks() -> None:
    """Test getting list of healthy checks."""
    checker = HealthChecker()

    def check_healthy() -> HealthChecker.HealthCheckResult:
        return HealthChecker.HealthCheckResult(
            check_name="healthy",
            status=HealthStatus.HEALTHY,
            message="OK",
        )

    def check_unhealthy() -> HealthChecker.HealthCheckResult:
        return HealthChecker.HealthCheckResult(
            check_name="unhealthy",
            status=HealthStatus.UNHEALTHY,
            message="Failed",
        )

    checker.register_check("check1", check_healthy)
    checker.register_check("check2", check_unhealthy)
    checker.register_check("check3", check_healthy)

    healthy_checks = checker.get_healthy_checks()

    assert len(healthy_checks) == 2
    assert "check1" in healthy_checks
    assert "check3" in healthy_checks


def test_health_checker_get_unhealthy_checks() -> None:
    """Test getting list of unhealthy checks."""
    checker = HealthChecker()

    def check_healthy() -> HealthChecker.HealthCheckResult:
        return HealthChecker.HealthCheckResult(
            check_name="healthy",
            status=HealthStatus.HEALTHY,
            message="OK",
        )

    def check_unhealthy() -> HealthChecker.HealthCheckResult:
        return HealthChecker.HealthCheckResult(
            check_name="unhealthy",
            status=HealthStatus.UNHEALTHY,
            message="Failed",
        )

    checker.register_check("check1", check_healthy)
    checker.register_check("check2", check_unhealthy)

    unhealthy_checks = checker.get_unhealthy_checks()

    assert len(unhealthy_checks) == 1
    assert "check2" in unhealthy_checks


def test_health_checker_exception_handling() -> None:
    """Test that exceptions in checks are handled gracefully."""
    checker = HealthChecker()

    def check_exception() -> HealthChecker.HealthCheckResult:
        raise RuntimeError("Check failed")

    checker.register_check("exception_check", check_exception)

    result = checker.run_check("exception_check")

    assert result.status == HealthStatus.UNHEALTHY
    assert "exception" in result.message.lower()


def test_create_disk_space_check() -> None:
    """Test creating disk space health check."""
    import tempfile

    check_fn = create_disk_space_check(
        path=tempfile.gettempdir(),
        warning_threshold_mb=1000000,  # Very high to avoid triggering
        critical_threshold_mb=500000,  # Very high to avoid triggering
    )

    result = check_fn()

    assert result.check_name == "disk_space"
    # Should be healthy since we're using high thresholds
    assert result.status in (
        HealthStatus.HEALTHY,
        HealthStatus.DEGRADED,
    )
    assert "path" in result.details
    assert "free_mb" in result.details


def test_create_memory_check() -> None:
    """Test creating memory health check."""
    check_fn = create_memory_check(
        warning_threshold_mb=1000000,  # Very high to avoid triggering
        critical_threshold_mb=2000000,  # Very high to avoid triggering
    )

    result = check_fn()

    assert result.check_name == "memory"
    # Should be healthy or unknown (if psutil not installed)
    assert result.status in (
        HealthStatus.HEALTHY,
        HealthStatus.DEGRADED,
        HealthStatus.UNKNOWN,
    )


def test_health_check_result_timestamps() -> None:
    """Test that health check results have timestamps."""
    result = HealthChecker.HealthCheckResult(
        check_name="test",
        status=HealthStatus.HEALTHY,
        message="OK",
    )

    assert isinstance(result.timestamp, datetime)
    assert result.duration_ms >= 0
