"""
Health check system for Vibe Piper.

This module provides health check capabilities for monitoring:
- Pipeline health
- Asset health
- System health
- Custom health checks
"""

import logging
import time
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum, auto
from typing import Any

# =============================================================================
# Logger
# =============================================================================

logger = logging.getLogger(__name__)

# =============================================================================
# Health Status
# =============================================================================


class HealthStatus(Enum):
    """Health status indicators."""

    HEALTHY = auto()  # Everything is working correctly
    DEGRADED = auto()  # Working but with some issues
    UNHEALTHY = auto()  # Not working correctly
    UNKNOWN = auto()  # Unable to determine health


# =============================================================================
# Health Check Result
# =============================================================================


@dataclass(frozen=True)
class HealthCheckResult:
    """
    Result of a health check.

    Attributes:
        check_name: Name of the health check
        status: Health status
        message: Human-readable message
        details: Additional details about the check
        timestamp: When the check was performed
        duration_ms: Time taken to perform the check
    """

    check_name: str
    status: HealthStatus
    message: str
    details: Mapping[str, Any] = field(default_factory=dict)
    timestamp: datetime = field(default_factory=datetime.utcnow)
    duration_ms: float = 0.0

    def is_healthy(self) -> bool:
        """Check if the result indicates a healthy state."""
        return self.status == HealthStatus.HEALTHY

    def is_degraded(self) -> bool:
        """Check if the result indicates a degraded state."""
        return self.status == HealthStatus.DEGRADED

    def is_unhealthy(self) -> bool:
        """Check if the result indicates an unhealthy state."""
        return self.status in (HealthStatus.UNHEALTHY, HealthStatus.UNKNOWN)


# =============================================================================
# Health Check Function
# =============================================================================

HealthCheckFn = Callable[[], HealthCheckResult]


# =============================================================================
# Health Checker
# =============================================================================


class HealthChecker:
    """
    Health checker for monitoring system and pipeline health.

    This class provides a framework for defining and running health checks
    on various components of the system.

    Attributes:
        check_timeout: Maximum time to wait for each check (in seconds)
        strict_mode: If True, any unhealthy check marks the overall status as unhealthy

    Example:
        Define and run health checks::

            checker = HealthChecker()

            # Define a database health check
            def check_db() -> HealthCheckResult:
                try:
                    db.connect()
                    return HealthCheckResult(
                        check_name="database",
                        status=HealthStatus.HEALTHY,
                        message="Database is accessible"
                    )
                except Exception as e:
                    return HealthCheckResult(
                        check_name="database",
                        status=HealthStatus.UNHEALTHY,
                        message=f"Database error: {e}"
                    )

            checker.register_check("database", check_db)
            results = checker.run_all_checks()
    """

    def __init__(
        self,
        check_timeout: float = 10.0,
        strict_mode: bool = False,
    ) -> None:
        """
        Initialize the health checker.

        Args:
            check_timeout: Maximum time to wait for each check (in seconds)
            strict_mode: If True, any unhealthy check marks overall status as unhealthy
        """
        self.check_timeout = check_timeout
        self.strict_mode = strict_mode
        self._checks: dict[str, HealthCheckFn] = {}

    def register_check(self, name: str, check_fn: HealthCheckFn) -> None:
        """
        Register a health check.

        Args:
            name: Unique name for the check
            check_fn: Function that performs the health check

        Raises:
            ValueError: If a check with the same name already exists
        """
        if name in self._checks:
            msg = f"Health check '{name}' is already registered"
            raise ValueError(msg)

        self._checks[name] = check_fn
        logger.debug(f"Registered health check: {name}")

    def unregister_check(self, name: str) -> None:
        """
        Unregister a health check.

        Args:
            name: Name of the check to unregister
        """
        if name in self._checks:
            del self._checks[name]
            logger.debug(f"Unregistered health check: {name}")

    def run_check(self, name: str) -> HealthCheckResult:
        """
        Run a specific health check.

        Args:
            name: Name of the check to run

        Returns:
            HealthCheckResult from the check

        Raises:
            ValueError: If the check is not found
        """
        if name not in self._checks:
            msg = f"Health check '{name}' not found"
            raise ValueError(msg)

        check_fn = self._checks[name]
        start_time = time.time()

        try:
            result = check_fn()
            duration_ms = (time.time() - start_time) * 1000

            # Ensure result has the correct check name
            return HealthCheckResult(
                check_name=name,
                status=result.status,
                message=result.message,
                details=result.details,
                timestamp=result.timestamp,
                duration_ms=duration_ms,
            )
        except Exception as e:
            duration_ms = (time.time() - start_time) * 1000
            logger.error(f"Health check '{name}' raised exception: {e}", exc_info=True)

            return HealthCheckResult(
                check_name=name,
                status=HealthStatus.UNHEALTHY,
                message=f"Health check raised exception: {e}",
                details={"exception_type": type(e).__name__},
                duration_ms=duration_ms,
            )

    def run_all_checks(self) -> dict[str, HealthCheckResult]:
        """
        Run all registered health checks.

        Returns:
            Dictionary mapping check names to their results
        """
        results: dict[str, HealthCheckResult] = {}

        for check_name in self._checks:
            results[check_name] = self.run_check(check_name)

        return results

    def get_overall_health(self) -> HealthStatus:
        """
        Get the overall health status based on all checks.

        Returns:
            Overall HealthStatus

        Rules:
        - If all checks are HEALTHY, return HEALTHY
        - If any check is UNHEALTHY or UNKNOWN, return UNHEALTHY (strict mode) or DEGRADED (non-strict)
        - If any check is DEGRADED, return DEGRADED
        - Otherwise, return HEALTHY
        """
        results = self.run_all_checks()

        if not results:
            return HealthStatus.UNKNOWN

        statuses = {r.status for r in results.values()}

        if len(statuses) == 1 and HealthStatus.HEALTHY in statuses:
            return HealthStatus.HEALTHY

        if HealthStatus.UNHEALTHY in statuses or HealthStatus.UNKNOWN in statuses:
            return HealthStatus.UNHEALTHY if self.strict_mode else HealthStatus.DEGRADED

        if HealthStatus.DEGRADED in statuses:
            return HealthStatus.DEGRADED

        return HealthStatus.HEALTHY

    def get_healthy_checks(self) -> Sequence[str]:
        """
        Get names of all healthy checks.

        Returns:
            Sequence of check names that are healthy
        """
        results = self.run_all_checks()
        return [name for name, result in results.items() if result.is_healthy()]

    def get_unhealthy_checks(self) -> Sequence[str]:
        """
        Get names of all unhealthy checks.

        Returns:
            Sequence of check names that are unhealthy or unknown
        """
        results = self.run_all_checks()
        return [name for name, result in results.items() if result.is_unhealthy()]

    def list_checks(self) -> Sequence[str]:
        """
        List all registered health checks.

        Returns:
            Sequence of check names
        """
        return list(self._checks.keys())


# =============================================================================
# Predefined Health Checks
# =============================================================================


def create_disk_space_check(
    path: str,
    warning_threshold_mb: int = 1000,
    critical_threshold_mb: int = 500,
) -> HealthCheckFn:
    """
    Create a disk space health check.

    Args:
        path: Path to check disk space for
        warning_threshold_mb: Threshold for DEGRADED status (in MB)
        critical_threshold_mb: Threshold for UNHEALTHY status (in MB)

    Returns:
        Health check function
    """

    def check_disk_space() -> HealthCheckResult:
        try:
            import shutil

            usage = shutil.disk_usage(path)
            free_mb = float(usage.free / (1024 * 1024))

            if free_mb < critical_threshold_mb:
                return HealthCheckResult(
                    check_name="disk_space",
                    status=HealthStatus.UNHEALTHY,
                    message=f"Low disk space: {free_mb:.0f}MB free (critical: {critical_threshold_mb}MB)",
                    details={
                        "path": path,
                        "free_mb": free_mb,
                        "total_mb": float(usage.total / (1024 * 1024)),
                    },
                )
            elif free_mb < warning_threshold_mb:
                return HealthCheckResult(
                    check_name="disk_space",
                    status=HealthStatus.DEGRADED,
                    message=f"Low disk space: {free_mb:.0f}MB free (warning: {warning_threshold_mb}MB)",
                    details={
                        "path": path,
                        "free_mb": free_mb,
                        "total_mb": float(usage.total / (1024 * 1024)),
                    },
                )
            else:
                return HealthCheckResult(
                    check_name="disk_space",
                    status=HealthStatus.HEALTHY,
                    message=f"Disk space OK: {free_mb:.0f}MB free",
                    details={
                        "path": path,
                        "free_mb": free_mb,
                        "total_mb": float(usage.total / (1024 * 1024)),
                    },
                )
        except Exception as e:
            return HealthCheckResult(
                check_name="disk_space",
                status=HealthStatus.UNHEALTHY,
                message=f"Failed to check disk space: {e}",
            )

    return check_disk_space


def create_memory_check(
    warning_threshold_mb: int = 1000,
    critical_threshold_mb: int = 2000,
) -> HealthCheckFn:
    """
    Create a memory usage health check.

    Args:
        warning_threshold_mb: Threshold for DEGRADED status (in MB)
        critical_threshold_mb: Threshold for UNHEALTHY status (in MB)

    Returns:
        Health check function
    """

    def check_memory() -> HealthCheckResult:
        try:
            import psutil  # type: ignore[import-untyped]

            mem = psutil.virtual_memory()
            used_mb = float(mem.used / (1024 * 1024))

            if used_mb > critical_threshold_mb:
                return HealthCheckResult(
                    check_name="memory",
                    status=HealthStatus.UNHEALTHY,
                    message=f"High memory usage: {used_mb:.0f}MB (critical: {critical_threshold_mb}MB)",
                    details={
                        "used_mb": used_mb,
                        "total_mb": float(mem.total / (1024 * 1024)),
                        "percent": mem.percent,
                    },
                )
            elif used_mb > warning_threshold_mb:
                return HealthCheckResult(
                    check_name="memory",
                    status=HealthStatus.DEGRADED,
                    message=f"High memory usage: {used_mb:.0f}MB (warning: {warning_threshold_mb}MB)",
                    details={
                        "used_mb": used_mb,
                        "total_mb": float(mem.total / (1024 * 1024)),
                        "percent": mem.percent,
                    },
                )
            else:
                return HealthCheckResult(
                    check_name="memory",
                    status=HealthStatus.HEALTHY,
                    message=f"Memory OK: {used_mb:.0f}MB used ({mem.percent:.1f}%)",
                    details={
                        "used_mb": used_mb,
                        "total_mb": float(mem.total / (1024 * 1024)),
                        "percent": mem.percent,
                    },
                )
        except ImportError:
            return HealthCheckResult(
                check_name="memory",
                status=HealthStatus.UNKNOWN,
                message="psutil not installed, cannot check memory usage",
            )
        except Exception as e:
            return HealthCheckResult(
                check_name="memory",
                status=HealthStatus.UNHEALTHY,
                message=f"Failed to check memory: {e}",
            )

    return check_memory
