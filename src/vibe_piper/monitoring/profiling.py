"""
Performance profiling tools for Vibe Piper.

This module provides profiling capabilities for:
- Function execution timing
- Memory usage tracking
- CPU usage monitoring
- Custom performance metrics
"""

import logging
import time
from collections.abc import Callable
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, ParamSpec, TypeVar

# =============================================================================
# Logger
# =============================================================================

logger = logging.getLogger(__name__)

# =============================================================================
# Type Variables
# =============================================================================

P = ParamSpec("P")
T = TypeVar("T")

# =============================================================================
# Profiling Data
# =============================================================================


@dataclass(frozen=True)
class ProfileData:
    """
    Profiling data for a function or code block.

    Attributes:
        name: Name of the function or block
        duration_ms: Execution duration in milliseconds
        timestamp: When the profiling was done
        memory_before_mb: Memory usage before (if available)
        memory_after_mb: Memory usage after (if available)
        memory_delta_mb: Memory change during execution (if available)
        extra: Additional profiling metrics
    """

    name: str
    duration_ms: float
    timestamp: datetime = field(default_factory=datetime.utcnow)
    memory_before_mb: float | None = None
    memory_after_mb: float | None = None
    memory_delta_mb: float | None = None
    extra: dict[str, Any] = field(default_factory=dict)


# =============================================================================
# Profiler
# =============================================================================


class Profiler:
    """
    Performance profiler for tracking execution metrics.

    This class provides tools for profiling function execution and
    tracking performance metrics over time.

    Attributes:
        enabled: Whether profiling is enabled
        track_memory: Whether to track memory usage (requires psutil)
        history_size: Maximum number of profile records to keep

    Example:
        Profile a function::

            profiler = Profiler(track_memory=True)

            @profiler.profile
            def my_function():
                # ... do work ...
                pass

            my_function()
            print(profiler.get_stats())
    """

    def __init__(
        self,
        enabled: bool = True,
        track_memory: bool = False,
        history_size: int = 1000,
    ) -> None:
        """
        Initialize the profiler.

        Args:
            enabled: Whether profiling is enabled
            track_memory: Whether to track memory usage (requires psutil)
            history_size: Maximum number of profile records to keep
        """
        self.enabled = enabled
        self.track_memory = track_memory
        self.history_size = history_size
        self._profile_history: list[ProfileData] = []
        self._psutil_available = self._check_psutil()

    def _check_psutil(self) -> bool:
        """Check if psutil is available for memory tracking."""
        try:
            import importlib.util

            spec = importlib.util.find_spec("psutil")
            return spec is not None
        except Exception:
            if self.track_memory:
                logger.warning(
                    "psutil not installed, memory tracking will be disabled. "
                    "Install with: pip install psutil"
                )
            return False

    def _get_memory_mb(self) -> float | None:
        """Get current memory usage in MB."""
        if not self._psutil_available:
            return None

        try:
            import psutil

            process = psutil.Process()
            return float(process.memory_info().rss / (1024 * 1024))
        except Exception:
            return None

    def profile(self, func: Callable[P, T]) -> Callable[P, T]:
        """
        Decorator to profile a function.

        Args:
            func: Function to profile

        Returns:
            Wrapped function that collects profiling data

        Example:
            Use as a decorator::

                @profiler.profile
                def my_function(x, y):
                    return x + y
        """

        def wrapper(*args: P.args, **kwargs: P.kwargs) -> T:
            if not self.enabled:
                return func(*args, **kwargs)

            name = func.__name__
            start_time = time.time()
            memory_before = self._get_memory_mb() if self.track_memory else None

            try:
                result = func(*args, **kwargs)
                return result
            finally:
                duration_ms = (time.time() - start_time) * 1000
                memory_after = self._get_memory_mb() if self.track_memory else None
                memory_delta = (
                    memory_after - memory_before
                    if memory_before is not None and memory_after is not None
                    else None
                )

                profile_data = ProfileData(
                    name=name,
                    duration_ms=duration_ms,
                    memory_before_mb=memory_before,
                    memory_after_mb=memory_after,
                    memory_delta_mb=memory_delta,
                )

                self._add_profile_data(profile_data)

        return wrapper

    def _add_profile_data(self, data: ProfileData) -> None:
        """
        Add profiling data to history, respecting history size limit.

        Args:
            data: Profile data to add
        """
        self._profile_history.append(data)
        if len(self._profile_history) > self.history_size:
            self._profile_history.pop(0)

    def get_stats(self, name: str | None = None) -> dict[str, Any]:
        """
        Get statistics for profiled functions.

        Args:
            name: Optional function name to filter by. If None, returns stats for all.

        Returns:
            Dictionary with statistics

        Example:
            Get profiling statistics::

                stats = profiler.get_stats("my_function")
                print(f"Average duration: {stats['avg_duration_ms']:.2f}ms")
        """
        filtered = (
            [p for p in self._profile_history if p.name == name] if name else self._profile_history
        )

        if not filtered:
            return {}

        durations = [p.duration_ms for p in filtered]
        avg_duration = sum(durations) / len(durations)
        min_duration = min(durations)
        max_duration = max(durations)

        stats = {
            "name": name if name else "all",
            "call_count": len(filtered),
            "total_duration_ms": sum(durations),
            "avg_duration_ms": avg_duration,
            "min_duration_ms": min_duration,
            "max_duration_ms": max_duration,
        }

        # Add memory stats if available
        memory_deltas = [p.memory_delta_mb for p in filtered if p.memory_delta_mb is not None]
        if memory_deltas:
            stats["avg_memory_delta_mb"] = sum(memory_deltas) / len(memory_deltas)
            stats["total_memory_delta_mb"] = sum(memory_deltas)

        return stats

    def get_history(self, name: str | None = None, limit: int = 100) -> list[ProfileData]:
        """
        Get profiling history.

        Args:
            name: Optional function name to filter by
            limit: Maximum number of records to return

        Returns:
            List of ProfileData records
        """
        filtered = (
            [p for p in self._profile_history if p.name == name] if name else self._profile_history
        )
        return filtered[-limit:] if limit else filtered

    def clear(self, name: str | None = None) -> None:
        """
        Clear profiling history.

        Args:
            name: Optional function name to clear. If None, clears all.
        """
        if name:
            self._profile_history = [p for p in self._profile_history if p.name != name]
        else:
            self._profile_history.clear()

    def print_summary(self, name: str | None = None) -> None:
        """
        Print a human-readable summary of profiling data.

        Args:
            name: Optional function name to summarize. If None, summarizes all.
        """
        stats = self.get_stats(name)
        if not stats:
            print("No profiling data available")
            return

        print(f"\n{'=' * 60}")
        print(f"Profiling Summary: {stats['name']}")
        print(f"{'=' * 60}")
        print(f"Call count:        {stats['call_count']}")
        print(f"Total duration:    {stats['total_duration_ms']:.2f}ms")
        print(f"Average duration:  {stats['avg_duration_ms']:.2f}ms")
        print(f"Min duration:      {stats['min_duration_ms']:.2f}ms")
        print(f"Max duration:      {stats['max_duration_ms']:.2f}ms")

        if "avg_memory_delta_mb" in stats:
            print(f"Total memory delta: {stats['total_memory_delta_mb']:.2f}MB")
            print(f"Avg memory delta:   {stats['avg_memory_delta_mb']:.2f}MB")

        print(f"{'=' * 60}\n")


# =============================================================================
# Context Manager for Profiling
# =============================================================================


@contextmanager
def profile_execution(
    name: str,
    profiler: Profiler | None = None,
    track_memory: bool = False,
) -> Any:
    """
    Context manager for profiling a code block.

    Args:
        name: Name to identify the profiled block
        profiler: Optional Profiler instance to use. If None, creates a new one.
        track_memory: Whether to track memory usage

    Yields:
        ProfileData with the profiling results

    Example:
        Profile a code block::

            with profile_execution("data_processing", track_memory=True) as data:
                # ... do work ...
                result = process_data(data)

            print(f"Duration: {data.duration_ms}ms")
    """
    if profiler is None:
        profiler = Profiler(enabled=True, track_memory=track_memory)
        _cleanup = True
    else:
        _cleanup = False

    start_time = time.time()
    memory_before = profiler._get_memory_mb() if profiler.track_memory else None

    try:
        yield None
    finally:
        duration_ms = (time.time() - start_time) * 1000
        memory_after = profiler._get_memory_mb() if profiler.track_memory else None
        memory_delta = (
            memory_after - memory_before
            if memory_before is not None and memory_after is not None
            else None
        )

        profile_data = ProfileData(
            name=name,
            duration_ms=duration_ms,
            memory_before_mb=memory_before,
            memory_after_mb=memory_after,
            memory_delta_mb=memory_delta,
        )

        profiler._add_profile_data(profile_data)

        if _cleanup:
            profiler.print_summary(name)
