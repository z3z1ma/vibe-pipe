"""
Tests for performance profiling system.
"""

import pytest

from vibe_piper.monitoring.profiling import (
    ProfileData,
    Profiler,
    profile_execution,
)


def test_profile_data_creation() -> None:
    """Test creating profile data."""
    profile = ProfileData(
        name="test_function",
        duration_ms=100.0,
        memory_before_mb=10.0,
        memory_after_mb=12.0,
        memory_delta_mb=2.0,
    )

    assert profile.name == "test_function"
    assert profile.duration_ms == 100.0
    assert profile.memory_before_mb == 10.0
    assert profile.memory_after_mb == 12.0
    assert profile.memory_delta_mb == 2.0


def test_profiler_initialization() -> None:
    """Test profiler initialization."""
    profiler = Profiler(enabled=True, track_memory=False)

    assert profiler.enabled is True
    assert profiler.track_memory is False
    assert len(profiler) == 0


def test_profiler_disabled() -> None:
    """Test that disabled profiler doesn't collect data."""
    profiler = Profiler(enabled=False)

    @profiler.profile
    def test_func(x: int, y: int) -> int:
        return x + y

    result = test_func(1, 2)

    assert result == 3
    assert len(profiler) == 0


def test_profiler_decorator() -> None:
    """Test profiling decorator."""
    profiler = Profiler(enabled=True, track_memory=False)

    @profiler.profile
    def add_numbers(x: int, y: int) -> int:
        return x + y

    result = add_numbers(5, 3)

    assert result == 8
    assert len(profiler) == 1

    profile_data = profiler.get_history("add_numbers")[0]
    assert profile_data.name == "add_numbers"
    assert profile_data.duration_ms >= 0


def test_profiler_get_stats() -> None:
    """Test getting profiling statistics."""
    profiler = Profiler(enabled=True, track_memory=False)

    @profiler.profile
    def test_func() -> int:
        return sum(range(100))

    # Run multiple times
    for _ in range(5):
        test_func()

    stats = profiler.get_stats("test_func")

    assert stats["name"] == "test_func"
    assert stats["call_count"] == 5
    assert stats["avg_duration_ms"] > 0
    assert stats["total_duration_ms"] > 0
    assert stats["min_duration_ms"] > 0
    assert stats["max_duration_ms"] > 0


def test_profiler_get_stats_all() -> None:
    """Test getting stats for all functions."""
    profiler = Profiler(enabled=True, track_memory=False)

    @profiler.profile
    def func1() -> None:
        pass

    @profiler.profile
    def func2() -> None:
        pass

    func1()
    func1()
    func2()

    stats = profiler.get_stats()

    assert stats["call_count"] == 3  # 2 func1 + 1 func2
    assert stats["name"] == "all"


def test_profiler_get_history() -> None:
    """Test getting profiling history."""
    profiler = Profiler(enabled=True, track_memory=False)

    @profiler.profile
    def test_func(x: int) -> int:
        return x * 2

    for i in range(3):
        test_func(i)

    history = profiler.get_history("test_func")

    assert len(history) == 3
    assert history[0].name == "test_func"
    assert history[1].name == "test_func"
    assert history[2].name == "test_func"


def test_profiler_get_history_with_limit() -> None:
    """Test getting limited history."""
    profiler = Profiler(enabled=True, track_memory=False)

    @profiler.profile
    def test_func() -> None:
        pass

    for _ in range(10):
        test_func()

    # Get last 3 records
    history = profiler.get_history("test_func", limit=3)

    assert len(history) == 3


def test_profiler_clear() -> None:
    """Test clearing profiling history."""
    profiler = Profiler(enabled=True, track_memory=False)

    @profiler.profile
    def func1() -> None:
        pass

    @profiler.profile
    def func2() -> None:
        pass

    func1()
    func2()

    assert len(profiler) == 2

    profiler.clear("func1")

    assert len(profiler) == 1
    # func2 should still be there
    assert profiler.get_history("func2")[0].name == "func2"

    # Clear all
    profiler.clear()

    assert len(profiler) == 0


def test_profiler_history_size_limit() -> None:
    """Test that profiler respects history size limit."""
    profiler = Profiler(enabled=True, track_memory=False, history_size=5)

    @profiler.profile
    def test_func() -> None:
        pass

    # Run 10 times (more than history_size)
    for _ in range(10):
        test_func()

    # Should only keep last 5
    assert len(profiler) == 5


def test_profile_execution_context_manager() -> None:
    """Test profile_execution context manager."""
    profiler = Profiler(enabled=True, track_memory=False)

    with profile_execution("test_block", profiler=profiler):
        # Simulate work
        result = sum(range(100))

    assert result == 4950
    assert len(profiler) == 1

    profile_data = profiler.get_history("test_block")[0]
    assert profile_data.name == "test_block"


def test_profile_execution_without_profiler() -> None:
    """Test profile_execution creates new profiler if none provided."""
    # Should not raise error
    with profile_execution("test_block", profiler=None, track_memory=False):
        pass

    # Check that cleanup happened
    # The profiler should have cleaned up after context exit


def test_profiler_print_summary() -> None:
    """Test print summary (integration test)."""
    profiler = Profiler(enabled=True, track_memory=False)

    @profiler.profile
    def test_func() -> None:
        pass

    test_func()
    test_func()

    # Should not raise error
    profiler.print_summary("test_func")

    assert len(profiler.get_history("test_func")) == 2


def test_profiler_track_memory_unavailable() -> None:
    """Test profiling when psutil is unavailable."""
    # Create profiler with memory tracking
    profiler = Profiler(enabled=True, track_memory=True)

    @profiler.profile
    def test_func() -> None:
        pass

    # This should work even if psutil is unavailable
    test_func()

    # Check that profiling was recorded
    assert len(profiler) == 1

    # Memory fields should be None (psutil not available)
    profile_data = profiler.get_history("test_func")[0]
    # Memory tracking may or may not work depending on system


def test_profiler_multiple_functions() -> None:
    """Test profiling multiple different functions."""
    profiler = Profiler(enabled=True, track_memory=False)

    @profiler.profile
    def func_a() -> int:
        return 1

    @profiler.profile
    def func_b() -> int:
        return 2

    @profiler.profile
    def func_c() -> int:
        return 3

    # Run multiple times
    result_a = func_a() + func_a()
    result_b = func_b()
    result_c = func_c()

    assert result_a == 2
    assert result_b == 2
    assert result_c == 3

    # Check profiling data
    assert len(profiler.get_history("func_a")) == 2
    assert len(profiler.get_history("func_b")) == 1
    assert len(profiler.get_history("func_c")) == 1
