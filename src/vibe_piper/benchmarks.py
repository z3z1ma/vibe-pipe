"""
Performance benchmarks for Vibe Piper.

This module provides:
- Benchmark utilities for measuring pipeline performance
- Before/after comparisons
- Statistics collection
"""

import logging
import time
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import datetime
from statistics import mean, median, stdev
from typing import Any, ParamSpec

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
# Benchmark Data Structures
# =============================================================================


@dataclass(frozen=True)
class BenchmarkResult:
    """
    Result of a single benchmark run.

    Attributes:
        name: Name of benchmarked function
        duration_ms: Duration in milliseconds
        timestamp: When the benchmark was run
        memory_mb: Memory usage in MB (if measured)
        metadata: Additional metadata
    """

    name: str
    duration_ms: float
    timestamp: datetime = field(default_factory=datetime.utcnow)
    memory_mb: float | None = None
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass
class BenchmarkStats:
    """
    Statistics from multiple benchmark runs.

    Attributes:
        name: Name of benchmarked function
        runs: List of individual run results
        min_ms: Minimum duration
        max_ms: Maximum duration
        mean_ms: Mean duration
        median_ms: Median duration
        stdev_ms: Standard deviation
        total_ms: Total duration across all runs
        memory_mb: Memory usage (if measured)
    """

    name: str
    runs: list[BenchmarkResult]
    min_ms: float
    max_ms: float
    mean_ms: float
    median_ms: float
    stdev_ms: float
    total_ms: float
    memory_mb: float | None = None


@dataclass
class ComparisonResult:
    """
    Result of comparing two benchmarks.

    Attributes:
        baseline_name: Name of baseline function
        optimized_name: Name of optimized function
        speedup: Speedup factor (optimized / baseline)
        improvement_pct: Improvement percentage
        baseline_stats: Statistics for baseline
        optimized_stats: Statistics for optimized
    """

    baseline_name: str
    optimized_name: str
    speedup: float
    improvement_pct: float
    baseline_stats: BenchmarkStats
    optimized_stats: BenchmarkStats


# =============================================================================
# Benchmark Runner
# =============================================================================


class BenchmarkRunner:
    """
    Runner for executing benchmarks and collecting statistics.

    Attributes:
        warmup_runs: Number of warmup runs before measuring
        measurement_runs: Number of measurement runs
        measure_memory: Whether to measure memory usage
    """

    def __init__(
        self,
        warmup_runs: int = 3,
        measurement_runs: int = 10,
        measure_memory: bool = False,
    ) -> None:
        """
        Initialize benchmark runner.

        Args:
            warmup_runs: Number of warmup runs before measuring
            measurement_runs: Number of measurement runs
            measure_memory: Whether to measure memory usage
        """
        self.warmup_runs = warmup_runs
        self.measurement_runs = measurement_runs
        self.measure_memory = measure_memory
        self._psutil_available = self._check_psutil()

    @staticmethod
    def _check_psutil() -> bool:
        """Check if psutil is available for memory tracking."""
        try:
            import importlib.util

            spec = importlib.util.find_spec("psutil")
            return spec is not None
        except Exception:
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

    def run(
        self, fn: Callable[P, T], name: str | None = None, *args: P.args, **kwargs: P.kwargs
    ) -> BenchmarkStats:
        """
        Run a benchmark on a function.

        Args:
            fn: Function to benchmark
            name: Optional name (defaults to function name)
            *args: Positional arguments to pass
            **kwargs: Keyword arguments to pass

        Returns:
            Benchmark statistics
        """
        func_name = name or fn.__name__

        # Warmup runs (not measured)
        for i in range(self.warmup_runs):
            fn(*args, **kwargs)

        # Measurement runs
        runs: list[BenchmarkResult] = []

        for i in range(self.measurement_runs):
            start_time = time.time()
            memory_before = self._get_memory_mb() if self.measure_memory else None

            # Run function
            fn(*args, **kwargs)

            end_time = time.time()
            memory_after = self._get_memory_mb() if self.measure_memory else None

            duration_ms = (end_time - start_time) * 1000
            memory_mb = None
            if self.measure_memory and memory_before is not None and memory_after is not None:
                memory_mb = memory_after - memory_before

            runs.append(
                BenchmarkResult(
                    name=func_name,
                    duration_ms=duration_ms,
                    memory_mb=memory_mb,
                )
            )

            logger.debug(f"Run {i + 1}/{self.measurement_runs}: {duration_ms:.2f}ms")

        # Calculate statistics
        durations = [r.duration_ms for r in runs]

        return BenchmarkStats(
            name=func_name,
            runs=runs,
            min_ms=min(durations),
            max_ms=max(durations),
            mean_ms=mean(durations),
            median_ms=median(durations),
            stdev_ms=stdev(durations) if len(durations) > 1 else 0,
            total_ms=sum(durations),
            memory_mb=runs[0].memory_mb if runs else None,
        )

    def compare(
        self,
        baseline_fn: Callable[P, Any],
        optimized_fn: Callable[P, Any],
        baseline_name: str | None = None,
        optimized_name: str | None = None,
        *args: P.args,
        **kwargs: P.kwargs,
    ) -> ComparisonResult:
        """
        Compare baseline and optimized implementations.

        Args:
            baseline_fn: Baseline function
            optimized_fn: Optimized function
            baseline_name: Optional name for baseline
            optimized_name: Optional name for optimized
            *args: Positional arguments to pass to both functions
            **kwargs: Keyword arguments to pass to both functions

        Returns:
            Comparison result with speedup metrics
        """
        # Run both benchmarks
        baseline_stats = self.run(baseline_fn, baseline_name or "baseline", *args, **kwargs)
        optimized_stats = self.run(optimized_fn, optimized_name or "optimized", *args, **kwargs)

        # Calculate speedup
        if optimized_stats.mean_ms > 0:
            speedup = baseline_stats.mean_ms / optimized_stats.mean_ms
            improvement_pct = (
                (baseline_stats.mean_ms - optimized_stats.mean_ms) / baseline_stats.mean_ms
            ) * 100
        else:
            speedup = 1.0
            improvement_pct = 0.0

        return ComparisonResult(
            baseline_name=baseline_stats.name,
            optimized_name=optimized_stats.name,
            speedup=speedup,
            improvement_pct=improvement_pct,
            baseline_stats=baseline_stats,
            optimized_stats=optimized_stats,
        )

    def print_results(self, stats: BenchmarkStats) -> None:
        """
        Print benchmark results in a formatted table.

        Args:
            stats: Benchmark statistics to print
        """
        print(f"\n{'=' * 70}")
        print(f"Benchmark Results: {stats.name}")
        print(f"{'=' * 70}")
        print(f"Runs:        {len(stats.runs)}")
        print(f"Min:         {stats.min_ms:.3f}ms")
        print(f"Max:         {stats.max_ms:.3f}ms")
        print(f"Mean:        {stats.mean_ms:.3f}ms")
        print(f"Median:      {stats.median_ms:.3f}ms")
        print(f"Std Dev:     {stats.stdev_ms:.3f}ms")
        print(f"Total:       {stats.total_ms:.3f}ms")

        if stats.memory_mb is not None:
            print(f"Memory:      {stats.memory_mb:.2f}MB")

        print(f"{'=' * 70}\n")

    def print_comparison(self, result: ComparisonResult) -> None:
        """
        Print comparison results in a formatted table.

        Args:
            result: Comparison result to print
        """
        print(f"\n{'=' * 70}")
        print("Benchmark Comparison")
        print(f"{'=' * 70}")
        print(f"\nBaseline:  {result.baseline_name}")
        self.print_results(result.baseline_stats)

        print(f"\nOptimized: {result.optimized_name}")
        self.print_results(result.optimized_stats)

        print(f"\n{'=' * 70}")
        print(f"Speedup:    {result.speedup:.2f}x")
        print(f"Improvement: {result.improvement_pct:.1f}%")
        print(f"{'=' * 70}\n")


# =============================================================================
# Decorators
# =============================================================================


def benchmark(
    runner: BenchmarkRunner | None = None,
    name: str | None = None,
):
    """
    Decorator to benchmark a function.

    Args:
        runner: BenchmarkRunner to use (creates new one if None)
        name: Optional name for benchmark

    Returns:
        Decorator function

    Example:
        Benchmark a function::

            runner = BenchmarkRunner(warmup_runs=3, measurement_runs=10)

            @benchmark(runner)
            def my_function(data):
                return transform(data)

            # Function will be benchmarked when called
            result = my_function(data)
    """

    def decorator(fn):
        benchmark_runner = runner or BenchmarkRunner()

        def wrapper(*args, **kwargs):
            # Run benchmark
            stats = benchmark_runner.run(fn, name, *args, **kwargs)

            # Print results
            benchmark_runner.print_results(stats)

            # Return function result from last run
            return fn(*args, **kwargs)

        return wrapper

    return decorator


def compare_benchmarks(
    baseline_name: str,
    optimized_name: str,
    runner: BenchmarkRunner | None = None,
):
    """
    Decorator to compare two functions.

    Args:
        baseline_name: Name for baseline function
        optimized_name: Name for optimized function
        runner: BenchmarkRunner to use (creates new one if None)

    Returns:
        Decorator function

    Example:
        Compare two functions::

            runner = BenchmarkRunner(warmup_runs=3, measurement_runs=10)

            @compare_benchmarks("baseline", "optimized", runner)
            def compare_functions():
                return {
                    "baseline": baseline_fn(data),
                    "optimized": optimized_fn(data),
                }

            # Both functions will be benchmarked and compared
            result = compare_functions()
    """

    def decorator(fn):
        benchmark_runner = runner or BenchmarkRunner()

        def wrapper(*args, **kwargs):
            # Get results from function
            results = fn(*args, **kwargs)

            # Run comparison
            comparison = benchmark_runner.compare(
                results["baseline"],
                results["optimized"],
                baseline_name,
                optimized_name,
                *args,
                **kwargs,
            )

            # Print comparison
            benchmark_runner.print_comparison(comparison)

            return results

        return wrapper

    return decorator
