"""
Core execution utilities shared between ExecutionEngine and OrchestrationEngine.

This module contains reusable functions for asset graph execution,
including ordering, metrics aggregation, and execution helpers.
"""

import time
from collections.abc import Mapping, Sequence
from datetime import datetime

from vibe_piper.types import AssetGraph, AssetResult, DataRecord


def get_execution_order_for_targets(graph: AssetGraph, targets: tuple[str, ...]) -> tuple[str, ...]:
    """
    Get the execution order for specific target assets and their dependencies.

    This function computes the minimal set of assets needed to execute
    the given targets, respecting all transitive dependencies.

    Args:
        graph: The asset graph
        targets: Target asset names to execute

    Returns:
        Tuple of asset names in topological execution order
    """
    # Get all dependencies for the target assets (recursively)
    to_execute: set[str] = set()
    for target in targets:
        to_execute.add(target)
        # Add all upstream dependencies
        deps = graph.get_dependencies(target)
        for dep in deps:
            to_execute.add(dep.name)
            # Recursively add dependencies of dependencies
            upstream = get_execution_order_for_targets(graph, (dep.name,))
            to_execute.update(upstream)

    # Get full topological order
    full_order = graph.topological_order()

    # Filter to only include assets we need to execute
    return tuple(asset for asset in full_order if asset in to_execute)


def aggregate_base_metrics(asset_results: Mapping[str, AssetResult]) -> dict[str, int | float]:
    """
    Aggregate base metrics from all asset results.

    This function provides the core metrics that should be included
    in any execution result.

    Args:
        asset_results: Mapping of asset name to result

    Returns:
        Dictionary of aggregated metrics
    """
    total_duration = sum(r.duration_ms for r in asset_results.values())

    # Count rows across all assets
    total_rows = 0
    for result in asset_results.values():
        if (
            result.data
            and isinstance(result.data, Sequence)
            and len(result.data) > 0
            and isinstance(result.data[0], DataRecord)
        ):
            total_rows += len(result.data)

    metrics = {
        "total_assets": len(asset_results),
        "total_duration_ms": total_duration,
        "avg_duration_ms": (total_duration / len(asset_results) if asset_results else 0),
        "total_rows": total_rows,
    }

    return metrics


def build_execution_result(
    asset_results: Mapping[str, AssetResult],
    start_time: float,
    timestamp: datetime,
    error_strategy_handling: str = "fail_fast",
    additional_metrics: dict[str, int | float] | None = None,
) -> tuple[bool, int, int, dict[str, int | float], list[str]]:
    """
    Build execution result summary from asset results.

    This is a shared helper for both ExecutionEngine and OrchestrationEngine
    to compute success/failure counts and aggregate metrics.

    Args:
        asset_results: Mapping of asset name to execution result
        start_time: Execution start time (from time.time())
        timestamp: Execution timestamp
        error_strategy_handling: How errors were handled
        additional_metrics: Additional metrics to merge (for orchestration features)

    Returns:
        Tuple of (overall_success, succeeded, failed, metrics, errors)
    """
    # Count successes and failures
    succeeded = sum(1 for r in asset_results.values() if r.success)
    failed = sum(1 for r in asset_results.values() if not r.success)

    # Collect error messages
    errors = [
        f"{name}: {result.error}" for name, result in asset_results.items() if not result.success
    ]

    # Determine overall success
    overall_success = failed == 0

    # Calculate duration
    duration_ms = (time.time() - start_time) * 1000

    # Aggregate base metrics
    metrics = aggregate_base_metrics(asset_results)

    # Add additional metrics if provided
    if additional_metrics:
        metrics.update(additional_metrics)

    # Add duration
    metrics["duration_ms"] = duration_ms

    return overall_success, succeeded, failed, metrics, errors
