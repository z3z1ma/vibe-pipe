"""
Execution engine for Vibe Piper.

This module provides the core execution engine that orchestrates
the execution of asset graphs with support for dependencies,
error handling, and observability.
"""

import time
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any

from vibe_piper.io_managers import get_io_manager
from vibe_piper.types import (
    Asset,
    AssetGraph,
    AssetResult,
    DataRecord,
    ErrorStrategy,
    ExecutionResult,
    Executor,
    PipelineContext,
)

# =============================================================================
# Default Executor Implementation
# =============================================================================


class DefaultExecutor:
    """
    Default executor implementation for assets.

    This executor handles basic asset execution by delegating to
    asset-specific logic based on the asset type.
    """

    def execute(
        self,
        asset: Asset,
        context: PipelineContext,
        upstream_results: Mapping[str, Any],
    ) -> AssetResult:
        """
        Execute an asset and return the result.

        Args:
            asset: The asset to execute
            context: The pipeline execution context
            upstream_results: Results from upstream assets

        Returns:
            AssetResult containing execution outcome
        """
        start_time = time.time()

        try:
            # Check if asset has an operator to execute
            if asset.operator:
                # Get upstream data from dependencies
                # If there are upstream dependencies, pass the first one's data
                # This is a simplification - real implementation might merge data
                upstream_data = None
                if upstream_results:
                    # Get the first upstream result's data
                    first_upstream = list(upstream_results.values())[0]
                    if hasattr(first_upstream, "data"):
                        upstream_data = first_upstream.data

                # Execute the operator's function with upstream data and context
                result_data = asset.operator.fn(upstream_data, context)

                # Materialize data using IO manager
                io_manager_name = asset.io_manager or "memory"
                io_manager = get_io_manager(io_manager_name)

                # Create a modified context for the IO manager
                # Use asset name as pipeline_id for proper isolation
                io_context = PipelineContext(
                    pipeline_id=asset.name,
                    run_id=context.run_id,
                    config=context.config,
                    state=context.state,
                    metadata=context.metadata,
                )

                # Store the output data
                io_manager.handle_output(io_context, result_data)

                # Collect quality metrics if output is a list of DataRecords
                metrics = self._collect_quality_metrics(result_data)

                return AssetResult(
                    asset_name=asset.name,
                    success=True,
                    data=result_data,
                    metrics=metrics,
                    duration_ms=(time.time() - start_time) * 1000,
                    timestamp=datetime.now(),
                    lineage=tuple(upstream_results.keys()),
                )
            else:
                # No operator, just return success with no data
                return AssetResult(
                    asset_name=asset.name,
                    success=True,
                    data=None,
                    duration_ms=(time.time() - start_time) * 1000,
                    timestamp=datetime.now(),
                    lineage=tuple(upstream_results.keys()),
                )

        except Exception as e:
            return AssetResult(
                asset_name=asset.name,
                success=False,
                error=str(e),
                duration_ms=(time.time() - start_time) * 1000,
                timestamp=datetime.now(),
                lineage=tuple(upstream_results.keys()),
            )

    def _collect_quality_metrics(self, result_data: Any) -> Mapping[str, int | float]:
        """
        Collect quality metrics from asset execution result.

        Args:
            result_data: The output data from execution

        Returns:
            Mapping of metric names to values
        """
        metrics: dict[str, int | float] = {}

        # Check if result is a list of DataRecords
        if (
            isinstance(result_data, Sequence)
            and result_data
            and isinstance(result_data[0], DataRecord)
        ):
            # Add row count
            metrics["row_count"] = len(result_data)

            # Add schema information
            if result_data[0].schema:
                metrics["field_count"] = len(result_data[0].schema.fields)

        return metrics


# =============================================================================
# Execution Engine
# =============================================================================


@dataclass
class ExecutionEngine:
    """
    Engine for executing asset graphs.

    The ExecutionEngine orchestrates the execution of assets in an
    AssetGraph, respecting dependencies and handling errors according
    to the configured strategy.

    Attributes:
        executor: The executor to use for running assets
        error_strategy: How to handle execution errors
        max_retries: Maximum number of retries for failed assets (only used with RETRY strategy)

    Example:
        Execute a simple asset graph::

            engine = ExecutionEngine()
            result = engine.execute(my_graph)

            if result.success:
                print(f"Executed {result.assets_executed} assets successfully")
            else:
                print(f"Failed assets: {result.get_failed_assets()}")
    """

    executor: Executor = field(default_factory=DefaultExecutor)
    error_strategy: ErrorStrategy = ErrorStrategy.FAIL_FAST
    max_retries: int = 3

    def execute(
        self,
        graph: AssetGraph,
        target_assets: tuple[str, ...] | None = None,
        context: PipelineContext | None = None,
    ) -> ExecutionResult:
        """
        Execute an asset graph.

        Args:
            graph: The asset graph to execute
            target_assets: Optional specific assets to execute (and their dependencies).
                         If None, all assets in the graph are executed.
            context: Optional pipeline context. If None, creates a new context.

        Returns:
            ExecutionResult with details of the execution
        """
        start_time = time.time()
        timestamp = datetime.now()

        # Create or use provided context
        if context is None:
            import uuid

            context = PipelineContext(pipeline_id=graph.name, run_id=str(uuid.uuid4()))

        # Determine execution order
        if target_assets:
            execution_order = self._get_execution_order_for_targets(
                graph, target_assets
            )
        else:
            execution_order = graph.topological_order()

        # Execute assets
        asset_results: dict[str, AssetResult] = {}
        errors: list[str] = []
        succeeded = 0
        failed = 0
        retry_counts: dict[str, int] = {}

        for asset_name in execution_order:
            asset = graph.get_asset(asset_name)
            if asset is None:
                errors.append(f"Asset {asset_name!r} not found in graph")
                failed += 1
                continue

            # Get upstream results for this asset
            dependencies = graph.get_dependencies(asset_name)
            upstream_results = {
                dep.name: asset_results.get(dep.name, None)
                for dep in dependencies
                if dep.name in asset_results
            }

            # Execute the asset
            result = self._execute_asset_with_retry(
                asset, context, upstream_results, retry_counts
            )

            asset_results[asset_name] = result

            if result.success:
                succeeded += 1
            else:
                failed += 1
                errors.append(f"{asset_name}: {result.error}")

                # Handle error based on strategy
                if self.error_strategy == ErrorStrategy.FAIL_FAST:
                    # Stop execution immediately
                    break
                elif self.error_strategy == ErrorStrategy.CONTINUE:
                    # Continue with next asset
                    continue
                elif self.error_strategy == ErrorStrategy.RETRY:
                    # Retry was already attempted by _execute_asset_with_retry
                    # If we're here, retry failed - stop execution
                    break

        # Calculate overall success
        overall_success = failed == 0

        # Calculate duration
        duration_ms = (time.time() - start_time) * 1000

        # Aggregate metrics
        metrics = self._aggregate_metrics(asset_results)

        return ExecutionResult(
            success=overall_success,
            asset_results=asset_results,
            errors=tuple(errors),
            metrics=metrics,
            duration_ms=duration_ms,
            timestamp=timestamp,
            assets_executed=len(asset_results),
            assets_succeeded=succeeded,
            assets_failed=failed,
        )

    def _get_execution_order_for_targets(
        self, graph: AssetGraph, targets: tuple[str, ...]
    ) -> tuple[str, ...]:
        """
        Get the execution order for specific target assets and their dependencies.

        Args:
            graph: The asset graph
            targets: Target asset names

        Returns:
            Tuple of asset names in execution order
        """
        # Get all dependencies for the target assets (recursively)
        to_execute = set()
        for target in targets:
            to_execute.add(target)
            # Add all upstream dependencies
            deps = graph.get_dependencies(target)
            for dep in deps:
                to_execute.add(dep.name)
                # Recursively add dependencies of dependencies
                upstream = self._get_execution_order_for_targets(graph, (dep.name,))
                to_execute.update(upstream)

        # Get full topological order
        full_order = graph.topological_order()

        # Filter to only include assets we need to execute
        return tuple(asset for asset in full_order if asset in to_execute)

    def _execute_asset_with_retry(
        self,
        asset: Asset,
        context: PipelineContext,
        upstream_results: Mapping[str, Any],
        retry_counts: dict[str, int],
    ) -> AssetResult:
        """
        Execute an asset with retry logic if configured.

        Args:
            asset: The asset to execute
            context: Pipeline context
            upstream_results: Results from upstream assets
            retry_counts: Dictionary tracking retry counts per asset

        Returns:
            AssetResult from the execution attempt
        """
        result = self.executor.execute(asset, context, upstream_results)

        # If retry strategy is enabled and execution failed
        if self.error_strategy == ErrorStrategy.RETRY and not result.success:
            retry_count = retry_counts.get(asset.name, 0)

            if retry_count < self.max_retries:
                retry_counts[asset.name] = retry_count + 1
                # Retry the asset
                return self._execute_asset_with_retry(
                    asset, context, upstream_results, retry_counts
                )

        return result

    def _aggregate_metrics(
        self, asset_results: Mapping[str, AssetResult]
    ) -> Mapping[str, int | float]:
        """
        Aggregate metrics from all asset results.

        Args:
            asset_results: Mapping of asset name to result

        Returns:
            Aggregated metrics
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
            "avg_duration_ms": (
                total_duration / len(asset_results) if asset_results else 0
            ),
            "total_rows": total_rows,
        }

        return metrics
