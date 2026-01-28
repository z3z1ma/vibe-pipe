"""
Execution engine for Vibe Piper.

This module provides the core execution engine that orchestrates
the execution of asset graphs with support for dependencies,
error handling, checkpointing, and observability.
"""

import logging
import time
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any

from vibe_piper.error_handling import (
    CheckpointManager,
    CheckpointState,
    capture_error_context,
)
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
# Logger
# =============================================================================

logger = logging.getLogger(__name__)

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
    to the configured strategy. Supports checkpointing for recovery
    and enhanced error context capture.

    Attributes:
        executor: The executor to use for running assets
        error_strategy: How to handle execution errors
        max_retries: Maximum number of retries for failed assets (only used with RETRY strategy)
        checkpoint_manager: Optional checkpoint manager for recovery
        enable_checkpoints: Whether to enable checkpointing
        capture_error_context: Whether to capture detailed error context

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
    checkpoint_manager: CheckpointManager = field(default_factory=CheckpointManager)
    enable_checkpoints: bool = True
    capture_error_context: bool = True

    def execute(
        self,
        graph: AssetGraph,
        target_assets: tuple[str, ...] | None = None,
        context: PipelineContext | None = None,
        recover_from_checkpoint: bool = False,
    ) -> ExecutionResult:
        """
        Execute an asset graph.

        Args:
            graph: The asset graph to execute
            target_assets: Optional specific assets to execute (and their dependencies).
                         If None, all assets in the graph are executed.
            context: Optional pipeline context. If None, creates a new context.
            recover_from_checkpoint: Whether to attempt recovery from checkpoints

        Returns:
            ExecutionResult with details of the execution
        """
        start_time = time.time()
        timestamp = datetime.now()

        # Create or use provided context
        if context is None:
            import uuid

            context = PipelineContext(pipeline_id=graph.name, run_id=str(uuid.uuid4()))

        # Initialize checkpoint manager (only if not already initialized)
        if (
            self.enable_checkpoints
            and self.checkpoint_manager.get_checkpoint_state() is None
        ):
            checkpoint_state = None
            if recover_from_checkpoint:
                # Try to load existing checkpoint state
                checkpoint_state = self._load_checkpoint_state(context.run_id)

            self.checkpoint_manager.initialize(
                pipeline_id=graph.name,
                run_id=context.run_id,
                existing_state=checkpoint_state,
            )

        # Determine execution order
        if target_assets:
            execution_order = self._get_execution_order_for_targets(
                graph, target_assets
            )
        else:
            execution_order = graph.topological_order()

        # Adjust execution order based on checkpoints
        if self.enable_checkpoints and recover_from_checkpoint:
            execution_order = (
                self.checkpoint_manager.get_execution_plan_from_checkpoint(
                    execution_order
                )
            )
            logger.info(f"Recovered execution plan from checkpoint: {execution_order}")

        # Execute assets
        asset_results: dict[str, Any] = {}
        errors: list[str] = []
        succeeded = 0
        failed = 0
        retry_counts: dict[str, int] = {}

        for asset_name in execution_order:
            # Check if we have a checkpoint for this asset
            if (
                self.enable_checkpoints
                and self.checkpoint_manager.can_resume_from_asset(asset_name)
            ):
                logger.info(
                    f"Skipping asset '{asset_name}' - using checkpointed result"
                )
                checkpointed_result = (
                    self.checkpoint_manager.get_asset_result_from_checkpoint(asset_name)
                )
                if checkpointed_result:
                    asset_results[asset_name] = checkpointed_result
                    succeeded += 1
                    continue

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

            # Create checkpoint if enabled and execution succeeded
            if self.enable_checkpoints and result.success:
                self.checkpoint_manager.create_checkpoint(
                    asset_name=asset_name,
                    asset_result=result,
                    upstream_results=upstream_results,
                )

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
        _retry_counts: dict[str, int],  # Unused in new implementation
    ) -> AssetResult:
        """
        Execute an asset with retry logic if configured.

        Args:
            asset: The asset to execute
            context: Pipeline context
            upstream_results: Results from upstream assets
            _retry_counts: Dictionary tracking retry counts per asset (unused)

        Returns:
            AssetResult from the execution attempt
        """
        # Check if asset has retry config
        asset_retries = asset.config.get("retries", 0)
        asset_backoff = asset.config.get("backoff", "exponential")

        # Use asset-level retry config if available, otherwise use engine-level
        max_retries = asset_retries if asset_retries > 0 else self.max_retries

        # Execute with retries
        for attempt in range(max_retries + 1):
            result = self.executor.execute(asset, context, upstream_results)

            if result.success:
                return result

            # Execution failed
            if self.capture_error_context and result.error:
                # Capture detailed error context
                error_context = capture_error_context(
                    error=Exception(result.error),  # Create exception from error string
                    asset_name=asset.name,
                    inputs=upstream_results,
                    attempt_number=attempt,
                    retryable=attempt < max_retries,
                )
                # Log error context
                msg = (
                    f"Error executing asset '{asset.name}' "
                    f"(attempt {attempt + 1}/{max_retries + 1}): "
                    f"{error_context.error_message}"
                )
                logger.error(msg)
                logger.debug(f"Error context: {error_context.to_dict()}")

            # Check if we should retry
            if attempt < max_retries:
                # Calculate delay based on backoff strategy
                if asset_backoff == "exponential":
                    delay = 2**attempt
                elif asset_backoff == "linear":
                    delay = attempt + 1
                else:  # fixed
                    delay = 0

                delay = min(delay, 60.0)  # Cap at 60 seconds

                if delay > 0:
                    logger.warning(
                        f"Retrying asset '{asset.name}' in {delay}s "
                        f"(attempt {attempt + 2}/{max_retries + 1})"
                    )
                    time.sleep(delay)
                else:
                    logger.warning(
                        f"Retrying asset '{asset.name}' immediately "
                        f"(attempt {attempt + 2}/{max_retries + 1})"
                    )
            else:
                # No more retries
                break

        return result

    def _load_checkpoint_state(self, _run_id: str) -> "CheckpointState | None":
        """
        Load checkpoint state from storage.

        This is a placeholder implementation. In a real system, this would
        load from persistent storage (database, file system, etc.).

        Args:
            _run_id: The run ID to load checkpoints for (unused in placeholder)

        Returns:
            CheckpointState if found, None otherwise
        """
        # TODO: Implement persistent checkpoint storage
        # For now, return None to indicate no checkpoint state found
        return None

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
