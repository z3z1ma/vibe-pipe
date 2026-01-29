"""
Orchestration engine for Vibe Piper.

This module provides advanced orchestration features including parallel execution,
state tracking, checkpointing, and incremental run optimization.
"""

import json
import logging
import threading
import time
from collections.abc import Mapping, Sequence
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any

from vibe_piper.execution import DefaultExecutor
from vibe_piper.types import (
    Asset,
    AssetGraph,
    AssetResult,
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
# State Tracking
# =============================================================================


@dataclass
class ExecutionState:
    """
    Persistent state for pipeline execution.

    ExecutionState tracks execution progress across runs, enabling
    checkpointing, recovery, and incremental execution.

    Attributes:
        pipeline_id: Unique identifier for the pipeline
        run_id: Current execution run identifier
        completed_assets: Set of asset names that completed successfully
        failed_assets: Set of asset names that failed
        start_time: When execution started
        last_updated: Last time state was updated
        metadata: Additional execution metadata
    """

    pipeline_id: str
    run_id: str
    completed_assets: set[str] = field(default_factory=set)
    failed_assets: set[str] = field(default_factory=set)
    start_time: datetime = field(default_factory=datetime.utcnow)
    last_updated: datetime = field(default_factory=datetime.utcnow)
    metadata: dict[str, Any] = field(default_factory=dict)

    def mark_completed(self, asset_name: str) -> None:
        """Mark an asset as completed."""
        self.completed_assets.add(asset_name)
        self.last_updated = datetime.utcnow()

    def mark_failed(self, asset_name: str) -> None:
        """Mark an asset as failed."""
        self.failed_assets.add(asset_name)
        self.last_updated = datetime.utcnow()

    def is_asset_completed(self, asset_name: str) -> bool:
        """Check if an asset has been completed."""
        return asset_name in self.completed_assets

    def is_asset_failed(self, asset_name: str) -> bool:
        """Check if an asset has failed."""
        return asset_name in self.failed_assets

    def to_dict(self) -> dict[str, Any]:
        """Convert state to dictionary for serialization."""
        return {
            "pipeline_id": self.pipeline_id,
            "run_id": self.run_id,
            "completed_assets": sorted(self.completed_assets),
            "failed_assets": sorted(self.failed_assets),
            "start_time": self.start_time.isoformat(),
            "last_updated": self.last_updated.isoformat(),
            "metadata": self.metadata,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "ExecutionState":
        """Create ExecutionState from dictionary."""
        return cls(
            pipeline_id=data["pipeline_id"],
            run_id=data["run_id"],
            completed_assets=set(data.get("completed_assets", [])),
            failed_assets=set(data.get("failed_assets", [])),
            start_time=datetime.fromisoformat(data["start_time"])
            if "start_time" in data
            else datetime.utcnow(),
            last_updated=datetime.fromisoformat(data["last_updated"])
            if "last_updated" in data
            else datetime.utcnow(),
            metadata=data.get("metadata", {}),
        )


@dataclass
class StateManager:
    """
    Manages persistent execution state.

    StateManager provides thread-safe access to execution state,
    with support for persistence to disk.

    Attributes:
        state_dir: Directory to store state files
        lock: Thread lock for state access
    """

    state_dir: Path
    lock: threading.Lock = field(default_factory=threading.Lock)

    def _get_state_path(self, pipeline_id: str) -> Path:
        """Get path to state file for a pipeline."""
        return self.state_dir / f"{pipeline_id}.json"

    def save_state(self, state: ExecutionState) -> None:
        """
        Save execution state to disk.

        Args:
            state: The execution state to save
        """
        with self.lock:
            state_path = self._get_state_path(state.pipeline_id)
            state_path.parent.mkdir(parents=True, exist_ok=True)

            with open(state_path, "w") as f:
                json.dump(state.to_dict(), f, indent=2, default=str)

            logger.debug(f"Saved state for {state.pipeline_id}: {state.to_dict()}")

    def load_state(self, pipeline_id: str) -> ExecutionState | None:
        """
        Load execution state from disk.

        Args:
            pipeline_id: The pipeline ID to load state for

        Returns:
            ExecutionState if found, None otherwise
        """
        with self.lock:
            state_path = self._get_state_path(pipeline_id)

            if not state_path.exists():
                return None

            try:
                with open(state_path, "r") as f:
                    data = json.load(f)
                    return ExecutionState.from_dict(data)
            except (json.JSONDecodeError, KeyError, ValueError) as e:
                logger.warning(
                    f"Failed to load state for {pipeline_id}: {e}. Starting with fresh state."
                )
                return None

    def clear_state(self, pipeline_id: str) -> None:
        """
        Clear execution state for a pipeline.

        Args:
            pipeline_id: The pipeline ID to clear state for
        """
        with self.lock:
            state_path = self._get_state_path(pipeline_id)
            if state_path.exists():
                state_path.unlink()
                logger.info(f"Cleared state for {pipeline_id}")


# =============================================================================
# Parallel Executor
# =============================================================================


class ParallelExecutor:
    """
    Executor that supports parallel asset execution.

    ParallelExecutor executes assets concurrently using a thread pool,
    respecting dependencies and maximizing parallelism where possible.

    Attributes:
        max_workers: Maximum number of concurrent workers
        executor: ThreadPoolExecutor for parallel execution
    """

    def __init__(self, max_workers: int = 4) -> None:
        """Initialize ParallelExecutor."""
        self.max_workers = max_workers
        self.executor: ThreadPoolExecutor | None = None

    def __enter__(self) -> "ParallelExecutor":
        """Enter context manager."""
        self.executor = ThreadPoolExecutor(max_workers=self.max_workers)
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        """Exit context manager."""
        if self.executor is not None:
            self.executor.shutdown(wait=True)
            self.executor = None

    def execute(
        self,
        asset: Asset,
        context: PipelineContext,
        upstream_results: Mapping[str, Any],
    ) -> AssetResult:
        """
        Execute an asset (delegates to DefaultExecutor).

        Args:
            asset: The asset to execute
            context: The pipeline execution context
            upstream_results: Results from upstream assets

        Returns:
            AssetResult containing execution outcome
        """
        default_executor = DefaultExecutor()
        return default_executor.execute(asset, context, upstream_results)


# =============================================================================
# Parallel Execution Engine
# =============================================================================


@dataclass
class OrchestrationConfig:
    """
    Configuration for orchestration engine.

    Attributes:
        max_workers: Maximum number of parallel workers
        enable_incremental: Whether to enable incremental runs
        checkpoint_interval: Checkpoint every N assets
        checkpoint_dir: Directory for checkpoints
        state_dir: Directory for state files
        error_strategy: How to handle execution errors
    """

    max_workers: int = 4
    enable_incremental: bool = True
    checkpoint_interval: int = 10
    checkpoint_dir: Path = Path(".checkpoints")
    state_dir: Path = Path(".state")
    skip_on_cached: bool = True
    error_strategy: ErrorStrategy = ErrorStrategy.FAIL_FAST


@dataclass
class OrchestrationEngine:
    """
    Advanced orchestration engine with parallel execution and state management.

    OrchestrationEngine extends ExecutionEngine with:
    - Parallel execution using thread pools
    - Persistent state tracking
    - Checkpoint-based recovery
    - Incremental run optimization

    Attributes:
        config: Orchestration configuration
        state_manager: Manages execution state persistence
        executor: The executor to use for running assets
        error_strategy: How to handle execution errors
    """

    config: OrchestrationConfig = field(default_factory=OrchestrationConfig)
    state_manager: StateManager = field(init=False)
    executor: Executor = field(default_factory=lambda: DefaultExecutor())
    error_strategy: ErrorStrategy = ErrorStrategy.FAIL_FAST

    def __post_init__(self) -> None:
        """Initialize state manager."""
        self.state_manager = StateManager(state_dir=self.config.state_dir)

    def execute(
        self,
        graph: AssetGraph,
        target_assets: tuple[str, ...] | None = None,
        context: PipelineContext | None = None,
        incremental: bool | None = None,
    ) -> ExecutionResult:
        """
        Execute an asset graph with orchestration features.

        Args:
            graph: The asset graph to execute
            target_assets: Optional specific assets to execute (and their dependencies)
            context: Optional pipeline context. If None, creates a new context.
            incremental: Whether to use incremental execution. If None, uses config default.

        Returns:
            ExecutionResult with details of execution
        """
        start_time = time.time()
        timestamp = datetime.now()

        # Create or use provided context
        if context is None:
            import uuid

            context = PipelineContext(pipeline_id=graph.name, run_id=str(uuid.uuid4()))

        # Determine execution order
        if target_assets:
            execution_order = self._get_execution_order_for_targets(graph, target_assets)
        else:
            execution_order = graph.topological_order()

        logger.debug(f"Execution order: {execution_order}")

        # Determine incremental mode
        use_incremental = incremental if incremental is not None else self.config.enable_incremental

        # Load or create execution state
        if use_incremental:
            state = self.state_manager.load_state(graph.name)
            if state is None:
                state = ExecutionState(pipeline_id=graph.name, run_id=context.run_id)
            else:
                # Update run_id for this execution
                state.run_id = context.run_id
                logger.info(
                    f"Loaded incremental state for {graph.name}: "
                    f"{len(state.completed_assets)} completed assets"
                )
        else:
            state = ExecutionState(pipeline_id=graph.name, run_id=context.run_id)

        # Determine assets to execute (skip if incremental and completed)
        assets_to_execute = self._filter_assets_for_incremental(
            execution_order, state, use_incremental
        )

        logger.info(f"Executing {len(assets_to_execute)} assets (incremental={use_incremental})")

        # Execute assets (parallel or sequential)
        logger.debug(f"Execution order: {execution_order}")
        logger.debug(f"Assets to execute: {assets_to_execute}")
        logger.debug(f"Graph assets: {graph.assets}")

        if self.config.max_workers > 1:
            logger.debug(f"Using parallel execution with {self.config.max_workers} workers")
            asset_results = self._execute_parallel(graph, assets_to_execute, context, state)
        else:
            logger.debug("Using sequential execution")
            asset_results = self._execute_sequential(graph, assets_to_execute, context, state)

        logger.info(f"Executing {len(assets_to_execute)} assets (incremental={use_incremental})")

        # Execute assets (parallel or sequential)
        if self.config.max_workers > 1:
            asset_results = self._execute_parallel(graph, assets_to_execute, context, state)
        else:
            asset_results = self._execute_sequential(graph, assets_to_execute, context, state)

        # Save final state
        if use_incremental:
            self.state_manager.save_state(state)

        # Calculate overall success
        if not asset_results:
            # No assets were executed (all skipped due to incremental)
            succeeded = 0
            failed = 0
            errors = []
        else:
            succeeded = sum(1 for r in asset_results.values() if r.success)
            failed = sum(1 for r in asset_results.values() if not r.success)
            errors = [
                f"{name}: {result.error}"
                for name, result in asset_results.items()
                if not result.success
            ]

        overall_success = failed == 0

        # Calculate duration
        duration_ms = (time.time() - start_time) * 1000

        # Aggregate metrics
        metrics = self._aggregate_metrics(asset_results)

        # Build execution result
        result = ExecutionResult(
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

        logger.info(
            f"Execution completed: {result.assets_succeeded} succeeded, "
            f"{result.assets_failed} failed in {duration_ms:.0f}ms"
        )

        return result

    def _filter_assets_for_incremental(
        self, execution_order: tuple[str, ...], state: ExecutionState, use_incremental: bool
    ) -> tuple[str, ...]:
        """
        Filter assets for incremental execution.

        Skips assets that have already completed successfully in previous runs.

        Args:
            execution_order: Topological order of all assets
            state: Current execution state
            use_incremental: Whether incremental mode is enabled

        Returns:
            Tuple of asset names to execute
        """
        if not use_incremental or not self.config.skip_on_cached:
            return execution_order

        # Skip assets that are already completed
        filtered = tuple(name for name in execution_order if not state.is_asset_completed(name))

        if len(filtered) < len(execution_order):
            logger.info(
                f"Incremental: skipping {len(execution_order) - len(filtered)} "
                f"already-completed assets"
            )

        return filtered

    def _execute_parallel(
        self,
        graph: AssetGraph,
        execution_order: tuple[str, ...],
        context: PipelineContext,
        state: ExecutionState,
    ) -> dict[str, AssetResult]:
        """
        Execute assets in parallel using thread pool.

        Args:
            graph: The asset graph
            execution_order: Order of assets to execute
            context: Pipeline execution context
            state: Execution state to update

        Returns:
            Mapping of asset name to execution result
        """
        asset_results: dict[str, AssetResult] = {}
        parallel_exec = ParallelExecutor(max_workers=self.config.max_workers)

        with parallel_exec as exec_ctx:
            # executor is now guaranteed to be non-None
            assert exec_ctx.executor is not None
            # Execute assets in waves (respect dependencies)
            completed: set[str] = set()
            pending: set[str] = set(execution_order)

            while pending:
                # Find assets ready for execution (all deps completed)
                ready: list[str] = [
                    name
                    for name in pending
                    if all(dep.name in completed for dep in graph.get_dependencies(name))
                ]

                if not ready:
                    # No assets ready - likely circular dependency
                    msg = "No assets ready for execution - possible circular dependency"
                    logger.error(msg)
                    break

                # Submit ready assets for parallel execution
                futures: dict[Any, str] = {}
                if exec_ctx.executor is not None:
                    for name in ready:
                        asset = graph.get_asset(name)
                        if asset:
                            future = exec_ctx.executor.submit(
                                self._execute_asset_with_state,
                                asset,
                                context,
                                {
                                    dep.name: asset_results[dep.name]
                                    for dep in graph.get_dependencies(name)
                                    if dep.name in asset_results
                                },
                                state,
                            )
                            futures[future] = name

                # Wait for all to complete
                if futures:
                    for future in as_completed(futures):
                        asset_name = futures[future]
                        try:
                            result = future.result()
                            asset_results[asset_name] = result

                            if result.success:
                                state.mark_completed(asset_name)
                            else:
                                state.mark_failed(asset_name)

                                # Handle error based on strategy
                                if self.error_strategy == ErrorStrategy.FAIL_FAST:
                                    # Cancel remaining futures
                                    for f in futures:
                                        if f != future and not f.done():
                                            f.cancel()
                                    break
                        except Exception as e:
                            # Unexpected error
                            error_msg = f"Exception executing {asset_name}: {e}"
                            logger.error(error_msg)
                            asset_results[asset_name] = AssetResult(
                                asset_name=asset_name,
                                success=False,
                                error=error_msg,
                            )
                            state.mark_failed(asset_name)

                            if self.error_strategy == ErrorStrategy.FAIL_FAST:
                                for f in futures:
                                    if f != future and not f.done():
                                        f.cancel()
                                break

                # Mark completed as done
                for name in ready:
                    completed.add(name)
                    pending.discard(name)

                # Checkpoint periodically
                if len(completed) % self.config.checkpoint_interval == 0:
                    logger.info(f"Checkpoint: {len(completed)} assets completed so far")
                    self.state_manager.save_state(state)

        return asset_results

    def _execute_sequential(
        self,
        graph: AssetGraph,
        execution_order: tuple[str, ...],
        context: PipelineContext,
        state: ExecutionState,
    ) -> dict[str, AssetResult]:
        """
        Execute assets sequentially.

        Args:
            graph: The asset graph
            execution_order: Order of assets to execute
            context: Pipeline execution context
            state: Execution state to update

        Returns:
            Mapping of asset name to execution result
        """
        asset_results: dict[str, AssetResult] = {}

        for asset_name in execution_order:
            asset = graph.get_asset(asset_name)
            if asset is None:
                continue

            # Get upstream results
            upstream_results = {
                dep.name: asset_results[dep.name]
                for dep in graph.get_dependencies(asset_name)
                if dep.name in asset_results
            }

            # Execute asset
            result = self._execute_asset_with_state(asset, context, upstream_results, state)
            asset_results[asset_name] = result

            # Update state
            if result.success:
                state.mark_completed(asset_name)
            else:
                state.mark_failed(asset_name)

                # Handle error based on strategy
                if self.error_strategy == ErrorStrategy.FAIL_FAST:
                    break

            # Checkpoint periodically
            if len(asset_results) % self.config.checkpoint_interval == 0:
                logger.info(f"Checkpoint: {len(asset_results)} assets completed so far")
                self.state_manager.save_state(state)

        return asset_results

    def _execute_asset_with_state(
        self,
        asset: Asset,
        context: PipelineContext,
        upstream_results: Mapping[str, Any],
        state: ExecutionState,
    ) -> AssetResult:
        """
        Execute an asset with state tracking.

        Args:
            asset: The asset to execute
            context: Pipeline context
            upstream_results: Results from upstream assets
            state: Execution state to update

        Returns:
            AssetResult from execution
        """
        start_time = time.time()
        logger.debug(f"Executing asset: {asset.name}")

        result = self.executor.execute(asset, context, upstream_results)

        duration_ms = (time.time() - start_time) * 1000
        logger.debug(
            f"Asset {asset.name} completed in {duration_ms:.0f}ms: success={result.success}"
        )

        return result

    def _get_execution_order_for_targets(
        self, graph: AssetGraph, targets: tuple[str, ...]
    ) -> tuple[str, ...]:
        """
        Get execution order for specific target assets and their dependencies.

        Args:
            graph: The asset graph
            targets: Target asset names

        Returns:
            Tuple of asset names in execution order
        """
        # Get all dependencies for target assets (recursively)
        to_execute: set[str] = set()
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
        from vibe_piper.types import DataRecord

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
            "parallel": self.config.max_workers > 1,
            "max_workers": self.config.max_workers,
        }

        return metrics

    def clear_state(self, pipeline_id: str) -> None:
        """
        Clear execution state for a pipeline.

        Use this to reset incremental state when you want to
        force a full re-run of the pipeline.

        Args:
            pipeline_id: The pipeline ID to clear state for
        """
        self.state_manager.clear_state(pipeline_id)
        logger.info(f"Cleared state for pipeline {pipeline_id}")

    def get_state(self, pipeline_id: str) -> ExecutionState | None:
        """
        Get current execution state for a pipeline.

        Args:
            pipeline_id: The pipeline ID to get state for

        Returns:
            ExecutionState if found, None otherwise
        """
        return self.state_manager.load_state(pipeline_id)

    def clear_pipeline_state(self, pipeline_id: str) -> None:
        """
        Clear execution state for a pipeline.

        Use this to reset incremental state when you want to
        force a full re-run of the pipeline.

        Args:
            pipeline_id: The pipeline ID to clear state for
        """
        self.state_manager.clear_state(pipeline_id)
        logger.info(f"Cleared state for pipeline {pipeline_id}")
