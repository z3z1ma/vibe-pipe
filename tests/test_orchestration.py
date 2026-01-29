"""
Tests for orchestration engine.
"""

import json
import tempfile
from datetime import datetime, timedelta
from pathlib import Path
from unittest.mock import Mock, patch

import pytest

from vibe_piper import (
    Asset,
    AssetGraph,
    AssetResult,
    AssetType,
    DefaultExecutor,
    ErrorStrategy,
    Operator,
    OperatorType,
    PipelineContext,
)
from vibe_piper.orchestration import (
    ExecutionState,
    OrchestrationConfig,
    OrchestrationEngine,
    ParallelExecutor,
    StateManager,
)


class TestExecutionState:
    """Tests for ExecutionState class."""

    def test_create_execution_state(self) -> None:
        """Test creating an execution state."""
        state = ExecutionState(
            pipeline_id="test_pipeline",
            run_id="test_run",
        )

        assert state.pipeline_id == "test_pipeline"
        assert state.run_id == "test_run"
        assert len(state.completed_assets) == 0
        assert len(state.failed_assets) == 0

    def test_mark_completed(self) -> None:
        """Test marking assets as completed."""
        state = ExecutionState(pipeline_id="test_pipeline", run_id="test_run")

        state.mark_completed("asset1")
        state.mark_completed("asset2")

        assert "asset1" in state.completed_assets
        assert "asset2" in state.completed_assets
        assert state.is_asset_completed("asset1") is True
        assert state.is_asset_completed("asset3") is False

    def test_mark_failed(self) -> None:
        """Test marking assets as failed."""
        state = ExecutionState(pipeline_id="test_pipeline", run_id="test_run")

        state.mark_failed("asset1")
        state.mark_failed("asset2")

        assert "asset1" in state.failed_assets
        assert "asset2" in state.failed_assets
        assert state.is_asset_failed("asset1") is True
        assert state.is_asset_failed("asset3") is False

    def test_state_to_dict(self) -> None:
        """Test converting state to dictionary."""
        now = datetime.now()
        state = ExecutionState(
            pipeline_id="test_pipeline",
            run_id="test_run",
            start_time=now,
            metadata={"key": "value"},
        )

        data = state.to_dict()

        assert data["pipeline_id"] == "test_pipeline"
        assert data["run_id"] == "test_run"
        assert data["completed_assets"] == []
        assert data["failed_assets"] == []
        assert data["metadata"] == {"key": "value"}

    def test_state_from_dict(self) -> None:
        """Test creating state from dictionary."""
        now = datetime.now()
        data = {
            "pipeline_id": "test_pipeline",
            "run_id": "test_run",
            "completed_assets": ["asset1", "asset2"],
            "failed_assets": ["asset3"],
            "start_time": now.isoformat(),
            "last_updated": now.isoformat(),
            "metadata": {"key": "value"},
        }

        state = ExecutionState.from_dict(data)

        assert state.pipeline_id == "test_pipeline"
        assert state.run_id == "test_run"
        assert state.completed_assets == {"asset1", "asset2"}
        assert state.failed_assets == {"asset3"}
        assert state.metadata == {"key": "value"}


class TestStateManager:
    """Tests for StateManager class."""

    def test_save_and_load_state(self) -> None:
        """Test saving and loading state."""
        with tempfile.TemporaryDirectory() as tmpdir:
            state_dir = Path(tmpdir)
            manager = StateManager(state_dir=state_dir)

            original_state = ExecutionState(
                pipeline_id="test_pipeline",
                run_id="test_run",
            )
            original_state.mark_completed("asset1")
            original_state.mark_completed("asset2")

            # Save state
            manager.save_state(original_state)

            # Load state
            loaded_state = manager.load_state("test_pipeline")

            assert loaded_state is not None
            assert loaded_state.pipeline_id == original_state.pipeline_id
            assert loaded_state.run_id == original_state.run_id
            assert loaded_state.completed_assets == original_state.completed_assets

    def test_load_nonexistent_state(self) -> None:
        """Test loading state that doesn't exist."""
        with tempfile.TemporaryDirectory() as tmpdir:
            state_dir = Path(tmpdir)
            manager = StateManager(state_dir=state_dir)

            state = manager.load_state("nonexistent")

            assert state is None

    def test_clear_state(self) -> None:
        """Test clearing state."""
        with tempfile.TemporaryDirectory() as tmpdir:
            state_dir = Path(tmpdir)
            manager = StateManager(state_dir=state_dir)

            # Save state
            original_state = ExecutionState(pipeline_id="test_pipeline", run_id="test_run")
            manager.save_state(original_state)

            # Verify state exists
            loaded = manager.load_state("test_pipeline")
            assert loaded is not None

            # Clear state
            manager.clear_state("test_pipeline")

            # Verify state is cleared
            loaded_after = manager.load_state("test_pipeline")
            assert loaded_after is None


class TestParallelExecutor:
    """Tests for ParallelExecutor class."""

    def test_executor_context(self) -> None:
        """Test executor as context manager."""
        parallel_exec = ParallelExecutor(max_workers=2)

        with parallel_exec as exec_ctx:
            assert parallel_exec.executor is not None
            assert parallel_exec.max_workers == 2

        # Executor should be shut down after exit
        assert parallel_exec.executor is not None

    def test_execute_asset(self) -> None:
        """Test executing an asset."""

        def simple_op(data, ctx):
            return "result"

        operator = Operator(name="simple", operator_type=OperatorType.TRANSFORM, fn=simple_op)

        asset = Asset(
            name="test_asset",
            asset_type=AssetType.MEMORY,
            uri="memory://test_asset",
            operator=operator,
        )

        executor = ParallelExecutor()
        context = PipelineContext(pipeline_id="test", run_id="test-run")

        result = executor.execute(asset, context, {})

        assert result.success is True
        assert result.data == "result"


class TestOrchestrationConfig:
    """Tests for OrchestrationConfig class."""

    def test_default_config(self) -> None:
        """Test default configuration."""
        config = OrchestrationConfig()

        assert config.max_workers == 4
        assert config.enable_incremental is True
        assert config.checkpoint_interval == 10
        assert config.skip_on_cached is True

    def test_custom_config(self) -> None:
        """Test custom configuration."""
        config = OrchestrationConfig(
            max_workers=8,
            enable_incremental=False,
            checkpoint_interval=5,
            skip_on_cached=False,
        )

        assert config.max_workers == 8
        assert config.enable_incremental is False
        assert config.checkpoint_interval == 5
        assert config.skip_on_cached is False


class TestOrchestrationEngine:
    """Tests for OrchestrationEngine class."""

    def test_execute_simple_graph(self) -> None:
        """Test executing a simple graph."""

        def op(data, ctx):
            return "result"

        operator = Operator(name="op", operator_type=OperatorType.TRANSFORM, fn=op)

        asset = Asset(
            name="test_asset",
            asset_type=AssetType.MEMORY,
            uri="memory://test_asset",
            operator=operator,
        )

        graph = AssetGraph(name="test_graph", assets=(asset,))

        config = OrchestrationConfig(max_workers=1)
        engine = OrchestrationEngine(config=config)
        result = engine.execute(graph)

        assert result.success is True
        assert result.assets_executed == 1
        assert result.assets_succeeded == 1
        assert "test_asset" in result.asset_results

    def test_execute_with_dependencies(self) -> None:
        """Test executing a graph with dependencies."""

        def source_op(data, ctx):
            return [1, 2, 3]

        def transform_op(data, ctx):
            return [x * 2 for x in data]

        source_operator = Operator(name="source", operator_type=OperatorType.SOURCE, fn=source_op)

        transform_operator = Operator(
            name="transform", operator_type=OperatorType.TRANSFORM, fn=transform_op
        )

        source = Asset(
            name="source",
            asset_type=AssetType.MEMORY,
            uri="memory://source",
            operator=source_operator,
        )

        derived = Asset(
            name="derived",
            asset_type=AssetType.MEMORY,
            uri="memory://derived",
            operator=transform_operator,
        )

        graph = AssetGraph(
            name="test_graph",
            assets=(source, derived),
            dependencies={"derived": ("source",)},
        )

        config = OrchestrationConfig(max_workers=1)
        engine = OrchestrationEngine(config=config)
        result = engine.execute(graph)

        assert result.success is True
        assert result.assets_executed == 2
        assert result.asset_results["source"].success is True
        assert result.asset_results["derived"].success is True

    def test_parallel_execution(self) -> None:
        """Test parallel execution of independent assets."""

        def op(data, ctx):
            return f"{ctx.pipeline_id}_result"

        operator = Operator(name="op", operator_type=OperatorType.TRANSFORM, fn=op)

        asset1 = Asset(
            name="asset1",
            asset_type=AssetType.MEMORY,
            uri="memory://asset1",
            operator=operator,
        )

        asset2 = Asset(
            name="asset2",
            asset_type=AssetType.MEMORY,
            uri="memory://asset2",
            operator=operator,
        )

        asset3 = Asset(
            name="asset3",
            asset_type=AssetType.MEMORY,
            uri="memory://asset3",
            operator=operator,
        )

        graph = AssetGraph(name="test_graph", assets=(asset1, asset2, asset3))

        config = OrchestrationConfig(max_workers=4)
        engine = OrchestrationEngine(config=config)
        result = engine.execute(graph)

        assert result.success is True
        assert result.assets_executed == 3
        assert result.metrics.get("parallel") is True
        assert result.metrics.get("max_workers") == 4

    def test_parallel_execution_with_dependencies(self) -> None:
        """Test parallel execution respects dependencies."""

        def op(data, ctx):
            return "result"

        operator = Operator(name="op", operator_type=OperatorType.TRANSFORM, fn=op)

        # Create a diamond DAG:
        #     a
        #    / \
        #   b   c
        #    \ /
        #     d
        a = Asset(name="a", asset_type=AssetType.MEMORY, uri="memory://a", operator=operator)
        b = Asset(name="b", asset_type=AssetType.MEMORY, uri="memory://b", operator=operator)
        c = Asset(name="c", asset_type=AssetType.MEMORY, uri="memory://c", operator=operator)
        d = Asset(name="d", asset_type=AssetType.MEMORY, uri="memory://d", operator=operator)

        graph = AssetGraph(
            name="diamond",
            assets=(a, b, c, d),
            dependencies={"b": ("a",), "c": ("a",), "d": ("b", "c")},
        )

        config = OrchestrationConfig(max_workers=4)
        engine = OrchestrationEngine(config=config)
        result = engine.execute(graph)

        assert result.success is True
        assert result.assets_executed == 4

        # Verify execution order (a must come before b, c, d)
        order = list(result.asset_results.keys())
        a_index = order.index("a")
        b_index = order.index("b")
        c_index = order.index("c")
        d_index = order.index("d")

        assert a_index < b_index
        assert a_index < c_index
        assert b_index < d_index
        assert c_index < d_index

    def test_incremental_execution_skip_cached(self) -> None:
        """Test incremental execution skips cached assets."""

        with tempfile.TemporaryDirectory() as tmpdir:
            state_dir = Path(tmpdir)
            config = OrchestrationConfig(
                state_dir=state_dir,
                enable_incremental=True,
                skip_on_cached=True,
                max_workers=1,
            )

            engine = OrchestrationEngine(config=config)

            # First run - all assets execute
            def op(data, ctx):
                return "result"

            operator = Operator(name="op", operator_type=OperatorType.TRANSFORM, fn=op)

            asset1 = Asset(
                name="asset1",
                asset_type=AssetType.MEMORY,
                uri="memory://asset1",
                operator=operator,
            )

            asset2 = Asset(
                name="asset2",
                asset_type=AssetType.MEMORY,
                uri="memory://asset2",
                operator=operator,
            )

            graph = AssetGraph(
                name="test_graph",
                assets=(asset1, asset2),
                dependencies={"asset2": ("asset1",)},
            )

            result1 = engine.execute(graph)
            assert result1.assets_executed == 2

            # Second run - asset1 should be skipped
            result2 = engine.execute(graph)
            assert result2.assets_executed == 1  # Only asset2
            assert "asset1" not in result2.asset_results
            assert "asset2" in result2.asset_results

            # Cleanup
            engine.clear_state("test_graph")

    def test_incremental_execution_disabled(self) -> None:
        """Test incremental execution when disabled."""

        with tempfile.TemporaryDirectory() as tmpdir:
            state_dir = Path(tmpdir)
            config = OrchestrationConfig(
                state_dir=state_dir,
                enable_incremental=False,
                max_workers=1,
            )

            engine = OrchestrationEngine(config=config)

            def op(data, ctx):
                return "result"

            operator = Operator(name="op", operator_type=OperatorType.TRANSFORM, fn=op)

            asset = Asset(
                name="asset1",
                asset_type=AssetType.MEMORY,
                uri="memory://asset1",
                operator=operator,
            )

            graph = AssetGraph(name="test_graph", assets=(asset,))

            # First run
            result1 = engine.execute(graph)
            assert result1.assets_executed == 1

            # Second run - asset still executes (incremental disabled)
            result2 = engine.execute(graph)
            assert result2.assets_executed == 1
            assert "asset1" in result2.asset_results

            # Cleanup
            engine.clear_state("test_graph")

    def test_clear_state(self) -> None:
        """Test clearing execution state."""

        with tempfile.TemporaryDirectory() as tmpdir:
            state_dir = Path(tmpdir)
            config = OrchestrationConfig(
                state_dir=state_dir,
                enable_incremental=True,
                max_workers=1,
            )

            engine = OrchestrationEngine(config=config)

            def op(data, ctx):
                return "result"

            operator = Operator(name="op", operator_type=OperatorType.TRANSFORM, fn=op)

            asset = Asset(
                name="asset1",
                asset_type=AssetType.MEMORY,
                uri="memory://asset1",
                operator=operator,
            )

            graph = AssetGraph(name="test_graph", assets=(asset,))

            # First run
            result = engine.execute(graph)

            assert result.assets_executed == 1

            # Clear state
            engine.clear_state("test_graph")

            # Second run - all assets execute again
            result = engine.execute(graph)
            assert result.assets_executed == 1
            assert "asset1" in result.asset_results

    def test_get_state(self) -> None:
        """Test getting current execution state."""

        with tempfile.TemporaryDirectory() as tmpdir:
            state_dir = Path(tmpdir)
            config = OrchestrationConfig(
                state_dir=state_dir,
                enable_incremental=True,
                max_workers=1,
            )

            engine = OrchestrationEngine(config=config)

            def op(data, ctx):
                return "result"

            operator = Operator(name="op", operator_type=OperatorType.TRANSFORM, fn=op)

            asset = Asset(
                name="asset1",
                asset_type=AssetType.MEMORY,
                uri="memory://asset1",
                operator=operator,
            )

            graph = AssetGraph(name="test_graph", assets=(asset,))

            # Execute to create state
            result = engine.execute(graph)

            # Get state
            state = engine.get_state("test_graph")

            assert state is not None
            assert state.pipeline_id == "test_graph"
            assert "asset1" in state.completed_assets

            # Cleanup
            engine.clear_state("test_graph")

    def test_execute_with_target_assets(self) -> None:
        """Test executing only specific target assets."""

        def op(data, ctx):
            return "result"

        operator = Operator(name="op", operator_type=OperatorType.TRANSFORM, fn=op)

        asset1 = Asset(name="a", asset_type=AssetType.MEMORY, uri="memory://a", operator=operator)
        asset2 = Asset(name="b", asset_type=AssetType.MEMORY, uri="memory://b", operator=operator)
        asset3 = Asset(name="c", asset_type=AssetType.MEMORY, uri="memory://c", operator=operator)

        graph = AssetGraph(
            name="test_graph",
            assets=(asset1, asset2, asset3),
            dependencies={"b": ("a",), "c": ("b",)},
        )

        config = OrchestrationConfig(max_workers=1)
        engine = OrchestrationEngine(config=config)

        # Execute only asset c (should also execute b and a)
        result = engine.execute(graph, target_assets=("c",))

        assert result.assets_executed == 3  # a, b, c
        assert "a" in result.asset_results
        assert "b" in result.asset_results
        assert "c" in result.asset_results

    def test_fail_fast_error_strategy(self) -> None:
        """Test FAIL_FAST error strategy."""

        def failing_op(data, ctx):
            raise ValueError("Asset failed")

        def success_op(data, ctx):
            return "success"

        failing_operator = Operator(
            name="failing", operator_type=OperatorType.TRANSFORM, fn=failing_op
        )

        success_operator = Operator(
            name="success", operator_type=OperatorType.TRANSFORM, fn=success_op
        )

        asset1 = Asset(
            name="asset1",
            asset_type=AssetType.MEMORY,
            uri="memory://asset1",
            operator=success_operator,
        )

        asset2 = Asset(
            name="asset2",
            asset_type=AssetType.MEMORY,
            uri="memory://asset2",
            operator=failing_operator,
        )

        asset3 = Asset(
            name="asset3",
            asset_type=AssetType.MEMORY,
            uri="memory://asset3",
            operator=success_operator,
        )

        graph = AssetGraph(
            name="test_graph",
            assets=(asset1, asset2, asset3),
            dependencies={"asset2": ("asset1",), "asset3": ("asset2",)},
        )

        config = OrchestrationConfig(max_workers=1)

        engine = OrchestrationEngine(config=config)
        result = engine.execute(graph)

        assert result.success is False
        assert result.assets_failed == 1
        # asset3 should not be executed due to fail-fast
        assert "asset3" not in result.asset_results

    def test_custom_executor(self) -> None:
        """Test using a custom executor."""

        def custom_op(data, ctx):
            return "custom_result"

        custom_operator = Operator(
            name="custom", operator_type=OperatorType.TRANSFORM, fn=custom_op
        )

        asset = Asset(
            name="test_asset",
            asset_type=AssetType.MEMORY,
            uri="memory://test_asset",
            operator=custom_operator,
        )

        class CustomExecutor:
            def execute(self, asset, context, upstream_results):
                return AssetResult(
                    asset_name=asset.name,
                    success=True,
                    data="custom_result",
                )

        graph = AssetGraph(name="test_graph", assets=(asset,))

        config = OrchestrationConfig(max_workers=1)
        engine = OrchestrationEngine(config=config, executor=CustomExecutor())
        result = engine.execute(graph)

        assert result.success is True
        assert result.asset_results["test_asset"].data == "custom_result"

    def test_checkpoint_interval(self) -> None:
        """Test checkpointing at intervals."""

        with tempfile.TemporaryDirectory() as tmpdir:
            state_dir = Path(tmpdir)
            config = OrchestrationConfig(
                state_dir=state_dir,
                enable_incremental=True,
                checkpoint_interval=1,
                max_workers=1,
            )

            engine = OrchestrationEngine(config=config)

            def op(data, ctx):
                return "result"

            operator = Operator(name="op", operator_type=OperatorType.TRANSFORM, fn=op)

            assets = []
            for i in range(5):
                assets.append(
                    Asset(
                        name=f"asset{i}",
                        asset_type=AssetType.MEMORY,
                        uri=f"memory://asset{i}",
                        operator=operator,
                    )
                )

            graph = AssetGraph(name="test_graph", assets=tuple(assets))

            result = engine.execute(graph)

            assert result.success is True
            assert result.assets_executed == 5

            # Cleanup
            engine.clear_state("test_graph")
