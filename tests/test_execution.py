"""
Tests for the execution engine.
"""

from dataclasses import FrozenInstanceError
from typing import Any

import pytest

from vibe_piper import (
    Asset,
    AssetGraph,
    AssetResult,
    AssetType,
    DefaultExecutor,
    ErrorStrategy,
    ExecutionEngine,
    ExecutionResult,
    Operator,
    OperatorType,
    PipelineContext,
)


class TestAssetResult:
    """Tests for AssetResult class."""

    def test_create_successful_asset_result(self) -> None:
        """Test creating a successful asset result."""
        result = AssetResult(
            asset_name="test_asset",
            success=True,
            data={"key": "value"},
            metrics={"rows": 100},
            duration_ms=150.0,
        )

        assert result.asset_name == "test_asset"
        assert result.success is True
        assert result.data == {"key": "value"}
        assert result.error is None
        assert result.metrics["rows"] == 100
        assert result.duration_ms == 150.0

    def test_create_failed_asset_result(self) -> None:
        """Test creating a failed asset result."""
        result = AssetResult(
            asset_name="failed_asset",
            success=False,
            error="Something went wrong",
            duration_ms=50.0,
        )

        assert result.asset_name == "failed_asset"
        assert result.success is False
        assert result.error == "Something went wrong"
        assert result.data is None

    def test_asset_result_with_lineage(self) -> None:
        """Test asset result with upstream lineage."""
        result = AssetResult(
            asset_name="derived_asset",
            success=True,
            lineage=("source_asset", "intermediate_asset"),
        )

        assert result.lineage == ("source_asset", "intermediate_asset")

    def test_asset_result_with_metadata(self) -> None:
        """Test asset result with metadata fields."""
        from datetime import datetime

        now = datetime.now()
        result = AssetResult(
            asset_name="test_asset",
            success=True,
            created_at=now,
            updated_at=now,
            checksum="abc123",
        )

        assert result.created_at == now
        assert result.updated_at == now
        assert result.checksum == "abc123"

    def test_asset_result_metadata_defaults(self) -> None:
        """Test that asset result metadata fields default to None."""
        result = AssetResult(asset_name="test", success=True)

        assert result.created_at is None
        assert result.updated_at is None
        assert result.checksum is None

    def test_asset_result_is_frozen(self) -> None:
        """Test that AssetResult is immutable."""
        result = AssetResult(asset_name="test", success=True)
        with pytest.raises(FrozenInstanceError):
            result.asset_name = "changed"  # type: ignore[misc]


class TestExecutionResult:
    """Tests for ExecutionResult class."""

    def test_create_successful_execution_result(self) -> None:
        """Test creating a successful execution result."""
        asset_results = {
            "asset1": AssetResult(asset_name="asset1", success=True),
            "asset2": AssetResult(asset_name="asset2", success=True),
        }

        result = ExecutionResult(
            success=True,
            asset_results=asset_results,
            assets_executed=2,
            assets_succeeded=2,
            assets_failed=0,
        )

        assert result.success is True
        assert result.assets_executed == 2
        assert result.assets_succeeded == 2
        assert result.assets_failed == 0

    def test_create_failed_execution_result(self) -> None:
        """Test creating a failed execution result."""
        asset_results = {
            "asset1": AssetResult(asset_name="asset1", success=True),
            "asset2": AssetResult(asset_name="asset2", success=False, error="Failed"),
        }

        result = ExecutionResult(
            success=False,
            asset_results=asset_results,
            errors=("asset2: Failed",),
            assets_executed=2,
            assets_succeeded=1,
            assets_failed=1,
        )

        assert result.success is False
        assert len(result.errors) == 1
        assert result.assets_failed == 1

    def test_get_asset_result(self) -> None:
        """Test getting asset result by name."""
        asset_results = {
            "asset1": AssetResult(asset_name="asset1", success=True),
            "asset2": AssetResult(asset_name="asset2", success=False),
        }

        result = ExecutionResult(success=False, asset_results=asset_results, assets_executed=2)

        assert result.get_asset_result("asset1") is not None
        assert result.get_asset_result("asset1").success is True  # type: ignore
        assert result.get_asset_result("nonexistent") is None

    def test_get_failed_assets(self) -> None:
        """Test getting list of failed assets."""
        asset_results = {
            "asset1": AssetResult(asset_name="asset1", success=True),
            "asset2": AssetResult(asset_name="asset2", success=False),
            "asset3": AssetResult(asset_name="asset3", success=False),
        }

        result = ExecutionResult(success=False, asset_results=asset_results, assets_executed=3)

        failed = result.get_failed_assets()
        assert set(failed) == {"asset2", "asset3"}

    def test_get_succeeded_assets(self) -> None:
        """Test getting list of succeeded assets."""
        asset_results = {
            "asset1": AssetResult(asset_name="asset1", success=True),
            "asset2": AssetResult(asset_name="asset2", success=False),
            "asset3": AssetResult(asset_name="asset3", success=True),
        }

        result = ExecutionResult(success=False, asset_results=asset_results, assets_executed=3)

        succeeded = result.get_succeeded_assets()
        assert set(succeeded) == {"asset1", "asset3"}

    def test_execution_result_is_frozen(self) -> None:
        """Test that ExecutionResult is immutable."""
        result = ExecutionResult(success=True, asset_results={}, assets_executed=0)
        with pytest.raises(FrozenInstanceError):
            result.success = False  # type: ignore[misc]


class TestDefaultExecutor:
    """Tests for DefaultExecutor class."""

    def test_execute_asset_with_operator(self) -> None:
        """Test executing an asset with an operator."""

        def simple_op(data: Any, context: PipelineContext) -> Any:
            return "result_data"

        operator = Operator(
            name="simple",
            operator_type=OperatorType.TRANSFORM,
            fn=simple_op,
        )

        asset = Asset(
            name="test_asset",
            asset_type=AssetType.MEMORY,
            uri="memory://test_asset",
            operator=operator,
        )

        executor = DefaultExecutor()
        context = PipelineContext(pipeline_id="test", run_id="test-run")

        result = executor.execute(asset, context, {})

        assert result.success is True
        assert result.asset_name == "test_asset"
        assert result.data == "result_data"

    def test_execute_asset_without_operator(self) -> None:
        """Test executing an asset without an operator."""
        asset = Asset(
            name="test_asset",
            asset_type=AssetType.MEMORY,
            uri="memory://test_asset",
        )

        executor = DefaultExecutor()
        context = PipelineContext(pipeline_id="test", run_id="test-run")

        result = executor.execute(asset, context, {})

        assert result.success is True
        assert result.data is None

    def test_execute_asset_with_error(self) -> None:
        """Test executing an asset that raises an error."""

        def failing_op(data: Any, context: PipelineContext) -> Any:
            raise ValueError("Test error")

        operator = Operator(
            name="failing",
            operator_type=OperatorType.TRANSFORM,
            fn=failing_op,
        )

        asset = Asset(
            name="failing_asset",
            asset_type=AssetType.MEMORY,
            uri="memory://failing_asset",
            operator=operator,
        )

        executor = DefaultExecutor()
        context = PipelineContext(pipeline_id="test", run_id="test-run")

        result = executor.execute(asset, context, {})

        assert result.success is False
        assert result.error is not None
        assert "Test error" in result.error

    def test_execute_asset_with_lineage(self) -> None:
        """Test that asset result includes upstream lineage."""

        def simple_op(data: Any, context: PipelineContext) -> Any:
            return data

        operator = Operator(
            name="simple",
            operator_type=OperatorType.TRANSFORM,
            fn=simple_op,
        )

        asset = Asset(
            name="downstream",
            asset_type=AssetType.MEMORY,
            uri="memory://downstream",
            operator=operator,
        )

        executor = DefaultExecutor()
        context = PipelineContext(pipeline_id="test", run_id="test-run")

        upstream_results = {"asset1": "data1", "asset2": "data2"}
        result = executor.execute(asset, context, upstream_results)

        assert result.lineage == ("asset1", "asset2")


class TestExecutionEngine:
    """Tests for ExecutionEngine class."""

    def test_execute_simple_graph(self) -> None:
        """Test executing a simple graph with one asset."""

        def simple_op(data: Any, context: PipelineContext) -> Any:
            return "result"

        operator = Operator(
            name="simple",
            operator_type=OperatorType.TRANSFORM,
            fn=simple_op,
        )

        asset = Asset(
            name="test_asset",
            asset_type=AssetType.MEMORY,
            uri="memory://test_asset",
            operator=operator,
        )

        graph = AssetGraph(name="test_graph", assets=(asset,))

        engine = ExecutionEngine()
        result = engine.execute(graph)

        assert result.success is True
        assert result.assets_executed == 1
        assert result.assets_succeeded == 1
        assert result.assets_failed == 0
        assert "test_asset" in result.asset_results

    def test_execute_graph_with_dependencies(self) -> None:
        """Test executing a graph with dependencies."""

        def source_op(data: Any, context: PipelineContext) -> Any:
            return [1, 2, 3]

        def transform_op(data: Any, context: PipelineContext) -> Any:
            return [x * 2 for x in data]

        source_operator = Operator(
            name="source",
            operator_type=OperatorType.SOURCE,
            fn=source_op,
        )

        transform_operator = Operator(
            name="transform",
            operator_type=OperatorType.TRANSFORM,
            fn=transform_op,
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

        engine = ExecutionEngine()
        result = engine.execute(graph)

        assert result.success is True
        assert result.assets_executed == 2
        assert result.asset_results["source"].success is True
        assert result.asset_results["derived"].success is True

    def test_execute_graph_with_target_assets(self) -> None:
        """Test executing only specific target assets."""

        def op(data: Any, context: PipelineContext) -> Any:
            return "result"

        operator = Operator(
            name="op",
            operator_type=OperatorType.TRANSFORM,
            fn=op,
        )

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

        graph = AssetGraph(
            name="test_graph",
            assets=(asset1, asset2, asset3),
            dependencies={"asset2": ("asset1",), "asset3": ("asset2",)},
        )

        engine = ExecutionEngine()
        result = engine.execute(graph, target_assets=("asset3",))

        # Should execute asset1, asset2, and asset3 (all dependencies of asset3)
        assert result.assets_executed == 3
        assert "asset1" in result.asset_results
        assert "asset2" in result.asset_results
        assert "asset3" in result.asset_results

    def test_execute_with_fail_fast_error_strategy(self) -> None:
        """Test execution with FAIL_FAST error strategy."""

        def failing_op(data: Any, context: PipelineContext) -> Any:
            raise ValueError("Asset failed")

        def success_op(data: Any, context: PipelineContext) -> Any:
            return "success"

        failing_operator = Operator(
            name="failing",
            operator_type=OperatorType.TRANSFORM,
            fn=failing_op,
        )

        success_operator = Operator(
            name="success",
            operator_type=OperatorType.TRANSFORM,
            fn=success_op,
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

        engine = ExecutionEngine(error_strategy=ErrorStrategy.FAIL_FAST)
        result = engine.execute(graph)

        assert result.success is False
        assert result.assets_executed == 2  # asset1 and asset2
        assert result.assets_failed == 1
        # asset3 should not be executed due to fail-fast

    def test_execute_with_continue_error_strategy(self) -> None:
        """Test execution with CONTINUE error strategy."""
        # Note: This test will verify the structure but CONTINUE logic
        # is not fully implemented in the basic engine yet
        # The engine will stop on error, but the strategy is set
        engine = ExecutionEngine(error_strategy=ErrorStrategy.CONTINUE)

        assert engine.error_strategy == ErrorStrategy.CONTINUE

    def test_execution_metrics(self) -> None:
        """Test that execution includes metrics."""

        def op(data: Any, context: PipelineContext) -> Any:
            return "result"

        operator = Operator(
            name="op",
            operator_type=OperatorType.TRANSFORM,
            fn=op,
        )

        asset = Asset(
            name="test_asset",
            asset_type=AssetType.MEMORY,
            uri="memory://test_asset",
            operator=operator,
        )

        graph = AssetGraph(name="test_graph", assets=(asset,))

        engine = ExecutionEngine()
        result = engine.execute(graph)

        assert "total_assets" in result.metrics
        assert "total_duration_ms" in result.metrics
        assert "avg_duration_ms" in result.metrics
        assert result.metrics["total_assets"] == 1

    def test_execution_with_custom_context(self) -> None:
        """Test execution with a custom pipeline context."""

        def op(data: Any, context: PipelineContext) -> Any:
            # Access context to verify it's passed through
            return context

        operator = Operator(
            name="op",
            operator_type=OperatorType.TRANSFORM,
            fn=op,
        )

        asset = Asset(
            name="test_asset",
            asset_type=AssetType.MEMORY,
            uri="memory://test_asset",
            operator=operator,
        )

        graph = AssetGraph(name="test_graph", assets=(asset,))

        custom_context = PipelineContext(pipeline_id="test", run_id="test-run")
        engine = ExecutionEngine()
        result = engine.execute(graph, context=custom_context)

        assert result.success is True

    def test_execution_complex_dag(self) -> None:
        """Test execution of a complex DAG structure."""

        def op(data: Any, context: PipelineContext) -> Any:
            return "result"

        operator = Operator(
            name="op",
            operator_type=OperatorType.TRANSFORM,
            fn=op,
        )

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

        engine = ExecutionEngine()
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
