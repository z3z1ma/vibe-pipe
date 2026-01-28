"""
Integration tests for materialization strategies.

This module tests materialization strategies with the asset decorator
and execution engine.
"""

import pytest

from vibe_piper import (
    Asset,
    AssetGraph,
    AssetType,
    ExecutionEngine,
    MaterializationStrategy,
    asset,
)
from vibe_piper.types import Operator, OperatorType, PipelineContext  # type: ignore[attr-defined]

# =============================================================================
# Asset Decorator Tests
# =============================================================================


class TestAssetDecoratorMaterialization:
    """Tests for asset decorator with materialization parameter."""

    def test_asset_with_table_materialization(self):
        """Test @asset decorator with table materialization."""

        @asset(materialization="table")
        def my_asset():
            return [{"id": 1}]

        assert isinstance(my_asset, Asset)
        assert my_asset.materialization == MaterializationStrategy.TABLE

    def test_asset_with_view_materialization(self):
        """Test @asset decorator with view materialization."""

        @asset(materialization="view")
        def my_asset():
            return [{"id": 1}]

        assert isinstance(my_asset, Asset)
        assert my_asset.materialization == MaterializationStrategy.VIEW

    def test_asset_with_file_materialization(self):
        """Test @asset decorator with file materialization."""

        @asset(materialization="file")
        def my_asset():
            return [{"id": 1}]

        assert isinstance(my_asset, Asset)
        assert my_asset.materialization == MaterializationStrategy.FILE

    def test_asset_with_incremental_materialization(self):
        """Test @asset decorator with incremental materialization."""

        @asset(materialization="incremental", config={"incremental_key": "date"})
        def my_asset():
            return [{"date": "2024-01-01", "value": 100}]

        assert isinstance(my_asset, Asset)
        assert my_asset.materialization == MaterializationStrategy.INCREMENTAL
        assert my_asset.config["incremental_key"] == "date"

    def test_asset_with_materialization_enum(self):
        """Test @asset decorator with MaterializationStrategy enum."""

        @asset(materialization=MaterializationStrategy.TABLE)
        def my_asset():
            return [{"id": 1}]

        assert isinstance(my_asset, Asset)
        assert my_asset.materialization == MaterializationStrategy.TABLE

    def test_asset_default_materialization(self):
        """Test @asset decorator defaults to table materialization."""

        @asset
        def my_asset():
            return [{"id": 1}]

        assert isinstance(my_asset, Asset)
        assert my_asset.materialization == MaterializationStrategy.TABLE

    def test_asset_invalid_materialization(self):
        """Test @asset decorator raises error for invalid materialization."""
        with pytest.raises(ValueError, match="Invalid materialization strategy"):

            @asset(materialization="invalid_strategy")
            def my_asset():
                return [{"id": 1}]

    def test_asset_case_insensitive_materialization(self):
        """Test @asset decorator handles case-insensitive materialization."""

        @asset(materialization="TABLE")
        def my_asset():
            return [{"id": 1}]

        assert my_asset.materialization == MaterializationStrategy.TABLE

        @asset(materialization="View")
        def my_asset2():
            return [{"id": 1}]

        assert my_asset2.materialization == MaterializationStrategy.VIEW


# =============================================================================
# Execution Engine Tests
# =============================================================================


class TestExecutionWithMaterialization:
    """Tests for execution engine with materialization strategies."""

    def test_execute_table_asset(self):
        """Test executing an asset with table materialization."""
        operator = Operator(
            name="test_op",
            operator_type=OperatorType.SOURCE,
            fn=lambda _data, _ctx: [{"id": 1}, {"id": 2}],
        )

        table_asset = Asset(
            name="test_table",
            asset_type=AssetType.MEMORY,
            uri="memory://test_table",
            operator=operator,
            materialization=MaterializationStrategy.TABLE,
        )

        graph = AssetGraph(name="test_graph", assets=(table_asset,))
        engine = ExecutionEngine()
        context = PipelineContext(pipeline_id="test", run_id="test_run")

        result = engine.execute(graph, context=context)

        assert result.success
        assert "test_table" in result.asset_results
        asset_result = result.asset_results["test_table"]
        assert asset_result.success
        assert asset_result.data == [{"id": 1}, {"id": 2}]

    def test_execute_view_asset_skips_materialization(self):
        """Test executing an asset with view materialization skips storage."""
        operator = Operator(
            name="test_op",
            operator_type=OperatorType.SOURCE,
            fn=lambda _data, _ctx: [{"id": 1}, {"id": 2}],
        )

        view_asset = Asset(
            name="test_view",
            asset_type=AssetType.MEMORY,
            uri="memory://test_view",
            operator=operator,
            materialization=MaterializationStrategy.VIEW,
        )

        graph = AssetGraph(name="test_graph", assets=(view_asset,))
        engine = ExecutionEngine()
        context = PipelineContext(pipeline_id="test", run_id="test_run")

        result = engine.execute(graph, context=context)

        # View strategy should execute successfully but not materialize
        assert result.success
        assert "test_view" in result.asset_results
        asset_result = result.asset_results["test_view"]
        assert asset_result.success
        assert asset_result.data == [{"id": 1}, {"id": 2}]

    def test_execute_file_asset_with_partition_key(self):
        """Test executing an asset with file materialization and partition key."""
        operator = Operator(
            name="test_op",
            operator_type=OperatorType.SOURCE,
            fn=lambda _data, _ctx: [{"id": 1, "date": "2024-01-01"}],
        )

        file_asset = Asset(
            name="test_file",
            asset_type=AssetType.FILE,
            uri="file://test_file.json",
            operator=operator,
            materialization=MaterializationStrategy.FILE,
            partition_key="date",
        )

        graph = AssetGraph(name="test_graph", assets=(file_asset,))
        engine = ExecutionEngine()
        context = PipelineContext(pipeline_id="test", run_id="test_run")

        result = engine.execute(graph, context=context)

        assert result.success
        asset_result = result.asset_results["test_file"]
        assert asset_result.success

    def test_execute_incremental_asset_merges_data(self):
        """Test executing an asset with incremental materialization merges data."""
        # First execution
        operator1 = Operator(
            name="test_op1",
            operator_type=OperatorType.SOURCE,
            fn=lambda data, ctx: [
                {"id": 1, "value": "a"},
                {"id": 2, "value": "b"},
            ],
        )

        incremental_asset = Asset(
            name="test_incremental",
            asset_type=AssetType.MEMORY,
            uri="memory://test_incremental",
            operator=operator1,
            materialization=MaterializationStrategy.INCREMENTAL,
            config={"incremental_key": "id"},
        )

        graph = AssetGraph(name="test_graph", assets=(incremental_asset,))
        engine = ExecutionEngine()
        context = PipelineContext(pipeline_id="test", run_id="test_run1")

        result1 = engine.execute(graph, context=context)
        assert result1.success

        # Second execution with updated data
        operator2 = Operator(
            name="test_op2",
            operator_type=OperatorType.SOURCE,
            fn=lambda data, ctx: [
                {"id": 2, "value": "b_updated"},  # Update existing
                {"id": 3, "value": "c"},  # Add new
            ],
        )

        incremental_asset2 = Asset(
            name="test_incremental",
            asset_type=AssetType.MEMORY,
            uri="memory://test_incremental",
            operator=operator2,
            materialization=MaterializationStrategy.INCREMENTAL,
            config={"incremental_key": "id"},
        )

        graph2 = AssetGraph(name="test_graph", assets=(incremental_asset2,))
        context2 = PipelineContext(pipeline_id="test", run_id="test_run2")

        result2 = engine.execute(graph2, context=context2)
        assert result2.success
        # Note: The actual merging happens in the strategy, which is tested
        # in unit tests. This integration test ensures execution doesn't fail.


# =============================================================================
# Complex Scenario Tests
# =============================================================================


class TestComplexScenarios:
    """Tests for complex scenarios with materialization."""

    def test_mixed_materialization_strategies(self):
        """Test executing assets with different materialization strategies."""
        # Create assets with different strategies
        table_op = Operator(
            name="table_op",
            operator_type=OperatorType.SOURCE,
            fn=lambda data, ctx: [{"id": 1}],
        )
        view_op = Operator(
            name="view_op",
            operator_type=OperatorType.TRANSFORM,
            fn=lambda _data, _ctx: [{"id": 1, "doubled": 2}],
        )

        table_asset = Asset(
            name="source_table",
            asset_type=AssetType.MEMORY,
            uri="memory://source",
            operator=table_op,
            materialization=MaterializationStrategy.TABLE,
        )

        view_asset = Asset(
            name="computed_view",
            asset_type=AssetType.MEMORY,
            uri="memory://view",
            operator=view_op,
            materialization=MaterializationStrategy.VIEW,
        )

        graph = AssetGraph(
            name="mixed_graph",
            assets=(table_asset, view_asset),
            dependencies={"computed_view": ("source_table",)},
        )

        engine = ExecutionEngine()
        context = PipelineContext(pipeline_id="test", run_id="test_run")

        result = engine.execute(graph, context=context)

        assert result.success
        assert len(result.asset_results) == 2
        assert result.assets_succeeded == 2
