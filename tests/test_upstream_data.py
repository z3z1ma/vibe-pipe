"""
Tests for UpstreamData multi-upstream contract.

This module tests the new UpstreamData type that enables assets
to receive data from multiple upstream dependencies.
"""

from typing import Any

import pytest

from vibe_piper import (
    Asset,
    AssetGraph,
    AssetResult,
    AssetType,
    DefaultExecutor,
    ExecutionEngine,
    Operator,
    OperatorType,
    PipelineContext,
    UpstreamData,
)


class TestUpstreamData:
    """Tests for UpstreamData container type."""

    def test_upstream_data_init(self) -> None:
        """Test creating UpstreamData with upstream results."""
        raw_data = {
            "source_a": [1, 2, 3],
            "source_b": [4, 5, 6],
        }
        upstream_data = UpstreamData(raw=raw_data)

        assert upstream_data.raw == raw_data

    def test_upstream_data_keys_property(self) -> None:
        """Test UpstreamData.keys property."""
        raw_data = {
            "first_asset": "data1",
            "second_asset": "data2",
            "third_asset": "data3",
        }
        upstream_data = UpstreamData(raw=raw_data)

        keys = upstream_data.keys
        assert isinstance(keys, tuple)
        assert len(keys) == 3
        assert "first_asset" in keys
        assert "second_asset" in keys
        assert "third_asset" in keys

    def test_upstream_data_getitem(self) -> None:
        """Test UpstreamData dictionary-like access."""
        raw_data = {
            "asset1": "value1",
            "asset2": "value2",
        }
        upstream_data = UpstreamData(raw=raw_data)

        assert upstream_data["asset1"] == "value1"
        assert upstream_data["asset2"] == "value2"

    def test_upstream_data_getitem_with_asset_result(self) -> None:
        """Test UpstreamData unwraps AssetResult.data."""
        # Create an AssetResult as upstream would provide
        asset_result = AssetResult(
            asset_name="upstream_asset",
            success=True,
            data=[1, 2, 3],
        )

        raw_data = {"upstream_asset": asset_result}
        upstream_data = UpstreamData(raw=raw_data)

        # Should extract data from AssetResult
        assert upstream_data["upstream_asset"] == [1, 2, 3]

    def test_upstream_data_get(self) -> None:
        """Test UpstreamData.get with default."""
        raw_data = {"asset1": "value1"}
        upstream_data = UpstreamData(raw=raw_data)

        assert upstream_data.get("asset1") == "value1"
        assert upstream_data.get("nonexistent") is None
        assert upstream_data.get("nonexistent", "default") == "default"

    def test_upstream_data_contains(self) -> None:
        """Test UpstreamData __contains__."""
        raw_data = {"asset1": "value1", "asset2": "value2"}
        upstream_data = UpstreamData(raw=raw_data)

        assert "asset1" in upstream_data
        assert "asset2" in upstream_data
        assert "asset3" not in upstream_data

    def test_upstream_data_getitem_key_error(self) -> None:
        """Test UpstreamData __getitem__ raises KeyError for missing asset."""
        raw_data = {"asset1": "value1"}
        upstream_data = UpstreamData(raw=raw_data)

        with pytest.raises(KeyError, match="Upstream asset 'asset2' not found"):
            _ = upstream_data["asset2"]

    def test_upstream_data_as_dict(self) -> None:
        """Test UpstreamData.as_dict returns proper dict."""
        raw_data = {"asset1": "value1", "asset2": "value2"}
        upstream_data = UpstreamData(raw=raw_data)

        result = upstream_data.as_dict()
        assert result == {"asset1": "value1", "asset2": "value2"}
        assert isinstance(result, dict)

    def test_upstream_data_with_empty_mapping(self) -> None:
        """Test UpstreamData with empty mapping (source asset)."""
        upstream_data = UpstreamData(raw={})

        assert upstream_data.keys == ()
        assert upstream_data.as_dict() == {}


class TestMultiUpstreamExecution:
    """Tests for execution with multiple upstream dependencies."""

    def test_executor_with_upstream_data(self) -> None:
        """Test DefaultExecutor accepts UpstreamData."""

        def multi_upstream_op(upstream: UpstreamData, context: PipelineContext) -> Any:
            # Access data from multiple upstreams
            data1 = upstream["source1"]
            data2 = upstream["source2"]
            return data1 + data2

        operator = Operator(
            name="multi_upstream",
            operator_type=OperatorType.TRANSFORM,
            fn=multi_upstream_op,
        )

        asset = Asset(
            name="downstream",
            asset_type=AssetType.MEMORY,
            uri="memory://downstream",
            operator=operator,
        )

        executor = DefaultExecutor()
        context = PipelineContext(pipeline_id="test", run_id="test-run")

        # Simulate upstream results with multiple assets
        upstream_results = UpstreamData(
            raw={
                "source1": AssetResult(
                    asset_name="source1",
                    success=True,
                    data=[1, 2, 3],
                ),
                "source2": AssetResult(
                    asset_name="source2",
                    success=True,
                    data=[4, 5, 6],
                ),
            }
        )

        result = executor.execute(asset, context, upstream_results)

        assert result.success is True
        assert result.data == [1, 2, 3, 4, 5, 6]

    def test_execution_engine_with_multi_upstream_asset(self) -> None:
        """Test ExecutionEngine executes graph with multi-upstream dependencies."""

        def source1_op(data: Any, context: PipelineContext) -> Any:
            return [1, 2, 3]

        def source2_op(data: Any, context: PipelineContext) -> Any:
            return [4, 5, 6]

        def merge_op(upstream: UpstreamData, context: PipelineContext) -> Any:
            # Merge data from multiple upstreams
            data1 = upstream["source1"]
            data2 = upstream["source2"]
            return data1 + data2

        source1_operator = Operator(
            name="source1",
            operator_type=OperatorType.SOURCE,
            fn=source1_op,
        )

        source2_operator = Operator(
            name="source2",
            operator_type=OperatorType.SOURCE,
            fn=source2_op,
        )

        merge_operator = Operator(
            name="merge",
            operator_type=OperatorType.TRANSFORM,
            fn=merge_op,
        )

        source1 = Asset(
            name="source1",
            asset_type=AssetType.MEMORY,
            uri="memory://source1",
            operator=source1_operator,
        )

        source2 = Asset(
            name="source2",
            asset_type=AssetType.MEMORY,
            uri="memory://source2",
            operator=source2_operator,
        )

        merged = Asset(
            name="merged",
            asset_type=AssetType.MEMORY,
            uri="memory://merged",
            operator=merge_operator,
        )

        graph = AssetGraph(
            name="multi_upstream_test",
            assets=(source1, source2, merged),
            dependencies={"merged": ("source1", "source2")},
        )

        engine = ExecutionEngine()
        result = engine.execute(graph)

        assert result.success is True
        assert result.assets_executed == 3

        # Check that merged asset received data from both sources
        merged_result = result.asset_results["merged"]
        assert merged_result.success is True
        assert merged_result.data == [1, 2, 3, 4, 5, 6]

    def test_backward_compatibility_single_upstream(self) -> None:
        """Test that old-style operators still work with single upstream."""

        def old_style_op(data: Any, context: PipelineContext) -> Any:
            # Old-style operator expects raw data, not UpstreamData
            return [x * 2 for x in data]

        operator = Operator(
            name="old_style",
            operator_type=OperatorType.TRANSFORM,
            fn=old_style_op,
        )

        asset = Asset(
            name="downstream",
            asset_type=AssetType.MEMORY,
            uri="memory://downstream",
            operator=operator,
        )

        executor = DefaultExecutor()
        context = PipelineContext(pipeline_id="test", run_id="test-run")

        # Single upstream - executor should unwrap for backward compatibility
        upstream_results = UpstreamData(
            raw={
                "source": AssetResult(
                    asset_name="source",
                    success=True,
                    data=[1, 2, 3],
                )
            }
        )

        result = executor.execute(asset, context, upstream_results)

        assert result.success is True
        # Old-style operator should receive unwrapped data
        assert result.data == [2, 4, 6]

    def test_multi_upstream_with_old_contract_fails_gracefully(self) -> None:
        """Test that multi-upstream with old contract doesn't crash but may not work correctly."""

        def old_style_op(data: Any, context: PipelineContext) -> Any:
            # Old-style operator tries to iterate over data
            # This won't work correctly with UpstreamData but shouldn't crash
            try:
                return [x * 2 for x in data]
            except (TypeError, KeyError):
                # If iteration fails, return None
                return None

        operator = Operator(
            name="old_style_multi",
            operator_type=OperatorType.TRANSFORM,
            fn=old_style_op,
        )

        asset = Asset(
            name="downstream",
            asset_type=AssetType.MEMORY,
            uri="memory://downstream",
            operator=operator,
        )

        executor = DefaultExecutor()
        context = PipelineContext(pipeline_id="test", run_id="test-run")

        # Multiple upstream - can't unwrap for backward compat
        upstream_results = UpstreamData(
            raw={
                "source1": AssetResult(
                    asset_name="source1",
                    success=True,
                    data=[1, 2, 3],
                ),
                "source2": AssetResult(
                    asset_name="source2",
                    success=True,
                    data=[4, 5, 6],
                ),
            }
        )

        # Should succeed but may not produce correct results
        # The operator should handle both TypeError and KeyError
        result = executor.execute(asset, context, upstream_results)

        # Execution succeeds even if operator returns None
        assert result.success is True
        # But data may be None because old contract doesn't work with multi-upstream
        assert result.data is None

    def test_upstream_data_lineage_extraction(self) -> None:
        """Test that lineage is properly extracted from UpstreamData."""

        def op(upstream: UpstreamData, context: PipelineContext) -> Any:
            return "result"

        operator = Operator(
            name="test",
            operator_type=OperatorType.TRANSFORM,
            fn=op,
        )

        asset = Asset(
            name="downstream",
            asset_type=AssetType.MEMORY,
            uri="memory://downstream",
            operator=operator,
        )

        executor = DefaultExecutor()
        context = PipelineContext(pipeline_id="test", run_id="test-run")

        upstream_results = UpstreamData(
            raw={
                "source1": AssetResult(
                    asset_name="source1",
                    success=True,
                    data=[1, 2, 3],
                ),
                "source2": AssetResult(
                    asset_name="source2",
                    success=True,
                    data=[4, 5, 6],
                ),
            }
        )

        result = executor.execute(asset, context, upstream_results)

        # Lineage should include all upstream asset names
        assert result.lineage == ("source1", "source2")

    def test_upstream_data_with_mapping_input(self) -> None:
        """Test that executor handles Mapping[str, Any] input."""

        def new_style_op(upstream: UpstreamData, context: PipelineContext) -> Any:
            return upstream["source"]

        operator = Operator(
            name="new_style",
            operator_type=OperatorType.TRANSFORM,
            fn=new_style_op,
        )

        asset = Asset(
            name="downstream",
            asset_type=AssetType.MEMORY,
            uri="memory://downstream",
            operator=operator,
        )

        executor = DefaultExecutor()
        context = PipelineContext(pipeline_id="test", run_id="test-run")

        # Pass Mapping instead of UpstreamData - should be converted
        upstream_results = {
            "source": AssetResult(
                asset_name="source",
                success=True,
                data=[1, 2, 3],
            )
        }

        result = executor.execute(asset, context, upstream_results)

        assert result.success is True
        assert result.data == [1, 2, 3]
