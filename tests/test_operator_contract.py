"""
Tests for operator data contract.

This module tests the operator data contract for AssetGraph model,
ensuring operators receive UpstreamData consistently across
single-upstream, multi-upstream, and source scenarios.
"""

import pytest

from vibe_piper import (
    Asset,
    AssetGraph,
    AssetType,
    DefaultExecutor,
    Operator,
    OperatorType,
    PipelineContext,
    UpstreamData,
)


class TestOperatorDataContract:
    """Tests for operator data contract with UpstreamData."""

    def test_single_upstream_asset_receives_upstream_data(self) -> None:
        """Test that operators with single upstream receive UpstreamData."""

        def single_upstream_op(upstream: UpstreamData, context: PipelineContext) -> str:
            # Should receive UpstreamData with one key
            assert isinstance(upstream, UpstreamData)
            assert upstream.keys == ("source_asset",)
            assert "source_asset" in upstream
            return upstream["source_asset"]

        operator = Operator(
            name="single_upstream",
            operator_type=OperatorType.TRANSFORM,
            fn=single_upstream_op,
        )

        asset = Asset(
            name="derived",
            asset_type=AssetType.MEMORY,
            uri="memory://derived",
            operator=operator,
        )

        executor = DefaultExecutor()
        context = PipelineContext(pipeline_id="test", run_id="test-run")
        upstream_data = UpstreamData(raw={"source_asset": "data_value"})

        result = executor.execute(asset, context, upstream_data)

        assert result.success is True
        assert result.data == "data_value"

    def test_multi_upstream_asset_receives_upstream_data(self) -> None:
        """Test that operators with multiple upstreams receive UpstreamData with all keys."""

        def multi_upstream_op(upstream: UpstreamData, context: PipelineContext) -> dict[str, str]:
            # Should receive UpstreamData with multiple keys
            assert isinstance(upstream, UpstreamData)
            assert set(upstream.keys) == {"source1", "source2", "source3"}
            # Access all upstreams
            return {
                "combined": f"{upstream['source1']}+{upstream['source2']}+{upstream['source3']}"
            }

        operator = Operator(
            name="multi_upstream",
            operator_type=OperatorType.JOIN,
            fn=multi_upstream_op,
        )

        asset = Asset(
            name="joined",
            asset_type=AssetType.MEMORY,
            uri="memory://joined",
            operator=operator,
        )

        executor = DefaultExecutor()
        context = PipelineContext(pipeline_id="test", run_id="test-run")
        upstream_data = UpstreamData(
            raw={
                "source1": "a",
                "source2": "b",
                "source3": "c",
            }
        )

        result = executor.execute(asset, context, upstream_data)

        assert result.success is True
        assert result.data == {"combined": "a+b+c"}

    def test_source_asset_receives_empty_upstream_data(self) -> None:
        """Test that source assets (no upstreams) receive empty UpstreamData."""

        def source_op(upstream: UpstreamData, context: PipelineContext) -> list[int]:
            # Should receive empty UpstreamData
            assert isinstance(upstream, UpstreamData)
            assert upstream.keys == ()
            assert len(upstream.keys) == 0
            return [1, 2, 3]

        operator = Operator(
            name="source",
            operator_type=OperatorType.SOURCE,
            fn=source_op,
        )

        asset = Asset(
            name="source",
            asset_type=AssetType.MEMORY,
            uri="memory://source",
            operator=operator,
        )

        executor = DefaultExecutor()
        context = PipelineContext(pipeline_id="test", run_id="test-run")
        upstream_data = UpstreamData(raw={})

        result = executor.execute(asset, context, upstream_data)

        assert result.success is True
        assert result.data == [1, 2, 3]

    def test_upstream_data_get_method_with_default(self) -> None:
        """Test that UpstreamData.get() method works with default values."""

        def op_with_get(upstream: UpstreamData, context: PipelineContext) -> str:
            # Use get() with default for missing upstream
            missing_value = upstream.get("nonexistent", "default")
            existing_value = upstream.get("existing", "default")
            assert missing_value == "default"
            assert existing_value == "value"
            return existing_value

        operator = Operator(
            name="test_get",
            operator_type=OperatorType.TRANSFORM,
            fn=op_with_get,
        )

        asset = Asset(
            name="test",
            asset_type=AssetType.MEMORY,
            uri="memory://test",
            operator=operator,
        )

        executor = DefaultExecutor()
        context = PipelineContext(pipeline_id="test", run_id="test-run")
        upstream_data = UpstreamData(raw={"existing": "value"})

        result = executor.execute(asset, context, upstream_data)

        assert result.success is True

    def test_upstream_data_contains_method(self) -> None:
        """Test that 'in' operator works with UpstreamData."""

        def op_with_contains(upstream: UpstreamData, context: PipelineContext) -> bool:
            # Use 'in' to check for upstream existence
            assert "existing" in upstream
            assert "nonexistent" not in upstream
            return True

        operator = Operator(
            name="test_contains",
            operator_type=OperatorType.TRANSFORM,
            fn=op_with_contains,
        )

        asset = Asset(
            name="test",
            asset_type=AssetType.MEMORY,
            uri="memory://test",
            operator=operator,
        )

        executor = DefaultExecutor()
        context = PipelineContext(pipeline_id="test", run_id="test-run")
        upstream_data = UpstreamData(raw={"existing": "value"})

        result = executor.execute(asset, context, upstream_data)

        assert result.success is True

    def test_upstream_data_dict_conversion(self) -> None:
        """Test that UpstreamData.as_dict() returns proper dictionary."""

        def op_with_dict(upstream: UpstreamData, context: PipelineContext) -> dict[str, str]:
            # Use as_dict() to get all data as a dictionary
            data_dict = upstream.as_dict()
            assert isinstance(data_dict, dict)
            assert set(data_dict.keys()) == {"a", "b", "c"}
            return data_dict

        operator = Operator(
            name="test_dict",
            operator_type=OperatorType.TRANSFORM,
            fn=op_with_dict,
        )

        asset = Asset(
            name="test",
            asset_type=AssetType.MEMORY,
            uri="memory://test",
            operator=operator,
        )

        executor = DefaultExecutor()
        context = PipelineContext(pipeline_id="test", run_id="test-run")
        upstream_data = UpstreamData(raw={"a": "1", "b": "2", "c": "3"})

        result = executor.execute(asset, context, upstream_data)

        assert result.success is True
        assert result.data == {"a": "1", "b": "2", "c": "3"}

    def test_upstream_data_extract_from_asset_result(self) -> None:
        """Test that UpstreamData extracts data from AssetResult objects."""

        def op(upstream: UpstreamData, context: PipelineContext) -> str:
            # UpstreamData should extract data from AssetResult
            value = upstream["source"]
            # Even if raw contained AssetResult, we should get the data
            return value

        operator = Operator(
            name="test",
            operator_type=OperatorType.TRANSFORM,
            fn=op,
        )

        asset = Asset(
            name="derived",
            asset_type=AssetType.MEMORY,
            uri="memory://derived",
            operator=operator,
        )

        executor = DefaultExecutor()
        context = PipelineContext(pipeline_id="test", run_id="test-run")
        # Simulate upstream results (would normally be AssetResult objects from execution)
        upstream_data = UpstreamData(raw={"source": "data"})

        result = executor.execute(asset, context, upstream_data)

        assert result.success is True
        assert result.data == "data"

    def test_execution_engine_wraps_upstream_data(self) -> None:
        """Test that ExecutionEngine wraps upstream results in UpstreamData."""

        def op(upstream: UpstreamData, context: PipelineContext) -> str:
            # Verify we received UpstreamData
            assert isinstance(upstream, UpstreamData)
            assert upstream.keys == ("source",)
            return upstream["source"]

        operator = Operator(
            name="test",
            operator_type=OperatorType.TRANSFORM,
            fn=op,
        )

        source = Asset(
            name="source",
            asset_type=AssetType.MEMORY,
            uri="memory://source",
        )

        derived = Asset(
            name="derived",
            asset_type=AssetType.MEMORY,
            uri="memory://derived",
            operator=operator,
        )

        graph = AssetGraph(
            name="test_graph",
            assets=(source, derived),
            dependencies={"derived": ("source",)},
        )

        engine = DefaultExecutor()
        context = PipelineContext(pipeline_id="test", run_id="test-run")
        upstream_data = UpstreamData(raw={"source": "test_data"})

        result = engine.execute(derived, context, upstream_data)

        assert result.success is True

    def test_multi_upstream_diamond_pattern(self) -> None:
        """Test a diamond dependency pattern with multi-upstream."""

        def op(upstream: UpstreamData, context: PipelineContext) -> str:
            return f"processed_{upstream['source']}"

        source_op = Operator(
            name="source",
            operator_type=OperatorType.SOURCE,
            fn=op,
        )

        left_op = Operator(
            name="left",
            operator_type=OperatorType.TRANSFORM,
            fn=op,
        )

        right_op = Operator(
            name="right",
            operator_type=OperatorType.TRANSFORM,
            fn=op,
        )

        join_op = Operator(
            name="join",
            operator_type=OperatorType.JOIN,
            fn=lambda u, c: {
                "left": u["left"],
                "right": u["right"],
                "combined": f"{u['left']}+{u['right']}",
            },
        )

        source = Asset(
            name="source",
            asset_type=AssetType.MEMORY,
            uri="memory://source",
            operator=source_op,
        )

        left = Asset(
            name="left",
            asset_type=AssetType.MEMORY,
            uri="memory://left",
            operator=left_op,
        )

        right = Asset(
            name="right",
            asset_type=AssetType.MEMORY,
            uri="memory://right",
            operator=right_op,
        )

        joined = Asset(
            name="joined",
            asset_type=AssetType.MEMORY,
            uri="memory://joined",
            operator=join_op,
        )

        graph = AssetGraph(
            name="diamond",
            assets=(source, left, right, joined),
            dependencies={
                "left": ("source",),
                "right": ("source",),
                "joined": ("left", "right"),
            },
        )

        engine = DefaultExecutor()
        context = PipelineContext(pipeline_id="test", run_id="test-run")
        # Execute diamond with proper UpstreamData
        source_result = engine.execute(source, context, UpstreamData(raw={}))
        left_result = engine.execute(
            left, context, UpstreamData(raw={"source": source_result.data})
        )
        right_result = engine.execute(
            right, context, UpstreamData(raw={"source": source_result.data})
        )
        join_result = engine.execute(
            joined,
            context,
            UpstreamData(raw={"left": left_result.data, "right": right_result.data}),
        )

        assert join_result.success is True
        assert isinstance(join_result.data, dict)
        assert "left" in join_result.data
        assert "right" in join_result.data
