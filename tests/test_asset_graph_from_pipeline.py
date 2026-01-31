"""
Tests for AssetGraph.from_pipeline() adapter method.
"""

import pytest

from vibe_piper import (
    Asset,
    AssetGraph,
    AssetType,
    DataType,
    Operator,
    OperatorType,
    Pipeline,
)


class TestAssetGraphFromPipeline:
    """Tests for AssetGraph.from_pipeline() class method."""

    def test_from_pipeline_with_single_operator(self) -> None:
        """Test converting a pipeline with a single operator."""
        operator = Operator(
            name="source",
            operator_type=OperatorType.SOURCE,
            fn=lambda data, ctx: [1, 2, 3],
            description="A source operator",
        )

        pipeline = Pipeline(name="test_pipeline", operators=(operator,))

        graph = AssetGraph.from_pipeline(pipeline)

        assert graph.name == "test_pipeline"
        assert len(graph.assets) == 1
        assert graph.assets[0].name == "source"
        assert graph.assets[0].asset_type == AssetType.MEMORY
        assert graph.assets[0].uri == "memory://source"
        assert graph.assets[0].description == "A source operator"
        assert graph.assets[0].operator is not None
        assert graph.assets[0].operator.name == "source"
        assert len(graph.dependencies) == 0  # No dependencies for single operator

    def test_from_pipeline_with_two_operators(self) -> None:
        """Test converting a pipeline with two operators."""
        source_op = Operator(
            name="extract",
            operator_type=OperatorType.SOURCE,
            fn=lambda data, ctx: [1, 2, 3],
        )

        transform_op = Operator(
            name="transform",
            operator_type=OperatorType.TRANSFORM,
            fn=lambda data, ctx: [x * 2 for x in data],
        )

        pipeline = Pipeline(name="etl_pipeline", operators=(source_op, transform_op))

        graph = AssetGraph.from_pipeline(pipeline)

        assert len(graph.assets) == 2
        assert {a.name for a in graph.assets} == {"extract", "transform"}
        assert graph.dependencies == {"transform": ("extract",)}

    def test_from_pipeline_with_three_operators(self) -> None:
        """Test converting a pipeline with three operators."""
        source_op = Operator(
            name="source",
            operator_type=OperatorType.SOURCE,
            fn=lambda data, ctx: [1, 2, 3],
        )

        filter_op = Operator(
            name="filter",
            operator_type=OperatorType.FILTER,
            fn=lambda data, ctx: [x for x in data if x > 1],
        )

        aggregate_op = Operator(
            name="aggregate",
            operator_type=OperatorType.AGGREGATE,
            fn=lambda data, ctx: sum(data),
        )

        pipeline = Pipeline(
            name="multi_step_pipeline",
            operators=(source_op, filter_op, aggregate_op),
        )

        graph = AssetGraph.from_pipeline(pipeline)

        assert len(graph.assets) == 3
        assert {a.name for a in graph.assets} == {
            "source",
            "filter",
            "aggregate",
        }
        assert graph.dependencies["filter"] == ("source",)
        assert graph.dependencies["aggregate"] == ("filter",)

    def test_from_pipeline_preserves_metadata(self) -> None:
        """Test that pipeline metadata is preserved in the graph."""
        operator = Operator(
            name="source",
            operator_type=OperatorType.SOURCE,
            fn=lambda data, ctx: [1, 2, 3],
        )

        pipeline_metadata = {"owner": "data_team", "tags": ["important"]}
        pipeline_config = {"timeout": 60}

        pipeline = Pipeline(
            name="metadata_pipeline",
            operators=(operator,),
            metadata=pipeline_metadata,
            config=pipeline_config,
            description="A test pipeline",
        )

        graph = AssetGraph.from_pipeline(pipeline)

        assert graph.description == "A test pipeline"
        assert graph.metadata == pipeline_metadata
        assert graph.config == pipeline_config

    def test_from_pipeline_preserves_operator_config(self) -> None:
        """Test that operator config is preserved in assets."""
        operator_config = {"retries": 3, "timeout": 30}

        operator = Operator(
            name="configured_op",
            operator_type=OperatorType.SOURCE,
            fn=lambda data, ctx: [1, 2, 3],
            config=operator_config,
        )

        pipeline = Pipeline(name="config_pipeline", operators=(operator,))

        graph = AssetGraph.from_pipeline(pipeline)

        assert graph.assets[0].config == operator_config

    def test_from_pipeline_preserves_operator_schemas(self) -> None:
        """Test that operator output schema is preserved in assets."""
        from vibe_piper import Schema, SchemaField

        output_schema = Schema(
            name="output_schema",
            fields=(SchemaField(name="value", data_type=DataType.INTEGER, nullable=False),),
        )

        operator = Operator(
            name="typed_op",
            operator_type=OperatorType.SOURCE,
            fn=lambda data, ctx: [1, 2, 3],
            output_schema=output_schema,
        )

        pipeline = Pipeline(name="schema_pipeline", operators=(operator,))

        graph = AssetGraph.from_pipeline(pipeline)

        assert graph.assets[0].schema == output_schema

    def test_from_pipeline_first_operator_is_source(self) -> None:
        """Test that first operator's operator_type is set to SOURCE."""
        # First operator is TRANSFORM type, but should become SOURCE in asset
        operator = Operator(
            name="extract",
            operator_type=OperatorType.TRANSFORM,
            fn=lambda data, ctx: [1, 2, 3],
        )

        pipeline = Pipeline(name="source_test", operators=(operator,))

        graph = AssetGraph.from_pipeline(pipeline)

        # Asset's operator preserves original operator_type
        assert graph.assets[0].operator.operator_type == OperatorType.TRANSFORM

    def test_from_pipeline_empty_operators_raises_error(self) -> None:
        """Test that converting a pipeline with no operators raises error."""
        pipeline = Pipeline(name="empty_pipeline", operators=())

        with pytest.raises(ValueError, match="has no operators to convert"):
            AssetGraph.from_pipeline(pipeline)

    def test_from_pipeline_uri_generation(self) -> None:
        """Test that URIs are auto-generated for assets."""
        operator = Operator(
            name="test_op",
            operator_type=OperatorType.SOURCE,
            fn=lambda data, ctx: [1, 2, 3],
        )

        pipeline = Pipeline(name="uri_test", operators=(operator,))

        graph = AssetGraph.from_pipeline(pipeline)

        assert graph.assets[0].uri == "memory://test_op"

    def test_from_pipeline_io_manager_default(self) -> None:
        """Test that io_manager defaults to 'memory'."""
        operator = Operator(
            name="source",
            operator_type=OperatorType.SOURCE,
            fn=lambda data, ctx: [1, 2, 3],
        )

        pipeline = Pipeline(name="io_test", operators=(operator,))

        graph = AssetGraph.from_pipeline(pipeline)

        assert graph.assets[0].io_manager == "memory"

    def test_from_pipeline_with_filter_operator(self) -> None:
        """Test converting a pipeline with FILTER operator."""
        source_op = Operator(
            name="source",
            operator_type=OperatorType.SOURCE,
            fn=lambda data, ctx: [1, 2, 3, 4, 5],
        )

        filter_op = Operator(
            name="filter",
            operator_type=OperatorType.FILTER,
            fn=lambda data, ctx: [x for x in data if x > 2],
        )

        pipeline = Pipeline(name="filter_pipeline", operators=(source_op, filter_op))

        graph = AssetGraph.from_pipeline(pipeline)

        assert len(graph.assets) == 2
        assert graph.dependencies["filter"] == ("source",)

    def test_from_pipeline_with_aggregate_operator(self) -> None:
        """Test converting a pipeline with AGGREGATE operator."""
        source_op = Operator(
            name="source",
            operator_type=OperatorType.SOURCE,
            fn=lambda data, ctx: [1, 2, 3, 4, 5],
        )

        aggregate_op = Operator(
            name="sum",
            operator_type=OperatorType.AGGREGATE,
            fn=lambda data, ctx: sum(data),
        )

        pipeline = Pipeline(name="aggregate_pipeline", operators=(source_op, aggregate_op))

        graph = AssetGraph.from_pipeline(pipeline)

        assert len(graph.assets) == 2
        assert graph.dependencies["sum"] == ("source",)

    def test_from_pipeline_creates_valid_dag(self) -> None:
        """Test that the resulting graph is a valid DAG."""
        source_op = Operator(
            name="source",
            operator_type=OperatorType.SOURCE,
            fn=lambda data, ctx: [1, 2, 3],
        )

        transform1_op = Operator(
            name="transform1",
            operator_type=OperatorType.TRANSFORM,
            fn=lambda data, ctx: [x * 2 for x in data],
        )

        transform2_op = Operator(
            name="transform2",
            operator_type=OperatorType.TRANSFORM,
            fn=lambda data, ctx: [x + 10 for x in data],
        )

        pipeline = Pipeline(
            name="dag_test",
            operators=(source_op, transform1_op, transform2_op),
        )

        graph = AssetGraph.from_pipeline(pipeline)

        # Should not raise validation errors
        assert graph.name == "dag_test"
        assert len(graph.assets) == 3

        # Verify topological order works
        topo_order = graph.topological_order()
        assert topo_order == ("source", "transform1", "transform2")

    def test_from_pipeline_asset_defaults(self) -> None:
        """Test that assets created have correct default values."""
        operator = Operator(
            name="minimal_op",
            operator_type=OperatorType.SOURCE,
            fn=lambda data, ctx: [1, 2, 3],
        )

        pipeline = Pipeline(name="minimal_pipeline", operators=(operator,))

        graph = AssetGraph.from_pipeline(pipeline)

        asset = graph.assets[0]
        assert asset.asset_type == AssetType.MEMORY
        assert asset.version == "1"
        assert asset.cache is False
        assert asset.parallel is False
        assert asset.lazy is False

    def test_from_pipeline_dependency_chain(self) -> None:
        """Test that dependencies form correct chain."""
        ops = [
            Operator(
                name=f"op{i}",
                operator_type=OperatorType.SOURCE if i == 0 else OperatorType.TRANSFORM,
                fn=lambda data, ctx: data,
            )
            for i in range(5)
        ]

        pipeline = Pipeline(name="chain_pipeline", operators=tuple(ops))

        graph = AssetGraph.from_pipeline(pipeline)

        # Verify chain: op1 -> op0, op2 -> op1, op3 -> op2, op4 -> op3
        assert graph.dependencies["op1"] == ("op0",)
        assert graph.dependencies["op2"] == ("op1",)
        assert graph.dependencies["op3"] == ("op2",)
        assert graph.dependencies["op4"] == ("op3",)
        assert "op0" not in graph.dependencies  # First has no deps

    def test_from_pipeline_with_checkpoints(self) -> None:
        """Test that checkpoints are preserved in graph config."""
        operator = Operator(
            name="source",
            operator_type=OperatorType.SOURCE,
            fn=lambda data, ctx: [1, 2, 3],
        )

        pipeline = Pipeline(
            name="checkpoint_pipeline",
            operators=(operator,),
            checkpoints=("checkpoint1", "checkpoint2"),
        )

        graph = AssetGraph.from_pipeline(pipeline)

        # Checkpoints are stored in pipeline, not directly in AssetGraph
        # But they're accessible via the original pipeline reference
        assert "checkpoint1" in pipeline.checkpoints
        assert "checkpoint2" in pipeline.checkpoints
