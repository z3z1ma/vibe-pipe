"""
Tests for declarative pipeline syntax.
"""

import pytest

from vibe_piper import (
    Asset,
    AssetGraph,
    AssetType,
    ExecutionEngine,
    OperatorType,
    PipelineBuilder,
    PipelineDefContext,
    build_pipeline,
)


class TestPipelineBuilder:
    """Tests for PipelineBuilder class."""

    def test_create_pipeline_builder(self) -> None:
        """Test creating a PipelineBuilder."""
        builder = PipelineBuilder("test_pipeline")
        assert builder.name == "test_pipeline"
        assert builder.description is None

    def test_create_pipeline_builder_with_description(self) -> None:
        """Test creating a PipelineBuilder with description."""
        builder = PipelineBuilder("test_pipeline", description="A test pipeline")
        assert builder.name == "test_pipeline"
        assert builder.description == "A test pipeline"

    def test_add_single_asset(self) -> None:
        """Test adding a single asset to the pipeline."""
        from vibe_piper import PipelineContext

        builder = PipelineBuilder("test_pipeline")

        def source_fn(ctx: PipelineContext) -> list[int]:
            return [1, 2, 3]

        builder.asset(name="source", fn=source_fn)

        graph = builder.build()
        assert len(graph.assets) == 1
        assert graph.assets[0].name == "source"

    def test_add_multiple_assets(self) -> None:
        """Test adding multiple assets to the pipeline."""
        from vibe_piper import PipelineContext

        builder = PipelineBuilder("test_pipeline")

        builder.asset(name="source", fn=lambda ctx: [1, 2, 3])
        builder.asset(name="derived", fn=lambda data, ctx: [x * 2 for x in data])

        graph = builder.build()
        assert len(graph.assets) == 2
        assert {a.name for a in graph.assets} == {"source", "derived"}

    def test_add_asset_with_dependencies(self) -> None:
        """Test adding assets with dependencies."""
        builder = PipelineBuilder("test_pipeline")

        builder.asset(name="source", fn=lambda ctx: [1, 2, 3])
        builder.asset(
            name="derived",
            fn=lambda data, ctx: [x * 2 for x in data],
            depends_on=["source"],
        )

        graph = builder.build()
        assert graph.dependencies == {"derived": ("source",)}

    def test_add_asset_with_multiple_dependencies(self) -> None:
        """Test adding an asset with multiple dependencies."""
        builder = PipelineBuilder("test_pipeline")

        builder.asset(name="source1", fn=lambda ctx: [1, 2, 3])
        builder.asset(name="source2", fn=lambda ctx: [4, 5, 6])
        builder.asset(
            name="combined",
            fn=lambda data, ctx: data,
            depends_on=["source1", "source2"],
        )

        graph = builder.build()
        assert set(graph.dependencies["combined"]) == {"source1", "source2"}

    def test_method_chaining(self) -> None:
        """Test that asset() returns self for method chaining."""
        builder = PipelineBuilder("test_pipeline")

        result = builder.asset(name="asset1", fn=lambda ctx: [1, 2, 3])

        assert result is builder

    def test_fluent_interface(self) -> None:
        """Test using fluent interface to build a pipeline."""
        graph = (
            PipelineBuilder("test_pipeline")
            .asset(name="source", fn=lambda ctx: [1, 2, 3])
            .asset(
                name="derived",
                fn=lambda data, ctx: [x * 2 for x in data],
                depends_on=["source"],
            )
            .build()
        )

        assert len(graph.assets) == 2
        assert graph.dependencies == {"derived": ("source",)}

    def test_duplicate_asset_name_raises_error(self) -> None:
        """Test that adding a duplicate asset name raises an error."""
        builder = PipelineBuilder("test_pipeline")

        builder.asset(name="source", fn=lambda ctx: [1, 2, 3])

        with pytest.raises(ValueError, match="already exists"):
            builder.asset(name="source", fn=lambda ctx: [4, 5, 6])

    def test_build_creates_asset_graph(self) -> None:
        """Test that build() creates an AssetGraph."""
        builder = PipelineBuilder("test_pipeline")

        builder.asset(name="source", fn=lambda ctx: [1, 2, 3])

        graph = builder.build()
        assert isinstance(graph, AssetGraph)
        assert graph.name == "test_pipeline"

    def test_asset_with_custom_type(self) -> None:
        """Test adding an asset with a custom asset type."""
        builder = PipelineBuilder("test_pipeline")

        builder.asset(
            name="source",
            fn=lambda ctx: [1, 2, 3],
            asset_type=AssetType.STREAM,
        )

        graph = builder.build()
        assert graph.assets[0].asset_type == AssetType.STREAM

    def test_asset_with_custom_uri(self) -> None:
        """Test adding an asset with a custom URI."""
        builder = PipelineBuilder("test_pipeline")

        custom_uri = "custom://my_asset"
        builder.asset(
            name="source",
            fn=lambda ctx: [1, 2, 3],
            uri=custom_uri,
        )

        graph = builder.build()
        assert graph.assets[0].uri == custom_uri

    def test_asset_with_metadata(self) -> None:
        """Test adding an asset with metadata."""
        builder = PipelineBuilder("test_pipeline")

        metadata = {"owner": "data_team", "tags": ["important"]}
        builder.asset(
            name="source",
            fn=lambda ctx: [1, 2, 3],
            metadata=metadata,
        )

        graph = builder.build()
        assert graph.assets[0].metadata == metadata

    def test_execute_pipeline_built_with_builder(self) -> None:
        """Test executing a pipeline built with PipelineBuilder."""
        from vibe_piper import PipelineContext

        builder = PipelineBuilder("test_pipeline")

        builder.asset(name="source", fn=lambda ctx: [1, 2, 3])
        builder.asset(
            name="derived",
            fn=lambda data, ctx: [x * 2 for x in data],
            depends_on=["source"],
        )

        graph = builder.build()
        engine = ExecutionEngine()
        result = engine.execute(graph)

        assert result.success is True
        assert result.assets_executed == 2


class TestBuildPipelineFunction:
    """Tests for build_pipeline() function."""

    def test_build_pipeline_returns_builder(self) -> None:
        """Test that build_pipeline returns a PipelineBuilder."""
        builder = build_pipeline("test_pipeline")
        assert isinstance(builder, PipelineBuilder)
        assert builder.name == "test_pipeline"

    def test_build_pipeline_with_description(self) -> None:
        """Test build_pipeline with description."""
        builder = build_pipeline("test_pipeline", description="My pipeline")
        assert builder.description == "My pipeline"

    def test_build_pipeline_full_example(self) -> None:
        """Test building a complete pipeline with build_pipeline()."""
        graph = (
            build_pipeline("data_processing")
            .asset("raw_data", lambda ctx: [1, 2, 3])
            .asset(
                "processed_data",
                lambda data, ctx: [x * 2 for x in data],
                depends_on=["raw_data"],
            )
            .build()
        )

        assert isinstance(graph, AssetGraph)
        assert graph.name == "data_processing"
        assert len(graph.assets) == 2


class TestPipelineDefContext:
    """Tests for PipelineDefContext class."""

    def test_create_context(self) -> None:
        """Test creating a PipelineDefContext."""
        context = PipelineDefContext("test_pipeline")
        assert context._builder.name == "test_pipeline"

    def test_context_manager_usage(self) -> None:
        """Test using PipelineDefContext as a context manager."""
        with PipelineDefContext("test_pipeline") as ctx:
            assert isinstance(ctx, PipelineDefContext)

    def test_add_asset_via_decorator(self) -> None:
        """Test adding assets using the decorator syntax."""
        from vibe_piper import PipelineContext

        with PipelineDefContext("test_pipeline") as pipeline:
            @pipeline.asset()
            def source(ctx: PipelineContext) -> list[int]:
                return [1, 2, 3]

        graph = pipeline.build()
        assert len(graph.assets) == 1
        assert graph.assets[0].name == "source"

    def test_add_asset_with_name_override(self) -> None:
        """Test adding an asset with a custom name."""
        from vibe_piper import PipelineContext as PCtx

        with PipelineDefContext("test_pipeline") as pipeline:
            @pipeline.asset(name="custom_name")
            def source(ctx: PCtx) -> list[int]:
                return [1, 2, 3]

        graph = pipeline.build()
        assert graph.assets[0].name == "custom_name"

    def test_add_asset_with_dependencies(self) -> None:
        """Test adding assets with dependencies using decorator syntax."""
        from vibe_piper import PipelineContext

        with PipelineDefContext("test_pipeline") as pipeline:
            @pipeline.asset()
            def source(ctx: PipelineContext) -> list[int]:
                return [1, 2, 3]

            @pipeline.asset(depends_on=["source"])
            def derived(data: list[int], ctx: PipelineContext) -> list[int]:
                return [x * 2 for x in data]

        graph = pipeline.build()
        assert len(graph.assets) == 2
        assert graph.dependencies == {"derived": ("source",)}

    def test_add_multiple_assets_in_context(self) -> None:
        """Test adding multiple assets within a context."""
        from vibe_piper import PipelineContext

        with PipelineDefContext("test_pipeline") as pipeline:
            @pipeline.asset()
            def source1(ctx: PipelineContext) -> list[int]:
                return [1, 2, 3]

            @pipeline.asset()
            def source2(ctx: PipelineContext) -> list[int]:
                return [4, 5, 6]

            @pipeline.asset(depends_on=["source1", "source2"])
            def combined(data: list[int], ctx: PipelineContext) -> list[int]:
                return data

        graph = pipeline.build()
        assert len(graph.assets) == 3
        assert set(graph.dependencies["combined"]) == {"source1", "source2"}

    def test_decorator_without_parentheses(self) -> None:
        """Test using @pipeline.asset without parentheses."""
        from vibe_piper import PipelineContext

        with PipelineDefContext("test_pipeline") as pipeline:
            @pipeline.asset
            def source(ctx: PipelineContext) -> list[int]:
                return [1, 2, 3]

        graph = pipeline.build()
        assert len(graph.assets) == 1
        assert graph.assets[0].name == "source"

    def test_asset_with_custom_type_in_context(self) -> None:
        """Test adding an asset with custom type in context."""
        from vibe_piper import PipelineContext

        with PipelineDefContext("test_pipeline") as pipeline:
            @pipeline.asset(asset_type=AssetType.FILE)
            def source(ctx: PipelineContext) -> list[int]:
                return [1, 2, 3]

        graph = pipeline.build()
        assert graph.assets[0].asset_type == AssetType.FILE

    def test_asset_with_metadata_in_context(self) -> None:
        """Test adding an asset with metadata in context."""
        from vibe_piper import PipelineContext

        metadata = {"owner": "team"}

        with PipelineDefContext("test_pipeline") as pipeline:
            @pipeline.asset(metadata=metadata)
            def source(ctx: PipelineContext) -> list[int]:
                return [1, 2, 3]

        graph = pipeline.build()
        assert graph.assets[0].metadata == metadata

    def test_build_from_context(self) -> None:
        """Test building a graph from PipelineDefContext."""
        from vibe_piper import PipelineContext

        with PipelineDefContext("test_pipeline") as pipeline:
            @pipeline.asset()
            def source(ctx: PipelineContext) -> list[int]:
                return [1, 2, 3]

        graph = pipeline.build()
        assert isinstance(graph, AssetGraph)
        assert graph.name == "test_pipeline"

    def test_execute_pipeline_from_context(self) -> None:
        """Test executing a pipeline built from PipelineDefContext."""
        from vibe_piper import PipelineContext

        with PipelineDefContext("test_pipeline") as pipeline:
            @pipeline.asset()
            def source(ctx: PipelineContext) -> list[int]:
                return [1, 2, 3]

            @pipeline.asset(depends_on=["source"])
            def derived(data: list[int], ctx: PipelineContext) -> list[int]:
                return [x * 2 for x in data]

        graph = pipeline.build()
        engine = ExecutionEngine()
        result = engine.execute(graph)

        assert result.success is True
        assert result.assets_executed == 2

    def test_complex_pipeline_in_context(self) -> None:
        """Test building a more complex pipeline with multiple levels."""
        from vibe_piper import PipelineContext

        with PipelineDefContext("complex_pipeline") as pipeline:
            @pipeline.asset()
            def raw(ctx: PipelineContext) -> list[int]:
                return [1, 2, 3, 4, 5]

            @pipeline.asset(depends_on=["raw"])
            def filtered(data: list[int], ctx: PipelineContext) -> list[int]:
                return [x for x in data if x > 2]

            @pipeline.asset(depends_on=["filtered"])
            def transformed(data: list[int], ctx: PipelineContext) -> list[int]:
                return [x * 2 for x in data]

        graph = pipeline.build()
        engine = ExecutionEngine()
        result = engine.execute(graph)

        assert result.success is True
        assert result.assets_executed == 3

        # Verify the transformations worked
        assert result.asset_results["raw"].data == [1, 2, 3, 4, 5]
        assert result.asset_results["filtered"].data == [3, 4, 5]
        assert result.asset_results["transformed"].data == [6, 8, 10]


class TestIntegration:
    """Integration tests for declarative pipeline syntax."""

    def test_builder_vs_context_equivalence(self) -> None:
        """Test that builder and context produce equivalent graphs."""
        # Build with builder
        builder_graph = (
            build_pipeline("test")
            .asset("source", lambda ctx: [1, 2, 3])
            .asset("derived", lambda data, ctx: [x * 2 for x in data], depends_on=["source"])
            .build()
        )

        # Build with context
        from vibe_piper import PipelineContext

        with PipelineDefContext("test") as pipeline:
            @pipeline.asset()
            def source(ctx: PipelineContext) -> list[int]:
                return [1, 2, 3]

            @pipeline.asset(depends_on=["source"])
            def derived(data: list[int], ctx: PipelineContext) -> list[int]:
                return [x * 2 for x in data]

        context_graph = pipeline.build()

        # Both should have same structure
        assert len(builder_graph.assets) == len(context_graph.assets)
        assert builder_graph.dependencies == context_graph.dependencies

    def test_diamond_dependency_pattern(self) -> None:
        """Test creating a diamond dependency pattern."""
        #     a
        #    / \
        #   b   c
        #    \ /
        #     d
        from vibe_piper import PipelineContext

        with PipelineDefContext("diamond") as pipeline:
            @pipeline.asset()
            def a(ctx: PipelineContext) -> list[int]:
                return [1, 2, 3]

            @pipeline.asset(depends_on=["a"])
            def b(data: list[int], ctx: PipelineContext) -> list[int]:
                return [x + 1 for x in data]

            @pipeline.asset(depends_on=["a"])
            def c(data: list[int], ctx: PipelineContext) -> list[int]:
                return [x + 2 for x in data]

            @pipeline.asset(depends_on=["b", "c"])
            def d(data: list[int], ctx: PipelineContext) -> list[int]:
                return [x * 2 for x in data]

        graph = pipeline.build()
        engine = ExecutionEngine()
        result = engine.execute(graph)

        assert result.success is True
        assert result.assets_executed == 4

    def test_pipeline_with_no_dependencies(self) -> None:
        """Test a pipeline with independent assets (no dependencies)."""
        from vibe_piper import PipelineContext

        with PipelineDefContext("independent") as pipeline:
            @pipeline.asset()
            def asset1(ctx: PipelineContext) -> list[int]:
                return [1, 2, 3]

            @pipeline.asset()
            def asset2(ctx: PipelineContext) -> list[int]:
                return [4, 5, 6]

            @pipeline.asset()
            def asset3(ctx: PipelineContext) -> list[int]:
                return [7, 8, 9]

        graph = pipeline.build()
        engine = ExecutionEngine()
        result = engine.execute(graph)

        assert result.success is True
        assert result.assets_executed == 3
        assert len(graph.dependencies) == 0

    def test_declarative_vs_imperative_comparison(self) -> None:
        """Compare declarative syntax with imperative construction."""
        # Imperative (original) way
        from vibe_piper import Operator, PipelineContext

        source_op = Operator(
            name="source",
            operator_type=OperatorType.SOURCE,
            fn=lambda data, ctx: [1, 2, 3],
        )
        source_asset = Asset(
            name="source",
            asset_type=AssetType.MEMORY,
            uri="memory://source",
            operator=source_op,
        )

        derived_op = Operator(
            name="derived",
            operator_type=OperatorType.TRANSFORM,
            fn=lambda data, ctx: [x * 2 for x in data],
        )
        derived_asset = Asset(
            name="derived",
            asset_type=AssetType.MEMORY,
            uri="memory://derived",
            operator=derived_op,
        )

        imperative_graph = AssetGraph(
            name="test",
            assets=(source_asset, derived_asset),
            dependencies={"derived": ("source",)},
        )

        # Declarative way
        declarative_graph = (
            build_pipeline("test")
            .asset("source", lambda ctx: [1, 2, 3])
            .asset("derived", lambda data, ctx: [x * 2 for x in data], depends_on=["source"])
            .build()
        )

        # Both should produce similar structure
        assert len(imperative_graph.assets) == len(declarative_graph.assets)
        assert imperative_graph.dependencies == declarative_graph.dependencies
