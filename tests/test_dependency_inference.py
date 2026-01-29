"""
Tests for automatic dependency inference.

This module tests the automatic dependency inference feature that
allows assets to automatically detect their dependencies from function
signatures.
"""

from vibe_piper import (
    PipelineBuilder,
    PipelineDefContext,
    infer_dependencies_from_signature,
)


class TestInferDependenciesFromSignature:
    """Tests for the infer_dependencies_from_signature helper function."""

    def test_infer_single_dependency(self) -> None:
        """Test inferring a single dependency from function signature."""

        def process_data(raw_data, context):
            return [x * 2 for x in raw_data]

        deps = infer_dependencies_from_signature(process_data, known_assets={"raw_data", "other"})
        assert deps == ["raw_data"]

    def test_infer_multiple_dependencies(self) -> None:
        """Test inferring multiple dependencies from function signature."""

        def combine(source1, source2, context):
            return source1 + source2

        deps = infer_dependencies_from_signature(
            combine, known_assets={"source1", "source2", "source3"}
        )
        assert set(deps) == {"source1", "source2"}

    def test_infer_filters_special_parameters(self) -> None:
        """Test that special parameters like 'context' and 'ctx' are filtered out."""

        def process(data, context, ctx):
            return data

        deps = infer_dependencies_from_signature(process, known_assets={"data", "context", "ctx"})
        # Only 'context' and 'ctx' are filtered, 'data' is inferred
        assert deps == ["data"]

    def test_infer_excludes_data_parameter(self) -> None:
        """Test that 'context' and 'ctx' parameters are excluded from inference.

        Note: 'data' is NOT excluded as users may have assets named 'data'.
        """

        def transform(data, upstream, context):
            return upstream

        deps = infer_dependencies_from_signature(transform, known_assets={"data", "upstream"})
        # Both 'data' and 'upstream' should be inferred, 'context' is filtered
        assert set(deps) == {"data", "upstream"}

    def test_infer_with_no_known_assets(self) -> None:
        """Test inference when no known assets are provided."""

        def process(raw_data, context):
            return raw_data

        deps = infer_dependencies_from_signature(process, known_assets=None)
        # Returns all non-special parameters
        assert deps == ["raw_data"]

    def test_infer_with_no_matching_assets(self) -> None:
        """Test inference when no parameters match known assets."""

        def process(foo, bar, context):
            return foo

        deps = infer_dependencies_from_signature(process, known_assets={"other_asset"})
        assert deps == []

    def test_infer_with_varargs(self) -> None:
        """Test that *args and **kwargs are excluded from inference."""

        def process(*args, **kwargs):
            return args

        deps = infer_dependencies_from_signature(process, known_assets={"something"})
        assert deps == []

    def test_infer_from_lambda(self) -> None:
        """Test inference from lambda functions."""
        lambda_fn = lambda raw_data, context: raw_data
        deps = infer_dependencies_from_signature(lambda_fn, known_assets={"raw_data"})
        assert deps == ["raw_data"]


class TestPipelineBuilderAutoInference:
    """Tests for automatic dependency inference in PipelineBuilder."""

    def test_auto_infer_single_dependency(self) -> None:
        """Test automatic inference of a single dependency."""
        builder = PipelineBuilder("test_pipeline")

        # Add source asset
        builder.asset(name="raw_data", fn=lambda ctx: [1, 2, 3])

        # Add dependent asset without specifying depends_on
        builder.asset(
            name="processed",
            fn=lambda raw_data, ctx: [x * 2 for x in raw_data],
        )

        graph = builder.build()
        assert "processed" in graph.dependencies
        assert graph.dependencies["processed"] == ("raw_data",)

    def test_auto_infer_multiple_dependencies(self) -> None:
        """Test automatic inference of multiple dependencies."""
        builder = PipelineBuilder("test_pipeline")

        # Add source assets
        builder.asset(name="source1", fn=lambda ctx: [1, 2, 3])
        builder.asset(name="source2", fn=lambda ctx: [4, 5, 6])

        # Add dependent asset with multiple inferred dependencies
        builder.asset(
            name="combined",
            fn=lambda source1, source2, ctx: source1 + source2,
        )

        graph = builder.build()
        assert "combined" in graph.dependencies
        assert set(graph.dependencies["combined"]) == {"source1", "source2"}

    def test_auto_infer_with_no_dependencies(self) -> None:
        """Test that assets with no matching parameters are sources."""
        builder = PipelineBuilder("test_pipeline")

        builder.asset(
            name="standalone",
            fn=lambda ctx: [1, 2, 3],
        )

        graph = builder.build()
        assert "standalone" not in graph.dependencies

    def test_explicit_depends_on_takes_precedence(self) -> None:
        """Test that explicit depends_on overrides inferred dependencies."""
        builder = PipelineBuilder("test_pipeline")

        builder.asset(name="asset1", fn=lambda ctx: [1])
        builder.asset(name="asset2", fn=lambda ctx: [2])

        # Explicitly specify depends_on, ignoring parameter names
        builder.asset(
            name="dependent",
            fn=lambda asset1, ctx: asset1,
            depends_on=["asset2"],  # Explicit depends_on
        )

        graph = builder.build()
        assert graph.dependencies["dependent"] == ("asset2",)

    def test_auto_infer_filters_special_params(self) -> None:
        """Test that special parameters (context, ctx) are not inferred as dependencies.

        Note: 'data' parameter IS inferred if it matches an asset name.
        """
        builder = PipelineBuilder("test_pipeline")

        builder.asset(name="data_asset", fn=lambda ctx: [1, 2, 3])

        # 'data_asset' should be inferred, 'context' should be filtered
        builder.asset(
            name="processed",
            fn=lambda data_asset, context: data_asset,
        )

        graph = builder.build()
        assert graph.dependencies["processed"] == ("data_asset",)

    def test_auto_infer_with_partial_match(self) -> None:
        """Test inference when only some parameters match assets."""
        builder = PipelineBuilder("test_pipeline")

        builder.asset(name="upstream", fn=lambda ctx: [1, 2, 3])

        # Only 'upstream' matches an asset, 'other_param' doesn't
        builder.asset(
            name="dependent",
            fn=lambda upstream, other_param, ctx: upstream,
        )

        graph = builder.build()
        assert graph.dependencies["dependent"] == ("upstream",)

    def test_auto_infer_executes_correctly(self) -> None:
        """Test that auto-inferred pipelines execute correctly."""
        from vibe_piper import ExecutionEngine

        builder = PipelineBuilder("test_pipeline")

        builder.asset(name="numbers", fn=lambda ctx: [1, 2, 3, 4, 5])
        builder.asset(
            name="doubled",
            fn=lambda numbers, ctx: [x * 2 for x in numbers],
        )

        graph = builder.build()
        engine = ExecutionEngine()
        result = engine.execute(graph)

        assert result.success is True
        assert result.assets_executed == 2


class TestPipelineDefContextAutoInference:
    """Tests for automatic dependency inference in PipelineContext."""

    def test_auto_infer_in_context_manager(self) -> None:
        """Test automatic inference within PipelineDefContext."""
        with PipelineDefContext("test_pipeline") as pipeline:

            @pipeline.asset()
            def raw_data():
                return [1, 2, 3]

            @pipeline.asset()  # No depends_on specified!
            def processed(raw_data):  # Inferred from parameter name
                return [x * 2 for x in raw_data]

        graph = pipeline.build()
        assert "processed" in graph.dependencies
        assert graph.dependencies["processed"] == ("raw_data",)

    def test_auto_infer_multiple_in_context(self) -> None:
        """Test inferring multiple dependencies in context manager."""
        with PipelineDefContext("test_pipeline") as pipeline:

            @pipeline.asset()
            def source1():
                return [1, 2]

            @pipeline.asset()
            def source2():
                return [3, 4]

            @pipeline.asset()
            def combined(source1, source2):  # Inferred from params
                return source1 + source2

        graph = pipeline.build()
        assert set(graph.dependencies["combined"]) == {"source1", "source2"}

    def test_explicit_depends_on_in_context(self) -> None:
        """Test explicit depends_on in context manager."""
        with PipelineDefContext("test_pipeline") as pipeline:

            @pipeline.asset()
            def asset1():
                return [1]

            @pipeline.asset()
            def asset2():
                return [2]

            # Explicit depends_on overrides inference
            @pipeline.asset(depends_on=["asset2"])
            def dependent(asset1):
                return asset1

        graph = pipeline.build()
        assert graph.dependencies["dependent"] == ("asset2",)

    def test_auto_infer_executes_in_context(self) -> None:
        """Test that auto-inferred context pipelines execute correctly."""
        from vibe_piper import ExecutionEngine
        from vibe_piper import PipelineContext as PContext

        with PipelineDefContext("test_pipeline") as pipeline:

            @pipeline.asset()
            def data(context: PContext):
                return [10, 20, 30]

            @pipeline.asset()
            def transformed(data, context: PContext):
                return [x + 5 for x in data]

        graph = pipeline.build()
        engine = ExecutionEngine()
        result = engine.execute(graph)

        assert result.success is True
        assert result.assets_executed == 2

    def test_mixed_explicit_and_inferred_in_context(self) -> None:
        """Test mixing explicit and inferred dependencies in context."""
        with PipelineDefContext("test_pipeline") as pipeline:

            @pipeline.asset()
            def source1():
                return [1]

            @pipeline.asset()
            def source2():
                return [2]

            @pipeline.asset()  # Inferred
            def step1(source1):
                return source1

            @pipeline.asset(depends_on=["step1"])  # Explicit
            def step2(source2):
                return source2

        graph = pipeline.build()
        assert graph.dependencies["step1"] == ("source1",)
        assert graph.dependencies["step2"] == ("step1",)


class TestEdgeCases:
    """Tests for edge cases in dependency inference."""

    def test_infer_with_forward_reference(self) -> None:
        """Test inference when asset is defined later.

        Note: The current implementation infers dependencies at build time,
        not at definition time, so forward references ARE supported.
        This is actually a feature, not a bug!
        """
        builder = PipelineBuilder("test_pipeline")

        # Define dependent asset first
        builder.asset(
            name="dependent",
            fn=lambda upstream, ctx: upstream,
        )

        # Define upstream asset later
        builder.asset(name="upstream", fn=lambda ctx: [1, 2, 3])

        graph = builder.build()
        # 'dependent' WILL have inferred dependencies because build()
        # re-infers from all known assets at build time
        assert "dependent" in graph.dependencies
        assert graph.dependencies["dependent"] == ("upstream",)

    def test_infer_with_duplicate_param_names(self) -> None:
        """Test that inference handles duplicate parameter names gracefully."""
        # This is actually a Python syntax error, so we can't test it
        # Python doesn't allow duplicate parameter names in function signatures
        pass

    def test_infer_from_builtin_function(self) -> None:
        """Test inference from built-in functions.

        In Python 3.14+, many built-in functions can be inspected.
        This test verifies the behavior doesn't crash on built-ins.
        """
        # Test with a built-in that has a 'data' parameter (if any)
        # Most built-ins can be inspected in modern Python
        # We just verify it doesn't crash
        deps = infer_dependencies_from_signature(print, known_assets={"value"})
        # print has parameters like *objects, sep, end, etc.
        # None should match our known_assets, so we expect empty list
        assert isinstance(deps, list)

    def test_empty_signature(self) -> None:
        """Test inference from function with no parameters."""

        def no_params():
            return [1, 2, 3]

        deps = infer_dependencies_from_signature(no_params, known_assets={"something"})
        assert deps == []
