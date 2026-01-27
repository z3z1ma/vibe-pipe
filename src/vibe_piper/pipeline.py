"""
Declarative pipeline syntax for Vibe Piper.

This module provides a high-level, declarative API for building data pipelines
using the @asset decorator and pipeline builders.
"""

from collections.abc import Callable
from contextlib import contextmanager
from typing import Any, ParamSpec, TypeVar

from vibe_piper.types import Asset, AssetGraph, AssetType, Operator, OperatorType

P = ParamSpec("P")
T = TypeVar("T")


class PipelineBuilder:
    """
    A builder for creating declarative pipelines.

    PipelineBuilder provides a fluent interface for constructing pipelines
    with assets and their dependencies in a declarative way.

    Example:
        Build a pipeline with multiple assets::

            builder = PipelineBuilder("my_pipeline")

            builder.asset(
                name="raw_data",
                fn=lambda: [1, 2, 3],
            )

            builder.asset(
                name="processed_data",
                fn=lambda data: [x * 2 for x in data],
                depends_on=["raw_data"],
            )

            graph = builder.build()
    """

    def __init__(self, name: str, description: str | None = None) -> None:
        """
        Initialize a new PipelineBuilder.

        Args:
            name: The name of the pipeline
            description: Optional description of the pipeline
        """
        self.name = name
        self.description = description
        self._assets: dict[str, Asset] = {}
        self._dependencies: dict[str, list[str]] = {}

    def asset(
        self,
        name: str,
        fn: Callable[P, T],
        *,
        depends_on: list[str] | tuple[str, ...] | None = None,
        asset_type: AssetType = AssetType.MEMORY,
        uri: str | None = None,
        description: str | None = None,
        metadata: dict[str, Any] | None = None,
        config: dict[str, Any] | None = None,
    ) -> "PipelineBuilder":
        """
        Add an asset to the pipeline.

        Args:
            name: The name of the asset
            fn: A callable that produces or transforms data
            depends_on: List of asset names this asset depends on
            asset_type: The type of asset (defaults to MEMORY)
            uri: Optional URI for the asset
            description: Optional description of the asset
            metadata: Optional metadata for the asset
            config: Optional configuration for the asset

        Returns:
            Self for method chaining

        Raises:
            ValueError: If an asset with the same name already exists
        """
        if name in self._assets:
            msg = f"Asset '{name}' already exists in pipeline"
            raise ValueError(msg)

        # Wrap the function to handle data and context parameters
        def wrapped_fn(data: Any, context: Any) -> Any:
            # If this is a source (no dependencies), call with just context
            if not depends_on:
                # Source functions should be fn(context) -> T
                try:
                    return fn(context)  # type: ignore
                except TypeError:
                    # If function expects 2 args, call with both
                    return fn(data, context)  # type: ignore
            else:
                # Transform functions receive upstream data
                return fn(data, context)  # type: ignore

        # Determine operator type based on dependencies
        operator_type = OperatorType.SOURCE if not depends_on else OperatorType.TRANSFORM

        # Create operator from the wrapped function
        operator = Operator(
            name=name,
            operator_type=operator_type,
            fn=wrapped_fn,
            description=description,
        )

        # Generate URI if not provided
        asset_uri = uri or f"memory://{name}"

        # Create the asset
        asset = Asset(
            name=name,
            asset_type=asset_type,
            uri=asset_uri,
            operator=operator,
            description=description,
            metadata=metadata or {},
            config=config or {},
        )

        self._assets[name] = asset

        # Store dependencies
        if depends_on:
            self._dependencies[name] = list(depends_on)

        return self

    def build(self) -> AssetGraph:
        """
        Build the AssetGraph from the defined assets and dependencies.

        Returns:
            An AssetGraph representing the pipeline

        Raises:
            ValueError: If dependency references don't exist or if there
                are circular dependencies
        """
        # Convert to tuples for immutable AssetGraph
        assets_tuple = tuple(self._assets.values())
        dependencies_tuple = {
            name: tuple(deps) for name, deps in self._dependencies.items()
        }

        return AssetGraph(
            name=self.name,
            assets=assets_tuple,
            dependencies=dependencies_tuple,
            description=self.description,
        )


class PipelineContext:
    """
    Context manager for defining pipelines with nested assets.

    This class provides a context for collecting assets defined within
    a 'with' block, enabling a more declarative syntax.

    Example:
        Define a pipeline using context manager syntax::

            with PipelineContext("my_pipeline") as pipeline:
                @pipeline.asset()
                def raw_data():
                    return [1, 2, 3]

                @pipeline.asset(depends_on=[raw_data])
                def processed_data(data):
                    return [x * 2 for x in data]

            graph = pipeline.build()
    """

    def __init__(self, name: str, description: str | None = None) -> None:
        """
        Initialize a new PipelineContext.

        Args:
            name: The name of the pipeline
            description: Optional description of the pipeline
        """
        self._builder = PipelineBuilder(name, description)
        self._assets: dict[str, Asset] = {}

    def __enter__(self) -> "PipelineContext":
        """Enter the context manager."""
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        """Exit the context manager."""
        pass

    def asset(
        self,
        fn: Callable[P, T] | None = None,
        *,
        name: str | None = None,
        depends_on: list[str] | tuple[str, ...] | None = None,
        asset_type: AssetType = AssetType.MEMORY,
        uri: str | None = None,
        description: str | None = None,
        metadata: dict[str, Any] | None = None,
        config: dict[str, Any] | None = None,
    ) -> Any:
        """
        Decorator or method to add an asset to the pipeline.

        Can be used as:
        - A decorator: @pipeline.asset()
        - A decorator with params: @pipeline.asset(depends_on=[other_asset])
        - A method call: pipeline.asset(fn=func, name="my_asset")

        Args:
            fn: The function to convert to an asset (when used as a decorator)
            name: Optional name for the asset (defaults to function name)
            depends_on: List of asset names this asset depends on. Can be strings
                or callable objects (whose __name__ will be used)
            asset_type: The type of asset (defaults to MEMORY)
            uri: Optional URI for the asset
            description: Optional description of the asset
            metadata: Optional metadata for the asset
            config: Optional configuration for the asset

        Returns:
            Either a decorator function or the decorated function
        """

        # Convert callable dependencies to names
        dependency_names: list[str] | None = None
        if depends_on is not None:
            dependency_names = []
            for dep in depends_on:
                if isinstance(dep, str):
                    dependency_names.append(dep)
                else:
                    # Check if it's callable (mypy reports this as unreachable but it's not)
                    if callable(dep):  # type: ignore[unreachable]
                        dependency_names.append(dep.__name__)
                    else:
                        msg = f"Invalid dependency: {dep!r}. Must be string or callable"
                        raise TypeError(msg)

        def decorator(func: Callable[P, T]) -> Callable[P, T]:
            asset_name = name or func.__name__

            # Wrap the function to handle data and context parameters
            def wrapped_fn(data: Any, context: Any) -> Any:
                # If this is a source (no dependencies), call with just context
                if not dependency_names:
                    # Source functions should be fn(context) -> T
                    try:
                        return func(context)  # type: ignore
                    except TypeError:
                        # If function expects 2 args, call with both
                        return func(data, context)  # type: ignore
                else:
                    # Transform functions receive upstream data
                    return func(data, context)  # type: ignore

            # Determine operator type based on dependencies
            operator_type = OperatorType.SOURCE if not dependency_names else OperatorType.TRANSFORM

            # Generate URI if not provided
            asset_uri = uri or f"memory://{asset_name}"

            # Create the asset directly (don't use builder.asset() to avoid double-wrapping)
            operator = Operator(
                name=asset_name,
                operator_type=operator_type,
                fn=wrapped_fn,
                description=description or func.__doc__,
            )

            asset = Asset(
                name=asset_name,
                asset_type=asset_type,
                uri=asset_uri,
                operator=operator,
                description=description,
                metadata=metadata or {},
                config=config or {},
            )

            # Add to builder's internal state
            self._builder._assets[asset_name] = asset
            if dependency_names:
                self._builder._dependencies[asset_name] = dependency_names

            # Store for potential dependency reference
            self._assets[asset_name] = asset

            return func

        if fn is not None:
            # Called as @pipeline.asset (without parentheses)
            return decorator(fn)
        else:
            # Called as @pipeline.asset() or @pipeline.asset(depends_on=[...])
            return decorator

    def build(self) -> AssetGraph:
        """
        Build the AssetGraph from the defined assets and dependencies.

        Returns:
            An AssetGraph representing the pipeline
        """
        return self._builder.build()


def build_pipeline(name: str, description: str | None = None) -> PipelineBuilder:
    """
    Create a new PipelineBuilder for building a pipeline.

    This is a convenience function that creates a PipelineBuilder
    with a fluent interface for method chaining.

    Args:
        name: The name of the pipeline
        description: Optional description of the pipeline

    Returns:
        A new PipelineBuilder instance

    Example:
        Build a pipeline using the fluent interface::

            pipeline = (
                build_pipeline("my_pipeline")
                .asset("source", lambda: [1, 2, 3])
                .asset("derived", lambda data: [x * 2 for x in data],
                       depends_on=["source"])
            )

            graph = pipeline.build()
    """
    return PipelineBuilder(name, description)
