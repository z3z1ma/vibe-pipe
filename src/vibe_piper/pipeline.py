"""
Declarative pipeline syntax for Vibe Piper.

This module provides a high-level, declarative API for building data pipelines
using the @asset decorator and pipeline builders.
"""

import inspect
from collections.abc import Callable
from typing import Any, ParamSpec, TypeVar

from vibe_piper.types import Asset, AssetGraph, AssetType, Operator, OperatorType

P = ParamSpec("P")
T = TypeVar("T")


# Parameters to exclude from dependency inference
# Note: We don't exclude "data" because users might have assets named "data"
_SPECIAL_PARAMS = {"context", "ctx"}


def infer_dependencies_from_signature(
    func: Callable[..., Any],
    known_assets: set[str] | None = None,
) -> list[str]:
    """
    Infer asset dependencies from a function's signature.

    This function inspects the function signature and extracts parameter names
    that match known asset names, automatically inferring dependencies.

    Special parameters like 'context' and 'ctx' are excluded from
    dependency inference. Note that 'data' is NOT excluded as users
    may legitimately have assets named 'data'.

    Args:
        func: The function to inspect for dependencies
        known_assets: Set of known asset names to filter against. If None,
            all non-special parameters are returned as potential dependencies.

    Returns:
        A list of inferred asset dependency names

    Example:
        Infer dependencies from a function::

            def process_data(raw_data, context):
                return [x * 2 for x in raw_data]

            deps = infer_dependencies_from_signature(
                process_data,
                known_assets={"raw_data", "other_asset"}
            )
            # Returns: ["raw_data"]
    """
    if known_assets is None:
        known_assets = set()

    try:
        sig = inspect.signature(func)
    except (ValueError, TypeError):
        # Some built-in or C functions can't be inspected
        return []

    # Get all parameter names except special ones
    params = [
        name
        for name, param in sig.parameters.items()
        if name not in _SPECIAL_PARAMS
        and param.kind not in (inspect.Parameter.VAR_POSITIONAL, inspect.Parameter.VAR_KEYWORD)
    ]

    # Filter to only known assets if provided
    if known_assets:
        return [p for p in params if p in known_assets]
    else:
        # Return all non-special parameters as potential dependencies
        return params


class PipelineBuilder:
    """
    A builder for creating declarative pipelines.

    PipelineBuilder provides a fluent interface for constructing pipelines
    with assets and their dependencies in a declarative way.

    Dependencies are automatically inferred from function parameter names
    that match existing asset names.

    Example:
        Build a pipeline with multiple assets::

            builder = PipelineBuilder("my_pipeline")

            builder.asset(
                name="raw_data",
                fn=lambda: [1, 2, 3],
            )

            builder.asset(
                name="processed_data",
                fn=lambda raw_data: [x * 2 for x in raw_data],
                # depends_on is automatically inferred from parameter 'raw_data'
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
        cache: bool = False,
        cache_ttl: int | None = None,
        parallel: bool = False,
        lazy: bool = False,
    ) -> "PipelineBuilder":
        """
        Add an asset to the pipeline.

        Dependencies are automatically inferred from the function signature
        if not explicitly provided. Parameter names that match existing
        asset names are treated as dependencies (except for special
        parameters like 'context', 'ctx', and 'data').

        Args:
            name: The name of the asset
            fn: A callable that produces or transforms data
            depends_on: Optional list of asset names this asset depends on.
                If not provided, dependencies are inferred from function
                parameter names.
            asset_type: The type of asset (defaults to MEMORY)
            uri: Optional URI for the asset
            description: Optional description of the asset
            metadata: Optional metadata for the asset
            config: Optional configuration for the asset

        Returns:
            Self for method chaining

        Raises:
            ValueError: If an asset with the same name already exists

        Example:
            Add an asset with automatic dependency inference::

                builder.asset("source", fn=lambda ctx: [1, 2, 3])
                builder.asset(
                    name="derived",
                    fn=lambda source, ctx: [x * 2 for x in source],
                    # depends_on is automatically inferred from parameter 'source'
                )
        """
        if name in self._assets:
            msg = f"Asset '{name}' already exists in pipeline"
            raise ValueError(msg)

        # Automatically infer dependencies if not explicitly provided
        resolved_dependencies: list[str] | None = None
        if depends_on is None:
            # Infer dependencies from function signature
            inferred = infer_dependencies_from_signature(fn, known_assets=set(self._assets.keys()))
            resolved_dependencies = inferred if inferred else None
        else:
            resolved_dependencies = list(depends_on)

        # Wrap the function to handle data and context parameters
        def wrapped_fn(data: Any, context: Any) -> Any:
            # If this is a source (no dependencies), call with just context
            if not resolved_dependencies:
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
        operator_type = OperatorType.SOURCE if not resolved_dependencies else OperatorType.TRANSFORM

        # Create operator from the wrapped function
        operator = Operator(
            name=name,
            operator_type=operator_type,
            fn=wrapped_fn,
            description=description,
        )

        # Generate URI if not provided
        asset_uri = uri or f"memory://{name}"

        # Create operator from wrapped function
        operator = Operator(
            name=name,
            operator_type=operator_type,
            fn=wrapped_fn,
            description=description,
        )

        # Create config with performance parameters
        asset_config = dict(config or {})
        if cache:
            asset_config["cache"] = True
            if cache_ttl is not None:
                asset_config["cache_ttl"] = cache_ttl
        if parallel:
            asset_config["parallel"] = True
        if lazy:
            asset_config["lazy"] = True

        # Create asset
        asset = Asset(
            name=name,
            asset_type=asset_type,
            uri=asset_uri,
            operator=operator,
            description=description,
            metadata=metadata or {},
            config=asset_config,
        )

        self._assets[name] = asset

        # Store dependencies (including inferred ones)
        if resolved_dependencies:
            self._dependencies[name] = resolved_dependencies

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
        # Validation: Check that all dependencies exist
        known_assets = set(self._assets.keys())
        for asset_name, deps in self._dependencies.items():
            for dep in deps:
                if dep not in known_assets:
                    msg = (
                        f"Asset '{asset_name}' depends on '{dep}' "
                        f"which is not defined in the pipeline"
                    )
                    raise ValueError(msg)

        # Validation: Check for circular dependencies using DFS
        visited: set[str] = set()
        rec_stack: set[str] = set()
        path: list[str] = []

        def dfs(node: str) -> None:
            """Depth-first search to detect cycles."""
            visited.add(node)
            rec_stack.add(node)
            path.append(node)

            # Check all dependencies of this node
            for neighbor in self._dependencies.get(node, []):
                if neighbor not in visited:
                    dfs(neighbor)
                elif neighbor in rec_stack:
                    # Found a cycle - construct the cycle path
                    cycle_start = path.index(neighbor)
                    cycle_path = path[cycle_start:] + [neighbor]
                    msg = f"Circular dependency detected: {' -> '.join(cycle_path)}"
                    raise ValueError(msg)

            path.pop()
            rec_stack.remove(node)

        # Run DFS on all assets to detect cycles
        for asset_name in known_assets:
            if asset_name not in visited:
                dfs(asset_name)

        # Convert to tuples for immutable AssetGraph
        assets_tuple = tuple(self._assets.values())
        dependencies_tuple = {name: tuple(deps) for name, deps in self._dependencies.items()}

        return AssetGraph(
            name=self.name,
            assets=assets_tuple,
            dependencies=dependencies_tuple,
            description=self.description,
        )


class PipelineDefinitionContext:
    """
    Context manager for defining pipelines with nested assets.

    This class provides a context for collecting assets defined within
    a 'with' block, enabling a more declarative syntax.

    Dependencies are automatically inferred from function parameter names
    that match existing asset names.

    Example:
        Define a pipeline using context manager syntax::

            with PipelineDefinitionContext("my_pipeline") as pipeline:
                @pipeline.asset()
                def raw_data():
                    return [1, 2, 3]

                @pipeline.asset()  # No need to specify depends_on!
                def processed_data(raw_data):  # Parameter name matches asset
                    return [x * 2 for x in raw_data]

            graph = pipeline.build()
    """

    def __init__(self, name: str, description: str | None = None) -> None:
        """
        Initialize a new PipelineDefinitionContext.

        Args:
            name: The name of the pipeline
            description: Optional description of pipeline
        """
        self._builder = PipelineBuilder(name, description)
        self._assets: dict[str, Asset] = {}

    def __enter__(self) -> "PipelineDefinitionContext":
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
        cache: bool = False,
        cache_ttl: int | None = None,
        parallel: bool = False,
        lazy: bool = False,
    ) -> Any:
        """
        Decorator or method to add an asset to the pipeline.

        Can be used as:
        - A decorator: @pipeline.asset()
        - A decorator with params: @pipeline.asset(depends_on=[other_asset])
        - A method call: pipeline.asset(fn=func, name="my_asset")

        Dependencies are automatically inferred from the function signature
        if not explicitly provided. Parameter names that match existing
        asset names are treated as dependencies (except for special
        parameters like 'context', 'ctx', and 'data').

        Args:
            fn: The function to convert to an asset (when used as a decorator)
            name: Optional name for the asset (defaults to function name)
            depends_on: Optional list of asset names this asset depends on.
                Can be strings or callable objects (whose __name__ will be used).
                If not provided, dependencies are inferred from function
                parameter names.
            asset_type: The type of asset (defaults to MEMORY)
            uri: Optional URI for the asset
            description: Optional description of the asset
            metadata: Optional metadata for the asset
            config: Optional configuration for the asset

        Returns:
            Either a decorator function or the decorated function
        """

        # Convert callable dependencies to names (if explicitly provided)
        explicit_dependencies: list[str] | None = None
        if depends_on is not None:
            explicit_dependencies = []
            for dep in depends_on:
                if isinstance(dep, str):
                    explicit_dependencies.append(dep)
                else:
                    # Check if it's callable (mypy reports this as unreachable but it's not)
                    if callable(dep):  # type: ignore[unreachable]
                        explicit_dependencies.append(dep.__name__)
                    else:
                        msg = f"Invalid dependency: {dep!r}. Must be string or callable"
                        raise TypeError(msg)

        def decorator(func: Callable[P, T]) -> Callable[P, T]:
            asset_name = name or func.__name__

            # Infer dependencies if not explicitly provided
            resolved_dependencies: list[str] | None = None
            if explicit_dependencies is not None:
                resolved_dependencies = explicit_dependencies
            else:
                # Automatically infer from function signature
                inferred = infer_dependencies_from_signature(
                    func, known_assets=set(self._builder._assets.keys())
                )
                resolved_dependencies = inferred if inferred else None

            # Wrap the function to handle data and context parameters
            def wrapped_fn(data: Any, context: Any) -> Any:
                # If this is a source (no dependencies), call with just context
                if not resolved_dependencies:
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
            operator_type = (
                OperatorType.SOURCE if not resolved_dependencies else OperatorType.TRANSFORM
            )

            # Generate URI if not provided
            asset_uri = uri or f"memory://{asset_name}"

            # Create the asset directly (don't use builder.asset() to avoid double-wrapping)
            operator = Operator(
                name=asset_name,
                operator_type=operator_type,
                fn=wrapped_fn,
                description=description or func.__doc__,
            )

            # Convert config to mutable dict for modifications
            asset_config = dict(config or {})

            # Add performance parameters to config
            if cache:
                asset_config["cache"] = True
                if cache_ttl is not None:
                    asset_config["cache_ttl"] = cache_ttl
            if parallel:
                asset_config["parallel"] = True
            if lazy:
                asset_config["lazy"] = True

            asset = Asset(
                name=asset_name,
                asset_type=asset_type,
                uri=asset_uri,
                operator=operator,
                description=description,
                metadata=metadata or {},
                config=asset_config,
            )

            # Add to builder's internal state
            self._builder._assets[asset_name] = asset
            if resolved_dependencies:
                self._builder._dependencies[asset_name] = resolved_dependencies

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
        Build a pipeline using fluent interface::

            pipeline = (
                build_pipeline("my_pipeline")
                .asset("source", lambda: [1, 2, 3])
                .asset("derived", lambda data: [x * 2 for x in data],
                       depends_on=["source"])
            )

            graph = pipeline.build()
    """
    return PipelineBuilder(name, description)
