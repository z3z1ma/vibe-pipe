"""
Decorator utilities for Vibe Piper.

This module provides decorators for creating Assets, Expectations, and other
Vibe Piper objects in a declarative way.
"""

from collections.abc import Callable
from typing import Any, ParamSpec, TypeVar

from vibe_piper.types import (
    Asset,
    AssetType,
    Expectation,
    MaterializationStrategy,
    Schema,
    ValidationResult,
)

P = ParamSpec("P")
T = TypeVar("T")


def _create_asset_from_function(
    func: Callable[P, T],
    name: str | None,
    asset_type: AssetType,
    uri: str | None,
    schema: Schema | None,
    description: str | None,
    metadata: dict[str, Any] | None,
    config: dict[str, Any] | None,
    io_manager: str | None,
    materialization: str | MaterializationStrategy | None,
    retries: int | None = None,
    backoff: str | None = None,
    cache: bool = False,
    cache_ttl: int | None = None,
    parallel: bool = False,
    lazy: bool = False,
) -> Asset:
    """Helper function to create an Asset from a function."""
    # Determine asset name
    asset_name = name or func.__name__

    # Generate URI if not provided
    asset_uri = uri
    if asset_uri is None:
        # Generate URI based on asset type
        type_prefix = asset_type.name.lower()
        asset_uri = f"{type_prefix}://{asset_name}"

    # Use docstring as description if not provided
    asset_description = description
    if asset_description is None and func.__doc__:
        asset_description = func.__doc__.strip()

    # Normalize materialization parameter
    asset_materialization: MaterializationStrategy | str
    if materialization is None:
        asset_materialization = MaterializationStrategy.TABLE
    elif isinstance(materialization, str):
        # Convert string to MaterializationStrategy enum
        try:
            # Case-insensitive lookup
            asset_materialization = MaterializationStrategy[materialization.upper()]
        except KeyError:
            valid_options = [s.name.lower() for s in MaterializationStrategy]
            msg = (
                f"Invalid materialization strategy "
                f"'{materialization}'. Must be one of: {valid_options}"
            )
            raise ValueError(msg) from None
    else:
        asset_materialization = materialization

    # Build config with retry, cache, and parallel settings
    asset_config = config or {}
    if retries is not None:
        asset_config = dict(asset_config)  # Make a copy
        asset_config["retries"] = retries
    if backoff is not None:
        asset_config = dict(asset_config)  # Make a copy
        asset_config["backoff"] = backoff
    if cache:
        asset_config = dict(asset_config)  # Make a copy
        asset_config["cache"] = True
        if cache_ttl is not None:
            asset_config["cache_ttl"] = cache_ttl
    if parallel:
        asset_config = dict(asset_config)  # Make a copy
        asset_config["parallel"] = True
    if lazy:
        asset_config = dict(asset_config)  # Make a copy
        asset_config["lazy"] = True

    # Create the Asset instance
    return Asset(
        name=asset_name,
        asset_type=asset_type,
        uri=asset_uri,
        schema=schema,
        description=asset_description,
        metadata=metadata or {},
        config=asset_config,
        io_manager=io_manager or "memory",
        materialization=asset_materialization,
    )


class AssetDecorator:
    """Decorator class that supports both @asset and @asset(...) patterns."""

    def __call__(
        self,
        func_or_name: Callable[P, T] | str | None = None,
        **kwargs: Any,
    ) -> Asset | Callable[[Callable[P, T]], Asset]:
        """
        Decorator to convert a function into an Asset.

        Can be used as:
        - @asset (without parentheses)
        - @asset() (with empty parentheses)
        - @asset(name="foo", ...) (with parameters)

        Args:
            func_or_name: Either the function to decorate (when using @asset)
                          or a custom asset name (when using @asset(...))
            **kwargs: Additional keyword arguments (asset_type, uri, schema, etc.)

        Returns:
            Either an Asset (when used as @asset) or a decorator function
            (when used as @asset(...))
        """
        # Extract parameters from kwargs with defaults
        asset_type = kwargs.pop("asset_type", AssetType.MEMORY)
        uri = kwargs.pop("uri", None)
        schema = kwargs.pop("schema", None)
        description = kwargs.pop("description", None)
        metadata = kwargs.pop("metadata", None)
        config = kwargs.pop("config", None)
        name_param = kwargs.pop("name", None)
        io_manager = kwargs.pop("io_manager", None)
        materialization = kwargs.pop("materialization", None)

        # Extract retry parameters
        retries = kwargs.pop("retries", None)
        backoff = kwargs.pop("backoff", None)

        # Extract performance parameters
        cache = kwargs.pop("cache", False)
        cache_ttl = kwargs.pop("cache_ttl", None)
        parallel = kwargs.pop("parallel", False)
        lazy = kwargs.pop("lazy", False)

        # Case 1: @asset (no parentheses) - func_or_name is the function
        if callable(func_or_name):
            return _create_asset_from_function(
                func=func_or_name,
                name=name_param,
                asset_type=asset_type,
                uri=uri,
                schema=schema,
                description=description,
                metadata=metadata,
                config=config,
                io_manager=io_manager,
                materialization=materialization,
                retries=retries,
                backoff=backoff,
                cache=cache,
                cache_ttl=cache_ttl,
                parallel=parallel,
                lazy=lazy,
            )

        # Case 2 & 3: @asset(...) - with or without parameters
        # Return a decorator function
        # Use name from kwargs if provided, otherwise use func_or_name
        name = name_param if name_param is not None else func_or_name

        def decorator(func: Callable[P, T]) -> Asset:
            asset_name = name or func_or_name

            return _create_asset_from_function(
                func=func,
                name=asset_name,
                asset_type=asset_type,
                uri=uri,
                schema=schema,
                description=description,
                metadata=metadata,
                config=config,
                io_manager=io_manager,
                materialization=materialization,
                retries=retries,
                backoff=backoff,
                cache=cache,
                cache_ttl=cache_ttl,
                parallel=parallel,
                lazy=lazy,
            )

        return decorator


# Create an instance that can be used as a decorator
asset = AssetDecorator()


# =============================================================================
# Expectation Decorator
# =============================================================================


def _create_expectation_from_function(
    func: Callable[[Any], ValidationResult | bool],
    name: str | None,
    severity: str,
    description: str | None,
    metadata: dict[str, Any] | None,
    config: dict[str, Any] | None,
) -> Expectation:
    """Helper function to create an Expectation from a function."""
    # Determine expectation name
    expectation_name = name or func.__name__

    # Use docstring as description if not provided
    expectation_description = description
    if expectation_description is None and func.__doc__:
        expectation_description = func.__doc__.strip()

    # Wrap the function to ensure it returns ValidationResult
    def validation_fn(data: Any) -> ValidationResult:
        result = func(data)
        if isinstance(result, ValidationResult):
            return result
        # If function returns bool, convert to ValidationResult
        if result:
            return ValidationResult(is_valid=True)
        return ValidationResult(
            is_valid=False,
            errors=(f"Expectation '{expectation_name}' failed",),
        )

    # Create the Expectation instance
    return Expectation(
        name=expectation_name,
        fn=validation_fn,
        description=expectation_description,
        severity=severity,
        metadata=metadata or {},
        config=config or {},
    )


class ExpectationDecorator:
    """Decorator class that supports both @expect and @expect(...) patterns."""

    def __call__(
        self,
        func_or_name: Callable[[Any], ValidationResult | bool] | str | None = None,
        **kwargs: Any,
    ) -> Expectation | Callable[[Callable[[Any], ValidationResult | bool]], Expectation]:
        """
        Decorator to convert a function into an Expectation.

        Can be used as:
        - @expect (without parentheses)
        - @expect() (with empty parentheses)
        - @expect(name="foo", ...) (with parameters)

        The decorated function should either:
        - Return a ValidationResult
        - Return a bool (True = valid, False = invalid)

        Args:
            func_or_name: Either the function to decorate (when using @expect)
                          or a custom expectation name (when using @expect(...))
            **kwargs: Additional keyword arguments (severity, description, etc.)

        Returns:
            Either an Expectation (when used as @expect) or a decorator function
            (when used as @expect(...))
        """
        # Extract parameters from kwargs with defaults
        severity = kwargs.pop("severity", "error")
        description = kwargs.pop("description", None)
        metadata = kwargs.pop("metadata", None)
        config = kwargs.pop("config", None)
        name_param = kwargs.pop("name", None)

        # Case 1: @expect (no parentheses) - func_or_name is the function
        if callable(func_or_name):
            return _create_expectation_from_function(
                func=func_or_name,
                name=name_param,
                severity=severity,
                description=description,
                metadata=metadata,
                config=config,
            )

        # Case 2 & 3: @expect(...) - with or without parameters
        # Return a decorator function
        # Use name from kwargs if provided, otherwise use func_or_name
        name = name_param if name_param is not None else func_or_name

        def decorator(func: Callable[[Any], ValidationResult | bool]) -> Expectation:
            return _create_expectation_from_function(
                func=func,
                name=name,
                severity=severity,
                description=description,
                metadata=metadata,
                config=config,
            )

        return decorator


# Create an instance that can be used as a decorator
expect = ExpectationDecorator()
