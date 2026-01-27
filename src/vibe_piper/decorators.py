"""
Decorator utilities for Vibe Piper.

This module provides decorators for creating Assets and other Vibe Piper
objects in a declarative way.
"""

from collections.abc import Callable
from typing import Any, ParamSpec, TypeVar

from vibe_piper.types import Asset, AssetType, Schema

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

    # Create the Asset instance
    return Asset(
        name=asset_name,
        asset_type=asset_type,
        uri=asset_uri,
        schema=schema,
        description=asset_description,
        metadata=metadata or {},
        config=config or {},
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
            )

        # Case 2 & 3: @asset(...) - with or without parameters
        # Return a decorator function
        # Use name from kwargs if provided, otherwise use func_or_name
        name = name_param if name_param is not None else func_or_name

        def decorator(func: Callable[P, T]) -> Asset:
            return _create_asset_from_function(
                func=func,
                name=name,
                asset_type=asset_type,
                uri=uri,
                schema=schema,
                description=description,
                metadata=metadata,
                config=config,
            )

        return decorator


# Create an instance that can be used as a decorator
asset = AssetDecorator()
