"""
Asset factory for creating Asset objects consistently.

This module provides a centralized factory function for creating Asset objects,
ensuring consistent behavior across decorators, builders, and other asset
creation paths.
"""

from collections.abc import Callable
from typing import Any, ParamSpec, TypeVar

from vibe_piper.types import (
    Asset,
    AssetType,
    MaterializationStrategy,
    Operator,
    OperatorType,
    Schema,
)

P = ParamSpec("P")
T = TypeVar("T")


def create_asset(
    *,
    name: str,
    fn: Callable[P, T] | None = None,
    asset_type: AssetType = AssetType.MEMORY,
    uri: str | None = None,
    schema: Schema | None = None,
    description: str | None = None,
    metadata: dict[str, Any] | None = None,
    config: dict[str, Any] | None = None,
    io_manager: str | None = None,
    materialization: str | MaterializationStrategy | None = None,
    retries: int | None = None,
    backoff: str | None = None,
    cache: bool = False,
    cache_ttl: int | None = None,
    parallel: bool = False,
    lazy: bool = False,
    create_operator: bool = False,
    operator_type: OperatorType | None = None,
) -> Asset:
    """
    Create an Asset with consistent configuration.

    This is the canonical factory for creating Asset objects. It handles
    all configuration parameters consistently and can optionally create an
    Operator for the asset.

    Args:
        name: The name of the asset
        fn: Optional callable function for the asset (used when creating operator)
        asset_type: The type of asset (defaults to MEMORY)
        uri: Optional URI for the asset (auto-generated if not provided)
        schema: Optional schema for the asset
        description: Optional description (uses fn.__doc__ if not provided)
        metadata: Optional metadata dictionary
        config: Optional config dictionary (merged with retry/cache/performance settings)
        io_manager: IO manager name (defaults to "memory")
        materialization: Materialization strategy (defaults to TABLE)
        retries: Number of retry attempts
        backoff: Backoff strategy (e.g., "exponential", "linear")
        cache: Whether to enable caching
        cache_ttl: Cache time-to-live in seconds
        parallel: Whether to enable parallel execution
        lazy: Whether to enable lazy evaluation
        create_operator: Whether to create an Operator from fn
        operator_type: Type of operator to create (SOURCE or TRANSFORM)

    Returns:
        A configured Asset instance

    Raises:
        ValueError: If materialization string is invalid

    Examples:
        Create a basic asset without operator::

            asset = create_asset(name="my_asset")

        Create an asset with operator for use in pipelines::

            asset = create_asset(
                name="my_asset",
                fn=lambda ctx: [1, 2, 3],
                create_operator=True,
                operator_type=OperatorType.SOURCE,
            )

        Create an asset with performance settings::

            asset = create_asset(
                name="my_asset",
                cache=True,
                cache_ttl=3600,
                parallel=True,
            )
    """
    # Generate URI if not provided
    asset_uri = uri
    if asset_uri is None:
        # Generate URI based on asset type
        type_prefix = asset_type.name.lower()
        asset_uri = f"{type_prefix}://{name}"

    # Use function docstring as description if not provided
    asset_description = description
    if asset_description is None and fn is not None and fn.__doc__:
        asset_description = fn.__doc__.strip()

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

    # Build config with retry, cache, and performance settings
    asset_config = dict(config or {})
    if retries is not None:
        asset_config["retries"] = retries
    if backoff is not None:
        asset_config["backoff"] = backoff

    # Note: cache, cache_ttl, parallel, lazy are stored as top-level fields on Asset,
    # not in the config dict. This is a design decision for direct access.

    # Create operator if requested
    asset_operator: Operator | None = None
    if create_operator and fn is not None:
        # Determine operator type if not specified
        op_type = operator_type or OperatorType.SOURCE

        # Wrap function to handle data and context parameters
        def wrapped_fn(data: Any, context: Any) -> Any:
            # If this is a source (operator_type == SOURCE), call with just context
            if op_type == OperatorType.SOURCE:
                try:
                    return fn(context)  # type: ignore
                except TypeError:
                    # If function expects 2 args, call with both
                    return fn(data, context)  # type: ignore
            else:
                # Transform functions receive upstream data
                return fn(data, context)  # type: ignore

        asset_operator = Operator(
            name=name,
            operator_type=op_type,
            fn=wrapped_fn,
            description=asset_description,
        )

    # Create the Asset instance
    return Asset(
        name=name,
        asset_type=asset_type,
        uri=asset_uri,
        schema=schema,
        operator=asset_operator,
        description=asset_description,
        metadata=metadata or {},
        config=asset_config,
        io_manager=io_manager or "memory",
        materialization=asset_materialization,
        cache=cache,
        cache_ttl=cache_ttl,
        parallel=parallel,
        lazy=lazy,
    )
