"""Pipeline generator for creating assets from configuration.

This module generates @asset decorated functions and pipeline builders
from declarative pipeline configuration.
"""

from collections.abc import Callable
from types import ModuleType
from typing import Any

from vibe_piper.pipeline_config.schema import (
    ExpectationCheck,
    PipelineConfig,
    SinkConfig,
    SourceConfig,
    TransformConfig,
    TransformStep,
    TransformType,
)


class PipelineGeneratorError(Exception):
    """Pipeline generator error."""

    pass


def generate_pipeline_from_config(config: PipelineConfig) -> ModuleType:
    """Generate a Python module with asset functions from config.

    This function creates a dynamic module containing functions that can
    be decorated with @asset and used to build a pipeline.

    Args:
        config: Pipeline configuration

    Returns:
        Dynamic module with asset functions

    Raises:
        PipelineGeneratorError: If generation fails

    Example:
        ```python
        from vibe_piper.pipeline_config import load_pipeline_config, generate_pipeline_from_config
        from vibe_piper.decorators import asset
        from vibe_piper import PipelineBuilder

        # Load and generate
        config = load_pipeline_config("pipeline.toml")
        module = generate_pipeline_from_config(config)

        # Build pipeline
        builder = PipelineBuilder(config.pipeline.name)

        for asset_name in dir(module):
            if not asset_name.startswith('_') and callable(getattr(module, asset_name)):
                asset_fn = getattr(module, asset_name)
                decorated = asset(name=asset_name)(asset_fn)
                builder.asset(asset_name, decorated)

        graph = builder.build()
        ```
    """
    # Create a new module
    module = ModuleType(f"generated_pipeline_{config.pipeline.name.replace('-', '_')}")

    # Store configuration for reference
    module._vibe_piper_config = config  # type: ignore[attr-defined]

    # Generate source functions
    for source_name, source_config in config.sources.items():
        source_fn = _generate_source_function(source_name, source_config)
        setattr(module, source_name, source_fn)

    # Generate transform functions
    for transform_name, transform_config in config.transforms.items():
        transform_fn = _generate_transform_function(transform_name, transform_config)
        setattr(module, transform_name, transform_fn)

    # Generate sink functions
    for sink_name, sink_config in config.sinks.items():
        sink_fn = _generate_sink_function(sink_name, sink_config)
        setattr(module, sink_name, sink_fn)

    return module


def _generate_source_function(name: str, config: SourceConfig) -> Callable:
    """Generate a function for a source asset.

    Args:
        name: Source name
        config: Source configuration

    Returns:
        Function that fetches data from source
    """

    def source_fn(ctx: Any) -> Any:
        """Fetch data from source.

        This is a generated asset function. The actual implementation
        will be provided by source connectors.

        To use this function, decorate it with @asset:

        ```python
        @asset(name="{name}")
        def {name}(ctx):
            return source_fn(ctx)
        ```
        """
        # This is a placeholder - actual implementation will use source connectors
        # For now, we raise NotImplementedError
        raise NotImplementedError(
            f"Source '{name}' implementation not yet generated. "
            f"Use source connectors to implement data fetching."
        )

    source_fn.__name__ = name
    source_fn.__doc__ = config.description or f"Source asset: {name}"
    source_fn._config_type = "source"  # type: ignore[attr-defined]
    source_fn._config = config  # type: ignore[attr-defined]

    return source_fn


def _generate_transform_function(name: str, config: TransformConfig) -> Callable:
    """Generate a function for a transform asset.

    Args:
        name: Transform name
        config: Transform configuration

    Returns:
        Function that applies transformations
    """

    def transform_fn(ctx: Any, **kwargs: Any) -> Any:
        """Apply transformations to data.

        This is a generated asset function. The actual implementation
        will execute the transformation steps.

        To use this function, decorate it with @asset:

        ```python
        @asset(name="{name}", depends_on=["{config.source}"])
        def {name}(ctx, {config.source}):
            return transform_fn(ctx, {config.source}={config.source})
        ```
        """
        # Get source data from kwargs (inferred from dependencies)
        source_data = kwargs.get(config.source)
        if source_data is None:
            # Try to get from context
            if hasattr(ctx, "assets"):
                source_data = ctx.assets.get(config.source)

        if source_data is None:
            raise ValueError(
                f"Transform '{name}' depends on source '{config.source}' but it was not provided"
            )

        # Apply transformation steps
        result = source_data
        for step in config.steps:
            result = _apply_transform_step(step, result)

        return result

    transform_fn.__name__ = name
    transform_fn.__doc__ = config.description or f"Transform asset: {name}"
    transform_fn._config_type = "transform"  # type: ignore[attr-defined]
    transform_fn._config = config  # type: ignore[attr-defined]
    transform_fn._depends_on = [config.source]  # type: ignore[attr-defined]

    return transform_fn


def _apply_transform_step(step: TransformStep, data: Any) -> Any:
    """Apply a single transformation step.

    Args:
        step: Transformation step configuration
        data: Input data

    Returns:
        Transformed data

    Raises:
        PipelineGeneratorError: If transformation fails
    """
    try:
        if step.type == TransformType.EXTRACT_FIELDS:
            return _extract_fields(step, data)
        elif step.type == TransformType.VALIDATE:
            return data  # Validation is handled separately
        elif step.type == TransformType.FILTER:
            return _filter_rows(step, data)
        elif step.type == TransformType.COMPUTE_FIELD:
            return _compute_field(step, data)
        else:
            raise NotImplementedError(f"Transform step type '{step.type}' not yet implemented")
    except Exception as e:
        msg = f"Failed to apply transform step '{step.type}': {e}"
        raise PipelineGeneratorError(msg) from e


def _extract_fields(step: TransformStep, data: Any) -> Any:
    """Extract nested fields from data.

    Args:
        step: Transform step with field mappings
        data: Input data (should be list of records)

    Returns:
        Data with extracted fields
    """
    if step.mappings is None:
        return data

    # If data is a list of dicts, apply mapping to each record
    if isinstance(data, list):
        result = []
        for record in data:
            if not isinstance(record, dict):
                result.append(record)
                continue

            mapped_record = {}
            for target_field, source_path in step.mappings.items():
                # Support dot notation for nested fields
                value = _get_nested_value(record, source_path)
                mapped_record[target_field] = value

            # Copy all other fields
            for key, value in record.items():
                if key not in step.mappings.values():
                    mapped_record[key] = value

            result.append(mapped_record)
        return result

    # If data is a single dict
    elif isinstance(data, dict):
        mapped_record = {}
        for target_field, source_path in step.mappings.items():
            value = _get_nested_value(data, source_path)
            mapped_record[target_field] = value

        # Copy all other fields
        for key, value in data.items():
            if key not in step.mappings.values():
                mapped_record[key] = value

        return mapped_record

    return data


def _get_nested_value(data: dict[str, Any], path: str) -> Any:
    """Get a value from nested dictionary using dot notation.

    Args:
        data: Nested dictionary
        path: Dot-notation path (e.g., "company.name")

    Returns:
        Value at path, or None if not found
    """
    keys = path.split(".")
    value = data

    for key in keys:
        if isinstance(value, dict) and key in value:
            value = value[key]
        else:
            return None

    return value


def _filter_rows(step: TransformStep, data: Any) -> Any:
    """Filter rows based on condition.

    Args:
        step: Transform step with filter condition
        data: Input data (should be list of records)

    Returns:
        Filtered data

    Raises:
        PipelineGeneratorError: If condition is invalid
    """
    if step.condition is None:
        return data

    if not isinstance(data, list):
        return data

    # Simple condition evaluation (this is a simplified implementation)
    # A full implementation would parse and evaluate conditions safely
    try:
        result = []
        for record in data:
            if _evaluate_condition(step.condition, record):
                result.append(record)
        return result
    except Exception as e:
        msg = f"Failed to evaluate filter condition '{step.condition}': {e}"
        raise PipelineGeneratorError(msg) from e


def _evaluate_condition(condition: str, record: dict[str, Any]) -> bool:
    """Evaluate a simple filter condition.

    Args:
        condition: Condition string (e.g., "email is not null")
        record: Record to evaluate

    Returns:
        True if condition passes

    Note:
        This is a simplified implementation. A full implementation would
        include a proper condition parser and evaluator.
    """
    # Simple "field is not null" check
    if " is not null" in condition:
        field = condition.replace(" is not null", "").strip()
        return field in record and record[field] is not None

    # Simple "field contains 'value'" check
    elif " contains '" in condition:
        parts = condition.split(' contains "')
        field = parts[0].strip()
        value = parts[1].replace('"', "").strip()
        if field in record:
            return value in str(record[field])
        return False

    # For now, return True for unknown conditions
    # TODO: Implement proper condition parser
    return True


def _compute_field(step: TransformStep, data: Any) -> Any:
    """Compute a new field for each record.

    Args:
        step: Transform step with field computation
        data: Input data (should be list of records)

    Returns:
        Data with computed field added
    """
    if step.field is None:
        return data

    if not isinstance(data, list):
        # Single record
        if isinstance(data, dict):
            data[step.field] = _compute_value(step.value or "", data)
        return data

    # List of records
    result = []
    for record in data:
        if isinstance(record, dict):
            record_copy = record.copy()
            record_copy[step.field] = _compute_value(step.value or "", record)
            result.append(record_copy)
        else:
            result.append(record)

    return result


def _compute_value(expression: str, record: dict[str, Any]) -> Any:
    """Compute a field value from expression.

    Args:
        expression: Value expression (e.g., "now()", "upper(name)")
        record: Record for field references

    Returns:
        Computed value

    Note:
        This is a simplified implementation. A full implementation would
        include a proper expression evaluator.
    """
    # Handle "now()" function
    if expression == "now()":
        from datetime import datetime, timezone

        return datetime.now(timezone.utc).isoformat()

    # Handle simple field references
    if expression in record:
        return record[expression]

    # For now, return the expression as-is
    # TODO: Implement proper expression evaluator
    return expression


def _generate_sink_function(name: str, config: SinkConfig) -> Callable:
    """Generate a function for a sink asset.

    Args:
        name: Sink name
        config: Sink configuration

    Returns:
        Function that writes data to sink
    """

    def sink_fn(ctx: Any, data: Any) -> Any:
        """Write data to sink.

        This is a generated asset function. The actual implementation
        will be provided by the sink connectors.

        To use this function, decorate it with @asset:

        ```python
        @asset(name="{name}")
        def {name}(ctx, data):
            return sink_fn(ctx, data)
        ```
        """
        # This is a placeholder - actual implementation will use sink connectors
        # For now, we raise NotImplementedError
        raise NotImplementedError(
            f"Sink '{name}' implementation not yet generated. "
            f"Use sink connectors to implement data writing."
        )

    sink_fn.__name__ = name
    sink_fn.__doc__ = config.description or f"Sink asset: {name}"
    sink_fn._config_type = "sink"  # type: ignore[attr-defined]
    sink_fn._config = config  # type: ignore[attr-defined]

    return sink_fn


def generate_expectations(config: PipelineConfig) -> dict[str, list[ExpectationCheck]]:
    """Generate expectation checks from configuration.

    Args:
        config: Pipeline configuration

    Returns:
        Dictionary mapping asset names to lists of expectation checks
    """
    expectations_by_asset: dict[str, list[ExpectationCheck]] = {}

    for exp_name, exp_config in config.expectations.items():
        if exp_config.asset not in expectations_by_asset:
            expectations_by_asset[exp_config.asset] = []

        expectations_by_asset[exp_config.asset].extend(exp_config.checks)

    return expectations_by_asset


def generate_asset_dependencies(config: PipelineConfig) -> dict[str, list[str]]:
    """Generate asset dependency mapping from configuration.

    Args:
        config: Pipeline configuration

    Returns:
        Dictionary mapping asset names to their dependencies
    """
    dependencies: dict[str, list[str]] = {}

    # Sources have no dependencies
    for source_name in config.sources:
        dependencies[source_name] = []

    # Transforms depend on sources or other transforms
    for transform_name, transform_config in config.transforms.items():
        dependencies[transform_name] = [transform_config.source]

    # Sinks - for now we assume they depend on transforms
    # TODO: Implement explicit sink dependencies in config schema
    for sink_name in config.sinks:
        dependencies[sink_name] = []

    return dependencies


def build_asset_graph(config: PipelineConfig) -> dict[str, Any]:
    """Build an asset graph representation from configuration.

    Args:
        config: Pipeline configuration

    Returns:
        Dictionary with nodes and edges representing the asset graph
    """
    nodes: list[str] = []
    edges: list[dict[str, str]] = []

    # Add all assets as nodes
    nodes.extend(config.sources.keys())
    nodes.extend(config.transforms.keys())
    nodes.extend(config.sinks.keys())

    # Add edges from transforms
    for transform_name, transform_config in config.transforms.items():
        edges.append(
            {
                "from": transform_config.source,
                "to": transform_name,
            }
        )

    # Add edges from sinks (if we had explicit dependencies)
    # This is a placeholder for future enhancement

    return {
        "nodes": nodes,
        "edges": edges,
    }
