"""Configuration loader utility for pipeline definitions.

This module provides convenience functions for loading and finding pipeline
configuration files.
"""

from pathlib import Path
from typing import Any

from vibe_piper.pipeline_config.parser import (
    PipelineConfig,
    PipelineConfigError,
    load_pipeline_config,
)


def find_pipeline_file(search_path: Path | None = None) -> Path | None:
    """Find pipeline configuration file by searching upward from path.

    Searches for configuration files in order of preference:
    1. pipeline.toml
    2. pipeline.yaml / pipeline.yml
    3. pipeline.json

    Args:
        search_path: Starting path (defaults to current directory)

    Returns:
        Path to configuration file, or None if not found
    """
    search_path = search_path or Path.cwd()

    # Search upward for config file in order of preference
    config_names = ["pipeline.toml", "pipeline.yaml", "pipeline.yml", "pipeline.json"]

    for path in [search_path, *search_path.parents]:
        for config_name in config_names:
            config_file = path / config_name
            if config_file.exists():
                return config_file

    return None


def load_pipeline_from_path(
    path: str | Path | None = None,
    env_overrides: dict[str, Any] | None = None,
) -> PipelineConfig:
    """Load pipeline configuration from path or search for it.

    If path is not provided, searches upward from current directory.

    Args:
        path: Optional path to configuration file
        env_overrides: Optional environment variable overrides

    Returns:
        Loaded and validated pipeline configuration

    Raises:
        PipelineConfigError: If configuration cannot be loaded or is invalid
    """
    if path is None:
        config_path = find_pipeline_file()
        if config_path is None:
            msg = (
                "No pipeline configuration file found. "
                "Please create a pipeline.toml, pipeline.yaml, or pipeline.json file."
            )
            raise PipelineConfigError(msg)
        return load_pipeline_config(config_path, env_overrides)
    else:
        return load_pipeline_config(path, env_overrides)


def validate_pipeline_config(
    config: PipelineConfig,
    check_references: bool = True,
) -> list[str]:
    """Validate pipeline configuration.

    Checks for:
    - Missing required fields
    - Invalid references (sources, sinks, transforms, expectations)
    - Circular dependencies in transforms

    Args:
        config: Pipeline configuration to validate
        check_references: Whether to check asset references

    Returns:
        List of error messages (empty if valid)
    """
    errors: list[str] = []

    # Validate pipeline metadata
    if not config.pipeline.name:
        errors.append("Pipeline name is required")

    # Check transform references
    if check_references:
        for transform_name, transform in config.transforms.items():
            # Check source reference
            if transform.source not in config.sources and transform.source not in config.transforms:
                errors.append(
                    f"Transform '{transform_name}' references unknown source or transform: {transform.source}"
                )

            # Check for circular dependencies
            if _has_circular_dependency(config, transform_name):
                errors.append(f"Transform '{transform_name}' has a circular dependency")

        # Check expectation references
        for exp_name, expectation in config.expectations.items():
            if (
                expectation.asset not in config.sources
                and expectation.asset not in config.transforms
                and expectation.asset not in config.sinks
            ):
                errors.append(
                    f"Expectation '{exp_name}' references unknown asset: {expectation.asset}"
                )

        # Check job references
        for job_name, job in config.jobs.items():
            for source_name in job.sources:
                if source_name not in config.sources:
                    errors.append(f"Job '{job_name}' references unknown source: {source_name}")

            for sink_name in job.sinks:
                if sink_name not in config.sinks:
                    errors.append(f"Job '{job_name}' references unknown sink: {sink_name}")

            for transform_name in job.transforms:
                if transform_name not in config.transforms:
                    errors.append(
                        f"Job '{job_name}' references unknown transform: {transform_name}"
                    )

            for exp_name in job.expectations:
                if exp_name not in config.expectations:
                    errors.append(f"Job '{job_name}' references unknown expectation: {exp_name}")

    return errors


def _has_circular_dependency(config: PipelineConfig, transform_name: str) -> bool:
    """Check if a transform has a circular dependency.

    Args:
        config: Pipeline configuration
        transform_name: Transform name to check

    Returns:
        True if circular dependency exists
    """
    visited: set[str] = set()

    def check(current: str) -> bool:
        if current in visited:
            return True
        if current not in config.transforms:
            return False
        visited.add(current)
        transform = config.transforms[current]
        return check(transform.source)

    return check(transform_name)


def get_dependency_order(config: PipelineConfig) -> list[str]:
    """Get topological order of assets for execution.

    Args:
        config: Pipeline configuration

    Returns:
        List of asset names in execution order

    Raises:
        ValueError: If circular dependencies exist
    """
    # Build dependency graph
    graph: dict[str, list[str]] = {}
    all_assets = set()

    # Add sources
    for source_name in config.sources:
        graph[source_name] = []
        all_assets.add(source_name)

    # Add transforms
    for transform_name, transform in config.transforms.items():
        dependencies = [transform.source]
        graph[transform_name] = dependencies
        all_assets.add(transform_name)

    # Add sinks (depend on sources or transforms)
    # Note: Sinks are not explicitly linked in current schema
    # This is a simplified approach
    for sink_name in config.sinks:
        graph[sink_name] = []
        all_assets.add(sink_name)

    # Topological sort (Kahn's algorithm)
    in_degree: dict[str, int] = {node: 0 for node in all_assets}
    for node in graph:
        for neighbor in graph[node]:
            if neighbor in in_degree:
                in_degree[neighbor] += 1

    # Queue nodes with no incoming edges
    queue = [node for node in all_assets if in_degree[node] == 0]
    result: list[str] = []

    while queue:
        node = queue.pop(0)
        result.append(node)

        for neighbor in graph[node]:
            if neighbor in in_degree:
                in_degree[neighbor] -= 1
                if in_degree[neighbor] == 0:
                    queue.append(neighbor)

    if len(result) != len(all_assets):
        msg = "Circular dependency detected in pipeline configuration"
        raise ValueError(msg)

    return result
