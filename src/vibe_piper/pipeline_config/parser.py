"""Configuration parser for pipeline definitions.

This module handles parsing TOML, YAML, and JSON pipeline configuration files
with environment variable interpolation.
"""

import json
import os
import re
import tomllib
from pathlib import Path
from typing import Any

try:
    import yaml
    from yaml import YAMLError

    YAML_AVAILABLE = True
except ImportError:
    YAML_AVAILABLE = False
    YAMLError = Exception  # type: ignore[misc,assignment]

from vibe_piper.pipeline_config.schema import (
    AuthConfig,
    CheckType,
    ExpectationCheck,
    ExpectationConfig,
    JobConfig,
    MaterializationType,
    PaginationConfig,
    PipelineConfig,
    PipelineMetadata,
    RateLimitConfig,
    SinkConfig,
    SinkType,
    SourceConfig,
    SourceType,
    TransformConfig,
    TransformStep,
    TransformType,
)


class PipelineConfigError(Exception):
    """Pipeline configuration error."""

    def __init__(
        self, message: str, path: Path | None = None, cause: Exception | None = None
    ) -> None:
        """Initialize pipeline config error.

        Args:
            message: Error message
            path: Optional path to config file
            cause: Optional underlying exception
        """
        self.path = path
        self.cause = cause
        if path:
            message = f"{message} (path: {path})"
        if cause:
            message = f"{message}: {cause}"
        super().__init__(message)


def interpolate_env_vars(data: Any) -> Any:
    """Recursively interpolate environment variables in configuration values.

    Supports ${VAR_NAME} syntax for environment variable substitution.

    Args:
        data: Configuration data (can be dict, list, or scalar)

    Returns:
        Data with environment variables interpolated

    Examples:
        >>> os.environ['DB_HOST'] = 'localhost'
        >>> interpolate_env_vars({'host': '${DB_HOST}'})
        {'host': 'localhost'}
        >>> interpolate_env_vars('${DB_HOST}')
        'localhost'
    """
    if isinstance(data, dict):
        return {key: interpolate_env_vars(value) for key, value in data.items()}
    elif isinstance(data, list):
        return [interpolate_env_vars(item) for item in data]
    elif isinstance(data, str):
        # Replace ${VAR_NAME} with actual environment variable values
        pattern = r"\$\{([^}]+)\}"

        def replace_env_var(match: re.Match[str]) -> str:
            var_name = match.group(1)
            # Support ${VAR_NAME} and ${VAR_NAME:-default} syntax
            if ":-" in var_name:
                var_name, default = var_name.split(":-", 1)
            else:
                default = ""
            return os.environ.get(var_name, default)

        return re.sub(pattern, replace_env_var, data)
    else:
        return data


def load_pipeline_config(
    path: str | Path, env_overrides: dict[str, Any] | None = None
) -> PipelineConfig:
    """Load pipeline configuration from file.

    Args:
        path: Path to configuration file (TOML, YAML, or JSON)
        env_overrides: Optional environment variable overrides

    Returns:
        Parsed and validated pipeline configuration

    Raises:
        PipelineConfigError: If configuration cannot be loaded or is invalid
    """
    path = Path(path)
    env_overrides = env_overrides or {}

    if not path.exists():
        msg = f"Configuration file not found: {path}"
        raise PipelineConfigError(msg, path=path)

    try:
        raw_data = _load_file_data(path)
    except Exception as e:
        suffix = path.suffix.lower()
        format_name = _get_format_name(suffix)
        msg = f"Invalid {format_name} syntax"
        raise PipelineConfigError(msg, path=path, cause=e) from e

    # Interpolate environment variables
    data = interpolate_env_vars(raw_data)

    # Apply environment overrides (temporary environment vars for interpolation)
    if env_overrides:
        original_env = os.environ.copy()
        try:
            os.environ.update({k: str(v) for k, v in env_overrides.items() if v is not None})
            data = interpolate_env_vars(raw_data)
        finally:
            os.environ.clear()
            os.environ.update(original_env)

    try:
        config = _parse_pipeline_config(data, path)
    except (KeyError, ValueError, TypeError) as e:
        msg = "Failed to parse pipeline configuration"
        raise PipelineConfigError(msg, path=path, cause=e) from e

    return config


def _load_file_data(path: Path) -> dict[str, Any]:
    """Load configuration data from file based on extension.

    Args:
        path: Path to configuration file

    Returns:
        Parsed configuration data

    Raises:
        ValueError: If file format is not supported
    """
    suffix = path.suffix.lower()

    if suffix == ".toml":
        with path.open("rb") as f:
            return tomllib.load(f)  # type: ignore[return-value]
    elif suffix in (".yaml", ".yml"):
        if not YAML_AVAILABLE:
            msg = "YAML support requires PyYAML. Install with: uv pip install pyyaml"
            raise ValueError(msg)
        with path.open("r", encoding="utf-8") as f:
            return yaml.safe_load(f) or {}  # type: ignore[no-any-return]
    elif suffix == ".json":
        with path.open("r", encoding="utf-8") as f:
            return json.load(f)  # type: ignore[no-any-return]
    else:
        msg = f"Unsupported configuration file format: {suffix}"
        raise ValueError(msg)


def _get_format_name(suffix: str) -> str:
    """Get human-readable format name from file extension.

    Args:
        suffix: File extension (e.g., '.toml')

    Returns:
        Format name (TOML, YAML, or JSON)
    """
    if suffix == ".toml":
        return "TOML"
    elif suffix in (".yaml", ".yml"):
        return "YAML"
    elif suffix == ".json":
        return "JSON"
    return "unknown"


def _parse_pipeline_config(data: dict[str, Any], path: Path) -> PipelineConfig:
    """Parse pipeline configuration from dictionary.

    Args:
        data: Parsed configuration data
        path: Path to config file (for reference)

    Returns:
        Pipeline configuration

    Raises:
        KeyError: If required fields are missing
        ValueError: If configuration is invalid
    """
    # Parse pipeline metadata
    pipeline_data = data.get("pipeline", {})
    if not pipeline_data.get("name"):
        msg = "Pipeline name is required in [pipeline] section"
        raise ValueError(msg)

    pipeline = PipelineMetadata(
        name=pipeline_data.get("name", ""),
        version=pipeline_data.get("version", "1.0.0"),
        description=pipeline_data.get("description"),
        author=pipeline_data.get("author"),
        created=pipeline_data.get("created"),
    )

    # Parse sources
    sources: dict[str, SourceConfig] = {}
    sources_data = data.get("sources", [])
    if not isinstance(sources_data, list):
        msg = "[[sources]] must be an array of source configurations"
        raise ValueError(msg)

    for source_data in sources_data:
        if not isinstance(source_data, dict):
            msg = "Each [[sources]] entry must be a table"
            raise ValueError(msg)

        source = _parse_source_config(source_data)
        if source.name in sources:
            msg = f"Duplicate source name: {source.name}"
            raise ValueError(msg)
        sources[source.name] = source

    # Parse sinks
    sinks: dict[str, SinkConfig] = {}
    sinks_data = data.get("sinks", [])
    if not isinstance(sinks_data, list):
        msg = "[[sinks]] must be an array of sink configurations"
        raise ValueError(msg)

    for sink_data in sinks_data:
        if not isinstance(sink_data, dict):
            msg = "Each [[sinks]] entry must be a table"
            raise ValueError(msg)

        sink = _parse_sink_config(sink_data)
        if sink.name in sinks:
            msg = f"Duplicate sink name: {sink.name}"
            raise ValueError(msg)
        sinks[sink.name] = sink

    # Parse transforms
    transforms: dict[str, TransformConfig] = {}
    transforms_data = data.get("transforms", [])
    if not isinstance(transforms_data, list):
        msg = "[[transforms]] must be an array of transform configurations"
        raise ValueError(msg)

    for transform_data in transforms_data:
        if not isinstance(transform_data, dict):
            msg = "Each [[transforms]] entry must be a table"
            raise ValueError(msg)

        transform = _parse_transform_config(transform_data)
        if transform.name in transforms:
            msg = f"Duplicate transform name: {transform.name}"
            raise ValueError(msg)
        transforms[transform.name] = transform

    # Parse expectations
    expectations: dict[str, ExpectationConfig] = {}
    expectations_data = data.get("expectations", [])
    if not isinstance(expectations_data, list):
        msg = "[[expectations]] must be an array of expectation configurations"
        raise ValueError(msg)

    for exp_data in expectations_data:
        if not isinstance(exp_data, dict):
            msg = "Each [[expectations]] entry must be a table"
            raise ValueError(msg)

        expectation = _parse_expectation_config(exp_data)
        if expectation.name in expectations:
            msg = f"Duplicate expectation name: {expectation.name}"
            raise ValueError(msg)
        expectations[expectation.name] = expectation

    # Parse jobs
    jobs: dict[str, JobConfig] = {}
    jobs_data = data.get("jobs", [])
    if not isinstance(jobs_data, list):
        msg = "[[jobs]] must be an array of job configurations"
        raise ValueError(msg)

    for job_data in jobs_data:
        if not isinstance(jobs_data, dict):
            msg = "Each [[jobs]] entry must be a table"
            raise ValueError(msg)

        job = _parse_job_config(job_data)
        if job.name in jobs:
            msg = f"Duplicate job name: {job.name}"
            raise ValueError(msg)
        jobs[job.name] = job

    return PipelineConfig(
        pipeline=pipeline,
        sources=sources,
        sinks=sinks,
        transforms=transforms,
        expectations=expectations,
        jobs=jobs,
        config_path=path,
    )


def _parse_source_config(data: dict[str, Any]) -> SourceConfig:
    """Parse source configuration from dictionary.

    Args:
        data: Source configuration data

    Returns:
        Source configuration
    """
    name = data.get("name")
    if not name:
        msg = "Source must have a 'name' field"
        raise ValueError(msg)

    type_value = data.get("type")
    if not type_value:
        msg = f"Source '{name}' must have a 'type' field"
        raise ValueError(msg)

    try:
        source_type = SourceType(type_value)
    except ValueError as err:
        valid_types = [t.value for t in SourceType]
        msg = f"Source '{name}' has invalid type '{type_value}'. Must be one of {valid_types}"
        raise ValueError(msg) from err

    # Parse authentication
    auth = None
    auth_data = data.get("auth")
    if auth_data:
        auth = _parse_auth_config(auth_data)

    # Parse pagination
    pagination = None
    pagination_data = data.get("pagination")
    if pagination_data:
        pagination = _parse_pagination_config(pagination_data)

    # Parse rate limit
    rate_limit = None
    rate_limit_data = data.get("rate_limit")
    if rate_limit_data:
        rate_limit = _parse_rate_limit_config(rate_limit_data)

    return SourceConfig(
        name=name,
        type=source_type,
        endpoint=data.get("endpoint"),
        base_url=data.get("base_url"),
        query=data.get("query"),
        path=data.get("path"),
        connection=data.get("connection"),
        table=data.get("table"),
        schema=data.get("schema"),
        auth=auth,
        pagination=pagination,
        rate_limit=rate_limit,
        incremental=data.get("increment", False),
        watermark_column=data.get("watermark_column"),
        description=data.get("description"),
        tags=data.get("tags", []),
        additional_config={
            k: v
            for k, v in data.items()
            if k
            not in {
                "name",
                "type",
                "endpoint",
                "base_url",
                "query",
                "path",
                "connection",
                "table",
                "schema",
                "auth",
                "pagination",
                "rate_limit",
                "increment",
                "watermark_column",
                "description",
                "tags",
            }
        },
    )


def _parse_auth_config(data: dict[str, Any]) -> AuthConfig:
    """Parse authentication configuration.

    Args:
        data: Authentication configuration data

    Returns:
        Authentication configuration
    """
    auth_type = data.get("type")
    if not auth_type:
        msg = "Auth configuration must have a 'type' field"
        raise ValueError(msg)

    return AuthConfig(
        type=auth_type,
        from_env=data.get("from_env"),
        token=data.get("token"),
        username=data.get("username"),
        password=data.get("password"),
    )


def _parse_pagination_config(data: dict[str, Any]) -> PaginationConfig:
    """Parse pagination configuration.

    Args:
        data: Pagination configuration data

    Returns:
        Pagination configuration
    """
    return PaginationConfig(
        type=data.get("type", ""),
        items_path=data.get("items_path"),
        limit_param=data.get("limit_param"),
        offset_param=data.get("offset_param"),
        page_param=data.get("page_param"),
        total_key=data.get("total_key"),
    )


def _parse_rate_limit_config(data: dict[str, Any]) -> RateLimitConfig:
    """Parse rate limit configuration.

    Args:
        data: Rate limit configuration data

    Returns:
        Rate limit configuration
    """
    return RateLimitConfig(
        requests=data.get("requests", 10),
        window_seconds=data.get("window_seconds", 1),
    )


def _parse_sink_config(data: dict[str, Any]) -> SinkConfig:
    """Parse sink configuration from dictionary.

    Args:
        data: Sink configuration data

    Returns:
        Sink configuration
    """
    name = data.get("name")
    if not name:
        msg = "Sink must have a 'name' field"
        raise ValueError(msg)

    type_value = data.get("type")
    if not type_value:
        msg = f"Sink '{name}' must have a 'type' field"
        raise ValueError(msg)

    try:
        sink_type = SinkType(type_value)
    except ValueError as err:
        valid_types = [t.value for t in SinkType]
        msg = f"Sink '{name}' has invalid type '{type_value}'. Must be one of {valid_types}"
        raise ValueError(msg) from err

    materialization_value = data.get("materialization", "table")
    try:
        materialization = MaterializationType(materialization_value)
    except ValueError as err:
        valid_types = [t.value for t in MaterializationType]
        msg = (
            f"Sink '{name}' has invalid materialization '{materialization_value}'. "
            f"Must be one of {valid_types}"
        )
        raise ValueError(msg) from err

    return SinkConfig(
        name=name,
        type=sink_type,
        connection=data.get("connection"),
        table=data.get("table"),
        schema_name=data.get("schema"),
        path=data.get("path"),
        format=data.get("format"),
        materialization=materialization,
        upsert_key=data.get("upsert_key"),
        batch_size=data.get("batch_size", 1000),
        partition_cols=data.get("partition_cols", []),
        compression=data.get("compression"),
        schema=data.get("schema"),
        description=data.get("description"),
        tags=data.get("tags", []),
        additional_config={
            k: v
            for k, v in data.items()
            if k
            not in {
                "name",
                "type",
                "connection",
                "table",
                "schema",
                "path",
                "format",
                "materialization",
                "upsert_key",
                "batch_size",
                "partition_cols",
                "compression",
                "description",
                "tags",
            }
        },
    )


def _parse_transform_config(data: dict[str, Any]) -> TransformConfig:
    """Parse transform configuration from dictionary.

    Args:
        data: Transform configuration data

    Returns:
        Transform configuration
    """
    name = data.get("name")
    if not name:
        msg = "Transform must have a 'name' field"
        raise ValueError(msg)

    source = data.get("source")
    if not source:
        msg = f"Transform '{name}' must specify a 'source'"
        raise ValueError(msg)

    steps_data = data.get("steps", [])
    if not isinstance(steps_data, list):
        msg = f"Transform '{name}' steps must be an array"
        raise ValueError(msg)

    if not steps_data:
        msg = f"Transform '{name}' must have at least one step"
        raise ValueError(msg)

    steps = [_parse_transform_step(step) for step in steps_data]

    return TransformConfig(
        name=name,
        source=source,
        steps=steps,
        description=data.get("description"),
        tags=data.get("tags", []),
    )


def _parse_transform_step(data: dict[str, Any]) -> TransformStep:
    """Parse transform step from dictionary.

    Args:
        data: Transform step data

    Returns:
        Transform step
    """
    type_value = data.get("type")
    if not type_value:
        msg = "Transform step must have a 'type' field"
        raise ValueError(msg)

    try:
        step_type = TransformType(type_value)
    except ValueError as err:
        valid_types = [t.value for t in TransformType]
        msg = f"Transform step has invalid type '{type_value}'. Must be one of {valid_types}"
        raise ValueError(msg) from err

    return TransformStep(
        type=step_type,
        field=data.get("field"),
        mappings=data.get("mappings"),
        schema=data.get("schema"),
        condition=data.get("condition"),
        pattern=data.get("pattern"),
        value=data.get("value"),
        aggregation=data.get("aggregation"),
        sort_by=data.get("sort_by"),
        join_with=data.get("join_with"),
        join_on=data.get("join_on"),
        description=data.get("description"),
    )


def _parse_expectation_config(data: dict[str, Any]) -> ExpectationConfig:
    """Parse expectation configuration from dictionary.

    Args:
        data: Expectation configuration data

    Returns:
        Expectation configuration
    """
    name = data.get("name")
    if not name:
        msg = "Expectation must have a 'name' field"
        raise ValueError(msg)

    asset = data.get("asset")
    if not asset:
        msg = f"Expectation '{name}' must specify an 'asset'"
        raise ValueError(msg)

    checks_data = data.get("checks", [])
    if not isinstance(checks_data, list):
        msg = f"Expectation '{name}' checks must be an array"
        raise ValueError(msg)

    if not checks_data:
        msg = f"Expectation '{name}' must have at least one check"
        raise ValueError(msg)

    checks = [_parse_expectation_check(check) for check in checks_data]

    return ExpectationConfig(
        name=name,
        asset=asset,
        checks=checks,
        description=data.get("description"),
    )


def _parse_expectation_check(data: dict[str, Any]) -> ExpectationCheck:
    """Parse expectation check from dictionary.

    Args:
        data: Expectation check data

    Returns:
        Expectation check
    """
    type_value = data.get("type")
    if not type_value:
        msg = "Expectation check must have a 'type' field"
        raise ValueError(msg)

    try:
        check_type = CheckType(type_value)
    except ValueError as err:
        valid_types = [t.value for t in CheckType]
        msg = f"Expectation check has invalid type '{type_value}'. Must be one of {valid_types}"
        raise ValueError(msg) from err

    return ExpectationCheck(
        type=check_type,
        column=data.get("column"),
        pattern=data.get("pattern"),
        min_value=data.get("min_value"),
        max_value=data.get("max_value"),
        values=data.get("values"),
        min_rows=data.get("min_rows"),
        max_rows=data.get("max_rows"),
        severity=data.get("severity", "error"),
        description=data.get("description"),
    )


def _parse_job_config(data: dict[str, Any]) -> JobConfig:
    """Parse job configuration from dictionary.

    Args:
        data: Job configuration data

    Returns:
        Job configuration
    """
    name = data.get("name")
    if not name:
        msg = "Job must have a 'name' field"
        raise ValueError(msg)

    schedule = data.get("schedule")
    if not schedule:
        msg = f"Job '{name}' must have a 'schedule' field"
        raise ValueError(msg)

    return JobConfig(
        name=name,
        schedule=schedule,
        sources=data.get("sources", []),
        sinks=data.get("sinks", []),
        transforms=data.get("transforms", []),
        expectations=data.get("expectations", []),
        environment=data.get("environment", "default"),
        retry_on_failure=data.get("retry_on_failure", False),
        timeout=data.get("timeout"),
        description=data.get("description"),
        tags=data.get("tags", []),
    )
