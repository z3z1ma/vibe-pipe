"""Pipeline configuration module.

This module provides tools for defining pipelines declaratively via
configuration files (TOML, YAML, JSON) without writing code.

The configuration-driven pipeline system enables:
- Declarative pipeline definitions
- Environment variable interpolation
- Automatic dependency inference
- Schema validation
- Asset graph generation

Example usage:

    ```python
    from vibe_piper.pipeline_config import load_pipeline_config, generate_pipeline

    # Load configuration
    config = load_pipeline_config("pipeline.toml")

    # Generate Python module with assets
    pipeline_module = generate_pipeline(config)

    # Use generated assets
    from vibe_piper import PipelineBuilder

    builder = PipelineBuilder(config.pipeline.name)
    builder.asset("users_api", getattr(pipeline_module, "users_api"))
    builder.asset("clean_users", getattr(pipeline_module, "clean_users"))

    graph = builder.build()
    ```

Configuration file format (TOML):

    ```toml
    [pipeline]
    name = "user_ingestion"
    version = "1.0.0"

    [[sources]]
    name = "users_api"
    type = "api"
    endpoint = "/users"
    base_url = "https://api.example.com/v1"

    [[sinks]]
    name = "users_db"
    type = "database"
    connection = "postgres://user:pass@localhost:5432/db"
    table = "users"

    [[transforms]]
    name = "clean_users"
    source = "users_api"
    steps = [
        { type = "filter", condition = "email is not null" },
    ]
    ```

Environment variable interpolation:

    ```toml
    [[sources]]
    connection = "postgres://${DB_USER}:${DB_PASSWORD}@${DB_HOST}:5432/${DB_NAME}"
    ```

"""

from vibe_piper.pipeline_config.generator import (
    PipelineGeneratorError,
    build_asset_graph,
    generate_asset_dependencies,
    generate_expectations,
    generate_pipeline_from_config,
)
from vibe_piper.pipeline_config.loader import (
    find_pipeline_file,
    get_dependency_order,
    load_pipeline_from_path,
    validate_pipeline_config,
)
from vibe_piper.pipeline_config.parser import (
    PipelineConfig,
    PipelineConfigError,
    interpolate_env_vars,
    load_pipeline_config,
)

__all__ = [
    # Parser
    "load_pipeline_config",
    "PipelineConfig",
    "PipelineConfigError",
    "interpolate_env_vars",
    # Loader
    "load_pipeline_from_path",
    "find_pipeline_file",
    "validate_pipeline_config",
    "get_dependency_order",
    # Generator
    "generate_pipeline_from_config",
    "generate_expectations",
    "generate_asset_dependencies",
    "build_asset_graph",
    "PipelineGeneratorError",
]
