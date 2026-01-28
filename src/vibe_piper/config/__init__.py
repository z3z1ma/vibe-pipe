"""Configuration management for Vibe Piper.

This module provides environment-specific configuration management with secrets support.

Example usage:
    ```python
    from vibe_piper.config import load_config, load_secrets

    # Load configuration for specific environment
    config = load_config("vibepiper.toml", environment="prod")

    # Load secrets
    secrets = load_secrets(config)

    # Access configuration
    env_config = config.get_environment("prod")
    print(f"IO Manager: {env_config.io_manager}")
    ```

Configuration file format (vibepiper.toml):
    ```toml
    [project]
    name = "my-pipeline"
    version = "0.1.0"

    [environments.dev]
    io_manager = "memory"
    log_level = "debug"

    [environments.prod]
    io_manager = "s3"
    bucket = "my-bucket"
    region = "us-west-2"
    log_level = "info"

    [secrets]
    AWS_SECRET_ACCESS_KEY = { from = "env" }
    API_KEY = { from = "vault", path = "secret/api-key" }
    ```
"""

from vibe_piper.config.loader import (
    ConfigLoadError,
    find_config_file,
    load_config,
    load_config_from_environment,
)
from vibe_piper.config.schema import (
    Config,
    EnvironmentConfig,
    ProjectConfig,
    SecretSource,
    SecretSpec,
)
from vibe_piper.config.secrets import (
    SecretNotFoundError,
    get_secret,
    interpolate_secrets,
    load_secrets,
    mask_secrets,
)
from vibe_piper.config.validation import (
    ConfigValidationError,
    validate_config,
    validate_environment_override,
)

__all__ = [
    # Loader
    "load_config",
    "load_config_from_environment",
    "find_config_file",
    "ConfigLoadError",
    # Schema
    "Config",
    "ProjectConfig",
    "EnvironmentConfig",
    "SecretSpec",
    "SecretSource",
    # Secrets
    "load_secrets",
    "get_secret",
    "interpolate_secrets",
    "mask_secrets",
    "SecretNotFoundError",
    # Validation
    "validate_config",
    "validate_environment_override",
    "ConfigValidationError",
]
