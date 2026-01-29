"""Configuration validation for Vibe Piper."""

from typing import Any

from vibe_piper.config.schema import Config


class ConfigValidationError(Exception):
    """Configuration validation error."""

    def __init__(self, message: str, field: str | None = None) -> None:
        """Initialize config validation error.

        Args:
            message: Error message
            field: Optional field name that failed validation
        """
        self.field = field
        if field:
            message = f"Validation failed for field '{field}': {message}"
        super().__init__(message)


def validate_config(config: Config, environment: str | None = None) -> None:
    """Validate configuration.

    Args:
        config: Configuration to validate
        environment: Optional environment to validate specifically

    Raises:
        ConfigValidationError: If configuration is invalid
    """
    # Validate project config
    _validate_project_config(config)

    # Validate environments
    _validate_environments(config)

    # Validate specific environment if requested
    if environment:
        _validate_environment_exists(config, environment)

    # Validate secrets
    _validate_secrets(config)


def _validate_project_config(config: Config) -> None:
    """Validate project configuration.

    Args:
        config: Configuration to validate

    Raises:
        ConfigValidationError: If project configuration is invalid
    """
    if not config.project.name:
        msg = "Project name is required"
        raise ConfigValidationError(msg, field="project.name")

    if not config.project.version:
        msg = "Project version is required"
        raise ConfigValidationError(msg, field="project.version")


def _validate_environments(config: Config) -> None:
    """Validate all environments.

    Args:
        config: Configuration to validate

    Raises:
        ConfigValidationError: If any environment is invalid
    """
    for env_name, env_config in config.environments.items():
        _validate_environment_config(config, env_name, env_config)


def _validate_environment_config(_config: Config, env_name: str, env_config: Any) -> None:
    """Validate a single environment configuration.

    Args:
        config: Full configuration
        env_name: Environment name
        env_config: Environment configuration to validate

    Raises:
        ConfigValidationError: If environment configuration is invalid
    """
    # Validate log level
    valid_log_levels = {"debug", "info", "warning", "error", "critical"}
    if env_config.log_level not in valid_log_levels:
        msg = f"Invalid log_level '{env_config.log_level}'. Must be one of {valid_log_levels}"
        raise ConfigValidationError(msg, field=f"environments.{env_name}.log_level")

    # Validate parallelism
    if env_config.parallelism is not None and env_config.parallelism < 1:
        msg = f"Parallelism must be >= 1, got {env_config.parallelism}"
        raise ConfigValidationError(msg, field=f"environments.{env_name}.parallelism")

    # Validate cloud storage configuration
    if env_config.io_manager in ("s3", "gcs", "azure") and not env_config.bucket:
        msg = f"Bucket is required for io_manager '{env_config.io_manager}'"
        raise ConfigValidationError(msg, field=f"environments.{env_name}.bucket")


def _validate_environment_exists(config: Config, environment: str) -> None:
    """Validate that requested environment exists.

    Args:
        config: Configuration to validate
        environment: Environment name

    Raises:
        ConfigValidationError: If environment doesn't exist
    """
    if not config.has_environment(environment):
        available = list(config.environments.keys())
        msg = f"Environment '{environment}' not found. Available environments: {available}"
        raise ConfigValidationError(msg, field="environment")


def _validate_secrets(config: Config) -> None:
    """Validate secret specifications.

    Args:
        config: Configuration to validate

    Raises:
        ConfigValidationError: If any secret specification is invalid
    """
    for secret_name, secret_spec in config.secrets.items():
        # Validate that required secrets have a path
        if secret_spec.required and not secret_spec.path:
            msg = f"Required secret '{secret_name}' must have a path"
            raise ConfigValidationError(msg, field=f"secrets.{secret_name}.path")

        # Validate that default values are only for non-required secrets
        if secret_spec.default is not None and secret_spec.required:
            msg = f"Secret '{secret_name}' cannot have both required=true and a default value"
            raise ConfigValidationError(msg, field=f"secrets.{secret_name}")

        # Validate vault path format
        if (
            secret_spec.from_.value == "vault"
            and secret_spec.path
            and not secret_spec.path.startswith("secret/")
        ):
            msg = (
                f"Vault path for secret '{secret_name}' should start with 'secret/' "
                f"(got: {secret_spec.path})"
            )
            # This is a warning, not an error
            import warnings

            warnings.warn(msg, stacklevel=2)


def validate_environment_override(
    config: Config,
    environment: str,
    override_key: str,
    override_value: Any,
) -> None:
    """Validate a configuration override.

    Args:
        config: Full configuration
        environment: Environment name
        override_key: Key being overridden
        override_value: New value

    Raises:
        ConfigValidationError: If override is invalid
    """
    # Check if environment exists
    if not config.has_environment(environment):
        msg = f"Cannot override unknown environment '{environment}'"
        raise ConfigValidationError(msg)

    # Validate specific overrides
    if override_key == "log_level":
        valid_log_levels = {"debug", "info", "warning", "error", "critical"}
        if override_value not in valid_log_levels:
            msg = (
                f"Invalid log_level override '{override_value}'. Must be one of {valid_log_levels}"
            )
            raise ConfigValidationError(msg, field=override_key)

    elif override_key == "parallelism":
        if not isinstance(override_value, int) or override_value < 1:
            msg = f"Parallelism override must be >= 1, got {override_value}"
            raise ConfigValidationError(msg, field=override_key)

    elif override_key == "bucket":
        if not override_value:
            msg = "Bucket override cannot be empty"
            raise ConfigValidationError(msg, field=override_key)
