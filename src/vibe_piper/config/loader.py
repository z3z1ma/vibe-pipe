"""Configuration loading and parsing for Vibe Piper."""

from pathlib import Path
from typing import Any

import tomli

from vibe_piper.config.schema import (
    Config,
    EnvironmentConfig,
    ProjectConfig,
    SecretSource,
    SecretSpec,
)
from vibe_piper.config.validation import validate_config


class ConfigLoadError(Exception):
    """Configuration loading error."""

    def __init__(
        self, message: str, path: Path | None = None, cause: Exception | None = None
    ) -> None:
        """Initialize config load error.

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


def load_config(
    path: str | Path,
    environment: str | None = None,
    cli_overrides: dict[str, Any] | None = None,
) -> Config:
    """Load configuration from TOML file.

    Args:
        path: Path to configuration file
        environment: Optional environment to load (if None, loads base config only)
        cli_overrides: Optional CLI-provided configuration overrides

    Returns:
        Loaded and validated configuration

    Raises:
        ConfigLoadError: If configuration cannot be loaded or is invalid
    """
    path = Path(path)
    cli_overrides = cli_overrides or {}

    if not path.exists():
        msg = f"Configuration file not found: {path}"
        raise ConfigLoadError(msg, path=path)

    try:
        with path.open("rb") as f:
            data = tomli.load(f)
    except tomli.TOMLDecodeError as e:
        msg = "Invalid TOML syntax"
        raise ConfigLoadError(msg, path=path, cause=e) from e
    except OSError as e:
        msg = "Failed to read configuration file"
        raise ConfigLoadError(msg, path=path, cause=e) from e

    try:
        config = _parse_config(data, path)
    except (KeyError, ValueError, TypeError) as e:
        msg = "Failed to parse configuration"
        raise ConfigLoadError(msg, path=path, cause=e) from e

    # Apply CLI overrides
    if cli_overrides:
        config.cli_overrides = cli_overrides

    # Validate the final configuration
    validate_config(config, environment)

    return config


def _parse_config(data: dict[str, Any], path: Path) -> Config:
    """Parse configuration from TOML data.

    Args:
        data: Parsed TOML data
        path: Path to config file (for reference)

    Returns:
        Parsed configuration

    Raises:
        KeyError: If required fields are missing
        ValueError: If configuration is invalid
    """
    # Parse project section
    project_data = data.get("project", {})
    project = ProjectConfig(
        name=project_data.get("name", ""),
        version=project_data.get("version", ""),
        description=project_data.get("description"),
    )

    # Parse environments section
    environments: dict[str, EnvironmentConfig] = {}
    environments_data = data.get("environments", {})
    for env_name, env_data in environments_data.items():
        if not isinstance(env_data, dict):
            msg = f"Environment '{env_name}' must be a table"
            raise ValueError(msg)

        # Extract additional config (any fields we don't explicitly handle)
        known_fields = {
            "io_manager",
            "log_level",
            "parallelism",
            "bucket",
            "region",
            "endpoint",
            "credentials_path",
        }
        additional_config = {k: v for k, v in env_data.items() if k not in known_fields}

        environments[env_name] = EnvironmentConfig(
            io_manager=env_data.get("io_manager"),
            log_level=env_data.get("log_level", "info"),
            parallelism=env_data.get("parallelism"),
            bucket=env_data.get("bucket"),
            region=env_data.get("region"),
            endpoint=env_data.get("endpoint"),
            credentials_path=env_data.get("credentials_path"),
            additional_config=additional_config,
        )

    # Parse secrets section
    secrets: dict[str, SecretSpec] = {}
    secrets_data = data.get("secrets", {})
    for secret_name, secret_spec in secrets_data.items():
        if not isinstance(secret_spec, dict):
            msg = f"Secret '{secret_name}' must be a table with 'from' field"
            raise ValueError(msg)

        from_value = secret_spec.get("from")
        if not from_value:
            msg = f"Secret '{secret_name}' must specify 'from' field"
            raise ValueError(msg)

        try:
            source = SecretSource(from_value)
        except ValueError as err:
            valid_sources = [s.value for s in SecretSource]
            msg = (
                f"Secret '{secret_name}' has invalid source '{from_value}'. "
                f"Must be one of {valid_sources}"
            )
            raise ValueError(msg) from err

        secrets[secret_name] = SecretSpec(
            from_=source,
            path=secret_spec.get("path", secret_name),
            required=secret_spec.get("required", True),
            default=secret_spec.get("default"),
        )

    return Config(
        project=project,
        environments=environments,
        secrets=secrets,
        config_path=path,
    )


def find_config_file(search_path: Path | None = None) -> Path | None:
    """Find configuration file by searching upward from path.

    Args:
        search_path: Starting path (defaults to current directory)

    Returns:
        Path to configuration file, or None if not found
    """
    search_path = search_path or Path.cwd()

    # Search upward for config file
    for path in [search_path, *search_path.parents]:
        config_file = path / "vibepiper.toml"
        if config_file.exists():
            return config_file

    return None


def load_config_from_environment(
    environment: str,
    search_path: Path | None = None,
    cli_overrides: dict[str, Any] | None = None,
) -> Config:
    """Load configuration for a specific environment.

    This is a convenience function that finds the config file and loads it
    for the specified environment.

    Args:
        environment: Environment name (e.g., dev, staging, prod)
        search_path: Optional starting path for config file search
        cli_overrides: Optional CLI-provided configuration overrides

    Returns:
        Loaded and validated configuration

    Raises:
        ConfigLoadError: If configuration cannot be loaded or is invalid
    """
    config_path = find_config_file(search_path)
    if config_path is None:
        msg = "No configuration file found. Please create a vibepiper.toml file."
        raise ConfigLoadError(msg)

    return load_config(
        config_path, environment=environment, cli_overrides=cli_overrides
    )
