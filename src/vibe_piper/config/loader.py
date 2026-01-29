"""Configuration loading and parsing for Vibe Piper."""

import json
import tomllib
from pathlib import Path
from typing import Any

from vibe_piper.config.schema import (
    Config,
    EnvironmentConfig,
    ProjectConfig,
    SecretSource,
    SecretSpec,
)
from vibe_piper.config.validation import validate_config

try:
    import yaml
    from yaml import YAMLError

    YAML_AVAILABLE = True
except ImportError:
    YAML_AVAILABLE = False
    YAMLError = Exception  # type: ignore[misc,assignment]


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
        data = _load_file_data(path)
    except Exception as e:
        # Check if it's a specific parsing error or a general error
        if isinstance(e, (tomllib.TOMLDecodeError, json.JSONDecodeError, YAMLError)):
            msg = f"Invalid {_get_format_name(path)} syntax"
        else:
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
            return tomllib.load(f)
    elif suffix == ".yaml" or suffix == ".yml":
        if not YAML_AVAILABLE:
            msg = "YAML support requires PyYAML. Install it with: uv pip install pyyaml"
            raise ValueError(msg)
        with path.open("r", encoding="utf-8") as f:
            return yaml.safe_load(f)  # type: ignore[no-any-return]
    elif suffix == ".json":
        with path.open("r", encoding="utf-8") as f:
            return json.load(f)  # type: ignore[no-any-return]
    else:
        msg = f"Unsupported configuration file format: {suffix}"
        raise ValueError(msg)


def _get_format_name(path: Path) -> str:
    """Get human-readable format name from file extension.

    Args:
        path: Path to configuration file

    Returns:
        Format name (TOML, YAML, or JSON)
    """
    suffix = path.suffix.lower()
    if suffix == ".toml":
        return "TOML"
    elif suffix in (".yaml", ".yml"):
        return "YAML"
    elif suffix == ".json":
        return "JSON"
    return "unknown"


def _merge_additional_config(parent: dict[str, Any], child: dict[str, Any]) -> dict[str, Any]:
    """Merge additional configuration from parent and child.

    Child values override parent values.

    Args:
        parent: Parent environment's additional config
        child: Child environment's additional config

    Returns:
        Merged additional configuration
    """
    merged = parent.copy()
    merged.update(child)
    return merged


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

    # First pass: parse all environment configs
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
            "inherits",
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

    # Second pass: apply inheritance
    for env_name, env_data in environments_data.items():
        if not isinstance(env_data, dict):
            continue

        inherits_from = env_data.get("inherits")
        if inherits_from:
            if inherits_from not in environments:
                msg = (
                    f"Environment '{env_name}' inherits from '{inherits_from}', "
                    f"but '{inherits_from}' does not exist"
                )
                raise ValueError(msg)

            # Merge child config with parent config (child overrides parent)
            parent_env = environments[inherits_from]
            child_env = environments[env_name]

            environments[env_name] = EnvironmentConfig(
                io_manager=child_env.io_manager or parent_env.io_manager,
                log_level=child_env.log_level or parent_env.log_level,
                parallelism=child_env.parallelism or parent_env.parallelism,
                bucket=child_env.bucket or parent_env.bucket,
                region=child_env.region or parent_env.region,
                endpoint=child_env.endpoint or parent_env.endpoint,
                credentials_path=child_env.credentials_path or parent_env.credentials_path,
                additional_config=_merge_additional_config(
                    parent_env.additional_config,
                    child_env.additional_config,
                ),
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

    Searches for configuration files in order of preference:
    1. vibepiper.toml
    2. vibepiper.yaml / vibepiper.yml
    3. vibepiper.json

    Args:
        search_path: Starting path (defaults to current directory)

    Returns:
        Path to configuration file, or None if not found
    """
    search_path = search_path or Path.cwd()

    # Search upward for config file in order of preference
    config_names = [
        "vibepiper.toml",
        "vibepiper.yaml",
        "vibepiper.yml",
        "vibepiper.json",
    ]

    for path in [search_path, *search_path.parents]:
        for config_name in config_names:
            config_file = path / config_name
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

    return load_config(config_path, environment=environment, cli_overrides=cli_overrides)
