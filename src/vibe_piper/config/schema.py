"""Configuration schema definitions for Vibe Piper."""

from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path
from typing import Any


class SecretSource(str, Enum):
    """Secret source types."""

    ENV = "env"
    VAULT = "vault"
    FILE = "file"


@dataclass
class SecretSpec:
    """Secret specification.

    Attributes:
        from_: Source type (env, vault, file)
        path: Path to secret (e.g., env var name, vault path, file path)
        required: Whether the secret is required
        default: Optional default value (only used if not required)
    """

    from_: SecretSource
    path: str
    required: bool = True
    default: str | None = None


@dataclass
class ProjectConfig:
    """Project-level configuration.

    Attributes:
        name: Project name
        version: Project version
        description: Optional project description
    """

    name: str
    version: str
    description: str | None = None


@dataclass
class EnvironmentConfig:
    """Environment-specific configuration.

    Attributes:
        io_manager: IO manager to use (e.g., memory, s3, gcs)
        log_level: Logging level (debug, info, warning, error)
        parallelism: Maximum parallelism for pipeline execution
        bucket: S3/GCS bucket name (for cloud storage)
        region: Cloud region (e.g., us-west-2)
        endpoint: Custom endpoint URL
        credentials_path: Path to credentials file
        additional_config: Any additional environment-specific configuration
    """

    io_manager: str | None = None
    log_level: str = "info"
    parallelism: int | None = None
    bucket: str | None = None
    region: str | None = None
    endpoint: str | None = None
    credentials_path: str | None = None
    additional_config: dict[str, Any] = field(default_factory=dict)


@dataclass
class Config:
    """Complete configuration.

    Attributes:
        project: Project configuration
        environments: Dictionary of environment configurations
        secrets: Dictionary of secret specifications
        cli_overrides: CLI-provided overrides (not persisted)
        config_path: Path to the config file (for reference)
    """

    project: ProjectConfig
    environments: dict[str, EnvironmentConfig] = field(default_factory=dict)
    secrets: dict[str, SecretSpec] = field(default_factory=dict)
    cli_overrides: dict[str, Any] = field(default_factory=dict)
    config_path: Path | None = None

    def get_environment(self, env_name: str, apply_overrides: bool = True) -> EnvironmentConfig:
        """Get environment configuration.

        Args:
            env_name: Environment name (e.g., dev, staging, prod)
            apply_overrides: Whether to apply CLI overrides to the config

        Returns:
            Environment configuration

        Raises:
            KeyError: If environment not found
        """
        if env_name not in self.environments:
            msg = f"Environment '{env_name}' not found in configuration"
            raise KeyError(msg)

        env_config = self.environments[env_name]

        # Apply CLI overrides if requested
        if apply_overrides and self.cli_overrides:
            return self._apply_overrides(env_config)

        return env_config

    def has_environment(self, env_name: str) -> bool:
        """Check if environment exists.

        Args:
            env_name: Environment name

        Returns:
            True if environment exists
        """
        return env_name in self.environments

    def get_secret(self, key: str) -> SecretSpec:
        """Get secret specification.

        Args:
            key: Secret key

        Returns:
            Secret specification

        Raises:
            KeyError: If secret not found
        """
        if key not in self.secrets:
            msg = f"Secret '{key}' not found in configuration"
            raise KeyError(msg)
        return self.secrets[key]

    def has_secret(self, key: str) -> bool:
        """Check if secret exists.

        Args:
            key: Secret key

        Returns:
            True if secret exists
        """
        return key in self.secrets

    def _apply_overrides(self, env_config: EnvironmentConfig) -> EnvironmentConfig:
        """Apply CLI overrides to environment configuration.

        Args:
            env_config: Environment configuration to apply overrides to

        Returns:
            Environment configuration with overrides applied
        """
        overrides = self.cli_overrides

        # Create a copy with overrides applied
        additional_config = env_config.additional_config.copy()
        additional_config.update(
            {
                k: v
                for k, v in overrides.items()
                if k
                not in {
                    "io_manager",
                    "log_level",
                    "parallelism",
                    "bucket",
                    "region",
                    "endpoint",
                    "credentials_path",
                }
            }
        )

        return EnvironmentConfig(
            io_manager=overrides.get("io_manager", env_config.io_manager),
            log_level=overrides.get("log_level", env_config.log_level),
            parallelism=overrides.get("parallelism", env_config.parallelism),
            bucket=overrides.get("bucket", env_config.bucket),
            region=overrides.get("region", env_config.region),
            endpoint=overrides.get("endpoint", env_config.endpoint),
            credentials_path=overrides.get("credentials_path", env_config.credentials_path),
            additional_config=additional_config,
        )
