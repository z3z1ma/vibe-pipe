"""Tests for configuration validation."""

import pytest

from vibe_piper.config.schema import (
    Config,
    EnvironmentConfig,
    ProjectConfig,
    SecretSource,
    SecretSpec,
)
from vibe_piper.config.validation import (
    ConfigValidationError,
    validate_config,
    validate_environment_override,
)


class TestValidateConfig:
    """Tests for validate_config function."""

    def test_validate_valid_config(self) -> None:
        """Test validating a valid configuration."""
        project = ProjectConfig(name="test", version="1.0")
        env = EnvironmentConfig(log_level="info")
        config = Config(project=project, environments={"dev": env})

        # Should not raise
        validate_config(config)

    def test_validate_missing_project_name(self) -> None:
        """Test validation fails with missing project name."""
        project = ProjectConfig(name="", version="1.0")
        config = Config(project=project)

        with pytest.raises(ConfigValidationError, match="Project name is required"):
            validate_config(config)

    def test_validate_missing_project_version(self) -> None:
        """Test validation fails with missing project version."""
        project = ProjectConfig(name="test", version="")
        config = Config(project=project)

        with pytest.raises(ConfigValidationError, match="Project version is required"):
            validate_config(config)

    def test_validate_invalid_log_level(self) -> None:
        """Test validation fails with invalid log level."""
        project = ProjectConfig(name="test", version="1.0")
        env = EnvironmentConfig(log_level="invalid")
        config = Config(project=project, environments={"dev": env})

        with pytest.raises(ConfigValidationError, match="Invalid log_level"):
            validate_config(config)

    def test_validate_invalid_parallelism(self) -> None:
        """Test validation fails with invalid parallelism."""
        project = ProjectConfig(name="test", version="1.0")
        env = EnvironmentConfig(parallelism=0)
        config = Config(project=project, environments={"dev": env})

        with pytest.raises(ConfigValidationError, match="Parallelism must be >= 1"):
            validate_config(config)

    def test_validate_cloud_storage_requires_bucket(self) -> None:
        """Test validation fails when cloud IO manager is missing bucket."""
        project = ProjectConfig(name="test", version="1.0")
        env = EnvironmentConfig(io_manager="s3", bucket=None)
        config = Config(project=project, environments={"dev": env})

        with pytest.raises(ConfigValidationError, match="Bucket is required"):
            validate_config(config)

    def test_validate_environment_exists(self) -> None:
        """Test validation of specific environment."""
        project = ProjectConfig(name="test", version="1.0")
        env = EnvironmentConfig()
        config = Config(project=project, environments={"dev": env})

        # Should not raise for existing environment
        validate_config(config, environment="dev")

        # Should raise for non-existing environment
        with pytest.raises(ConfigValidationError, match="Environment 'prod' not found"):
            validate_config(config, environment="prod")

    def test_validate_secret_with_path(self) -> None:
        """Test validation of secret with path."""
        project = ProjectConfig(name="test", version="1.0")
        spec = SecretSpec(from_=SecretSource.ENV, path="API_KEY", required=True)
        config = Config(project=project, secrets={"API_KEY": spec})

        # Should not raise
        validate_config(config)

    def test_validate_required_secret_without_path(self) -> None:
        """Test validation fails for required secret without path."""
        project = ProjectConfig(name="test", version="1.0")
        spec = SecretSpec(from_=SecretSource.ENV, path="", required=True)
        config = Config(project=project, secrets={"API_KEY": spec})

        with pytest.raises(ConfigValidationError, match="must have a path"):
            validate_config(config)

    def test_validate_secret_with_default_and_required(self) -> None:
        """Test validation fails for secret with both default and required."""
        project = ProjectConfig(name="test", version="1.0")
        spec = SecretSpec(
            from_=SecretSource.ENV, path="KEY", required=True, default="default"
        )
        config = Config(project=project, secrets={"KEY": spec})

        with pytest.raises(
            ConfigValidationError, match="cannot have both required=true and a default"
        ):
            validate_config(config)


class TestValidateEnvironmentOverride:
    """Tests for validate_environment_override function."""

    def test_validate_valid_override(self) -> None:
        """Test validating valid override."""
        project = ProjectConfig(name="test", version="1.0")
        env = EnvironmentConfig()
        config = Config(project=project, environments={"dev": env})

        # Should not raise
        validate_environment_override(config, "dev", "log_level", "debug")
        validate_environment_override(config, "dev", "parallelism", 4)
        validate_environment_override(config, "dev", "bucket", "my-bucket")

    def test_validate_override_unknown_environment(self) -> None:
        """Test validating override for unknown environment."""
        project = ProjectConfig(name="test", version="1.0")
        config = Config(project=project)

        with pytest.raises(
            ConfigValidationError, match="Cannot override unknown environment"
        ):
            validate_environment_override(config, "prod", "log_level", "debug")

    def test_validate_override_invalid_log_level(self) -> None:
        """Test validating override with invalid log level."""
        project = ProjectConfig(name="test", version="1.0")
        env = EnvironmentConfig()
        config = Config(project=project, environments={"dev": env})

        with pytest.raises(ConfigValidationError, match="Invalid log_level override"):
            validate_environment_override(config, "dev", "log_level", "invalid")

    def test_validate_override_invalid_parallelism(self) -> None:
        """Test validating override with invalid parallelism."""
        project = ProjectConfig(name="test", version="1.0")
        env = EnvironmentConfig()
        config = Config(project=project, environments={"dev": env})

        with pytest.raises(
            ConfigValidationError, match="Parallelism override must be >= 1"
        ):
            validate_environment_override(config, "dev", "parallelism", 0)

        with pytest.raises(
            ConfigValidationError, match="Parallelism override must be >= 1"
        ):
            validate_environment_override(config, "dev", "parallelism", -5)

    def test_validate_override_invalid_bucket(self) -> None:
        """Test validating override with empty bucket."""
        project = ProjectConfig(name="test", version="1.0")
        env = EnvironmentConfig()
        config = Config(project=project, environments={"dev": env})

        with pytest.raises(
            ConfigValidationError, match="Bucket override cannot be empty"
        ):
            validate_environment_override(config, "dev", "bucket", "")

    def test_validate_override_non_int_parallelism(self) -> None:
        """Test validating override with non-int parallelism."""
        project = ProjectConfig(name="test", version="1.0")
        env = EnvironmentConfig()
        config = Config(project=project, environments={"dev": env})

        with pytest.raises(
            ConfigValidationError, match="Parallelism override must be >= 1"
        ):
            validate_environment_override(config, "dev", "parallelism", "not-an-int")
