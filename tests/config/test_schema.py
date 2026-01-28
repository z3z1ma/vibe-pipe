"""Tests for configuration schema."""

import pytest

from vibe_piper.config.schema import (
    Config,
    EnvironmentConfig,
    ProjectConfig,
    SecretSource,
    SecretSpec,
)


class TestProjectConfig:
    """Tests for ProjectConfig."""

    def test_create_project_config(self) -> None:
        """Test creating a project configuration."""
        config = ProjectConfig(name="test-project", version="1.0.0")
        assert config.name == "test-project"
        assert config.version == "1.0.0"
        assert config.description is None

    def test_project_config_with_description(self) -> None:
        """Test creating project config with description."""
        config = ProjectConfig(name="test", version="1.0", description="Test project")
        assert config.description == "Test project"


class TestEnvironmentConfig:
    """Tests for EnvironmentConfig."""

    def test_create_environment_config(self) -> None:
        """Test creating an environment configuration."""
        config = EnvironmentConfig()
        assert config.io_manager is None
        assert config.log_level == "info"
        assert config.parallelism is None
        assert config.bucket is None

    def test_environment_config_with_values(self) -> None:
        """Test creating environment config with values."""
        config = EnvironmentConfig(
            io_manager="s3",
            log_level="debug",
            parallelism=4,
            bucket="my-bucket",
            region="us-west-2",
        )
        assert config.io_manager == "s3"
        assert config.log_level == "debug"
        assert config.parallelism == 4
        assert config.bucket == "my-bucket"
        assert config.region == "us-west-2"

    def test_environment_config_valid_log_levels(self) -> None:
        """Test all valid log levels."""
        valid_levels = ["debug", "info", "warning", "error", "critical"]
        for level in valid_levels:
            config = EnvironmentConfig(log_level=level)
            assert config.log_level == level

    def test_environment_config_additional_config(self) -> None:
        """Test additional config storage."""
        config = EnvironmentConfig(additional_config={"custom_key": "custom_value"})
        assert config.additional_config == {"custom_key": "custom_value"}


class TestSecretSpec:
    """Tests for SecretSpec."""

    def test_create_secret_spec(self) -> None:
        """Test creating a secret specification."""
        spec = SecretSpec(from_=SecretSource.ENV, path="MY_SECRET")
        assert spec.from_ == SecretSource.ENV
        assert spec.path == "MY_SECRET"
        assert spec.required is True
        assert spec.default is None

    def test_secret_spec_optional_fields(self) -> None:
        """Test secret spec with optional fields."""
        spec = SecretSpec(
            from_=SecretSource.VAULT,
            path="secret/my-secret",
            required=False,
            default="default-value",
        )
        assert spec.required is False
        assert spec.default == "default-value"

    def test_secret_source_enum(self) -> None:
        """Test secret source enum values."""
        assert SecretSource.ENV == "env"
        assert SecretSource.VAULT == "vault"
        assert SecretSource.FILE == "file"


class TestConfig:
    """Tests for Config."""

    def test_create_config(self) -> None:
        """Test creating a configuration."""
        project = ProjectConfig(name="test", version="1.0")
        config = Config(project=project)
        assert config.project.name == "test"
        assert config.environments == {}
        assert config.secrets == {}

    def test_config_with_environments(self) -> None:
        """Test config with environments."""
        project = ProjectConfig(name="test", version="1.0")
        env_config = EnvironmentConfig(io_manager="s3")
        config = Config(project=project, environments={"prod": env_config})
        assert "prod" in config.environments
        assert config.environments["prod"].io_manager == "s3"

    def test_config_with_secrets(self) -> None:
        """Test config with secrets."""
        project = ProjectConfig(name="test", version="1.0")
        spec = SecretSpec(from_=SecretSource.ENV, path="API_KEY")
        config = Config(project=project, secrets={"API_KEY": spec})
        assert "API_KEY" in config.secrets
        assert config.secrets["API_KEY"].path == "API_KEY"

    def test_get_environment(self) -> None:
        """Test getting environment configuration."""
        project = ProjectConfig(name="test", version="1.0")
        env_config = EnvironmentConfig(log_level="debug")
        config = Config(project=project, environments={"dev": env_config})
        retrieved = config.get_environment("dev")
        assert retrieved.log_level == "debug"

    def test_get_environment_not_found(self) -> None:
        """Test getting non-existent environment raises error."""
        project = ProjectConfig(name="test", version="1.0")
        config = Config(project=project)
        with pytest.raises(KeyError, match="Environment 'prod' not found"):
            config.get_environment("prod")

    def test_has_environment(self) -> None:
        """Test checking if environment exists."""
        project = ProjectConfig(name="test", version="1.0")
        env_config = EnvironmentConfig()
        config = Config(project=project, environments={"dev": env_config})
        assert config.has_environment("dev") is True
        assert config.has_environment("prod") is False

    def test_get_secret(self) -> None:
        """Test getting secret specification."""
        project = ProjectConfig(name="test", version="1.0")
        spec = SecretSpec(from_=SecretSource.ENV, path="KEY")
        config = Config(project=project, secrets={"KEY": spec})
        retrieved = config.get_secret("KEY")
        assert retrieved.path == "KEY"

    def test_get_secret_not_found(self) -> None:
        """Test getting non-existent secret raises error."""
        project = ProjectConfig(name="test", version="1.0")
        config = Config(project=project)
        with pytest.raises(KeyError, match="Secret 'KEY' not found"):
            config.get_secret("KEY")

    def test_has_secret(self) -> None:
        """Test checking if secret exists."""
        project = ProjectConfig(name="test", version="1.0")
        spec = SecretSpec(from_=SecretSource.ENV, path="KEY")
        config = Config(project=project, secrets={"KEY": spec})
        assert config.has_secret("KEY") is True
        assert config.has_secret("OTHER") is False

    def test_config_with_cli_overrides(self) -> None:
        """Test config with CLI overrides."""
        project = ProjectConfig(name="test", version="1.0")
        config = Config(project=project, cli_overrides={"log_level": "debug"})
        assert config.cli_overrides == {"log_level": "debug"}
