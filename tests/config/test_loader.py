"""Tests for configuration loader."""

import tempfile
from pathlib import Path

import pytest

from vibe_piper.config.loader import (
    ConfigLoadError,
    find_config_file,
    load_config,
    load_config_from_environment,
)


class TestLoadConfig:
    """Tests for load_config function."""

    def test_load_minimal_config(self) -> None:
        """Test loading minimal valid configuration."""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".toml", delete=False) as f:
            f.write('[project]\nname = "test"\nversion = "1.0"\n')
            f.flush()
            path = Path(f.name)

        try:
            config = load_config(path)
            assert config.project.name == "test"
            assert config.project.version == "1.0"
        finally:
            path.unlink()

    def test_load_full_config(self) -> None:
        """Test loading full configuration with all sections."""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".toml", delete=False) as f:
            f.write(
                """
[project]
name = "my-pipeline"
version = "0.1.0"
description = "Test pipeline"

[environments.dev]
io_manager = "memory"
log_level = "debug"
parallelism = 2

[environments.prod]
io_manager = "s3"
bucket = "my-bucket"
region = "us-west-2"
log_level = "info"

[secrets]
API_KEY = { from = "env" }
DB_PASSWORD = { from = "file", path = "/secrets/db_password" }
"""
            )
            f.flush()
            path = Path(f.name)

        try:
            config = load_config(path)
            assert config.project.name == "my-pipeline"
            assert config.project.description == "Test pipeline"
            assert len(config.environments) == 2
            assert config.get_environment("dev").io_manager == "memory"
            assert config.get_environment("dev").log_level == "debug"
            assert config.get_environment("dev").parallelism == 2
            assert config.get_environment("prod").io_manager == "s3"
            assert config.get_environment("prod").bucket == "my-bucket"
            assert config.get_environment("prod").region == "us-west-2"
            assert config.get_secret("API_KEY").from_.value == "env"
            assert config.get_secret("DB_PASSWORD").from_.value == "file"
        finally:
            path.unlink()

    def test_load_config_file_not_found(self) -> None:
        """Test loading non-existent file raises error."""
        with pytest.raises(ConfigLoadError, match="Configuration file not found"):
            load_config("/nonexistent/path.toml")

    def test_load_config_invalid_toml(self) -> None:
        """Test loading invalid TOML raises error."""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".toml", delete=False) as f:
            f.write("[invalid toml\n")
            f.flush()
            path = Path(f.name)

        try:
            with pytest.raises(ConfigLoadError, match="Invalid TOML syntax"):
                load_config(path)
        finally:
            path.unlink()

    def test_load_config_with_additional_fields(self) -> None:
        """Test loading config with additional environment fields."""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".toml", delete=False) as f:
            f.write(
                """
[project]
name = "test"
version = "1.0"

[environments.dev]
log_level = "debug"
custom_field = "custom_value"
numeric_field = 42
"""
            )
            f.flush()
            path = Path(f.name)

        try:
            config = load_config(path)
            env = config.get_environment("dev")
            assert env.additional_config == {
                "custom_field": "custom_value",
                "numeric_field": 42,
            }
        finally:
            path.unlink()

    def test_load_config_with_cli_overrides(self) -> None:
        """Test loading config with CLI overrides."""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".toml", delete=False) as f:
            f.write('[project]\nname = "test"\nversion = "1.0"\n')
            f.flush()
            path = Path(f.name)

        try:
            config = load_config(path, cli_overrides={"log_level": "debug"})
            assert config.cli_overrides == {"log_level": "debug"}
        finally:
            path.unlink()

    def test_load_config_invalid_secret_source(self) -> None:
        """Test loading config with invalid secret source."""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".toml", delete=False) as f:
            f.write(
                """
[project]
name = "test"
version = "1.0"

[secrets]
KEY = { from = "invalid_source" }
"""
            )
            f.flush()
            path = Path(f.name)

        try:
            with pytest.raises(ConfigLoadError, match="Failed to parse configuration"):
                load_config(path)
        finally:
            path.unlink()

    def test_load_config_secret_without_from(self) -> None:
        """Test loading config with secret missing 'from' field."""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".toml", delete=False) as f:
            f.write(
                """
[project]
name = "test"
version = "1.0"

[secrets]
KEY = { path = "some/path" }
"""
            )
            f.flush()
            path = Path(f.name)

        try:
            with pytest.raises(ConfigLoadError, match="Failed to parse configuration"):
                load_config(path)
        finally:
            path.unlink()

    def test_load_config_with_optional_secrets(self) -> None:
        """Test loading config with optional secrets."""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".toml", delete=False) as f:
            f.write(
                """
[project]
name = "test"
version = "1.0"

[secrets]
OPTIONAL_KEY = { from = "env", required = false, default = "default_value" }
"""
            )
            f.flush()
            path = Path(f.name)

        try:
            config = load_config(path)
            secret = config.get_secret("OPTIONAL_KEY")
            assert secret.required is False
            assert secret.default == "default_value"
        finally:
            path.unlink()


class TestFindConfigFile:
    """Tests for find_config_file function."""

    def test_find_config_in_current_directory(self) -> None:
        """Test finding config file in current directory."""
        with tempfile.TemporaryDirectory() as tmpdir:
            tmppath = Path(tmpdir)
            config_file = tmppath / "vibepiper.toml"
            config_file.write_text('[project]\nname = "test"\nversion = "1.0"\n')

            result = find_config_file(tmppath)
            assert result == config_file

    def test_find_config_in_parent_directory(self) -> None:
        """Test finding config file in parent directory."""
        with tempfile.TemporaryDirectory() as tmpdir:
            tmppath = Path(tmpdir)
            config_file = tmppath / "vibepiper.toml"
            config_file.write_text('[project]\nname = "test"\nversion = "1.0"\n')

            subdir = tmppath / "subdir"
            subdir.mkdir()

            result = find_config_file(subdir)
            assert result == config_file

    def test_find_config_not_found(self) -> None:
        """Test finding config when it doesn't exist."""
        with tempfile.TemporaryDirectory() as tmpdir:
            result = find_config_file(Path(tmpdir))
            assert result is None


class TestLoadConfigFromEnvironment:
    """Tests for load_config_from_environment function."""

    def test_load_config_from_environment_success(self) -> None:
        """Test loading config for specific environment."""
        with tempfile.TemporaryDirectory() as tmpdir:
            tmppath = Path(tmpdir)
            config_file = tmppath / "vibepiper.toml"
            config_file.write_text(
                """
[project]
name = "test"
version = "1.0"

[environments.prod]
io_manager = "s3"
bucket = "my-bucket"
"""
            )

            config = load_config_from_environment("prod", search_path=tmppath)
            assert config.get_environment("prod").io_manager == "s3"

    def test_load_config_from_environment_not_found(self) -> None:
        """Test loading config when file doesn't exist."""
        with (
            tempfile.TemporaryDirectory() as tmpdir,
            pytest.raises(ConfigLoadError, match="No configuration file found"),
        ):
            load_config_from_environment("prod", search_path=Path(tmpdir))

    def test_load_config_from_environment_with_overrides(self) -> None:
        """Test loading config with CLI overrides."""
        with tempfile.TemporaryDirectory() as tmpdir:
            tmppath = Path(tmpdir)
            config_file = tmppath / "vibepiper.toml"
            config_file.write_text(
                """
[project]
name = "test"
version = "1.0"

[environments.dev]
io_manager = "memory"
"""
            )

            config = load_config_from_environment(
                "dev", search_path=tmppath, cli_overrides={"custom": "value"}
            )
            assert config.cli_overrides == {"custom": "value"}
