"""Tests for multi-format configuration loading and inheritance."""

import tempfile
from pathlib import Path

import pytest

from vibe_piper.config.loader import ConfigLoadError, load_config


class TestMultiFormatSupport:
    """Tests for loading configurations in different formats."""

    def test_load_yaml_config(self) -> None:
        """Test loading configuration from YAML file."""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
            f.write(
                """
project:
  name: "test-yaml"
  version: "1.0.0"

environments:
  dev:
    io_manager: memory
    log_level: debug

  prod:
    io_manager: s3
    bucket: my-bucket
    region: us-west-2

secrets:
  API_KEY:
    from: env
"""
            )
            f.flush()
            path = Path(f.name)

        try:
            config = load_config(path)
            assert config.project.name == "test-yaml"
            assert config.project.version == "1.0.0"
            assert config.get_environment("dev").io_manager == "memory"
            assert config.get_environment("prod").io_manager == "s3"
            assert config.get_environment("prod").bucket == "my-bucket"
        finally:
            path.unlink()

    def test_load_yml_config(self) -> None:
        """Test loading configuration from .yml file."""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".yml", delete=False) as f:
            f.write(
                """
project:
  name: "test-yml"
  version: "1.0.0"
"""
            )
            f.flush()
            path = Path(f.name)

        try:
            config = load_config(path)
            assert config.project.name == "test-yml"
        finally:
            path.unlink()

    def test_load_json_config(self) -> None:
        """Test loading configuration from JSON file."""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
            f.write(
                """
{
  "project": {
    "name": "test-json",
    "version": "1.0.0",
    "description": "JSON config test"
  },
  "environments": {
    "dev": {
      "io_manager": "memory",
      "log_level": "debug",
      "parallelism": 4
    },
    "prod": {
      "io_manager": "s3",
      "bucket": "prod-bucket",
      "region": "eu-west-1"
    }
  }
}
"""
            )
            f.flush()
            path = Path(f.name)

        try:
            config = load_config(path)
            assert config.project.name == "test-json"
            assert config.project.description == "JSON config test"
            assert config.get_environment("dev").parallelism == 4
            assert config.get_environment("prod").bucket == "prod-bucket"
            assert config.get_environment("prod").region == "eu-west-1"
        finally:
            path.unlink()

    def test_invalid_yaml_syntax(self) -> None:
        """Test loading invalid YAML raises error."""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
            f.write("invalid: yaml: content:\n  - item1\n  item2")
            f.flush()
            path = Path(f.name)

        try:
            with pytest.raises(ConfigLoadError, match="Invalid YAML syntax"):
                load_config(path)
        finally:
            path.unlink()

    def test_invalid_json_syntax(self) -> None:
        """Test loading invalid JSON raises error."""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
            f.write('{"invalid": json content}')
            f.flush()
            path = Path(f.name)

        try:
            with pytest.raises(ConfigLoadError, match="Invalid JSON syntax"):
                load_config(path)
        finally:
            path.unlink()

    def test_unsupported_format(self) -> None:
        """Test loading unsupported file format raises error."""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".xml", delete=False) as f:
            f.write("<config></config>")
            f.flush()
            path = Path(f.name)

        try:
            with pytest.raises(ConfigLoadError, match="Unsupported configuration file format"):
                load_config(path)
        finally:
            path.unlink()


class TestConfigInheritance:
    """Tests for configuration inheritance."""

    def test_simple_inheritance(self) -> None:
        """Test simple environment inheritance."""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".toml", delete=False) as f:
            f.write(
                """
[project]
name = "test"
version = "1.0"

[environments.base]
io_manager = "s3"
region = "us-east-1"
bucket = "base-bucket"
log_level = "info"

[environments.dev]
inherits = "base"
bucket = "dev-bucket"
log_level = "debug"

[environments.prod]
inherits = "base"
"""
            )
            f.flush()
            path = Path(f.name)

        try:
            config = load_config(path)

            # Dev inherits from base but overrides bucket and log_level
            dev_env = config.get_environment("dev")
            assert dev_env.io_manager == "s3"  # inherited
            assert dev_env.region == "us-east-1"  # inherited
            assert dev_env.bucket == "dev-bucket"  # overridden
            assert dev_env.log_level == "debug"  # overridden

            # Prod inherits all from base
            prod_env = config.get_environment("prod")
            assert prod_env.io_manager == "s3"  # inherited
            assert prod_env.region == "us-east-1"  # inherited
            assert prod_env.bucket == "base-bucket"  # inherited
            assert prod_env.log_level == "info"  # inherited
        finally:
            path.unlink()

    def test_inheritance_with_additional_config(self) -> None:
        """Test inheritance merges additional config."""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".toml", delete=False) as f:
            f.write(
                """
[project]
name = "test"
version = "1.0"

[environments.base]
io_manager = "memory"
custom_key = "base_value"
another_key = "another_base"

[environments.dev]
inherits = "base"
custom_key = "dev_value"
new_key = "dev_new"
"""
            )
            f.flush()
            path = Path(f.name)

        try:
            config = load_config(path)
            dev_env = config.get_environment("dev")

            # Child overrides parent values
            assert dev_env.additional_config["custom_key"] == "dev_value"
            # Child keeps parent values not overridden
            assert dev_env.additional_config["another_key"] == "another_base"
            # Child adds new values
            assert dev_env.additional_config["new_key"] == "dev_new"
        finally:
            path.unlink()

    def test_inheritance_yaml(self) -> None:
        """Test inheritance with YAML format."""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
            f.write(
                """
project:
  name: "test"
  version: "1.0"

environments:
  base:
    io_manager: s3
    bucket: "base-bucket"
    region: us-west-2

  dev:
    inherits: base
    log_level: debug

  prod:
    inherits: base
    log_level: warning
"""
            )
            f.flush()
            path = Path(f.name)

        try:
            config = load_config(path)
            dev_env = config.get_environment("dev")
            prod_env = config.get_environment("prod")

            assert dev_env.io_manager == "s3"  # inherited
            assert dev_env.bucket == "base-bucket"  # inherited
            assert dev_env.region == "us-west-2"  # inherited
            assert dev_env.log_level == "debug"  # overridden

            assert prod_env.io_manager == "s3"  # inherited
            assert prod_env.bucket == "base-bucket"  # inherited
            assert prod_env.region == "us-west-2"  # inherited
            assert prod_env.log_level == "warning"  # overridden
        finally:
            path.unlink()

    def test_inheritance_json(self) -> None:
        """Test inheritance with JSON format."""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
            f.write(
                """
{
  "project": {
    "name": "test",
    "version": "1.0"
  },
  "environments": {
    "base": {
      "io_manager": "memory",
      "parallelism": 8
    },
    "dev": {
      "inherits": "base",
      "parallelism": 4
    }
  }
}
"""
            )
            f.flush()
            path = Path(f.name)

        try:
            config = load_config(path)
            dev_env = config.get_environment("dev")

            assert dev_env.io_manager == "memory"  # inherited
            assert dev_env.parallelism == 4  # overridden
        finally:
            path.unlink()

    def test_inherit_from_nonexistent_environment(self) -> None:
        """Test inheriting from non-existent environment raises error."""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".toml", delete=False) as f:
            f.write(
                """
[project]
name = "test"
version = "1.0"

[environments.dev]
inherits = "nonexistent"
"""
            )
            f.flush()
            path = Path(f.name)

        try:
            with pytest.raises(ConfigLoadError, match="inherits from 'nonexistent'"):
                load_config(path)
        finally:
            path.unlink()

    def test_no_inheritance(self) -> None:
        """Test that environments without inheritance work normally."""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".toml", delete=False) as f:
            f.write(
                """
[project]
name = "test"
version = "1.0"

[environments.dev]
io_manager = "memory"

[environments.prod]
io_manager = "s3"
bucket = "prod-bucket"
"""
            )
            f.flush()
            path = Path(f.name)

        try:
            config = load_config(path)
            assert config.get_environment("dev").io_manager == "memory"
            assert config.get_environment("prod").io_manager == "s3"
            assert config.get_environment("prod").bucket == "prod-bucket"
        finally:
            path.unlink()
