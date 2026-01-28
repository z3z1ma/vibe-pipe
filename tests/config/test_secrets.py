"""Tests for secrets management."""

import os
import tempfile
from pathlib import Path

import pytest

from vibe_piper.config.schema import Config, ProjectConfig, SecretSource, SecretSpec
from vibe_piper.config.secrets import (
    SecretNotFoundError,
    get_secret,
    interpolate_secrets,
    load_secrets,
    mask_secrets,
    validate_secret_spec,
)


class TestLoadSecrets:
    """Tests for load_secrets function."""

    def test_load_env_secret(self) -> None:
        """Test loading secret from environment variable."""
        os.environ["TEST_SECRET"] = "secret_value"

        try:
            project = ProjectConfig(name="test", version="1.0")
            spec = SecretSpec(from_=SecretSource.ENV, path="TEST_SECRET")
            config = Config(project=project, secrets={"SECRET": spec})

            secrets = load_secrets(config)
            assert secrets["SECRET"] == "secret_value"
        finally:
            del os.environ["TEST_SECRET"]

    def test_load_env_secret_not_found(self) -> None:
        """Test loading non-existent env secret raises error."""
        # Make sure the env var doesn't exist
        if "NONEXISTENT_SECRET" in os.environ:
            del os.environ["NONEXISTENT_SECRET"]

        project = ProjectConfig(name="test", version="1.0")
        spec = SecretSpec(from_=SecretSource.ENV, path="NONEXISTENT_SECRET")
        config = Config(project=project, secrets={"SECRET": spec})

        with pytest.raises(SecretNotFoundError, match="Secret 'SECRET' not found"):
            load_secrets(config)

    def test_load_optional_secret_with_default(self) -> None:
        """Test loading optional secret uses default when not found."""
        # Make sure the env var doesn't exist
        if "OPTIONAL_SECRET" in os.environ:
            del os.environ["OPTIONAL_SECRET"]

        project = ProjectConfig(name="test", version="1.0")
        spec = SecretSpec(
            from_=SecretSource.ENV,
            path="OPTIONAL_SECRET",
            required=False,
            default="default_value",
        )
        config = Config(project=project, secrets={"SECRET": spec})

        secrets = load_secrets(config)
        assert secrets["SECRET"] == "default_value"

    def test_load_file_secret(self) -> None:
        """Test loading secret from file."""
        with tempfile.NamedTemporaryFile(mode="w", delete=False) as f:
            f.write("file_secret_value\n")
            f.flush()
            path = f.name

        try:
            project = ProjectConfig(name="test", version="1.0")
            spec = SecretSpec(from_=SecretSource.FILE, path=path)
            config = Config(project=project, secrets={"SECRET": spec})

            secrets = load_secrets(config)
            assert secrets["SECRET"] == "file_secret_value"
        finally:
            Path(path).unlink()

    def test_load_file_secret_not_found(self) -> None:
        """Test loading from non-existent file raises error."""
        project = ProjectConfig(name="test", version="1.0")
        spec = SecretSpec(from_=SecretSource.FILE, path="/nonexistent/file")
        config = Config(project=project, secrets={"SECRET": spec})

        with pytest.raises(SecretNotFoundError, match="Secret 'SECRET' not found"):
            load_secrets(config)

    def test_load_vault_secret_not_implemented(self) -> None:
        """Test loading vault secret raises NotImplementedError."""
        project = ProjectConfig(name="test", version="1.0")
        spec = SecretSpec(from_=SecretSource.VAULT, path="secret/test")
        config = Config(project=project, secrets={"SECRET": spec})

        with pytest.raises(
            NotImplementedError, match="Vault integration not yet implemented"
        ):
            load_secrets(config)

    def test_load_multiple_secrets(self) -> None:
        """Test loading multiple secrets."""
        os.environ["SECRET1"] = "value1"
        os.environ["SECRET2"] = "value2"

        try:
            project = ProjectConfig(name="test", version="1.0")
            spec1 = SecretSpec(from_=SecretSource.ENV, path="SECRET1")
            spec2 = SecretSpec(from_=SecretSource.ENV, path="SECRET2")
            config = Config(project=project, secrets={"KEY1": spec1, "KEY2": spec2})

            secrets = load_secrets(config)
            assert secrets["KEY1"] == "value1"
            assert secrets["KEY2"] == "value2"
        finally:
            del os.environ["SECRET1"]
            del os.environ["SECRET2"]


class TestGetSecret:
    """Tests for get_secret function."""

    def test_get_secret(self) -> None:
        """Test getting a single secret."""
        os.environ["MY_SECRET"] = "my_value"

        try:
            project = ProjectConfig(name="test", version="1.0")
            spec = SecretSpec(from_=SecretSource.ENV, path="MY_SECRET")
            config = Config(project=project, secrets={"MY_SECRET": spec})

            value = get_secret(config, "MY_SECRET")
            assert value == "my_value"
        finally:
            del os.environ["MY_SECRET"]

    def test_get_secret_not_defined(self) -> None:
        """Test getting undefined secret raises KeyError."""
        project = ProjectConfig(name="test", version="1.0")
        config = Config(project=project)

        with pytest.raises(KeyError, match="Secret 'UNDEFINED' not defined"):
            get_secret(config, "UNDEFINED")

    def test_get_secret_not_found_with_default(self) -> None:
        """Test getting secret with default when not found."""
        if "OPTIONAL_SECRET" in os.environ:
            del os.environ["OPTIONAL_SECRET"]

        project = ProjectConfig(name="test", version="1.0")
        spec = SecretSpec(
            from_=SecretSource.ENV,
            path="OPTIONAL_SECRET",
            required=False,
            default="default",
        )
        config = Config(project=project, secrets={"OPTIONAL_SECRET": spec})

        value = get_secret(config, "OPTIONAL_SECRET")
        assert value == "default"


class TestInterpolateSecrets:
    """Tests for interpolate_secrets function."""

    def test_interpolate_single_secret(self) -> None:
        """Test interpolating a single secret."""
        secrets = {"API_KEY": "abc123"}
        value = "http://api.example.com?key=${secret:API_KEY}"
        result = interpolate_secrets(value, secrets)
        assert result == "http://api.example.com?key=abc123"

    def test_interpolate_multiple_secrets(self) -> None:
        """Test interpolating multiple secrets."""
        secrets = {"USER": "admin", "PASSWORD": "secret"}
        value = "mysql://${secret:USER}:${secret:PASSWORD}@localhost/db"
        result = interpolate_secrets(value, secrets)
        assert result == "mysql://admin:secret@localhost/db"

    def test_interpolate_missing_secret(self) -> None:
        """Test interpolating with missing secret leaves placeholder."""
        secrets = {"USER": "admin"}
        value = "${secret:USER}:${secret:PASSWORD}"
        result = interpolate_secrets(value, secrets)
        assert result == "admin:${secret:PASSWORD}"

    def test_interpolate_no_secrets(self) -> None:
        """Test interpolating string without secrets."""
        secrets = {"API_KEY": "abc123"}
        value = "http://api.example.com/endpoint"
        result = interpolate_secrets(value, secrets)
        assert result == "http://api.example.com/endpoint"

    def test_interpolate_empty_secrets(self) -> None:
        """Test interpolating with empty secrets dict."""
        secrets = {}
        value = "http://api.example.com?key=${secret:API_KEY}"
        result = interpolate_secrets(value, secrets)
        assert result == "http://api.example.com?key=${secret:API_KEY}"


class TestMaskSecrets:
    """Tests for mask_secrets function."""

    def test_mask_string_secret(self) -> None:
        """Test masking secret in string."""
        project = ProjectConfig(name="test", version="1.0")
        config = Config(project=project)
        secrets = {"PASSWORD": "secret123"}

        data = {"password": "secret123"}
        result = mask_secrets(config, data, secrets)
        assert result["password"] == "***"

    def test_mask_secret_in_url(self) -> None:
        """Test masking secret in URL."""
        project = ProjectConfig(name="test", version="1.0")
        config = Config(project=project)
        secrets = {"PASSWORD": "secret123"}

        data = {"connection_string": "postgresql://user:secret123@localhost/db"}
        result = mask_secrets(config, data, secrets)
        assert result["connection_string"] == "postgresql://user:***@localhost/db"

    def test_mask_multiple_secrets(self) -> None:
        """Test masking multiple secrets."""
        project = ProjectConfig(name="test", version="1.0")
        config = Config(project=project)
        secrets = {"KEY1": "value1", "KEY2": "value2"}

        data = {"key1": "value1", "key2": "value2", "other": "other"}
        result = mask_secrets(config, data, secrets)
        assert result["key1"] == "***"
        assert result["key2"] == "***"
        assert result["other"] == "other"

    def test_mask_nested_dict(self) -> None:
        """Test masking secrets in nested dictionary."""
        project = ProjectConfig(name="test", version="1.0")
        config = Config(project=project)
        secrets = {"PASSWORD": "secret123"}

        data = {"database": {"host": "localhost", "password": "secret123"}}
        result = mask_secrets(config, data, secrets)
        assert result["database"]["password"] == "***"
        assert result["database"]["host"] == "localhost"

    def test_mask_in_list(self) -> None:
        """Test masking secrets in list."""
        project = ProjectConfig(name="test", version="1.0")
        config = Config(project=project)
        secrets = {"PASSWORD": "secret123"}

        data = {"items": ["normal", "secret123", "other"]}
        result = mask_secrets(config, data, secrets)
        assert result["items"] == ["normal", "***", "other"]

    def test_mask_empty_secrets(self) -> None:
        """Test masking with empty secrets dict."""
        project = ProjectConfig(name="test", version="1.0")
        config = Config(project=project)
        secrets = {}

        data = {"password": "secret123"}
        result = mask_secrets(config, data, secrets)
        assert result["password"] == "secret123"


class TestValidateSecretSpec:
    """Tests for validate_secret_spec function."""

    def test_validate_valid_secret_spec(self) -> None:
        """Test validating a valid secret spec."""
        spec = SecretSpec(from_=SecretSource.ENV, path="MY_SECRET", required=True)
        warnings = validate_secret_spec(spec)
        assert warnings == []

    def test_validate_secret_required_no_path(self) -> None:
        """Test validating required secret without path."""
        spec = SecretSpec(from_=SecretSource.ENV, path="", required=True)
        warnings = validate_secret_spec(spec)
        assert len(warnings) == 1
        assert "must have a path" in warnings[0]

    def test_validate_secret_with_default_and_required(self) -> None:
        """Test validating secret with both default and required."""
        spec = SecretSpec(
            from_=SecretSource.ENV, path="MY_SECRET", required=True, default="default"
        )
        warnings = validate_secret_spec(spec)
        assert len(warnings) == 1
        assert "cannot have both required=true and a default" in warnings[0]

    def test_validate_vault_path_format_warning(self) -> None:
        """Test validating vault path format generates warning."""
        spec = SecretSpec(from_=SecretSource.VAULT, path="custom/path", required=False)
        warnings = validate_secret_spec(spec)
        assert len(warnings) == 1
        assert "should start with 'secret/'" in warnings[0]

    def test_validate_vault_path_correct_format(self) -> None:
        """Test validating vault path with correct format."""
        spec = SecretSpec(
            from_=SecretSource.VAULT, path="secret/my-secret", required=True
        )
        warnings = validate_secret_spec(spec)
        assert warnings == []
