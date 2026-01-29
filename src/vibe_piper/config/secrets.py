"""Secrets management for Vibe Piper."""

import os
from pathlib import Path
from typing import Any

from vibe_piper.config.schema import Config, SecretSource, SecretSpec


class SecretNotFoundError(Exception):
    """Secret not found error."""

    def __init__(self, key: str, source: SecretSource, path: str) -> None:
        """Initialize secret not found error.

        Args:
            key: Secret key
            source: Secret source type
            path: Path to secret
        """
        self.key = key
        self.source = source
        self.path = path
        super().__init__(f"Secret '{key}' not found from {source.value} at '{path}'")


def load_secrets(config: Config, _environment: str | None = None) -> dict[str, str]:
    """Load all secrets defined in configuration.

    Args:
        config: Configuration containing secret definitions
        _environment: Optional environment name (reserved for future use)

    Returns:
        Dictionary of loaded secrets (key -> value)

    Raises:
        SecretNotFoundError: If a required secret cannot be loaded
        Exception: If secret source is not supported
    """
    secrets: dict[str, str] = {}

    for key, spec in config.secrets.items():
        try:
            value = _load_secret(key, spec)
            secrets[key] = value
        except SecretNotFoundError:
            if spec.required:
                raise
            # Use default value for non-required secrets
            if spec.default is not None:
                secrets[key] = spec.default

    return secrets


def _load_secret(key: str, spec: SecretSpec) -> str:
    """Load a single secret from its source.

    Args:
        key: Secret key
        spec: Secret specification

    Returns:
        Secret value

    Raises:
        SecretNotFoundError: If secret cannot be loaded
        Exception: If secret source is not supported
    """
    if spec.from_ == SecretSource.ENV:
        return _load_from_env(key, spec)
    elif spec.from_ == SecretSource.FILE:
        return _load_from_file(key, spec)
    else:  # SecretSource.VAULT
        return _load_from_vault(key, spec)


def _load_from_env(key: str, spec: SecretSpec) -> str:
    """Load secret from environment variable.

    Args:
        key: Secret key
        spec: Secret specification

    Returns:
        Secret value

    Raises:
        SecretNotFoundError: If environment variable is not set
    """
    # The path is the environment variable name
    env_var = spec.path
    value = os.environ.get(env_var)

    if value is None:
        raise SecretNotFoundError(key, spec.from_, env_var)

    return value


def _load_from_file(key: str, spec: SecretSpec) -> str:
    """Load secret from file.

    Args:
        key: Secret key
        spec: Secret specification

    Returns:
        Secret value

    Raises:
        SecretNotFoundError: If file cannot be read
    """
    file_path = Path(spec.path)

    if not file_path.exists():
        raise SecretNotFoundError(key, spec.from_, str(file_path))

    try:
        # Read file and strip whitespace
        value = file_path.read_text(encoding="utf-8").strip()
    except OSError as e:
        raise SecretNotFoundError(key, spec.from_, str(file_path)) from e

    return value


def _load_from_vault(key: str, spec: SecretSpec) -> str:
    """Load secret from HashiCorp Vault.

    Note: This is a placeholder implementation. Real Vault integration
    requires the hvac library and proper authentication.

    Args:
        key: Secret key
        spec: Secret specification

    Returns:
        Secret value

    Raises:
        NotImplementedError: Vault integration not yet implemented
    """
    # TODO: Implement proper Vault integration
    # This would require:
    # 1. hvac library
    # 2. Vault authentication (token, approle, etc.)
    # 3. Reading from the specified path
    msg = (
        "Vault integration not yet implemented. "
        "Please use environment variables or file-based secrets for now."
    )
    raise NotImplementedError(msg)


def get_secret(config: Config, key: str, _environment: str | None = None) -> str:
    """Load a single secret by key.

    Args:
        config: Configuration containing secret definitions
        key: Secret key
        _environment: Optional environment name (reserved for future use)

    Returns:
        Secret value

    Raises:
        KeyError: If secret is not defined in configuration
        SecretNotFoundError: If secret cannot be loaded
    """
    if key not in config.secrets:
        available = list(config.secrets.keys())
        msg = f"Secret '{key}' not defined in configuration. Available secrets: {available}"
        raise KeyError(msg)

    spec = config.secrets[key]
    try:
        return _load_secret(key, spec)
    except SecretNotFoundError:
        if spec.default is not None:
            return spec.default
        raise


def interpolate_secrets(value: str, secrets: dict[str, str]) -> str:
    """Interpolate secret references in a string.

    Supports ${secret:KEY} syntax.

    Args:
        value: String potentially containing secret references
        secrets: Dictionary of loaded secrets

    Returns:
        String with secrets interpolated

    Examples:
        >>> secrets = {"API_KEY": "abc123"}
        >>> interpolate_secrets("http://api.example.com?key=${secret:API_KEY}", secrets)
        'http://api.example.com?key=abc123'
    """
    import re

    pattern = r"\$\{secret:([a-zA-Z_][a-zA-Z0-9_]*)\}"

    def replacer(match: re.Match[str]) -> str:
        key = match.group(1)
        if key not in secrets:
            # Return the original string if secret not found
            return match.group(0)
        return secrets[key]

    return re.sub(pattern, replacer, value)


def mask_secrets(_config: Config, data: dict[str, Any], secrets: dict[str, str]) -> dict[str, Any]:
    """Mask secret values in data structure for logging.

    Args:
        _config: Configuration (reserved for future use)
        data: Data structure that may contain secrets
        secrets: Dictionary of loaded secrets

    Returns:
        Data structure with secrets masked
    """
    import copy

    result = copy.deepcopy(data)

    def mask_value(value: Any) -> Any:
        """Recursively mask secret values in a data structure."""
        if isinstance(value, str):
            for secret_value in secrets.values():
                if secret_value and secret_value in value:
                    # Mask the secret value
                    value = value.replace(secret_value, "***")
            return value
        if isinstance(value, dict):
            return {k: mask_value(v) for k, v in value.items()}
        if isinstance(value, list):
            return [mask_value(item) for item in value]
        return value

    return mask_value(result)  # type: ignore[no-any-return]


def validate_secret_spec(spec: SecretSpec) -> list[str]:
    """Validate a secret specification.

    Args:
        spec: Secret specification to validate

    Returns:
        List of validation warnings (empty if valid)
    """
    warnings: list[str] = []

    # Check that required secrets have a path
    if spec.required and not spec.path:
        warnings.append("Required secret must have a path")

    # Check that defaults are only for non-required secrets
    if spec.default is not None and spec.required:
        warnings.append("Secret cannot have both required=true and a default value")

    # Check vault path format
    if spec.from_ == SecretSource.VAULT and spec.path and not spec.path.startswith("secret/"):
        warnings.append(f"Vault path should start with 'secret/' (got: {spec.path})")

    return warnings
