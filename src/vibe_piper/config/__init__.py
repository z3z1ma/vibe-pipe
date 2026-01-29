"""Configuration management for Vibe Piper.

This module provides environment-specific configuration management with secrets support.

Configuration file formats supported:
- TOML (.toml) - Native Python 3.11+ support
- YAML (.yaml, .yml) - Requires PyYAML
- JSON (.json) - Native Python support

The loader searches for configuration files in order of preference:
1. vibepiper.toml
2. vibepiper.yaml
3. vibepiper.yml
4. vibepiper.json

Configuration inheritance:
Environments can inherit from other environments using the 'inherits' field:

```toml
[environments.base]
io_manager = "s3"
bucket = "base-bucket"

[environments.prod]
inherits = "base"
log_level = "warning"
```

Runtime configuration overrides:
CLI overrides can be applied to environment configurations:

```python
from vibe_piper.config import load_config

config = load_config("vibepiper.toml", cli_overrides={"log_level": "debug"})
env = config.get_environment("prod", apply_overrides=True)
```

Example usage:
    ```python
    from vibe_piper.config import load_config, load_secrets

    # Load configuration for specific environment
    config = load_config("vibepiper.toml", environment="prod")

    # Load secrets
    secrets = load_secrets(config)

    # Access configuration with runtime overrides
    env_config = config.get_environment("prod", apply_overrides=True)
    print(f"IO Manager: {env_config.io_manager}")
    ```
"""

from vibe_piper.config.loader import (
    ConfigLoadError,
    find_config_file,
    load_config,
    load_config_from_environment,
)
from vibe_piper.config.schema import (
    Config,
    EnvironmentConfig,
    ProjectConfig,
    SecretSource,
    SecretSpec,
)
from vibe_piper.config.secrets import (
    SecretNotFoundError,
    get_secret,
    interpolate_secrets,
    load_secrets,
    mask_secrets,
)
from vibe_piper.config.validation import (
    ConfigValidationError,
    validate_config,
    validate_environment_override,
)

__all__ = [
    # Loader
    "load_config",
    "load_config_from_environment",
    "find_config_file",
    "ConfigLoadError",
    # Schema
    "Config",
    "ProjectConfig",
    "EnvironmentConfig",
    "SecretSpec",
    "SecretSource",
    # Secrets
    "load_secrets",
    "get_secret",
    "interpolate_secrets",
    "mask_secrets",
    "SecretNotFoundError",
    # Validation
    "validate_config",
    "validate_environment_override",
    "ConfigValidationError",
]
