# Configuration Management

Vibe Piper uses TOML-based configuration files to manage environment-specific settings and secrets.

## Quick Start

1. Create a `vibepiper.toml` file in your project root:

```toml
[project]
name = "my-pipeline"
version = "0.1.0"

[environments.dev]
io_manager = "memory"
log_level = "debug"
```

2. Load configuration in your code:

```python
from vibe_piper.config import load_config

config = load_config("vibepiper.toml", environment="dev")
env_config = config.get_environment("dev")
print(f"IO Manager: {env_config.io_manager}")
```

## Configuration File Structure

### Project Section

```toml
[project]
name = "my-pipeline"      # Required: Project name
version = "0.1.0"         # Required: Project version
description = "My pipeline"  # Optional: Project description
```

### Environments Section

Define multiple environments (dev, staging, prod):

```toml
[environments.dev]
io_manager = "memory"           # IO manager type (memory, file, s3, gcs, azure)
log_level = "debug"             # Logging level (debug, info, warning, error, critical)
parallelism = 2                 # Maximum parallel execution
bucket = "my-bucket"            # Required for s3/gcs/azure IO managers
region = "us-west-2"            # Cloud region
endpoint = "http://localhost"    # Custom endpoint URL
credentials_path = "/path/to/creds"  # Path to credentials file
```

### Secrets Section

Define secrets from various sources:

```toml
[secrets]
# Environment variable
DB_PASSWORD = { from = "env" }

# Optional with default
FEATURE_FLAG = { from = "env", required = false, default = "false" }

# File-based secret
API_KEY = { from = "file", path = "/run/secrets/api_key" }

# Vault secret (placeholder for future use)
# VAULT_SECRET = { from = "vault", path = "secret/my-secret" }
```

## Loading Configuration

### Basic Loading

```python
from vibe_piper.config import load_config

config = load_config("vibepiper.toml")
```

### Load with Environment

```python
config = load_config("vibepiper.toml", environment="prod")
env_config = config.get_environment("prod")
```

### Load with CLI Overrides

```python
config = load_config(
    "vibepiper.toml",
    environment="dev",
    cli_overrides={"log_level": "debug"}
)
```

### Find Config File Automatically

```python
from vibe_piper.config import load_config_from_environment

config = load_config_from_environment("prod")
```

## Loading Secrets

```python
from vibe_piper.config import load_config, load_secrets

config = load_config("vibepiper.toml", environment="prod")
secrets = load_secrets(config)

# Access secrets
db_password = secrets["DB_PASSWORD"]
```

### Secret Interpolation

Use `${secret:KEY}` syntax to interpolate secrets in configuration values:

```python
from vibe_piper.config import interpolate_secrets

connection_string = "postgresql://user:${secret:DB_PASSWORD}@localhost/db"
result = interpolate_secrets(connection_string, secrets)
```

### Masking Secrets

Mask secrets in logs or output:

```python
from vibe_piper.config import mask_secrets

data = {"connection": "postgresql://user:secret123@localhost/db"}
masked = mask_secrets(config, data, secrets)
# Output: {"connection": "postgresql://user:***@localhost/db"}
```

## Configuration Validation

Configuration is validated on load with clear error messages:

- Invalid log levels
- Missing required fields (e.g., bucket for S3)
- Invalid secret configurations
- Missing environments

### Custom Validation

Validate configuration overrides:

```python
from vibe_piper.config import validate_environment_override

validate_environment_override(
    config,
    environment="prod",
    override_key="log_level",
    override_value="debug"
)
```

## Environment Variables

Set environment variables before loading secrets:

```bash
export DB_PASSWORD="my-secret-password"
export API_KEY="my-api-key"
python your_pipeline.py
```

## Examples

See the `examples/` directory for complete configuration examples:

- `vibepiper.minimal.toml` - Minimal configuration
- `vibepiper.example.toml` - Full-featured example

## Best Practices

1. **Never commit secrets** - Use environment variables or secret management systems
2. **Use different environments** - dev, staging, prod with appropriate settings
3. **Validate early** - Configuration is validated on load
4. **Use interpolation** - Reference secrets with `${secret:KEY}` syntax
5. **Mask secrets** - Use `mask_secrets()` for logging to avoid leaking secrets

## Troubleshooting

### Configuration Not Found

```
ConfigLoadError: Configuration file not found: vibepiper.toml
```

Ensure `vibepiper.toml` exists in your project root or current directory.

### Environment Not Found

```
ConfigValidationError: Environment 'prod' not found. Available environments: ['dev']
```

Check that the environment is defined in your configuration file.

### Missing Bucket for Cloud Storage

```
ConfigValidationError: Bucket is required for io_manager 's3'
```

Add `bucket = "your-bucket-name"` to your environment configuration.

### Secret Not Found

```
SecretNotFoundError: Secret 'DB_PASSWORD' not found from env at 'DB_PASSWORD'
```

Ensure the environment variable is set before loading the configuration.
