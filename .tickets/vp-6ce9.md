---
"id": "vp-6ce9"
"status": "open"
"deps": []
"links": []
"created": "2026-01-28T01:26:59Z"
"type": "feature"
"priority": 1
"assignee": "z3z1ma"
"tags":
- "phase2"
- "config"
- "tooling"
"external": {}
---
# Configuration Management

Add environment-specific configuration management with secrets support.

## Tasks
1. Define config schema (TOML format)
2. Support multiple environments (dev, staging, prod)
3. Add secrets management (env vars, vault)
4. Add parameter overrides
5. Add config validation
6. Add config merging (base + environment)
7. Add CLI config override

## Example Usage
```toml
# vibepiper.toml
[project]
name = "my-pipeline"
version = "0.1.0"

[environments.dev]
io_manager = "memory"
log_level = "debug"

[environments.prod]
io_manager = "s3"
bucket = "my-bucket"
region = "us-west-2"
log_level = "info"

[secrets]
AWS_SECRET_ACCESS_KEY = { from = "env" }
API_KEY = { from = "vault", path = "secret/api-key" }
```

## Dependencies
- vp-206 (CLI Framework)

## Technical Notes
- Use TOML for config files
- Support environment variable expansion
- Never log secrets
- Validate config on load
- Provide clear error messages for invalid config

## Acceptance Criteria

Config file format defined (TOML)
Multiple environments supported (dev, staging, prod)
Secrets loaded from env vars
Config validated on load with clear errors
CLI can override config values
Config merging (base + environment) working
Documentation and example configs
Tests for config loading and validation
Test coverage > 85%
