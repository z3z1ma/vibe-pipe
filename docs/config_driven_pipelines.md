# Configuration-Driven Pipelines

This guide explains how to define and run Vibe Piper pipelines using
configuration files (TOML, YAML, or JSON) without writing code.

## Overview

Configuration-driven pipelines enable you to:

- **Define complete pipelines declaratively** - No Python code required for simple pipelines
- **Version control your pipelines** - Track changes to pipeline definitions in Git
- **Easy deployment** - Deploy pipelines by copying config files
- **Environment-specific configs** - Use environment variables for different environments
- **Non-technical users** - Modify pipelines without coding knowledge

## Configuration File Format

### TOML Format

```toml
[pipeline]
name = "my_pipeline"
version = "1.0.0"
description = "My data pipeline"

[[sources]]
name = "my_source"
type = "api"
endpoint = "/data"

[[sinks]]
name = "my_sink"
type = "database"
connection = "postgres://localhost/mydb"
table = "my_table"
```

### YAML Format

```yaml
pipeline:
  name: my_pipeline
  version: 1.0.0
  description: My data pipeline

sources:
  - name: my_source
    type: api
    endpoint: /data

sinks:
  - name: my_sink
    type: database
    connection: postgres://localhost/mydb
    table: my_table
```

## Configuration Sections

### Pipeline Metadata

```toml
[pipeline]
name = "user_ingestion"         # Required: Unique pipeline name
version = "1.0.0"               # Required: Semantic version
description = "Ingest users"       # Optional: Description
author = "Data Team"             # Optional: Author
created = "2024-01-01"          # Optional: Creation date
```

### Sources

Sources define where data comes from (API, database, file).

```toml
[[sources]]
name = "users_api"                      # Required: Unique name
type = "api"                           # Required: api, database, file
endpoint = "/users"                     # For API sources
base_url = "https://api.example.com/v1"   # For API sources
query = "SELECT * FROM users"            # For database sources
path = "data/*.csv"                     # For file sources
connection = "postgres://..."               # For database sources
table = "users"                          # For database sources
schema = "path/to/schema.py::UserSchema"  # Optional: Schema reference
auth.type = "bearer"                     # For API sources
auth.from_env = "API_KEY"                # For API sources
pagination.type = "offset"                # For API sources
pagination.items_path = "data"             # For API sources
rate_limit.requests = 10                    # Optional: Rate limit
rate_limit.window_seconds = 1               # Optional: Rate limit window
incremental = true                         # Optional: Enable incremental
watermark_column = "updated_at"           # Optional: Watermark column
description = "Fetch users from API"        # Optional: Description
tags = ["users", "api"]                  # Optional: Tags for organization
```

### Sinks

Sinks define where data goes to (database, file, S3).

```toml
[[sinks]]
name = "users_db"                        # Required: Unique name
type = "database"                        # Required: database, file, s3
connection = "postgres://localhost/mydb"     # For database sinks
path = "s3://bucket/prefix/"           # For file/S3 sinks
table = "users"                           # For database sinks
schema_name = "public"                   # Optional: Database schema name
format = "parquet"                       # For file sinks: parquet, json, csv, jsonl
materialization = "table"                  # Required: table, view, incremental, file
upsert_key = "email"                      # For database sinks: Conflict resolution key
batch_size = 1000                        # Optional: Write batch size
partition_cols = ["year", "month"]       # Optional: Partition columns
compression = "snappy"                    # Optional: Compression codec
schema = "path/to/schema.py::UserSchema" # Optional: Schema reference
description = "Store users in DB"          # Optional: Description
tags = ["users", "database"]             # Optional: Tags for organization
```

### Transforms

Transforms define data processing steps.

```toml
[[transforms]]
name = "clean_users"                  # Required: Unique name
source = "users_api"                   # Required: Source/transform name
description = "Clean user data"          # Optional: Description
tags = ["cleaning"]                     # Optional: Tags

# Transformation steps (array)
steps = [
    # Extract nested fields
    { type = "extract_fields", mappings = { "company_name" = "company.name", "city" = "address.city" } },

    # Filter rows
    { type = "filter", condition = "email is not null" },
    { type = "filter", condition = 'status contains "active"' },

    # Validate against schema
    { type = "validate", schema = "path/to/schema.py::UserSchema" },

    # Add computed field
    { type = "compute_field", field = "ingested_at", value = "now()" },

    # Aggregate (group by)
    { type = "aggregate", aggregation = { "by" = ["category"], "metrics" = [{"name" = "count", "fn" = "count"}] } },

    # Sort rows
    { type = "sort", sort_by = [{ "column" = "created_at", "order" = "desc" }] },

    # Join with another source
    { type = "join", join_with = "categories", join_on = "category_id" },
]
```

### Expectations

Expectations define data quality checks.

```toml
[[expectations]]
name = "data_quality"              # Required: Unique name
asset = "clean_users"                # Required: Asset to check
description = "Validate data"        # Optional: Description

# Quality checks (array)
checks = [
    # Column cannot be null
    { type = "not_null", column = "email", severity = "error", description = "Email is required" },

    # Column must match regex
    { type = "regex", column = "email", pattern = "^[^@]+@[^@]+$", severity = "error" },

    # Column must be unique
    { type = "unique", column = "email", severity = "error" },

    # Column in range
    { type = "range", column = "age", min_value = 18, max_value = 120, severity = "warning" },

    # Column in value set
    { type = "value_set", column = "status", values = ["active", "inactive"], severity = "error" },

    # Row count in range
    { type = "row_count", min_rows = 10, max_rows = 10000, severity = "warning" },
]
```

### Jobs

Jobs define scheduled pipeline executions.

```toml
[[jobs]]
name = "daily_sync"              # Required: Unique name
schedule = "0 2 * * *"         # Required: Cron expression
sources = ["users_api"]          # Optional: Sources to include
transforms = ["clean_users"]       # Optional: Transforms to include
sinks = ["users_db"]             # Optional: Sinks to include
expectations = ["data_quality"]  # Optional: Expectations to include
environment = "prod"             # Optional: Environment name
retry_on_failure = false         # Optional: Retry on failure
timeout = 3600                  # Optional: Timeout in seconds
description = "Daily sync"        # Optional: Description
tags = ["daily", "sync"]        # Optional: Tags
```

## Environment Variable Interpolation

Use `${VAR_NAME}` syntax to reference environment variables.

```toml
[[sources]]
connection = "postgres://${DB_USER}:${DB_PASSWORD}@${DB_HOST}:5432/${DB_NAME}"

[[sinks]]
path = "s3://${AWS_ACCESS_KEY_ID}:${AWS_SECRET_ACCESS_KEY}@${S3_BUCKET}/data/"
```

With defaults:
```toml
connection = "postgres://${DB_USER:-defaultuser}:${DB_PASSWORD:-defaultpass}@${DB_HOST:-localhost}"
```

## Transform Operations Reference

### extract_fields

Extract nested fields using dot notation.

```toml
{ type = "extract_fields", mappings = {
    "company_name" = "company.name",
    "city" = "address.city",
    "country" = "address.country.code"
} }
```

### filter

Filter rows based on condition.

Supported conditions:
- `field is not null` - Field is not null/None
- `field contains "value"` - Field contains substring
- `field == value` - Field equals value
- `field > value` - Numeric comparison

```toml
{ type = "filter", condition = "email is not null" }
{ type = "filter", condition = 'status contains "active"' }
{ type = "filter", condition = "age >= 18" }
```

### validate

Validate data against schema.

```toml
{ type = "validate", schema = "path/to/schema.py::UserSchema" }
```

### compute_field

Add a new computed field.

Supported expressions:
- `now()` - Current timestamp (ISO 8601)
- `upper(field)` - Uppercase field value
- `lower(field)` - Lowercase field value
- `field_name` - Reference another field

```toml
{ type = "compute_field", field = "ingested_at", value = "now()" }
{ type = "compute_field", field = "email_upper", value = "upper(email)" }
```

### aggregate

Group and aggregate data.

```toml
{
    type = "aggregate",
    aggregation = {
        "by" = ["category", "region"],
        "metrics" = [
            { "name" = "total_count", "fn" = "count" },
            { "name" = "avg_age", "fn" = "avg", "column" = "age" },
        ]
    }
}
```

### sort

Sort rows by column.

```toml
{ type = "sort", sort_by = [
    { "column" = "created_at", "order" = "desc" },
    { "column" = "name", "order" = "asc" },
] }
```

### join

Join with another source/transform.

```toml
{ type = "join", join_with = "categories", join_on = "category_id" }
```

## CLI Commands

### Run Pipeline

```bash
# Run all assets in pipeline
vibepiper config-run-cmd --config pipeline.toml

# Run specific asset
vibepiper config-run-cmd --config pipeline.toml --asset clean_users

# Run with environment overrides
vibepiper config-run-cmd --config pipeline.toml --env DB_HOST=prod-db.example.com

# Dry run (show what would execute)
vibepiper config-run-cmd --config pipeline.toml --dry-run

# Verbose output
vibepiper config-run-cmd --config pipeline.toml --verbose
```

### Validate Configuration

```bash
# Validate pipeline configuration
vibepiper config-validate-cmd --config pipeline.toml

# Validate with verbose errors
vibepiper config-validate-cmd --config pipeline.toml --verbose
```

### Describe Pipeline

```bash
# Show pipeline DAG
vibepiper config-describe-cmd --config pipeline.toml

# Show specific asset
vibepiper config-describe-cmd --config pipeline.toml --asset clean_users
```

## Complete Example

See `examples/pipelines/demo_pipeline.toml` and `examples/pipelines/demo_pipeline.yaml` for complete working examples.

## Best Practices

1. **Use environment variables** for secrets and connection strings
2. **Version control** your pipeline configuration files
3. **Use descriptive names** for sources, sinks, transforms
4. **Add expectations** to validate data quality
5. **Document transformations** with descriptions
6. **Use tags** to organize related assets
7. **Test locally** before deploying to production
8. **Use incremental loading** for large datasets
9. **Set appropriate materialization** based on use case
10. **Configure rate limiting** for API sources

## Migration from Code to Config

### Before (Python Code)

```python
@asset(name="users_api")
def fetch_users(ctx):
    api = RESTClient(base_url="https://api.example.com")
    return api.get("/users")

@asset(name="clean_users", depends_on=["users_api"])
def clean_users(ctx, users_api):
    return [u for u in users_api if u.get("email")]
```

### After (Configuration)

```toml
[[sources]]
name = "users_api"
type = "api"
base_url = "https://api.example.com"
endpoint = "/users"

[[transforms]]
name = "clean_users"
source = "users_api"
steps = [
    { type = "filter", condition = "email is not null" },
]
```

## Troubleshooting

### Common Errors

**"Pipeline name is required"**
- Add `[pipeline]` section with `name` field

**"Source must have a 'type' field"**
- Add `type = "api"` (or `database`, `file`) to source

**"Transform depends on unknown source"**
- Ensure the `source` field matches an existing source or transform name

**"Environment variable not found"**
- Set the environment variable before running: `export VAR_NAME=value`

**"Circular dependency detected"**
- Check that transforms don't depend on each other in a loop

### Debug Mode

Enable verbose logging to debug issues:

```bash
vibepiper run --config pipeline.toml --verbose
```
