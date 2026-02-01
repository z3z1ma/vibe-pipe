# SQL Asset Integration

The `@sql_asset` decorator provides SQL-based transformations with Jinja2 templating,
SQL validation, and support for multiple database dialects.

## Features

- **Jinja2 Templating**: Use `{{ variable }}` syntax in SQL templates
- **SQL Validation**: Automatic syntax validation using sqlglot
- **Parameter Binding**: Safe parameter binding to prevent SQL injection
- **Multi-Dialect Support**: PostgreSQL, MySQL, Snowflake, BigQuery
- **Dependency Tracking**: Automatic extraction of `{{ asset }}` references
- **CTE & Subquery Support**: Full SQL feature support
- **Executable Assets**: SQL assets return Assets with executable operators
- **Dependency Mapping**: Support `config["relations"]` for custom table names

## Installation

SQL assets require optional dependencies:

```bash
pip install vibe-piper[sql]
```

Or install dependencies manually:

```bash
pip install jinja2 sqlglot
```

## Basic Usage

### Simple SQL Asset

```python
from vibe_piper import sql_asset

@sql_asset
def clean_users():
    return '''
    SELECT
        id,
        LOWER(email) as email,
        created_at
    FROM raw_users
    WHERE email IS NOT NULL
    '''
```

### Specifying Dialect

```python
@sql_asset(dialect="postgresql")
def postgres_query():
    return '''
    SELECT * FROM users
    WHERE created_at > NOW() - INTERVAL '30 days'
    '''

@sql_asset(dialect="mysql")
def mysql_query():
    return '''
    SELECT * FROM customers
    WHERE created_at > DATE_SUB(NOW(), INTERVAL 30 DAY)
    '''
```

### Asset Dependencies

```python
@sql_asset(
    dialect="postgresql",
    depends_on=["raw_users", "raw_orders"],
)
def aggregated_sales():
    return '''
    SELECT
        u.id,
        u.email,
        COUNT(o.id) as order_count,
        SUM(o.amount) as total_amount
    FROM {{ raw_users }} u
        LEFT JOIN {{ raw_orders }} o ON u.id = o.user_id
        GROUP BY u.id, u.email
    '''
```

### Dependency Mapping (Relations)

SQL assets support custom mapping of asset names to database relation names via `config["relations"]`:

```python
@sql_asset(
    dialect="postgresql",
    config={"relations": {"raw_users": "staging.users", "raw_orders": "staging.orders"}},
)
def staged_query():
    return '''
    SELECT * FROM {{ raw_users }}
    '''
```

## Execution Contract

SQL assets now include an **executable operator** that:
1. **Renders SQL template** with resolved dependencies (replaces `{{ asset }}` with relation names)
2. **Validates SQL** using sqlglot for the configured dialect
3. **Executes the query** via a database connector
4. **Returns query results** from the database

### Connector Requirement

SQL assets require a database connector with an `execute_query(query, params)` method. The connector can be provided in two ways:

**Option 1: Via Asset Config**
```python
class DatabaseConnector:
    def execute_query(self, query, params=None):
        # Execute query and return results
        return [{"id": 1, "name": "test"}]

@sql_asset(config={"connector": DatabaseConnector()})
def my_query():
    return "SELECT * FROM users"
```

**Option 2: Via Context Metadata**
```python
from vibe_piper import sql_asset, PipelineContext

@sql_asset()
def my_query():
    return "SELECT * FROM users"

# In execution pipeline:
context = PipelineContext(
    pipeline_id="my_pipeline",
    run_id="123",
    metadata={"connector": DatabaseConnector()}
)
# The operator will use connector from metadata when executing
```

## Advanced Features

### Common Table Expressions (CTEs)

```python
@sql_asset(dialect="postgresql")
def ranked_users():
    return '''
    WITH ranked_users AS (
        SELECT
            id,
            email,
            ROW_NUMBER() OVER (ORDER BY created_at) as rn
        FROM {{ users }}
    )
    SELECT * FROM ranked_users
    WHERE rn <= 100
    '''
```

### Window Functions

```python
@sql_asset(dialect="postgresql")
def running_totals():
    return '''
    SELECT
        user_id,
        order_date,
        amount,
        SUM(amount) OVER (
            PARTITION BY user_id
            ORDER BY order_date
        ) as running_total
    FROM {{ transactions }}
    '''
```

## SQL Validation

SQL validation automatically:
- **Syntax checking**: Validates SQL syntax before execution
- **Dangerous pattern detection**: Warns about DROP, TRUNCATE, etc.
- **Dialect-specific validation**: Ensures SQL matches selected dialect

```python
from vibe_piper.sql_assets import validate_sql

result = validate_sql(
    "SELECT id, name FROM users WHERE active = true",
    dialect="postgresql"
)

if result.is_valid:
    print("SQL is valid!")
else:
    for error in result.errors:
        print(f"Error: {error}")
```

## Parameter Binding for SQL Injection Prevention

The framework supports safe parameter binding to prevent SQL injection:

```python
@sql_asset()
def my_query(user_id: int):
    return '''
        SELECT * FROM users
        WHERE id = {{ user_id }}
    '''

# Parameters are passed to the render context and handled safely
```

## Dependency Tracking

Dependencies are automatically tracked from `{{ asset }}` references:

```python
@sql_asset()
def my_query():
    return '''
        SELECT * FROM {{ users }} u
        JOIN {{ orders }} o ON u.id = o.user_id
    '''
```

Dependencies are extracted and stored in:
- `asset.config["depends_on"]` - list of upstream asset names
- Used during execution to resolve `{{ asset }}` to actual relation names

## Error Handling

```python
from vibe_piper.sql_assets import SQLAssetDecorator

# Option 1: Catch during asset creation
try:
    @sql_asset(dialect="invalid")  # Will fail validation
except ValueError as e:
    print(f"Invalid dialect: {e}")

# Option 2: Validate during execution
@sql_asset(config={"connector": DatabaseConnector()})
def my_query():
    return "SELECT * FROM users"  # Will be validated before execution
```

## Best Practices

1. **Always use parameter binding** for user input
2. **Use CTEs** for complex queries to improve readability
3. **Specify dialect** explicitly for dialect-specific features
4. **Use dependency tracking** for pipeline lineage
5. **Validate SQL** before execution in production
6. **Provide connector** via config or context metadata

## Supported Dialects

- `postgresql` / `postgres`: PostgreSQL database
- `mysql`: MySQL / MariaDB database
- `snowflake`: Snowflake data warehouse
- `bigquery`: Google BigQuery

## See Also

- [Asset Decorator](./decorators.md) - General asset decorator
- [Database Connectors](./connectors.md) - Database connectivity
- [Execution Engine](./execution.md) - Pipeline execution
