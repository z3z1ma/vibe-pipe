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
    SELECT * FROM users
    WHERE created_at > DATE_SUB(NOW(), INTERVAL 30 DAY)
    '''
```

### Asset Dependencies

```python
@sql_asset(
    dialect="postgresql",
    depends_on=["raw_users", "raw_orders"]
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

### With Parameters

```python
@sql_asset(dialect="postgresql")
def filtered_sales(start_date, end_date):
    return '''
    SELECT
        date,
        SUM(amount) as total_sales
    FROM sales
    WHERE created_at >= {{ start_date }}
      AND created_at <= {{ end_date }}
    GROUP BY date
    '''
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

### Multiple Dialects

```python
# PostgreSQL
@sql_asset("postgresql")
def pg_query():
    return '''
    SELECT * FROM users
    WHERE created_at > NOW() - INTERVAL '{{ days }} days'
    '''

# Snowflake
@sql_asset("snowflake")
def snow_query():
    return '''
    SELECT * FROM users
    WHERE created_at > DATEADD(DAY, {{ days }}, CURRENT_DATE())
    '''

# BigQuery
@sql_asset("bigquery")
def bq_query():
    return '''
    SELECT * FROM `project.dataset.users`
    WHERE created_at > TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL {{ days }} DAY)
    '''
```

## Asset Configuration

```python
@sql_asset(
    dialect="postgresql",
    depends_on=("raw_data",),
    io_manager="postgresql",
    materialization="table",
    description="Clean and aggregate user data",
)
def complex_asset():
    return '''
    SELECT
        id,
        email,
        created_at
    FROM {{ raw_data }}
    WHERE status = 'active'
    '''
```

## Integration with Database Connectors

SQL assets work seamlessly with database connectors:

```python
from vibe_piper.connectors import PostgreSQLConnector
from vibe_piper import sql_asset, AssetGraph, ExecutionEngine

# Create SQL asset
@sql_asset(dialect="postgresql", io_manager="postgresql")
def active_users():
    return '''
    SELECT id, email, created_at
    FROM users
    WHERE status = 'active'
    '''

# Set up database connector
config = PostgreSQLConfig(
    host="localhost",
    port=5432,
    database="mydb",
    user="user",
    password="password"
)
connector = PostgreSQLConnector(config)
connector.connect()

# Execute asset (via AssetGraph and ExecutionEngine)
graph = AssetGraph(
    name="my_pipeline",
    assets=(active_users,),
    dependencies={}
)

engine = ExecutionEngine()
result = engine.execute(graph)

connector.disconnect()
```

## SQL Validation

SQL validation automatically:

- **Syntax checking**: Validates SQL syntax before execution
- **Dangerous pattern detection**: Warns about DROP, TRUNCATE, etc.
- **Dialect-specific validation**: Ensures SQL matches selected dialect

```python
from vibe_piper.sql_assets import validate_sql

result = validate_sql(
    "SELECT * FROM users WHERE id = 1",
    dialect="postgresql"
)

if result.is_valid:
    print("SQL is valid!")
else:
    for error in result.errors:
        print(f"Error: {error}")
```

## Parameter Binding for SQL Injection Prevention

The framework supports safe parameter binding:

```python
from vibe_piper.sql_assets import execute_sql_query

connector = PostgreSQLConnector(config)
connector.connect()

# Parameters are bound safely
sql = "SELECT * FROM users WHERE id = {{ user_id }}"
result = execute_sql_query(
    sql,
    connector=connector,
    params={"user_id": 123},
    dialect="postgresql"
)
```

**Note**: Always use `{{ variable }}` for user-supplied values. Never concatenate strings directly.

## Dependency Tracking

Dependencies are automatically tracked from `{{ asset }}` references:

```python
@sql_asset
def my_asset():
    return '''
    SELECT u.*, o.*
    FROM {{ users }} u
    JOIN {{ orders }} o ON u.id = o.user_id
    '''

# Dependencies ("users", "orders") are automatically extracted
```

## Error Handling

```python
from vibe_piper.sql_assets import validate_sql, SQLValidationResult

# Handle invalid SQL
result = validate_sql("SELECT FORM users", dialect="postgresql")
if not result.is_valid:
    print(f"Validation failed: {result.errors}")

# Handle dangerous patterns
result = validate_sql("DROP TABLE users", dialect="postgresql")
for warning in result.warnings:
    print(f"Warning: {warning}")
```

## Best Practices

1. **Always use parameter binding** for user input
2. **Use CTEs** for complex queries to improve readability
3. **Specify dialect** explicitly for dialect-specific features
4. **Use dependency tracking** for pipeline lineage
5. **Validate SQL** before execution in production

## Supported Dialects

- `postgresql` / `postgres`: PostgreSQL database
- `mysql`: MySQL / MariaDB database
- `snowflake`: Snowflake data warehouse
- `bigquery`: Google BigQuery

## See Also

- [Asset Decorator](./decorators.md) - General asset decorator
- [Database Connectors](./connectors.md) - Database connectivity
- [Execution Engine](./execution.md) - Pipeline execution
