# Database Connectors

This module provides database connectors for common databases, enabling VibePiper to interact with real data sources.

## Supported Databases

- **PostgreSQL** - Production-grade relational database
- **MySQL** - Popular open-source relational database
- **Snowflake** - Cloud data warehouse
- **BigQuery** - Google's serverless data warehouse

## Installation

Install the required dependencies for your database:

```bash
# For all connectors
pip install vibe-piper[all]

# For PostgreSQL only
pip install vibe-piper[postgres]
pip install psycopg2-binary

# For MySQL only
pip install vibe-piper[mysql]
pip install mysql-connector-python

# For Snowflake only
pip install vibe-piper[snowflake]
pip install snowflake-connector-python

# For BigQuery only
pip install vibe-piper[bigquery]
pip install google-cloud-bigquery
```

## Quick Start

### PostgreSQL Connector

```python
from vibe_piper.connectors import PostgreSQLConnector, PostgreSQLConfig

# Create configuration
config = PostgreSQLConfig(
    host="localhost",
    port=5432,
    database="mydb",
    user="user",
    password="password",
    pool_size=10
)

# Create and use connector
connector = PostgreSQLConnector(config)

# Use as context manager (auto connect/disconnect)
with connector:
    result = connector.query("SELECT * FROM users WHERE active = true")
    for row in result.rows:
        print(row)
```

### MySQL Connector

```python
from vibe_piper.connectors import MySQLConnector, MySQLConfig

config = MySQLConfig(
    host="localhost",
    port=3306,
    database="mydb",
    user="user",
    password="password",
    pool_size=10
)

connector = MySQLConnector(config)
with connector:
    result = connector.query("SELECT * FROM customers")
    print(f"Found {result.row_count} customers")
```

### Snowflake Connector

```python
from vibe_piper.connectors import SnowflakeConnector, SnowflakeConfig

config = SnowflakeConfig(
    account="xy12345.us-east-1",
    host="xy12345.us-east-1.snowflakecomputing.com",
    port=443,
    database="mydb",
    warehouse="compute_wh",
    schema="public",
    user="user",
    password="password"
)

connector = SnowflakeConnector(config)
with connector:
    result = connector.query("SELECT * FROM analytics_events LIMIT 100")
```

### BigQuery Connector

```python
from vibe_piper.connectors import BigQueryConnector, BigQueryConfig

config = BigQueryConfig(
    project_id="my-project",
    credentials_path="/path/to/service-account.json",
    location="US"
)

connector = BigQueryConnector(config)
with connector:
    result = connector.query("SELECT * FROM dataset.users LIMIT 1000")
```

## Query Builder

The `QueryBuilder` class provides a fluent interface for building SQL queries programmatically:

```python
from vibe_piper.connectors import QueryBuilder

builder = QueryBuilder("users")

# Build complex query
query, params = (
    builder
    .select("id", "name", "email")
    .where("status = :status", status="active")
    .where("age > :age", age=18)
    .order_by("created_at DESC")
    .limit(10)
).build_select()

# Execute with connector
result = connector.query(query, params)
```

### Supported Query Builder Operations

- **Select**: Specify columns to select
- **Where**: Add WHERE clauses with parameterized queries
- **Join**: Add INNER, LEFT, RIGHT, or FULL joins
- **Order By**: Sort results
- **Group By**: Group results
- **Limit/Offset**: Pagination
- **Insert**: Build INSERT queries
- **Update**: Build UPDATE queries
- **Delete**: Build DELETE queries

## Type-Safe Result Mapping

Map query results directly to Pydantic models for type safety:

```python
from pydantic import BaseModel

class User(BaseModel):
    id: int
    name: str
    email: str

# Query database
result = connector.query("SELECT * FROM users")

# Map to Pydantic models
users = connector.map_to_schema(result, User)

# Now you have type-safe objects
for user in users:
    print(f"{user.name}: {user.email}")
    assert isinstance(user.id, int)
```

## Connection Pooling

All connectors support connection pooling for better performance:

```python
config = PostgreSQLConfig(
    host="localhost",
    port=5432,
    database="mydb",
    user="user",
    password="password",
    pool_size=20,        # Minimum connections in pool
    max_overflow=40,     # Additional connections when needed
    pool_timeout=30,     # Wait time for connection
    pool_recycle=3600    # Recycle connections after 1 hour
)
```

## Transactions

Execute multiple operations in a transaction:

```python
# PostgreSQL/MySQL
with connector.transaction():
    connector.execute("INSERT INTO accounts (balance) VALUES (100)")
    connector.execute("UPDATE accounts SET balance = balance - 50 WHERE id = 1")
    # Both succeed or both roll back

# Snowflake
with connector.transaction() as cursor:
    cursor.execute("INSERT INTO accounts (balance) VALUES (100)")
    cursor.execute("UPDATE accounts SET balance = balance - 50 WHERE id = 1")
```

## Batch Operations

### PostgreSQL

```python
# Batch insert
insert_sql = "INSERT INTO products (name, price) VALUES (:name, :price)"
params_list = [
    {"name": "Product 1", "price": 10.99},
    {"name": "Product 2", "price": 20.99},
]
affected = connector.execute_batch(insert_sql, params_list)
```

### MySQL

```python
# Batch insert (execute_many is faster)
insert_sql = "INSERT INTO products (name, price) VALUES (%s, %s)"
params_list = [
    ("Product 1", 10.99),
    ("Product 2", 20.99),
]
affected = connector.execute_many(insert_sql, params_list)
```

## Using with VibePiper Assets

```python
from vibe_piper import asset

@asset(io_manager="postgresql", connector="my_connector")
def active_users(connector: PostgreSQLConnector):
    query = "SELECT * FROM users WHERE active = true"
    result = connector.query(query)

    # Type-safe mapping
    class User(BaseModel):
        id: int
        name: str
        email: str

    return connector.map_to_schema(result, User)
```

## Integration Tests

Run integration tests with Docker Compose:

```bash
# Start test databases
docker-compose -f docker-compose.test.yml up -d

# Run integration tests
pytest tests/connectors/test_postgres_integration.py -v -m integration
pytest tests/connectors/test_mysql_integration.py -v -m integration

# Stop test databases
docker-compose -f docker-compose.test.yml down
```

Or use the convenience script:

```bash
bash scripts/run_integration_tests.sh
```

## Best Practices

1. **Always use parameterized queries** to prevent SQL injection
2. **Use context managers** (`with connector:`) for automatic cleanup
3. **Set appropriate pool sizes** based on your workload
4. **Use transactions** for multi-step operations
5. **Map to Pydantic models** for type safety
6. **Handle connection errors** gracefully with try/except blocks
7. **Close connections** when done (context manager handles this)

## Error Handling

```python
from vibe_piper.connectors import PostgreSQLConnector, PostgreSQLConfig

try:
    connector = PostgreSQLConnector(config)
    with connector:
        result = connector.query("SELECT * FROM users")
except ConnectionError as e:
    print(f"Failed to connect: {e}")
except Exception as e:
    print(f"Query failed: {e}")
```

## API Reference

### DatabaseConnector Protocol

All connectors implement the `DatabaseConnector` protocol:

- **`connect()`** - Establish database connection
- **`disconnect()`** - Close database connection
- **`query(query, params)`** - Execute SELECT query
- **`execute(query, params)`** - Execute INSERT/UPDATE/DELETE
- **`transaction()`** - Context manager for transactions
- **`map_to_schema(result, schema)`** - Map results to Pydantic model

### QueryBuilder

- **`select(*columns)`** - Specify columns
- **`where(clause, **params)`** - Add WHERE clause
- **`join(table, on, type)`** - Add JOIN
- **`order_by(*columns)`** - Add ORDER BY
- **`group_by(*columns)`** - Add GROUP BY
- **`limit(count)`** - Add LIMIT
- **`offset(count)`** - Add OFFSET
- **`build_select()`** - Build SELECT query
- **`build_insert(table, data)`** - Build INSERT query
- **`build_update(table, data, where, params)`** - Build UPDATE query
- **`build_delete(table, where, params)`** - Build DELETE query

## License

MIT
