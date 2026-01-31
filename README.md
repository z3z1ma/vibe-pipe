# Vibe Piper

<div align="center">

**Declarative Data Pipeline, Integration, Quality, Transformation, and Activation Library**

[![Python Version](https://img.shields.io/badge/python-3.12%2B-blue.svg)](https://www.python.org/downloads/)
[![License](https://img.shields.io/badge/license-MIT-green.svg)](LICENSE)
[![Development Status](https://img.shields.io/badge/status-alpha-orange.svg)](https://github.com/your-org/vibe-piper)
[![Code Style](https://img.shields.io/badge/code%20style-ruff-000000.svg)](https://github.com/astral-sh/ruff)

[Features](#features) • [Quick Start](#quick-start) • [Installation](#installation) • [Examples](#usage-examples) • [Documentation](#documentation)

</div>

---

## Overview

**Vibe Piper** is a robust Python-based declarative data pipeline library designed for simplicity, expressiveness, and composability. Build production-grade data pipelines with type safety, comprehensive error handling, and seamless integrations—all with an intuitive API.

> **Status:** Early Development (Phase 0: Foundation)
>
> This project is in active development. APIs may evolve as we refine the architecture.

---

## Features

### 🎯 Core Capabilities

- **Declarative Pipeline Definition** - Build data pipelines using a clean, declarative syntax
- **Type Safety** - Full type hint support for better IDE integration and runtime reliability
- **Composable Stages** - Chain transformations in flexible, reusable ways
- **Data Quality Checks** - Built-in validation, quality metrics, and expectation suites
- **Error Handling & Recovery** - Retry logic, checkpointing, and graceful failure handling
- **Multi-format Support** - CSV, JSON, Parquet, Excel, and database connectors out of the box

### 🔌 Integrations

- **Databases** - PostgreSQL, MySQL, Snowflake, BigQuery
- **APIs** - REST clients with authentication, pagination, and GraphQL support
- **File I/O** - CSV, JSON, Parquet, Excel with schema inference
- **Webhooks** - Handle incoming webhooks with validation

---

## Quick Start

Get up and running in **5 minutes** with this end-to-end example.

### Installation

```bash
# Basic installation
pip install vibe-piper

# Or with all optional dependencies
pip install vibe-piper[all]

# For specific database support
pip install vibe-piper[postgres]    # PostgreSQL
pip install vibe-piper[mysql]       # MySQL
pip install vibe-piper[snowflake]   # Snowflake
pip install vibe-piper[bigquery]    # BigQuery
```

### Your First Pipeline

Create a file `pipeline.py`:

```python
from vibe_piper import (
    asset,
    add_field,
    filter_field_equals,
    aggregate_group_by,
    CSVReader,
    CSVWriter,
)
from pathlib import Path

# Define your data pipeline using @asset decorator
@asset
def extract_users() -> list[dict]:
    """Extract user data from CSV."""
    reader = CSVReader(Path("data/users.csv"))
    records = reader.read()
    return [record.data for record in records]

@asset
def transform_users(extract_users: list[dict]) -> list[dict]:
    """Transform and filter users."""
    from vibe_piper.operators import map_transform

    # Add a computed field
    with_category = map_transform(
        extract_users,
        add_field("category", lambda x: "premium" if x.get("age", 0) > 30 else "standard")
    )

    # Filter only active users
    active_users = filter_field_equals(with_category, "status", "active")

    return list(active_users)

@asset
def aggregate_by_category(transform_users: list[dict]) -> list[dict]:
    """Aggregate users by category."""
    return aggregate_group_by(
        transform_users,
        group_by="category",
        aggregations={"count": "count", "avg_age": "avg"}
    )

@asset
def load_results(aggregate_by_category: list[dict]) -> str:
    """Load results to output CSV."""
    output_path = Path("output/summary.csv")
    output_path.parent.mkdir(parents=True, exist_ok=True)

    from vibe_piper.types import DataRecord, Schema, SchemaField, DataType

    schema = Schema(
        name="summary",
        fields=(
            SchemaField(name="category", data_type=DataType.STRING),
            SchemaField(name="count", data_type=DataType.INTEGER),
            SchemaField(name="avg_age", data_type=DataType.FLOAT),
        )
    )

    records = [DataRecord(data=row, schema=schema) for row in aggregate_by_category]
    writer = CSVWriter(output_path)
    writer.write(records)

    return str(output_path)

# Execute the pipeline
if __name__ == "__main__":
    from vibe_piper import build_pipeline

    pipeline = build_pipeline(load_results)
    result = pipeline.execute()
    print(f"Pipeline completed! Output: {result}")
```

That's it! You now have a production-grade data pipeline with:
✅ Type-safe transformations
✅ Modular, reusable components
✅ Automatic dependency resolution
✅ Error handling and recovery

---

## Installation

### Core Installation

```bash
pip install vibe-piper
```

### Optional Dependencies

```bash
# File I/O (CSV, JSON, Parquet, Excel)
pip install vibe-piper[files]

# All database connectors
pip install vibe-piper[postgres,mysql,snowflake,bigquery]

# Development tools
pip install vibe-piper[dev]
```

### Dependencies

Core dependencies:
- `pandas>=3.0.0` - Data manipulation
- `pyarrow>=23.0.0` - Parquet support
- `openpyxl>=3.1.5` - Excel support
- `python-snappy>=0.7.3` - Compression

Optional database dependencies:
- `psycopg2-binary>=2.9.0` - PostgreSQL
- `mysql-connector-python>=8.0.0` - MySQL
- `snowflake-connector-python>=3.0.0` - Snowflake
- `google-cloud-bigquery>=3.0.0` - BigQuery

---

## Usage Examples

### Example 1: Database Connectivity (PostgreSQL)

Connect to PostgreSQL, query data, and transform it:

```python
from vibe_piper.connectors import PostgreSQLConnector, QueryBuilder
from vibe_piper import asset

# Configure connection
config = {
    "host": "localhost",
    "port": 5432,
    "database": "analytics",
    "user": "user",
    "password": "password",
}

connector = PostgreSQLConnector(config)

@asset
def fetch_active_users() -> list[dict]:
    """Fetch active users from PostgreSQL."""
    with connector:
        # Use QueryBuilder for type-safe queries
        builder = QueryBuilder("users")
        query, params = (
            builder
            .select("id", "name", "email", "created_at")
            .where("status = :status", status="active")
            .where("created_at > :date", date="2024-01-01")
            .order_by("created_at DESC")
            .limit(1000)
        ).build_select()

        result = connector.query(query, params)

        # Map to type-safe Pydantic models
        from pydantic import BaseModel

        class User(BaseModel):
            id: int
            name: str
            email: str
            created_at: str

        return connector.map_to_schema(result, User)

# Run the asset
users = fetch_active_users()
print(f"Found {len(users)} active users")
```

### Example 2: File I/O with Multiple Formats

Read from CSV, transform, and write to Parquet:

```python
from vibe_piper import asset
from vibe_piper.connectors import CSVReader, ParquetWriter
from vibe_piper.operators import map_transform, add_field
from pathlib import Path
from datetime import datetime

@asset
def csv_to_parquet() -> str:
    """Convert CSV to Parquet with schema validation."""
    # Read CSV
    csv_reader = CSVReader(Path("data/sales.csv"))
    records = csv_reader.read()

    # Infer schema from CSV
    schema = csv_reader.infer_schema()
    print(f"Inferred schema: {schema.name}")

    # Transform data
    transformed = map_transform(
        [r.data for r in records],
        add_field("processed_at", lambda x: datetime.now().isoformat())
    )

    # Write to Parquet with compression
    output_path = Path("output/sales.parquet")
    parquet_writer = ParquetWriter(output_path)

    from vibe_piper.types import DataRecord
    data_records = [DataRecord(data=row, schema=schema) for row in transformed]

    parquet_writer.write(data_records, compression="snappy")
    return str(output_path)
```

### Example 3: API Ingestion with Retry Logic

Fetch data from a REST API with automatic retries:

```python
from vibe_piper.integration import RESTClient, BearerTokenAuth
from vibe_piper.error_handling import retry_with_backoff, RetryConfig, BackoffStrategy
from vibe_piper import asset
import asyncio

@asset
def fetch_api_data() -> list[dict]:
    """Fetch data from REST API with retry logic."""

    @retry_with_backoff(
        RetryConfig(
            max_retries=3,
            backoff_strategy=BackoffStrategy.EXPONENTIAL,
            base_delay=1.0,
            max_delay=10.0,
        )
    )
    async def fetch_with_retry():
        # Configure API client
        auth = BearerTokenAuth("your-api-token")
        async with RESTClient("https://api.example.com", auth=auth) as client:
            # Fetch with pagination
            all_data = []
            page = 1

            while True:
                response = await client.get_json(
                    "/v1/users",
                    params={"page": page, "per_page": 100}
                )

                all_data.extend(response.get("data", []))

                # Check if more pages exist
                if len(response.get("data", [])) < 100:
                    break

                page += 1

            return all_data

    # Run async function
    return asyncio.run(fetch_with_retry())

# Use the fetched data
@asset
def process_api_data(fetch_api_data: list[dict]) -> int:
    """Process data from API."""
    # Filter valid records
    valid_records = [
        record for record in fetch_api_data
        if record.get("email") and "@" in record["email"]
    ]

    print(f"Processed {len(valid_records)} valid records")
    return len(valid_records)
```

### Example 4: Data Transformation with Joins and Aggregations

Combine data from multiple sources:

```python
from vibe_piper import asset
from vibe_piper.operators import (
    map_transform,
    filter_field_not_null,
    aggregate_group_by,
    custom_operator,
)

@asset
def users_with_orders() -> list[dict]:
    """Join users with their orders."""
    # Simulated data sources
    users = [
        {"id": 1, "name": "Alice", "country": "US"},
        {"id": 2, "name": "Bob", "country": "UK"},
        {"id": 3, "name": "Charlie", "country": "US"},
    ]

    orders = [
        {"user_id": 1, "total": 100.0},
        {"user_id": 1, "total": 50.0},
        {"user_id": 2, "total": 75.0},
        {"user_id": 1, "total": 25.0},
    ]

    # Custom join operator
    @custom_operator
    def left_join(users_data: list[dict], orders_data: list[dict]) -> list[dict]:
        """Left join users with orders."""
        orders_by_user = {}

        for order in orders_data:
            user_id = order["user_id"]
            if user_id not in orders_by_user:
                orders_by_user[user_id] = []
            orders_by_user[user_id].append(order)

        result = []
        for user in users_data:
            user_orders = orders_by_user.get(user["id"], [])
            total_spent = sum(o["total"] for o in user_orders)

            result.append({
                **user,
                "order_count": len(user_orders),
                "total_spent": total_spent,
            })

        return result

    # Perform join
    joined = left_join(users, orders)

    # Filter users with orders
    with_orders = filter_field_not_null(joined, "order_count")

    return list(with_orders)

@asset
def aggregate_by_country(users_with_orders: list[dict]) -> list[dict]:
    """Aggregate user spending by country."""
    return aggregate_group_by(
        users_with_orders,
        group_by="country",
        aggregations={
            "user_count": "count",
            "total_revenue": "sum",
            "avg_spending": "avg",
        }
    )
```

### Example 5: Error Handling and Data Quality

Implement comprehensive error handling and quality checks:

```python
from vibe_piper import asset, expect, ExpectationSuite
from vibe_piper.expectations import (
    expect_column_to_exist,
    expect_column_to_be_non_nullable,
    expect_table_column_count_to_equal,
)
from vibe_piper.quality import check_completeness, check_uniqueness
from vibe_piper.error_handling import CheckpointManager, Checkpoint

@asset
def validated_data() -> tuple[list[dict], dict]:
    """Extract and validate data with quality checks."""

    # Create expectation suite
    suite = ExpectationSuite(name="user_data_validation")

    suite.add_expectation(expect_column_to_exist("email"))
    suite.add_expectation(expect_column_to_be_non_nullable("id"))
    suite.add_expectation(expect_table_column_count_to_equal(5))

    # Sample data
    data = [
        {"id": 1, "name": "Alice", "email": "alice@example.com", "age": 30},
        {"id": 2, "name": "Bob", "email": "bob@example.com", "age": 25},
        {"id": 3, "name": "Charlie", "email": "charlie@example.com", "age": 35},
    ]

    # Validate against schema
    from vibe_piper.operators import validate_schema
    from vibe_piper.schema_definitions import define_schema, String, Integer

    schema = define_schema("user", {
        "id": Integer(required=True),
        "name": String(required=True),
        "email": String(required=True),
        "age": Integer(required=True),
    })

    validated = validate_schema(data, schema)

    # Run quality checks
    completeness = check_completeness(validated)
    uniqueness = check_uniqueness(validated, "id")

    quality_report = {
        "completeness": completeness.score,
        "uniqueness": uniqueness.score,
        "total_records": len(validated),
    }

    print(f"Quality Report: {quality_report}")

    return validated, quality_report

# Use checkpointing for recovery
@asset
def resilient_processing(validated_data: tuple[list[dict], dict]) -> int:
    """Process data with checkpoint-based recovery."""

    checkpoint_mgr = CheckpointManager(checkpoint_dir="checkpoints")

    # Try to load from checkpoint
    if checkpoint_mgr.has_checkpoint("processing"):
        checkpoint = checkpoint_mgr.load_checkpoint("processing")
        print(f"Resuming from checkpoint: {checkpoint.state}")
        start_index = checkpoint.metadata.get("processed_count", 0)
    else:
        start_index = 0
        checkpoint_mgr.create_checkpoint("processing", metadata={"processed_count": 0})

    data, _ = validated_data

    # Process with checkpointing
    for i, record in enumerate(data[start_index:], start=start_index):
        try:
            # Process record
            processed = {**record, "processed": True}

            # Update checkpoint every 10 records
            if (i + 1) % 10 == 0:
                checkpoint_mgr.update_checkpoint(
                    "processing",
                    metadata={"processed_count": i + 1}
                )

        except Exception as e:
            # Save error context
            from vibe_piper.error_handling import capture_error_context
            error_ctx = capture_error_context(e)
            print(f"Error at record {i}: {error_ctx.error_message}")

            # Checkpoint allows resuming from here
            raise

    # Clean up checkpoint on success
    checkpoint_mgr.delete_checkpoint("processing")

    return len(data)
```

### Example 6: GraphQL Integration

Query GraphQL APIs:

```python
from vibe_piper.integration import GraphQLClient
from vibe_piper import asset
import asyncio

@asset
def fetch_graphql_data() -> list[dict]:
    """Fetch data from GraphQL API."""

    async def fetch_data():
        client = GraphQLClient("https://api.github.com/graphql")

        # Set authentication
        client.set_auth("Bearer", "your-github-token")

        # Execute query
        query = """
        query GetRepositories($owner: String!, $limit: Int!) {
            repositoryOwner(login: $owner) {
                repositories(first: $limit) {
                    edges {
                        node {
                            name
                            stargazerCount
                            primaryLanguage {
                                name
                            }
                        }
                    }
                }
            }
        }
        """

        variables = {
            "owner": "facebook",
            "limit": 10
        }

        response = await client.execute(query, variables)
        return response

    return asyncio.run(fetch_data())
```

---

## Configuration

### Environment Variables

Configure Vibe Piper using environment variables:

```bash
# Database configuration
export VIBE_PIPER_DB_HOST=localhost
export VIBE_PIPER_DB_PORT=5432
export VIBE_PIPER_DB_NAME=analytics
export VIBE_PIPER_DB_USER=user
export VIBE_PIPER_DB_PASSWORD=password

# API configuration
export VIBE_PIPER_API_BASE_URL=https://api.example.com
export VIBE_PIPER_API_TIMEOUT=30

# Checkpoint directory
export VIBE_PIPER_CHECKPOINT_DIR=./checkpoints

# Log level
export VIBE_PIPER_LOG_LEVEL=INFO
```

### Programmatic Configuration

```python
from vibe_piper import PipelineContext

# Create custom context
context = PipelineContext(
    config={
        "checkpoint_dir": "./my_checkpoints",
        "log_level": "DEBUG",
        "max_workers": 4,
    }
)

# Use context in pipeline
pipeline = build_pipeline(my_asset, context=context)
```

---

## CLI Usage

Vibe Piper includes a CLI for common operations:

```bash
# Run a pipeline
vibe-piper run pipeline.py

# Validate a pipeline definition
vibe-piper validate pipeline.py

# Visualize pipeline DAG
vibe-piper visualize pipeline.py --output pipeline_graph.png

# Run tests
vibe-piper test

# Check data quality
vibe-piper check-quality data.csv --schema schema.json
```

---

## Architecture

Vibe Piper is built with a modular, composable architecture:

```
┌─────────────────────────────────────────────────────┐
│                   Pipeline Layer                    │
│  (Build, execute, and orchestrate data flows)      │
└─────────────────────────────────────────────────────┘
                         ↓
┌─────────────────────────────────────────────────────┐
│                  Asset Layer                        │
│  (Declarative data assets with dependencies)       │
└─────────────────────────────────────────────────────┘
                         ↓
┌─────────────────────────────────────────────────────┐
│              Operator Layer                         │
│  (Transform, filter, aggregate, validate)          │
└─────────────────────────────────────────────────────┘
                         ↓
┌─────────────────────────────────────────────────────┐
│           Integration Layer                         │
│  (Databases, APIs, Files, Webhooks)                │
└─────────────────────────────────────────────────────┘
                         ↓
┌─────────────────────────────────────────────────────┐
│         Error Handling & Quality Layer              │
│  (Retry, checkpointing, validation, metrics)       │
└─────────────────────────────────────────────────────┘
```

### Key Components

- **Assets**: Declarative data definitions with automatic dependency resolution
- **Operators**: Composable transformations (map, filter, aggregate, validate)
- **Connectors**: Standardized interfaces for external systems
- **Expectations**: Declarative data quality and validation rules
- **Error Handling**: Retry logic, checkpointing, and recovery mechanisms

---

## Documentation

Full documentation is available at: [https://your-org.github.io/vibe-piper](https://your-org.github.io/vibe-piper)

### Core Topics

- **[Getting Started](docs/source/getting_started.rst)** - Installation and basic usage
- **[Pipeline Guide](docs/source/pipeline_guide.rst)** - Building and orchestrating pipelines
- **[Connectors](docs/source/connectors.rst)** - Database and file connectors
- **[API Reference](docs/source/api_reference.rst)** - Complete API documentation
- **[Error Handling](docs/source/error_handling.rst)** - Retry logic and recovery
- **[Data Quality](docs/source/data_quality.rst)** - Validation and quality checks
- **[Integration Guide](docs/source/integration_guide.rst)** - REST, GraphQL, and webhooks
- **[Contributing](docs/source/contributing.rst)** - Contribution guidelines

### Building Documentation Locally

```bash
# Install dependencies
uv sync --dev

# Build documentation
cd docs
uv run sphinx-build -b html source build/html

# View documentation
open build/html/index.html  # macOS
# or
xdg-open build/html/index.html  # Linux
```

For development with live reload:

```bash
cd docs
uv run sphinx-autobuild source build/html
```

---

## Development

### Setup Development Environment

```bash
# Clone repository
git clone https://github.com/your-org/vibe-piper.git
cd vibe-piper

# Install development dependencies
uv sync --dev

# Install pre-commit hooks
pre-commit install
```

### Running Tests

```bash
# Run all tests
uv run pytest

# Run with coverage
uv run pytest --cov=src --cov-report=html

# Run specific test file
uv run pytest tests/test_decorators.py -v

# Run integration tests (requires Docker)
docker-compose -f docker-compose.test.yml up -d
uv run pytest -m integration
docker-compose -f docker-compose.test.yml down
```

### Code Quality

```bash
# Format code
uv run ruff format src tests

# Type checking
uv run mypy src/

# Linting
uv run ruff check src tests
```

---

## Migration Guide

If you were using early versions of Vibe Piper, here are the key API changes:

### Core Abstraction Changes

**Old API (deprecated):**
```python
from vibe_piper import Pipeline, Stage

pipeline = Pipeline(name="my_pipeline")
pipeline.add_stage(Stage(name="clean", transform=lambda x: x.strip()))
result = pipeline.run(data)
```

**New API (current):**
```python
from vibe_piper import PipelineBuilder, asset

# Using PipelineBuilder (explicit builder pattern)
pipeline = PipelineBuilder("my_pipeline")

pipeline.asset(name="source_data", fn=lambda: ["  hello  "])

@asset
def clean_data(source_data):
    return [x.strip() for x in source_data]

pipeline.asset(name="clean_data", fn=clean_data, depends_on=["source_data"])
graph = pipeline.build()

# Execute with ExecutionEngine
from vibe_piper import ExecutionEngine, PipelineContext
engine = ExecutionEngine()
context = PipelineContext(pipeline_id="my_pipeline", run_id="run_1")
result = engine.execute(graph, context)
```

### Key Changes

- **Stages → Assets**: Pipeline stages are now assets with explicit dependencies
- **Automatic Dependency Inference**: Dependencies are inferred from function parameter names
- **Separate Contexts**:
  - `PipelineContext` (runtime): Execution configuration and state
  - `PipelineDefinitionContext` (definition-time): For building pipelines declaratively
- **Multi-Upstream Support**: Assets with multiple dependencies receive structured `UpstreamData`

### Migration Tips

1. Replace `Pipeline` with `build_pipeline()` or `PipelineDefinitionContext`
2. Replace `Stage` with `@asset` decorator
   - **Important**: `@asset` decorator alone creates an Asset object
   - Use `PipelineBuilder.asset()` or `@pipeline.asset()` within a context to register assets
3. Dependencies are now inferred from parameter names (e.g., `def process(source_data:` depends on `source_data` asset)
4. Use `ExecutionEngine.execute()` to run pipelines instead of `pipeline.run()`

---

## Contributing

We welcome contributions! Please see [CONTRIBUTING.md](docs/source/contributing.rst) for guidelines.

### Areas for Contribution

- 🔌 **New Connectors** - Add support for more databases and file formats
- 🎯 **Operators** - Contribute new transformation operators
- 📚 **Documentation** - Improve docs and examples
- 🧪 **Tests** - Increase test coverage
- 🐛 **Bug Fixes** - Help squash bugs!

---

## Project Status

**Phase 0: Foundation** (Current)

We are establishing the core architecture and infrastructure. Features are being added rapidly as we build toward a stable release.

### Roadmap

- ✅ Core pipeline framework
- ✅ Asset decorators and dependency resolution
- ✅ Database connectors (PostgreSQL, MySQL, Snowflake, BigQuery)
- ✅ File I/O (CSV, JSON, Parquet, Excel)
- ✅ REST/GraphQL integration
- ✅ Error handling and retry logic
- ✅ Data quality checks
- 🚧 **In Progress**: Advanced materialization strategies
- 📋 **Planned**: Streaming data support
- 📋 **Planned**: Web UI for pipeline visualization
- 📋 **Planned**: Kubernetes execution backend

---

## License

MIT License - see [LICENSE](LICENSE) file for details.

---

## Acknowledgments

Built with inspiration from:
- [Dagster](https://dagster.io/) - Data orchestration concepts
- [Pandas](https://pandas.pydata.org/) - Data manipulation APIs
- [Great Expectations](https://greatexpectations.io/) - Data validation patterns
- [Airflow](https://airflow.apache.org/) - Pipeline abstractions

---

<div align="center">

**Built with ❤️ by the Vibe Piper Team**

[GitHub](https://github.com/your-org/vibe-piper) • [Documentation](https://your-org.github.io/vibe-piper) • [Issues](https://github.com/your-org/vibe-piper/issues) • [Discussions](https://github.com/your-org/vibe-piper/discussions)

</div>
