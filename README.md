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

## 🎯 Choosing the Right Pipeline Model

Vibe Piper offers two pipeline models optimized for different use cases:

| Feature | **AssetGraph** (Production) | **Pipeline** (Scripts) |
|---------|---------------------------|------------------------|
| **Use Case** | Production data pipelines, DAGs, orchestration | Quick scripts, prototypes, simple transformations |
| **Execution** | DAG-based with dependency resolution | Linear, sequential execution |
| **Data Access** | Structured `UpstreamData` for multiple upstreams | Raw data passed between operators |
| **Materialization** | Tables, files, views with storage strategies | In-memory only |
| **Features** | Caching, scheduling, quality checks, incremental | Simple, lightweight, composable |
| **Examples** | ETL pipelines, data warehouses, ML workflows | Data munging, unit tests, tutorials |

**Recommendation:** Start with **AssetGraph** for production workloads. Use **Pipeline** only for simple scripts and prototypes.

See [Choosing Between Pipeline Models](#choosing-between-pipeline-models) for detailed guidance.

NOTE: this entire codebase was created via the following command:

```bash
loom team start MiyagiDo \
  --harness opencode \
  --model zai-coding-plan/glm-4.7 \
  --investigator-model openai/gpt-5.2-codex \
  --worker-model zai-coding-plan/glm-4.7 \
  --manager-model github-copilot/gemini-3-flash-preview \
  --integrator-model zai-coding-plan/glm-4.7 \
  --objective "Create the most robust python based declararive data pipeline, integration, quality, transformation, activation library ever created. Our zen is simplicity, expressiveness, composability, and maximizing function. The UX must be intuitive. Everything must work. Use TDD."
```

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

Get up and running in **5 minutes** with one of these quick start guides.

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

### Quick Start 1: Production Pipeline (AssetGraph)

**Best for:** Production data pipelines, ETL workflows, data warehouses, ML pipelines.

Create a file `production_pipeline.py`:

```python
from vibe_piper import (
    asset,
    build_pipeline,
    UpstreamData,
    PipelineContext,
    map_transform,
    add_field,
    filter_field_equals,
    aggregate_group_by,
)
from pathlib import Path

# Define data assets using @asset decorator
@asset
def extract_users(upstream: UpstreamData, context: PipelineContext) -> list[dict]:
    """Extract user data from CSV (source asset - no dependencies)."""
    from vibe_piper.connectors import CSVReader

    reader = CSVReader(Path("data/users.csv"))
    records = reader.read()
    return [record.data for record in records]

@asset
def transform_users(upstream: UpstreamData, context: PipelineContext) -> list[dict]:
    """Transform and filter users."""
    # Access upstream data by asset name
    source_data = upstream["extract_users"]

    # Add computed field
    with_category = map_transform(
        source_data,
        add_field("category", lambda x: "premium" if x.get("age", 0) > 30 else "standard")
    )

    # Filter only active users
    active_users = filter_field_equals(with_category, "status", "active")

    return list(active_users)

@asset
def aggregate_by_category(upstream: UpstreamData, context: PipelineContext) -> list[dict]:
    """Aggregate users by category."""
    source_data = upstream["transform_users"]

    return aggregate_group_by(
        source_data,
        group_by="category",
        aggregations={"count": "count", "avg_age": "avg"}
    )

@asset
def load_results(upstream: UpstreamData, context: PipelineContext) -> str:
    """Load results to output CSV."""
    from vibe_piper.connectors import CSVWriter
    from vibe_piper.types import DataRecord, Schema, SchemaField, DataType

    source_data = upstream["aggregate_by_category"]

    output_path = Path("output/summary.csv")
    output_path.parent.mkdir(parents=True, exist_ok=True)

    schema = Schema(
        name="summary",
        fields=(
            SchemaField(name="category", data_type=DataType.STRING),
            SchemaField(name="count", data_type=DataType.INTEGER),
            SchemaField(name="avg_age", data_type=DataType.FLOAT),
        )
    )

    records = [DataRecord(data=row, schema=schema) for row in source_data]
    writer = CSVWriter(output_path)
    writer.write(records)

    return str(output_path)

# Execute the pipeline
if __name__ == "__main__":
    from vibe_piper import ExecutionEngine, PipelineContext

    # Build asset graph
    asset_graph = build_pipeline(load_results)

    # Execute with production-grade engine
    context = PipelineContext(
        pipeline_id="user_pipeline",
        run_id="run_001",
        config={},
    )

    engine = ExecutionEngine()
    result = engine.execute(asset_graph, context)

    print(f"✅ Pipeline completed! Output: {result.results[load_results.name].data}")
    print(f"   Assets executed: {len(result.results)}")
    print(f"   Execution time: {result.execution_time:.2f}s")
```

**Key AssetGraph features:**
✅ DAG-based execution with automatic dependency resolution
✅ Structured upstream data access via `UpstreamData`
✅ Production-ready error handling and retry logic
✅ Materialization strategies (tables, files, views)
✅ Orchestration, scheduling, and caching support

**More AssetGraph examples:**
- [ETL Pipeline Example](examples/etl_pipeline/) - PostgreSQL → Parquet with quality checks
- [API Ingestion Example](examples/api_ingestion/) - REST API with pagination
- See [Production Pipeline Guide](#production-pipelines-with-assetgraph) for advanced features

---

### Quick Start 2: Simple Script (Pipeline)

**Best for:** Quick data transformations, prototypes, unit tests, tutorials.

Create a file `simple_script.py`:

```python
from vibe_piper import Pipeline, Operator, OperatorType, PipelineContext

# Define a simple transformation function
def clean_text(data: list[str], context: PipelineContext) -> list[str]:
    """Clean and normalize text data."""
    return [text.strip().lower() for text in data if text.strip()]

def transform_text(data: list[str], context: PipelineContext) -> list[dict]:
    """Convert text to structured data."""
    return [{"text": text, "length": len(text)} for text in data]

def filter_short(data: list[dict], context: PipelineContext) -> list[dict]:
    """Filter out short items."""
    return [item for item in data if item["length"] > 5]

# Build the pipeline
pipeline = Pipeline(
    name="text_processing",
    operators=(
        Operator(name="clean", operator_type=OperatorType.TRANSFORM, fn=clean_text),
        Operator(name="transform", operator_type=OperatorType.TRANSFORM, fn=transform_text),
        Operator(name="filter", operator_type=OperatorType.FILTER, fn=filter_short),
    )
)

# Execute with sample data
if __name__ == "__main__":
    input_data = ["  Hello World  ", "Python", "Data", "  Vibe Piper  ", "ETL"]

    context = PipelineContext(
        pipeline_id="text_pipeline",
        run_id="script_001",
        config={},
    )

    result = pipeline.execute(input_data, context=context)

    print(f"✅ Pipeline completed!")
    print(f"   Input: {len(input_data)} items")
    print(f"   Output: {len(result)} items")
    print(f"   Results: {result}")
```

**Key Pipeline features:**
✅ Simple, linear execution model
✅ Easy to test and reason about
✅ Minimal boilerplate
✅ Great for quick prototypes and scripts

**More Pipeline examples:**
- See [Simple Script Guide](#simple-scripts-with-pipeline) for advanced features

---

## Choosing Between Pipeline Models

### Use AssetGraph (Production) When:

✅ **Production workloads** - Data pipelines that run regularly in production
✅ **Complex dependencies** - Multi-stage DAGs with branching and merging
✅ **Materialization needed** - Results need to be persisted (tables, files, views)
✅ **Orchestration required** - Scheduling, caching, incremental loading
✅ **Quality monitoring** - Data validation, drift detection, quality reports
✅ **Team collaboration** - Shared infrastructure, versioned schemas
✅ **Scalability** - Large datasets, parallel execution, resource management

**Examples:**
- ETL pipelines (Extract → Transform → Load)
- Data warehouse pipelines
- ML feature pipelines
- Real-time data processing
- Data quality monitoring

### Use Pipeline (Simple) When:

✅ **Quick scripts** - One-off data transformations
✅ **Prototyping** - Exploratory data analysis and experiments
✅ **Unit tests** - Testing individual operators or transformations
✅ **Educational** - Teaching or learning the library
✅ **Simple ETL** - Small datasets without persistence needs
✅ **Tutorials** - Demonstrating specific operators or patterns

**Examples:**
- Data munging in notebooks
- Quick CSV transformations
- Testing operator logic
- Prototype data flows
- Simple data cleaning scripts

### When to Migrate from Pipeline to AssetGraph

Consider migrating when your pipeline grows beyond simple scripts:

1. **You need persistence** - Add `@asset` decorators for materialization
2. **Dependencies become complex** - AssetGraph handles DAGs automatically
3. **Team sharing** - AssetGraph provides better structure for collaboration
4. **Production deployment** - AssetGraph integrates with orchestration tools
5. **Quality tracking** - AssetGraph has built-in quality monitoring

**Migration path:** See [Migrating from Pipeline to AssetGraph](#migration-guide) for step-by-step guidance.

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

## Production Pipelines with AssetGraph

AssetGraph is the canonical production model for Vibe Piper. Use it for all production data pipelines.

### Key Features

- **DAG-Based Execution**: Support complex dependency graphs, not just linear chains
- **Materialization**: Different storage strategies (tables, views, files) with automatic management
- **Orchestration**: Scheduling, caching, and incremental execution
- **Quality Monitoring**: Built-in data validation, drift detection, and quality reports
- **Parallel Execution**: Thread-based parallel processing for independent assets
- **State Management**: Checkpointing, recovery, and incremental runs
- **Explicit Data Contract**: `UpstreamData` provides structured access to upstream results

### Building Asset Graphs

```python
from vibe_piper import asset, build_pipeline, UpstreamData, PipelineContext, ExecutionEngine

# Define assets with @asset decorator
@asset
def extract_data(upstream: UpstreamData, context: PipelineContext) -> list[dict]:
    """Extract data from source (no dependencies)."""
    # Source asset - receives empty upstream
    assert upstream.keys == ()

    from vibe_piper.connectors import PostgreSQLConnector

    connector = PostgreSQLConnector({"host": "localhost", "database": "mydb"})
    with connector:
        return connector.query("SELECT * FROM users")

@asset
def transform_data(upstream: UpstreamData, context: PipelineContext) -> list[dict]:
    """Transform data (single dependency)."""
    # Access upstream data by asset name
    source_data = upstream["extract_data"]

    return [{"id": row["id"], "name": row["name"].upper()} for row in source_data]

@asset
def join_data(upstream: UpstreamData, context: PipelineContext) -> list[dict]:
    """Join multiple data sources (multiple dependencies)."""
    # Access all upstream data
    users = upstream["transform_data"]
    orders = upstream["extract_orders"]

    # Join logic here...
    return joined_data

@asset
def load_data(upstream: UpstreamData, context: PipelineContext) -> str:
    """Load data to target (materialization)."""
    from vibe_piper.connectors import CSVWriter

    data = upstream["join_data"]
    writer = CSVWriter(Path("output/result.csv"))
    writer.write(data)
    return str(writer.path)

# Build and execute
graph = build_pipeline(load_data)  # Builds DAG from load_data downstreams
context = PipelineContext(pipeline_id="my_pipeline", run_id="run_001")
engine = ExecutionEngine()
result = engine.execute(graph, context)
```

### Dependency Inference

AssetGraph automatically infers dependencies from function parameter names:

```python
@asset
def asset_a(upstream: UpstreamData, context: PipelineContext) -> list[dict]:
    """Source asset."""
    return [{"id": 1, "value": 100}]

@asset
def asset_b(upstream: UpstreamData, context: PipelineContext) -> list[dict]:
    """Depends on asset_a (parameter name)."""
    data = upstream["asset_a"]  # Automatically infers dependency
    return [{"id": row["id"], "doubled": row["value"] * 2} for row in data]

@asset
def asset_c(upstream: UpstreamData, context: PipelineContext) -> list[dict]:
    """Depends on asset_a AND asset_b."""
    data_a = upstream["asset_a"]
    data_b = upstream["asset_b"]
    # Join or combine data...
```

**Rules:**
- Parameter names must match upstream asset names
- Multiple parameters = multiple dependencies
- No parameters = source asset

### Materialization Strategies

AssetGraph supports multiple materialization strategies:

```python
from vibe_piper.materialization import TableStrategy, ViewStrategy, FileStrategy, IncrementalStrategy

@asset
def materialized_table(upstream: UpstreamData, context: PipelineContext) -> str:
    """Materialize as database table."""
    # Automatically handles table creation, schema management
    return TableStrategy(
        database="mydb",
        table="results",
        create_if_not_exists=True,
        drop_if_exists=False,
    )

@asset
def materialized_view(upstream: UpstreamData, context: PipelineContext) -> str:
    """Materialize as database view."""
    return ViewStrategy(
        database="mydb",
        view="results_view",
        refresh_on_query=False,
    )

@asset
def materialized_file(upstream: UpstreamData, context: PipelineContext) -> str:
    """Materialize as Parquet file."""
    from vibe_piper.connectors import ParquetWriter

    data = upstream["source_data"]
    writer = ParquetWriter(Path("output/results.parquet"))
    writer.write(data, compression="snappy")
    return str(writer.path)

@asset
def incremental_load(upstream: UpstreamData, context: PipelineContext) -> str:
    """Incremental materialization with watermark."""
    return IncrementalStrategy(
        watermark_column="updated_at",
        merge_keys=["id"],
        merge_strategy="upsert",
    )
```

### Orchestration and Scheduling

```python
from vibe_piper import OrchestrationEngine, OrchestrationConfig
from vibe_piper.scheduling import IntervalSchedule, CronSchedule

# Orchestration with parallel execution
config = OrchestrationConfig(
    max_workers=4,
    enable_incremental=True,
    enable_caching=True,
    cache_ttl_seconds=3600,
)

orch_engine = OrchestrationEngine(config)
result = orch_engine.execute(graph, context)

# Schedule for regular execution
schedule = IntervalSchedule(interval_minutes=60)
# or
schedule = CronSchedule(cron_expression="0 * * * *")  # Every hour

scheduler = Scheduler(graph, schedule)
scheduler.start()
```

### Data Quality and Monitoring

```python
from vibe_piper import expect, ExpectationSuite
from vibe_piper.expectations import (
    expect_column_to_exist,
    expect_column_to_be_non_nullable,
    expect_column_values_to_match_regex,
)

@asset
@expect(ExpectationSuite([
    expect_column_to_exist("email"),
    expect_column_to_be_non_nullable("id"),
    expect_column_values_to_match_regex("email", r"[^@]+@[^@]+\.[^@]+"),
]))
def validated_data(upstream: UpstreamData, context: PipelineContext) -> list[dict]:
    """Data with quality expectations."""
    return upstream["transformed_data"]

# Quality monitoring
from vibe_piper import check_completeness, check_uniqueness

@asset
def quality_metrics(upstream: UpstreamData, context: PipelineContext) -> dict:
    """Generate quality metrics."""
    data = upstream["validated_data"]

    completeness = check_completeness(data)
    uniqueness = check_uniqueness(data, "id")

    return {
        "completeness_score": completeness.score,
        "uniqueness_score": uniqueness.score,
        "total_records": len(data),
    }
```

### Advanced: Multi-Upstream Assets

When an asset has multiple dependencies, `UpstreamData` provides structured access:

```python
@asset
def merge_sources(upstream: UpstreamData, context: PipelineContext) -> list[dict]:
    """Merge data from multiple sources."""
    # All upstream assets available via named access
    source_a = upstream["source_a"]
    source_b = upstream["source_b"]
    source_c = upstream["source_c"]

    # Check what's available
    available_assets = upstream.keys  # ("source_a", "source_b", "source_c")

    # Safe access with defaults
    optional_data = upstream.get("optional_source", default=[])

    # Merge logic...
    return merged_data
```

### Production Checklist

Before deploying an AssetGraph to production:

- ✅ All assets have `@asset` decorator
- ✅ Dependencies are correctly inferred (parameter names match asset names)
- ✅ Materialization strategy defined for terminal assets
- ✅ Quality expectations added where needed
- ✅ Error handling and retry logic configured
- ✅ Scheduling and orchestration configured
- ✅ Monitoring and logging set up
- ✅ Integration tests pass
- ✅ Documentation updated

**Examples:**
- [ETL Pipeline Example](examples/etl_pipeline/) - Complete production ETL with PostgreSQL
- [API Ingestion Example](examples/api_ingestion/) - REST API integration
- See [CORE_ABSTRACTION_CONTRACT.md](CORE_ABSTRACTION_CONTRACT.md) for detailed contract specifications

---

## Simple Scripts with Pipeline

Use the Pipeline model for quick scripts, prototypes, and simple transformations where you don't need production features.

### When to Use Pipeline

- Quick data transformations in scripts
- Unit testing individual operators
- Simple data munging without persistence
- Educational examples and tutorials
- Exploratory data analysis

### Building Simple Pipelines

```python
from vibe_piper import Pipeline, Operator, OperatorType, PipelineContext

# Define transformation functions
def clean_data(data: list[dict], context: PipelineContext) -> list[dict]:
    """Clean and normalize data."""
    cleaned = []
    for row in data:
        cleaned.append({
            "name": row.get("name", "").strip().lower(),
            "age": int(row.get("age", 0)),
        })
    return cleaned

def filter_adults(data: list[dict], context: PipelineContext) -> list[dict]:
    """Filter out minors."""
    return [row for row in data if row["age"] >= 18]

def enrich_data(data: list[dict], context: PipelineContext) -> list[dict]:
    """Add computed fields."""
    for row in data:
        row["is_senior"] = row["age"] >= 65
        row["decade"] = (row["age"] // 10) * 10
    return data

# Build pipeline
pipeline = Pipeline(
    name="user_processing",
    operators=(
        Operator(name="clean", operator_type=OperatorType.TRANSFORM, fn=clean_data),
        Operator(name="filter", operator_type=OperatorType.FILTER, fn=filter_adults),
        Operator(name="enrich", operator_type=OperatorType.TRANSFORM, fn=enrich_data),
    )
)

# Execute
input_data = [
    {"name": "  Alice  ", "age": "25"},
    {"name": "Bob", "age": "15"},
    {"name": "  Charlie  ", "age": "70"},
]

context = PipelineContext(pipeline_id="users", run_id="script_001")
result = pipeline.execute(input_data, context=context)

print(f"Input: {len(input_data)} rows")
print(f"Output: {len(result)} rows")
print(f"Results: {result}")
```

### Operator Types

Pipeline supports different operator types:

```python
from vibe_piper import Operator, OperatorType

# Transform: Apply function to data
Operator(name="uppercase", operator_type=OperatorType.TRANSFORM, fn=lambda d, c: [x.upper() for x in d])

# Filter: Filter data based on predicate
Operator(name="keep_long", operator_type=OperatorType.FILTER, fn=lambda d, c: [x for x in d if len(x) > 5])

# Aggregate: Aggregate data
Operator(name="count", operator_type=OperatorType.AGGREGATE, fn=lambda d, c: len(d))

# Validate: Validate data (raises if fails)
Operator(name="validate", operator_type=OperatorType.VALIDATE, fn=lambda d, c: all(x is not None for x in d))
```

### Context and State

Use `PipelineContext` for configuration and state:

```python
from vibe_piper import PipelineContext

# Context with configuration
context = PipelineContext(
    pipeline_id="my_pipeline",
    run_id="run_001",
    config={
        "min_age": 18,
        "max_age": 120,
    },
    metadata={
        "environment": "dev",
        "version": "1.0",
    },
)

# Access config in operators
def filter_by_config(data: list[dict], context: PipelineContext) -> list[dict]:
    """Filter using context configuration."""
    min_age = context.get_config("min_age", 0)
    max_age = context.get_config("max_age", 200)

    return [row for row in data if min_age <= row["age"] <= max_age]

# Use state for cross-operator communication
def add_count(data: list[dict], context: PipelineContext) -> list[dict]:
    """Add count to state."""
    context.set_state("processed_count", len(data))
    return data

def print_stats(data: list[dict], context: PipelineContext) -> list[dict]:
    """Use state from previous operator."""
    count = context.get_state("processed_count", 0)
    print(f"Processed {count} records")
    return data
```

### Testing Individual Operators

Pipeline is great for testing operators in isolation:

```python
import pytest
from vibe_piper import Pipeline, Operator, OperatorType, PipelineContext

def test_clean_data_operator():
    """Test clean_data operator in isolation."""
    pipeline = Pipeline(
        name="test",
        operators=(
            Operator(name="clean", operator_type=OperatorType.TRANSFORM, fn=clean_data),
        )
    )

    input_data = [
        {"name": "  Alice  ", "age": "25"},
        {"name": "Bob", "age": "30"},
    ]

    context = PipelineContext(pipeline_id="test", run_id="test_001")
    result = pipeline.execute(input_data, context=context)

    assert len(result) == 2
    assert result[0]["name"] == "alice"
    assert result[0]["age"] == 25
    assert result[1]["name"] == "bob"
    assert result[1]["age"] == 30
```

### Custom Operators

Create reusable custom operators:

```python
def uppercase_names(data: list[dict], context: PipelineContext) -> list[dict]:
    """Uppercase all name fields."""
    for row in data:
        if "name" in row:
            row["name"] = row["name"].upper()
    return data

def normalize_phone(data: list[dict], context: PipelineContext) -> list[dict]:
    """Normalize phone numbers."""
    import re

    for row in data:
        if "phone" in row:
            # Remove all non-digits
            row["phone_clean"] = re.sub(r"[^\d]", "", row["phone"])
    return data

# Use in pipeline
pipeline = Pipeline(
    name="process_users",
    operators=(
        Operator(name="uppercase", operator_type=OperatorType.TRANSFORM, fn=uppercase_names),
        Operator(name="normalize_phone", operator_type=OperatorType.TRANSFORM, fn=normalize_phone),
    )
)
```

### Limitations of Pipeline Model

The Pipeline model is intentionally simple. Consider migrating to AssetGraph if you need:

- ❌ Complex DAGs with branching/merging
- ❌ Materialization to databases or files
- ❌ Parallel execution
- ❌ Caching and incremental runs
- ❌ Scheduling and orchestration
- ❌ Quality monitoring and validation suites
- ❌ Multi-upstream dependencies with structured access

**See Migration Guide** above for step-by-step migration to AssetGraph.

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

Vibe Piper is built with a modular, composable architecture. The canonical production model is **AssetGraph**, with **Pipeline** available for simple scripts.

```
┌─────────────────────────────────────────────────────┐
│          AssetGraph Layer (Production)             │
│  (DAG-based, materialization, orchestration)      │
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

**Alternative Model (Simple):**
```
┌─────────────────────────────────────────────────────┐
│           Pipeline Layer (Simple)                  │
│  (Linear execution, in-memory transformations)       │
└─────────────────────────────────────────────────────┘
                          ↓
┌─────────────────────────────────────────────────────┐
│              Operator Layer                         │
│  (Transform, filter, aggregate, validate)          │
└─────────────────────────────────────────────────────┘
```

### Key Components

- **AssetGraph** (Production): DAG-based asset graph with materialization, caching, orchestration
- **Pipeline** (Simple): Linear operator composition for quick scripts
- **Assets**: Declarative data definitions with automatic dependency resolution
- **Operators**: Composable transformations (map, filter, aggregate, validate)
- **Connectors**: Standardized interfaces for external systems
- **Expectations**: Declarative data quality and validation rules
- **Error Handling**: Retry logic, checkpointing, and recovery mechanisms

**Canonical Pattern:** Use `@asset` decorators → `build_pipeline()` → `ExecutionEngine` for production.

---

## Operator Data Contract

Vibe Piper uses two execution models with different operator contracts. When writing operators, it's important to understand which model you're using.

### AssetGraph Model (Production - Recommended)

For production pipelines with `@asset` decorators and `AssetGraph`, operators receive **`UpstreamData`** (structured upstream results):

```python
from vibe_piper import asset, UpstreamData, PipelineContext

# Single upstream dependency
@asset
def transform_single(upstream: UpstreamData, context: PipelineContext) -> list[dict]:
    """Operator receives UpstreamData with one upstream."""
    # Access upstream data by asset name
    source_data = upstream["source_asset"]
    return [{"processed": x} for x in source_data]

# Multiple upstream dependencies
@asset
def join_data(upstream: UpstreamData, context: PipelineContext) -> list[dict]:
    """Operator receives UpstreamData with multiple upstreams."""
    # Access all upstream data
    left_data = upstream["left_asset"]
    right_data = upstream["right_asset"]
    return {**left_data, **right_data}

# Source asset (no upstreams)
@asset
def extract_data(upstream: UpstreamData, context: PipelineContext) -> list[dict]:
    """Source asset receives empty UpstreamData."""
    assert upstream.keys == ()  # No upstreams
    return [{"id": 1, "name": "Alice"}]
```

**UpstreamData API:**
- `upstream["asset_name"]` - Get data from specific upstream asset
- `upstream.get("asset_name", default)` - Safe access with default value
- `upstream.keys` - Get tuple of all upstream asset names
- `"asset_name" in upstream` - Check if upstream exists
- `upstream.as_dict()` - Get all upstream data as dictionary

### Pipeline Model (Simple - Scripts)

For simple in-memory transformations using `Pipeline` class, operators receive **raw data** directly:

```python
from vibe_piper import Pipeline, Operator, OperatorType

def simple_transform(data: Any, context: PipelineContext) -> Any:
    """Operator receives raw data directly."""
    # data is the output from previous operator or initial input
    return [x * 2 for x in data]

operator = Operator(
    name="double_values",
    operator_type=OperatorType.TRANSFORM,
    fn=simple_transform,
)

pipeline = Pipeline(name="simple", operators=(operator,))
result = pipeline.execute([1, 2, 3])
```

### Choosing the Right Model

| Model | Use Case | Operator Signature |
|--------|-----------|------------------|
| **AssetGraph** | Production pipelines, materialization, DAGs | `fn(upstream: UpstreamData, context) -> Any` |
| **Pipeline** | Quick scripts, simple transformations, in-memory | `fn(data: Any, context) -> Any` |

**Recommendation:** Use AssetGraph with `@asset` decorators for production code. It provides structured access to upstreams, supports multi-upstream scenarios, and integrates with materialization and orchestration.

**Learn more:** See the [Execution Layering Guide](docs/execution_layering.md) for comprehensive documentation on when to use each layer.

---

## Documentation

Full documentation is available at: [https://your-org.github.io/vibe-piper](https://your-org.github.io/vibe-piper)

### Core Topics

- **[Getting Started](docs/source/getting_started.rst)** - Installation and basic usage
- **[Execution Layering Guide](docs/execution_layering.md)** - Understanding the three execution layers (Operator, Pipeline, AssetGraph)
- **[Pipeline Guide](docs/source/pipeline_guide.rst)** - Building and orchestrating pipelines
- **[Migration Guide: Pipeline → AssetGraph](docs/migration_pipeline_to_assetgraph.md)** - Migrating from Pipeline to AssetGraph
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

### Migrating from Pipeline to AssetGraph

If you've built a simple Pipeline script and need production features, here's how to migrate:

**Before (Pipeline - Simple Script):**

```python
from vibe_piper import Pipeline, Operator, OperatorType, PipelineContext

def clean(data: list[dict], context: PipelineContext) -> list[dict]:
    return [{"name": row["name"].strip().lower()} for row in data]

def filter_active(data: list[dict], context: PipelineContext) -> list[dict]:
    return [row for row in data if row.get("status") == "active"]

pipeline = Pipeline(
    name="users",
    operators=(
        Operator(name="clean", operator_type=OperatorType.TRANSFORM, fn=clean),
        Operator(name="filter", operator_type=OperatorType.FILTER, fn=filter_active),
    )
)

result = pipeline.execute(users_data, context=context)
```

**After (AssetGraph - Production Pipeline):**

```python
from vibe_piper import asset, build_pipeline, UpstreamData, PipelineContext, ExecutionEngine

@asset
def clean_users(upstream: UpstreamData, context: PipelineContext) -> list[dict]:
    """Extract and clean user data."""
    source_data = upstream["extract_users"]
    return [{"name": row["name"].strip().lower()} for row in source_data]

@asset
def filter_active_users(upstream: UpstreamData, context: PipelineContext) -> list[dict]:
    """Filter active users."""
    source_data = upstream["clean_users"]
    return [row for row in source_data if row.get("status") == "active"]

@asset
def load_results(upstream: UpstreamData, context: PipelineContext) -> str:
    """Load to output file."""
    from vibe_piper.connectors import CSVWriter
    from vibe_piper.types import DataRecord, Schema, SchemaField, DataType

    source_data = upstream["filter_active_users"]

    schema = Schema(
        name="users",
        fields=(SchemaField(name="name", data_type=DataType.STRING),)
    )

    records = [DataRecord(data=row, schema=schema) for row in source_data]
    writer = CSVWriter(Path("output/users.csv"))
    writer.write(records)

    return str(writer.path)

# Execute with production engine
graph = build_pipeline(load_results)
context = PipelineContext(pipeline_id="users", run_id="run_001")
engine = ExecutionEngine()
result = engine.execute(graph, context)
```

**Key Changes:**

| Pipeline (Simple) | AssetGraph (Production) | Why Change? |
|-------------------|------------------------|--------------|
| `def fn(data, ctx)` | `def fn(upstream, ctx)` | Structured access to upstreams |
| `Operator` objects | `@asset` decorator | Declarative definition |
| `pipeline.execute(data)` | `ExecutionEngine.execute(graph, ctx)` | Production execution engine |
| Linear operators only | DAG with dependencies | Complex workflows |
| No materialization | Tables/files/views | Persistence and caching |
| Raw data access | `upstream["asset_name"]` | Explicit data flow |

---

### Legacy API Migration (Pre-0.1)

If you were using early versions of Vibe Piper, here are the key API changes:

**Old API (deprecated):**
```python
from vibe_piper import Pipeline, Stage

pipeline = Pipeline(name="my_pipeline")
pipeline.add_stage(Stage(name="clean", transform=lambda x: x.strip()))
result = pipeline.run(data)
```

**New API (current - AssetGraph):**
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

# Execute with ExecutionEngine or OrchestrationEngine

from vibe_piper import ExecutionEngine, OrchestrationEngine, PipelineContext

# Basic execution (sequential)
engine = ExecutionEngine()
context = PipelineContext(pipeline_id="my_pipeline", run_id="run_1")
result = engine.execute(graph, context)

# Advanced orchestration (parallel, incremental, caching)
orch_engine = OrchestrationEngine(OrchestrationConfig(max_workers=4, enable_incremental=True))
context = PipelineContext(pipeline_id="my_pipeline", run_id="run_1")
result = orch_engine.execute(graph, context)
```

### Execution Layering

Vibe Piper provides a layered execution architecture with three execution levels:

- **Layer 1: Operator Execution** - Single transformation functions with unit testing support
- **Layer 2: Pipeline Execution** - Sequential operator chains for quick scripts
- **Layer 3: AssetGraph Execution** - DAG-based production pipelines with orchestration

- **ExecutionEngine** (`src/vibe_piper/execution.py`):
  - Sequential asset graph execution
  - Error handling with retry support
  - Uses shared core utilities for ordering and metrics

- **OrchestrationEngine** (`src/vibe_piper/orchestration.py`):
  - Parallel execution with thread pools
  - State tracking and incremental runs
  - Checkpointing and recovery
  - Result caching with TTL
  - Uses shared core utilities for ordering and metrics

**Learn more:** See the [Execution Layering Guide](docs/execution_layering.md) for:
- Detailed examples of each layer
- Comparison of use cases
- Migration guide from Pipeline to AssetGraph
- Advanced orchestration features

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
