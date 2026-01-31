# Execution Layering Guide

**Status:** ADR vp-8783 (Canonical Pipeline Abstractions)

Vibe Piper provides a layered execution architecture that scales from simple transformations to production-grade data pipelines. This guide explains the three execution layers, their relationships, and when to use each.

## Overview

Vibe Piper has three execution layers:

```
┌─────────────────────────────────────────────────────────────┐
│           Layer 3: AssetGraph Execution (DAG)              │
│  Purpose: Execute DAG of assets with dependencies          │
│  Use: Production pipelines with orchestration, caching      │
│  Context: ExecutionEngine, OrchestrationEngine             │
└─────────────────────────────────────────────────────────────┘
                            ↑
                            │ Uses Layer 2 for asset execution
┌─────────────────────────────────────────────────────────────┐
│         Layer 2: Pipeline Execution (Sequential)           │
│  Purpose: Execute sequence of operators linearly           │
│  Use: Quick scripts, simple transformations               │
│  Context: Pipeline class with execute() method            │
└─────────────────────────────────────────────────────────────┘
                            ↑
                            │ Uses Layer 1 for operator execution
┌─────────────────────────────────────────────────────────────┐
│         Layer 1: Operator Execution (Unit)                │
│  Purpose: Execute single transformation function           │
│  Use: Atomic transformations, unit testing                 │
│  Context: Operator functions with PipelineContext          │
└─────────────────────────────────────────────────────────────┘
```

## Layer 1: Operator Execution (Unit)

**Purpose:** Execute a single transformation function on data.

**Characteristics:**
- Atomic transformations (map, filter, aggregate, validate)
- Self-contained business logic
- Receives `PipelineContext` for runtime information
- Two operator contracts (Pipeline vs AssetGraph models)

### Operator Data Contracts

There are two operator contracts depending on which execution model you use.

#### Pipeline Model Contract (Simple)

Operators receive **raw data** directly:

```python
from vibe_piper import PipelineContext, OperatorType

def double_values(data: list[int], context: PipelineContext) -> list[int]:
    """
    Args:
        data: Raw input data from previous operator
        context: Runtime execution context

    Returns:
        Transformed data for next operator

    Invariants:
        - context is always provided (never None)
        - Return type can differ from input type
        - Exceptions propagate and fail pipeline
    """
    return [x * 2 for x in data]
```

#### AssetGraph Model Contract (Production)

Operators receive structured `UpstreamData`:

```python
from vibe_piper import asset, UpstreamData, PipelineContext

@asset
def transform_data(upstream: UpstreamData, context: PipelineContext) -> list[dict]:
    """
    Args:
        upstream: Structured upstream results from dependencies
        context: Runtime execution context

    Returns:
        Transformed data for downstream assets

    Accessing Upstream Data:
        - upstream["asset_name"] - Get data from specific asset
        - upstream.get("asset_name", default) - Safe access with default
        - upstream.keys - Get all upstream asset names
    """
    # Single upstream
    source_data = upstream["source_asset"]

    # Multiple upstreams
    left_data = upstream["left_data"]
    right_data = upstream["right_data"]

    return [x * 2 for x in source_data]
```

### Built-in Operators

Vibe Piper provides a library of built-in operators:

```python
from vibe_piper.operators import (
    # Mapping
    map_transform,
    map_field,
    add_field,

    # Filtering
    filter_operator,
    filter_field_equals,
    filter_field_not_null,

    # Aggregation
    aggregate_group_by,
    aggregate_count,
    aggregate_sum,

    # Quality checks
    check_quality_completeness,
    check_quality_uniqueness,
    validate_expectation,

    # Custom operators
    custom_operator,
)
```

### When to Use Layer 1

- **Unit testing**: Test individual transformations in isolation
- **Custom business logic**: Implement domain-specific transformations
- **Operator libraries**: Reusable transformation components
- **Debugging**: Step through single transformations

### Example: Operator-Level Execution

```python
from vibe_piper import PipelineContext
from vibe_piper.operators import map_transform, add_field

# Create context
context = PipelineContext(
    pipeline_id="test_pipeline",
    run_id="run_001",
    config={"debug": True}
)

# Execute single operator
input_data = [
    {"name": "Alice", "age": 30},
    {"name": "Bob", "age": 25},
]

# Map transformation
result = list(map_transform(
    input_data,
    add_field("is_adult", lambda x: x["age"] >= 18)
))

# Result: [{"name": "Alice", "age": 30, "is_adult": True}, ...]
print(f"Transformed: {result}")
```

## Layer 2: Pipeline Execution (Sequential)

**Purpose:** Execute a sequence of operators linearly.

**Characteristics:**
- Linear operator chain
- Sequential execution
- In-memory transformations
- No materialization
- Simple API for quick scripts

### Pipeline Model API

```python
from vibe_piper import Pipeline, Operator, OperatorType, PipelineContext

# Define operators
def clean_data(data: list[dict], context: PipelineContext) -> list[dict]:
    """Remove null values and trim strings."""
    return [
        {k: v.strip() if isinstance(v, str) else v
         for k, v in record.items() if v is not None}
        for record in data
    ]

def filter_active(data: list[dict], context: PipelineContext) -> list[dict]:
    """Filter only active records."""
    return [record for record in data if record.get("status") == "active"]

# Create operators
clean_op = Operator(
    name="clean",
    operator_type=OperatorType.TRANSFORM,
    fn=clean_data,
)

filter_op = Operator(
    name="filter",
    operator_type=OperatorType.FILTER,
    fn=filter_active,
)

# Build pipeline
pipeline = Pipeline(
    name="etl_pipeline",
    operators=(clean_op, filter_op)
)

# Execute with context
context = PipelineContext(
    pipeline_id="etl_pipeline",
    run_id="run_001"
)

input_data = [
    {"name": " Alice ", "status": "active", "age": 30},
    {"name": "Bob", "status": "inactive", "age": 25},
    {"name": None, "status": "active", "age": None},
]

result = pipeline.execute(input_data, context=context)
print(f"Final result: {result}")
# Output: [{"name": "Alice", "status": "active", "age": 30}]
```

### When to Use Layer 2

- **Quick scripts**: Ad-hoc data transformations
- **Prototyping**: Test transformations before building full pipeline
- **Data munging**: Simple ETL without persistence
- **Educational examples**: Teach basic pipeline concepts
- **Unit testing**: Test operator sequences in isolation

### Example: Simple ETL Pipeline

```python
from vibe_piper import Pipeline, Operator, OperatorType, PipelineContext

# 1. Extract (read data)
def extract_csv(context: PipelineContext) -> list[dict]:
    """Simulate CSV extraction."""
    return [
        {"id": 1, "name": "Alice", "salary": "50000"},
        {"id": 2, "name": "Bob", "salary": "60000"},
        {"id": 3, "name": "Charlie", "salary": "55000"},
    ]

# 2. Transform (convert salary to int)
def transform_salary(data: list[dict], context: PipelineContext) -> list[dict]:
    """Convert salary strings to integers."""
    for record in data:
        record["salary"] = int(record["salary"])
    return data

# 3. Load (prepare for output)
def prepare_output(data: list[dict], context: PipelineContext) -> dict:
    """Prepare summary output."""
    total = sum(r["salary"] for r in data)
    avg = total / len(data)
    return {
        "count": len(data),
        "total_salary": total,
        "average_salary": avg
    }

# Build pipeline
pipeline = Pipeline(
    name="etl_example",
    operators=(
        Operator("extract", OperatorType.TRANSFORM, extract_csv),
        Operator("transform", OperatorType.TRANSFORM, transform_salary),
        Operator("load", OperatorType.TRANSFORM, prepare_output),
    )
)

# Execute
result = pipeline.execute([], context=PipelineContext(pipeline_id="etl", run_id="run1"))
print(result)
# Output: {'count': 3, 'total_salary': 165000, 'average_salary': 55000.0}
```

## Layer 3: AssetGraph Execution (DAG)

**Purpose:** Execute a DAG of assets respecting dependencies.

**Characteristics:**
- DAG-based asset graph
- Automatic dependency resolution
- Materialization support (tables, files, views)
- Orchestration (scheduling, caching, retries)
- Production-grade features

### AssetGraph Model API

The canonical production model uses `@asset` decorators and `AssetGraph`:

```python
from vibe_piper import asset, build_pipeline, ExecutionEngine, PipelineContext
from vibe_piper.operators import map_transform, add_field

# Define assets with @asset decorator
@asset
def extract_users() -> list[dict]:
    """Source asset: Extract user data."""
    return [
        {"id": 1, "name": "Alice", "status": "active", "age": 30},
        {"id": 2, "name": "Bob", "status": "inactive", "age": 25},
        {"id": 3, "name": "Charlie", "status": "active", "age": 35},
    ]

@asset
def transform_users(extract_users: list[dict]) -> list[dict]:
    """Transform: Add computed fields."""
    return list(map_transform(
        extract_users,
        add_field("is_adult", lambda x: x["age"] >= 18)
    ))

@asset
def filter_active_users(transform_users: list[dict]) -> list[dict]:
    """Filter: Only active users."""
    return [u for u in transform_users if u["status"] == "active"]

@asset
def aggregate_age(filter_active_users: list[dict]) -> dict:
    """Aggregate: Calculate age statistics."""
    ages = [u["age"] for u in filter_active_users]
    return {
        "count": len(ages),
        "avg_age": sum(ages) / len(ages),
        "min_age": min(ages),
        "max_age": max(ages),
    }

# Build asset graph
graph = build_pipeline(aggregate_age)

# Execute with ExecutionEngine
engine = ExecutionEngine()
context = PipelineContext(
    pipeline_id="user_pipeline",
    run_id="run_001"
)

result = engine.execute(graph, context)
print(f"Execution result: {result}")
# Output contains results for all assets in the graph
```

### Multi-Upstream Assets

Assets can depend on multiple upstream assets:

```python
from vibe_piper import asset, build_pipeline, ExecutionEngine, UpstreamData

@asset
def users_data() -> list[dict]:
    """Source: User data."""
    return [
        {"user_id": 1, "name": "Alice"},
        {"user_id": 2, "name": "Bob"},
    ]

@asset
def orders_data() -> list[dict]:
    """Source: Order data."""
    return [
        {"order_id": 101, "user_id": 1, "amount": 100},
        {"order_id": 102, "user_id": 1, "amount": 50},
        {"order_id": 103, "user_id": 2, "amount": 75},
    ]

@asset
def join_user_orders(
    users_data: list[dict],
    orders_data: list[dict]
) -> list[dict]:
    """Join: Merge users with their orders."""
    # Access multiple upstreams via UpstreamData
    users_by_id = {u["user_id"]: u for u in users_data}

    result = []
    for order in orders_data:
        user = users_by_id.get(order["user_id"])
        if user:
            result.append({
                "user_name": user["name"],
                "order_amount": order["amount"],
            })

    return result

@asset
def total_by_user(join_user_orders: list[dict]) -> list[dict]:
    """Aggregate: Total spending per user."""
    # Simple aggregation
    from collections import defaultdict
    totals = defaultdict(float)

    for record in join_user_orders:
        name = record["user_name"]
        amount = record["order_amount"]
        totals[name] += amount

    return [{"user": name, "total": amount} for name, amount in totals.items()]

# Build and execute
graph = build_pipeline(total_by_user)
engine = ExecutionEngine()
result = engine.execute(graph, context=PipelineContext(pipeline_id="join_example", run_id="run1"))
```

### PipelineBuilder API (Alternative)

For more control, use `PipelineBuilder`:

```python
from vibe_piper import PipelineBuilder, build_pipeline

# Create builder
pipeline = PipelineBuilder("my_pipeline")

# Add assets explicitly
pipeline.asset(
    name="extract_data",
    fn=lambda: [1, 2, 3, 4, 5],
)

pipeline.asset(
    name="transform_data",
    fn=lambda data: [x * 2 for x in data],
    depends_on=["extract_data"],
)

pipeline.asset(
    name="sum_data",
    fn=sum,
    depends_on=["transform_data"],
)

# Build graph
graph = pipeline.build()

# Execute
engine = ExecutionEngine()
result = engine.execute(graph, context=PipelineContext(pipeline_id="builder_example", run_id="run1"))
```

### When to Use Layer 3

- **Production pipelines**: Data pipelines that run in production
- **Complex dependencies**: Multi-step transformations with DAG structure
- **Materialization**: Data needs to be persisted (tables, files, views)
- **Orchestration**: Scheduling, caching, retries, and incremental runs
- **Quality checks**: Data quality and monitoring
- **Scalability**: Large datasets with optimization needs

### Example: Production ETL Pipeline

```python
from vibe_piper import (
    asset, build_pipeline, ExecutionEngine, PipelineContext,
    CSVReader, CSVWriter
)
from vibe_piper.operators import (
    map_transform, add_field, filter_field_equals,
    aggregate_group_by, validate_expectation
)
from vibe_piper.expectations import (
    ExpectationSuite,
    expect_column_to_exist,
    expect_column_to_be_non_nullable,
)
from pathlib import Path

# 1. Extract from CSV
@asset
def extract_raw_sales() -> list[dict]:
    """Extract raw sales data from CSV."""
    reader = CSVReader(Path("data/sales.csv"))
    records = reader.read()
    return [r.data for r in records]

# 2. Validate schema
@asset
def validate_sales(extract_raw_sales: list[dict]) -> list[dict]:
    """Validate sales data against schema."""
    suite = ExpectationSuite(name="sales_validation")
    suite.add_expectation(expect_column_to_exist("product_id"))
    suite.add_expectation(expect_column_to_be_non_nullable("product_id"))

    return validate_expectation(extract_raw_sales, suite)

# 3. Transform data
@asset
def transform_sales(validate_sales: list[dict]) -> list[dict]:
    """Add computed fields and normalize."""
    return list(map_transform(
        validate_sales,
        add_field("revenue_category", lambda x: "high" if x.get("amount", 0) > 100 else "low")
    ))

# 4. Filter valid records
@asset
def filter_valid(transform_sales: list[dict]) -> list[dict]:
    """Filter only valid, high-value transactions."""
    return filter_field_equals(transform_sales, "status", "completed")

# 5. Aggregate metrics
@asset
def aggregate_metrics(filter_valid: list[dict]) -> list[dict]:
    """Aggregate sales metrics by product."""
    return aggregate_group_by(
        filter_valid,
        group_by="product_id",
        aggregations={
            "total_revenue": "sum",
            "transaction_count": "count",
            "avg_amount": "avg",
        }
    )

# 6. Load to output
@asset
def load_output(aggregate_metrics: list[dict]) -> str:
    """Load aggregated data to output CSV."""
    output_path = Path("output/sales_summary.csv")
    output_path.parent.mkdir(parents=True, exist_ok=True)

    # Write output
    from vibe_piper.types import Schema, SchemaField, DataType, DataRecord

    schema = Schema(
        name="sales_summary",
        fields=(
            SchemaField(name="product_id", data_type=DataType.INTEGER),
            SchemaField(name="total_revenue", data_type=DataType.FLOAT),
            SchemaField(name="transaction_count", data_type=DataType.INTEGER),
            SchemaField(name="avg_amount", data_type=DataType.FLOAT),
        )
    )

    records = [DataRecord(data=row, schema=schema) for row in aggregate_metrics]
    writer = CSVWriter(output_path)
    writer.write(records)

    return str(output_path)

# Build and execute production pipeline
graph = build_pipeline(load_output)
engine = ExecutionEngine()
context = PipelineContext(
    pipeline_id="sales_etl",
    run_id="production_run_20250131",
    config={
        "checkpoint_dir": "./checkpoints",
        "enable_caching": True,
    }
)

result = engine.execute(graph, context)
print(f"Pipeline completed successfully. Output: {result}")
```

## Comparison of Execution Layers

| Feature | Layer 1: Operator | Layer 2: Pipeline | Layer 3: AssetGraph |
|---------|-------------------|-------------------|---------------------|
| **Scope** | Single transformation | Linear operator chain | DAG of assets |
| **Data Flow** | Input → Output | Sequential flow | Dependency graph |
| **Materialization** | No | No | Yes (tables, files, views) |
| **Orchestration** | No | No | Yes (scheduling, caching) |
| **Dependencies** | N/A | Linear only | Complex DAG |
| **Multi-upstream** | N/A | No | Yes |
| **Type Safety** | Full | Full | Full |
| **Use Case** | Unit testing, custom logic | Quick scripts, prototyping | Production pipelines |

### Choosing the Right Layer

```python
# Use Layer 1 (Operator) when:
# - Testing a single transformation
def test_clean_name():
    result = clean_name({"name": "  Alice  "})
    assert result == "Alice"

# Use Layer 2 (Pipeline) when:
# - Quick one-off data transformation
def quick_cleanup():
    pipeline = Pipeline(name="cleanup", operators=(clean_op, filter_op))
    return pipeline.execute(raw_data)

# Use Layer 3 (AssetGraph) when:
# - Building production data pipeline
@asset
def production_pipeline():
    """This is the canonical choice for production."""
    ...
```

## Migration: Pipeline → AssetGraph

If you have a `Pipeline` and want to upgrade to `AssetGraph`:

### Before (Pipeline Model)

```python
from vibe_piper import Pipeline, Operator, OperatorType

def clean_data(data, context):
    return [x.strip() for x in data]

def filter_data(data, context):
    return [x for x in data if x]

pipeline = Pipeline(
    name="simple",
    operators=(
        Operator("clean", OperatorType.TRANSFORM, clean_data),
        Operator("filter", OperatorType.FILTER, filter_data),
    )
)
result = pipeline.execute(["  alice  ", "  bob  ", None])
```

### After (AssetGraph Model)

```python
from vibe_piper import asset, build_pipeline, ExecutionEngine

@asset
def clean_data():
    return ["  alice  ", "  bob  ", None]

@asset
def filter_data(clean_data: list[str]) -> list[str]:
    return [x.strip() for x in clean_data if x]

graph = build_pipeline(filter_data)
engine = ExecutionEngine()
result = engine.execute(graph, context=PipelineContext(pipeline_id="simple", run_id="run1"))
```

**Key Differences:**
- Dependencies are inferred from function parameters
- No need to create `Operator` objects
- Assets are automatically materialized
- Supports orchestration features

## Advanced: OrchestrationEngine

For advanced features, use `OrchestrationEngine` instead of `ExecutionEngine`:

```python
from vibe_piper import OrchestrationEngine, OrchestrationConfig

# Configure orchestration
orch_config = OrchestrationConfig(
    max_workers=4,           # Parallel execution
    enable_incremental=True,  # Incremental runs
    enable_caching=True,     # Result caching
    cache_ttl=3600,        # Cache TTL in seconds
)

# Create engine
engine = OrchestrationEngine(config=orch_config)

# Execute
result = engine.execute(graph, context)
```

**OrchestrationEngine Features:**
- **Parallel execution**: Run independent assets in parallel
- **Incremental runs**: Only run changed assets
- **Caching**: Store and reuse results
- **Checkpoints**: Resume from failures
- **Metrics**: Track execution performance

## Summary

- **Layer 1 (Operator)**: Atomic transformations for testing and reuse
- **Layer 2 (Pipeline)**: Quick scripts and prototyping
- **Layer 3 (AssetGraph)**: Production pipelines with orchestration

**Recommendation:** Start with Layer 3 (`AssetGraph`) for production code. It's the canonical model and provides the most features. Use Layer 1 for unit testing and Layer 2 for quick experiments.

## References

- [ADR: Canonical Pipeline Abstractions](../CORE_ABSTRACTION_CONTRACT.md) - Architecture decision defining execution layers
- [API Reference](api/index.md) - Complete API documentation
- [Pipeline Guide](pipeline_guide.md) - Building and orchestrating pipelines
- [Getting Started](getting_started.md) - Installation and basic usage
