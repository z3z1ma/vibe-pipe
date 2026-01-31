# Migration Guide: Pipeline → AssetGraph

**Status:** Stable
**Date:** 2026-01-31
**Related:** [ADR: Canonical Pipeline Abstractions](../CORE_ABSTRACTION_CONTRACT.md) (vp-8783)

---

## Overview

This guide helps you migrate from the simple `Pipeline` model to the canonical `AssetGraph` model. While both models remain supported, `AssetGraph` is recommended for production pipelines due to its DAG-based architecture, materialization support, and orchestration features.

## Table of Contents

- [When to Migrate](#when-to-migrate)
- [Key Differences](#key-differences)
- [Migration Examples](#migration-examples)
- [Common Pitfalls](#common-pitfalls)
- [Quick Reference](#quick-reference)

---

## When to Migrate

**Migrate to `AssetGraph` when:**

- You need production-grade data pipelines with materialization (tables, files, databases)
- Your pipeline has multiple branches or complex dependencies (not just linear)
- You require scheduling, orchestration, or incremental updates
- You need data quality checks, monitoring, or caching
- You want to share assets between multiple pipelines
- Your pipeline needs parallel execution or lazy evaluation

**Stay with `Pipeline` when:**

- You're building quick scripts or prototypes
- Your pipeline is a simple linear transformation chain
- You only need in-memory data transformations
- You're writing unit tests for individual operators
- You're creating educational examples or tutorials

---

## Key Differences

### Execution Model

| Feature | Pipeline | AssetGraph |
|----------|-----------|-------------|
| **Execution** | Sequential, linear | DAG-based, parallelizable |
| **Data Flow** | Pass-through (one operator to next) | Structured (UpstreamData mapping) |
| **Dependencies** | Implicit (operator order) | Explicit (asset dependencies) |
| **Materialization** | In-memory only | Tables, files, views, streams |
| **Caching** | Manual | Built-in (cache, cache_ttl) |
| **Retries** | Manual | Configured per asset (retries, backoff) |

### Operator Signature

**Pipeline model (simple):**
```python
def operator_function(
    data: Any,  # Raw data from previous operator
    context: PipelineContext,
) -> Any:
    return transformed_data
```

**AssetGraph model (production):**
```python
def asset_operator_function(
    upstream: UpstreamData,  # Structured upstream results
    context: PipelineContext,
) -> Any:
    # Access specific upstream assets
    data_from_asset_a = upstream["asset_a"]
    return transformed_data
```

### API Surface

**Pipeline:**
- `Pipeline(name, operators, ...)`
- `Operator(name, operator_type, fn, ...)`
- `pipeline.execute(data, context)`

**AssetGraph:**
- `AssetGraph(name, assets, dependencies, ...)`
- `Asset(name, asset_type, operator, ...)`
- `@asset` decorator
- `PipelineBuilder` or `PipelineDefinitionContext`

---

## Migration Examples

### Example 1: Simple Linear Pipeline

**Before (Pipeline):**
```python
from vibe_piper import Pipeline, Operator, OperatorType, PipelineContext

def extract(data: Any, context: PipelineContext) -> list[dict]:
    """Extract data from source."""
    return [{"name": "Alice", "age": 30}, {"name": "Bob", "age": 25}]

def transform(data: list[dict], context: PipelineContext) -> list[dict]:
    """Filter adults only."""
    return [row for row in data if row["age"] >= 18]

def load(data: list[dict], context: PipelineContext) -> None:
    """Write to file."""
    import json
    with open("output.json", "w") as f:
        json.dump(data, f)

# Build pipeline
pipeline = Pipeline(
    name="simple_etl",
    operators=(
        Operator(name="extract", operator_type=OperatorType.SOURCE, fn=extract),
        Operator(name="transform", operator_type=OperatorType.FILTER, fn=transform),
        Operator(name="load", operator_type=OperatorType.SINK, fn=load),
    ),
)

# Execute
ctx = PipelineContext(pipeline_id="simple_etl", run_id="run_1")
pipeline.execute(None, context=ctx)
```

**After (AssetGraph with decorator):**
```python
from vibe_piper import asset, PipelineDefinitionContext
from vibe_piper.types import UpstreamData, PipelineContext

# Use @asset decorator for clean syntax
@asset
def raw_data(context: PipelineContext) -> list[dict]:
    """Extract data from source."""
    return [{"name": "Alice", "age": 30}, {"name": "Bob", "age": 25}]

@asset  # Dependencies inferred from parameter name 'raw_data'
def adults_only(raw_data: list[dict], context: PipelineContext) -> list[dict]:
    """Filter adults only."""
    return [row for row in raw_data if row["age"] >= 18]

@asset  # Dependencies inferred from parameter name 'adults_only'
def output(adults_only: list[dict], context: PipelineContext) -> str:
    """Write to file."""
    import json
    output_path = "output.json"
    with open(output_path, "w") as f:
        json.dump(adults_only, f)
    return output_path

# Build and execute graph
with PipelineDefinitionContext("simple_etl") as pipeline:
    raw_data
    adults_only
    output

graph = pipeline.build()

# Execute with ExecutionEngine
from vibe_piper import ExecutionEngine
engine = ExecutionEngine()
result = engine.execute(graph)
```

**Key Changes:**
1. Replace `Pipeline` + `Operator` with `@asset` decorators
2. Dependencies are inferred from parameter names
3. Use `PipelineDefinitionContext` to collect assets
4. Execute with `ExecutionEngine` instead of `pipeline.execute()`

---

### Example 2: Pipeline with Multiple Branches

**Before (Pipeline - not supported natively):**
```python
# Pipeline doesn't support branching - you need multiple pipelines
from vibe_piper import Pipeline, Operator, OperatorType, PipelineContext

# Would require manual coordination between separate pipelines
# This is a limitation of the linear model
```

**After (AssetGraph - natural DAG support):**
```python
from vibe_piper import asset, PipelineDefinitionContext
from vibe_piper.types import PipelineContext

@asset
def raw_users(context: PipelineContext) -> list[dict]:
    """Extract raw users from API."""
    return [
        {"user_id": 1, "name": "Alice", "age": 30},
        {"user_id": 2, "name": "Bob", "age": 25},
    ]

@asset  # Branch 1: filter adults
def adult_users(raw_users: list[dict], context: PipelineContext) -> list[dict]:
    """Filter users 18+."""
    return [u for u in raw_users if u["age"] >= 18]

@asset  # Branch 2: get email list
def user_emails(raw_users: list[dict], context: PipelineContext) -> list[str]:
    """Extract email addresses."""
    return [u["email"] for u in raw_users if "email" in u]

@asset  # Consumer of adult_users
def adult_summary(adult_users: list[dict], context: PipelineContext) -> dict:
    """Count adults."""
    return {"count": len(adult_users)}

@asset  # Consumer of user_emails (parallel execution)
def email_report(user_emails: list[str], context: PipelineContext) -> str:
    """Generate email report."""
    report = "\n".join(user_emails)
    print(f"Email Report:\n{report}")
    return report

# Build graph - assets can depend on different upstreams
with PipelineDefinitionContext("user_pipeline") as pipeline:
    raw_users  # Source asset
    adult_users  # Depends on raw_users
    user_emails  # Also depends on raw_users (parallel branch)
    adult_summary  # Depends on adult_users
    email_report  # Depends on user_emails

graph = pipeline.build()

# Execute - adult_summary and email_report run in parallel
from vibe_piper import ExecutionEngine
engine = ExecutionEngine()
result = engine.execute(graph)
```

**Key Changes:**
1. Multiple assets can depend on the same upstream (fan-out pattern)
2. Branches execute in parallel when possible
3. DAG structure is explicit and visualizable

---

### Example 3: Pipeline with Retry Logic

**Before (Pipeline - manual retries):**
```python
from vibe_piper import Pipeline, Operator, OperatorType, PipelineContext
import time

def extract_with_retry(data: Any, context: PipelineContext) -> list[dict]:
    """Extract data from API with manual retry logic."""
    max_retries = 3
    for attempt in range(max_retries):
        try:
            # Simulate API call that might fail
            result = [{"data": "value"}]
            return result
        except Exception as e:
            if attempt < max_retries - 1:
                wait_time = 2 ** attempt  # Exponential backoff
                print(f"Retry {attempt + 1} after {wait_time}s")
                time.sleep(wait_time)
            else:
                raise

pipeline = Pipeline(
    name="manual_retry",
    operators=(
        Operator(name="extract", operator_type=OperatorType.SOURCE, fn=extract_with_retry),
    ),
)
```

**After (AssetGraph - declarative retries):**
```python
from vibe_piper import asset, PipelineDefinitionContext
from vibe_piper.types import PipelineContext

@asset
def raw_data(context: PipelineContext) -> list[dict]:
    """Extract data from API.

    Retries are configured at the asset level - no manual code needed.
    """
    # Simulate API call that might fail
    return [{"data": "value"}]

with PipelineDefinitionContext("declarative_retry") as pipeline:
    pipeline.asset(
        name="raw_data",
        fn=raw_data,
        retries=3,           # Number of retry attempts
        backoff="exponential",  # Backoff strategy
        cache=True,           # Enable caching
        cache_ttl=3600,      # Cache for 1 hour
    )

graph = pipeline.build()

# Execution engine handles retries automatically
from vibe_piper import ExecutionEngine
engine = ExecutionEngine()
result = engine.execute(graph)
```

**Key Changes:**
1. Move retry logic from function body to asset configuration
2. Configure `retries` and `backoff` declaratively
3. Add `cache` and `cache_ttl` for performance
4. No manual sleep/retry loops in code

---

### Example 4: Pipeline with State Sharing

**Before (Pipeline - context state):**
```python
from vibe_piper import Pipeline, Operator, OperatorType, PipelineContext

def step1(data: Any, context: PipelineContext) -> str:
    """Extract and store in state."""
    value = "important_value"
    context.set_state("my_key", value)
    return value

def step2(data: str, context: PipelineContext) -> str:
    """Read from state."""
    previous_value = context.get_state("my_key")
    return f"{previous_value}_processed"

pipeline = Pipeline(
    name="state_pipeline",
    operators=(
        Operator(name="step1", operator_type=OperatorType.SOURCE, fn=step1),
        Operator(name="step2", operator_type=OperatorType.TRANSFORM, fn=step2),
    ),
)

ctx = PipelineContext(pipeline_id="state_test", run_id="run_1")
pipeline.execute(None, context=ctx)
```

**After (AssetGraph - same pattern, cleaner):**
```python
from vibe_piper import asset, PipelineDefinitionContext
from vibe_piper.types import PipelineContext

@asset
def initial_data(context: PipelineContext) -> str:
    """Extract and store in state."""
    value = "important_value"
    context.set_state("my_key", value)
    return value

@asset
def processed_data(initial_data: str, context: PipelineContext) -> str:
    """Read from state."""
    previous_value = context.get_state("my_key")
    return f"{previous_value}_processed"

with PipelineDefinitionContext("state_pipeline") as pipeline:
    initial_data
    processed_data

graph = pipeline.build()

# Execute
from vibe_piper import ExecutionEngine
engine = ExecutionEngine()
result = engine.execute(graph)
```

**Key Changes:**
1. Same `PipelineContext` state API (no change needed)
2. Cleaner syntax with `@asset` decorators
3. Dependencies inferred from parameter names

---

### Example 5: PipelineBuilder Migration

**Before (manual Pipeline construction):**
```python
from vibe_piper import Pipeline, Operator, OperatorType, PipelineContext

def extract(data, context):
    return [1, 2, 3]

def double(data, context):
    return [x * 2 for x in data]

def square(data, context):
    return [x ** 2 for x in data]

# Manual composition
pipeline = Pipeline(
    name="math_pipeline",
    operators=(
        Operator(name="extract", operator_type=OperatorType.SOURCE, fn=extract),
        Operator(name="double", operator_type=OperatorType.TRANSFORM, fn=double),
        Operator(name="square", operator_type=OperatorType.TRANSFORM, fn=square),
    ),
)
```

**After (fluent PipelineBuilder):**
```python
from vibe_piper import build_pipeline

def extract():
    return [1, 2, 3]

def double(data):
    return [x * 2 for x in data]

def square(data):
    return [x ** 2 for x in data]

# Fluent builder with automatic dependency inference
pipeline = (
    build_pipeline("math_pipeline")
    .asset("extract", fn=extract)
    .asset("double", fn=double, depends_on=["extract"])
    .asset("square", fn=square, depends_on=["double"])
)

graph = pipeline.build()

# Execute
from vibe_piper import ExecutionEngine
engine = ExecutionEngine()
result = engine.execute(graph)
```

**Key Changes:**
1. Use `build_pipeline()` for fluent interface
2. Chain `.asset()` calls
3. Explicit `depends_on` (or let parameter names infer it)

---

## Common Pitfalls

### Pitfall 1: Missing Dependency Specification

**Problem:**
```python
@asset
def processed_data(context: PipelineContext) -> list[dict]:
    # Error: 'raw_data' not found - missing dependency
    return process(raw_data)
```

**Solution:**
```python
# Option 1: Parameter name inference
@asset
def raw_data(context: PipelineContext) -> list[dict]:
    return [{"id": 1, "name": "Alice"}]

@asset  # Parameter name matches upstream asset
def processed_data(raw_data: list[dict], context: PipelineContext) -> list[dict]:
    return process(raw_data)

# Option 2: Explicit depends_on
@asset
def processed_data(context: PipelineContext, depends_on=["raw_data"]):
    data = get_upstream("raw_data")  # Custom helper
    return process(data)
```

---

### Pitfall 2: Circular Dependencies

**Problem:**
```python
@asset
def asset_a(asset_b):
    return process_b(asset_b)

@asset
def asset_b(asset_a):
    return process_a(asset_a)

# Error: Circular dependency detected
```

**Solution:**
Restructure pipeline to remove cycle. Either:
- Combine assets
- Add intermediate asset
- Revisit requirements (do you actually need the cycle?)

```python
@asset
def source_asset():
    return get_source_data()

@asset
def derived_a(source_asset):
    return process_for_a(source_asset)

@asset
def derived_b(source_asset):
    return process_for_b(source_asset)
```

---

### Pitfall 3: Ignoring Materialization Strategy

**Problem:**
```python
@asset
def large_dataset(context: PipelineContext) -> list[dict]:
    # Returns millions of rows - will stay in memory!
    return fetch_all_rows()
```

**Solution:**
```python
from vibe_piper import AssetType, build_pipeline

pipeline = (
    build_pipeline("big_data_pipeline")
    .asset(
        name="large_dataset",
        fn=lambda ctx: fetch_all_rows(),
        asset_type=AssetType.FILE,        # Materialize to file
        uri="file://data/large_dataset.parquet",
        materialization="file",            # Store as Parquet
        cache=True,                     # Cache for reusability
        lazy=True,                      # Load only when needed
    )
)
```

---

### Pitfall 4: Not Using Caching

**Problem:**
```python
# Expensive API call runs every time
@asset
def external_data(context: PipelineContext) -> list[dict]:
    return call_slow_external_api()
```

**Solution:**
```python
from vibe_piper import PipelineDefinitionContext

with PipelineDefinitionContext("optimized_pipeline") as pipeline:
    @pipeline.asset(cache=True, cache_ttl=7200)  # Cache for 2 hours
    def external_data(context: PipelineContext) -> list[dict]:
        return call_slow_external_api()

graph = pipeline.build()
```

---

### Pitfall 5: Mixing Pipeline and AssetGraph Concepts

**Problem:**
```python
# Incorrect: Trying to use UpstreamData in Pipeline model
def operator_fn(data: UpstreamData, context: PipelineContext):
    # UpstreamData is only for AssetGraph!
    return data["asset_name"]
```

**Solution:**
Use the correct data contract for each model.

**Pipeline model:**
```python
def operator_fn(data: Any, context: PipelineContext):
    # Raw data passed through directly
    return transformed_data
```

**AssetGraph model:**
```python
def asset_fn(upstream: UpstreamData, context: PipelineContext):
    # Structured access to upstream assets
    data = upstream["asset_name"]
    return transformed_data
```

---

## Quick Reference

### Migration Checklist

- [ ] Identify pipelines that need migration (production, complex, or DAG-based)
- [ ] Refactor operators to use `UpstreamData` if accessing multiple upstreams
- [ ] Replace `Pipeline` with `PipelineDefinitionContext` or `PipelineBuilder`
- [ ] Replace `Operator` with `@asset` decorators
- [ ] Configure materialization strategy (TABLE, FILE, VIEW, etc.)
- [ ] Add caching, retries, and performance settings
- [ ] Update execution to use `ExecutionEngine`
- [ ] Test migration with sample data
- [ ] Update documentation and examples

### Operator Function Signature Mapping

| From (Pipeline) | To (AssetGraph) |
|------------------|------------------|
| `fn(data, context)` | `fn(upstream, context)` or `fn(asset_name, context)` |
| `data` = previous output | `upstream["asset_name"]` = specific upstream |
| `context.get/set_state()` | `context.get/set_state()` (unchanged) |
| Manual retries | `retries=N, backoff="exponential"` |

### Common Asset Configurations

```python
# Memory asset (default)
@asset(asset_type=AssetType.MEMORY)

# Database table
@asset(
    asset_type=AssetType.TABLE,
    uri="postgresql://db/my_table",
    materialization="table",
)

# File output
@asset(
    asset_type=AssetType.FILE,
    uri="file://data/output.parquet",
    materialization="file",
)

# With caching
@asset(cache=True, cache_ttl=3600)

# With retries
@asset(retries=3, backoff="exponential")

# Parallel execution
@asset(parallel=True)

# Lazy loading
@asset(lazy=True)
```

---

## References

- [ADR: Canonical Pipeline Abstractions](../CORE_ABSTRACTION_CONTRACT.md) (vp-8783)
- [Type System Documentation](type_system.md)
- [Pipeline Builder API](../src/vibe_piper/pipeline.py)
- [Asset Type System](../src/vibe_piper/types.py)
- [Execution Engine](../src/vibe_piper/execution.py)

---

## Need Help?

- Check the [examples/](../examples/) directory for more patterns
- Review [API documentation](../README.md) for detailed method signatures
- Open an issue on GitHub for questions or migration support
