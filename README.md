# Vibe Piper

<div align="center">

**Declarative Data Pipeline Library**

[![Python Version](https://img.shields.io/badge/python-3.12%2B-blue.svg)](https://www.python.org/downloads/)
[![License](https://img.shields.io/badge/license-MIT-green.svg)](LICENSE)
[![Code Style](https://img.shields.io/badge/code%20style-ruff-000000.svg)](https://github.com/astral-sh/ruff)

[Features](#features) • [Quick Start](#quick-start) • [Installation](#installation) • [Documentation](#documentation)

</div>

---

## Overview

**Vibe Piper** is a Python-based declarative data pipeline library designed for simplicity, expressiveness, and composability. Build production-grade data pipelines with type safety, comprehensive error handling, and seamless integrations—all with an intuitive API.

> **Status:** Early Development (Phase 0: Foundation)
>
> This project is in active development. APIs may evolve as we refine the architecture.

---

## Features

### Core Capabilities

- **Two Pipeline Models** - Choose AssetGraph for production or Pipeline for simple scripts
- **Type Safety** - Full type hint support for better IDE integration and runtime reliability
- **Declarative Definition** - Build pipelines with clean, declarative syntax using decorators
- **Data Quality** - Built-in validation, quality metrics, and expectation suites
- **Error Handling** - Retry logic, checkpointing, and graceful failure handling
- **Multi-format Support** - CSV, JSON, Parquet, Excel with schema inference
- **Database Integration** - PostgreSQL, MySQL, Snowflake, BigQuery (optional)

---

## Quick Start

Get up and running in **5 minutes**.

### Choosing the Right Pipeline Model

Vibe Piper offers two pipeline models optimized for different use cases:

| Feature | **AssetGraph** (Production) | **Pipeline** (Scripts) |
|---------|---------------------------|------------------------|
| **Use Case** | Production data pipelines, DAGs, orchestration | Quick scripts, prototypes, simple transformations |
| **Execution** | DAG-based with dependency resolution | Linear, sequential execution |
| **Data Access** | Structured `UpstreamData` for multiple upstreams | Raw data passed between operators |
| **Materialization** | Tables, files, views with storage strategies | In-memory only |
| **Features** | Caching, scheduling, quality checks, incremental | Simple, lightweight, composable |

**Recommendation:** Start with **AssetGraph** for production workloads. Use **Pipeline** only for simple scripts and prototypes.

### Installation

```bash
# Core installation
pip install vibe-piper

# With file I/O support (CSV, JSON, Parquet, Excel)
pip install vibe-piper[files]

# With database support
pip install vibe-piper[postgres]     # PostgreSQL
pip install vibe-piper[mysql]        # MySQL
pip install vibe-piper[snowflake]    # Snowflake
pip install vibe-piper[bigquery]     # BigQuery

# All optional features
pip install vibe-piper[all]
```

### Quick Start 1: Production Pipeline (AssetGraph)

**Best for:** Production data pipelines, ETL workflows, data warehouses, ML pipelines.

Create a file `production_pipeline.py`:

```python
from vibe_piper import (
    PipelineDefinitionContext,
    ExecutionEngine,
    PipelineContext,
    map_transform,
    add_field,
    filter_field_equals,
    aggregate_group_by,
)
from pathlib import Path

# Define pipeline with assets
with PipelineDefinitionContext("user_pipeline") as pipeline:

    @pipeline.asset()
    def extract_users(ctx: PipelineContext) -> list[dict]:
        """Extract user data from CSV (source asset - no dependencies)."""
        from vibe_piper import CSVReader

        reader = CSVReader(Path("data/users.csv"))
        records = reader.read()
        return [record.data for record in records]

    @pipeline.asset(depends_on=["extract_users"])
    def transform_users(extract_users: list[dict], ctx: PipelineContext) -> list[dict]:
        """Transform and filter users."""
        # Add computed field
        with_category = map_transform(
            extract_users,
            add_field("category", lambda x: "premium" if x.get("age", 0) > 30 else "standard")
        )

        # Filter only active users
        active_users = filter_field_equals(with_category, "status", "active")

        return list(active_users)

    @pipeline.asset(depends_on=["transform_users"])
    def aggregate_by_category(transform_users: list[dict], ctx: PipelineContext) -> list[dict]:
        """Aggregate users by category."""
        return aggregate_group_by(
            transform_users,
            group_by="category",
            aggregations={"count": "count", "avg_age": "avg"}
        )

    @pipeline.asset(depends_on=["aggregate_by_category"])
    def load_results(aggregate_by_category: list[dict], ctx: PipelineContext) -> str:
        """Load results to output CSV."""
        from vibe_piper import CSVWriter, Schema, SchemaField, DataType, DataRecord

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

        records = [DataRecord(data=row, schema=schema) for row in aggregate_by_category]
        writer = CSVWriter(output_path)
        writer.write(records)

        return str(output_path)

# Execute the pipeline
if __name__ == "__main__":
    # Build asset graph
    asset_graph = pipeline.build()

    # Execute with production-grade engine
    context = PipelineContext(
        pipeline_id="user_pipeline",
        run_id="run_001",
        config={},
    )

    engine = ExecutionEngine()
    result = engine.execute(asset_graph, context)

    print(f"✅ Pipeline completed! Assets executed: {len(result.results)}")
    print(f"   Execution time: {result.execution_time:.2f}s")
```

**Key AssetGraph features:**
- ✅ DAG-based execution with automatic dependency resolution
- ✅ Structured upstream data access via function parameters
- ✅ Production-ready error handling and retry logic
- ✅ Materialization strategies (tables, files, views)
- ✅ Orchestration, scheduling, and caching support

**More examples:**
- See `examples/etl_pipeline/` - PostgreSQL → Parquet with quality checks
- See `examples/api_ingestion/` - REST API with pagination
- See `examples/pipelines/` - Production pipeline patterns

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
- ✅ Simple, linear execution model
- ✅ Easy to test and reason about
- ✅ Minimal boilerplate
- ✅ Great for quick prototypes and scripts

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

# Database connectors
pip install vibe-piper[postgres,mysql,snowflake,bigquery]

# Development tools
pip install vibe-piper[dev]
```

### Dependencies

**Core dependencies:**
- `pandas>=3.0.0` - Data manipulation
- `pyarrow>=23.0.0` - Parquet support
- `openpyxl>=3.1.5` - Excel support
- `python-snappy>=0.7.3` - Compression
- `pydantic>=2.10.0` - Data validation
- `httpx>=0.27.0` - HTTP client
- `typer>=0.12.0` - CLI framework

**Optional database dependencies:**
- `psycopg2-binary>=2.9.0` - PostgreSQL
- `mysql-connector-python>=8.0.0` - MySQL
- `snowflake-connector-python>=3.0.0` - Snowflake
- `google-cloud-bigquery>=3.0.0` - BigQuery

---

## Choosing Between Pipeline Models

### Use AssetGraph (Production) When:

- ✅ **Production workloads** - Data pipelines that run regularly in production
- ✅ **Complex dependencies** - Multi-stage DAGs with branching and merging
- ✅ **Materialization needed** - Results need to be persisted (tables, files, views)
- ✅ **Orchestration required** - Scheduling, caching, incremental loading
- ✅ **Quality monitoring** - Data validation, drift detection, quality reports
- ✅ **Team collaboration** - Shared infrastructure, versioned schemas
- ✅ **Scalability** - Large datasets, parallel execution, resource management

### Use Pipeline (Simple) When:

- ✅ **Quick scripts** - One-off data transformations
- ✅ **Prototyping** - Exploratory data analysis and experiments
- ✅ **Unit tests** - Testing individual operators or transformations
- ✅ **Educational** - Teaching or learning the library
- ✅ **Simple ETL** - Small datasets without persistence needs
- ✅ **Tutorials** - Demonstrating specific operators or patterns

### When to Migrate from Pipeline to AssetGraph

Consider migrating when your pipeline grows beyond simple scripts:

1. **You need persistence** - Add `@pipeline.asset()` decorators for materialization
2. **Dependencies become complex** - AssetGraph handles DAGs automatically
3. **Team sharing** - AssetGraph provides better structure for collaboration
4. **Production deployment** - AssetGraph integrates with orchestration tools
5. **Quality tracking** - AssetGraph has built-in quality monitoring

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
from vibe_piper import PipelineDefinitionContext, PipelineContext

# Create custom context
context = PipelineContext(
    pipeline_id="my_pipeline",
    run_id="run_001",
    config={
        "checkpoint_dir": "./my_checkpoints",
        "log_level": "DEBUG",
        "max_workers": 4,
    }
)

# Build pipeline with context
with PipelineDefinitionContext("my_pipeline") as pipeline:
    @pipeline.asset()
    def my_asset(ctx: PipelineContext):
        # Use context configuration
        return []

# Build graph
graph = pipeline.build()
# Execute with context
from vibe_piper import ExecutionEngine
engine = ExecutionEngine()
result = engine.execute(graph, context=context)
```

---

## CLI Usage

Vibe Piper includes a CLI for common operations:

```bash
# Initialize a new project
vibepiper init my-project

# Validate a pipeline definition
vibepiper validate

# Execute a pipeline
vibepiper run

# Run tests
vibepiper test

# Generate documentation
vibepiper docs

# Show pipeline status
vibepiper pipeline-status-cmd

# Show pipeline history
vibepiper pipeline-history-cmd

# List all assets
vibepiper asset-list-cmd

# Show asset details
vibepiper asset-show-cmd asset_name

# Configuration-driven commands
vibepiper config
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
│  (Databases, APIs, Files)                │
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
- **Connectors**: Standardized interfaces for external systems (databases, files, APIs)
- **Expectations**: Declarative data quality and validation rules
- **Error Handling**: Retry logic, checkpointing, and recovery mechanisms

**Canonical Pattern:** Use `PipelineDefinitionContext` with `@pipeline.asset()` decorators → `pipeline.build()` → `ExecutionEngine` for production.

---

## Documentation

Full documentation is available in the [docs/](docs/) directory and examples in the [examples/](examples/) directory.

### Core Topics

- **[CORE_ABSTRACTION_CONTRACT.md](CORE_ABSTRACTION_CONTRACT.md)** - Canonical pipeline abstractions and API contracts
- **[API Reference](docs/source/api_reference.rst)** - Complete API documentation
- **[Execution Layering](docs/execution_layering.md)** - Understanding AssetGraph vs Pipeline models
- **[Connectors](docs/source/connectors.rst)** - Database and file connectors
- **[Error Handling](docs/source/error_handling.rst)** - Retry logic and recovery
- **[Data Quality](docs/source/data_quality.rst)** - Validation and quality checks
- **[Integration Guide](docs/source/integration_guide.rst)** - REST, GraphQL integration
- **[Contributing](docs/source/contributing.rst)** - Contribution guidelines

### Examples

- `examples/etl_pipeline/` - Complete production ETL with PostgreSQL
- `examples/api_ingestion/` - REST API integration with pagination
- `examples/pipelines/` - Production pipeline patterns
- `examples/sample_pipeline/` - Simple transformation examples
- `examples/transformation_example.py` - Common transformation patterns
- `examples/drift_detection_example.py` - Data drift detection

---

## Development

### Setup Development Environment

```bash
# Clone repository
git clone https://github.com/your-org/vibe-piper.git
cd vibe-piper

# Install with uv (recommended)
uv sync --dev
uv pip install -e .

# Or with pip
pip install -e .[dev]
```

### Running Tests

```bash
# Run all tests
uv run pytest

# Run with coverage
uv run pytest --cov=src --cov-report=term-missing

# Run fast unit-only tests (skip integration)
uv run pytest -m "not integration"

# Run specific test file
uv run pytest tests/test_pipeline.py -v

# Run single test
uv run pytest tests/test_pipeline.py::test_pipeline_execution -v
```

### Code Quality

```bash
# Format code
uv run ruff format src tests

# Lint
uv run ruff check src tests

# Lint + autofix
uv run ruff check --fix src tests

# Type checking
uv run mypy src
```

### Snapshot Testing

Snapshot testing is available for catching regressions:

```bash
# Create snapshots on first run
uv run pytest

# Update existing snapshots
uv run pytest --update-snapshots
```

---

## License

MIT License - see [LICENSE](LICENSE) file for details.
