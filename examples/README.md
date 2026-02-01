# Vibe Piper Examples

A curated collection of examples demonstrating Vibe Piper's capabilities for building production-grade data pipelines.

## Quick Start

All examples use `uv` as the package manager. From the project root:

```bash
# Install dependencies
uv sync --dev
uv pip install -e .

# Run any example (see sections below)
uv run python examples/<example_name>/pipeline.py
```

---

## Production Pipelines (AssetGraph)

### [ETL Pipeline: PostgreSQL → Parquet](./etl_pipeline/)

A comprehensive production ETL pipeline demonstrating database connectors, file I/O, data quality checks, and incremental loading.

**Features:**
- PostgreSQL connector with connection pooling
- Parquet output with partitioning (by year/month)
- Data quality validation (30+ checks)
- Incremental loading using watermarks
- Error handling with retry logic
- Scheduling and monitoring

**Run:**
```bash
cd examples/etl_pipeline
uv pip install -e ".[postgres]"
docker-compose up -d  # Start PostgreSQL
uv run python pipeline.py
```

**Learn:** Data connectors, incremental loading, quality checks, scheduling

---

### [API Ingestion: REST → PostgreSQL](./api_ingestion/)

Production API ingestion pipeline with automatic pagination, rate limiting, and retry logic.

**Features:**
- REST API integration with multiple pagination strategies
- Token bucket rate limiting
- Exponential backoff retry logic
- Data transformation and validation
- Database upserts
- Quality reporting

**Run:**
```bash
# Dry-run mode (no database required)
uv run python examples/api_ingestion/pipeline.py --dry-run

# With database (optional)
docker run -d --name vibe-piper-demo -e POSTGRES_PASSWORD=postgres -e POSTGRES_DB=vibe_piper_demo -p 5432:5432 postgres:15
export DB_HOST=localhost DB_PORT=5432 DB_NAME=vibe_piper_demo DB_USER=postgres DB_PASSWORD=postgres
uv run python examples/api_ingestion/pipeline.py
```

**Learn:** API connectors, pagination, rate limiting, retry logic, error handling

---

### [AssetGraph ETL: CSV → CSV](./asset_graph_etl/)

A canonical, fully runnable AssetGraph example with local file I/O and validation (no external services required).

**Features:**
- PipelineBuilder with explicit asset registration
- ExecutionEngine orchestration
- ValidationSuite with fail-fast behavior
- File-based CSV input/output
- Data transformations and enrichment

**Run:**
```bash
cd examples/asset_graph_etl
uv pip install -e ".[files]"
uv run python pipeline.py --once
```

**Learn:** PipelineDefinitionContext, ExecutionEngine, ValidationSuite, explicit asset registration

---

## Data Quality

### [Drift Detection](./drift_detection/)

Monitor data quality and detect distribution changes over time using statistical tests.

**Features:**
- Baseline storage and management
- KS test and PSI (Population Stability Index) drift detection
- Drift history tracking with trend analysis
- Threshold-based alerting
- Integration with validation framework

**Run:**
```bash
# Default (1000 samples)
uv run python examples/drift_detection/run.py

# Quick mode (100 samples, faster)
uv run python examples/drift_detection/run.py --quick
```

**Learn:** Drift detection, baseline management, quality monitoring, trend analysis

---

## Simple Scripts (Pipeline)

### [Simple Pipeline](./pipeline_simple/)

Demonstrates Vibe Piper's simple, linear Pipeline model for quick scripts and prototypes.

**Features:**
- Linear execution model
- Operator composition
- In-memory transformations
- Minimal boilerplate

**Run:**
```bash
cd examples/pipeline_simple
uv pip install -e ".[files]"
uv run python pipeline.py
```

**Learn:** Pipeline model, operators, simple linear execution

---

## Standalone Examples

### [Transformation Patterns](./transformation_example.py)

Common data transformation patterns using Vibe Piper's operator library.

**Run:**
```bash
uv run python examples/transformation_example.py
```

**Learn:** Map, filter, aggregate operators, transformation patterns

---

## Configuration Examples

### [vibepiper.example.toml](./vibepiper.example.toml)

Complete configuration example with all available settings.

### [vibepiper.minimal.toml](./vibepiper.minimal.toml)

Minimal configuration template for quick setup.

---

## Choosing the Right Example

| Goal | Example |
|------|---------|
| **Production ETL** | [ETL Pipeline](./etl_pipeline/) |
| **API Integration** | [API Ingestion](./api_ingestion/) |
| **Local File Processing** | [AssetGraph ETL](./asset_graph_etl/) |
| **Data Quality Monitoring** | [Drift Detection](./drift_detection/) |
| **Quick Script** | [Simple Pipeline](./pipeline_simple/) |
| **Transformation Patterns** | [transformation_example.py](./transformation_example.py) |

---

## AssetGraph vs Pipeline

Vibe Piper offers two pipeline models:

### AssetGraph (Production)
- **Use for:** Production data pipelines, ETL workflows, data warehouses
- **Execution:** DAG-based with dependency resolution
- **Data Access:** Structured `UpstreamData` for multiple upstreams
- **Materialization:** Tables, files, views with storage strategies
- **Features:** Caching, scheduling, quality checks, incremental loading

### Pipeline (Simple)
- **Use for:** Quick scripts, prototypes, simple transformations
- **Execution:** Linear, sequential execution
- **Data Access:** Raw data passed between operators
- **Materialization:** In-memory only
- **Features:** Simple, lightweight, composable

**Recommendation:** Start with **AssetGraph** for production workloads. Use **Pipeline** only for simple scripts and prototypes.

---

## Testing Examples

Each example includes a test suite:

```bash
# Run tests for a specific example
uv run pytest examples/<example_name>/tests/

# Run with coverage
uv run pytest examples/<example_name>/tests/ --cov=examples/<example_name>
```

---

## Output Directories

Most examples generate output in an `output/` directory (or `data/` directory for inputs). These directories are gitignored by default:

- `examples/*/output/` - Generated pipeline outputs
- `examples/*/data/` - Input data (check README for location)

---

## Getting Help

- **Documentation:** See [docs/](../docs/) for full API reference
- **Issues:** Report bugs or request features on GitHub
- **Examples:** Feel free to submit example improvements via pull requests
