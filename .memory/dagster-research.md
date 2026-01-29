# Dagster Research Memo

**Date:** 2026-01-27
**Framework:** Dagster
**Focus:** Asset-Based Data Orchestration

## Executive Summary

Dagster is a modern data orchestrator that pioneered the **asset-centric** approach to data pipelines. Instead of focusing on tasks (what to run), Dagster focuses on assets (what data exists). This paradigm shift provides better data lineage, testing, and observability.

## Core Architecture

### Design Philosophy
- **Asset-centric**: Pipelines defined by data assets produced/consumed
- **Software-defined assets (SDAs)**: Assets are first-class code objects
- **Separation of concerns**: Data definition separate from computation
- **Type-safe**: Strong typing with runtime validation

### Key Components

1. **Assets**
   ```python
   from dagster import asset

   @asset
   def clean_data(raw_data):
       return raw_data.dropna()

   @asset(deps=[clean_data])
   def aggregated_metrics(clean_data):
       return clean_data.groupby('category').sum()
   ```
   - Function produces an asset
   - Dependencies inferred from function parameters
   - Assets have schemas and metadata
   - Automatic lineage tracking

2. **Ops & Jobs** (Legacy Task-Based)
   - `@op` decorator for task-like operations
   - `@job` for composing ops into DAGs
   - Being phased out in favor of assets
   - Lesson: Task-based approach is less expressive

3. **IO Managers**
   - Handle asset serialization/deserialization
   - Pluggable storage backends
   - Type-based routing (different types → different storage)
   - Built-in support for Pandas, PySpark, etc.

4. **Resource System**
   - Dependency injection for external resources
   - Databases, APIs, file systems
   - Configurable per-environment
   - Enables testing without real resources

## Notable Patterns for VibePiper

### Strengths to Emulate

1. **Asset-Centric Model** ⭐
   - More intuitive mental model ("I want a customer table", not "I want to run a task")
   - Automatic data lineage (upstream/downstream assets)
   - Natural testing (test asset function in isolation)
   - Better documentation (asset is the unit of documentation)

2. **Strong Typing + Runtime Validation**
   ```python
   @asset
   def customers() -> DataFrame[CustomerSchema]:
       # Returns validated DataFrame
   ```
   - Type hints enforce contracts
   - Runtime validation catches errors early
   - IDE support for autocompletion
   - Self-documenting code

3. **Materialization Strategy**
   - Assets can be materialized to different storage
   - Partitioning support for large datasets
   - Versioning of assets
   - Incremental updates

4. **Testing Infrastructure**
   - Asset functions are easily unit testable
   - Mock resources for testing
   - `dagster-dev` for local development
   - Build testing into the platform

5. **Observability**
   - Asset catalog (all assets in one place)
   - Data lineage visualization
   - Asset freshness monitoring
   - Run history per asset

### Anti-Patterns to Avoid

1. **Complexity overhead**: Steeper learning curve than task-based tools
2. **Documentation gaps**: Rapidly evolving, docs can be outdated
3. **Opinionated storage**: IO managers can be inflexible for custom needs
4. **Monolith tendency**: All assets in one repo can get unwieldy

## Key Takeaways for VibePiper Design

1. **Embrace Asset-Centricity**
   - VibePiper's current `@asset` decorator is on the right track
   - Double down on assets as first-class citizens
   - Asset graph should be the core abstraction

2. **Automatic Dependency Inference**
   - Dagster infers dependencies from function signatures
   - VibePiper already does this! Keep and enhance
   - Allow explicit override when needed

3. **Separate Computation from Storage**
   - IO manager pattern is powerful
   - Let users plug in storage backends
   - Don't couple pipeline logic to storage

4. **Type System Integration**
   - Strong typing is a differentiator
   - Runtime validation catches bugs
   - Schema-first approach (VibePiper already has this!)

5. **Testing as First-Class**
   - Make assets easy to test
   - Provide testing utilities
   - Mock-friendly design

## Comparison with VibePiper (Current State)

### What VibePiper Already Does Well
- ✅ Asset decorator pattern
- ✅ Automatic dependency inference
- ✅ Declarative schema definitions
- ✅ Type system with SchemaField, DataType

### What VibePiper Could Learn from Dagster
- ❌ IO managers for storage abstraction
- ❌ Asset materialization strategies
- ❌ Resource system for external dependencies
- ❌ Asset catalog/lineage visualization
- ❌ Partitioning support
- ❌ Asset versioning
- ❌ Richer metadata on assets

### VibePiper Differentiation Opportunities
- 🚀 **Simpler mental model**: Dagster is complex; VibePiper can be simpler
- 🚀 **Built-in transformation framework**: Dagster focuses on orchestration, not transformations
- 🚀 **Data quality as first-class**: Dagster has validation, but not core to design
- 🚀 **Pythonic all the way**: Dagster has its own config language; VibePiper could stay pure Python

## Integration Ideas for VibePiper

1. **Enhanced Asset Decorator**
   ```python
   @asset(
       output=IOManager("s3://my-bucket/{name}"),
       partition_by="date",
       version="v1"
   )
   def my_asset(raw_data):
       pass
   ```

2. **Asset Graph Visualization**
   - Generate Mermaid diagrams from asset graph
   - HTML-based lineage explorer
   - Integration with DAG viz tools

3. **Resource System**
   ```python
   @asset(resources={"db": Database})
   def load_data(db: Database):
       return db.query("SELECT * FROM users")
   ```

4. **Materialization Strategies**
   - Incremental vs. full refresh
   - Partition-based materialization
   - Checkpointing for long-running jobs

## References

- https://dagster.io/
- https://dagster.io/learn/data-orchestration
- https://github.com/dagster-io/dagster
