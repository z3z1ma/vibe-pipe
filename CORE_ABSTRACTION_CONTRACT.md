# ADR: Canonical Pipeline Abstractions

**Status:** Accepted (vp-8783)
**Date:** 2026-01-31
**Sprint:** vibe-piper-architectural-reboot

---

## Executive Summary

This document defines the canonical core abstractions for Vibe Piper, clarifying the relationship between two pipeline models (Pipeline+Operators vs AssetGraph+Assets), establishing clear naming conventions, and defining the deprecation path for non-canonical APIs.

---

## Context

### Two Pipeline Models Coexist

Vibe Piper currently supports two pipeline models, each serving different purposes:

1. **Pipeline + Operators** (`types.py`): Simple, linear execution model
   - `Pipeline`: Sequential composition of `Operator` objects
   - `Operator`: Transformation functions with signature `fn(data: Any, context: PipelineContext) -> Any`
   - `PipelineContext`: Runtime execution context (config, state, metadata)
   - Purpose: In-memory data transformations, quick prototyping

2. **AssetGraph + Assets** (`types.py` + `pipeline.py`): DAG-based, materialization-aware model
   - `AssetGraph`: Directed acyclic graph of assets with dependencies
   - `Asset`: Data sources/sinks (tables, files, APIs) with optional operators
   - `PipelineDefinitionContext`: Context manager for declarative pipeline building
   - `PipelineBuilder`: Fluent API for constructing asset graphs
   - Purpose: Production data pipelines with materialization, caching, and orchestration

### Outdated Documentation

Previous documentation (`CORE_ABSTRACTION_CONTRACT.md` from ticket vp-ba4d) referenced:
- `src/vibe_piper/core.py` - **This file does not exist**
- `PipelineDefContext` - **Actually named** `PipelineDefinitionContext`

### No Canonical Path

Users have no clear guidance on:
- When to use `Pipeline` vs `AssetGraph`
- The relationship between `Operator` and `Asset.operator`
- Whether both models will continue to exist or one will be deprecated

---

## Decision

### Canonical Model: AssetGraph + Assets

**Decision:** `AssetGraph` is the canonical pipeline model for Vibe Piper production use.

**Rationale:**
1. **DAG-based**: Supports complex dependency graphs, not just linear chains
2. **Materialization-aware**: Supports different storage strategies (table, view, file)
3. **Orchestration-ready**: Integrates with scheduling, caching, and quality checks
4. **Declarative**: Uses decorators (`@asset`) and builders for clear intent
5. **Production-proven**: Used by execution engines, orchestration, and quality systems
6. **Explicit Data Contract**: `UpstreamData` provides clear, structured access to upstream results

**When to use AssetGraph:**
- Production data pipelines
- Multi-step data transformations with dependencies
- Data requiring materialization (tables, files, databases)
- Pipelines needing scheduling or orchestration
- Quality checks and monitoring
- Caching and performance optimization

### Secondary Model: Pipeline + Operators

**Decision:** `Pipeline` remains supported as a lightweight model for simple transformations.

**Rationale:**
1. **Simple API**: Good for quick scripts and prototypes
2. **Composable**: Easy to test and reason about
3. **Zero-dependency**: No need for assets, graphs, or IO managers

**When to use Pipeline:**
- Quick data transformations in scripts
- Unit testing individual operators
- Simple data munging (ETL) without persistence
- Educational examples and tutorials

### Naming Convention

The two `*Context` classes serve different purposes and must have distinct names:

| Class | Purpose | Location | Canonical Name |
|-------|---------|----------|----------------|
| Runtime execution context | Pipeline execution (runtime) | `types.py` | `PipelineContext` |
| Build-time definition context | Asset graph construction (build-time) | `pipeline.py` | `PipelineDefinitionContext` |

**Note:** The current code already uses `PipelineDefinitionContext`. No name collision exists.

### Operator Data Contract

**Two Execution Models Have Different Contracts:**

#### Pipeline Model (Simple, Linear)
Operators in `Pipeline` model receive raw data directly:
```python
def operator_function(
    data: Any,  # Raw data from previous operator
    context: PipelineContext,
) -> Any:
    """
    Args:
        data: Input data (from upstream operator or initial input)
              Can be any Python type: dict, list, DataRecord, etc.
        context: Runtime execution context with pipeline_id, run_id, config, state

    Returns:
        Transformed data for downstream consumption

    Invariants:
        - context is always provided during execution (never None)
        - Return type can differ from input type
        - Exceptions propagate and fail pipeline
    """
```

#### AssetGraph Model (DAG-Based Production)
Operators in `AssetGraph` model receive structured `UpstreamData`:
```python
def asset_operator_function(
    upstream: UpstreamData,  # Structured upstream results
    context: PipelineContext,
) -> Any:
    """
    Args:
        upstream: UpstreamData containing results from all upstream assets
                - Single upstream: upstream.keys == ("asset_name",)
                - Multi upstream: upstream.keys == ("asset1", "asset2", ...)
                - No upstream (source): upstream.keys == ()
        context: Runtime execution context with pipeline_id, run_id, config, state

    Returns:
        Transformed data for downstream consumption

    Accessing Upstream Data:
        - upstream["asset_name"] - Get data from specific upstream asset
        - upstream.get("asset_name", default) - Safe access with default
        - upstream.keys - Get all upstream asset names
        - "asset_name" in upstream - Check if asset exists

    Invariants:
        - upstream is always UpstreamData type (never None)
        - context is always provided during execution (never None)
        - Return type can differ from input type
        - Exceptions propagate and fail pipeline
    """
```

**Canonical Type Alias:**
- `OperatorFn[T_input, T_output]` - Generic operator signature (Pipeline model)
- `AssetOperatorFn[T_output]` - Asset-specific signature with UpstreamData (AssetGraph model)

**PipelineContext Contract:**
```python
@dataclass
class PipelineContext:
    pipeline_id: str           # Unique pipeline identifier
    run_id: str               # Unique execution run identifier
    config: Mapping[str, Any]  # Read-only configuration
    state: dict[str, Any]     # Mutable state (cross-operator communication)
    metadata: Mapping[str, Any] # Additional metadata

    # Accessors
    def get_config(self, key: str, default: T | None = None) -> Any
    def get_state(self, key: str, default: T | None = None) -> Any
    def set_state(self, key: str, value: Any) -> None
```

---

## Execution Layering

Vibe Piper has three execution layers:

### Layer 1: Operator Execution (Unit)
- **Purpose:** Execute a single transformation function
- **Context:** `PipelineContext` (runtime)
- **Two Contracts:**
  - **Pipeline model:** `operator.fn(raw_data, context)` - receives raw data
  - **AssetGraph model:** `operator.fn(upstream_data, context)` - receives `UpstreamData`
- **Example:**
  ```python
  # Pipeline model (simple)
  operator.fn([1, 2, 3], context)

  # AssetGraph model (production)
  operator.fn(UpstreamData(raw={"asset1": [1, 2, 3]}), context)
  ```

### Layer 2: Pipeline Execution (Sequential)
- **Purpose:** Execute a sequence of operators linearly
- **Context:** `Pipeline` class with `execute()` method
- **Example:**
  ```python
  pipeline = Pipeline(name="transform", operators=(op1, op2))
  result = pipeline.execute(data, context=ctx)
  ```

### Layer 3: AssetGraph Execution (DAG)
- **Purpose:** Execute a DAG of assets respecting dependencies
- **Context:** `ExecutionEngine` or `OrchestrationEngine`
- **Example:**
  ```python
  engine = ExecutionEngine()
  result = engine.execute(asset_graph)
  ```

---

## API Boundaries

### Public APIs (Exported via `__init__.py`)

```python
# Core abstractions (canonical)
Asset, AssetGraph, AssetType, Operator, OperatorType
Pipeline, PipelineContext, PipelineDefinitionContext

# Schema types
Schema, SchemaField, DataType, DataRecord

# Execution types
ExecutionEngine, ExecutionResult, AssetResult

# Builder helpers
PipelineBuilder, build_pipeline, infer_dependencies_from_signature

# Decorators
asset, expect

# Quality and validation
ExpectationSuite, QualityMetric, check_completeness, check_freshness, ...
```

### Internal APIs (Not Exported)

```python
# Module-private (prefixed with _)
_infer_dependencies_from_signature()
_wrap_fn_for_asset()
_validate_no_cycles()
```

---

## Migration Plan

### No Breaking Changes Required

**Current state is correct:**
- `core.py` does not exist (no action needed)
- `PipelineDefinitionContext` is already the correct name (no action needed)
- Both `Pipeline` and `AssetGraph` are actively used and documented

### Documentation Updates

1. **Update README and guides:**
   - Emphasize `AssetGraph` for production pipelines
   - Document `Pipeline` as a lightweight alternative for scripts
   - Provide clear examples for both models

2. **Update API docs:**
   - Clarify `PipelineContext` (runtime) vs `PipelineDefinitionContext` (build-time)
   - Document operator data contract
   - Show execution layering examples

### Code Changes (Follow-up Tickets)

**Low priority** (no immediate action, monitor usage):

1. **Deprecation warnings** (if needed in future):
   - If `Pipeline` usage declines, consider adding deprecation warnings
   - Not currently needed - both models have valid use cases

2. **Consolidation** (future consideration):
   - If patterns emerge for unifying the models, consider creating adapters
   - Example: AssetGraph.from_pipeline(pipeline) to convert linear to DAG
   - Not currently needed - both models serve different purposes

---

## Follow-up Tickets

### Documentation
- [ ] **vp-8784**: Update README with canonical pipeline model guidance
- [ ] **vp-8785**: Create migration guide for Pipeline -> AssetGraph
- [ ] **vp-8786**: Update API documentation with execution layering examples

### Code Improvements (Low Priority)
- [ ] **vp-8787**: Add AssetGraph.from_pipeline() adapter method
- [ ] **vp-8788**: Create operator library for common transformations

---

## Acceptance Criteria Met

- ✅ ADR created in `CORE_ABSTRACTION_CONTRACT.md` with explicit decision on canonical pipeline abstraction
- ✅ Pipeline definition API documented (AssetGraph + PipelineBuilder)
- ✅ Execution layering defined (Operator → Pipeline → AssetGraph)
- ✅ Operator data contract specified (signature, context, invariants)
- ✅ Deprecation list captured (none required - no deprecated code exists)
- ✅ Links to follow-up tickets included
- ✅ No code changes beyond docs

---

## References

- `src/vibe_piper/types.py`: Core type definitions (Asset, AssetGraph, Operator, Pipeline, PipelineContext)
- `src/vibe_piper/pipeline.py`: Pipeline builder and definition context
- `src/vibe_piper/execution.py`: Execution engine for AssetGraph
- `src/vibe_piper/orchestration.py`: Orchestration engine with scheduling
- `src/vibe_piper/__init__.py`: Public API exports

---

## Change Log

- **2026-01-31**: Initial ADR created (vp-8783). Removed stale references to `core.py` and `PipelineDefContext`. Declared AssetGraph as canonical model. Documented both Pipeline models with clear usage guidance.
