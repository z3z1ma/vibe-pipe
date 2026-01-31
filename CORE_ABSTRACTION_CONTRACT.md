# Core Abstraction Contract for Vibe Piper

**Status:** Draft (vp-ba4d)
**Date:** 2025-01-31
**Sprint:** Cohesive Core Abstractions

---

## Executive Summary

This document defines the canonical core abstractions for Vibe Piper, identifying duplicates, overlaps, and establishing clear public vs internal contracts. This contract serves as the foundation for all subsequent refactoring work.

---

## 1. Identified Duplicates and Overlaps

### 1.1 Pipeline (CRITICAL DUPLICATE)

**Location A:** `src/vibe_piper/core.py` (lines 16-124)
```python
@dataclass
class Pipeline(Generic[T]):
    """A declarative data pipeline composed of multiple stages."""
    name: str
    stages: list[Stage]
    description: str = ""
    def add_stage(self, stage: Stage) -> None
    def run(self, data: T) -> Any
```

**Location B:** `src/vibe_piper/types.py` (lines 371-497)
```python
@dataclass(frozen=True)
class Pipeline:
    """A data pipeline composed of operators."""
    name: str
    operators: tuple[Operator, ...]
    input_schema: Schema | None
    output_schema: Schema | None
    description: str | None = None
    metadata: Mapping[str, Any]
    config: Mapping[str, Any]
    checkpoints: tuple[str, ...]
    def add_operator(self, operator: Operator) -> Pipeline
    def execute(self, data: Any, context: PipelineContext | None) -> Any
```

**Analysis:**
- **Location A**: Simple, mutable, uses `Stage` abstraction
- **Location B**: Sophisticated, immutable, uses `Operator` abstraction, supports schemas, checkpoints
- **Export Status**: `vibe_piper/__init__.py` exports Pipeline from types.py (Location B)
- **Usage Status**: Location B is the canonical version; Location A appears to be legacy

**Recommendation**: **Deprecate Location A** (`src/vibe_piper/core.py` Pipeline), standardize on Location B.

---

### 1.2 PipelineContext (NAME COLLISION - CRITICAL)

**Location A:** `src/vibe_piper/types.py` (lines 243-277)
```python
@dataclass
class PipelineContext:
    """Execution context for pipeline operations."""
    pipeline_id: str
    run_id: str
    config: Mapping[str, Any]
    state: dict[str, Any]  # Mutable!
    metadata: Mapping[str, Any]
    def get_config(self, key: str, default: T | None = None) -> Any
    def get_state(self, key: str, default: T | None = None) -> Any
    def set_state(self, key: str, value: Any) -> None
```

**Location B:** `src/vibe_piper/pipeline.py` (lines 319-516)
```python
class PipelineContext:
    """Context manager for defining pipelines with nested assets."""
    def __init__(self, name: str, description: str | None = None) -> None
    def __enter__(self) -> PipelineContext
    def __exit__(self, exc_type, exc_val, exc_tb) -> None
    def asset(self, fn, *, name, depends_on, ...) -> Callable | Asset
    def build(self) -> AssetGraph
```

**Analysis:**
- **Location A**: Runtime execution context (passes config/state to operators)
- **Location B**: Build-time context manager (builder pattern for declarative pipelines)
- **Collision**: Both are imported and exported with the same name via `__init__.py` (lines 36, 436):
  ```python
  from vibe_piper.types import PipelineContext
  from vibe_piper.pipeline import PipelineContext as PipelineDefContext  # Workaround used!
  ```
- **Impact**: Users must use aliases to avoid collision; confusing documentation

**Recommendation**: **Rename Location B** to `PipelineDefinitionContext` or `AssetGraphBuilder`, deprecate collision.

---

### 1.3 Stage vs Operator (CONCEPTUAL OVERLAP)

**Location A:** `src/vibe_piper/core.py` (lines 16-57)
```python
@dataclass
class Stage(Generic[T, R]):
    """A single stage in a data pipeline."""
    name: str
    transform: Callable[[T], R]  # No context!
    description: str = ""
    def __call__(self, data: T) -> R
```

**Location B:** `src/vibe_piper/types.py` (lines 278-313)
```python
@dataclass(frozen=True)
class Operator:
    """A transformation operation in a pipeline."""
    name: str
    operator_type: OperatorType
    fn: OperatorFn[Any, Any]  # Includes context!
    input_schema: Schema | None
    output_schema: Schema | None
    description: str | None
    config: Mapping[str, Any]
```

**Analysis:**
- **Location A**: Simple, no context support, mutable
- **Location B**: Rich, context-aware, immutable, typed (OperatorType enum)
- **Export Status**: `vibe_piper/__init__.py` does NOT export Stage (Location A)
- **Usage Status**: Operators (Location B) are canonical; Stage (Location A) appears to be unused

**Recommendation**: **Deprecate Stage** (Location A), standardize on Operator (Location B).

---

### 1.4 Asset (NO DUPLICATES)

**Location:** `src/vibe_piper/types.py` (lines 314-370)

**Analysis:** Only one canonical implementation. No duplicates found.

---

### 1.5 AssetGraph (NO DUPLICATES)

**Location:** `src/vibe_piper/types.py` (lines 499-878)

**Analysis:** Only one canonical implementation. No duplicates found.

---

### 1.6 PipelineBuilder, PipelineDefContext (NO DUPLICATES)

**Location:** `src/vibe_piper/pipeline.py` (lines 82-516)

**Analysis:** Builder patterns unique to declarative pipeline syntax. No duplicates.

---

## 2. Canonical Abstractions

### 2.1 Asset (Canonical: `src/vibe_piper/types.py:314-370`)

**Purpose:** Represents a data source or sink in the system (table, file, API, etc.)

**Public Contract:**
```python
@dataclass(frozen=True)
class Asset:
    name: str
    asset_type: AssetType
    uri: str
    schema: Schema | None = None
    operator: Operator | None = None
    io_manager: str = "memory"
    materialization: str | MaterializationStrategy = MaterializationStrategy.TABLE
    description: str | None = None
    metadata: Mapping[str, Any] = field(default_factory=dict)
    config: Mapping[str, Any] = field(default_factory=dict)
    version: str = "1"
    partition_key: str | None = None
    created_at: datetime | None = None
    updated_at: datetime | None = None
    checksum: str | None = None
    cache: bool = False
    cache_ttl: int | None = None
    parallel: bool = False
    lazy: bool = False
```

**Public API:**
- Constructor: `Asset(name, asset_type, uri, ...)`
- Immutable: All fields frozen; use copy/replace pattern for updates
- Validation: Name and URI must be non-empty

**Internal Implementation Details:**
- Validates name and URI in `__post_init__`

---

### 2.2 AssetGraph (Canonical: `src/vibe_piper/types.py:499-878`)

**Purpose:** Directed acyclic graph (DAG) of assets with dependencies.

**Public Contract:**
```python
@dataclass(frozen=True)
class AssetGraph:
    name: str
    assets: tuple[Asset, ...]
    dependencies: Mapping[str, tuple[str, ...]]  # asset_name -> upstream_deps
    description: str | None = None
    metadata: Mapping[str, Any] = field(default_factory=dict)
    config: Mapping[str, Any] = field(default_factory=dict)
```

**Public API:**
- Constructor: `AssetGraph(name, assets, dependencies, ...)`
- Immutable: All fields frozen; use `add_asset()` to extend
- `get_asset(name: str) -> Asset | None`
- `get_dependencies(asset_name: str) -> tuple[Asset, ...]`
- `get_dependents(asset_name: str) -> tuple[Asset, ...]`
- `get_upstream(asset_name: str, depth: int | None) -> tuple[Asset, ...]`
- `get_downstream(asset_name: str, depth: int | None) -> tuple[Asset, ...]`
- `topological_order() -> tuple[str, ...]`
- `to_mermaid() -> str` (graph visualization)
- `add_asset(asset, depends_on=()) -> AssetGraph` (returns new graph)

**Internal Implementation Details:**
- Validates no duplicate asset names
- Validates dependencies reference existing assets
- Detects and rejects circular dependencies (DFS-based)
- Uses Kahn's algorithm for topological sort

---

### 2.3 Operator (Canonical: `src/vibe_piper/types.py:278-313`)

**Purpose:** A transformation operation in a pipeline.

**Public Contract:**
```python
@dataclass(frozen=True)
class Operator:
    name: str
    operator_type: OperatorType
    fn: OperatorFn[Any, Any]  # = Callable[[T_input, PipelineContext], T_output]
    input_schema: Schema | None = None
    output_schema: Schema | None = None
    description: str | None = None
    config: Mapping[str, Any] = field(default_factory=dict)
```

**Public API:**
- Constructor: `Operator(name, operator_type, fn, ...)`
- Immutable: All fields frozen
- Function signature: `fn(data: Any, context: PipelineContext) -> Any`

**Internal Implementation Details:**
- Validates name is non-empty and valid identifier
- OperatorType enum: SOURCE, TRANSFORM, FILTER, AGGREGATE, JOIN, SINK, VALIDATE, CUSTOM

---

### 2.4 Pipeline (Canonical: `src/vibe_piper/types.py:371-497`)

**Purpose:** A data pipeline composed of operators (linear execution).

**Public Contract:**
```python
@dataclass(frozen=True)
class Pipeline:
    name: str
    operators: tuple[Operator, ...]
    input_schema: Schema | None = None
    output_schema: Schema | None = None
    description: str | None = None
    metadata: Mapping[str, Any] = field(default_factory=dict)
    config: Mapping[str, Any] = field(default_factory=dict)
    checkpoints: tuple[str, ...] = field(default_factory=tuple)
```

**Public API:**
- Constructor: `Pipeline(name, operators, ...)`
- Immutable: All fields frozen
- `add_operator(operator: Operator) -> Pipeline` (returns new pipeline)
- `execute(data: Any, context: PipelineContext | None = None) -> Any`
  - Validates input/output schemas if present
  - Creates default context if None provided
  - Executes operators sequentially

**Internal Implementation Details:**
- Validates name is non-empty
- Validates no duplicate operator names
- Supports DataRecord validation against schemas

---

### 2.5 PipelineContext (Execution) (Canonical: `src/vibe_piper/types.py:243-277`)

**Purpose:** Execution context for pipeline operations (runtime, not build-time).

**Public Contract:**
```python
@dataclass
class PipelineContext:
    pipeline_id: str
    run_id: str
    config: Mapping[str, Any] = field(default_factory=dict)
    state: dict[str, Any] = field(default_factory=dict)  # MUTABLE!
    metadata: Mapping[str, Any] = field(default_factory=dict)
```

**Public API:**
- Constructor: `PipelineContext(pipeline_id, run_id, ...)`
- Mutable state: `set_state(key, value)` and `get_state(key, default)`
- Immutable config: `get_config(key, default)`
- Used in Operator functions as second parameter

**Internal Implementation Details:**
- Config is read-only Mapping
- State is mutable dict for cross-operator communication

---

### 2.6 PipelineBuilder (Canonical: `src/vibe_piper/pipeline.py:82-254`)

**Purpose:** Builder for creating declarative pipelines (build-time, not runtime).

**Public Contract:**
```python
class PipelineBuilder:
    def __init__(self, name: str, description: str | None = None) -> None
    def asset(
        self,
        name: str,
        fn: Callable,
        *,
        depends_on: list[str] | tuple[str, ...] | None = None,
        asset_type: AssetType = AssetType.MEMORY,
        uri: str | None = None,
        description: str | None = None,
        metadata: dict[str, Any] | None = None,
        config: dict[str, Any] | None = None,
        cache: bool = False,
        cache_ttl: int | None = None,
        parallel: bool = False,
        lazy: bool = False,
    ) -> PipelineBuilder  # Returns self for chaining
    def build(self) -> AssetGraph
```

**Public API:**
- Fluent interface: `builder.asset(...).asset(...).build()`
- Automatic dependency inference from function signatures
- Validates dependency existence and circular references

**Internal Implementation Details:**
- Wraps functions to handle context parameter
- Generates URIs automatically if not provided
- Creates Operators and Assets internally

---

## 3. Execution Data Contract

### 3.1 Operator Function Signature

**Canonical Signature:**
```python
def operator_function(data: Any, context: PipelineContext) -> Any:
    """
    Args:
        data: Input data from upstream (or initial data)
        context: Execution context with pipeline_id, run_id, config, state

    Returns:
        Transformed data for downstream
    """
    pass
```

**Invariants:**
- `data` can be any Python type (dict, list, DataRecord, DataFrame, etc.)
- `context` is guaranteed to be provided during execution
- Return type can be different from input type
- Exceptions propagate and fail the pipeline execution

**Upstream Dependencies:**
- Source operators (no deps): receive `data=None` from upstream
- Transform operators: receive output from direct upstream dependency
- Multiple upstream deps: receives combined results (handled by execution engine)

---

### 3.2 PipelineContext Contract

**Guaranteed Fields:**
```python
context.pipeline_id: str  # Unique pipeline identifier
context.run_id: str      # Unique execution run identifier
```

**Accessors:**
```python
# Read-only config
config_val = context.get_config("key", default=None)

# Mutable state (for cross-operator communication)
context.set_state("key", value)
state_val = context.get_state("key", default=None)

# Direct access (not recommended)
context.config: Mapping[str, Any]  # Read-only
context.state: dict[str, Any]    # Mutable
```

**Usage Patterns:**
- Configuration: Set by caller, read by operators (batch size, retries, etc.)
- State: Set by one operator, read by downstream operators (metadata, counters, etc.)

---

## 4. Deprecation and Migration Plan

### 4.1 Deprecate `core.py` Pipeline and Stage

**Deprecated:**
- `src/vibe_piper/core.py::Pipeline` (lines 60-124)
- `src/vibe_piper/core.py::Stage` (lines 16-57)

**Migration Path:**
```python
# OLD (deprecated)
from vibe_piper.core import Pipeline, Stage

pipeline = Pipeline(name="old_pipe")
pipeline.add_stage(Stage(name="clean", transform=lambda x: x.strip()))
result = pipeline.run(data)

# NEW (canonical)
from vibe_piper import Pipeline, Operator, OperatorType, PipelineContext

op = Operator(
    name="clean",
    operator_type=OperatorType.TRANSFORM,
    fn=lambda data, ctx: data.strip()
)
pipeline = Pipeline(name="new_pipe", operators=(op,))
ctx = PipelineContext(pipeline_id="new_pipe", run_id="run_1")
result = pipeline.execute(data, context=ctx)
```

**Timeline:**
- **v0.1.0**: Mark as deprecated (warnings)
- **v0.2.0**: Remove from `__init__.py` exports (still importable)
- **v0.3.0**: Delete file entirely

---

### 4.2 Resolve PipelineContext Name Collision

**Current Workaround (in __init__.py):**
```python
from vibe_piper.types import PipelineContext  # Execution context
from vibe_piper.pipeline import PipelineContext as PipelineDefContext  # Builder
```

**Proposed Fix:**
```python
# Rename src/vibe_piper/pipeline.py::PipelineContext
class PipelineDefinitionContext:  # Or AssetGraphBuilder
    """Context manager for defining pipelines with nested assets."""
    # ... same implementation ...

# Update __init__.py
from vibe_piper.types import PipelineContext  # Execution context (unchanged)
from vibe_piper.pipeline import PipelineDefinitionContext  # Builder (renamed)
```

**Migration Path:**
```python
# OLD (workaround)
from vibe_piper import PipelineDefContext

with PipelineDefContext("pipeline") as pipeline:
    @pipeline.asset()
    def source(ctx: PipelineContext):  # Still import execution context
        return [1, 2, 3]

# NEW (canonical)
from vibe_piper import PipelineDefinitionContext

with PipelineDefinitionContext("pipeline") as pipeline:
    @pipeline.asset()
    def source(ctx: PipelineContext):
        return [1, 2, 3]
```

**Timeline:**
- **v0.2.0**: Rename in codebase, keep `PipelineDefContext` alias for compatibility
- **v0.3.0**: Remove alias, require use of new name

---

## 5. Public vs Internal APIs

### 5.1 Public APIs (Stable, Exported)

**From `vibe_piper` (via `__init__.py`):**
```python
# Core types
Asset
AssetGraph
AssetType
Operator
OperatorType
Pipeline
PipelineContext  # Execution context
PipelineDefContext  # Builder (after rename)

# Schema types
Schema
SchemaField
DataType
DataRecord

# Execution types
ExecutionEngine
ExecutionResult
AssetResult

# Builder helpers
PipelineBuilder
build_pipeline
infer_dependencies_from_signature

# Decorators
asset
expect

# Operators (built-in)
map_transform
filter_operator
aggregate_count
... (etc)
```

### 5.2 Internal APIs (Implementation Details, Not Exported)

**Module-private (not in `__init__.py`):**
```python
# src/vibe_piper/core.py (to be deprecated)
Stage  # Internal to core.py
Pipeline  # Duplicates types.py, to be removed

# src/vibe_piper/pipeline.py (private helpers)
_infer_dependencies_from_signature()  # Actually public
_wrap_fn_for_asset()  # Implementation detail

# src/vibe_piper/types.py (private helpers)
_validate_no_cycles()  # Internal to AssetGraph
```

**Guideline:**
- Public APIs are in `__init__.py`
- Module-private helpers start with `_`
- Users should not import from submodules directly unless documented

---

## 6. TDD-Focused Test Plan for Downstream Refactors

### 6.1 Unit Tests (Per Abstraction)

**Asset Tests:**
```python
def test_asset_creation_validates_name():
    with pytest.raises(ValueError, match="cannot be empty"):
        Asset(name="", asset_type=AssetType.TABLE, uri="...")

def test_asset_creation_validates_uri():
    with pytest.raises(ValueError, match="cannot be empty"):
        Asset(name="test", asset_type=AssetType.TABLE, uri="")

def test_asset_fields_accessible():
    asset = Asset(name="test", asset_type=AssetType.MEMORY, uri="memory://test")
    assert asset.name == "test"
    assert asset.version == "1"
    assert asset.cache is False
```

**AssetGraph Tests:**
```python
def test_asset_graph_validates_no_duplicate_names():
    a1 = Asset(name="dup", ...)
    a2 = Asset(name="dup", ...)
    with pytest.raises(ValueError, match="Duplicate asset names"):
        AssetGraph(name="test", assets=(a1, a2))

def test_asset_graph_validates_no_circular_deps():
    a = Asset(name="a", ...)
    b = Asset(name="b", ...)
    with pytest.raises(ValueError, match="Circular dependency"):
        AssetGraph(name="test", assets=(a, b), dependencies={"a": ("b",), "b": ("a",)})

def test_topological_order_simple_chain():
    # a -> b -> c
    graph = AssetGraph(...)
    order = graph.topological_order()
    assert order == ("a", "b", "c")
```

**Operator Tests:**
```python
def test_operator_validates_name():
    with pytest.raises(ValueError, match="cannot be empty"):
        Operator(name="", operator_type=OperatorType.TRANSFORM, fn=lambda d, c: d)

def test_operator_fn_receives_context():
    called = []
    def test_fn(data, ctx):
        called.append(ctx.pipeline_id)
        return data

    op = Operator(name="test", operator_type=OperatorType.TRANSFORM, fn=test_fn)
    ctx = PipelineContext(pipeline_id="pipe", run_id="run_1")
    op.fn([1, 2, 3], ctx)
    assert called == ["pipe"]
```

**Pipeline Tests:**
```python
def test_pipeline_validates_no_duplicate_operator_names():
    op1 = Operator(name="dup", ...)
    op2 = Operator(name="dup", ...)
    with pytest.raises(ValueError, match="Duplicate operator names"):
        Pipeline(name="test", operators=(op1, op2))

def test_pipeline_execute_creates_default_context():
    op = Operator(name="test", operator_type=OperatorType.TRANSFORM, fn=lambda d, c: d)
    pipeline = Pipeline(name="test", operators=(op,))
    result = pipeline.execute([1, 2, 3])  # No context provided
    # Should succeed with default context

def test_pipeline_validate_input_schema():
    schema = Schema(name="test", fields=(SchemaField(name="id", data_type=DataType.INTEGER),))
    op = Operator(name="test", operator_type=OperatorType.TRANSFORM, fn=lambda d, c: d)
    pipeline = Pipeline(name="test", operators=(op,), input_schema=schema)

    # Invalid data
    with pytest.raises(ValueError, match="Input data validation failed"):
        pipeline.execute({"invalid": "data"})

    # Valid data
    result = pipeline.execute({"id": 42})
    assert result == {"id": 42}
```

**PipelineContext Tests:**
```python
def test_context_state_mutability():
    ctx = PipelineContext(pipeline_id="test", run_id="run_1")
    ctx.set_state("counter", 42)
    assert ctx.get_state("counter") == 42

def test_context_config_read_only():
    config = {"key": "value"}
    ctx = PipelineContext(pipeline_id="test", run_id="run_1", config=config)
    assert ctx.get_config("key") == "value"
    # Config is Mapping (read-only), not dict
```

---

### 6.2 Integration Tests (Cross-Abstraction)

**End-to-End Pipeline Tests:**
```python
def test_build_and_execute_pipeline():
    # Build with builder
    builder = PipelineBuilder("test")
    builder.asset(name="source", fn=lambda ctx: [1, 2, 3])
    builder.asset(name="derived", fn=lambda d, ctx: [x * 2 for x in d], depends_on=["source"])
    graph = builder.build()

    # Execute with engine
    engine = ExecutionEngine()
    result = engine.execute(graph)

    assert result.success is True
    assert result.assets_executed == 2
    assert result.asset_results["derived"].data == [2, 4, 6]
```

**Schema Validation Integration:**
```python
def test_pipeline_with_schema_validation():
    schema = Schema(name="user", fields=(SchemaField(name="id", data_type=DataType.INTEGER),))
    op = Operator(name="test", operator_type=OperatorType.TRANSFORM, fn=lambda d, c: d, input_schema=schema, output_schema=schema)
    pipeline = Pipeline(name="test", operators=(op,))

    ctx = PipelineContext(pipeline_id="test", run_id="run_1")
    result = pipeline.execute({"id": 42}, context=ctx)
    assert result == {"id": 42}

    with pytest.raises(ValueError):
        pipeline.execute({"invalid": "data"}, context=ctx)
```

**Dependency Resolution Integration:**
```python
def test_dependency_inference():
    builder = PipelineBuilder("test")

    @builder.asset(name="source")
    def source_fn(ctx: PipelineContext) -> list[int]:
        return [1, 2, 3]

    @builder.asset(name="derived")  # deps inferred from param name
    def derived_fn(source: list[int], ctx: PipelineContext) -> list[int]:
        return [x * 2 for x in source]

    graph = builder.build()
    assert graph.dependencies == {"derived": ("source",)}
```

---

### 6.3 Migration Tests (For Deprecation)

**Deprecation Warning Tests:**
```python
def test_deprecated_pipeline_import_warns():
    with pytest.warns(DeprecationWarning):
        from vibe_piper.core import Pipeline  # Will emit warning

def test_deprecated_stage_import_warns():
    with pytest.warns(DeprecationWarning):
        from vibe_piper.core import Stage
```

**Migration Path Tests:**
```python
def test_old_to_new_pipeline_equivalence():
    # Old API
    from vibe_piper import Pipeline as OldPipeline, Operator

    # Build equivalent with new API
    # ... (test that behavior is identical)
```

---

## 7. Open Questions

1. **Execution Engine Contract**: Should `ExecutionEngine` be part of this contract? Currently in `execution.py`.
2. **Materialization Strategy**: Should `MaterializationStrategy` enum be merged with `AssetType` or kept separate?
3. **IO Managers**: Should IO manager protocol be part of core abstraction contract or a separate concern?
4. **Context State Mutation**: Should `PipelineContext.state` be mutable? Current design allows cross-operator communication via mutation.
5. **Pipeline vs AssetGraph**: Should we consolidate these? Linear `Pipeline` vs DAG `AssetGraph` serve different purposes.

---

## 8. References

- `src/vibe_piper/core.py` (124 lines) - Contains deprecated Pipeline, Stage
- `src/vibe_piper/types.py` (1387 lines) - Contains canonical core types
- `src/vibe_piper/pipeline.py` (545 lines) - Contains builder patterns
- `src/vibe_piper/decorators.py` (325 lines) - Contains @asset, @expect decorators
- `src/vibe_piper/__init__.py` (610 lines) - Public API exports
- Test files referenced for usage patterns:
  - `tests/test_pipeline.py` (732 lines) - Pipeline builder/context tests
  - `tests/test_types.py` (1057 lines) - Core type tests
  - `tests/test_operators.py` (476 lines) - Operator tests

---

**Next Steps:**
1. Review and approve this contract
2. Create follow-up tickets for:
   - Deprecate `core.py` Pipeline and Stage
   - Rename `PipelineContext` (builder) to `PipelineDefinitionContext`
   - Update documentation and examples
   - Add deprecation warnings
