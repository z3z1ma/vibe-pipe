# Public API Curation Analysis

## Current State

**Total exports in `__all__`:** 224 items
**Total non-private dir() exports:** 234 items

## Goal

Define a focused public API surface based on the ADR (CORE_ABSTRACTION_CONTRACT.md) and add explicit `__all__` with proper categorization.

---

## API Categorization

### 1. CORE PUBLIC API (Stable, Documented in ADR)

These are the canonical abstractions defined in the ADR and should always be available.

**Core Abstractions (Canonical)**
- `Asset` - Asset type for AssetGraph
- `AssetGraph` - Canonical pipeline model for production
- `AssetType` - Enum of asset types
- `Operator` - Transformation function wrapper
- `OperatorType` - Enum of operator types
- `Pipeline` - Lightweight pipeline model for simple transformations
- `PipelineContext` - Runtime execution context
- `PipelineDefinitionContext` - Build-time context for declarative pipelines

**Schema Types**
- `Schema` - Schema definition
- `SchemaField` - Individual schema field
- `DataType` - Type system for schema fields
- `DataRecord` - Data container with schema validation

**Execution Types**
- `ExecutionEngine` - AssetGraph execution engine
- `ExecutionResult` - Result from pipeline execution
- `AssetResult` - Result from single asset execution
- `DefaultExecutor` - Default execution engine implementation
- `ErrorStrategy` - Strategy for handling errors
- `calculate_checksum` - Utility for data fingerprinting

**Builder Helpers**
- `PipelineBuilder` - Fluent API for building asset graphs
- `build_pipeline` - Helper function for pipeline construction
- `infer_dependencies_from_signature` - Auto-infer upstream dependencies

**Decorators**
- `asset` - Decorator for defining assets
- `expect` - Decorator for expectations

**Quality and Validation**
- `ExpectationSuite` - Collection of expectations
- `ExpectationLibrary` - Library of built-in expectations
- `SuiteResult` - Result from expectation suite execution
- `FailureStrategy` - Strategy for handling expectation failures
- `QualityMetric` - Quality metric type
- `QualityMetricType` - Enum of metric types
- `QualityCheckResult` - Result from quality check
- `DataQualityReport` - Aggregate quality report
- `ValidationResult` - Result from validation

**Quality Check Functions**
- `check_completeness` - Check data completeness
- `check_freshness` - Check data freshness
- `check_uniqueness` - Check data uniqueness
- `check_validity` - Check data validity
- `generate_quality_report` - Generate quality report

**Declarative Schema API**
- `define_schema` - Function for declarative schema definition
- `String`, `Integer`, `Float`, `Boolean`, `DateTime`, `Date` - Type constructors
- `Array`, `Object`, `AnyType` - Complex type constructors
- `DeclarativeSchema` - Schema from declarative definition

---

### 2. POWER USER / ADVANCED API (Stable but specialized)

These are stable APIs that advanced users may need, but are not part of the core canonical abstraction.

**Operators**
- `map_transform` - Transform data with function
- `map_field` - Transform specific field
- `add_field` - Add new field to data
- `filter_operator` - Filter data
- `filter_field_equals` - Filter by field equality
- `filter_field_not_null` - Filter for non-null fields
- `aggregate_count` - Count aggregation
- `aggregate_sum` - Sum aggregation
- `aggregate_group_by` - Group by aggregation
- `validate_schema` - Validate schema
- `validate_expectation` - Validate single expectation
- `validate_expectation_suite` - Validate expectation suite
- `custom_operator` - Create custom operator

**Built-in Expectations**
- `expect_column_to_exist`
- `expect_column_to_not_exist`
- `expect_column_type_to_be`
- `expect_table_column_count_to_equal`
- `expect_table_column_count_to_be_between`
- `expect_table_columns_to_match_set`
- `expect_table_columns_to_contain`
- `expect_table_columns_to_not_contain`
- `expect_column_to_be_required`
- `expect_column_to_be_optional`
- `expect_column_to_be_nullable`
- `expect_column_to_be_non_nullable`
- `expect_column_to_have_constraint`
- `expect_column_constraint_to_equal`

**Materialization Strategies**
- `MaterializationStrategyBase` - Base class for materialization
- `TableStrategy` - Materialize as table
- `ViewStrategy` - Materialize as view
- `FileStrategy` - Materialize as file
- `IncrementalStrategy` - Incremental materialization

**Expectation Helpers**
- `compose_expectations` - Compose multiple expectations
- `create_parameterized_expectation` - Create parameterized expectation

---

### 3. OPTIONAL FEATURE APIs (Feature flags, may be None)

These are APIs that depend on optional dependencies and may not be available.

**SQL Assets (Optional)**
- `sql_asset` - Decorator for SQL assets (may be None)
- `execute_sql_query` - Execute SQL query
- `extract_asset_dependencies` - Extract dependencies from SQL
- `render_sql_template` - Render SQL template
- `validate_sql` - Validate SQL syntax
- `SQLOperator` - SQL operator
- `SQLValidationResult` - SQL validation result

**Transformations (Optional)**
- `Join`, `JoinType` - Join operations
- `GroupBy` - Group by operation
- `Sum`, `Count`, `Avg`, `Min`, `Max` - Aggregations
- `Rollup`, `Cube` - OLAP operations
- `Window`, `window_function` - Window functions
- `Pivot`, `Unpivot` - Pivot operations
- `TransformationBuilder` - Builder for transformations
- `transform` - Transform data

**Schema Evolution (Optional)**
- `SemanticVersion` - Semantic versioning
- `SchemaChange` - Schema change type
- `SchemaDiff` - Schema diff result
- `MigrationStep` - Migration step
- `MigrationPlan` - Migration plan
- `SchemaHistoryEntry` - History entry
- `VersionedSchema` - Versioned schema
- `SchemaHistory` - Schema history
- `ChangeType` - Change type enum
- `BreakingChangeSeverity` - Severity enum
- `BreakingChangeDetector` - Breaking change detector
- `MigrationPlanner` - Migration planner
- `BackwardCompatibilityChecker` - Compatibility checker
- `schema_version` - Decorator for schema versioning
- `get_schema_history` - Get schema history
- `reset_schema_history` - Reset schema history

**Integration/API (Optional)**
- `APIClient` - API client base
- `APIError` - API error
- `AuthenticationError` - Authentication error (may be None)
- `BearerTokenAuth` - Bearer token auth
- `RESTClient` - REST client
- `RESTResponse` - REST response
- `GraphQLClient` - GraphQL client
- `GraphQLResponse` - GraphQL response
- `WebhookHandler` - Webhook handler
- `WebhookRequest` - Webhook request
- `CursorPagination`, `OffsetPagination`, `LinkHeaderPagination` - Pagination types
- `RateLimitError` - Rate limit error
- `validate_and_parse` - Validate and parse response
- `validate_response` - Validate response

**Database Connectors (Optional)**
- `DatabaseConnector` - Base connector
- `QueryBuilder` - Query builder
- `PostgreSQLConnector` - PostgreSQL connector
- `MySQLConnector` - MySQL connector
- `SnowflakeConnector` - Snowflake connector
- `BigQueryConnector` - BigQuery connector

**External Quality Tools (Optional)**
- `QualityToolAdapter` - Quality tool adapter (may be None)
- `QualityToolResult` - Quality tool result (may be None)
- `ToolType` - Tool type enum (may be None)
- `ge_asset` - Great Expectations asset (may be None)
- `GreatExpectationsAdapter` - GE adapter (may be None)
- `soda_asset` - Soda asset (may be None)
- `SodaAdapter` - Soda adapter (may be None)
- `merge_quality_results` - Merge results (may be None)
- `generate_unified_report` - Generate report (may be None)
- `display_quality_dashboard` - Display dashboard (may be None)

**File I/O (Optional)**
- `FileReader`, `FileWriter` - File I/O base
- `CSVReader`, `CSVWriter` - CSV I/O
- `JSONReader`, `JSONWriter` - JSON I/O
- `ParquetReader`, `ParquetWriter` - Parquet I/O
- `ExcelReader`, `ExcelWriter` - Excel I/O
- `infer_schema_from_file` - Infer schema from file

**Orchestration (Optional)**
- `ExecutionState` - Execution state
- `OrchestrationConfig` - Orchestration config
- `OrchestrationEngine` - Orchestration engine
- `ParallelExecutor` - Parallel executor
- `StateManager` - State manager

**Scheduling (Optional)**
- `BackfillConfig`, `BackfillManager`, `BackfillStatus`, `BackfillTask` - Backfill
- `CronSchedule`, `IntervalSchedule`, `Schedule` - Schedules
- `EventTrigger`, `TriggerEvent` - Triggers
- `ScheduleEvent`, `ScheduleStatus`, `ScheduleStore`, `ScheduleType` - Scheduling
- `Scheduler`, `SchedulerConfig` - Scheduler
- `TriggerType` - Trigger types

**Monitoring & Performance (Optional)**
- `ErrorAggregator`, `ErrorCategory`, `ErrorRecord`, `ErrorSeverity` - Error monitoring
- `HealthChecker`, `HealthStatus` - Health checks
- `LogLevel` - Logging levels
- `MetricsCollector`, `MetricsSnapshot`, `MetricType` - Metrics
- `Profiler` - Profiling
- `StructuredLogger` - Structured logging
- `configure_logging`, `get_logger`, `log_execution` - Logging functions
- `profile_execution` - Profiling function

**Error Handling (Optional)**
- `BackoffStrategy` - Backoff strategy
- `Checkpoint`, `CheckpointManager`, `CheckpointState` - Checkpoints
- `CircuitBreaker`, `CircuitBreakerConfig`, `CircuitBreakerError` - Circuit breaker
- `CircuitState` - Circuit state
- `DeadLetterItem`, `DeadLetterQueue` - Dead letter queue
- `ErrorContext` - Error context
- `JitterStrategy` - Jitter strategy
- `RetryConfig`, `RetryMetrics` - Retry config
- `capture_error_context` - Capture error context
- `retry_with_backoff` - Retry with backoff

**Caching & Lazy Evaluation (Optional)**
- `CacheBackend`, `CacheEntry`, `CacheKey`, `CacheManager` - Caching
- `DiskCacheBackend`, `MemoryCacheBackend` - Cache backends
- `cached` - Cached decorator
- `LazyContext`, `LazySequence`, `LazyTransform`, `LazyValue` - Lazy types
- `is_lazy` - Check if lazy
- `lazy`, `lazy_filter`, `lazy_map`, `lazy_reduce`, `lazy_transform` - Lazy operations
- `materialize` - Materialize lazy value

**Query Hints (Optional)**
- `IndexHint`, `IndexHintType`, `JoinHint`, `JoinStrategy` - Hints
- `LimitHint`, `MaterializeHint`, `ParallelHint`, `ScanHint`, `ScanHintType` - Hints
- `QueryHints`, `QueryHintsBuilder` - Query hints
- `with_query_hints` - Apply query hints

**Benchmarking (Optional)**
- `BenchmarkResult`, `BenchmarkRunner`, `BenchmarkStats` - Benchmarking
- `ComparisonResult`, `benchmark`, `compare_benchmarks` - Benchmarks

**Quality Check Operators (Optional)**
- `check_quality_completeness` - Quality completeness check
- `check_quality_validity` - Quality validity check
- `check_quality_uniqueness` - Quality uniqueness check
- `check_quality_freshness` - Quality freshness check

---

### 4. INTERNAL APIs (Should be removed from `__all__`)

These should not be exported in the public API:
- `_TRANSFORMATIONS_AVAILABLE` - Internal flag
- `_SCHEMA_EVOLUTION_AVAILABLE` - Internal flag
- `_integration_available` - Internal flag
- `_CONNECTORS_AVAILABLE` - Internal flag
- `_SQL_ASSETS_AVAILABLE` - Internal flag
- `_EXTERNAL_QUALITY_AVAILABLE` - Internal flag
- `_sql_asset_instance` - Internal instance

---

## Proposed Public API Structure

### `__all__` Categories

Instead of one giant flat list, we should structure `__all__` with clear categorization:

```python
__all__ = [
    # Version
    "__version__",

    # === CORE PUBLIC API (Always Available) ===

    # Core Abstractions
    "Asset",
    "AssetGraph",
    "AssetType",
    "Operator",
    "OperatorType",
    "Pipeline",
    "PipelineContext",
    "PipelineDefinitionContext",

    # Schema Types
    "Schema",
    "SchemaField",
    "DataType",
    "DataRecord",

    # Execution
    "ExecutionEngine",
    "ExecutionResult",
    "AssetResult",
    "DefaultExecutor",
    "ErrorStrategy",
    "calculate_checksum",

    # Builders
    "PipelineBuilder",
    "build_pipeline",
    "infer_dependencies_from_signature",

    # Decorators
    "asset",
    "expect",

    # Quality & Validation
    "ExpectationSuite",
    "ExpectationLibrary",
    "SuiteResult",
    "FailureStrategy",
    "QualityMetric",
    "QualityMetricType",
    "QualityCheckResult",
    "DataQualityReport",
    "ValidationResult",

    # Quality Checks
    "check_completeness",
    "check_freshness",
    "check_uniqueness",
    "check_validity",
    "generate_quality_report",

    # Declarative Schema
    "define_schema",
    "String", "Integer", "Float", "Boolean", "DateTime", "Date",
    "Array", "Object", "AnyType",
    "DeclarativeSchema",

    # === POWER USER API (Stable but Specialized) ===

    # Operators
    "map_transform", "map_field", "add_field",
    "filter_operator", "filter_field_equals", "filter_field_not_null",
    "aggregate_count", "aggregate_sum", "aggregate_group_by",
    "validate_schema", "validate_expectation", "validate_expectation_suite",
    "custom_operator",

    # Built-in Expectations
    "expect_column_to_exist", "expect_column_to_not_exist",
    "expect_column_type_to_be",
    "expect_table_column_count_to_equal", "expect_table_column_count_to_be_between",
    "expect_table_columns_to_match_set", "expect_table_columns_to_contain",
    "expect_table_columns_to_not_contain",
    "expect_column_to_be_required", "expect_column_to_be_optional",
    "expect_column_to_be_nullable", "expect_column_to_be_non_nullable",
    "expect_column_to_have_constraint", "expect_column_constraint_to_equal",

    # Materialization Strategies
    "MaterializationStrategyBase",
    "TableStrategy", "ViewStrategy", "FileStrategy",
    "IncrementalStrategy",

    # Expectation Helpers
    "compose_expectations",
    "create_parameterized_expectation",

    # === OPTIONAL FEATURES (May Be None) ===

    # SQL Assets
    "sql_asset",
    "execute_sql_query", "extract_asset_dependencies",
    "render_sql_template", "validate_sql",
    "SQLOperator", "SQLValidationResult",

    # Transformations
    "Join", "JoinType", "GroupBy",
    "Sum", "Count", "Avg", "Min", "Max",
    "Rollup", "Cube",
    "Window", "window_function",
    "Pivot", "Unpivot",
    "TransformationBuilder", "transform",

    # Schema Evolution
    "SemanticVersion", "SchemaChange", "SchemaDiff",
    "MigrationStep", "MigrationPlan",
    "SchemaHistoryEntry", "VersionedSchema", "SchemaHistory",
    "ChangeType", "BreakingChangeSeverity",
    "BreakingChangeDetector", "MigrationPlanner", "BackwardCompatibilityChecker",
    "schema_version", "get_schema_history", "reset_schema_history",

    # Integration/API
    "APIClient", "APIError", "AuthenticationError",
    "RESTClient", "RESTResponse",
    "GraphQLClient", "GraphQLResponse",
    "WebhookHandler", "WebhookRequest",
    "CursorPagination", "OffsetPagination", "LinkHeaderPagination",
    "RateLimitError",
    "validate_and_parse", "validate_response",

    # Database Connectors
    "DatabaseConnector", "QueryBuilder",
    "PostgreSQLConnector", "MySQLConnector",
    "SnowflakeConnector", "BigQueryConnector",

    # External Quality Tools
    "QualityToolAdapter", "QualityToolResult", "ToolType",
    "ge_asset", "GreatExpectationsAdapter",
    "soda_asset", "SodaAdapter",
    "merge_quality_results", "generate_unified_report", "display_quality_dashboard",

    # File I/O
    "FileReader", "FileWriter",
    "CSVReader", "CSVWriter",
    "JSONReader", "JSONWriter",
    "ParquetReader", "ParquetWriter",
    "ExcelReader", "ExcelWriter",
    "infer_schema_from_file",

    # Orchestration
    "ExecutionState", "OrchestrationConfig", "OrchestrationEngine",
    "ParallelExecutor", "StateManager",

    # Scheduling
    "BackfillConfig", "BackfillManager", "BackfillStatus", "BackfillTask",
    "CronSchedule", "IntervalSchedule", "Schedule",
    "EventTrigger", "TriggerEvent",
    "ScheduleEvent", "ScheduleStatus", "ScheduleStore", "ScheduleType",
    "Scheduler", "SchedulerConfig",
    "TriggerType",

    # Monitoring
    "ErrorAggregator", "ErrorCategory", "ErrorRecord", "ErrorSeverity",
    "HealthChecker", "HealthStatus",
    "LogLevel",
    "MetricsCollector", "MetricsSnapshot", "MetricType",
    "Profiler",
    "StructuredLogger",
    "configure_logging", "get_logger", "log_execution", "profile_execution",

    # Error Handling
    "BackoffStrategy",
    "Checkpoint", "CheckpointManager", "CheckpointState",
    "CircuitBreaker", "CircuitBreakerConfig", "CircuitBreakerError",
    "CircuitState",
    "DeadLetterItem", "DeadLetterQueue",
    "ErrorContext", "JitterStrategy",
    "RetryConfig", "RetryMetrics",
    "capture_error_context", "retry_with_backoff",

    # Caching & Lazy
    "CacheBackend", "CacheEntry", "CacheKey", "CacheManager",
    "DiskCacheBackend", "MemoryCacheBackend", "cached",
    "LazyContext", "LazySequence", "LazyTransform", "LazyValue",
    "is_lazy",
    "lazy", "lazy_filter", "lazy_map", "lazy_reduce", "lazy_transform",
    "materialize",

    # Query Hints
    "IndexHint", "IndexHintType", "JoinHint", "JoinStrategy",
    "LimitHint", "MaterializeHint", "ParallelHint",
    "ScanHint", "ScanHintType",
    "QueryHints", "QueryHintsBuilder", "with_query_hints",

    # Benchmarking
    "BenchmarkResult", "BenchmarkRunner", "BenchmarkStats",
    "ComparisonResult", "benchmark", "compare_benchmarks",

    # Quality Check Operators
    "check_quality_completeness",
    "check_quality_validity",
    "check_quality_uniqueness",
    "check_quality_freshness",
]
```

Total: ~200 items (down from 224, but better organized with clear categorization)

---

## Deprecation Strategy

For this initial curation, we don't need deprecation warnings because:
1. We're not removing anything that was previously in `__all__`
2. We're just organizing and documenting what's public
3. Optional features already have the "may be None" pattern

Future deprecation (if needed):
- Add `warnings.warn()` for items being removed
- Document timeline for breaking changes

---

## Documentation Updates

1. Update `src/vibe_piper/__init__.py` docstring to explain:
   - Core Public API (always available)
   - Power User API (stable but specialized)
   - Optional Features (may be None, require dependencies)

2. Add section to README or docs about:
   - Public API surface
   - Stability guarantees
   - Optional features and when to use them

---

## Tests to Create

Create `tests/test_public_api.py` to validate:
1. All items in `__all__` are actually exported
2. No private items (starting with `_`) are in `__all__`
3. Core API items are always available
4. Optional feature items may be None
5. Documentation for public API is up-to-date
