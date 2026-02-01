"""
Vibe Piper - Declarative Data Pipeline Library

A robust, composable framework for building data pipelines with type safety
and extensibility at its core.

Public API Surface
=================

This module exports three categories of APIs:

1. **CORE PUBLIC API** (Always Available)
   Canonical abstractions for building production data pipelines:
   - Core types: Asset, AssetGraph, Pipeline, Operator, Schema, DataRecord
   - Execution: ExecutionEngine, ExecutionResult, AssetResult
   - Builders: PipelineBuilder, build_pipeline, infer_dependencies_from_signature
   - Decorators: @asset, @expect
   - Quality: ExpectationSuite, QualityMetric, check_completeness, etc.
   - Schema API: define_schema, String, Integer, Float, etc.

   These APIs are stable, well-documented, and will always be available.

2. **POWER USER API** (Stable but Specialized)
   Advanced APIs for specific use cases:
   - Operators: map_transform, filter_operator, aggregate_*, custom_operator
   - Built-in expectations: expect_column_to_exist, etc.
   - Materialization strategies: TableStrategy, ViewStrategy, etc.

   These APIs are stable but intended for advanced users with specific needs.

3. **OPTIONAL FEATURES** (May Be None)
   APIs that depend on optional dependencies and may not be available:
   - SQL assets: sql_asset, execute_sql_query, etc.
   - Transformations: Join, GroupBy, Window, Pivot, etc.
   - Schema evolution: schema_version, MigrationPlan, etc.
   - Integration: RESTClient, GraphQLClient, etc.
   - Database connectors: PostgreSQLConnector, MySQLConnector, etc.
   - External quality tools: ge_asset, soda_asset, etc.
   - Orchestration: Scheduler, BackfillManager, etc.
   - Monitoring: MetricsCollector, Profiler, etc.

   Check availability with: `hasattr(vibe_piper, 'feature_name')`

Stability Guarantees
====================

- **Core Public API**: Semantic versioning guarantees (major.minor.patch)
- **Power User API**: Generally stable, may evolve with usage patterns
- **Optional Features**: May change or be removed in future major versions

Internal APIs
=============

Any symbol starting with underscore (_) is internal and not part of the public API.
These may change or be removed without notice.

Documentation
=============

- Core abstractions: See CORE_ABSTRACTION_CONTRACT.md
- API reference: See docs/ directory
- Examples: See examples/ directory
"""

__version__ = "0.1.0"

# ============================================================================
# CORE PUBLIC API (Always Available)
# ============================================================================

# Transformation framework (optional, for advanced use)
try:
    from vibe_piper.transformations import (
        Avg,
        Count,
        Cube,
        GroupBy,
        Join,
        JoinType,
        Max,
        Min,
        Pivot,
        Rollup,
        Sum,
        TransformationBuilder,
        Unpivot,
        Window,
        transform,
        window_function,
    )
except ImportError:
    # Set to None if not available
    Avg = None  # type: ignore
    Count = None  # type: ignore
    Cube = None  # type: ignore
    GroupBy = None  # type: ignore
    Join = None  # type: ignore
    JoinType = None  # type: ignore
    Max = None  # type: ignore
    Min = None  # type: ignore
    Pivot = None  # type: ignore
    Rollup = None  # type: ignore
    Sum = None  # type: ignore
    TransformationBuilder = None  # type: ignore
    Unpivot = None  # type: ignore
    Window = None  # type: ignore
    transform = None  # type: ignore
    window_function = None  # type: ignore

# Decorators
from vibe_piper.decorators import asset, expect

# Error handling (advanced use)
from vibe_piper.error_handling import (
    BackoffStrategy,
    Checkpoint,
    CheckpointManager,
    CheckpointState,
    CircuitBreaker,
    CircuitBreakerConfig,
    CircuitBreakerError,
    CircuitState,
    DeadLetterItem,
    DeadLetterQueue,
    ErrorContext,
    JitterStrategy,
    RetryConfig,
    RetryMetrics,
    capture_error_context,
    retry_with_backoff,
)

# Execution
from vibe_piper.execution import DefaultExecutor, ExecutionEngine, calculate_checksum

# Expectations
from vibe_piper.expectations import (
    ExpectationLibrary,
    ExpectationSuite,
    FailureStrategy,
    SuiteResult,
    compose_expectations,
    create_parameterized_expectation,
    expect_column_constraint_to_equal,
    expect_column_to_be_non_nullable,
    expect_column_to_be_nullable,
    expect_column_to_be_optional,
    expect_column_to_be_required,
    expect_column_to_exist,
    expect_column_to_have_constraint,
    expect_column_to_not_exist,
    expect_column_type_to_be,
    expect_table_column_count_to_be_between,
    expect_table_column_count_to_equal,
    expect_table_columns_to_contain,
    expect_table_columns_to_match_set,
    expect_table_columns_to_not_contain,
)

# Materialization strategies
from vibe_piper.materialization import (
    FileStrategy,
    IncrementalStrategy,
    MaterializationStrategyBase,
    TableStrategy,
    ViewStrategy,
)

# Operators
from vibe_piper.operators import (
    add_field,
    aggregate_count,
    aggregate_group_by,
    aggregate_sum,
    check_quality_completeness,
    check_quality_freshness,
    check_quality_uniqueness,
    check_quality_validity,
    custom_operator,
    filter_field_equals,
    filter_field_not_null,
    filter_operator,
    map_field,
    map_transform,
    validate_expectation,
    validate_expectation_suite,
    validate_schema,
)

# Asset adapters (optional feature)
try:
    from vibe_piper.asset_adapters import (
        sink_asset,
        source_asset,
    )
except ImportError:
    sink_asset = None  # type: ignore
    source_asset = None  # type: ignore

# Orchestration (optional, advanced use)
try:
    from vibe_piper.orchestration import (
        ExecutionState,
        OrchestrationConfig,
        OrchestrationEngine,
        ParallelExecutor,
        StateManager,
    )
except ImportError:
    ExecutionState = None  # type: ignore
    OrchestrationConfig = None  # type: ignore
    OrchestrationEngine = None  # type: ignore
    ParallelExecutor = None  # type: ignore
    StateManager = None  # type: ignore

# Pipeline builders
from vibe_piper.pipeline import (
    PipelineBuilder,
    PipelineDefinitionContext,
    build_pipeline,
    infer_dependencies_from_signature,
)

# Quality checks
from vibe_piper.quality import (
    check_completeness,
    check_freshness,
    check_uniqueness,
    check_validity,
    generate_quality_report,
)

# Scheduling (optional, advanced use)
try:
    from vibe_piper.scheduling import (
        BackfillConfig,
        BackfillManager,
        BackfillStatus,
        BackfillTask,
        CronSchedule,
        EventTrigger,
        IntervalSchedule,
        Schedule,
        ScheduleEvent,
        Scheduler,
        SchedulerConfig,
        ScheduleStatus,
        ScheduleStore,
        ScheduleType,
        TriggerEvent,
        TriggerType,
    )
except ImportError:
    BackfillConfig = None  # type: ignore
    BackfillManager = None  # type: ignore
    BackfillStatus = None  # type: ignore
    BackfillTask = None  # type: ignore
    CronSchedule = None  # type: ignore
    EventTrigger = None  # type: ignore
    IntervalSchedule = None  # type: ignore
    Schedule = None  # type: ignore
    ScheduleEvent = None  # type: ignore
    ScheduleStatus = None  # type: ignore
    ScheduleStore = None  # type: ignore
    ScheduleType = None  # type: ignore
    Scheduler = None  # type: ignore
    SchedulerConfig = None  # type: ignore
    TriggerEvent = None  # type: ignore
    TriggerType = None  # type: ignore

# Schema definitions
from vibe_piper.schema_definitions import (
    AnyType,
    Array,
    Boolean,
    Date,
    DateTime,
    DeclarativeSchema,
    Float,
    Integer,
    Object,
    String,
    define_schema,
)

# Schema evolution (optional, advanced use)
try:
    from vibe_piper.schema_evolution import (
        BackwardCompatibilityChecker,
        BreakingChangeDetector,
        BreakingChangeSeverity,
        ChangeType,
        MigrationPlan,
        MigrationPlanner,
        MigrationStep,
        SchemaChange,
        SchemaDiff,
        SchemaHistory,
        SchemaHistoryEntry,
        SemanticVersion,
        VersionedSchema,
        get_schema_history,
        reset_schema_history,
        schema_version,
    )
except ImportError:
    BackwardCompatibilityChecker = None  # type: ignore
    BreakingChangeDetector = None  # type: ignore
    BreakingChangeSeverity = None  # type: ignore
    ChangeType = None  # type: ignore
    MigrationPlan = None  # type: ignore
    MigrationPlanner = None  # type: ignore
    MigrationStep = None  # type: ignore
    SchemaChange = None  # type: ignore
    SchemaDiff = None  # type: ignore
    SchemaHistory = None  # type: ignore
    SchemaHistoryEntry = None  # type: ignore
    SemanticVersion = None  # type: ignore
    VersionedSchema = None  # type: ignore
    get_schema_history = None  # type: ignore
    reset_schema_history = None  # type: ignore
    schema_version = None  # type: ignore

# SQL assets (optional)
try:
    from vibe_piper.sql_assets import (
        SQLOperator,
        SQLValidationResult,
        execute_sql_query,
        extract_asset_dependencies,
        render_sql_template,
        sql_asset,
        validate_sql,
    )
except ImportError:
    SQLOperator = None  # type: ignore
    SQLValidationResult = None  # type: ignore
    execute_sql_query = None  # type: ignore
    extract_asset_dependencies = None  # type: ignore
    render_sql_template = None  # type: ignore
    sql_asset = None  # type: ignore
    validate_sql = None  # type: ignore

# Integration/API (optional, advanced use)
try:
    from vibe_piper.integration import (
        APIClient,
        APIError,
        BearerTokenAuth,
        CursorPagination,
        GraphQLClient,
        GraphQLResponse,
        LinkHeaderPagination,
        OffsetPagination,
        RateLimitError,
        RESTClient,
        RESTResponse,
        WebhookHandler,
        WebhookRequest,
        validate_and_parse,
        validate_response,
    )
    from vibe_piper.integration import (
        ValidationResult as IntegrationValidationResult,
    )
except ImportError:
    APIClient = None  # type: ignore
    APIError = None  # type: ignore
    BearerTokenAuth = None  # type: ignore
    CursorPagination = None  # type: ignore
    GraphQLClient = None  # type: ignore
    GraphQLResponse = None  # type: ignore
    LinkHeaderPagination = None  # type: ignore
    OffsetPagination = None  # type: ignore
    RateLimitError = None  # type: ignore
    RESTClient = None  # type: ignore
    RESTResponse = None  # type: ignore
    IntegrationValidationResult = None  # type: ignore
    WebhookHandler = None  # type: ignore
    WebhookRequest = None  # type: ignore
    validate_and_parse = None  # type: ignore
    validate_response = None  # type: ignore

try:
    from vibe_piper.integration import AuthenticationError
except ImportError:
    AuthenticationError = None  # type: ignore

# Type definitions
from vibe_piper.types import (
    Asset,
    AssetGraph,
    AssetResult,
    AssetType,
    DataQualityReport,
    DataRecord,
    DataType,
    ErrorStrategy,
    ExecutionResult,
    Expectation,
    MaterializationStrategy,
    Operator,
    OperatorFn,
    OperatorType,
    Pipeline,
    PipelineContext,
    QualityCheckResult,
    QualityMetric,
    QualityMetricType,
    Schema,
    SchemaField,
    UpstreamData,
    ValidationResult,
)

# Database connectors (optional, advanced use)
try:
    from vibe_piper.connectors import (
        BigQueryConnector,
        CSVReader,
        CSVWriter,
        DatabaseConnector,
        ExcelReader,
        ExcelWriter,
        FileReader,
        FileWriter,
        JSONReader,
        JSONWriter,
        MySQLConnector,
        ParquetReader,
        ParquetWriter,
        PostgreSQLConnector,
        QueryBuilder,
        SnowflakeConnector,
    )
except ImportError:
    BigQueryConnector = None  # type: ignore
    CSVReader = None  # type: ignore
    CSVWriter = None  # type: ignore
    DatabaseConnector = None  # type: ignore
    ExcelReader = None  # type: ignore
    ExcelWriter = None  # type: ignore
    FileReader = None  # type: ignore
    FileWriter = None  # type: ignore
    JSONReader = None  # type: ignore
    JSONWriter = None  # type: ignore
    MySQLConnector = None  # type: ignore
    ParquetReader = None  # type: ignore
    ParquetWriter = None  # type: ignore
    PostgreSQLConnector = None  # type: ignore
    QueryBuilder = None  # type: ignore
    SnowflakeConnector = None  # type: ignore

# Monitoring & Observability (optional, advanced use)
try:
    from vibe_piper.benchmarks import (
        BenchmarkResult,
        BenchmarkRunner,
        BenchmarkStats,
        ComparisonResult,
        benchmark,
        compare_benchmarks,
    )
except ImportError:
    BenchmarkResult = None  # type: ignore
    BenchmarkRunner = None  # type: ignore
    BenchmarkStats = None  # type: ignore
    ComparisonResult = None  # type: ignore
    benchmark = None  # type: ignore
    compare_benchmarks = None  # type: ignore

try:
    from vibe_piper.caching import (
        CacheBackend,
        CacheEntry,
        CacheKey,
        CacheManager,
        DiskCacheBackend,
        MemoryCacheBackend,
        cached,
    )
except ImportError:
    CacheBackend = None  # type: ignore
    CacheEntry = None  # type: ignore
    CacheKey = None  # type: ignore
    CacheManager = None  # type: ignore
    DiskCacheBackend = None  # type: ignore
    MemoryCacheBackend = None  # type: ignore
    cached = None  # type: ignore

try:
    from vibe_piper.lazy import (
        LazyContext,
        LazySequence,
        LazyTransform,
        LazyValue,
        is_lazy,
        lazy,
        lazy_filter,
        lazy_map,
        lazy_reduce,
        lazy_transform,
        materialize,
    )
except ImportError:
    LazyContext = None  # type: ignore
    LazySequence = None  # type: ignore
    LazyTransform = None  # type: ignore
    LazyValue = None  # type: ignore
    is_lazy = None  # type: ignore
    lazy = None  # type: ignore
    lazy_filter = None  # type: ignore
    lazy_map = None  # type: ignore
    lazy_reduce = None  # type: ignore
    lazy_transform = None  # type: ignore
    materialize = None  # type: ignore

try:
    from vibe_piper.monitoring import (
        ErrorAggregator,
        ErrorCategory,
        ErrorRecord,
        ErrorSeverity,
        HealthChecker,
        HealthStatus,
        LogLevel,
        MetricsCollector,
        MetricsSnapshot,
        MetricType,
        Profiler,
        StructuredLogger,
        configure_logging,
        get_logger,
        log_execution,
        profile_execution,
    )
except ImportError:
    ErrorAggregator = None  # type: ignore
    ErrorCategory = None  # type: ignore
    ErrorRecord = None  # type: ignore
    ErrorSeverity = None  # type: ignore
    HealthChecker = None  # type: ignore
    HealthStatus = None  # type: ignore
    LogLevel = None  # type: ignore
    MetricsCollector = None  # type: ignore
    MetricsSnapshot = None  # type: ignore
    MetricType = None  # type: ignore
    Profiler = None  # type: ignore
    StructuredLogger = None  # type: ignore
    configure_logging = None  # type: ignore
    get_logger = None  # type: ignore
    log_execution = None  # type: ignore
    profile_execution = None  # type: ignore

try:
    from vibe_piper.query_hints import (
        IndexHint,
        IndexHintType,
        JoinHint,
        JoinStrategy,
        LimitHint,
        MaterializeHint,
        ParallelHint,
        QueryHints,
        QueryHintsBuilder,
        ScanHint,
        ScanHintType,
        with_query_hints,
    )
except ImportError:
    IndexHint = None  # type: ignore
    IndexHintType = None  # type: ignore
    JoinHint = None  # type: ignore
    JoinStrategy = None  # type: ignore
    LimitHint = None  # type: ignore
    MaterializeHint = None  # type: ignore
    ParallelHint = None  # type: ignore
    QueryHints = None  # type: ignore
    QueryHintsBuilder = None  # type: ignore
    ScanHint = None  # type: ignore
    ScanHintType = None  # type: ignore
    with_query_hints = None  # type: ignore

# External quality tools (optional, advanced use)
try:
    from vibe_piper.external_quality import (
        GreatExpectationsAdapter,
        QualityToolAdapter,
        QualityToolResult,
        SodaAdapter,
        ToolType,
        display_quality_dashboard,
        ge_asset,
        generate_unified_report,
        merge_quality_results,
        soda_asset,
    )
except ImportError:
    GreatExpectationsAdapter = None  # type: ignore
    QualityToolAdapter = None  # type: ignore
    QualityToolResult = None  # type: ignore
    SodaAdapter = None  # type: ignore
    ToolType = None  # type: ignore
    display_quality_dashboard = None  # type: ignore
    ge_asset = None  # type: ignore
    generate_unified_report = None  # type: ignore
    merge_quality_results = None  # type: ignore
    soda_asset = None  # type: ignore

# ============================================================================
# Public API: Explicit __all__
# ============================================================================

__all__ = [
    # Version
    "__version__",
    # -----------------------------------------------------------------------
    # Core Public API (Always Available)
    # -----------------------------------------------------------------------
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
    "String",
    "Integer",
    "Float",
    "Boolean",
    "DateTime",
    "Date",
    "Array",
    "Object",
    "AnyType",
    "DeclarativeSchema",
    # -----------------------------------------------------------------------
    # Power User API (Stable but Specialized)
    # -----------------------------------------------------------------------
    # Operators
    "map_transform",
    "map_field",
    "add_field",
    "filter_operator",
    "filter_field_equals",
    "filter_field_not_null",
    "aggregate_count",
    "aggregate_sum",
    "aggregate_group_by",
    "validate_schema",
    "validate_expectation",
    "validate_expectation_suite",
    "custom_operator",
    # Built-in Expectations
    "expect_column_to_exist",
    "expect_column_to_not_exist",
    "expect_column_type_to_be",
    "expect_table_column_count_to_equal",
    "expect_table_column_count_to_be_between",
    "expect_table_columns_to_match_set",
    "expect_table_columns_to_contain",
    "expect_table_columns_to_not_contain",
    "expect_column_to_be_required",
    "expect_column_to_be_optional",
    "expect_column_to_be_nullable",
    "expect_column_to_be_non_nullable",
    "expect_column_to_have_constraint",
    "expect_column_constraint_to_equal",
    # Materialization Strategies
    "MaterializationStrategyBase",
    "TableStrategy",
    "ViewStrategy",
    "FileStrategy",
    "IncrementalStrategy",
    # Expectation Helpers
    "compose_expectations",
    "create_parameterized_expectation",
    # Quality Check Operators
    "check_quality_completeness",
    "check_quality_validity",
    "check_quality_uniqueness",
    "check_quality_freshness",
    # -----------------------------------------------------------------------
    # Optional Features (May Be None)
    # -----------------------------------------------------------------------
    # SQL Assets
    "sql_asset",
    "execute_sql_query",
    "extract_asset_dependencies",
    "render_sql_template",
    "validate_sql",
    "SQLOperator",
    "SQLValidationResult",
    # Transformations
    "Join",
    "JoinType",
    "GroupBy",
    "Sum",
    "Count",
    "Avg",
    "Min",
    "Max",
    "Rollup",
    "Cube",
    "Window",
    "window_function",
    "Pivot",
    "Unpivot",
    "TransformationBuilder",
    "transform",
    # Schema Evolution
    "SemanticVersion",
    "SchemaChange",
    "SchemaDiff",
    "MigrationStep",
    "MigrationPlan",
    "SchemaHistoryEntry",
    "VersionedSchema",
    "SchemaHistory",
    "ChangeType",
    "BreakingChangeSeverity",
    "BreakingChangeDetector",
    "MigrationPlanner",
    "BackwardCompatibilityChecker",
    "schema_version",
    "get_schema_history",
    "reset_schema_history",
    # Integration/API
    "APIClient",
    "APIError",
    "AuthenticationError",
    "RESTClient",
    "RESTResponse",
    "GraphQLClient",
    "GraphQLResponse",
    "WebhookHandler",
    "WebhookRequest",
    "CursorPagination",
    "OffsetPagination",
    "LinkHeaderPagination",
    "RateLimitError",
    "validate_and_parse",
    "validate_response",
    # Database Connectors
    "DatabaseConnector",
    "QueryBuilder",
    "PostgreSQLConnector",
    "MySQLConnector",
    "SnowflakeConnector",
    "BigQueryConnector",
    # External Quality Tools
    "QualityToolAdapter",
    "QualityToolResult",
    "ToolType",
    "ge_asset",
    "GreatExpectationsAdapter",
    "soda_asset",
    "SodaAdapter",
    "merge_quality_results",
    "generate_unified_report",
    "display_quality_dashboard",
    # File I/O
    "FileReader",
    "FileWriter",
    "CSVReader",
    "CSVWriter",
    "JSONReader",
    "JSONWriter",
    "ParquetReader",
    "ParquetWriter",
    "ExcelReader",
    "ExcelWriter",
    "infer_schema_from_file",
    # Orchestration
    "ExecutionState",
    "OrchestrationConfig",
    "OrchestrationEngine",
    "ParallelExecutor",
    "StateManager",
    # Scheduling
    "BackfillConfig",
    "BackfillManager",
    "BackfillStatus",
    "BackfillTask",
    "CronSchedule",
    "IntervalSchedule",
    "Schedule",
    "EventTrigger",
    "TriggerEvent",
    "ScheduleEvent",
    "ScheduleStatus",
    "ScheduleStore",
    "ScheduleType",
    "Scheduler",
    "SchedulerConfig",
    "TriggerType",
    # Monitoring
    "ErrorAggregator",
    "ErrorCategory",
    "ErrorRecord",
    "ErrorSeverity",
    "HealthChecker",
    "HealthStatus",
    "LogLevel",
    "MetricsCollector",
    "MetricsSnapshot",
    "MetricType",
    "Profiler",
    "StructuredLogger",
    "configure_logging",
    "get_logger",
    "log_execution",
    "profile_execution",
    # Error Handling
    "BackoffStrategy",
    "Checkpoint",
    "CheckpointManager",
    "CheckpointState",
    "CircuitBreaker",
    "CircuitBreakerConfig",
    "CircuitBreakerError",
    "CircuitState",
    "DeadLetterItem",
    "DeadLetterQueue",
    "ErrorContext",
    "JitterStrategy",
    "RetryConfig",
    "RetryMetrics",
    "capture_error_context",
    "retry_with_backoff",
    # Caching & Lazy
    "CacheBackend",
    "CacheEntry",
    "CacheKey",
    "CacheManager",
    "DiskCacheBackend",
    "MemoryCacheBackend",
    "cached",
    "LazyContext",
    "LazySequence",
    "LazyTransform",
    "LazyValue",
    "is_lazy",
    "lazy",
    "lazy_filter",
    "lazy_map",
    "lazy_reduce",
    "lazy_transform",
    "materialize",
    # Query Hints
    "IndexHint",
    "IndexHintType",
    "JoinHint",
    "JoinStrategy",
    "LimitHint",
    "MaterializeHint",
    "ParallelHint",
    "ScanHint",
    "ScanHintType",
    "QueryHints",
    "QueryHintsBuilder",
    "with_query_hints",
    # Benchmarking
    "BenchmarkResult",
    "BenchmarkRunner",
    "BenchmarkStats",
    "ComparisonResult",
    "benchmark",
    "compare_benchmarks",
]
