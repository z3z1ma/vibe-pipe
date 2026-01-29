"""
Vibe Piper - Declarative Data Pipeline Library

A robust, composable framework for building data pipelines with type safety
and extensibility at its core.
"""

__version__ = "0.1.0"

# Transformation framework
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

    _TRANSFORMATIONS_AVAILABLE = True
except ImportError:
    _TRANSFORMATIONS_AVAILABLE = False

from vibe_piper.decorators import asset, expect
from vibe_piper.error_handling import (
    BackoffStrategy,
    Checkpoint,
    CheckpointManager,
    CheckpointState,
    ErrorContext,
    RetryConfig,
    capture_error_context,
    retry_with_backoff,
)
from vibe_piper.execution import DefaultExecutor, ExecutionEngine, calculate_checksum
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
from vibe_piper.materialization import (
    FileStrategy,
    IncrementalStrategy,
    MaterializationStrategyBase,
    TableStrategy,
    ViewStrategy,
)
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
from vibe_piper.orchestration import (
    ExecutionState,
    OrchestrationConfig,
    OrchestrationEngine,
    ParallelExecutor,
    StateManager,
)
from vibe_piper.pipeline import (
    PipelineBuilder,
    build_pipeline,
    infer_dependencies_from_signature,
)
from vibe_piper.pipeline import (
    PipelineContext as PipelineDefContext,
)
from vibe_piper.quality import (
    check_completeness,
    check_freshness,
    check_uniqueness,
    check_validity,
    generate_quality_report,
)
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

# Schema evolution
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

    _SCHEMA_EVOLUTION_AVAILABLE = True
except ImportError:
    _SCHEMA_EVOLUTION_AVAILABLE = False

# Integration module exports (optional, for convenience)
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
        ValidationResult,
        WebhookHandler,
        WebhookRequest,
        validate_and_parse,
        validate_response,
    )

    _integration_available = True
except ImportError:
    _integration_available = False

# Add AuthenticationError if integration is available
if _integration_available:
    from vibe_piper.integration import AuthenticationError
else:
    AuthenticationError = None  # type: ignore
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
    ValidationResult,
)

# Database connectors (imported but not exported in __all__ for optional feature)
try:
    from vibe_piper.connectors import (
        BigQueryConnector,
        DatabaseConnector,
        MySQLConnector,
        PostgreSQLConnector,
        QueryBuilder,
        SnowflakeConnector,
    )

    _CONNECTORS_AVAILABLE = True
except ImportError:
    _CONNECTORS_AVAILABLE = False

# Monitoring & Observability
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

# External quality tools (optional, for convenience)
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

    _EXTERNAL_QUALITY_AVAILABLE = True
except ImportError:
    _EXTERNAL_QUALITY_AVAILABLE = False
    QualityToolAdapter = None  # type: ignore
    QualityToolResult = None  # type: ignore
    ToolType = None  # type: ignore
    ge_asset = None  # type: ignore
    GreatExpectationsAdapter = None  # type: ignore
    soda_asset = None  # type: ignore
    SodaAdapter = None  # type: ignore
    merge_quality_results = None  # type: ignore
    generate_unified_report = None  # type: ignore
    display_quality_dashboard = None  # type: ignore

__all__ = [
    "Asset",
    "AssetGraph",
    "AssetType",
    "Schema",
    "Pipeline",
    "Operator",
    "OperatorFn",
    "DataRecord",
    "SchemaField",
    "PipelineContext",
    "DataType",
    "OperatorType",
    "AssetResult",
    "ExecutionResult",
    "ExecutionEngine",
    "DefaultExecutor",
    "ErrorStrategy",
    "calculate_checksum",
    # Orchestration
    "ExecutionState",
    "OrchestrationConfig",
    "OrchestrationEngine",
    "ParallelExecutor",
    "StateManager",
    "Expectation",
    "ValidationResult",
    "QualityMetric",
    "QualityMetricType",
    "QualityCheckResult",
    "DataQualityReport",
    "MaterializationStrategy",
    "MaterializationStrategyBase",
    "TableStrategy",
    "ViewStrategy",
    "FileStrategy",
    "IncrementalStrategy",
    "__version__",
    # Decorators
    "asset",
    "expect",
    # Error Handling & Recovery
    "BackoffStrategy",
    "Checkpoint",
    "CheckpointManager",
    "CheckpointState",
    "ErrorContext",
    "RetryConfig",
    "capture_error_context",
    "retry_with_backoff",
    # Expectations
    "ExpectationLibrary",
    "ExpectationSuite",
    "SuiteResult",
    "FailureStrategy",
    "compose_expectations",
    "create_parameterized_expectation",
    # Pipeline builders
    "PipelineBuilder",
    "PipelineContext",
    "PipelineDefContext",
    "build_pipeline",
    "infer_dependencies_from_signature",
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
    # Quality metrics
    "check_completeness",
    "check_validity",
    "check_uniqueness",
    "check_freshness",
    "generate_quality_report",
    # Quality check operators
    "check_quality_completeness",
    "check_quality_validity",
    "check_quality_uniqueness",
    "check_quality_freshness",
    # Declarative Schema API
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
    # Transformation Framework
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
    # Scheduling
    "BackfillConfig",
    "BackfillManager",
    "BackfillStatus",
    "BackfillTask",
    "CronSchedule",
    "EventTrigger",
    "IntervalSchedule",
    "Schedule",
    "ScheduleEvent",
    "ScheduleStatus",
    "ScheduleStore",
    "ScheduleType",
    "Scheduler",
    "SchedulerConfig",
    "TriggerEvent",
    "TriggerType",
    # Database connectors
    "DatabaseConnector",
    "QueryBuilder",
    "PostgreSQLConnector",
    "MySQLConnector",
    "SnowflakeConnector",
    "BigQueryConnector",
    # File I/O Connectors
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
    # External quality tools
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
    # Schema evolution
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
]
