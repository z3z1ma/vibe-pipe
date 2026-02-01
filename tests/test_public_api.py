"""Test public API surface and exports.

This test validates that:
1. All items in __all__ are actually exported
2. No private items (starting with _) are in __all__
3. Core API items are always available (not None)
4. Optional feature items may be None
5. Public API documentation is accurate
"""

from __future__ import annotations

import pytest

import vibe_piper


class TestPublicAPIAll:
    """Test __all__ exports."""

    def test_all_exists(self) -> None:
        """__all__ must exist and be a list."""
        assert hasattr(vibe_piper, "__all__")
        assert isinstance(vibe_piper.__all__, list)
        assert len(vibe_piper.__all__) > 0

    def test_all_items_are_exported(self) -> None:
        """All items in __all__ must be exportable from vibe_piper."""
        for name in vibe_piper.__all__:
            assert hasattr(
                vibe_piper, name
            ), f"'{name}' in __all__ but not exported from vibe_piper"

    def test_no_private_names_in_all(self) -> None:
        """No private names (starting with _ but not __) should be in __all__."""
        for name in vibe_piper.__all__:
            # Allow dunder names (starts and ends with __) like __version__
            # But disallow single underscore names like _private
            if name.startswith("_") and not (name.startswith("__") and name.endswith("__")):
                pytest.fail(f"Private name '{name}' in __all__")

    def test_version_in_all(self) -> None:
        """__version__ should be in __all__."""
        assert "__version__" in vibe_piper.__all__
        assert hasattr(vibe_piper, "__version__")
        assert isinstance(vibe_piper.__version__, str)


class TestCorePublicAPI:
    """Test that core public API items are always available."""

    @pytest.mark.parametrize(
        "name",
        [
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
        ],
    )
    def test_core_api_exists(self, name: str) -> None:
        """Core public API items must exist and not be None."""
        assert name in vibe_piper.__all__, f"'{name}' not in __all__"
        assert hasattr(vibe_piper, name), f"'{name}' not exported"
        value = getattr(vibe_piper, name)
        assert value is not None, f"'{name}' is None (should always be available)"
        # It should be callable or a class (type), not a module
        if not callable(value):
            assert isinstance(value, type), f"'{name}' is neither callable nor a type"


class TestOptionalFeatures:
    """Test that optional features may be None."""

    OPTIONAL_FEATURES = [
        # SQL Assets
        "sql_asset",
        "SQLOperator",
        "SQLValidationResult",
        # Transformations
        "Join",
        "GroupBy",
        "Sum",
        "Count",
        "Avg",
        "Min",
        "Max",
        "Rollup",
        "Cube",
        "Window",
        "Pivot",
        "Unpivot",
        "TransformationBuilder",
        "transform",
        # Schema Evolution
        "SemanticVersion",
        "SchemaChange",
        "SchemaDiff",
        "MigrationPlan",
        "MigrationPlanner",
        "BackwardCompatibilityChecker",
        "schema_version",
        "get_schema_history",
        "reset_schema_history",
        # Integration/API
        "APIClient",
        "RESTClient",
        "GraphQLClient",
        "WebhookHandler",
        "AuthenticationError",
        "RateLimitError",
        # Database Connectors
        "PostgreSQLConnector",
        "MySQLConnector",
        "SnowflakeConnector",
        "BigQueryConnector",
        # External Quality Tools
        "QualityToolAdapter",
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

    @pytest.mark.parametrize("name", OPTIONAL_FEATURES)
    def test_optional_features_in_all(self, name: str) -> None:
        """Optional features must be in __all__."""
        assert name in vibe_piper.__all__, f"'{name}' not in __all__"

    @pytest.mark.parametrize("name", OPTIONAL_FEATURES)
    def test_optional_features_may_be_none(self, name: str) -> None:
        """Optional features may be None."""
        assert name in vibe_piper.__all__, f"'{name}' not in __all__"
        # Just check it's in __all__ - the actual value may be None
        # which is acceptable for optional features


class TestPowerUserAPI:
    """Test that power user API items are always available."""

    @pytest.mark.parametrize(
        "name",
        [
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
        ],
    )
    def test_power_user_api_exists(self, name: str) -> None:
        """Power user API items must exist and not be None."""
        assert name in vibe_piper.__all__, f"'{name}' not in __all__"
        assert hasattr(vibe_piper, name), f"'{name}' not exported"
        value = getattr(vibe_piper, name)
        assert value is not None, f"'{name}' is None (should always be available)"


class TestAdditionalOptionalFeatures:
    """Test additional optional feature items that may be None."""

    @pytest.mark.parametrize(
        "name",
        [
            # SQL Assets (additional)
            "execute_sql_query",
            "extract_asset_dependencies",
            "render_sql_template",
            "validate_sql",
            # Transformations (additional)
            "JoinType",
            "window_function",
            # Integration/API (additional)
            "APIError",
            "GraphQLResponse",
            "RESTResponse",
            "WebhookRequest",
            "CursorPagination",
            "OffsetPagination",
            "LinkHeaderPagination",
            "validate_and_parse",
            "validate_response",
            # Database Connectors (additional)
            "DatabaseConnector",
            "QueryBuilder",
            # External Quality Tools (additional)
            "QualityToolResult",
            "ToolType",
            # Schema Evolution (additional)
            "ChangeType",
            "BreakingChangeSeverity",
            "BreakingChangeDetector",
            "MigrationStep",
            "SchemaHistory",
            "SchemaHistoryEntry",
            "VersionedSchema",
        ],
    )
    def test_additional_optional_features_in_all(self, name: str) -> None:
        """Additional optional features must be in __all__."""
        assert name in vibe_piper.__all__, f"'{name}' not in __all__"

    @pytest.mark.parametrize(
        "name",
        [
            # SQL Assets (additional)
            "execute_sql_query",
            "extract_asset_dependencies",
            "render_sql_template",
            "validate_sql",
            # Transformations (additional)
            "JoinType",
            "window_function",
            # Integration/API (additional)
            "APIError",
            "GraphQLResponse",
            "RESTResponse",
            "WebhookRequest",
            "CursorPagination",
            "OffsetPagination",
            "LinkHeaderPagination",
            "validate_and_parse",
            "validate_response",
            # Database Connectors (additional)
            "DatabaseConnector",
            "QueryBuilder",
            # External Quality Tools (additional)
            "QualityToolResult",
            "ToolType",
            # Schema Evolution (additional)
            "ChangeType",
            "BreakingChangeSeverity",
            "BreakingChangeDetector",
            "MigrationStep",
            "SchemaHistory",
            "SchemaHistoryEntry",
            "VersionedSchema",
        ],
    )
    def test_additional_optional_features_may_be_none(self, name: str) -> None:
        """Additional optional features may be None."""
        assert name in vibe_piper.__all__, f"'{name}' not in __all__"
        # Just check it's in __all__ - actual value may be None
        # which is acceptable for optional features


class TestAPIDocumentation:
    """Test that public API is documented."""

    def test_module_has_docstring(self) -> None:
        """vibe_piper module must have a docstring."""
        assert vibe_piper.__doc__ is not None
        assert len(vibe_piper.__doc__) > 50  # More than a brief description

    def test_docstring_explains_api_categories(self) -> None:
        """Module docstring should explain API categories."""
        doc = vibe_piper.__doc__ or ""
        # Should mention at least "CORE PUBLIC API" or similar categorization
        assert (
            "CORE" in doc or "public API" in doc.lower()
        ), "Docstring should explain public API categories"
