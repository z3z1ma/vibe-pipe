"""
Vibe Piper - Declarative Data Pipeline Library

A robust, composable framework for building data pipelines with type safety
and extensibility at its core.
"""

__version__ = "0.1.0"

from vibe_piper.decorators import asset, expect
from vibe_piper.execution import DefaultExecutor, ExecutionEngine
from vibe_piper.expectations import (
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
from vibe_piper.operators import (
    add_field,
    aggregate_count,
    aggregate_group_by,
    aggregate_sum,
    custom_operator,
    filter_field_equals,
    filter_field_not_null,
    filter_operator,
    map_field,
    map_transform,
    validate_schema,
)
from vibe_piper.pipeline import (
    PipelineBuilder,
    PipelineContext,
    build_pipeline,
    infer_dependencies_from_signature,
)
from vibe_piper.pipeline import (
    PipelineContext as PipelineDefContext,
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
from vibe_piper.types import (
    Asset,
    AssetGraph,
    AssetResult,
    AssetType,
    DataRecord,
    DataType,
    ErrorStrategy,
    ExecutionResult,
    Expectation,
    Operator,
    OperatorFn,
    OperatorType,
    Pipeline,
    Schema,
    SchemaField,
    ValidationResult,
)

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
    "Expectation",
    "ValidationResult",
    "__version__",
    # Decorators
    "asset",
    "expect",
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
    "custom_operator",
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
]
