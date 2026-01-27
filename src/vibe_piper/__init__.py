"""
Vibe Piper - Declarative Data Pipeline Library

A robust, composable framework for building data pipelines with type safety
and extensibility at its core.
"""

__version__ = "0.1.0"

from vibe_piper.decorators import asset
from vibe_piper.execution import DefaultExecutor, ExecutionEngine
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
from vibe_piper.quality import (
    check_completeness,
    check_freshness,
    check_uniqueness,
    check_validity,
    generate_quality_report,
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
    DataQualityReport,
    DataRecord,
    DataType,
    ErrorStrategy,
    ExecutionResult,
    Operator,
    OperatorFn,
    OperatorType,
    Pipeline,
    QualityCheckResult,
    QualityMetric,
    QualityMetricType,
    Schema,
    SchemaField,
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
    "QualityMetric",
    "QualityMetricType",
    "QualityCheckResult",
    "DataQualityReport",
    "__version__",
    # Decorators
    "asset",
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
    # Quality metrics
    "check_completeness",
    "check_validity",
    "check_uniqueness",
    "check_freshness",
    "generate_quality_report",
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
]
