"""
Vibe Piper - Declarative Data Pipeline Library

A robust, composable framework for building data pipelines with type safety
and extensibility at its core.
"""

__version__ = "0.1.0"

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
from vibe_piper.types import (
    Asset,
    DataRecord,
    DataType,
    Operator,
    OperatorFn,
    OperatorType,
    Pipeline,
    PipelineContext,
    Schema,
    SchemaField,
)

__all__ = [
    "Asset",
    "Schema",
    "Pipeline",
    "Operator",
    "OperatorFn",
    "DataRecord",
    "SchemaField",
    "PipelineContext",
    "DataType",
    "OperatorType",
    "__version__",
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
]
