"""
Transformation Framework for Vibe Piper.

This module provides comprehensive data transformation capabilities including:
- Join operations (inner, left, right, full outer)
- Aggregation operations (groupby, rollup, cube)
- Window functions (row_number, rank, lag, lead)
- Pivot/unpivot operations
- Data cleaning utilities (deduplication, null handling, outliers, text cleaning)
- Fluent builder API for complex transformations
- Built-in transformations (extract_fields, map_field, compute_field, enrich_from_lookup)
- Validation helpers (validate_record, validate_batch, field validators)
"""

from vibe_piper.transformations.aggregations import (
    Avg,
    Count,
    Cube,
    GroupBy,
    Max,
    Min,
    Rollup,
    Sum,
)
from vibe_piper.transformations.builder import TransformationBuilder, transform
from vibe_piper.transformations.cleaning import (
    CleaningConfig,
    CleaningReport,
    NullStrategy,
    OutlierAction,
    OutlierMethod,
    cap_outliers,
    clean_data,
    clean_dataset,
    clean_text,
    convert_column_type,
    drop_nulls,
    fill_nulls,
    find_duplicates,
    get_data_profile,
    get_null_counts,
    get_value_counts,
    handle_nulls,
    handle_outliers,
    normalize_case,
    normalize_minmax,
    normalize_types,
    normalize_zscore,
    remove_duplicates,
    remove_special_chars,
    standardize_columns,
    summarize_report,
    trim_whitespace,
)
from vibe_piper.transformations.joins import Join, JoinType, join
from vibe_piper.transformations.pivot import Pivot, Unpivot
from vibe_piper.transformations.transforms import (
    cast_field,
    compute_field,
    compute_field_from_expression,
    drop_fields,
    enrich_from_lookup,
    extract_fields,
    extract_nested_value,
    filter_by_field,
    filter_rows,
    map_field,
    rename_fields,
    select_fields,
)
from vibe_piper.transformations.validators import (
    create_filter_validator,
    create_validator_from_schema,
    validate_batch,
    validate_email_format,
    validate_enum,
    validate_field_length,
    validate_field_required,
    validate_field_type,
    validate_range,
    validate_record,
    validate_regex_pattern,
)
from vibe_piper.transformations.windows import Window, window_function

__all__ = [
    # Joins
    "Join",
    "JoinType",
    "join",
    # Aggregations
    "GroupBy",
    "Sum",
    "Count",
    "Avg",
    "Min",
    "Max",
    "Rollup",
    "Cube",
    # Windows
    "Window",
    "window_function",
    # Pivot
    "Pivot",
    "Unpivot",
    # Builder
    "TransformationBuilder",
    "transform",
    # Built-in Transforms
    "extract_fields",
    "extract_nested_value",
    "map_field",
    "compute_field",
    "compute_field_from_expression",
    "filter_rows",
    "filter_by_field",
    "enrich_from_lookup",
    "rename_fields",
    "drop_fields",
    "select_fields",
    "cast_field",
    # Validators
    "validate_field_type",
    "validate_field_required",
    "validate_email_format",
    "validate_regex_pattern",
    "validate_range",
    "validate_field_length",
    "validate_enum",
    "validate_record",
    "validate_batch",
    "create_validator_from_schema",
    "create_filter_validator",
    # Cleaning - Decorator and Main Functions
    "clean_data",
    "clean_dataset",
    # Cleaning - Configuration and Reports
    "CleaningConfig",
    "CleaningReport",
    "NullStrategy",
    "OutlierMethod",
    "OutlierAction",
    # Cleaning - Deduplication
    "remove_duplicates",
    "find_duplicates",
    # Cleaning - Null Handling
    "handle_nulls",
    "drop_nulls",
    "fill_nulls",
    # Cleaning - Outliers
    "handle_outliers",
    "detect_outliers",
    "cap_outliers",
    # Cleaning - Type Normalization
    "normalize_types",
    "convert_column_type",
    # Cleaning - Standardization
    "standardize_columns",
    "normalize_minmax",
    "normalize_zscore",
    # Cleaning - Text Cleaning
    "clean_text",
    "trim_whitespace",
    "normalize_case",
    "remove_special_chars",
    # Cleaning - Utilities
    "get_null_counts",
    "get_value_counts",
    "get_data_profile",
    "summarize_report",
]
