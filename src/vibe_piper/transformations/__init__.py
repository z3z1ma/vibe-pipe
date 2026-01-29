"""
Transformation Framework for Vibe Piper.

This module provides comprehensive data transformation capabilities including:
- Join operations (inner, left, right, full outer)
- Aggregation operations (groupby, rollup, cube)
- Window functions (row_number, rank, lag, lead)
- Pivot/unpivot operations
- Data cleaning utilities (deduplication, null handling, outliers, text cleaning)
- Fluent builder API for complex transformations
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
