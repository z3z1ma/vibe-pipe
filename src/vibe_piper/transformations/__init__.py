"""
Transformation Framework for Vibe Piper.

This module provides comprehensive data transformation capabilities including:
- Join operations (inner, left, right, full outer)
- Aggregation operations (groupby, rollup, cube)
- Window functions (row_number, rank, lag, lead)
- Pivot/unpivot operations
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
from vibe_piper.transformations.joins import Join, JoinType
from vibe_piper.transformations.pivot import Pivot, Unpivot
from vibe_piper.transformations.windows import Window, window_function

__all__ = [
    # Joins
    "Join",
    "JoinType",
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
]
