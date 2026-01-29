"""
Data profiling module for inferring schemas and statistics from data.

This module provides comprehensive data profiling capabilities:
- Schema inference: Detect column types and constraints
- Statistics: Calculate descriptive statistics for each column
- Distribution analysis: Analyze value distributions and patterns
- Null analysis: Identify missing value patterns
"""

from __future__ import annotations

import statistics
from collections import Counter
from collections.abc import Sequence
from dataclasses import dataclass, field
from datetime import date, datetime
from typing import TYPE_CHECKING, Any

from vibe_piper.types import DataRecord, DataType, Schema, SchemaField

if TYPE_CHECKING:
    pass

# =============================================================================
# Data Profile Result Types
# =============================================================================


@dataclass(frozen=True)
class ColumnStatistics:
    """
    Statistical summary for a single column.

    Attributes:
        column_name: Name of the column
        data_type: Inferred data type
        null_count: Number of null values
        null_percentage: Percentage of null values (0-1)
        distinct_count: Number of distinct values
        unique_count: Number of unique values (appears exactly once)
        sample_values: Sample of distinct values (up to 10)
        min_value: Minimum value (for numeric/temporal)
        max_value: Maximum value (for numeric/temporal)
        mean: Mean value (for numeric)
        median: Median value (for numeric)
        std_dev: Standard deviation (for numeric)
        mode: Most common value(s)
        histogram: Value frequency distribution (for categorical/string)
        length_stats: String length statistics (for string columns)
    """

    column_name: str
    data_type: DataType
    null_count: int = 0
    null_percentage: float = 0.0
    distinct_count: int = 0
    unique_count: int = 0
    sample_values: tuple[Any, ...] = field(default_factory=tuple)
    min_value: Any | None = None
    max_value: Any | None = None
    mean: float | None = None
    median: Any | None = None
    std_dev: float | None = None
    mode: tuple[Any, ...] = field(default_factory=tuple)
    histogram: dict[str, int] | None = None
    length_stats: dict[str, float] | None = None


@dataclass(frozen=True)
class DataProfile:
    """
    Comprehensive data profile result.

    Attributes:
        total_rows: Total number of records profiled
        total_columns: Number of columns profiled
        column_stats: Statistics for each column
        inferred_schema: Inferred schema from data
        timestamp: When profile was generated
        duration_ms: Time taken to generate profile
    """

    total_rows: int
    total_columns: int
    column_stats: dict[str, ColumnStatistics] = field(default_factory=dict)
    inferred_schema: Schema | None = None
    timestamp: datetime = field(default_factory=datetime.utcnow)
    duration_ms: float = 0.0

    def get_null_summary(self) -> dict[str, float]:
        """Get null percentage for all columns."""
        return {col: stats.null_percentage for col, stats in self.column_stats.items()}

    def get_type_summary(self) -> dict[str, DataType]:
        """Get inferred type for all columns."""
        return {col: stats.data_type for col, stats in self.column_stats.items()}

    def get_summary_report(self) -> str:
        """Get a human-readable summary of the profile."""
        lines = [
            "Data Profile Summary",
            f"  Rows: {self.total_rows}",
            f"  Columns: {self.total_columns}",
            "",
            "Column Statistics:",
        ]

        for col, stats in self.column_stats.items():
            lines.append(f"  {col}:")
            lines.append(f"    Type: {stats.data_type.name}")
            lines.append(f"    Nulls: {stats.null_count} ({stats.null_percentage:.1%})")
            lines.append(f"    Distinct: {stats.distinct_count}")

            if stats.data_type in (DataType.INTEGER, DataType.FLOAT):
                if stats.mean is not None:
                    lines.append(f"    Mean: {stats.mean:.2f}")
                if stats.median is not None:
                    lines.append(f"    Median: {stats.median}")
                if stats.std_dev is not None:
                    lines.append(f"    Std Dev: {stats.std_dev:.2f}")
                if stats.min_value is not None:
                    lines.append(f"    Range: [{stats.min_value}, {stats.max_value}]")

        return "\n".join(lines)


# =============================================================================
# Type Inference
# =============================================================================


def _infer_type(values: list[Any], sample_size: int = 100) -> DataType:
    """
    Infer the data type of a column from sample values.

    Args:
        values: List of values to analyze
        sample_size: Number of values to sample for inference

    Returns:
        Inferred DataType
    """
    if not values:
        return DataType.ANY

    # Sample values for performance
    sample = values[:sample_size] if len(values) > sample_size else values

    # Filter out None values
    non_null = [v for v in sample if v is not None]
    if not non_null:
        return DataType.ANY

    # Check types
    int_count = sum(1 for v in non_null if isinstance(v, int) and not isinstance(v, bool))
    float_count = sum(1 for v in non_null if isinstance(v, float))
    bool_count = sum(1 for v in non_null if isinstance(v, bool))
    str_count = sum(1 for v in non_null if isinstance(v, str))
    datetime_count = sum(1 for v in non_null if isinstance(v, (datetime, date)))
    dict_count = sum(1 for v in non_null if isinstance(v, dict))
    list_count = sum(1 for v in non_null if isinstance(v, list))

    total = len(non_null)

    # Determine type based on majority
    if float_count / total > 0.8:
        return DataType.FLOAT
    elif int_count / total > 0.8:
        return DataType.INTEGER
    elif bool_count / total > 0.8:
        return DataType.BOOLEAN
    elif datetime_count / total > 0.8:
        return DataType.DATETIME
    elif dict_count / total > 0.8:
        return DataType.OBJECT
    elif list_count / total > 0.8:
        return DataType.ARRAY
    elif str_count / total > 0.8:
        return DataType.STRING
    else:
        # Mixed types - try to infer numeric
        if all(isinstance(v, (int, float)) for v in non_null):
            return DataType.FLOAT if float_count > 0 else DataType.INTEGER
        return DataType.ANY


def _is_numeric_type(dtype: DataType) -> bool:
    """Check if data type is numeric."""
    return dtype in (DataType.INTEGER, DataType.FLOAT)


def _is_temporal_type(dtype: DataType) -> bool:
    """Check if data type is temporal."""
    return dtype in (DataType.DATETIME, DataType.DATE)


# =============================================================================
# Column Statistics Calculation
# =============================================================================


def _calculate_numeric_stats(values: list[float | int], column_name: str) -> ColumnStatistics:
    """Calculate statistics for numeric columns."""
    if not values:
        return ColumnStatistics(column_name=column_name, data_type=DataType.INTEGER)

    min_val = min(values)
    max_val = max(values)
    mean_val = statistics.mean(values)
    median_val = statistics.median(values)
    std_dev = statistics.stdev(values) if len(values) > 1 else 0.0

    # Calculate mode
    counter = Counter(values)
    max_freq = max(counter.values())
    modes = [val for val, freq in counter.items() if freq == max_freq]

    # Sample values
    sample_vals = tuple(set(values[:10]))

    return ColumnStatistics(
        column_name=column_name,
        data_type=DataType.FLOAT if isinstance(values[0], float) else DataType.INTEGER,
        min_value=min_val,
        max_value=max_val,
        mean=mean_val,
        median=median_val,
        std_dev=std_dev,
        mode=tuple(modes),
        sample_values=sample_vals,
    )


def _calculate_string_stats(values: list[str], column_name: str) -> ColumnStatistics:
    """Calculate statistics for string columns."""
    if not values:
        return ColumnStatistics(column_name=column_name, data_type=DataType.STRING)

    # Length statistics
    lengths = [len(v) for v in values]
    length_stats = {
        "min_length": min(lengths),
        "max_length": max(lengths),
        "mean_length": statistics.mean(lengths),
        "median_length": statistics.median(lengths),
    }

    # Histogram (frequency distribution)
    counter = Counter(values)
    histogram = dict(counter.most_common(20))  # Top 20 values

    # Sample values
    sample_vals = tuple(list(counter.keys())[:10])

    # Mode
    max_freq = max(counter.values())
    modes = [val for val, freq in counter.items() if freq == max_freq]

    return ColumnStatistics(
        column_name=column_name,
        data_type=DataType.STRING,
        mode=tuple(modes),
        sample_values=sample_vals,
        histogram=histogram,
        length_stats=length_stats,
        distinct_count=len(counter),
    )


def _calculate_datetime_stats(values: list[datetime | date], column_name: str) -> ColumnStatistics:
    """Calculate statistics for datetime columns."""
    if not values:
        return ColumnStatistics(column_name=column_name, data_type=DataType.DATETIME)

    min_val = min(values)
    max_val = max(values)

    # Calculate mode
    counter = Counter(str(v) for v in values)
    max_freq = max(counter.values())
    modes = [val for val, freq in counter.items() if freq == max_freq]

    # Sample values
    sample_vals = tuple(list(set([str(v) for v in values]))[:10])

    return ColumnStatistics(
        column_name=column_name,
        data_type=DataType.DATETIME if isinstance(values[0], datetime) else DataType.DATE,
        min_value=min_val,
        max_value=max_val,
        mode=tuple(modes),
        sample_values=sample_vals,
        distinct_count=len(counter),
    )


# =============================================================================
# Main Profiling Function
# =============================================================================


def profile_data(
    records: Sequence[DataRecord],
    infer_schema: bool = True,
    max_sample_rows: int = 10000,
) -> DataProfile:
    """
    Profile data to infer schema and calculate statistics.

    Analyzes data to understand its structure, types, and distributions.
    Useful for data exploration and validation.

    Args:
        records: Records to profile (list of DataRecord or dict)
        infer_schema: Whether to infer a Schema from the data (default: True)
        max_sample_rows: Maximum rows to sample for profiling (performance optimization)

    Returns:
        DataProfile with comprehensive statistics

    Example:
        >>> profile = profile_data(customer_records)
        >>> print(profile.get_summary_report())
        >>> schema = profile.inferred_schema
    """
    import time

    start_time = time.time()

    if not records:
        return DataProfile(
            total_rows=0,
            total_columns=0,
            column_stats={},
        )

    # Sample records for performance
    sample_records = records[:max_sample_rows] if len(records) > max_sample_rows else records

    # Get all column names
    if isinstance(sample_records[0], dict):
        columns = list(sample_records[0].keys())
    else:
        # Assume DataRecord or similar
        columns = list(sample_records[0].data.keys()) if hasattr(sample_records[0], "data") else []

    # Calculate statistics for each column
    column_stats = {}
    for col in columns:
        # Extract values
        values = []
        for record in sample_records:
            val = record.get(col) if hasattr(record, "get") else record.data.get(col)
            values.append(val)

        # Filter nulls
        non_null = [v for v in values if v is not None]

        # Null statistics
        null_count = len(values) - len(non_null)
        null_pct = null_count / len(values) if values else 0.0

        # Infer type
        dtype = _infer_type(non_null)

        # Calculate type-specific statistics
        if dtype in (DataType.INTEGER, DataType.FLOAT):
            numeric_vals = [float(v) for v in non_null if isinstance(v, (int, float))]
            stats = _calculate_numeric_stats(numeric_vals, col)
        elif dtype == DataType.STRING:
            str_vals = [str(v) for v in non_null]
            stats = _calculate_string_stats(str_vals, col)
        elif dtype in (DataType.DATETIME, DataType.DATE):
            dt_vals = [v for v in non_null if isinstance(v, (datetime, date))]
            stats = _calculate_datetime_stats(dt_vals, col)
        else:
            # Generic stats for other types
            counter = Counter(non_null)
            stats = ColumnStatistics(
                column_name=col,
                data_type=dtype,
                sample_values=tuple(list(counter.keys())[:10]),
                distinct_count=len(counter),
            )

        # Update with null statistics
        stats = ColumnStatistics(
            **{k: v for k, v in stats.__dict__.items() if not k.startswith("null")},
            null_count=null_count,
            null_percentage=null_pct,
            distinct_count=stats.distinct_count if stats.distinct_count > 0 else 0,
        )

        # Count unique values (appears exactly once)
        value_counts = Counter(non_null)
        unique_count = sum(1 for count in value_counts.values() if count == 1)
        stats = ColumnStatistics(
            **stats.__dict__,
            unique_count=unique_count,
        )

        column_stats[col] = stats

    # Infer schema if requested
    inferred_schema = None
    if infer_schema:
        schema_fields = []
        for col, stats in column_stats.items():
            schema_fields.append(
                SchemaField(
                    name=col,
                    data_type=stats.data_type,
                    required=stats.null_percentage == 0.0,  # Required if no nulls
                    nullable=stats.null_percentage > 0.0,
                )
            )
        inferred_schema = Schema(
            name=f"inferred_{int(datetime.utcnow().timestamp())}",
            fields=tuple(schema_fields),
        )

    # Calculate duration
    duration_ms = (time.time() - start_time) * 1000

    return DataProfile(
        total_rows=len(records),
        total_columns=len(columns),
        column_stats=column_stats,
        inferred_schema=inferred_schema,
        duration_ms=duration_ms,
    )


def profile_column(
    records: Sequence[DataRecord],
    column: str,
) -> ColumnStatistics | None:
    """
    Profile a single column.

    Args:
        records: Records to analyze
        column: Name of column to profile

    Returns:
        ColumnStatistics for the column, or None if column doesn't exist
    """
    if not records:
        return None

    # Get column values
    values = []
    for record in records:
        val = record.get(column) if hasattr(record, "get") else record.data.get(column)
        values.append(val)

    if not values or all(v is None for v in values):
        return None

    # Get full profile and extract column stats
    profile = profile_data(records, infer_schema=False)
    return profile.column_stats.get(column)


__all__ = [
    "ColumnStatistics",
    "DataProfile",
    "profile_data",
    "profile_column",
]
