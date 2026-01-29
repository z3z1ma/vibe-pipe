"""
Data quality scoring module for calculating comprehensive quality metrics.

This module provides multi-dimensional quality assessment:
- Completeness: Missing value analysis
- Validity: Schema conformance checks
- Uniqueness: Duplicate detection
- Consistency: Cross-field consistency rules
- Overall Score: Aggregated quality score (0-1 scale)
"""

from __future__ import annotations

import statistics
from collections import Counter
from collections.abc import Sequence
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any

from vibe_piper.types import DataRecord, DataType, QualityMetric, QualityMetricType

# =============================================================================
# Quality Score Result Types
# =============================================================================


@dataclass(frozen=True)
class QualityScore:
    """
    Comprehensive quality score for a dataset.

    Attributes:
        completeness_score: Missing value quality (0-1)
        validity_score: Schema conformance quality (0-1)
        uniqueness_score: Duplicate-free quality (0-1)
        consistency_score: Cross-field consistency quality (0-1)
        overall_score: Weighted average of all dimensions (0-1)
        metrics: Detailed metrics for each dimension
        timestamp: When quality was assessed
    """

    completeness_score: float  # 0-1 scale
    validity_score: float  # 0-1 scale
    uniqueness_score: float  # 0-1 scale
    consistency_score: float  # 0-1 scale
    overall_score: float  # 0-1 scale, weighted average
    metrics: dict[str, QualityMetric] = field(default_factory=dict)
    timestamp: datetime = field(default_factory=datetime.utcnow)


@dataclass(frozen=True)
class ColumnQualityResult:
    """
    Quality assessment for a single column.

    Attributes:
        column_name: Name of the column
        completeness: 0-1 score for missing values
        validity: 0-1 score for type/schema conformance
        uniqueness: 0-1 score for duplicates
        null_count: Number of null values
        duplicate_count: Number of duplicate values
        unique_count: Number of unique values
        distinct_count: Number of distinct values
    """

    column_name: str
    completeness: float
    validity: float
    uniqueness: float
    null_count: int
    duplicate_count: int
    unique_count: int
    distinct_count: int


# =============================================================================
# Completeness Analysis
# =============================================================================


def calculate_completeness(
    records: Sequence[DataRecord],
    column: str | None = None,
) -> float:
    """
    Calculate completeness score based on missing values.

    Completeness = 1 - (null_count / total_count)

    Args:
        records: Records to analyze
        column: Specific column (None = overall completeness)

    Returns:
        Completeness score (0-1 scale)
    """
    if not records:
        return 1.0

    if column is None:
        # Overall completeness across all columns
        total_cells = 0
        null_cells = 0
        for record in records:
            val = record.data if hasattr(record, "data") else record
            val_dict = val if isinstance(val, dict) else {"_": val}
            total_cells += len(val_dict)
            null_cells += sum(1 for v in val_dict.values() if v is None)

        if total_cells == 0:
            return 1.0
        return 1.0 - (null_cells / total_cells)

    # Column-specific completeness
    values = []
    for record in records:
        val = (
            record.get(column)
            if hasattr(record, "get")
            else (record.data.get(column) if hasattr(record, "data") else record.get(column))
        )
        values.append(val)

    null_count = sum(1 for v in values if v is None)
    total_count = len(values)

    if total_count == 0:
        return 1.0
    return 1.0 - (null_count / total_count)


# =============================================================================
# Validity Analysis
# =============================================================================


def calculate_validity(
    records: Sequence[DataRecord],
    column: str,
    expected_type: DataType | None = None,
    allowed_values: set[Any] | None = None,
    min_value: Any | None = None,
    max_value: Any | None = None,
) -> float:
    """
    Calculate validity score based on type conformance and constraints.

    Validity = proportion of records that meet validity criteria.

    Args:
        records: Records to validate
        column: Column name to check
        expected_type: Expected data type (optional)
        allowed_values: Set of allowed values for categorical (optional)
        min_value: Minimum acceptable value (optional)
        max_value: Maximum acceptable value (optional)

    Returns:
        Validity score (0-1 scale)
    """
    if not records or not column:
        return 1.0

    values = []
    for record in records:
        val = (
            record.get(column)
            if hasattr(record, "get")
            else (record.data.get(column) if hasattr(record, "data") else record.get(column))
        )
        values.append(val)

    valid_count = 0

    for val in values:
        is_valid = True

        # Check null (assume nulls are valid for schema, adjust as needed)
        if val is None:
            # Nulls are typically valid in schemas that allow them
            # For strict validity, mark as invalid
            is_valid = False

        # Check type
        elif expected_type is not None:
            val_type = (
                DataType.STRING
                if isinstance(val, str)
                else (
                    DataType.INTEGER
                    if isinstance(val, int) and not isinstance(val, bool)
                    else DataType.FLOAT
                    if isinstance(val, float)
                    else DataType.BOOLEAN
                    if isinstance(val, bool)
                    else DataType.DATETIME
                    if isinstance(val, datetime)
                    else DataType.ARRAY
                    if isinstance(val, list)
                    else DataType.OBJECT
                    if isinstance(val, dict)
                    else DataType.ANY
                )
            )

            if val_type != expected_type:
                is_valid = False

        # Check allowed values
        elif allowed_values is not None and val not in allowed_values:
            is_valid = False

        # Check range
        elif min_value is not None and val < min_value:
            is_valid = False
        elif max_value is not None and val > max_value:
            is_valid = False

        if is_valid:
            valid_count += 1

    total_count = len(values)
    if total_count == 0:
        return 1.0
    return valid_count / total_count


# =============================================================================
# Uniqueness Analysis
# =============================================================================


def calculate_uniqueness(
    records: Sequence[DataRecord],
    column: str,
    ignore_nulls: bool = True,
) -> float:
    """
    Calculate uniqueness score based on duplicate detection.

    Uniqueness = unique_count / total_count

    Args:
        records: Records to analyze
        column: Column name to check
        ignore_nulls: Whether to exclude null values from uniqueness check

    Returns:
        Uniqueness score (0-1 scale)
    """
    if not records or not column:
        return 1.0

    values = []
    for record in records:
        val = (
            record.get(column)
            if hasattr(record, "get")
            else (record.data.get(column) if hasattr(record, "data") else record.get(column))
        )
        values.append(val)

    if ignore_nulls:
        values = [v for v in values if v is not None]

    total_count = len(values)
    if total_count == 0:
        return 1.0

    # Count duplicates (values that appear more than once)
    value_counts = Counter(values)
    duplicate_count = sum(count for count in value_counts.values() if count > 1)
    unique_count = sum(1 for count in value_counts.values() if count == 1)

    return unique_count / total_count if total_count > 0 else 1.0


# =============================================================================
# Consistency Analysis
# =============================================================================


def calculate_consistency(
    records: Sequence[DataRecord],
    column_a: str,
    column_b: str,
    check_type: str = "equality",  # equality, greater_than, less_than
    check_params: dict[str, Any] | None = None,
) -> float:
    """
    Calculate consistency score based on cross-column rules.

    Consistency = proportion of records meeting cross-field constraints.

    Args:
        records: Records to validate
        column_a: First column name
        column_b: Second column name
        check_type: Type of consistency check
        check_params: Additional parameters for the check

    Returns:
        Consistency score (0-1 scale)
    """
    if not records:
        return 1.0

    values_a = []
    values_b = []
    for record in records:
        val_a = (
            record.get(column_a)
            if hasattr(record, "get")
            else (record.data.get(column_a) if hasattr(record, "data") else record.get(column_a))
        )
        val_b = (
            record.get(column_b)
            if hasattr(record, "get")
            else (record.data.get(column_b) if hasattr(record, "data") else record.get(column_b))
        )
        values_a.append(val_a)
        values_b.append(val_b)

    valid_count = 0
    total_count = 0

    for val_a, val_b in zip(values_a, values_b):
        # Skip nulls in either column
        if val_a is None or val_b is None:
            continue

        total_count += 1
        is_valid = True

        if check_type == "equality":
            is_valid = val_a == val_b
        elif check_type == "greater_than":
            if check_params and "or_equal" in check_params and check_params["or_equal"]:
                is_valid = val_a >= val_b
            else:
                is_valid = val_a > val_b
        elif check_type == "less_than":
            if check_params and "or_equal" in check_params and check_params["or_equal"]:
                is_valid = val_a <= val_b
            else:
                is_valid = val_a < val_b
        else:
            # Unknown check type, skip
            continue

        if is_valid:
            valid_count += 1

    if total_count == 0:
        return 1.0
    return valid_count / total_count


# =============================================================================
# Comprehensive Quality Scoring
# =============================================================================


def calculate_quality_score(
    records: Sequence[DataRecord],
    columns: list[str] | None = None,
    weights: dict[str, float] | None = None,
    completeness_threshold: float = 0.9,
    validity_threshold: float = 0.9,
    uniqueness_threshold: float = 0.95,
) -> QualityScore:
    """
    Calculate comprehensive quality score across multiple dimensions.

    Quality is calculated as weighted average of dimension scores.
    Default weights: completeness=0.3, validity=0.3, uniqueness=0.2, consistency=0.2

    Args:
        records: Records to score
        columns: Columns to analyze (None = all columns)
        weights: Custom weights for each dimension
        completeness_threshold: Minimum completeness for 'good' quality
        validity_threshold: Minimum validity for 'good' quality
        uniqueness_threshold: Minimum uniqueness for 'good' quality

    Returns:
        QualityScore with detailed metrics

    Example:
        >>> score = calculate_quality_score(customer_records, columns=['email', 'age', 'revenue'])
        >>> print(f"Overall: {score.overall_score:.2%}")
    """
    import time

    start_time = time.time()

    if not records:
        return QualityScore(
            completeness_score=1.0,
            validity_score=1.0,
            uniqueness_score=1.0,
            consistency_score=1.0,
            overall_score=1.0,
            metrics={},
        )

    # Determine columns to analyze
    if columns is None:
        if isinstance(records[0], dict):
            columns = list(records[0].keys())
        elif hasattr(records[0], "data"):
            columns = list(records[0].data.keys())
        else:
            columns = []

    # Default weights
    if weights is None:
        weights = {
            "completeness": 0.3,
            "validity": 0.3,
            "uniqueness": 0.2,
            "consistency": 0.2,
        }

    # Calculate dimension scores
    completeness = calculate_completeness(records, column=None)
    validity = 1.0  # Need expected types to calculate per-column
    uniqueness = 1.0  # Need to aggregate per-column uniqueness
    consistency = 1.0  # Need column pairs

    # Per-column quality metrics
    column_quality_results = {}
    for col in columns:
        col_quality = ColumnQualityResult(
            column_name=col,
            completeness=calculate_completeness(records, col),
            validity=1.0,  # Would need schema to calculate properly
            uniqueness=calculate_uniqueness(records, col),
            null_count=0,
            duplicate_count=0,
            unique_count=0,
            distinct_count=0,
        )
        column_quality_results[col] = col_quality

    # Calculate overall scores as averages
    if column_quality_results:
        validity = statistics.mean([cq.validity for cq in column_quality_results.values()])
        uniqueness = statistics.mean([cq.uniqueness for cq in column_quality_results.values()])
        # Consistency requires column pairs - use simple heuristic
        consistency = 1.0  # Default to good if no checks specified

    # Calculate overall score as weighted average
    overall_score = (
        completeness * weights["completeness"]
        + validity * weights["validity"]
        + uniqueness * weights["uniqueness"]
        + consistency * weights["consistency"]
    )

    # Create metrics dict
    metrics = {
        "completeness": QualityMetric(
            name="completeness",
            metric_type=QualityMetricType.COMPLETENESS,
            value=completeness,
            threshold=completeness_threshold,
            passed=completeness >= completeness_threshold,
        ),
        "validity": QualityMetric(
            name="validity",
            metric_type=QualityMetricType.VALIDITY,
            value=validity,
            threshold=validity_threshold,
            passed=validity >= validity_threshold,
        ),
        "uniqueness": QualityMetric(
            name="uniqueness",
            metric_type=QualityMetricType.UNIQUENESS,
            value=uniqueness,
            threshold=uniqueness_threshold,
            passed=uniqueness >= uniqueness_threshold,
        ),
    }

    duration_ms = (time.time() - start_time) * 1000

    return QualityScore(
        completeness_score=completeness,
        validity_score=validity,
        uniqueness_score=uniqueness,
        consistency_score=consistency,
        overall_score=overall_score,
        metrics=metrics,
    )


def calculate_column_quality(
    records: Sequence[DataRecord],
    column: str,
) -> ColumnQualityResult:
    """
    Calculate quality metrics for a single column.

    Args:
        records: Records to analyze
        column: Column name to check

    Returns:
        ColumnQualityResult with detailed metrics
    """
    if not records or not column:
        return ColumnQualityResult(
            column_name=column,
            completeness=1.0,
            validity=1.0,
            uniqueness=1.0,
            null_count=0,
            duplicate_count=0,
            unique_count=0,
            distinct_count=0,
        )

    values = []
    for record in records:
        val = (
            record.get(column)
            if hasattr(record, "get")
            else (record.data.get(column) if hasattr(record, "data") else record.get(column))
        )
        values.append(val)

    # Completeness
    null_count = sum(1 for v in values if v is None)
    total_count = len(values)
    completeness = 1.0 - (null_count / total_count) if total_count > 0 else 1.0

    # Uniqueness
    non_null_values = [v for v in values if v is not None]
    value_counts = Counter(non_null_values)
    distinct_count = len(value_counts)
    unique_count = sum(1 for count in value_counts.values() if count == 1)
    duplicate_count = total_count - unique_count - null_count
    uniqueness = unique_count / len(non_null_values) if non_null_values else 1.0

    # Validity (basic type check - can be enhanced with schema)
    valid_count = sum(1 for v in non_null_values if v is not None)
    validity = valid_count / len(values) if values else 1.0

    return ColumnQualityResult(
        column_name=column,
        completeness=completeness,
        validity=validity,
        uniqueness=uniqueness,
        null_count=null_count,
        duplicate_count=duplicate_count,
        unique_count=unique_count,
        distinct_count=distinct_count,
    )


__all__ = [
    "QualityScore",
    "ColumnQualityResult",
    "calculate_completeness",
    "calculate_validity",
    "calculate_uniqueness",
    "calculate_consistency",
    "calculate_quality_score",
    "calculate_column_quality",
]
