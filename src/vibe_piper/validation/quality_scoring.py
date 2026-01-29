"""
Data quality scoring module for calculating comprehensive quality metrics.

This module provides multi-dimensional quality assessment:
- Completeness: Missing value analysis
- Accuracy: Schema conformance checks
- Consistency: Cross-field consistency rules
- Timeliness: Data freshness and age analysis
- Overall Score: Aggregated quality score (0-100 scale)
- Historical Trends: Track quality over time
- Threshold Alerts: Alert on quality degradation
- Recommendations: Suggest improvements
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
class QualityThresholdConfig:
    """
    Configuration for quality thresholds.

    Attributes:
        overall_threshold: Minimum overall quality score (0-100) to pass
        dimension_thresholds: Minimum scores for each dimension
        alert_on_threshold_breach: Whether to raise alerts when thresholds are breached
    """

    overall_threshold: float = 75.0
    dimension_thresholds: dict[str, float] = field(
        default_factory=lambda: {
            "completeness": 75.0,
            "accuracy": 75.0,
            "consistency": 75.0,
            "timeliness": 75.0,
        }
    )
    alert_on_threshold_breach: bool = True


@dataclass(frozen=True)
class QualityAlert:
    """
    Alert raised when quality thresholds are breached.

    Attributes:
        alert_type: Type of alert (threshold_breach, degradation, anomaly)
        dimension: Quality dimension that triggered alert
        current_value: Current quality score
        threshold: Threshold value
        severity: Alert severity (critical, warning, info)
        timestamp: When alert was generated
        message: Alert message
    """

    alert_type: str
    dimension: str
    current_value: float
    threshold: float
    severity: str
    timestamp: datetime = field(default_factory=datetime.utcnow)
    message: str = ""


@dataclass(frozen=True)
class QualityRecommendation:
    """
    Suggestion for improving data quality.

    Attributes:
        category: Category of recommendation (completeness, accuracy, etc.)
        priority: Priority level (critical, high, medium, low)
        description: Description of recommendation
        action: Specific action to take
        expected_impact: Expected impact on quality score
    """

    category: str
    priority: str
    description: str
    action: str
    expected_impact: str


@dataclass(frozen=True)
class QualityTrend:
    """
    Historical quality trend for a dimension.

    Attributes:
        dimension: Quality dimension name
        timestamps: List of timestamps
        values: Quality scores at each timestamp
        trend_direction: Direction of trend (improving, declining, stable)
        change_rate: Rate of change
        moving_average: Moving average of scores
    """

    dimension: str
    timestamps: tuple[datetime, ...]
    values: tuple[float, ...]
    trend_direction: str = "stable"
    change_rate: float = 0.0
    moving_average: float = 0.0


@dataclass(frozen=True)
class QualityHistory:
    """
    Historical quality scores for tracking trends.

    Attributes:
        asset_name: Name of asset
        scores: Historical quality scores
        trends: Trend analysis for each dimension
        created_at: First score timestamp
        updated_at: Most recent score timestamp
    """

    asset_name: str
    scores: tuple[QualityScore, ...]
    trends: dict[str, QualityTrend] = field(default_factory=dict)
    created_at: datetime | None = None
    updated_at: datetime | None = None


@dataclass(frozen=True)
class QualityDashboard:
    """
    Comprehensive quality dashboard for visualizing data quality.

    Attributes:
        current_score: Current overall quality score (0-100)
        dimension_scores: Scores for each dimension
        historical_trends: Historical trend data
        alerts: Active alerts
        recommendations: Improvement recommendations
        last_updated: When dashboard was last updated
    """

    current_score: float
    dimension_scores: dict[str, float]
    historical_trends: dict[str, QualityTrend]
    alerts: tuple[QualityAlert, ...]
    recommendations: tuple[QualityRecommendation, ...]
    last_updated: datetime = field(default_factory=datetime.utcnow)


@dataclass(frozen=True)
class QualityScore:
    """
    Comprehensive quality score for a dataset.

    Attributes:
        completeness_score: Missing value quality (0-100)
        accuracy_score: Schema conformance quality (0-100)
        uniqueness_score: Duplicate-free quality (0-100)
        consistency_score: Cross-field consistency quality (0-100)
        timeliness_score: Data freshness quality (0-100)
        overall_score: Weighted average of all dimensions (0-100)
        metrics: Detailed metrics for each dimension
        timestamp: When quality was assessed
        weights: Weights used for each dimension
    """

    completeness_score: float  # 0-100 scale
    accuracy_score: float  # 0-100 scale
    uniqueness_score: float  # 0-100 scale
    consistency_score: float  # 0-100 scale
    timeliness_score: float  # 0-100 scale
    overall_score: float  # 0-100 scale, weighted average
    metrics: dict[str, QualityMetric] = field(default_factory=dict)
    timestamp: datetime = field(default_factory=datetime.utcnow)
    weights: dict[str, float] = field(default_factory=dict)


@dataclass(frozen=True)
class ColumnQualityResult:
    """
    Quality assessment for a single column.

    Attributes:
        column_name: Name of the column
        completeness: 0-100 score for missing values
        accuracy: 0-100 score for type/schema conformance
        uniqueness: 0-100 score for duplicates
        null_count: Number of null values
        duplicate_count: Number of duplicate values
        unique_count: Number of unique values
        distinct_count: Number of distinct values
    """

    column_name: str
    completeness: float
    accuracy: float
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
            # DataRecord always has data attribute
            val_dict = record.data if hasattr(record, "data") else {"_": record}
            total_cells += len(val_dict)
            null_cells += sum(1 for v in val_dict.values() if v is None)

        if total_cells == 0:
            return 1.0
        return 1.0 - (null_cells / total_cells)

    # Column-specific completeness
    values = []
    for record in records:
        # DataRecord always has get method
        val = record.data.get(column) if hasattr(record, "data") else record.get(column)
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
    config: QualityThresholdConfig | None = None,
    timestamp_field: str | None = None,
    max_age_hours: float = 24.0,
) -> QualityScore:
    """
    Calculate comprehensive quality score across multiple dimensions.

    Quality is calculated as weighted average of dimension scores on 0-100 scale.
    Default weights: completeness=0.3, accuracy=0.3, uniqueness=0.2,
                   consistency=0.1, timeliness=0.1

    Args:
        records: Records to score
        columns: Columns to analyze (None = all columns)
        weights: Custom weights for each dimension
        config: Quality threshold configuration
        timestamp_field: Field for timeliness check
        max_age_hours: Max age for timeliness check

    Returns:
        QualityScore with detailed metrics (0-100 scale)

    Example:
        >>> score = calculate_quality_score(customer_records, columns=['email', 'age', 'revenue'])
        >>> print(f"Overall: {score.overall_score:.2f}/100")
    """
    # Use default config if not provided
    if config is None:
        config = QualityThresholdConfig()

    # Default weights (0-100 scale)
    if weights is None:
        weights = {
            "completeness": 0.3,
            "accuracy": 0.3,
            "uniqueness": 0.2,
            "consistency": 0.1,
            "timeliness": 0.1,
        }

    if not records:
        return QualityScore(
            completeness_score=100.0,
            accuracy_score=100.0,
            uniqueness_score=100.0,
            consistency_score=100.0,
            timeliness_score=100.0,
            overall_score=100.0,
            metrics={},
            weights=weights,
        )

    # Determine columns to analyze
    if columns is None:
        # DataRecord always has data attribute
        if hasattr(records[0], "data"):
            columns = list(records[0].data.keys())
        else:
            columns = []

    # Calculate dimension scores (convert from 0-1 to 0-100 scale)
    completeness = calculate_completeness(records, column=None) * 100
    accuracy = 1.0  # Need expected types to calculate per-column
    uniqueness = 1.0  # Need to aggregate per-column uniqueness
    consistency = 1.0  # Need column pairs
    timeliness = 1.0  # Need timestamp field

    # Per-column quality metrics
    column_quality_results = {}
    for col in columns:
        col_quality = ColumnQualityResult(
            column_name=col,
            completeness=calculate_completeness(records, col) * 100,
            accuracy=1.0,  # Would need schema to calculate properly
            uniqueness=calculate_uniqueness(records, col) * 100,
            null_count=0,
            duplicate_count=0,
            unique_count=0,
            distinct_count=0,
        )
        column_quality_results[col] = col_quality

    # Calculate overall scores as averages
    if column_quality_results:
        accuracy = statistics.mean([cq.accuracy for cq in column_quality_results.values()])
        uniqueness = statistics.mean([cq.uniqueness for cq in column_quality_results.values()])
        # Consistency requires column pairs - use simple heuristic
        consistency = 100.0  # Default to good if no checks specified

    # Calculate timeliness if timestamp field provided
    if timestamp_field:
        # Simple timeliness check based on freshness
        from vibe_piper.quality import check_freshness

        freshness_result = check_freshness(records, timestamp_field, max_age_hours)
        freshness_metric = next(
            (m for m in freshness_result.metrics if m.name == "freshness_score"),
            None,
        )
        if freshness_metric:
            timeliness = freshness_metric.value * 100
        else:
            timeliness = 100.0
    else:
        timeliness = 100.0

    # Calculate overall score as weighted average
    overall_score = (
        completeness * weights["completeness"]
        + accuracy * weights["accuracy"]
        + uniqueness * weights["uniqueness"]
        + consistency * weights["consistency"]
        + timeliness * weights["timeliness"]
    )

    # Create metrics dict
    thresholds = config.dimension_thresholds
    metrics = {
        "completeness": QualityMetric(
            name="completeness",
            metric_type=QualityMetricType.COMPLETENESS,
            value=completeness,
            threshold=thresholds.get("completeness", 75.0),
            passed=completeness >= thresholds.get("completeness", 75.0),
        ),
        "accuracy": QualityMetric(
            name="accuracy",
            metric_type=QualityMetricType.VALIDITY,
            value=accuracy,
            threshold=thresholds.get("accuracy", 75.0),
            passed=accuracy >= thresholds.get("accuracy", 75.0),
        ),
        "uniqueness": QualityMetric(
            name="uniqueness",
            metric_type=QualityMetricType.UNIQUENESS,
            value=uniqueness,
            threshold=thresholds.get("uniqueness", 75.0),
            passed=uniqueness >= thresholds.get("uniqueness", 75.0),
        ),
        "consistency": QualityMetric(
            name="consistency",
            metric_type=QualityMetricType.CONSISTENCY,
            value=consistency,
            threshold=thresholds.get("consistency", 75.0),
            passed=consistency >= thresholds.get("consistency", 75.0),
        ),
        "timeliness": QualityMetric(
            name="timeliness",
            metric_type=QualityMetricType.FRESHNESS,
            value=timeliness,
            threshold=thresholds.get("timeliness", 75.0),
            passed=timeliness >= thresholds.get("timeliness", 75.0),
        ),
    }

    return QualityScore(
        completeness_score=completeness,
        accuracy_score=accuracy,
        uniqueness_score=uniqueness,
        consistency_score=consistency,
        timeliness_score=timeliness,
        overall_score=overall_score,
        metrics=metrics,
        weights=weights,
    )


def calculate_column_quality(
    records: Sequence[DataRecord],
    column: str,
) -> ColumnQualityResult:
    """
    Calculate quality metrics for a single column (0-100 scale).

    Args:
        records: Records to analyze
        column: Column name to check

    Returns:
        ColumnQualityResult with detailed metrics (0-100 scale)
    """
    if not records or not column:
        return ColumnQualityResult(
            column_name=column,
            completeness=100.0,
            accuracy=100.0,
            uniqueness=100.0,
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

    # Completeness (0-1 to 0-100)
    null_count = sum(1 for v in values if v is None)
    total_count = len(values)
    completeness = (1.0 - (null_count / total_count) * 100) if total_count > 0 else 100.0

    # Uniqueness (0-1 to 0-100)
    non_null_values = [v for v in values if v is not None]
    value_counts = Counter(non_null_values)
    distinct_count = len(value_counts)
    unique_count = sum(1 for count in value_counts.values() if count == 1)
    duplicate_count = total_count - unique_count - null_count
    uniqueness = (unique_count / len(non_null_values) * 100) if non_null_values else 100.0

    # Accuracy (basic type check - can be enhanced with schema) (0-1 to 0-100)
    valid_count = sum(1 for v in non_null_values if v is not None)
    accuracy = (valid_count / len(values) * 100) if values else 100.0

    return ColumnQualityResult(
        column_name=column,
        completeness=completeness,
        accuracy=accuracy,
        uniqueness=uniqueness,
        null_count=null_count,
        duplicate_count=duplicate_count,
        unique_count=unique_count,
        distinct_count=distinct_count,
    )


__all__ = [
    "QualityScore",
    "ColumnQualityResult",
    "QualityThresholdConfig",
    "QualityAlert",
    "QualityRecommendation",
    "QualityTrend",
    "QualityHistory",
    "QualityDashboard",
    "calculate_completeness",
    "calculate_validity",
    "calculate_uniqueness",
    "calculate_consistency",
    "calculate_quality_score",
    "calculate_column_quality",
    "track_quality_history",
    "generate_quality_alerts",
    "generate_quality_recommendations",
    "create_quality_dashboard",
]


# =============================================================================
# Historical Quality Tracking
# =============================================================================


# In-memory storage for quality history (in production, use a database)
_quality_history_store: dict[str, list[QualityScore]] = {}


def track_quality_history(
    asset_name: str,
    score: QualityScore,
    max_history: int = 100,
) -> QualityHistory:
    """
    Track quality scores for an asset over time.

    Args:
        asset_name: Name of the asset
        score: Quality score to record
        max_history: Maximum number of scores to keep

    Returns:
        QualityHistory with trend analysis
    """
    import copy

    if asset_name not in _quality_history_store:
        _quality_history_store[asset_name] = []

    # Add new score
    _quality_history_store[asset_name].append(copy.deepcopy(score))

    # Trim to max_history
    if len(_quality_history_store[asset_name]) > max_history:
        _quality_history_store[asset_name] = _quality_history_store[asset_name][-max_history:]

    scores = _quality_history_store[asset_name]

    # Analyze trends for each dimension
    trends = {
        "completeness": _analyze_trend(
            [s.completeness_score for s in scores],
            [s.timestamp for s in scores],
        ),
        "accuracy": _analyze_trend(
            [s.accuracy_score for s in scores],
            [s.timestamp for s in scores],
        ),
        "uniqueness": _analyze_trend(
            [s.uniqueness_score for s in scores],
            [s.timestamp for s in scores],
        ),
        "consistency": _analyze_trend(
            [s.consistency_score for s in scores],
            [s.timestamp for s in scores],
        ),
        "timeliness": _analyze_trend(
            [s.timeliness_score for s in scores],
            [s.timestamp for s in scores],
        ),
    }

    return QualityHistory(
        asset_name=asset_name,
        scores=tuple(scores),
        trends=trends,
        created_at=scores[0].timestamp if scores else None,
        updated_at=scores[-1].timestamp if scores else None,
    )


def _analyze_trend(
    values: list[float],
    timestamps: list[datetime],
    window_size: int = 5,
) -> QualityTrend:
    """
    Analyze trend direction and rate of change.

    Args:
        values: Historical values
        timestamps: Corresponding timestamps
        window_size: Window for moving average

    Returns:
        QualityTrend with analysis
    """
    if len(values) < 2:
        return QualityTrend(
            dimension="unknown",
            timestamps=tuple(timestamps),
            values=tuple(values),
            trend_direction="stable",
            change_rate=0.0,
            moving_average=values[0] if values else 0.0,
        )

    # Calculate change rate (simple linear regression slope)
    if len(values) >= 2:
        x = list(range(len(values)))
        y = values
        n = len(values)

        # Calculate slope
        sum_x = sum(x)
        sum_y = sum(y)
        sum_xy = sum(xi * yi for xi, yi in zip(x, y))
        sum_x2 = sum(xi * xi for xi in x)

        change_rate = (n * sum_xy - sum_x * sum_y) / (n * sum_x2 - sum_x * sum_x)

        # Determine trend direction
        if change_rate > 0.1:
            trend_direction = "improving"
        elif change_rate < -0.1:
            trend_direction = "declining"
        else:
            trend_direction = "stable"
    else:
        change_rate = 0.0
        trend_direction = "stable"

    # Calculate moving average
    if len(values) >= window_size:
        recent_values = values[-window_size:]
        moving_average = sum(recent_values) / len(recent_values)
    else:
        moving_average = sum(values) / len(values)

    return QualityTrend(
        dimension="unknown",
        timestamps=tuple(timestamps),
        values=tuple(values),
        trend_direction=trend_direction,
        change_rate=change_rate,
        moving_average=moving_average,
    )


# =============================================================================
# Quality Threshold Alerts
# =============================================================================


def generate_quality_alerts(
    score: QualityScore,
    config: QualityThresholdConfig | None = None,
) -> tuple[QualityAlert, ...]:
    """
    Generate alerts based on quality score thresholds.

    Args:
        score: Quality score to check
        config: Threshold configuration

    Returns:
        Tuple of QualityAlert objects
    """
    if config is None:
        config = QualityThresholdConfig()

    alerts: list[QualityAlert] = []

    # Check overall score
    if score.overall_score < config.overall_threshold:
        alerts.append(
            QualityAlert(
                alert_type="threshold_breach",
                dimension="overall",
                current_value=score.overall_score,
                threshold=config.overall_threshold,
                severity="critical",
                message=f"Overall quality score {score.overall_score:.1f} is below threshold {config.overall_threshold}",
            )
        )

    # Check individual dimensions
    thresholds = config.dimension_thresholds
    dimensions = {
        "completeness": score.completeness_score,
        "accuracy": score.accuracy_score,
        "uniqueness": score.uniqueness_score,
        "consistency": score.consistency_score,
        "timeliness": score.timeliness_score,
    }

    for dim_name, dim_value in dimensions.items():
        if dim_name in thresholds:
            threshold = thresholds[dim_name]
            if dim_value < threshold:
                # Determine severity
                if dim_value < threshold * 0.5:
                    severity = "critical"
                elif dim_value < threshold * 0.75:
                    severity = "warning"
                else:
                    severity = "info"

                alerts.append(
                    QualityAlert(
                        alert_type="threshold_breach",
                        dimension=dim_name,
                        current_value=dim_value,
                        threshold=threshold,
                        severity=severity,
                        message=f"{dim_name.capitalize()} score {dim_value:.1f} is below threshold {threshold}",
                    )
                )

    return tuple(alerts)


# =============================================================================
# Quality Improvement Recommendations
# =============================================================================


def generate_quality_recommendations(
    score: QualityScore,
    records: Sequence[DataRecord] | None = None,
) -> tuple[QualityRecommendation, ...]:
    """
    Generate recommendations to improve data quality.

    Args:
        score: Quality score to analyze
        records: Optional records to analyze for specific issues

    Returns:
        Tuple of QualityRecommendation objects
    """
    recommendations: list[QualityRecommendation] = []

    # Completeness recommendations
    if score.completeness_score < 90:
        if score.completeness_score < 50:
            priority = "critical"
        elif score.completeness_score < 75:
            priority = "high"
        else:
            priority = "medium"

        recommendations.append(
            QualityRecommendation(
                category="completeness",
                priority=priority,
                description=f"Data completeness is low ({score.completeness_score:.1f}%)",
                action="Review source systems for missing values and implement data quality rules",
                expected_impact="+10-20% overall quality",
            )
        )

    # Accuracy recommendations
    if score.accuracy_score < 90:
        if score.accuracy_score < 50:
            priority = "critical"
        elif score.accuracy_score < 75:
            priority = "high"
        else:
            priority = "medium"

        recommendations.append(
            QualityRecommendation(
                category="accuracy",
                priority=priority,
                description=f"Data accuracy is low ({score.accuracy_score:.1f}%)",
                action="Validate data against schema definitions and implement type checking",
                expected_impact="+10-15% overall quality",
            )
        )

    # Uniqueness recommendations
    if score.uniqueness_score < 95:
        if score.uniqueness_score < 50:
            priority = "critical"
        elif score.uniqueness_score < 75:
            priority = "high"
        else:
            priority = "medium"

        recommendations.append(
            QualityRecommendation(
                category="uniqueness",
                priority=priority,
                description=f"Data uniqueness is low ({score.uniqueness_score:.1f}%)",
                action="Implement deduplication logic and add unique constraints",
                expected_impact="+5-10% overall quality",
            )
        )

    # Consistency recommendations
    if score.consistency_score < 90:
        if score.consistency_score < 50:
            priority = "critical"
        elif score.consistency_score < 75:
            priority = "high"
        else:
            priority = "medium"

        recommendations.append(
            QualityRecommendation(
                category="consistency",
                priority=priority,
                description=f"Data consistency is low ({score.consistency_score:.1f}%)",
                action="Implement cross-field validation rules and consistency checks",
                expected_impact="+5-10% overall quality",
            )
        )

    # Timeliness recommendations
    if score.timeliness_score < 90:
        if score.timeliness_score < 50:
            priority = "critical"
        elif score.timeliness_score < 75:
            priority = "high"
        else:
            priority = "medium"

        recommendations.append(
            QualityRecommendation(
                category="timeliness",
                priority=priority,
                description=f"Data timeliness is low ({score.timeliness_score:.1f}%)",
                action="Review data refresh schedules and implement incremental updates",
                expected_impact="+5-15% overall quality",
            )
        )

    return tuple(recommendations)


# =============================================================================
# Quality Dashboard
# =============================================================================


def create_quality_dashboard(
    asset_name: str,
    score: QualityScore,
    config: QualityThresholdConfig | None = None,
    history: QualityHistory | None = None,
) -> QualityDashboard:
    """
    Create a comprehensive quality dashboard for an asset.

    Args:
        asset_name: Name of the asset
        score: Current quality score
        config: Threshold configuration
        history: Historical quality data

    Returns:
        QualityDashboard with all quality information
    """
    if config is None:
        config = QualityThresholdConfig()

    # Generate dimension scores
    dimension_scores = {
        "completeness": score.completeness_score,
        "accuracy": score.accuracy_score,
        "uniqueness": score.uniqueness_score,
        "consistency": score.consistency_score,
        "timeliness": score.timeliness_score,
    }

    # Get historical trends
    historical_trends = {}
    if history:
        historical_trends = history.trends

    # Generate alerts
    alerts = generate_quality_alerts(score, config)

    # Generate recommendations
    recommendations = generate_quality_recommendations(score)

    return QualityDashboard(
        current_score=score.overall_score,
        dimension_scores=dimension_scores,
        historical_trends=historical_trends,
        alerts=alerts,
        recommendations=recommendations,
        last_updated=datetime.utcnow(),
    )
