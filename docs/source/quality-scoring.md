# Quality Scoring

Comprehensive data quality scoring with multi-dimensional assessment and tracking.

## Overview

The quality scoring module provides:

- **0-100 scale scoring** for intuitive quality measurement
- **Multi-dimensional assessment**: Completeness, Accuracy, Uniqueness, Consistency, Timeliness
- **Configurable weights**: Customize dimension weights based on business priorities
- **Historical tracking**: Track quality trends over time
- **Threshold alerts**: Automated alerts when quality degrades
- **Improvement recommendations**: Actionable suggestions to improve data quality
- **Quality dashboard**: Comprehensive view of all quality information

## Quick Start

```python
from vibe_piper import (
    DataRecord,
    Schema,
    SchemaField,
    DataType,
)
from vibe_piper.validation import (
    calculate_quality_score,
    create_quality_dashboard,
    track_quality_history,
    QualityThresholdConfig,
)

# Create sample data
schema = Schema(
    name="customers",
    fields=(
        SchemaField(name="id", data_type=DataType.INTEGER),
        SchemaField(name="email", data_type=DataType.STRING),
        SchemaField(name="created_at", data_type=DataType.DATETIME),
    ),
)

records = [
    DataRecord(data={"id": 1, "email": "alice@example.com", "created_at": "2026-01-29T10:00:00Z"}, schema=schema),
    DataRecord(data={"id": 2, "email": "bob@example.com", "created_at": "2026-01-29T11:00:00Z"}, schema=schema),
]

# Calculate quality score
score = calculate_quality_score(
    records,
    timestamp_field="created_at",
    max_age_hours=24,
)

print(f"Overall quality: {score.overall_score:.1f}/100")
print(f"  Completeness: {score.completeness_score:.1f}/100")
print(f"  Accuracy: {score.accuracy_score:.1f}/100")
print(f"  Timeliness: {score.timeliness_score:.1f}/100")
```

## Quality Dimensions

### Completeness
Measures how much data is present vs. missing.

- **Calculation**: `(1 - null_count / total_count) * 100`
- **Ideal score**: 100% (no missing values)
- **Impact**: High impact on data usability

### Accuracy
Measures how well data conforms to schema definitions.

- **Calculation**: Percentage of records passing schema validation
- **Ideal score**: 100% (all records valid)
- **Impact**: Critical for downstream applications

### Uniqueness
Measures duplicate-free quality of data.

- **Calculation**: `(unique_count / total_count) * 100`
- **Ideal score**: 100% (no duplicates)
- **Impact**: Medium impact depending on use case

### Consistency
Measures cross-field consistency and logical constraints.

- **Calculation**: Requires consistency rules, defaults to 100% if no rules defined
- **Ideal score**: 100% (all fields consistent)
- **Impact**: Medium impact for analytics

### Timeliness
Measures data freshness and age.

- **Calculation**: Based on freshness check, converted to 0-100 scale
- **Ideal score**: 100% (all data within acceptable age)
- **Impact**: High impact for time-sensitive analysis

## Configurable Weights

Customize dimension importance using weights:

```python
# Default weights
default_weights = {
    "completeness": 0.3,
    "accuracy": 0.3,
    "uniqueness": 0.2,
    "consistency": 0.1,
    "timeliness": 0.1,
}

# Custom weights
custom_weights = {
    "completeness": 0.4,  # Emphasize completeness
    "accuracy": 0.4,  # Emphasize accuracy
    "uniqueness": 0.1,
    "consistency": 0.05,
    "timeliness": 0.05,
}

score = calculate_quality_score(records, weights=custom_weights)
```

## Historical Quality Tracking

Track quality over time to identify trends:

```python
from vibe_piper.validation import track_quality_history

# Track first score
history1 = track_quality_history("customers", score1)

# Track another score (e.g., after data refresh)
history2 = track_quality_history("customers", score2)

# Access trend analysis
for dimension, trend in history2.trends.items():
    print(f"{dimension}: {trend.trend_direction} (rate: {trend.change_rate:.3f})")
```

**Trend directions**:
- `improving`: Quality is getting better over time
- `declining`: Quality is getting worse
- `stable`: Quality is consistent

## Quality Threshold Alerts

Configure automated alerts for quality degradation:

```python
from vibe_piper.validation import (
    QualityThresholdConfig,
    generate_quality_alerts,
)

# Configure thresholds
config = QualityThresholdConfig(
    overall_threshold=80.0,  # Alert if overall score < 80
    dimension_thresholds={
        "completeness": 90.0,  # Alert if completeness < 90
        "accuracy": 95.0,
        "uniqueness": 95.0,
        "consistency": 90.0,
        "timeliness": 85.0,
    },
    alert_on_threshold_breach=True,
)

# Generate alerts
alerts = generate_quality_alerts(score, config)

for alert in alerts:
    print(f"[{alert.severity}] {alert.dimension}: {alert.message}")
```

**Alert severity levels**:
- `critical`: Score < 50% of threshold
- `warning`: Score < 75% of threshold
- `info`: Score < threshold

## Quality Improvement Recommendations

Get actionable suggestions to improve data quality:

```python
from vibe_piper.validation import generate_quality_recommendations

recommendations = generate_quality_recommendations(score)

for rec in recommendations:
    print(f"[{rec.priority}] {rec.category}")
    print(f"  {rec.description}")
    print(f"  Action: {rec.action}")
    print(f"  Expected impact: {rec.expected_impact}")
    print()
```

**Recommendation priorities**:
- `critical`: Immediate action required
- `high`: Action recommended soon
- `medium`: Consider implementing
- `low`: Nice to have, not urgent

## Quality Dashboard

Get a comprehensive view of quality information:

```python
from vibe_piper.validation import create_quality_dashboard

dashboard = create_quality_dashboard("customers", score)

print(f"Current Score: {dashboard.current_score:.1f}/100")
print(f"\nDimension Scores:")
for dim, value in dashboard.dimension_scores.items():
    print(f"  {dim}: {value:.1f}/100")

print(f"\nActive Alerts ({len(dashboard.alerts)}):")
for alert in dashboard.alerts:
    print(f"  - {alert.dimension}: {alert.message}")

print(f"\nRecommendations ({len(dashboard.recommendations)}):")
for rec in dashboard.recommendations:
    print(f"  - {rec.action}")
```

## Column-Level Quality

Assess quality at the column level:

```python
from vibe_piper.validation import calculate_column_quality

col_quality = calculate_column_quality(records, "email")

print(f"Column: {col_quality.column_name}")
print(f"  Completeness: {col_quality.completeness:.1f}%")
print(f"  Accuracy: {col_quality.accuracy:.1f}%")
print(f"  Uniqueness: {col_quality.uniqueness:.1f}%")
print(f"  Null count: {col_quality.null_count}")
print(f"  Duplicate count: {col_quality.duplicate_count}")
```

## Best Practices

### Setting Thresholds

Choose thresholds based on your data requirements:

| Use Case | Overall Threshold | Dimension Thresholds |
|----------|------------------|---------------------|
| Critical financial data | 95-100 | 95-100 |
| Customer PII data | 90-100 | 90-100 |
| Analytics data | 75-85 | 70-90 |
| Internal logging | 60-75 | 50-70 |

### Choosing Weights

Adjust weights based on your priorities:

```python
# For accuracy-critical systems
accuracy_focused = {
    "completeness": 0.2,
    "accuracy": 0.5,  # Higher weight
    "uniqueness": 0.1,
    "consistency": 0.1,
    "timeliness": 0.1,
}

# For freshness-critical systems
timeliness_focused = {
    "completeness": 0.2,
    "accuracy": 0.2,
    "uniqueness": 0.2,
    "consistency": 0.1,
    "timeliness": 0.3,  # Higher weight
}
```

### Monitoring Quality Trends

Track quality regularly to detect issues early:

1. **Daily scoring**: Run quality checks on fresh data
2. **Trend monitoring**: Set up alerts for declining trends
3. **Root cause analysis**: Investigate sudden quality drops
4. **Continuous improvement**: Iterate on recommendations

## Integration with Validation

Quality scoring integrates with Vibe Piper's validation framework:

```python
from vibe_piper.validation import (
    ValidationSuite,
    create_validation_suite,
    calculate_quality_score,
)

# Create validation suite
suite = create_validation_suite("my_suite")

# Add validation rules
suite.expect_column_values_to_not_be_null("email")
suite.expect_column_values_to_be_unique("id")

# Run validation and get quality score
result = suite.validate(records)
quality_score = calculate_quality_score(records, schema=result.schema)
```

## API Reference

### QualityScore

Main quality score result with all dimensions on 0-100 scale.

**Attributes**:
- `completeness_score`: float (0-100)
- `accuracy_score`: float (0-100)
- `uniqueness_score`: float (0-100)
- `consistency_score`: float (0-100)
- `timeliness_score`: float (0-100)
- `overall_score`: float (0-100), weighted average
- `metrics`: Dict[str, QualityMetric], detailed metrics
- `timestamp`: datetime, when assessed
- `weights`: Dict[str, float], weights used

### QualityThresholdConfig

Configuration for quality thresholds.

**Attributes**:
- `overall_threshold`: float, minimum overall score (default: 75.0)
- `dimension_thresholds`: Dict[str, float], per-dimension thresholds
- `alert_on_threshold_breach`: bool, enable/disable alerts

### QualityAlert

Alert raised when thresholds are breached.

**Attributes**:
- `alert_type`: str, alert category
- `dimension`: str, quality dimension
- `current_value`: float, current score
- `threshold`: float, threshold value
- `severity`: str, 'critical'|'warning'|'info'
- `timestamp`: datetime, alert time
- `message`: str, alert description

### QualityRecommendation

Suggestion for improving quality.

**Attributes**:
- `category`: str, quality dimension
- `priority`: str, 'critical'|'high'|'medium'|'low'
- `description`: str, issue description
- `action`: str, specific action to take
- `expected_impact`: str, expected improvement

### QualityTrend

Historical trend for a quality dimension.

**Attributes**:
- `dimension`: str, dimension name
- `timestamps`: Tuple[datetime], historical timestamps
- `values`: Tuple[float], historical scores
- `trend_direction`: str, 'improving'|'declining'|'stable'
- `change_rate`: float, rate of change
- `moving_average`: float, moving average

### QualityHistory

Complete quality history for an asset.

**Attributes**:
- `asset_name`: str, asset identifier
- `scores`: Tuple[QualityScore], historical scores
- `trends`: Dict[str, QualityTrend], trend analysis
- `created_at`: datetime, first score time
- `updated_at`: datetime, last score time

### QualityDashboard

Comprehensive quality dashboard.

**Attributes**:
- `current_score`: float, current overall score (0-100)
- `dimension_scores`: Dict[str, float], scores per dimension
- `historical_trends`: Dict[str, QualityTrend], trend data
- `alerts`: Tuple[QualityAlert], active alerts
- `recommendations`: Tuple[QualityRecommendation], improvement suggestions
- `last_updated`: datetime, dashboard update time

## See Also

- [Validation Framework](validation.md) - Comprehensive data validation
- [Data Profiling](data-profiling.md) - Statistical data analysis
- [Drift Detection](drift-detection.md) - Distribution change detection
- [Anomaly Detection](anomaly-detection.md) - Outlier detection
