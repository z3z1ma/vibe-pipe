# Anomaly Detection

Anomaly detection identifies outliers and unusual patterns in your data. Vibe Piper provides multiple algorithms for detecting anomalies:

## Statistical Methods

### Z-score Detection

Detects outliers based on standard deviation from the mean. Values with |z| > threshold are flagged as anomalies.

```python
from vibe_piper.validation import detect_anomalies_zscore

detector = detect_anomalies_zscore("price", threshold=3.0)
result = detector(records)

print(f"Found {result.outlier_count} anomalies at indices: {result.outlier_indices}")
```

### IQR Detection

Uses Interquartile Range (Tukey's fences) to identify outliers. Default multiplier of 1.5 captures ~99.3% of normal data.

```python
from vibe_piper.validation import detect_anomalies_iqr

detector = detect_anomalies_iqr("revenue", multiplier=1.5)
result = detector(records)

print(f"Q1: {result.statistics['q1']}, Q3: {result.statistics['q3']}")
print(f"IQR bounds: [{result.statistics['lower_bound']}, {result.statistics['upper_bound']}]")
```

## ML-based Methods

### Isolation Forest

An ensemble method that isolates anomalies by randomly selecting features and split values. Anomalies have shorter path lengths.

```python
from vibe_piper.validation import detect_anomalies_isolation_forest

detector = detect_anomalies_isolation_forest("amount", contamination=0.05)
result = detector(records)

print(f"Found {result.outlier_count} anomalies")
```

### One-Class SVM

An unsupervised algorithm that learns a decision boundary and flags records outside it.

```python
from vibe_piper.validation import detect_anomalies_one_class_svm

detector = detect_anomalies_one_class_svm("metric", nu=0.1, kernel="rbf")
result = detector(records)

print(f"Found {result.outlier_count} anomalies")
```

## Advanced Features

### Anomaly Ranking

Rank records by anomaly score to identify the most extreme outliers.

```python
from vibe_piper.validation import rank_anomalies

ranker = rank_anomalies("value", top_n=10)
result = ranker(records)

print(f"Top {len(result.top_n_indices)} most anomalous records:")
for idx, score in zip(result.top_n_indices, result.top_n_scores):
    print(f"  Index {idx}: score={score:.2f}")
```

### Historical Baseline Comparison

Compare current data against historical baseline to detect drift.

```python
from vibe_piper.validation import detect_anomalies_against_baseline

# Load historical baseline data
baseline_data = load_historical_data()

detector = detect_anomalies_against_baseline(
    "revenue",
    baseline_data,
    baseline_window="last_30_days",
    threshold_std=3.0,
    threshold_iqr_multiplier=1.5
)
result = detector(current_data)

print(f"Drift score: {result.drift_score:.2%}")
print(f"Found {len(result.drifted_indices)} drifted records")
```

## Integration with @validate Decorator

Use anomaly detection as part of your validation pipeline with the `@validate` decorator.

```python
from vibe_piper.validation import validate, expect_column_no_anomalies_zscore

@validate(
    checks=[
        expect_column_no_anomalies_zscore("price", threshold=3.0, max_anomalies=5),
        expect_column_no_anomalies_iqr("revenue", multiplier=1.5, max_anomalies=5),
    ]
)
def process_sales_data():
    # Your data processing logic
    return fetch_records()

# Automatically validates on every run
result = process_sales_data()
```

## Multi-Method Detection

Run multiple anomaly detection methods and compare results.

```python
from vibe_piper.validation import detect_anomalies_multi_method

detector = detect_anomalies_multi_method(
    "value",
    methods=["zscore", "iqr", "isolation_forest", "one_class_svm"],
    zscore_threshold=3.0,
    iqr_multiplier=1.5,
    isolation_forest_contamination=0.1,
    one_class_svm_nu=0.1
)
results = detector(records)

for method, result in results.items():
    print(f"{method}: {result.outlier_count} anomalies")
```

## Configuration

All anomaly detection methods support configurable sensitivity:

- **Z-score**: `threshold` (default: 3.0, captures ~99.7% of normal data)
- **IQR**: `multiplier` (default: 1.5 for Tukey's fences, use 3.0 for extreme outliers only)
- **Isolation Forest**: `contamination` (expected proportion of outliers, 0-1)
- **One-Class SVM**: `nu` (upper bound on fraction of training errors, 0-1)

## Result Types

### AnomalyResult

Returned by detection methods:

```python
@dataclass(frozen=True)
class AnomalyResult:
    method: str  # Detection method name
    outlier_indices: tuple[int, ...]  # Indices of anomalous records
    outlier_count: int  # Number of anomalies
    anomaly_scores: tuple[float, ...]  # Scores for all records
    threshold: float  # Threshold used
    statistics: dict[str, Any]  # Additional statistics
    timestamp: datetime  # When detection was performed
```

### AnomalyRankingResult

Returned by ranking:

```python
@dataclass(frozen=True)
class AnomalyRankingResult:
    column: str  # Column analyzed
    ranked_indices: tuple[int, ...]  # All indices sorted by score
    ranked_scores: tuple[float, ...]  # Scores sorted descending
    top_n_indices: tuple[int, ...]  # Top N anomaly indices
    top_n_scores: tuple[float, ...]  # Top N anomaly scores
    total_anomalies: int  # Total anomalies (Z > 3)
```

### BaselineComparisonResult

Returned by baseline comparison:

```python
@dataclass(frozen=True)
class BaselineComparisonResult:
    column: str  # Column analyzed
    baseline_stats: dict[str, Any]  # Statistics from baseline
    current_stats: dict[str, Any]  # Statistics from current data
    drifted_indices: tuple[int, ...]  # Indices of drifted records
    drift_score: float  # Overall drift magnitude (0-1)
    baseline_window: str | None  # Description of baseline window
```

## Use Cases

1. **Data Quality Monitoring**: Detect corrupted or invalid data points
2. **Fraud Detection**: Identify suspicious transactions or activities
3. **System Monitoring**: Flag unusual system metrics or errors
4. **Business Intelligence**: Find unexpected patterns in sales, revenue, or user behavior
5. **Data Validation**: Ensure new data follows expected distributions

## Best Practices

1. **Use Multiple Methods**: Combine Z-score, IQR, and ML methods for robustness
2. **Validate Anomalies**: Manually inspect flagged records to reduce false positives
3. **Tune Thresholds**: Adjust sensitivity based on domain knowledge and data characteristics
4. **Consider Seasonality**: Use baseline comparison for time-series data with seasonal patterns
5. **Handle Nulls**: Ensure your data schema properly defines nullable fields
6. **Historical Baselines**: Use sufficient historical data for baseline comparison (100+ records recommended)
