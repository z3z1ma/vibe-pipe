# Validation History

The validation history feature provides comprehensive storage and analysis of validation run history in Vibe Piper.

## Overview

Validation history tracks all validation executions, storing:
- Run metadata (asset, suite, status, timing)
- Individual check results (pass/fail, errors, warnings)
- Quality metrics over time (completeness, validity, uniqueness)

This enables:
- Per-asset validation history views
- Trend analysis (improving/declining/stable)
- Failure pattern detection (recurring failures, flaky checks)
- Historical baseline comparison (current vs. historical average)
- Search and filtering by date, asset, status

## Quick Start

### Initialize Database Schema

```python
from vibe_piper.connectors.postgres import PostgreSQLConnector, PostgreSQLConfig
from vibe_piper.validation.history import PostgreSQLValidationHistoryStore

# Create database connector
config = PostgreSQLConfig(
    host="localhost",
    port=5432,
    database="vibe_piper",
    user="your_user",
    password="your_password",
)

connector = PostgreSQLConnector(config)
connector.connect()

# Initialize validation history schema
store = PostgreSQLValidationHistoryStore(connector)
store.initialize_schema()

connector.disconnect()
```

### Auto-Store Validation Results

```python
from vibe_piper.validation import ValidationSuite, validate
from vibe_piper.validation.integration import store_validation_result

# Create validation suite
suite = ValidationSuite(name="customer_data_quality")
suite.add_check("unique_emails", expect_column_values_to_be_unique("email"))
suite.add_check("valid_ages", expect_column_values_to_be_between("age", 0, 120))

# Run validation
result = suite.validate(customer_records)

# Auto-store in history
validation_run_id = store_validation_result(
    result,
    asset_name="customers",
    history_store=store,
    pipeline_id="daily_data_pipeline",
)

print(f"Stored validation run: {validation_run_id}")
```

## Querying History

### Get Asset Validation History

```python
from vibe_piper.validation.history import ValidationHistoryAnalyzer

# Get validation history for a specific asset
history = store.get_asset_history("customers", limit=100)

for run in history:
    print(f"{run.started_at}: {run.status} - "
          f"{run.passed_checks}/{run.total_checks} checks passed")
```

### Query with Filters

```python
# Get failed runs in the last 7 days
from datetime import datetime, timedelta

start_date = datetime.now() - timedelta(days=7)
failed_runs = store.query_validation_runs(
    asset_name="customers",
    status="failed",
    start_date=start_date,
    limit=50,
)

for run in failed_runs:
    print(f"Failed run: {run.validation_run_id} - {run.started_at}")
```

### Get Metrics History

```python
# Get pass rate metrics over time
metrics = store.get_metrics_history(
    asset_name="customers",
    metric_name="pass_rate",
    start_date=datetime.now() - timedelta(days=30),
    limit=1000,
)

for metric in metrics:
    print(f"{metric.timestamp}: {metric.value:.2%}")
```

## Trend Analysis

```python
from vibe_piper.validation.history import ValidationHistoryAnalyzer

analyzer = ValidationHistoryAnalyzer(store)

# Analyze pass rate trend over last 30 days
trend = analyzer.analyze_trends(
    asset_name="customers",
    metric_name="pass_rate",
    period_days=30,
)

if trend:
    print(f"Direction: {trend.direction}")  # improving, declining, stable
    print(f"Confidence: {trend.confidence:.2%}")
    print(f"Data points: {trend.data_points}")
else:
    print("Insufficient data for trend analysis")
```

## Failure Pattern Detection

```python
# Detect recurring failure patterns
patterns = analyzer.detect_failure_patterns(
    asset_name="customers",
    period_days=30,
    min_occurrences=3,
)

for pattern in patterns:
    print(f"Pattern: {pattern.pattern_type}")
    print(f"Check: {pattern.check_name}")
    print(f"Frequency: {pattern.frequency} times")
    print(f"Description: {pattern.description}")
```

## Baseline Comparison

```python
# Compare current metrics against 30-run baseline
comparison = analyzer.compare_with_baseline(
    asset_name="customers",
    metric_name="pass_rate",
    baseline_period="last_30_runs",
    tolerance_percent=10.0,
)

if comparison:
    print(f"Baseline: {comparison.baseline_value:.2%}")
    print(f"Current: {comparison.current_value:.2%}")
    print(f"Difference: {comparison.difference:+.2%}")
    print(f"Change: {comparison.percent_change:+.1%}")
    print(f"Status: {comparison.status}")  # within_tolerance, warning, critical
else:
    print("Insufficient data for baseline comparison")
```

## Summary Statistics

```python
# Get summary statistics for an asset
summary = analyzer.get_summary_statistics(
    asset_name="customers",
    start_date=datetime.now() - timedelta(days=30),
)

print(f"Total runs: {summary['total_runs']}")
print(f"Pass rate: {summary['pass_rate']:.1%}")
print(f"Average duration: {summary['avg_duration_ms']:.0f}ms")
print(f"Records validated: {summary['total_records_validated']}")
```

## Data Models

### ValidationRunMetadata

Stores metadata for a validation run:
- `validation_run_id`: Unique identifier
- `asset_name`: Asset being validated
- `suite_name`: Validation suite name
- `status`: Overall status ('passed', 'failed', 'warning')
- `started_at`, `completed_at`: Timing information
- `duration_ms`: Execution duration
- `total_checks`, `passed_checks`, `failed_checks`, `warning_checks`: Check counts
- `total_records`: Number of records validated
- `error_count`, `warning_count`: Error/warning counts

### ValidationCheckRecord

Stores result of a single check:
- `validation_run_id`: Parent validation run
- `check_name`, `check_type`: Check identification
- `passed`: Whether check passed
- `error_message`: Error if failed
- `warning_messages`: List of warnings
- `metrics`: Additional check metrics
- `column_name`: Column validated (if applicable)
- `duration_ms`: Check execution time

### ValidationMetric

Stores a quality metric measurement:
- `metric_name`: Metric identifier (e.g., 'pass_rate', 'completeness')
- `metric_type`: Type of quality metric (from QualityMetricType enum)
- `asset_name`: Asset this metric is for
- `value`: Metric value
- `timestamp`: When metric was recorded
- `status`: 'passed', 'failed', or 'warning'
- `threshold`: Optional threshold value
- `check_name`: Check that generated this metric (if applicable)

### TrendAnalysisResult

Result of trend analysis:
- `direction`: 'improving', 'declining', 'stable', or 'unknown'
- `trend_value`: Numerical trend value (slope)
- `confidence`: Confidence score (0-1)
- `period_start`, `period_end`: Analysis period
- `data_points`: Number of data points analyzed

### FailurePattern

Detected failure pattern:
- `pattern_type`: 'recurring_failure', 'flaky_check', 'degradation', or 'new_failure'
- `check_name`: Check that is failing (if applicable)
- `frequency`: How often pattern occurs
- `first_occurrence`, `last_occurrence`: Time range of occurrences
- `affected_runs`: Validation run IDs affected
- `description`: Human-readable description

### BaselineComparisonResult

Result comparing current vs. baseline:
- `baseline_value`: Historical average
- `baseline_period`: Period used for baseline (e.g., 'last_30_runs')
- `current_value`: Current metric value
- `difference`: Current minus baseline
- `percent_change`: Percentage change
- `status`: 'within_tolerance', 'warning', or 'critical'
- `tolerance_percent`: Acceptable tolerance

## API Reference

### ValidationHistoryStore Protocol

```python
class ValidationHistoryStore(Protocol):
    def save_validation_run(self, run_metadata: ValidationRunMetadata) -> None: ...
    def save_check_results(self, check_records: Sequence[ValidationCheckRecord]) -> None: ...
    def save_metrics(self, metrics: Sequence[ValidationMetric]) -> None: ...
    def get_validation_run(self, validation_run_id: str) -> ValidationRunMetadata | None: ...
    def query_validation_runs(
        self,
        asset_name: str | None = None,
        status: str | None = None,
        start_date: datetime | None = None,
        end_date: datetime | None = None,
        limit: int = 100,
    ) -> Sequence[ValidationRunMetadata]: ...
    def get_asset_history(self, asset_name: str, limit: int = 100) -> Sequence[ValidationRunMetadata]: ...
    def get_metrics_history(
        self,
        asset_name: str,
        metric_name: str | None = None,
        start_date: datetime | None = None,
        end_date: datetime | None = None,
        limit: int = 1000,
    ) -> Sequence[ValidationMetric]: ...
    def search_validation_runs(self, query: str, limit: int = 100) -> Sequence[ValidationRunMetadata]: ...
    def delete_old_runs(self, before_date: datetime, asset_name: str | None = None) -> int: ...
```

### ValidationHistoryAnalyzer

```python
class ValidationHistoryAnalyzer:
    def __init__(self, store: ValidationHistoryStore) -> None: ...
    def analyze_trends(
        self,
        asset_name: str,
        metric_name: str,
        period_days: int = 30,
        min_data_points: int = 5,
    ) -> TrendAnalysisResult | None: ...
    def detect_failure_patterns(
        self,
        asset_name: str,
        period_days: int = 30,
        min_occurrences: int = 3,
    ) -> Sequence[FailurePattern]: ...
    def compare_with_baseline(
        self,
        asset_name: str,
        metric_name: str,
        baseline_period: str = "last_30_runs",
        tolerance_percent: float = 10.0,
    ) -> BaselineComparisonResult | None: ...
    def get_summary_statistics(
        self,
        asset_name: str,
        start_date: datetime | None = None,
        end_date: datetime | None = None,
    ) -> dict[str, Any]: ...
```

### Integration Functions

```python
from vibe_piper.validation.integration import (
    suite_result_to_run_metadata,
    suite_result_to_check_records,
    extract_metrics_from_suite_result,
    store_validation_result,
)

# Convert SuiteValidationResult to history format
run_metadata = suite_result_to_run_metadata(suite_result, asset_name)

# Extract check records
check_records = suite_result_to_check_records(suite_result, validation_run_id)

# Store complete validation result
validation_run_id = store_validation_result(
    suite_result,
    asset_name,
    history_store=store,
    pipeline_id="pipeline_id",
)
```

## Notes

- **Optional Dependencies**: Trend analysis requires `pandas` and `numpy`. These are imported with try/except and functionality gracefully degrades if not available.

- **PostgreSQL Schema**: The `initialize_schema()` method creates three tables:
  - `validation_runs`: Stores validation run metadata
  - `validation_check_results`: Stores individual check results
  - `validation_metrics`: Stores metric measurements

- **Indexes**: Database indexes are created on frequently queried columns for performance:
  - `idx_validation_runs_asset`: Query by asset
  - `idx_validation_runs_status`: Query by status
  - `idx_validation_runs_started_at`: Query by date (descending)
  - `idx_metrics_asset`: Query metrics by asset
  - `idx_metrics_timestamp`: Query metrics by date

- **Cleanup**: Use `delete_old_runs()` to remove old validation history. Recommended to regularly clean up data older than a retention period.
