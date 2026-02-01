# Drift Detection Example

This example demonstrates how to use Vibe Piper's drift detection features to monitor data quality and detect distribution changes over time.

## Features Demonstrated

- **Baseline Storage**: Create and manage historical baselines for comparison
- **Drift Detection**: Detect distribution shifts using KS test and PSI (Population Stability Index)
- **Drift History**: Track drift checks over time and analyze trends
- **Threshold-Based Alerting**: Configure warning and critical thresholds for automated alerting
- **Validation Integration**: Use drift checks with the @validate decorator
- **Trend Analysis**: Monitor drift patterns and identify increasing/decreasing trends

## Quick Start

### Run with default settings (1000 samples)

```bash
uv run python examples/drift_detection/run.py
```

### Run in quick mode (100 samples, faster execution)

```bash
uv run python examples/drift_detection/run.py --quick
```

### Clean output directory before running

```bash
uv run python examples/drift_detection/run.py --clean
```

### Combine flags (quick + clean)

```bash
uv run python examples/drift_detection/run.py --quick --clean
```

## Output Files

All outputs are written to `examples/drift_detection/output/`:

- `baselines/production_baseline.json` - Historical baseline data
- `drift_history/production_baseline_history.jsonl` - Drift check history (JSONL format)

The output directory is excluded from git by a local `.gitignore` file.

## Command-Line Options

- `--quick` - Use smaller sample size (100 records instead of 1000) for faster execution
- `--clean` - Clean output directory before running
- `--help` - Show help message

## Drift Detection Methods

### Kolmogorov-Smirnov (KS) Test

Compares cumulative distribution functions of two samples. Good for detecting shifts in continuous distributions.

**Interpretation**:
- Low KS statistic (< 0.1): No significant drift
- Moderate KS statistic (0.1 - 0.2): Minor drift
- High KS statistic (> 0.2): Significant drift

### Population Stability Index (PSI)

Measures how much a variable has shifted in distribution. Commonly used in credit scoring and monitoring.

**Interpretation**:
- PSI < 0.1: No significant change
- 0.1 ≤ PSI < 0.2: Moderate change
- PSI ≥ 0.2: Significant shift

## Threshold Configuration

The example uses the following thresholds:

```python
DriftThresholds(
    warning=0.1,        # 10% drift triggers warning
    critical=0.25,      # 25% drift triggers critical alert
    ks_significance=0.05, # Statistical significance for KS test
    psi_warning=0.1,     # PSI threshold for warnings
    psi_critical=0.2,    # PSI threshold for critical alerts
)
```

Adjust these values based on your use case and risk tolerance.

## Example Workflow

1. **Create Baseline**: Store historical data as a baseline
2. **Detect Drift**: Compare new data against baseline using KS test or PSI
3. **Track History**: Add drift check results to history for trend analysis
4. **Analyze Trends**: Review drift patterns over time to identify issues early

## Code Patterns

### Creating and Using Baselines

```python
from vibe_piper.validation import BaselineStore

# Initialize baseline store
baseline_store = BaselineStore(storage_dir="./baselines")

# Add baseline
baseline_metadata = baseline_store.add_baseline(
    "production_baseline",
    historical_data,
    description="Production data from 2024-01-01"
)

# Retrieve baseline
baseline = baseline_store.get_baseline("production_baseline")

# List all baselines
baselines = baseline_store.list_baselines()
```

### Detecting Drift

```python
from vibe_piper.validation import detect_drift_ks, detect_drift_psi

# KS test for continuous distributions
ks_detector = detect_drift_ks("amount", significance_level=0.05)
result = ks_detector((historical_data, new_data))

# PSI for population stability
psi_detector = detect_drift_psi("amount", num_bins=10, psi_threshold=0.2)
result = psi_detector((historical_data, new_data))
```

### Tracking Drift History

```python
from vibe_piper.validation import DriftHistory, DriftThresholds

# Initialize history tracker
drift_history = DriftHistory(storage_dir="./drift_history")

# Add drift check result to history
thresholds = DriftThresholds(warning=0.1, critical=0.25)
history_entry = drift_history.add_entry(result, "production_baseline", thresholds)

# Get trend analysis
trend = drift_history.get_trend("production_baseline", window=10)
print(f"Average drift: {trend['avg_drift_score']}")
print(f"Trend: {trend['trend']}")  # 'increasing', 'decreasing', 'stable'
```

### Using Drift Checks with Validation Framework

```python
from vibe_piper.validation import check_drift_ks

# Create drift check compatible with @validate decorator
drift_check = check_drift_ks("amount", historical_data, thresholds=thresholds)

# Apply check to new data
validation_result = drift_check(new_data)

if not validation_result.is_valid:
    print(f"Critical drift detected: {validation_result.errors}")
```

## Next Steps

1. **Review Output**: Examine generated files in `examples/drift_detection/output/`
2. **Experiment**: Try different sample sizes and threshold values
3. **Integrate**: Use these patterns in your production pipelines
4. **Monitor**: Set up scheduled drift checks for ongoing monitoring
5. **Alert**: Configure automated alerts when drift exceeds thresholds

## Related Examples

- [API Ingestion Example](../api_ingestion/) - Data ingestion patterns
- [ETL Pipeline Example](../etl_pipeline/) - End-to-end ETL workflows
- [Transformation Example](../transformation_example.py) - Data transformations

## Additional Resources

- [Vibe Piper Documentation](https://docs.vibepiper.dev)
- [Drift Detection API Reference](https://docs.vibepiper.dev/validation/drift_detection)
- [Validation Framework Guide](https://docs.vibepiper.dev/validation)
