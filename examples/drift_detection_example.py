"""
Drift Detection Example

This example demonstrates how to use Vibe Piper's drift detection
features to monitor data quality and detect distribution changes over time.

Features demonstrated:
- Creating and managing baselines
- Detecting drift with KS test and PSI
- Tracking drift history
- Using drift checks with @validate decorator
- Threshold-based alerting
"""

from vibe_piper.types import DataRecord, Schema, SchemaField, DataType
from vibe_piper.validation import (
    BaselineStore,
    DriftHistory,
    DriftThresholds,
    detect_drift_ks,
    detect_drift_psi,
    check_drift_ks,
    check_drift_psi,
)
import random
from datetime import datetime


# =============================================================================
# Setup
# =============================================================================

# Create schema for our data
schema = Schema(
    name="sales_data",
    fields=(
        SchemaField(name="transaction_id", data_type=DataType.INTEGER, required=True),
        SchemaField(name="amount", data_type=DataType.FLOAT, required=True),
        SchemaField(name="customer_segment", data_type=DataType.STRING, required=False),
    ),
)

# Configure drift thresholds
thresholds = DriftThresholds(
    warning=0.1,  # 10% drift triggers warning
    critical=0.25,  # 25% drift triggers critical alert
    ks_significance=0.05,  # Statistical significance for KS test
)

# Initialize baseline store and history
baseline_store = BaselineStore(storage_dir="./baselines")
drift_history = DriftHistory(storage_dir="./drift_history")


# =============================================================================
# Generate Historical Baseline Data
# =============================================================================

print("Generating historical baseline data...")
random.seed(42)
historical_data = [
    DataRecord(
        data={
            "transaction_id": i,
            "amount": random.gauss(100, 20),  # Mean: $100, StdDev: $20
            "customer_segment": random.choice(["A", "B", "C"]),
        },
        schema=schema,
    )
    for i in range(1000)
]

# Store baseline
baseline_metadata = baseline_store.add_baseline(
    "production_baseline_2024",
    historical_data,
    description="Production sales data from Q1 2024",
)

print(f"Created baseline: {baseline_metadata.baseline_id}")
print(f"Sample size: {baseline_metadata.sample_size}")
print(f"Created at: {baseline_metadata.created_at}")


# =============================================================================
# Generate New Data (with and without drift)
# =============================================================================

print("\nGenerating new data...")

# Case 1: No significant drift
random.seed(100)
stable_data = [
    DataRecord(
        data={
            "transaction_id": i,
            "amount": random.gauss(102, 20),  # Small mean shift (+$2)
            "customer_segment": random.choice(["A", "B", "C"]),
        },
        schema=schema,
    )
    for i in range(1000)
]

# Case 2: Significant drift (mean shift)
random.seed(200)
drifted_data = [
    DataRecord(
        data={
            "transaction_id": i,
            "amount": random.gauss(130, 25),  # Large mean shift (+$30)
            "customer_segment": random.choice(["A", "B", "C", "D"]),
        },
        schema=schema,
    )
    for i in range(1000)
]


# =============================================================================
# Detect Drift Using KS Test
# =============================================================================

print("\n" + "=" * 60)
print("DRIFT DETECTION: KS Test")
print("=" * 60)

# Check stable data
ks_detector = detect_drift_ks("amount", significance_level=thresholds.ks_significance)
stable_result = ks_detector((historical_data, stable_data))

print(f"\nStable data analysis:")
print(f"  Drift score: {stable_result.drift_score:.3f}")
print(f"  Drifted columns: {stable_result.drifted_columns}")
print(
    f"  Recommendations: {stable_result.recommendations[0] if stable_result.recommendations else 'None'}"
)

# Check drifted data
drifted_result = ks_detector((historical_data, drifted_data))

print(f"\nDrifted data analysis:")
print(f"  Drift score: {drifted_result.drift_score:.3f}")
print(f"  Drifted columns: {drifted_result.drifted_columns}")
print(
    f"  Recommendations: {drifted_result.recommendations[0] if drifted_result.recommendations else 'None'}"
)


# =============================================================================
# Detect Drift Using PSI
# =============================================================================

print("\n" + "=" * 60)
print("DRIFT DETECTION: PSI (Population Stability Index)")
print("=" * 60)

psi_detector = detect_drift_psi("amount", num_bins=10, psi_threshold=thresholds.psi_critical)
psi_result = psi_detector((historical_data, drifted_data))

print(f"\nPSI Analysis:")
print(f"  PSI Score: {psi_result.statistics.get('psi_score', 'N/A'):.3f}")
print(f"  Drifted columns: {psi_result.drifted_columns}")
print(
    f"  Recommendations: {psi_result.recommendations[0] if psi_result.recommendations else 'None'}"
)


# =============================================================================
# Track Drift History
# =============================================================================

print("\n" + "=" * 60)
print("TRACKING DRIFT HISTORY")
print("=" * 60)

# Add stable check to history
stable_history = drift_history.add_entry(stable_result, "production_baseline_2024", thresholds)
print(f"\nStable data check added to history:")
print(f"  Alert level: {stable_history.alert_level}")
print(f"  Timestamp: {stable_history.timestamp}")

# Add drifted check to history
drifted_history = drift_history.add_entry(drifted_result, "production_baseline_2024", thresholds)
print(f"\nDrifted data check added to history:")
print(f"  Alert level: {drifted_history.alert_level}")
print(f"  Timestamp: {drifted_history.timestamp}")

# Get trend analysis
trend = drift_history.get_trend("production_baseline_2024", window=10)
print(f"\nDrift trend (last 10 checks):")
print(f"  Average drift score: {trend['avg_drift_score']:.3f}")
print(f"  Max drift score: {trend['max_drift_score']:.3f}")
print(f"  Trend: {trend['trend']}")


# =============================================================================
# Using Drift Checks with @validate Decorator
# =============================================================================

print("\n" + "=" * 60)
print("USING DRIFT CHECKS WITH @validate DECORATOR")
print("=" * 60)

# Create drift check function compatible with @validate
drift_check = check_drift_ks("amount", historical_data, thresholds=thresholds)

# Apply check to new data
validation_result = drift_check(drifted_data)

print(f"\nValidation result:")
print(f"  Valid: {validation_result.is_valid}")
if validation_result.errors:
    print(f"  Errors:")
    for error in validation_result.errors:
        print(f"    - {error}")
if validation_result.warnings:
    print(f"  Warnings:")
    for warning in validation_result.warnings[:3]:  # Show first 3 warnings
        print(f"    - {warning}")


# =============================================================================
# Listing and Managing Baselines
# =============================================================================

print("\n" + "=" * 60)
print("BASELINE MANAGEMENT")
print("=" * 60)

# List all baselines
baselines = baseline_store.list_baselines()
print(f"\nAvailable baselines ({len(baselines)}):")
for bl in baselines:
    print(f"  - {bl.baseline_id}")
    print(f"    Sample size: {bl.sample_size}")
    print(f"    Columns: {', '.join(bl.columns[:3])}...")
    print(f"    Created: {bl.created_at}")


# =============================================================================
# Summary
# =============================================================================

print("\n" + "=" * 60)
print("SUMMARY")
print("=" * 60)

print("\nDrift detection features demonstrated:")
print("  ✓ Baseline storage (add, retrieve, list, delete)")
print("  ✓ Drift detection (KS test, PSI)")
print("  ✓ Drift history tracking")
print("  ✓ Threshold-based alerting")
print("  ✓ Integration with @validate decorator")
print("  ✓ Trend analysis")

print("\nFiles created:")
print("  - ./baselines/production_baseline_2024.json")
print("  - ./drift_history/production_baseline_2024_history.jsonl")

print("\nNext steps:")
print("  1. Use these examples in your production pipelines")
print("  2. Schedule regular drift checks (daily/weekly)")
print("  3. Set up automated alerts on critical drift")
print("  4. Monitor drift trends over time")
print("  5. Retrain models when drift exceeds thresholds")
