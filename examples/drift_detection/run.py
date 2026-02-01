#!/usr/bin/env python
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

Usage:
    uv run python examples/drift_detection/run.py [--quick] [--clean] [--help]

Flags:
    --quick:    Use smaller sample size for faster execution
    --clean:    Clean output directory before running
    --help:     Show this help message
"""

import argparse
import shutil
from pathlib import Path

from vibe_piper.types import DataRecord, DataType, Schema, SchemaField
from vibe_piper.validation import (
    BaselineStore,
    DriftHistory,
    DriftThresholds,
    check_drift_ks,
    detect_drift_ks,
    detect_drift_psi,
)

# =============================================================================
# Configuration
# =============================================================================

OUTPUT_DIR = Path(__file__).parent / "output"


def parse_args():
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(
        description="Drift detection example with configurable sample sizes"
    )
    parser.add_argument(
        "--quick",
        action="store_true",
        help="Use smaller sample size for faster execution (100 records instead of 1000)",
    )
    parser.add_argument(
        "--clean",
        action="store_true",
        help="Clean output directory before running",
    )
    return parser.parse_args()


def clean_output():
    """Remove all files from output directory."""
    if OUTPUT_DIR.exists():
        print(f"Cleaning output directory: {OUTPUT_DIR}")
        for item in OUTPUT_DIR.iterdir():
            if item.is_file():
                item.unlink()
            elif item.is_dir():
                shutil.rmtree(item)
        print("  Removed all files and directories")


# =============================================================================
# Setup
# =============================================================================


def setup_schema():
    """Create schema for our data."""
    return Schema(
        name="sales_data",
        fields=(
            SchemaField(name="transaction_id", data_type=DataType.INTEGER, required=True),
            SchemaField(name="amount", data_type=DataType.FLOAT, required=True),
            SchemaField(name="customer_segment", data_type=DataType.STRING, required=False),
        ),
    )


def setup_thresholds():
    """Configure drift thresholds."""
    return DriftThresholds(
        warning=0.1,  # 10% drift triggers warning
        critical=0.25,  # 25% drift triggers critical alert
        ks_significance=0.05,  # Statistical significance for KS test
    )


def setup_storage():
    """Initialize baseline store and history."""
    baseline_store = BaselineStore(storage_dir=str(OUTPUT_DIR / "baselines"))
    drift_history = DriftHistory(storage_dir=str(OUTPUT_DIR / "drift_history"))
    return baseline_store, drift_history


# =============================================================================
# Data Generation
# =============================================================================


def generate_baseline_data(schema, sample_size):
    """Generate historical baseline data."""
    import random

    print(f"Generating historical baseline data ({sample_size} records)...")
    random.seed(42)
    return [
        DataRecord(
            data={
                "transaction_id": i,
                "amount": random.gauss(100, 20),  # Mean: $100, StdDev: $20
                "customer_segment": random.choice(["A", "B", "C"]),
            },
            schema=schema,
        )
        for i in range(sample_size)
    ]


def generate_stable_data(schema, sample_size):
    """Generate new data without significant drift."""
    import random

    print(f"Generating stable data ({sample_size} records)...")
    random.seed(100)
    return [
        DataRecord(
            data={
                "transaction_id": i,
                "amount": random.gauss(102, 20),  # Small mean shift (+$2)
                "customer_segment": random.choice(["A", "B", "C"]),
            },
            schema=schema,
        )
        for i in range(sample_size)
    ]


def generate_drifted_data(schema, sample_size):
    """Generate new data with significant drift."""
    import random

    print(f"Generating drifted data ({sample_size} records)...")
    random.seed(200)
    return [
        DataRecord(
            data={
                "transaction_id": i,
                "amount": random.gauss(130, 25),  # Large mean shift (+$30)
                "customer_segment": random.choice(["A", "B", "C", "D"]),
            },
            schema=schema,
        )
        for i in range(sample_size)
    ]


# =============================================================================
# Main Execution
# =============================================================================


def main():
    """Run drift detection example."""
    args = parse_args()

    # Determine sample size
    sample_size = 100 if args.quick else 1000
    print(f"Sample size: {sample_size}")
    print(f"Quick mode: {args.quick}")
    print(f"Clean mode: {args.clean}")
    print()

    # Clean output if requested
    if args.clean:
        clean_output()

    # Ensure output directory exists
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    # Setup
    schema = setup_schema()
    thresholds = setup_thresholds()
    baseline_store, drift_history = setup_storage()

    # Generate data
    historical_data = generate_baseline_data(schema, sample_size)
    stable_data = generate_stable_data(schema, sample_size)
    drifted_data = generate_drifted_data(schema, sample_size)

    # Store baseline
    print("\n" + "=" * 60)
    print("BASELINE STORAGE")
    print("=" * 60)

    baseline_metadata = baseline_store.add_baseline(
        "production_baseline",
        historical_data,
        description="Production sales data",
    )

    print(f"Created baseline: {baseline_metadata.baseline_id}")
    print(f"  Sample size: {baseline_metadata.sample_size}")
    print(f"  Columns: {', '.join(baseline_metadata.columns)}")
    print(f"  Created at: {baseline_metadata.created_at}")

    # Detect drift with KS test
    print("\n" + "=" * 60)
    print("DRIFT DETECTION: KS Test")
    print("=" * 60)

    ks_detector = detect_drift_ks("amount", significance_level=thresholds.ks_significance)

    # Check stable data
    stable_result = ks_detector((historical_data, stable_data))
    print("\nStable data analysis:")
    print(f"  Drift score: {stable_result.drift_score:.3f}")
    print(f"  Drifted columns: {stable_result.drifted_columns}")
    print(
        f"  Recommendation: {stable_result.recommendations[0] if stable_result.recommendations else 'None'}"
    )

    # Add stable check to history
    stable_history = drift_history.add_entry(stable_result, "production_baseline", thresholds)
    print(f"  Added to history: Alert level = {stable_history.alert_level}")

    # Check drifted data
    drifted_result = ks_detector((historical_data, drifted_data))
    print("\nDrifted data analysis:")
    print(f"  Drift score: {drifted_result.drift_score:.3f}")
    print(f"  Drifted columns: {drifted_result.drifted_columns}")
    print(
        f"  Recommendation: {drifted_result.recommendations[0] if drifted_result.recommendations else 'None'}"
    )

    # Add drifted check to history
    drifted_history = drift_history.add_entry(drifted_result, "production_baseline", thresholds)
    print(f"  Added to history: Alert level = {drifted_history.alert_level}")

    # Detect drift with PSI
    print("\n" + "=" * 60)
    print("DRIFT DETECTION: PSI (Population Stability Index)")
    print("=" * 60)

    psi_detector = detect_drift_psi("amount", num_bins=10, psi_threshold=thresholds.psi_critical)
    psi_result = psi_detector((historical_data, drifted_data))

    print("\nPSI Analysis:")
    psi_score = psi_result.statistics.get("psi_score", "N/A")
    print(f"  PSI Score: {psi_score:.3f}" if psi_score != "N/A" else "  PSI Score: N/A")
    print(f"  Drifted columns: {psi_result.drifted_columns}")
    print(
        f"  Recommendation: {psi_result.recommendations[0] if psi_result.recommendations else 'None'}"
    )

    # Get trend analysis
    print("\n" + "=" * 60)
    print("DRIFT HISTORY AND TRENDS")
    print("=" * 60)

    trend = drift_history.get_trend("production_baseline", window=10)
    print("\nDrift trend (last 10 checks):")
    print(f"  Check count: {trend['count']}")
    print(f"  Average drift score: {trend['avg_drift_score']:.3f}")
    print(f"  Max drift score: {trend['max_drift_score']:.3f}")
    print(f"  Min drift score: {trend['min_drift_score']:.3f}")
    print(f"  Trend: {trend['trend']}")
    print(f"  Critical alerts: {trend['critical_count']}")
    print(f"  Warning alerts: {trend['warning_count']}")

    # List baselines
    print("\n" + "=" * 60)
    print("BASELINE MANAGEMENT")
    print("=" * 60)

    baselines = baseline_store.list_baselines()
    print(f"\nAvailable baselines ({len(baselines)}):")
    for bl in baselines:
        print(f"  - {bl.baseline_id}")
        print(f"    Sample size: {bl.sample_size}")
        print(f"    Columns: {', '.join(bl.columns[:3])}...")
        print(f"    Created: {bl.created_at}")

    # Using drift check with validation result
    print("\n" + "=" * 60)
    print("DRIFT CHECK WITH VALIDATION RESULT")
    print("=" * 60)

    drift_check = check_drift_ks("amount", historical_data, thresholds=thresholds)
    validation_result = drift_check(drifted_data)

    print("\nValidation result:")
    print(f"  Valid: {validation_result.is_valid}")
    if validation_result.errors:
        print("  Errors:")
        for error in validation_result.errors:
            print(f"    - {error}")
    if validation_result.warnings:
        print("  Warnings:")
        for warning in validation_result.warnings[:3]:  # Show first 3 warnings
            print(f"    - {warning}")

    # Summary
    print("\n" + "=" * 60)
    print("SUMMARY")
    print("=" * 60)

    print("\nDrift detection features demonstrated:")
    print("  ✓ Baseline storage (add, retrieve, list)")
    print("  ✓ Drift detection (KS test, PSI)")
    print("  ✓ Drift history tracking")
    print("  ✓ Threshold-based alerting")
    print("  ✓ Integration with validation framework")
    print("  ✓ Trend analysis")

    print("\nFiles created:")
    print(f"  - {OUTPUT_DIR / 'baselines' / 'production_baseline.json'}")
    print(f"  - {OUTPUT_DIR / 'drift_history' / 'production_baseline_history.jsonl'}")

    print("\nNext steps:")
    print("  1. Review output files in examples/drift_detection/output/")
    print("  2. Try different sample sizes with --quick flag")
    print("  3. Use --clean to start fresh")
    print("  4. Integrate these patterns into your production pipelines")


if __name__ == "__main__":
    main()
