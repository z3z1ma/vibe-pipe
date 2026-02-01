"""
AssetGraph ETL Pipeline Example

A canonical, fully runnable ETL pipeline example that showcases:
- Simple asset functions with clear data flow
- ValidationSuite for data quality checks
- File-based I/O (CSV input/output)
- Extract, transform, validate, load, and summarize steps

This implementation uses the correct PipelineBuilder.asset() pattern:

Usage:
    python pipeline.py [--once]
"""

from __future__ import annotations

import json
import logging
import os
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import TYPE_CHECKING

from vibe_piper import ExecutionEngine, PipelineBuilder
from vibe_piper.connectors.csv import CSVReader, CSVWriter
from vibe_piper.types import DataRecord, DataType, Schema, SchemaField
from vibe_piper.validation.checks import (
    expect_column_values_to_be_in_set,
    expect_column_values_to_match_regex,
    expect_column_values_to_not_be_null,
    expect_table_row_count_to_be_between,
)
from vibe_piper.validation.suite import ValidationSuite

if TYPE_CHECKING:
    from vibe_piper.types import AssetGraph, PipelineContext

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

# =============================================================================
# Configuration
# =============================================================================


@dataclass
class ETLConfig:
    """Configuration for ETL pipeline."""

    input_path: str = "data/users.csv"
    output_dir: str = "output"

    # Validation thresholds
    min_row_count: int = 5
    max_row_count: int = 1000
    max_null_proportion: float = 0.1

    def __post_init__(self) -> None:
        """Ensure output directory exists."""
        Path(self.output_dir).mkdir(parents=True, exist_ok=True)


_pipeline_config: ETLConfig = ETLConfig()


# =============================================================================
# Asset Functions
# =============================================================================


def extract(context: PipelineContext) -> list[dict]:
    """Read raw data from CSV."""
    logger.info(f"Extracting data from {_pipeline_config.input_path}")
    reader = CSVReader(_pipeline_config.input_path)
    records = reader.read()
    # Convert DataRecord objects to dicts for easier manipulation
    data = [record.data for record in records]
    logger.info(f"Extracted {len(data)} records")
    return data


def transform(upstream_data, context: PipelineContext) -> list[dict]:
    """Clean and enrich user data."""
    logger.info("Transforming data...")

    try:
        # Get upstream data from extract asset
        # When using depends_on explicitly, upstream_data.get() returns data directly
        extract_data = upstream_data.get("extract")
        if extract_data is None:
            logger.warning("No data from extract")
            return []

        logger.info(f"Got {len(extract_data)} records from extract")

        transformed = []

        for row in extract_data:
            # Clean and normalize data
            cleaned = row.copy()

            # Normalize email to lowercase and strip
            if cleaned.get("email"):
                cleaned["email"] = cleaned["email"].lower().strip()

            # Clean phone: extract digits only, or None if missing
            if cleaned.get("phone"):
                phone = str(cleaned["phone"]).strip()
                cleaned["phone_clean"] = "".join(c for c in phone if c.isdigit())
            else:
                cleaned["phone_clean"] = None

            # Normalize status to lowercase
            if cleaned.get("status"):
                cleaned["status"] = cleaned["status"].lower().strip()

            # Parse dates and add derived fields
            signup_date = cleaned.get("signup_date")
            if signup_date:
                try:
                    dt = datetime.strptime(signup_date, "%Y-%m-%d")
                    cleaned["signup_year"] = dt.year
                    cleaned["signup_month"] = dt.month
                except (ValueError, TypeError):
                    cleaned["signup_year"] = None
                    cleaned["signup_month"] = None
            else:
                cleaned["signup_year"] = None
                cleaned["signup_month"] = None

            # Calculate customer tier based on total spent
            total_spent = float(cleaned.get("total_spent", 0))
            if total_spent >= 500:
                cleaned["customer_tier"] = "gold"
            elif total_spent >= 200:
                cleaned["customer_tier"] = "silver"
            elif total_spent > 0:
                cleaned["customer_tier"] = "bronze"
            else:
                cleaned["customer_tier"] = "inactive"

            # Calculate days since last login
            last_login = cleaned.get("last_login")
            if last_login:
                try:
                    dt_login = datetime.strptime(last_login, "%Y-%m-%d")
                    cleaned["days_since_login"] = (datetime.now() - dt_login).days
                except (ValueError, TypeError):
                    cleaned["days_since_login"] = None
            else:
                cleaned["days_since_login"] = None

            transformed.append(cleaned)

        logger.info(f"Transformed {len(transformed)} records")
        return transformed
    except Exception as e:
        logger.error(f"Transform failed: {e}", exc_info=True)
        raise


def validate(upstream_data, context: PipelineContext) -> dict[str, object]:
    """Run data quality validation checks."""
    logger.info("Validating data...")

    # Get upstream data from transform asset
    transform_data = upstream_data.get("transform")

    # If no data, validation will still run (checks will fail)
    # but we need to handle schema inference carefully
    if not transform_data:
        logger.warning("No data to validate - validation checks will fail")
        # Create validation suite and let it fail on empty data
        suite = ValidationSuite(name="etl_quality_checks")
        suite.strategy = "fail_fast"
        suite.add_check(
            "min_row_count",
            expect_table_row_count_to_be_between(
                min_value=_pipeline_config.min_row_count,
                max_value=_pipeline_config.max_row_count,
            ),
        )
        # Validate empty list - this should fail
        result = suite.validate([])
        # Raise error to indicate validation failure
        msg = f"Data validation failed: {result.errors[0] if result.errors else 'Empty data'}"
        raise ValueError(msg)

    # Create validation suite
    suite = ValidationSuite(name="etl_quality_checks")
    suite.strategy = "fail_fast"

    # Add validation checks using config thresholds
    suite.add_check(
        "min_row_count",
        expect_table_row_count_to_be_between(
            min_value=_pipeline_config.min_row_count,
            max_value=_pipeline_config.max_row_count,
        ),
    )
    suite.add_check(
        "email_not_null",
        expect_column_values_to_not_be_null("email"),
    )
    suite.add_check(
        "email_format_valid",
        expect_column_values_to_match_regex(
            "email",
            r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$",
        ),
    )
    suite.add_check(
        "status_in_allowed_values",
        expect_column_values_to_be_in_set(
            "status",
            value_set={"active", "inactive", "pending"},
        ),
    )

    # Convert dicts to DataRecord objects for validation
    # Infer schema from first record
    schema_fields = [
        SchemaField(name=key, data_type=DataType.STRING, nullable=True)
        for key in transform_data[0].keys()
    ]
    schema = Schema(name="transformed_users", fields=tuple(schema_fields))
    records = [DataRecord(data=row, schema=schema) for row in transform_data]

    # Run validation
    result = suite.validate(records)

    if not result.success:
        logger.error("Data validation failed!")
        for error in result.errors:
            logger.error(f"  - {error}")
        msg = "Data validation failed"
        raise ValueError(msg)

    # Return validation result for downstream assets
    return {
        "is_valid": True,
        "checks_passed": len(result.warnings),
        "check_names": [check_name for check_name in suite.list_checks()],
    }


def load(upstream_data, context: PipelineContext) -> str:
    """Write transformed data to CSV."""
    logger.info("Loading data to output...")

    # Get upstream data from transform asset
    transform_data = upstream_data.get("transform")
    if not transform_data:
        logger.warning("No data to load")
        return str(Path(_pipeline_config.output_dir) / "users_transformed.csv")

    # Select and order columns for output
    output_columns = [
        "user_id",
        "name",
        "email",
        "phone_clean",
        "country",
        "status",
        "customer_tier",
        "signup_year",
        "signup_month",
        "days_since_login",
        "total_orders",
        "total_spent",
    ]

    # Filter and reorder data
    output_data = []
    for row in transform_data:
        filtered = {col: row.get(col) for col in output_columns if col in row}
        output_data.append(filtered)

    # Write to CSV
    output_schema_fields = [
        SchemaField(name=col, data_type=DataType.STRING, nullable=True) for col in output_columns
    ]
    output_schema = Schema(name="output_users", fields=tuple(output_schema_fields))

    records = [DataRecord(data=filtered, schema=output_schema) for filtered in output_data]

    output_path = Path(_pipeline_config.output_dir) / "users_transformed.csv"
    writer = CSVWriter(output_path)
    count = writer.write(records, schema=output_schema)

    logger.info(f"Loaded {count} records to {output_path}")
    return str(output_path)


def summarize(upstream_data, context: PipelineContext) -> dict[str, object]:
    """Generate summary statistics."""
    logger.info("Generating summary...")

    # Get upstream data
    transform_data = upstream_data.get("transform")
    load_path = upstream_data.get("load", "")

    if not transform_data:
        logger.warning("No data to summarize")
        return {
            "total_users": 0,
            "status_distribution": {},
            "tier_distribution": {},
            "total_revenue": 0.0,
            "total_orders": 0,
            "average_order_value": 0.0,
            "output_file": load_path,
            "generated_at": datetime.now().isoformat(),
        }

    # Calculate statistics
    total_users = len(transform_data)
    status_counts = {}
    tier_counts = {}
    total_revenue = 0.0
    total_orders = 0

    for row in transform_data:
        # Count by status
        status = row.get("status", "unknown")
        status_counts[status] = status_counts.get(status, 0) + 1

        # Count by tier
        tier = row.get("customer_tier", "unknown")
        tier_counts[tier] = tier_counts.get(tier, 0) + 1

        # Sum revenue and orders
        total_revenue += float(row.get("total_spent", 0))
        total_orders += int(row.get("total_orders", 0))

    # Calculate average order value
    avg_order_value = total_revenue / total_orders if total_orders > 0 else 0.0

    summary = {
        "total_users": total_users,
        "status_distribution": status_counts,
        "tier_distribution": tier_counts,
        "total_revenue": total_revenue,
        "total_orders": total_orders,
        "average_order_value": avg_order_value,
        "output_file": load_path,
        "generated_at": datetime.now().isoformat(),
    }

    # Log summary
    logger.info("Summary:")
    logger.info(f"  Total users: {total_users}")
    logger.info(f"  Status distribution: {status_counts}")
    logger.info(f"  Tier distribution: {tier_counts}")
    logger.info(f"  Total revenue: ${total_revenue:.2f}")
    logger.info(f"  Average order value: ${avg_order_value:.2f}")

    # Write summary to file
    summary_path = Path(_pipeline_config.output_dir) / "summary.json"
    with open(summary_path, "w") as f:
        json.dump(summary, f, indent=2)

    logger.info(f"Summary written to {summary_path}")

    return summary


# =============================================================================
# Pipeline Builder Function
# =============================================================================


def build_pipeline(config: ETLConfig) -> AssetGraph:
    """
    Build the ETL pipeline AssetGraph.

    This function creates and returns a fully configured AssetGraph
    with all assets registered and dependencies set.

    Args:
        config: ETL configuration object

    Returns:
        Configured AssetGraph ready for execution
    """
    global _pipeline_config
    _pipeline_config = config

    # Create PipelineBuilder
    builder = PipelineBuilder(
        "asset_graph_etl",
        description="Local ETL pipeline with CSV I/O and ValidationSuite",
    )

    # Register assets using builder.asset(name, fn=..., ...)
    # Dependencies are inferred from function parameter names

    # Asset 1: Extract raw data from CSV
    builder.asset(
        name="extract",
        fn=extract,
        description="Read raw user data from input CSV",
    )

    # Asset 2: Transform data
    builder.asset(
        name="transform",
        fn=transform,
        depends_on=["extract"],
        description="Clean and enrich user data",
    )

    # Asset 3: Validate transformed data
    builder.asset(
        name="validate",
        fn=validate,
        depends_on=["transform"],
        description="Run data quality validation checks",
    )

    # Asset 4: Load validated data to CSV
    builder.asset(
        name="load",
        fn=load,
        depends_on=["transform", "validate"],
        description="Write transformed data to output CSV",
    )

    # Asset 5: Generate summary statistics
    builder.asset(
        name="summarize",
        fn=summarize,
        depends_on=["transform", "load"],
        description="Generate pipeline summary statistics",
    )

    # Build and return the graph
    return builder.build()


# =============================================================================
# Pipeline Class
# =============================================================================


class AssetGraphETLPipeline:
    """
    ETL Pipeline class for local data processing.

    This class wraps the build_pipeline function for convenient
    execution and result reporting.

    Pipeline Steps:
    1. extract - Read raw data from CSV
    2. transform - Clean and enrich user data
    3. validate - Run data quality validation checks
    4. load - Write transformed data to CSV
    5. summarize - Generate summary statistics

    Example:
        >>> config = ETLConfig()
        >>> pipeline = AssetGraphETLPipeline(config)
        >>> pipeline.run_once()
    """

    def __init__(self, config: ETLConfig) -> None:
        """Initialize pipeline with configuration."""
        self.config = config
        self.graph = build_pipeline(config)

    def run_once(self) -> None:
        """Run pipeline once and print summary."""
        logger.info("=" * 60)
        logger.info("Starting AssetGraph ETL Pipeline")
        logger.info("=" * 60)

        logger.info(f"Built pipeline graph with {len(self.graph.assets)} assets")

        engine = ExecutionEngine()
        result = engine.execute(self.graph)

        # Report results
        if result.success:
            logger.info(
                f"Pipeline completed successfully. Executed {result.assets_executed} assets."
            )
            # Print final summary
            summary_result = result.get_asset_result("summarize")
            if summary_result and summary_result.data:
                summary = summary_result.data
                print("\n" + "=" * 50)
                print("PIPELINE SUMMARY")
                print("=" * 50)
                print(f"Total users: {summary.get('total_users')}")
                print(f"Status distribution: {summary.get('status_distribution')}")
                print(f"Tier distribution: {summary.get('tier_distribution')}")
                print(f"Total revenue: ${summary.get('total_revenue', 0):.2f}")
                print(f"Average order value: ${summary.get('average_order_value', 0):.2f}")
                print(f"Output file: {summary.get('output_file')}")
                print("=" * 50 + "\n")
        else:
            logger.error(
                f"Pipeline failed. {result.assets_failed} assets failed. See logs for details."
            )
            for asset_name in result.get_failed_assets():
                logger.error(f"Failed asset: {asset_name}")

            msg = "Pipeline execution failed"
            raise RuntimeError(msg)


# =============================================================================
# Main Entry Point
# =============================================================================


def main() -> None:
    """
    Main entry point for running AssetGraph ETL pipeline.

    Usage:
        python pipeline.py [--once]
    """
    import argparse

    parser = argparse.ArgumentParser(
        description="AssetGraph ETL Pipeline: CSV extract, transform, validate, load, and summarize"
    )
    parser.add_argument(
        "--once",
        action="store_true",
        help="Run once and exit",
    )
    _ = parser.parse_args()

    # Create configuration
    config = ETLConfig()

    # Override config for tests using environment variable if set
    if "VIBE_PIPER_TEST_CONFIG" in os.environ:
        # For testing, allow config override via environment
        test_config_str = os.environ["VIBE_PIPER_TEST_CONFIG"]
        from ast import literal_eval

        test_config: dict = literal_eval(test_config_str)
        config = ETLConfig(**test_config)

    # Create and run pipeline
    pipeline = AssetGraphETLPipeline(config)
    pipeline.run_once()


if __name__ == "__main__":
    main()
