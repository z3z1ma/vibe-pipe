"""
AssetGraph ETL Pipeline Example

A canonical, fully runnable AssetGraph example that showcases:
- Declarative assets with PipelineDefinitionContext
- ExecutionEngine for orchestration
- ValidationSuite for data quality checks
- File-based I/O (CSV input/output)
- Extract, transform, validate, load, and summarize steps
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import TYPE_CHECKING

from vibe_piper.connectors.csv import CSVReader, CSVWriter
from vibe_piper.execution import ExecutionEngine
from vibe_piper.pipeline import PipelineDefinitionContext
from vibe_piper.validation.checks import (
    expect_column_values_to_be_in_set,
    expect_column_values_to_match_regex,
    expect_column_values_to_not_be_null,
    expect_table_row_count_to_be_between,
)
from vibe_piper.validation.suite import ValidationSuite

if TYPE_CHECKING:
    from vibe_piper.types import AssetGraph

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
    """Configuration for the AssetGraph ETL pipeline."""

    input_path: str = "data/users.csv"
    output_dir: str = "output"

    # Validation thresholds
    min_row_count: int = 5
    max_row_count: int = 1000
    max_null_proportion: float = 0.1

    def __post_init__(self) -> None:
        """Ensure output directory exists."""
        Path(self.output_dir).mkdir(parents=True, exist_ok=True)


# =============================================================================
# Pipeline Definition
# =============================================================================


def build_pipeline(config: ETLConfig) -> AssetGraph:
    """
    Build an AssetGraph for the ETL pipeline.

    Assets:
    1. extract - Read raw data from CSV
    2. transform - Clean and enrich the data
    3. validate - Run data quality checks
    4. load - Write transformed data to CSV
    5. summarize - Generate summary statistics

    Args:
        config: Pipeline configuration

    Returns:
        AssetGraph ready for execution
    """
    with PipelineDefinitionContext(
        "asset_graph_etl",
        description="Local ETL pipeline with AssetGraph, ValidationSuite, and CSV I/O",
    ) as pipeline:
        # Asset 1: Extract raw data from CSV
        @pipeline.asset(description="Read raw user data from input CSV")
        def extract() -> list[dict]:
            logger.info(f"Extracting data from {config.input_path}")
            reader = CSVReader(config.input_path)
            records = reader.read()
            # Convert DataRecord objects to dicts for easier manipulation
            data = [record.data for record in records]
            logger.info(f"Extracted {len(data)} records")
            return data

        # Asset 2: Transform the data
        @pipeline.asset(description="Clean and enrich user data")
        def transform(extract: list[dict]) -> list[dict]:
            logger.info("Transforming data...")
            transformed = []

            for row in extract:
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
                        days_since_login = (datetime.now() - dt_login).days
                        cleaned["days_since_login"] = days_since_login
                    except (ValueError, TypeError):
                        cleaned["days_since_login"] = None
                else:
                    cleaned["days_since_login"] = None

                transformed.append(cleaned)

            logger.info(f"Transformed {len(transformed)} records")
            return transformed

        # Asset 3: Validate the transformed data
        @pipeline.asset(description="Run data quality validation checks")
        def validate(transform: list[dict]) -> dict:
            logger.info("Validating data...")

            # Create validation suite
            suite = ValidationSuite(name="etl_quality_checks")
            suite.strategy = "fail_fast"

            # Add validation checks
            suite.add_check(
                "min_row_count",
                expect_table_row_count_to_be_between(config.min_row_count, config.max_row_count),
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
            from vibe_piper.types import DataRecord, DataType, Schema, SchemaField

            # Infer schema from first record
            schema_fields = [
                SchemaField(name=key, data_type=DataType.STRING, nullable=True)
                for key in transform[0].keys()
            ]
            schema = Schema(name="transformed_users", fields=tuple(schema_fields))
            records = [DataRecord(data=row, schema=schema) for row in transform]

            # Run validation
            result = suite.validate(records)

            if not result.is_valid:
                logger.error("Data validation failed!")
                for error in result.errors:
                    logger.error(f"  - {error}")
                msg = "Data validation failed"
                raise ValueError(msg)

            # Return validation result for downstream assets
            return {
                "is_valid": True,
                "checks_passed": len(result.warnings),
                "check_names": [check.__name__ for check in suite.checks],
            }

        # Asset 4: Load validated data to CSV
        @pipeline.asset(description="Write transformed data to output CSV")
        def load(transform: list[dict]) -> str:
            logger.info("Loading data to output...")

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
            for row in transform:
                filtered = {col: row.get(col) for col in output_columns if col in row}
                output_data.append(filtered)

            # Write to CSV
            from vibe_piper.types import DataRecord, DataType, Schema, SchemaField

            output_schema_fields = [
                SchemaField(name=col, data_type=DataType.STRING, nullable=True)
                for col in output_columns
            ]
            output_schema = Schema(name="output_users", fields=tuple(output_schema_fields))

            records = [DataRecord(data=filtered, schema=output_schema) for filtered in output_data]

            output_path = Path(config.output_dir) / "users_transformed.csv"
            writer = CSVWriter(output_path)
            count = writer.write(records, schema=output_schema)

            logger.info(f"Loaded {count} records to {output_path}")
            return str(output_path)

        # Asset 5: Generate summary statistics
        @pipeline.asset(description="Generate pipeline summary statistics")
        def summarize(transform: list[dict], load: str) -> dict:
            logger.info("Generating summary...")

            # Calculate statistics
            total_users = len(transform)
            status_counts = {}
            tier_counts = {}
            total_revenue = 0.0
            total_orders = 0

            for row in transform:
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
                "output_file": load,
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
            summary_path = Path(config.output_dir) / "summary.json"
            import json

            with open(summary_path, "w") as f:
                json.dump(summary, f, indent=2)

            logger.info(f"Summary written to {summary_path}")

            return summary

    # Build and return the graph
    graph = pipeline.build()
    logger.info(f"Built pipeline graph with {len(graph.assets)} assets")
    return graph


# =============================================================================
# Main Entry Point
# =============================================================================


def main() -> None:
    """
    Main entry point for running the AssetGraph ETL pipeline.

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
    _ = parser.parse_args()  # args not used yet, but may be used for scheduling in future

    # Create configuration
    config = ETLConfig()

    # Build the pipeline
    graph = build_pipeline(config)

    # Execute the pipeline
    engine = ExecutionEngine()
    result = engine.execute(graph)

    # Report results
    if result.success:
        logger.info(f"Pipeline completed successfully. Executed {result.assets_executed} assets.")
        # Print final summary
        summary_result = result.get_asset_output("summarize")
        if summary_result:
            print("\n" + "=" * 50)
            print("PIPELINE SUMMARY")
            print("=" * 50)
            print(f"Total users: {summary_result.get('total_users')}")
            print(f"Status distribution: {summary_result.get('status_distribution')}")
            print(f"Tier distribution: {summary_result.get('tier_distribution')}")
            print(f"Total revenue: ${summary_result.get('total_revenue', 0):.2f}")
            print(f"Average order value: ${summary_result.get('average_order_value', 0):.2f}")
            print(f"Output file: {summary_result.get('output_file')}")
            print("=" * 50 + "\n")
    else:
        logger.error(
            f"Pipeline failed. {result.assets_failed} assets failed. See logs for details."
        )
        for asset_name in result.get_failed_assets():
            logger.error(f"Failed asset: {asset_name}")

        raise RuntimeError("Pipeline execution failed")


if __name__ == "__main__":
    main()
