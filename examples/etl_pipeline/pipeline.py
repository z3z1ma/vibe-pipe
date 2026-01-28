"""
ETL Pipeline Example: PostgreSQL → Parquet → Analytics

This example demonstrates a complete ETL pipeline with:
- PostgreSQL source connector
- Data transformation and validation
- Parquet file output with partitioning
- Data quality checks
- Error handling and retry logic
- Incremental loading support
- Scheduling capabilities
"""

from __future__ import annotations

import logging
import os
import time
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any, TypeVar

from vibe_piper.connectors.parquet import ParquetWriter
from vibe_piper.connectors.postgres import PostgreSQLConfig, PostgreSQLConnector
from vibe_piper.validation.checks import (
    expect_column_proportion_of_nulls_to_be_between,
    expect_column_values_to_be_dateutil_parseable,
    expect_column_values_to_be_in_set,
    expect_column_values_to_match_regex,
    expect_column_values_to_not_be_null,
    expect_table_row_count_to_be_between,
)
from vibe_piper.validation.suite import ValidationSuite

T = TypeVar("T")


# =============================================================================
# Configuration
# =============================================================================


@dataclass
class ETLPipelineConfig:
    """Configuration for the ETL pipeline."""

    # PostgreSQL connection settings
    pg_host: str = "localhost"
    pg_port: int = 5432
    pg_database: str = "source_db"
    pg_user: str = "etl_user"
    pg_password: str = "etl_password"

    # Source table settings
    source_table: str = "customers"
    batch_size: int = 10000

    # Output settings
    output_dir: str = "output"
    partition_cols: list[str] = field(default_factory=lambda: ["year", "month"])
    compression: str = "snappy"

    # Incremental loading
    incremental: bool = True
    watermark_column: str = "updated_at"
    watermark_file: str = "watermark.txt"

    # Quality settings
    max_null_proportion: float = 0.1
    min_row_count: int = 100

    # Retry settings
    max_retries: int = 3
    retry_delay: int = 5

    # Logging
    log_level: str = "INFO"

    def __post_init__(self) -> None:
        """Initialize configuration from environment variables."""
        # Allow environment variable overrides
        self.pg_host = os.getenv("PG_HOST", self.pg_host)
        self.pg_port = int(os.getenv("PG_PORT", str(self.pg_port)))
        self.pg_database = os.getenv("PG_DATABASE", self.pg_database)
        self.pg_user = os.getenv("PG_USER", self.pg_user)
        self.pg_password = os.getenv("PG_PASSWORD", self.pg_password)

        # Setup logging
        logging.basicConfig(
            level=getattr(logging, self.log_level),
            format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        )


# =============================================================================
# Retry Decorator
# =============================================================================


def retry_on_failure(
    max_retries: int = 3, delay: int = 5
) -> Callable[[Callable[..., T]], Callable[..., T]]:
    """
    Decorator to retry function on failure with exponential backoff.

    Args:
        max_retries: Maximum number of retry attempts
        delay: Initial delay in seconds

    Example:
        >>> @retry_on_failure(max_retries=3, delay=2)
        ... def fetch_data():
        ...     return connector.query("SELECT * FROM table")
    """

    def decorator(func: Callable[..., T]) -> Callable[..., T]:
        def wrapper(*args: Any, **kwargs: Any) -> T:
            last_exception: Exception | None = None
            for attempt in range(max_retries):
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    last_exception = e
                    if attempt < max_retries - 1:
                        wait_time = delay * (2**attempt)  # Exponential backoff
                        logging.warning(
                            f"Attempt {attempt + 1} failed: {e}. "
                            f"Retrying in {wait_time}s..."
                        )
                        time.sleep(wait_time)
                    else:
                        logging.error(f"All {max_retries} attempts failed")

            raise RuntimeError(
                f"Function failed after {max_retries} retries"
            ) from last_exception

        return wrapper

    return decorator


# =============================================================================
# Pipeline Steps
# =============================================================================


@dataclass
class PipelineMetrics:
    """Metrics collected during pipeline execution."""

    rows_extracted: int = 0
    rows_transformed: int = 0
    rows_loaded: int = 0
    validation_errors: int = 0
    start_time: datetime | None = None
    end_time: datetime | None = None
    watermark: datetime | None = None

    def to_dict(self) -> dict[str, Any]:
        """Convert metrics to dictionary."""
        duration = None
        if self.start_time and self.end_time:
            duration = (self.end_time - self.start_time).total_seconds()

        return {
            "rows_extracted": self.rows_extracted,
            "rows_transformed": self.rows_transformed,
            "rows_loaded": self.rows_loaded,
            "validation_errors": self.validation_errors,
            "start_time": self.start_time.isoformat() if self.start_time else None,
            "end_time": self.end_time.isoformat() if self.end_time else None,
            "duration_seconds": duration,
            "watermark": self.watermark.isoformat() if self.watermark else None,
        }


class ETLPipeline:
    """
    Complete ETL Pipeline from PostgreSQL to Parquet.

    Pipeline Steps:
    1. Extract: Read data from PostgreSQL (incremental if enabled)
    2. Transform: Clean, validate, and enrich data
    3. Validate: Run data quality checks
    4. Load: Write to partitioned Parquet files
    5. Update watermark: Track progress for incremental loads

    Example:
        >>> config = ETLPipelineConfig()
        >>> pipeline = ETLPipeline(config)
        >>> metrics = pipeline.run()
        >>> print(f"Processed {metrics.rows_loaded} rows")
    """

    def __init__(self, config: ETLPipelineConfig) -> None:
        """
        Initialize the ETL pipeline.

        Args:
            config: Pipeline configuration
        """
        self.config = config
        self.logger = logging.getLogger(__name__)
        self.metrics = PipelineMetrics()

        # Create output directory
        Path(config.output_dir).mkdir(parents=True, exist_ok=True)

        # Initialize PostgreSQL connector
        self.pg_config = PostgreSQLConfig(
            host=config.pg_host,
            port=config.pg_port,
            database=config.pg_database,
            user=config.pg_user,
            password=config.pg_password,
            pool_size=5,
        )
        self.connector = PostgreSQLConnector(self.pg_config)  # type: ignore[abstract]

        # Initialize validation suite
        self.validation_suite = self._create_validation_suite()

    def _create_validation_suite(self) -> ValidationSuite:
        """Create validation suite with data quality checks."""
        suite = ValidationSuite(name="etl_quality_checks")

        # Add validation checks
        suite.add_check(
            "row_count_check",
            expect_table_row_count_to_be_between(
                min_value=self.config.min_row_count, max_value=1000000
            ),
        )

        suite.add_check(
            "customer_id_not_null",
            expect_column_values_to_not_be_null("customer_id"),
        )

        suite.add_check(
            "email_not_null",
            expect_column_values_to_not_be_null("email"),
        )

        suite.add_check(
            "phone_null_proportion",
            expect_column_proportion_of_nulls_to_be_between(
                "phone", min_value=0.0, max_value=self.config.max_null_proportion
            ),
        )

        suite.add_check(
            "email_format_valid",
            expect_column_values_to_match_regex(
                "email", pattern=r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$"
            ),
        )

        suite.add_check(
            "status_in_allowed_values",
            expect_column_values_to_be_in_set(
                "status", value_set={"active", "inactive", "pending"}
            ),
        )

        suite.add_check(
            "created_at_parseable",
            expect_column_values_to_be_dateutil_parseable("created_at"),
        )

        suite.add_check(
            "updated_at_parseable",
            expect_column_values_to_be_dateutil_parseable("updated_at"),
        )

        return suite

    def _get_watermark(self) -> datetime | None:
        """
        Get current watermark for incremental loading.

        Returns:
            Watermark datetime or None if first run
        """
        watermark_path = Path(self.config.output_dir) / self.config.watermark_file
        if not watermark_path.exists():
            return None

        with open(watermark_path) as f:
            watermark_str = f.read().strip()
            return datetime.fromisoformat(watermark_str)

    def _update_watermark(self, watermark: datetime) -> None:
        """
        Update watermark file.

        Args:
            watermark: New watermark value
        """
        watermark_path = Path(self.config.output_dir) / self.config.watermark_file
        with open(watermark_path, "w") as f:
            f.write(watermark.isoformat())

    def _extract_data(self) -> list[dict[str, Any]]:
        """
        Extract data from PostgreSQL with retry logic.

        Supports incremental loading based on watermark.

        Returns:
            List of data rows
        """

        @retry_on_failure(
            max_retries=self.config.max_retries, delay=self.config.retry_delay
        )
        def _query() -> list[dict[str, Any]]:
            # Build query with or without watermark
            if self.config.incremental:
                watermark = self._get_watermark()
                if watermark:
                    query = f"""
                        SELECT * FROM {self.config.source_table}
                        WHERE {self.config.watermark_column} > %s
                        ORDER BY {self.config.watermark_column}
                    """
                    params = {"watermark": watermark.isoformat()}
                    self.logger.info(f"Extracting data with watermark: {watermark}")
                else:
                    query = f"SELECT * FROM {self.config.source_table}"
                    params = None
                    self.logger.info("Full extract (no watermark)")
            else:
                query = f"SELECT * FROM {self.config.source_table}"
                params = None
                self.logger.info("Full extract (incremental disabled)")

            # Execute query
            result = self.connector.query(query, params)
            return result.rows

        # Connect and query
        with self.connector:
            rows = _query()
            self.metrics.rows_extracted = len(rows)
            self.logger.info(f"Extracted {len(rows)} rows from PostgreSQL")
            return rows

    def _transform_data(self, raw_data: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """
        Transform and clean data.

        Transformations:
        - Add partition columns (year, month)
        - Clean string fields
        - Normalize dates
        - Handle missing values

        Args:
            raw_data: Raw data from source

        Returns:
            Transformed data
        """
        self.logger.info("Transforming data...")

        transformed = []
        for row in raw_data:
            try:
                # Extract date for partitioning
                updated_at = row.get("updated_at") or row.get("created_at")
                if isinstance(updated_at, str):
                    updated_at = datetime.fromisoformat(
                        updated_at.replace("Z", "+00:00")
                    )
                elif not isinstance(updated_at, datetime):
                    updated_at = datetime.now()

                # Add partition columns
                row["year"] = updated_at.year
                row["month"] = f"{updated_at.month:02d}"

                # Clean email (lowercase, strip)
                if row.get("email"):
                    row["email"] = row["email"].lower().strip()

                # Clean phone (strip, remove non-digits)
                if row.get("phone"):
                    phone = str(row["phone"]).strip()
                    row["phone_clean"] = "".join(c for c in phone if c.isdigit())
                else:
                    row["phone_clean"] = None

                # Ensure status is lowercase
                if row.get("status"):
                    row["status"] = row["status"].lower()

                transformed.append(row)

            except Exception as e:
                self.logger.warning(
                    f"Error transforming row {row.get('customer_id')}: {e}"
                )
                continue

        self.metrics.rows_transformed = len(transformed)
        self.logger.info(f"Transformed {len(transformed)} rows")
        return transformed

    def _validate_data(self, data: list[dict[str, Any]]) -> bool:
        """
        Validate data quality using validation suite.

        Args:
            data: Data to validate

        Returns:
            True if validation passed
        """
        self.logger.info("Running data quality validation...")

        # Convert to DataRecord objects
        from vibe_piper.types import DataRecord, Schema

        schema = Schema(
            name="customers",
            fields=[
                # Fields would be defined here based on actual schema
            ],
        )

        records = [DataRecord(data=row, schema=schema) for row in data]

        # Run validation
        result = self.validation_suite.validate(records)

        if not result.is_valid:
            self.logger.error("Data quality validation failed!")
            for error in result.errors:
                self.logger.error(f"  - {error}")
                self.metrics.validation_errors += 1
            return False

        self.logger.info("Data quality validation passed")
        return True

    def _load_data(self, data: list[dict[str, Any]]) -> list[str]:
        """
        Load data to partitioned Parquet files.

        Args:
            data: Data to load

        Returns:
            List of created file paths
        """
        self.logger.info("Loading data to Parquet...")

        # Convert to DataRecord objects
        from vibe_piper.types import DataRecord, Schema

        schema = Schema(
            name="customers",
            fields=[
                # Fields would be defined here
            ],
        )

        records = [DataRecord(data=row, schema=schema) for row in data]

        # Write partitioned Parquet
        output_path = Path(self.config.output_dir) / "customers"
        writer = ParquetWriter(output_path)

        file_paths = writer.write_partitioned(
            records,
            partition_cols=self.config.partition_cols,
            compression=self.config.compression,
        )

        self.metrics.rows_loaded = len(data)
        self.logger.info(f"Loaded {len(data)} rows to {len(file_paths)} Parquet files")
        return file_paths

    def _generate_quality_report(self) -> str:
        """
        Generate quality report from metrics.

        Returns:
            Report file path
        """
        report_path = Path(self.config.output_dir) / "quality_report.txt"

        with open(report_path, "w") as f:
            f.write("ETL Pipeline Quality Report\n")
            f.write("=" * 50 + "\n\n")

            f.write(f"Start Time: {self.metrics.start_time}\n")
            f.write(f"End Time: {self.metrics.end_time}\n")
            if self.metrics.start_time and self.metrics.end_time:
                duration = (
                    self.metrics.end_time - self.metrics.start_time
                ).total_seconds()
                f.write(f"Duration: {duration:.2f} seconds\n")
            f.write("\n")

            f.write("Row Counts:\n")
            f.write(f"  Extracted: {self.metrics.rows_extracted}\n")
            f.write(f"  Transformed: {self.metrics.rows_transformed}\n")
            f.write(f"  Loaded: {self.metrics.rows_loaded}\n")
            f.write("\n")

            f.write("Quality Metrics:\n")
            f.write(f"  Validation Errors: {self.metrics.validation_errors}\n")

            if self.metrics.rows_extracted > 0:
                transformation_rate = (
                    self.metrics.rows_transformed / self.metrics.rows_extracted
                ) * 100
                f.write(f"  Transformation Success Rate: {transformation_rate:.1f}%\n")

            f.write("\n")

            f.write(f"Watermark: {self.metrics.watermark}\n")
            f.write("\n")

            # Validation results summary
            f.write("Validation Checks:\n")
            for check in self.validation_suite.checks:
                check_name = (
                    check.__name__ if hasattr(check, "__name__") else str(check)
                )
                f.write(f"  ✓ {check_name}\n")

        self.logger.info(f"Quality report written to {report_path}")
        return str(report_path)

    def run(self) -> PipelineMetrics:
        """
        Run the complete ETL pipeline.

        Returns:
            Pipeline execution metrics

        Raises:
            RuntimeError: If pipeline fails critically
        """
        self.logger.info("Starting ETL pipeline...")
        self.metrics.start_time = datetime.now()

        try:
            # Step 1: Extract
            raw_data = self._extract_data()
            if not raw_data:
                self.logger.warning("No data extracted, pipeline complete")
                self.metrics.end_time = datetime.now()
                return self.metrics

            # Step 2: Transform
            transformed_data = self._transform_data(raw_data)

            # Step 3: Validate
            if not self._validate_data(transformed_data):
                self.logger.error("Data validation failed, aborting pipeline")
                raise RuntimeError("Data validation failed")

            # Step 4: Load
            self._load_data(transformed_data)

            # Step 5: Update watermark
            if transformed_data:
                # Get max watermark value from data
                watermark_values = [
                    row.get(self.config.watermark_column) for row in transformed_data
                ]
                non_null_watermarks = [w for w in watermark_values if w is not None]
                if non_null_watermarks:
                    max_watermark = max(non_null_watermarks)  # type: ignore[assignment]
                    if isinstance(max_watermark, str):
                        max_watermark = datetime.fromisoformat(
                            max_watermark.replace("Z", "+00:00")
                        )
                    assert isinstance(max_watermark, datetime)
                    self.metrics.watermark = max_watermark
                    self._update_watermark(max_watermark)

            # Step 6: Generate quality report
            self._generate_quality_report()

            self.metrics.end_time = datetime.now()
            self.logger.info(
                f"ETL pipeline completed successfully. "
                f"Loaded {self.metrics.rows_loaded} rows in "
                f"{(self.metrics.end_time - self.metrics.start_time).total_seconds():.2f}s"
            )

            return self.metrics

        except Exception as e:
            self.metrics.end_time = datetime.now()
            self.logger.error(f"Pipeline failed: {e}")
            raise


# =============================================================================
# Scheduling Support
# =============================================================================


class ETLScheduler:
    """
    Scheduler for running ETL pipeline on a schedule.

    Supports:
    - Interval-based scheduling
    - Cron-like scheduling
    - Manual triggering

    Example:
        >>> scheduler = ETLScheduler(pipeline, interval_minutes=60)
        >>> scheduler.start()  # Runs every hour
    """

    def __init__(
        self,
        pipeline: ETLPipeline,
        interval_minutes: int | None = None,
        cron_expression: str | None = None,
    ) -> None:
        """
        Initialize scheduler.

        Args:
            pipeline: ETL pipeline to schedule
            interval_minutes: Run every N minutes (simple scheduling)
            cron_expression: Cron expression for complex scheduling
        """
        self.pipeline = pipeline
        self.interval_minutes = interval_minutes
        self.cron_expression = cron_expression
        self.logger = logging.getLogger(f"{__name__}.Scheduler")
        self._running = False

    def start(self) -> None:
        """Start the scheduler."""
        self._running = True
        self.logger.info(
            f"Scheduler started (interval: {self.interval_minutes} minutes)"
        )

        while self._running:
            try:
                self.logger.info("Running scheduled pipeline execution...")
                metrics = self.pipeline.run()
                self.logger.info(
                    f"Pipeline completed: {metrics.rows_loaded} rows loaded"
                )
            except Exception as e:
                self.logger.error(f"Scheduled pipeline failed: {e}")

            # Wait for next interval
            if self.interval_minutes:
                import time

                self.logger.info(
                    f"Waiting {self.interval_minutes} minutes until next run..."
                )
                time.sleep(self.interval_minutes * 60)
            else:
                break

    def stop(self) -> None:
        """Stop the scheduler."""
        self._running = False
        self.logger.info("Scheduler stopped")

    def run_once(self) -> PipelineMetrics:
        """
        Run pipeline once immediately.

        Returns:
            Pipeline metrics
        """
        return self.pipeline.run()


# =============================================================================
# Main Entry Point
# =============================================================================


def main() -> None:
    """
    Main entry point for running the ETL pipeline.

    Usage:
        python pipeline.py [--full] [--once]

    Environment Variables:
        PG_HOST, PG_PORT, PG_DATABASE, PG_USER, PG_PASSWORD
    """
    import argparse

    parser = argparse.ArgumentParser(description="ETL Pipeline: PostgreSQL to Parquet")
    parser.add_argument(
        "--full", action="store_true", help="Disable incremental loading"
    )
    parser.add_argument("--once", action="store_true", help="Run once and exit")
    parser.add_argument(
        "--interval", type=int, default=60, help="Run interval in minutes"
    )
    args = parser.parse_args()

    # Create configuration
    config = ETLPipelineConfig(incremental=not args.full)

    # Create pipeline
    pipeline = ETLPipeline(config)

    if args.once:
        # Run once
        metrics = pipeline.run()
        print("\nPipeline completed:")
        print(f"  Rows loaded: {metrics.rows_loaded}")
        if metrics.start_time and metrics.end_time:
            duration = (metrics.end_time - metrics.start_time).total_seconds()
            print(f"  Duration: {duration:.2f}s")
        print(f"  Report: {Path(config.output_dir) / 'quality_report.txt'}")
    else:
        # Run with scheduler
        scheduler = ETLScheduler(pipeline, interval_minutes=args.interval)
        try:
            scheduler.start()
        except KeyboardInterrupt:
            print("\nShutting down scheduler...")
            scheduler.stop()


if __name__ == "__main__":
    main()
