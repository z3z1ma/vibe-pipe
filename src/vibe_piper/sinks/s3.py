"""
S3 Sink Implementation

This module provides S3Sink for writing data to S3 with
automatic batching, format support, partitioning, and retry logic.
"""

import logging
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from io import BytesIO
from typing import Any

from vibe_piper.error_handling import RetryConfig, retry_with_backoff
from vibe_piper.sinks.base import SinkResult
from vibe_piper.sinks.file import FileFormat
from vibe_piper.types import DataRecord, PipelineContext, Schema

# =============================================================================
# Logger
# =============================================================================

logger = logging.getLogger(__name__)


# =============================================================================
# Enums
# =============================================================================


class S3Compression(Enum):
    """Supported compression types for S3 uploads."""

    NONE = "none"
    GZIP = "gzip"
    SNAPPY = "snappy"


# =============================================================================
# Configuration
# =============================================================================


@dataclass(frozen=True)
class S3SinkConfig:
    """
    Configuration for S3Sink.

    Attributes:
        bucket: S3 bucket name
        prefix: S3 key prefix (e.g., 'data/users/')
        format: File format (CSV, JSON, JSONL, PARQUET)
        schema: Optional schema for output
        partition_cols: List of columns to partition by
        compression: Compression type
        batch_size: Number of records to batch before upload
        boto3_client: Optional boto3 S3 client (will be created if not provided)
        aws_region: AWS region for S3 client
        retry_config: Retry configuration for upload operations
    """

    bucket: str
    prefix: str = ""
    format: FileFormat = FileFormat.CSV
    schema: Schema | None = None
    partition_cols: list[str] | None = None
    compression: S3Compression = S3Compression.NONE
    batch_size: int = 1000
    boto3_client: Any | None = None
    aws_region: str = "us-east-1"
    retry_config: RetryConfig | None = None


# =============================================================================
# S3 Sink Implementation
# =============================================================================


class S3Sink:
    """
    S3 cloud sink with batching and partitioning.

    This sink supports:
    - Multiple formats: CSV, JSON, JSONL, Parquet
    - Automatic partitioning by columns
    - Automatic batching for efficient uploads
    - Compression support (gzip, snappy)
    - Automatic retry on upload failures

    Example:
        >>> schema = Schema(
        ...     name="users",
        ...     fields=(
        ...         SchemaField(name="id", data_type=DataType.INTEGER),
        ...         SchemaField(name="name", data_type=DataType.STRING),
        ...     )
        ... )
        >>> sink = S3Sink(
        ...     config=S3SinkConfig(
        ...         bucket="my-bucket",
        ...         prefix="users/",
        ...         format=FileFormat.CSV,
        ...         schema=schema,
        ...         batch_size=500,
        ...     )
        ... )
        >>> sink.initialize(context)
        >>> result = sink.write(data_records, context)
        >>> print(f"Uploaded {result.records_written} records")
    """

    def __init__(self, config: S3SinkConfig) -> None:
        """
        Initialize S3Sink.

        Args:
            config: S3Sink configuration
        """
        self._config = config
        self._s3_client: Any = None
        self._total_records_uploaded = 0
        self._total_uploads = 0
        self._total_bytes_uploaded = 0

    def initialize(self, context: PipelineContext) -> None:
        """
        Initialize sink by creating S3 client if not provided.

        Args:
            context: Pipeline execution context
        """
        if self._config.boto3_client is None:
            self._s3_client = self._create_s3_client()
        else:
            self._s3_client = self._config.boto3_client

        logger.info(f"S3Sink initialized for bucket: {self._config.bucket}")

    def cleanup(self, context: PipelineContext) -> None:
        """
        Clean up resources after uploading.

        Args:
            context: Pipeline execution context
        """
        logger.info(
            f"S3Sink cleanup: {self._total_records_uploaded} records uploaded, "
            f"{self._total_uploads} uploads, "
            f"{self._total_bytes_uploaded / 1024 / 1024:.2f} MB transferred"
        )

    def write(
        self,
        data: Sequence[DataRecord],
        context: PipelineContext,
    ) -> SinkResult:
        """
        Write data to S3 with batching and retry.

        Args:
            data: Sequence of DataRecord objects to write
            context: Pipeline execution context

        Returns:
            SinkResult with operation outcome
        """
        if not data:
            return SinkResult(
                success=True,
                records_written=0,
                timestamp=datetime.now(),
            )

        # Get retry decorator if config provided
        if self._config.retry_config:
            retry_decorator = self._create_retry_decorator()
            write_func = retry_decorator(self._upload_with_retry)
        else:
            write_func = self._upload_with_retry

        # Upload with retry
        try:
            records_uploaded = write_func(data, context)
            return SinkResult(
                success=True,
                records_written=records_uploaded,
                timestamp=datetime.now(),
                metrics={
                    "total_records": self._total_records_uploaded,
                    "total_uploads": self._total_uploads,
                    "total_bytes": self._total_bytes_uploaded,
                },
            )
        except Exception as e:
            logger.error(f"S3Sink upload failed: {e}")
            return SinkResult(
                success=False,
                records_written=0,
                error=str(e),
                timestamp=datetime.now(),
            )

    def _upload_with_retry(self, data: Sequence[DataRecord], context: PipelineContext) -> int:
        """
        Upload data to S3 (internal method for retry).

        Args:
            data: Sequence of DataRecord objects to upload
            context: Pipeline execution context

        Returns:
            Number of records uploaded

        Raises:
            Exception: If upload operation fails
        """
        if self._s3_client is None:
            msg = "S3 client not initialized. Call initialize() first."
            raise RuntimeError(msg)

        # Upload in batches
        total_uploaded = 0

        for i in range(0, len(data), self._config.batch_size):
            batch = data[i : i + self._config.batch_size]
            batch_count = self._upload_batch(batch, context)
            total_uploaded += batch_count

        return total_uploaded

    def _upload_batch(self, batch: Sequence[DataRecord], context: PipelineContext) -> int:
        """
        Upload a single batch to S3.

        Args:
            batch: Batch of DataRecord objects to upload
            context: Pipeline execution context

        Returns:
            Number of records uploaded in this batch

        Raises:
            Exception: If upload operation fails
        """
        if not batch:
            return 0

        # Determine S3 key
        s3_key = self._build_s3_key(batch)

        # Generate file content
        file_content = self._generate_file_content(batch)

        # Upload to S3
        try:
            extra_args: dict[str, Any] = {}

            # Add compression if specified
            if self._config.compression != S3Compression.NONE:
                if self._config.compression == S3Compression.GZIP:
                    extra_args["ContentEncoding"] = "gzip"
                    extra_args["ContentType"] = "application/gzip"
                elif self._config.compression == S3Compression.SNAPPY:
                    extra_args["ContentType"] = "application/octet-stream"

            self._s3_client.put_object(
                Bucket=self._config.bucket,
                Key=s3_key,
                Body=file_content,
                **extra_args,
            )

            # Track metrics
            self._total_records_uploaded += len(batch)
            self._total_uploads += 1
            self._total_bytes_uploaded += len(file_content)

            logger.debug(f"Uploaded {len(batch)} records to s3://{self._config.bucket}/{s3_key}")

            return len(batch)

        except Exception as e:
            logger.error(f"Failed to upload to S3: {e}")
            raise

    def _build_s3_key(self, batch: Sequence[DataRecord]) -> str:
        """
        Build S3 key for a batch of records.

        Args:
            batch: Batch of DataRecord objects

        Returns:
            S3 key (path within bucket)
        """
        key_parts = []

        # Add prefix
        if self._config.prefix:
            key_parts.append(self._config.prefix.rstrip("/"))

        # Add partitioning if configured
        if self._config.partition_cols and batch:
            # Get first record's partition values
            first_record = batch[0]

            for col in self._config.partition_cols:
                if col in first_record.data:
                    value = str(first_record.data[col])
                    key_parts.append(f"{col}={value}")

        # Add file name with timestamp
        import time

        timestamp = int(time.time())
        key_parts.append(f"batch_{timestamp}.{self._config.format.value}")

        return "/".join(key_parts)

    def _generate_file_content(self, data: Sequence[DataRecord]) -> bytes:
        """
        Generate file content for upload based on format.

        Args:
            data: Sequence of DataRecord objects

        Returns:
            File content as bytes
        """
        # Convert DataRecords to list of dicts
        records_data = [record.data for record in data]

        if self._config.format == FileFormat.CSV:
            content = self._generate_csv_content(records_data)
        elif self._config.format == FileFormat.JSON:
            import json

            content = json.dumps(records_data, indent=2)
        elif self._config.format == FileFormat.JSONL:
            import json

            content = "\n".join([json.dumps(record) for record in records_data])
        elif self._config.format == FileFormat.PARQUET:
            content = self._generate_parquet_content(records_data)
        else:
            msg = f"Unsupported file format: {self._config.format}"
            raise ValueError(msg)

        # Apply compression if needed
        if self._config.compression == S3Compression.GZIP:
            import gzip

            compressed = BytesIO()
            with gzip.GzipFile(compressed, mode="wb") as gz:
                gz.write(content.encode("utf-8"))
            return compressed.getvalue()
        else:
            return content.encode("utf-8")

    def _generate_csv_content(self, records_data: list[dict[str, Any]]) -> str:
        """Generate CSV content from list of dicts."""
        import csv
        from io import StringIO

        output = StringIO()

        if not records_data:
            return ""

        # Get headers from first record
        headers = list(records_data[0].keys())

        writer = csv.DictWriter(output, fieldnames=headers)
        writer.writeheader()
        writer.writerows(records_data)

        return output.getvalue()

    def _generate_parquet_content(self, records_data: list[dict[str, Any]]) -> bytes:
        """
        Generate Parquet content from list of dicts.

        Args:
            records_data: List of record dictionaries

        Returns:
            Parquet content as bytes
        """
        try:
            import pandas as pd

            df = pd.DataFrame(records_data)
            output = BytesIO()
            df.to_parquet(output, index=False)
            return output.getvalue()
        except ImportError:
            msg = "Parquet format requires pandas and pyarrow to be installed"
            raise ImportError(msg)

    def _create_s3_client(self) -> Any:
        """
        Create boto3 S3 client.

        Returns:
            S3 client instance
        """
        try:
            import boto3

            return boto3.client("s3", region_name=self._config.aws_region)
        except ImportError:
            msg = "boto3 library is required for S3Sink"
            raise ImportError(msg)

    def _create_retry_decorator(self):
        """Create retry decorator from config."""
        config = self._config.retry_config

        if config:
            return retry_with_backoff(
                max_retries=config.max_retries,
                backoff=config.backoff_strategy.name.lower(),
                jitter=config.jitter_strategy.name.lower(),
                base_delay=config.base_delay,
                max_delay=config.max_delay,
                jitter_amount=config.jitter_amount,
                retry_on_exceptions=config.retry_on_exceptions,
            )

        return None

    def get_metrics(self) -> dict[str, int | float]:
        """
        Get metrics about sink operations.

        Returns:
            Dictionary of metric names to values
        """
        return {
            "total_records_uploaded": self._total_records_uploaded,
            "total_uploads": self._total_uploads,
            "total_bytes_uploaded": self._total_bytes_uploaded,
        }
