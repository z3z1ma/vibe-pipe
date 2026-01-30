"""
File Sink Implementation

This module provides FileSink for writing data to files with
format support (CSV, JSON, JSONL, Parquet), automatic partitioning,
and compression support.
"""

import logging
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from pathlib import Path

from vibe_piper.error_handling import RetryConfig, retry_with_backoff
from vibe_piper.sinks.base import SinkResult
from vibe_piper.types import DataRecord, PipelineContext, Schema

# =============================================================================
# Logger
# =============================================================================

logger = logging.getLogger(__name__)


# =============================================================================
# Enums
# =============================================================================


class FileFormat(Enum):
    """Supported file formats for FileSink."""

    CSV = "csv"
    JSON = "json"
    JSONL = "jsonl"
    PARQUET = "parquet"


class Compression(Enum):
    """Supported compression types."""

    NONE = "none"
    SNAPPY = "snappy"
    GZIP = "gzip"


# =============================================================================
# Configuration
# =============================================================================


@dataclass(frozen=True)
class FileSinkConfig:
    """
    Configuration for FileSink.

    Attributes:
        path: Output file path or directory
        format: File format (CSV, JSON, JSONL, PARQUET)
        schema: Optional schema for output
        partition_cols: List of columns to partition by
        compression: Compression type
        create_directory: Create parent directory if needed
        retry_config: Retry configuration for write operations
    """

    path: str | Path
    format: FileFormat = FileFormat.CSV
    schema: Schema | None = None
    partition_cols: list[str] | None = None
    compression: Compression = Compression.NONE
    create_directory: bool = True
    retry_config: RetryConfig | None = None


# =============================================================================
# File Sink Implementation
# =============================================================================


class FileSink:
    """
    File-based sink with format support and partitioning.

    This sink supports:
    - Multiple formats: CSV, JSON, JSONL, Parquet
    - Automatic partitioning by columns (year, month, day)
    - Compression support (snappy, gzip, none)
    - Auto-directory creation
    - Batched writes with retry

    Example:
        >>> schema = Schema(
        ...     name="users",
        ...     fields=(
        ...         SchemaField(name="id", data_type=DataType.INTEGER),
        ...         SchemaField(name="name", data_type=DataType.STRING),
        ...     )
        ... )
        >>> sink = FileSink(
        ...     config=FileSinkConfig(
        ...         path="output/users.csv",
        ...         format=FileFormat.CSV,
        ...         schema=schema,
        ...         partition_cols=["year", "month"],
        ...     )
        ... )
        >>> sink.initialize(context)
        >>> result = sink.write(data_records, context)
        >>> print(f"Written {result.records_written} records")
    """

    def __init__(self, config: FileSinkConfig) -> None:
        """
        Initialize FileSink.

        Args:
            config: FileSink configuration
        """
        self._config = config
        self._total_records_written = 0
        self._total_files_created = 0

    def initialize(self, context: PipelineContext) -> None:
        """
        Initialize sink by creating directory if needed.

        Args:
            context: Pipeline execution context
        """
        path = Path(self._config.path)

        # Create parent directory if needed
        if self._config.create_directory:
            if self._config.partition_cols:
                # For partitioned files, create the base directory
                path.mkdir(parents=True, exist_ok=True)
                logger.info(f"Created partition directory: {path}")
            elif path.suffix:
                # For single files, create parent directory
                path.parent.mkdir(parents=True, exist_ok=True)
                logger.info(f"Created directory: {path.parent}")

    def cleanup(self, context: PipelineContext) -> None:
        """
        Clean up resources after writing.

        Args:
            context: Pipeline execution context
        """
        logger.info(
            f"FileSink cleanup: {self._total_records_written} records written, "
            f"{self._total_files_created} files created"
        )

    def write(
        self,
        data: Sequence[DataRecord],
        context: PipelineContext,
    ) -> SinkResult:
        """
        Write data to file(s) with partitioning and retry.

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
            write_func = retry_decorator(self._write_with_retry)
        else:
            write_func = self._write_with_retry

        # Write with retry
        try:
            records_written = write_func(data, context)
            return SinkResult(
                success=True,
                records_written=records_written,
                timestamp=datetime.now(),
                metrics={
                    "total_records": self._total_records_written,
                    "total_files": self._total_files_created,
                },
            )
        except Exception as e:
            logger.error(f"FileSink write failed: {e}")
            return SinkResult(
                success=False,
                records_written=0,
                error=str(e),
                timestamp=datetime.now(),
            )

    def _write_with_retry(self, data: Sequence[DataRecord], context: PipelineContext) -> int:
        """
        Write data to file (internal method for retry).

        Args:
            data: Sequence of DataRecord objects to write
            context: Pipeline execution context

        Returns:
            Number of records written

        Raises:
            Exception: If write operation fails
        """
        if self._config.partition_cols:
            # Write partitioned files
            return self._write_partitioned(data, context)
        else:
            # Write single file
            return self._write_single_file(data, context)

    def _write_partitioned(self, data: Sequence[DataRecord], context: PipelineContext) -> int:
        """
        Write data to partitioned files.

        Args:
            data: Sequence of DataRecord objects to write
            context: Pipeline execution context

        Returns:
            Number of records written
        """
        # Use pandas for grouping and partitioning
        import pandas as pd

        # Convert to DataFrame for grouping
        records_data = [record.data for record in data]
        df = pd.DataFrame(records_data)

        # Get unique partition values
        partition_df = df.groupby(self._config.partition_cols, dropna=False)

        # Write each partition
        total_written = 0
        for partition_key, partition_df in partition_df:
            # Create partition directory name
            if isinstance(partition_key, tuple):
                partition_parts = [
                    f"{col}={val}"
                    for col, val in zip(self._config.partition_cols, partition_key, strict=False)
                ]
            else:
                partition_parts = [f"{self._config.partition_cols[0]}={partition_key}"]

            partition_name = "/".join(partition_parts)
            base_path = Path(self._config.path)
            partition_dir = base_path / partition_name

            # Create partition directory
            partition_dir.mkdir(parents=True, exist_ok=True)

            # Write partition file
            partition_path = partition_dir / f"data.{self._config.format.value}"

            # Convert DataFrame back to DataRecords
            partition_records = [
                DataRecord(data=row.to_dict(), schema=self._config.schema)
                for _, row in partition_df.iterrows()
            ]

            self._write_file(partition_path, partition_records)
            self._total_files_created += 1
            total_written += len(partition_records)

        return total_written

    def _write_single_file(self, data: Sequence[DataRecord], context: PipelineContext) -> int:
        """
        Write data to a single file.

        Args:
            data: Sequence of DataRecord objects to write
            context: Pipeline execution context

        Returns:
            Number of records written
        """
        output_path = Path(self._config.path)
        self._write_file(output_path, data)
        self._total_files_created += 1
        return len(data)

    def _write_file(self, file_path: Path, data: Sequence[DataRecord]) -> None:
        """
        Write data to a file in the configured format.

        Args:
            file_path: Path to write to
            data: Data to write
        """
        # Get compression setting
        compression_val = None
        if self._config.compression != Compression.NONE:
            compression_val = self._config.compression.value

        # Write based on format
        if self._config.format == FileFormat.CSV:
            self._write_csv(file_path, data, compression_val)
        elif self._config.format == FileFormat.JSON:
            self._write_json(file_path, data, compression_val)
        elif self._config.format == FileFormat.JSONL:
            self._write_jsonl(file_path, data, compression_val)
        elif self._config.format == FileFormat.PARQUET:
            self._write_parquet(file_path, data, compression_val)
        else:
            msg = f"Unsupported file format: {self._config.format}"
            raise ValueError(msg)

    def _write_csv(
        self, file_path: Path, data: Sequence[DataRecord], compression: str | None
    ) -> None:
        """Write data to CSV file."""
        import csv
        from io import StringIO

        output = StringIO()

        if not data:
            # Write empty file
            file_path.write_text("", encoding="utf-8")
            return

        # Get headers from first record
        headers = list(data[0].data.keys())

        writer = csv.DictWriter(output, fieldnames=headers)
        writer.writeheader()
        writer.writerows([record.data for record in data])

        # Handle compression
        if compression == "gzip":
            import gzip

            file_path.write_bytes(gzip.compress(output.getvalue().encode("utf-8")))
        else:
            file_path.write_text(output.getvalue(), encoding="utf-8")

    def _write_json(
        self, file_path: Path, data: Sequence[DataRecord], compression: str | None
    ) -> None:
        """Write data to JSON file."""
        import json

        content = json.dumps([record.data for record in data], indent=2)

        # Handle compression
        if compression == "gzip":
            import gzip

            file_path.write_bytes(gzip.compress(content.encode("utf-8")))
        else:
            file_path.write_text(content, encoding="utf-8")

    def _write_jsonl(
        self, file_path: Path, data: Sequence[DataRecord], compression: str | None
    ) -> None:
        """Write data to JSONL file."""
        import json

        content = "\n".join([json.dumps(record.data) for record in data])

        # Handle compression
        if compression == "gzip":
            import gzip

            file_path.write_bytes(gzip.compress(content.encode("utf-8")))
        else:
            file_path.write_text(content, encoding="utf-8")

    def _write_parquet(
        self, file_path: Path, data: Sequence[DataRecord], compression: str | None
    ) -> None:
        """Write data to Parquet file."""
        try:
            import pandas as pd

            records_data = [record.data for record in data]
            df = pd.DataFrame(records_data)

            # Handle compression
            compression_type = None
            if compression == "gzip":
                compression_type = "gzip"
            elif compression == "snappy":
                compression_type = "snappy"

            df.to_parquet(file_path, compression=compression_type, index=False)
        except ImportError:
            msg = "Parquet format requires pandas and pyarrow to be installed"
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
            "total_records_written": self._total_records_written,
            "total_files_created": self._total_files_created,
        }
