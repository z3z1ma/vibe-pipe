"""
Base Sink Protocol and Classes

This module defines the base Sink protocol and result types used by all
sink implementations in Vibe Piper.
"""

from collections.abc import Sequence
from dataclasses import dataclass
from datetime import datetime
from typing import Protocol, runtime_checkable

from vibe_piper.types import DataRecord, PipelineContext


@dataclass(frozen=True)
class SinkResult:
    """
    Result of a sink write operation.

    Attributes:
        success: Whether the write operation succeeded
        records_written: Number of records written
        error: Error message if operation failed
        metrics: Additional metrics about the operation
        timestamp: When the write operation completed
    """

    success: bool
    records_written: int = 0
    error: str | None = None
    metrics: dict[str, int | float] | None = None
    timestamp: datetime | None = None


@runtime_checkable
class BaseSink(Protocol):
    """
    Protocol for all sink implementations.

    Sinks are responsible for writing data to various destinations
    (databases, files, S3, etc.) with automatic handling of
    DDL, batching, and error recovery.

    Example:
        Basic sink usage::

            sink = MySink(config)
            result = sink.write(data, context)
            if result.success:
                print(f"Written {result.records_written} records")
    """

    def write(
        self,
        data: Sequence[DataRecord],
        context: PipelineContext,
    ) -> SinkResult:
        """
        Write data to this sink.

        Args:
            data: Sequence of DataRecord objects to write
            context: Pipeline execution context

        Returns:
            SinkResult with operation outcome

        Raises:
            Exception: If write operation fails (sink should handle retries internally)
        """
        ...

    def initialize(self, context: PipelineContext) -> None:
        """
        Initialize the sink before writing.

        This method is called before the first write operation.
        Sinks can use this to create tables, directories,
        or set up connections.

        Args:
            context: Pipeline execution context
        """
        ...

    def cleanup(self, context: PipelineContext) -> None:
        """
        Clean up resources after writing.

        This method is called after all writes are complete.
        Sinks can use this to close connections, flush buffers,
        or release resources.

        Args:
            context: Pipeline execution context
        """
        ...

    def get_metrics(self) -> dict[str, int | float]:
        """
        Get metrics about sink operations.

        Returns:
            Dictionary of metric names to values
        """
        ...
