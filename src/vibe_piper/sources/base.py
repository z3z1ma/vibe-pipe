"""
Base Source Protocol for Vibe Piper Sources

This module defines the protocol that all source types must implement.
"""

from abc import ABC, abstractmethod
from collections.abc import AsyncIterator, Sequence
from typing import Any, Generic, TypeVar

from vibe_piper.types import PipelineContext, Schema

T = TypeVar("T")


class Source(ABC, Generic[T]):
    """
    Abstract base class for all data sources.

    Sources provide a declarative interface for data ingestion with built-in
    support for pagination, retry logic, rate limiting, and schema inference.

    Example:
        Define a custom source::

            class MyCustomSource(Source[DataRecord]):
                def __init__(self, config: MyConfig):
                    self.config = config

                async def fetch(self, context: PipelineContext) -> Sequence[DataRecord]:
                    # Fetch data from custom source
                    return []

                async def stream(self, context: PipelineContext) -> AsyncIterator[DataRecord]:
                    # Stream data for large datasets
                    for item in await self.fetch(context):
                        yield item
    """

    @abstractmethod
    async def fetch(self, context: PipelineContext) -> Sequence[T]:
        """
        Fetch all data from this source.

        Args:
            context: Pipeline execution context

        Returns:
            Sequence of data records

        Raises:
            Exception: If fetch fails
        """
        ...

    async def stream(self, context: PipelineContext) -> AsyncIterator[T]:
        """
        Stream data from this source for large datasets.

        Default implementation calls fetch() and yields items sequentially.
        Subclasses can override for more efficient streaming.

        Args:
            context: Pipeline execution context

        Yields:
            Individual data records

        Raises:
            Exception: If stream fails
        """
        for item in await self.fetch(context):
            yield item

    @abstractmethod
    def infer_schema(self) -> Schema:
        """
        Infer the schema of data from this source.

        Returns:
            Inferred schema

        Raises:
            Exception: If schema inference fails
        """
        ...

    def get_metadata(self) -> dict[str, Any]:
        """
        Get metadata about this source.

        Returns:
            Dictionary of metadata (source type, connection info, etc.)
        """
        return {"source_type": self.__class__.__name__}
