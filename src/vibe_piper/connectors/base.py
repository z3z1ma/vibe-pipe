"""
Base protocols for file readers and writers.

This module defines the abstract interfaces that all file connectors must implement.
These protocols provide a unified API for working with different file formats.
"""

from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any, Protocol, runtime_checkable

from vibe_piper.types import DataRecord, Schema


@runtime_checkable
class FileReader(Protocol):
    """
    Protocol for reading data from files.

    All file readers must implement this protocol to ensure a consistent
    interface across different file formats.
    """

    path: str | Path
    """Path to the file to read."""

    def read(
        self,
        schema: Schema | None = None,
        chunk_size: int | None = None,
        **kwargs: Any,
    ) -> Sequence[DataRecord] | "FileReaderIterator":
        """
        Read data from the file.

        Args:
            schema: Optional schema to validate data against.
                   If None, schema will be inferred from the file.
            chunk_size: If provided, returns an iterator that yields
                       chunks of records. Otherwise, returns all records.
            **kwargs: Format-specific options (e.g., delimiter for CSV).

        Returns:
            Either a sequence of DataRecord objects or an iterator
            that yields chunks of records.

        Raises:
            FileNotFoundError: If the file doesn't exist.
            ValueError: If the file format is invalid.
            SchemaError: If data doesn't match the provided schema.
        """
        ...

    def infer_schema(self, **kwargs: Any) -> Schema:
        """
        Infer the schema from the file structure.

        Args:
            **kwargs: Format-specific options for schema inference.

        Returns:
            A Schema object representing the inferred structure.

        Raises:
            FileNotFoundError: If the file doesn't exist.
            ValueError: If the file format is invalid or cannot be read.
        """
        ...

    def get_metadata(self, **kwargs: Any) -> Mapping[str, Any]:
        """
        Get metadata about the file.

        Args:
            **kwargs: Format-specific options.

        Returns:
            A mapping containing metadata such as:
            - size: File size in bytes
            - modified: Last modification timestamp
            - format: File format (csv, json, etc.)
            - compression: Compression type if applicable
            - Additional format-specific metadata
        """
        ...


@runtime_checkable
class FileWriter(Protocol):
    """
    Protocol for writing data to files.

    All file writers must implement this protocol to ensure a consistent
    interface across different file formats.
    """

    path: str | Path
    """Path to the file to write."""

    def write(
        self,
        data: Sequence[DataRecord],
        schema: Schema | None = None,
        compression: str | None = None,
        mode: str = "w",
        **kwargs: Any,
    ) -> int:
        """
        Write data to the file.

        Args:
            data: Sequence of DataRecord objects to write.
            schema: Optional schema to include in the file.
                   For formats that support embedded schemas (e.g., Parquet).
            compression: Compression type (gzip, zip, snappy, etc.).
            mode: Write mode - 'w' for overwrite, 'a' for append.
            **kwargs: Format-specific options (e.g., delimiter for CSV).

        Returns:
            The number of records written.

        Raises:
            ValueError: If data is invalid or incompatible with the format.
            IOError: If the file cannot be written.
            SchemaError: If data doesn't conform to the schema.
        """
        ...

    def write_partitioned(
        self,
        data: Sequence[DataRecord],
        partition_cols: Sequence[str],
        schema: Schema | None = None,
        compression: str | None = None,
        **kwargs: Any,
    ) -> Sequence[str]:
        """
        Write data to partitioned files.

        This method creates a directory structure with files partitioned
        by the specified columns. Useful for large datasets.

        Args:
            data: Sequence of DataRecord objects to write.
            partition_cols: Columns to partition by.
            schema: Optional schema to include in files.
            compression: Compression type.
            **kwargs: Format-specific options.

        Returns:
            Sequence of paths to the written files.

        Raises:
            ValueError: If partition columns are not found in the data.
            IOError: If files cannot be written.
        """
        ...


@runtime_checkable
class FileReaderIterator(Protocol):
    """
    Protocol for iterating over file chunks.

    File readers can return this protocol when chunk_size is specified,
    allowing for memory-efficient processing of large files.
    """

    def __iter__(self) -> "FileReaderIterator":
        """Return the iterator object."""
        ...

    def __next__(self) -> Sequence[DataRecord]:
        """
        Get the next chunk of records.

        Returns:
            A sequence of DataRecord objects.

        Raises:
            StopIteration: When there are no more chunks.
        """
        ...

    def close(self) -> None:
        """Close the file and release resources."""
        ...
