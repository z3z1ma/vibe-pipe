"""
Parquet file reader and writer.

Provides Parquet I/O with support for:
- Efficient columnar storage
- Multiple compression codecs (snappy, gzip, brotli, lz4)
- Schema preservation
- Partitioned datasets
"""

from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any

import pandas as pd

from vibe_piper.connectors.base import FileReader, FileReaderIterator, FileWriter
from vibe_piper.connectors.utils.inference import infer_schema_from_pandas
from vibe_piper.types import DataRecord, Schema

# =============================================================================
# Parquet Reader
# =============================================================================#


class ParquetReader(FileReader):
    """
    Reader for Parquet files.

    Parquet is an efficient columnar storage format ideal for large datasets.

    Example:
        >>> reader = ParquetReader("data.parquet")
        >>> data = reader.read()
        >>>
        >>> # Read specific columns
        >>> reader = ParquetReader("data.parquet")
        >>> data = reader.read(columns=["id", "name"])
        >>>
        >>> # Chunked reading for large files
        >>> reader = ParquetReader("large.parquet")
        >>> for chunk in reader.read(chunk_size=1000):
        ...     process(chunk)
    """

    def __init__(
        self,
        path: str | Path,
        engine: str = "pyarrow",
        **kwargs: Any,
    ) -> None:
        """
        Initialize the Parquet reader.

        Args:
            path: Path to the Parquet file or directory.
            engine: Parquet engine ('pyarrow' or 'fastparquet').
            **kwargs: Additional options passed to pandas.read_parquet.
        """
        self.path = Path(path)
        self.engine = engine
        self.kwargs = kwargs

    def read(
        self,
        schema: Schema | None = None,
        chunk_size: int | None = None,
        **kwargs: Any,
    ) -> Sequence[DataRecord] | "ParquetReaderIterator":
        """
        Read data from the Parquet file.

        Args:
            schema: Optional schema to validate against.
            chunk_size: If specified, returns an iterator yielding chunks.
            **kwargs: Additional options for pandas.read_parquet.

        Returns:
            Sequence of DataRecord objects or an iterator.

        Raises:
            FileNotFoundError: If the file doesn't exist.
            ValueError: If the Parquet format is invalid.
        """
        # Combine kwargs
        read_kwargs = {**self.kwargs, **kwargs}

        # Set engine
        read_kwargs["engine"] = self.engine

        # Read the data
        if chunk_size:
            # Return iterator for chunked reading
            return ParquetReaderIterator(self.path, chunk_size, read_kwargs)

        # Read all data
        df = pd.read_parquet(self.path, **read_kwargs)

        # Validate against schema if provided
        if schema:
            self._validate_dataframe(df, schema)

        # Convert to DataRecord objects
        return self._dataframe_to_records(df, schema)

    def infer_schema(self, **kwargs: Any) -> Schema:
        """
        Infer the schema from the Parquet file.

        Parquet files contain embedded schema information.

        Args:
            **kwargs: Additional options for pandas.read_parquet.

        Returns:
            Inferred Schema.
        """
        read_kwargs = {**self.kwargs, **kwargs}
        read_kwargs["engine"] = self.engine

        # Read just the schema metadata (pandas reads all data anyway)
        df = pd.read_parquet(self.path, **read_kwargs)

        return infer_schema_from_pandas(df, name=self.path.stem)

    def get_metadata(self) -> Mapping[str, Any]:
        """
        Get metadata about the Parquet file.

        Returns:
            Metadata mapping.
        """
        try:
            import pyarrow.parquet as pq

            parquet_file = pq.ParquetFile(self.path)
            metadata = parquet_file.metadata

            # Get compression from the first row group's schema
            compression_str = None
            if metadata.num_row_groups > 0:
                try:
                    compression_str = str(
                        metadata.schema.to_arrow_schema().pandas_metadata
                    )
                except Exception:
                    compression_str = None

            return {
                "format": "parquet",
                "engine": self.engine,
                "rows": metadata.num_rows,
                "columns": metadata.num_columns,
                "column_names": metadata.schema.names,
                "compression": compression_str,
                "row_groups": metadata.num_row_groups,
                "size_bytes": metadata.serialized_size,
            }

        except ImportError:
            # Fallback to pandas
            stat = self.path.stat()

            # Read sample to get columns
            df = pd.read_parquet(self.path, nrows=1, engine=self.engine)

            return {
                "format": "parquet",
                "engine": self.engine,
                "size": stat.st_size,
                "modified": stat.st_mtime,
                "columns": len(df.columns),
                "column_names": list(df.columns),
            }

    def _validate_dataframe(self, df: pd.DataFrame, schema: Schema) -> None:
        """Validate DataFrame against schema."""
        # Check that all required fields are present
        schema_field_names = {f.name for f in schema.fields}
        df_columns = set(df.columns)

        missing_fields = schema_field_names - df_columns
        if missing_fields:
            msg = f"Missing required fields in Parquet: {missing_fields}"
            raise ValueError(msg)

    def _dataframe_to_records(
        self,
        df: pd.DataFrame,
        schema: Schema | None = None,
    ) -> list[DataRecord]:
        """Convert DataFrame to DataRecord objects."""
        # Use provided schema or infer from DataFrame
        if schema is None:
            schema = infer_schema_from_pandas(df, name=self.path.stem)

        records: list[DataRecord] = []

        for _, row in df.iterrows():
            # Convert row to dict, handling NaN values
            data = {col: (None if pd.isna(val) else val) for col, val in row.items()}

            record = DataRecord(data=data, schema=schema)
            records.append(record)

        return records


class ParquetReaderIterator(FileReaderIterator):
    """
    Iterator for reading Parquet files in chunks.

    Example:
        >>> reader = ParquetReader("large.parquet")
        >>> for chunk in reader.read(chunk_size=1000):
        ...     for record in chunk:
        ...         process(record)
    """

    def __init__(
        self,
        path: Path,
        chunk_size: int,
        kwargs: Mapping[str, Any],
    ) -> None:
        """Initialize the iterator."""
        self.path = path
        self.chunk_size = chunk_size
        self.kwargs = kwargs
        self._iterator: Any = None

    def __iter__(self) -> "ParquetReaderIterator":
        """Initialize the pandas chunk iterator."""
        try:
            import pyarrow.parquet as pq

            # Use PyArrow for chunked reading
            parquet_file = pq.ParquetFile(self.path)
            self._iterator = parquet_file.iter_batches(batch_size=self.chunk_size)
        except ImportError as err:
            msg = "Chunked Parquet reading requires pyarrow. Install with: pip install pyarrow"
            raise ImportError(msg) from err

        return self

    def __next__(self) -> Sequence[DataRecord]:
        """Get the next chunk of records."""
        if self._iterator is None:
            msg = "Iterator not initialized. Use iter() first."
            raise RuntimeError(msg)

        try:
            batch = next(self._iterator)
            # Convert arrow batch to pandas
            df = batch.to_pandas()
        except StopIteration:
            raise

        # Convert DataFrame to records
        schema = infer_schema_from_pandas(df, name=self.path.stem)
        records: list[DataRecord] = []

        for _, row in df.iterrows():
            data = {col: (None if pd.isna(val) else val) for col, val in row.items()}
            record = DataRecord(data=data, schema=schema)
            records.append(record)

        return records

    def close(self) -> None:
        """Close the iterator."""
        self._iterator = None


# =============================================================================
# Parquet Writer
# =============================================================================#


class ParquetWriter(FileWriter):
    """
    Writer for Parquet files.

    Parquet is an efficient columnar storage format ideal for large datasets.

    Example:
        >>> writer = ParquetWriter("output.parquet")
        >>> count = writer.write(data)
        >>>
        >>> # With compression
        >>> writer = ParquetWriter("output.parquet")
        >>> count = writer.write(data, compression="snappy")
        >>>
        >>> # Write partitioned dataset
        >>> writer = ParquetWriter("output/")
        >>> paths = writer.write_partitioned(data, partition_cols=["year", "month"])
    """

    # Supported compression codecs
    SUPPORTED_COMPRESSION: tuple[str, ...] = ("snappy", "gzip", "brotli", "lz4", "zstd")

    def __init__(
        self,
        path: str | Path,
        engine: str = "pyarrow",
        **kwargs: Any,
    ) -> None:
        """
        Initialize the Parquet writer.

        Args:
            path: Path to the output Parquet file or directory.
            engine: Parquet engine ('pyarrow' or 'fastparquet').
            **kwargs: Additional options passed to pandas.DataFrame.to_parquet.
        """
        self.path = Path(path)
        self.engine = engine
        self.kwargs = kwargs

    def write(
        self,
        data: Sequence[DataRecord],
        schema: Schema | None = None,
        compression: str | None = None,
        mode: str = "w",
        **kwargs: Any,
    ) -> int:
        """
        Write data to a Parquet file.

        Args:
            data: Sequence of DataRecord objects to write.
            schema: Optional schema (Parquet preserves schema automatically).
            compression: Compression type ('snappy', 'gzip', 'brotli', 'lz4', 'zstd').
            mode: Write mode ('w' for overwrite, 'a' for append).
            **kwargs: Additional options for pandas.DataFrame.to_parquet.

        Returns:
            Number of records written.

        Raises:
            ValueError: If data is empty or compression is not supported.
            IOError: If the file cannot be written.
        """
        if not data:
            msg = "Cannot write empty data to Parquet"
            raise ValueError(msg)

        # Validate compression
        if compression and compression not in self.SUPPORTED_COMPRESSION:
            msg = (
                f"Unsupported compression: {compression!r}. "
                f"Supported: {self.SUPPORTED_COMPRESSION}"
            )
            raise ValueError(msg)

        # Combine kwargs
        write_kwargs = {**self.kwargs, **kwargs}

        # Convert records to DataFrame
        df = self._records_to_dataframe(data, schema)

        # Set engine
        write_kwargs["engine"] = self.engine

        # Set compression
        if compression:
            write_kwargs["compression"] = compression

        # Write mode
        if mode == "a" and self.path.exists():
            # Append mode - read existing data and combine
            existing_df = pd.read_parquet(self.path, engine=self.engine)
            df = pd.concat([existing_df, df], ignore_index=True)

        # Write to Parquet
        df.to_parquet(self.path, **write_kwargs)

        return len(data)

    def write_partitioned(
        self,
        data: Sequence[DataRecord],
        partition_cols: Sequence[str],
        schema: Schema | None = None,
        compression: str | None = None,
        **kwargs: Any,
    ) -> Sequence[str]:
        """
        Write data to a partitioned Parquet dataset.

        Creates a directory structure with Parquet files partitioned by the specified columns.
        This uses Apache Parquet's native partitioning (Hive-style).

        Args:
            data: Sequence of DataRecord objects to write.
            partition_cols: Columns to partition by.
            schema: Optional schema.
            compression: Compression type.
            **kwargs: Additional options.

        Returns:
            Sequence of paths to the written partition files.

        Raises:
            ValueError: If partition columns are not found in the data.
        """
        if not data:
            return []

        # Convert to DataFrame
        df = self._records_to_dataframe(data, schema)

        # Validate partition columns
        missing_cols = set(partition_cols) - set(df.columns)
        if missing_cols:
            msg = f"Partition columns not found in data: {missing_cols}"
            raise ValueError(msg)

        # Create base directory
        base_path = self.path
        base_path.mkdir(parents=True, exist_ok=True)

        # Combine kwargs
        write_kwargs = {**self.kwargs, **kwargs}
        write_kwargs["engine"] = self.engine

        # Set compression
        if compression:
            if compression not in self.SUPPORTED_COMPRESSION:
                msg = (
                    f"Unsupported compression: {compression!r}. "
                    f"Supported: {self.SUPPORTED_COMPRESSION}"
                )
                raise ValueError(msg)
            write_kwargs["compression"] = compression

        # Write partitioned dataset using pandas
        df.to_parquet(
            base_path,
            partition_cols=list(partition_cols),
            **write_kwargs,
        )

        # Return list of created files
        partition_files = list(base_path.rglob("*.parquet"))
        return [str(p) for p in partition_files]

    def _records_to_dataframe(
        self,
        records: Sequence[DataRecord],
        schema: Schema | None = None,
    ) -> pd.DataFrame:
        """Convert DataRecord objects to a pandas DataFrame."""
        # Use schema for column ordering if provided
        if schema:
            columns = [f.name for f in schema.fields]
        else:
            # Infer from first record
            columns = list(records[0].data.keys()) if records else []

        # Extract data
        data = [record.data for record in records]

        # Create DataFrame
        df = pd.DataFrame(
            data, columns=columns if set(columns) == set(data[0].keys()) else None
        )

        return df
