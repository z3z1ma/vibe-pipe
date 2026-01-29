"""
CSV file reader and writer.

Provides CSV I/O with support for:
- Auto-detection of delimiters and dialects
- Schema inference and validation
- Compression (gzip)
- Chunked reading for large files
- Custom dialects and encodings
"""

from collections.abc import Iterator, Mapping, Sequence
from csv import Sniffer
from pathlib import Path
from typing import Any

import pandas as pd

from vibe_piper.connectors.base import FileReader, FileReaderIterator, FileWriter
from vibe_piper.connectors.utils.compression import detect_compression
from vibe_piper.connectors.utils.inference import infer_schema_from_pandas
from vibe_piper.types import DataRecord, Schema

# =============================================================================
# CSV Reader
# =============================================================================#


class CSVReader(FileReader):
    """
    Reader for CSV files.

    Supports auto-detection of delimiters, compression, and schema inference.

    Example:
        >>> reader = CSVReader("data.csv")
        >>> data = reader.read()
        >>>
        >>> # With custom delimiter
        >>> reader = CSVReader("data.tsv", delimiter="\\t")
        >>> data = reader.read()
        >>>
        >>> # Chunked reading
        >>> reader = CSVReader("large.csv")
        >>> for chunk in reader.read(chunk_size=1000):
        ...     process(chunk)
    """

    def __init__(
        self,
        path: str | Path,
        encoding: str = "utf-8",
        **kwargs: Any,
    ) -> None:
        """
        Initialize the CSV reader.

        Args:
            path: Path to the CSV file.
            encoding: File encoding (default: utf-8).
            **kwargs: Additional options passed to pandas.read_csv.
        """
        self.path = Path(path)
        self.encoding = encoding
        self.kwargs = kwargs

    def read(
        self,
        schema: Schema | None = None,
        chunk_size: int | None = None,
        **kwargs: Any,
    ) -> Sequence[DataRecord] | "CSVReaderIterator":
        """
        Read data from the CSV file.

        Args:
            schema: Optional schema to validate against.
            chunk_size: If specified, returns an iterator yielding chunks.
            **kwargs: Additional options for pandas.read_csv.

        Returns:
            Sequence of DataRecord objects or an iterator.

        Raises:
            FileNotFoundError: If the file doesn't exist.
            ValueError: If the CSV format is invalid.
        """
        # Combine with initial kwargs
        read_kwargs = {**self.kwargs, **kwargs}

        # Auto-detect delimiter if not specified
        if "delimiter" not in read_kwargs and "sep" not in read_kwargs:
            read_kwargs["delimiter"] = self._detect_delimiter()

        # Handle compression
        compression = detect_compression(self.path)
        if compression:
            read_kwargs["compression"] = compression

        # Set encoding
        read_kwargs["encoding"] = self.encoding

        # Read the data
        if chunk_size:
            # Return iterator for chunked reading
            return CSVReaderIterator(self.path, chunk_size, read_kwargs)

        # Read all data
        df = pd.read_csv(self.path, **read_kwargs)

        # Validate against schema if provided
        if schema:
            self._validate_dataframe(df, schema)

        # Convert to DataRecord objects
        return self._dataframe_to_records(df, schema)

    def infer_schema(self, **kwargs: Any) -> Schema:
        """
        Infer the schema from the CSV file.

        Args:
            **kwargs: Additional options for pandas.read_csv.

        Returns:
            Inferred Schema.
        """
        read_kwargs = {**self.kwargs, **kwargs}

        # Auto-detect delimiter if not specified
        if "delimiter" not in read_kwargs and "sep" not in read_kwargs:
            read_kwargs["delimiter"] = self._detect_delimiter()

        # Read sample
        df = pd.read_csv(self.path, nrows=1000, **read_kwargs)

        return infer_schema_from_pandas(df, name=self.path.stem)

    def get_metadata(self, **kwargs: Any) -> Mapping[str, Any]:
        """
        Get metadata about the CSV file.

        Args:
            **kwargs: Additional options for pandas.read_csv.

        Returns:
            Metadata mapping.
        """
        stat = self.path.stat()

        read_kwargs = {**self.kwargs, **kwargs}

        # Auto-detect delimiter if not specified
        if "delimiter" not in read_kwargs and "sep" not in read_kwargs:
            read_kwargs["delimiter"] = self._detect_delimiter()

        # Read first row to get column info
        df_sample = pd.read_csv(self.path, nrows=1, **read_kwargs)

        return {
            "size": stat.st_size,
            "modified": stat.st_mtime,
            "format": "csv",
            "compression": detect_compression(self.path),
            "encoding": self.encoding,
            "delimiter": read_kwargs.get("delimiter", ","),
            "columns": len(df_sample.columns),
            "column_names": list(df_sample.columns),
        }

    def _detect_delimiter(self) -> str:
        """Auto-detect the CSV delimiter."""
        try:
            with open(self.path, encoding=self.encoding) as f:
                # Read first line
                sample = f.read(1024)

                # Use csv.Sniffer to detect dialect
                dialect = Sniffer().sniff(sample)

                if hasattr(dialect, "delimiter"):
                    return dialect.delimiter

        except Exception:
            pass

        # Default to comma
        return ","

    def _validate_dataframe(self, df: pd.DataFrame, schema: Schema) -> None:
        """Validate DataFrame against schema."""
        # Check that all required fields are present
        schema_field_names = {f.name for f in schema.fields}
        df_columns = set(df.columns)

        missing_fields = schema_field_names - df_columns
        if missing_fields:
            msg = f"Missing required fields in CSV: {missing_fields}"
            raise ValueError(msg)

        # Type checking could be added here
        # For now, we rely on pandas dtypes

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


class CSVReaderIterator(FileReaderIterator):
    """
    Iterator for reading CSV files in chunks.

    Example:
        >>> reader = CSVReader("large.csv")
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
        self._iterator: Iterator[pd.DataFrame] | None = None

    def __iter__(self) -> "CSVReaderIterator":
        """Initialize the pandas chunk iterator."""
        self._iterator = pd.read_csv(self.path, chunksize=self.chunk_size, **self.kwargs)
        return self

    def __next__(self) -> Sequence[DataRecord]:
        """Get the next chunk of records."""
        if self._iterator is None:
            msg = "Iterator not initialized. Use iter() first."
            raise RuntimeError(msg)

        try:
            df = next(self._iterator)
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
# CSV Writer
# =============================================================================#


class CSVWriter(FileWriter):
    """
    Writer for CSV files.

    Supports schema embedding, compression, and flexible formatting options.

    Example:
        >>> writer = CSVWriter("output.csv")
        >>> count = writer.write(data, delimiter=",")
        >>>
        >>> # With compression
        >>> writer = CSVWriter("output.csv.gz")
        >>> count = writer.write(data, compression="gzip")
    """

    def __init__(
        self,
        path: str | Path,
        encoding: str = "utf-8",
        **kwargs: Any,
    ) -> None:
        """
        Initialize the CSV writer.

        Args:
            path: Path to the output CSV file.
            encoding: File encoding (default: utf-8).
            **kwargs: Additional options passed to pandas.DataFrame.to_csv.
        """
        self.path = Path(path)
        self.encoding = encoding
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
        Write data to a CSV file.

        Args:
            data: Sequence of DataRecord objects to write.
            schema: Optional schema (used for column ordering).
            compression: Compression type ('gzip').
            mode: Write mode ('w' for overwrite, 'a' for append).
            **kwargs: Additional options for pandas.DataFrame.to_csv.

        Returns:
            Number of records written.

        Raises:
            ValueError: If data is empty or invalid.
            IOError: If the file cannot be written.
        """
        if not data:
            msg = "Cannot write empty data to CSV"
            raise ValueError(msg)

        # Combine kwargs
        write_kwargs = {**self.kwargs, **kwargs}

        # Convert records to DataFrame
        df = self._records_to_dataframe(data, schema)

        # Set encoding
        write_kwargs["encoding"] = self.encoding

        # Set compression
        if compression:
            write_kwargs["compression"] = compression

        # Write mode
        if mode == "a":
            write_kwargs["mode"] = "a"
            write_kwargs["header"] = False  # Don't write header on append
        else:
            write_kwargs["mode"] = "w"
            write_kwargs["header"] = True

        # Map 'delimiter' to 'sep' for pandas to_csv
        if "delimiter" in write_kwargs:
            write_kwargs["sep"] = write_kwargs.pop("delimiter")

        # Write to CSV
        df.to_csv(self.path, index=False, **write_kwargs)

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
        Write data to partitioned CSV files.

        Creates a directory structure with files partitioned by the specified columns.

        Args:
            data: Sequence of DataRecord objects to write.
            partition_cols: Columns to partition by.
            schema: Optional schema.
            compression: Compression type.
            **kwargs: Additional options.

        Returns:
            Sequence of paths to the written files.
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

        # Group by partition columns and write
        written_paths: list[str] = []

        for keys, group_df in df.groupby(list(partition_cols), dropna=False):
            if not isinstance(keys, tuple):
                keys = (keys,)

            # Create partition directory path
            partition_parts = []
            for col, val in zip(partition_cols, keys, strict=False):
                val_str = str(val).replace("/", "_")  # Sanitize
                partition_parts.append(f"{col}={val_str}")

            partition_dir = base_path / "/".join(partition_parts)
            partition_dir.mkdir(parents=True, exist_ok=True)

            # Write partition file
            partition_path = partition_dir / "data.csv"
            partition_writer = CSVWriter(partition_path, encoding=self.encoding)
            partition_writer.write(
                [
                    self._row_to_record(row, schema or infer_schema_from_pandas(group_df))
                    for _, row in group_df.iterrows()
                ],
                schema,
                compression,
                **kwargs,
            )
            written_paths.append(str(partition_path))

        return written_paths

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
        df = pd.DataFrame(data, columns=columns if set(columns) == set(data[0].keys()) else None)

        return df

    def _row_to_record(self, row: pd.Series, schema: Schema) -> DataRecord:
        """Convert a pandas Series row to a DataRecord."""
        data = {col: (None if pd.isna(val) else val) for col, val in row.items()}
        return DataRecord(data=data, schema=schema)
