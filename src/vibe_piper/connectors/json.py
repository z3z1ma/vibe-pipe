"""
JSON file reader and writer.

Provides JSON I/O with support for:
- Standard JSON and NDJSON (newline-delimited JSON)
- Schema validation
- Compression (gzip)
- Pretty printing
"""

import json
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any

import pandas as pd

from vibe_piper.connectors.base import FileReader, FileWriter
from vibe_piper.connectors.utils.compression import detect_compression
from vibe_piper.connectors.utils.inference import infer_schema_from_pandas
from vibe_piper.types import DataRecord, Schema

# =============================================================================
# JSON Reader
# =============================================================================#


class JSONReader(FileReader):
    """
    Reader for JSON files.

    Supports both standard JSON arrays and newline-delimited JSON (NDJSON).

    Example:
        >>> reader = JSONReader("data.json")
        >>> data = reader.read()
        >>>
        >>> # Read newline-delimited JSON
        >>> reader = JSONReader("data.jsonl")
        >>> data = reader.read(lines=True)
        >>>
        >>> # With schema validation
        >>> reader = JSONReader("data.json")
        >>> data = reader.read(schema=my_schema)
    """

    def __init__(
        self,
        path: str | Path,
        encoding: str = "utf-8",
        **kwargs: Any,
    ) -> None:
        """
        Initialize the JSON reader.

        Args:
            path: Path to the JSON file.
            encoding: File encoding (default: utf-8).
            **kwargs: Additional options passed to pandas.read_json.
        """
        self.path = Path(path)
        self.encoding = encoding
        self.kwargs = kwargs

    def read(
        self,
        schema: Schema | None = None,
        chunk_size: int | None = None,
        lines: bool | None = None,
        **kwargs: Any,
    ) -> Sequence[DataRecord]:
        """
        Read data from the JSON file.

        Args:
            schema: Optional schema to validate against.
            chunk_size: Not supported for JSON (must read entire file).
            lines: If True, reads as newline-delimited JSON (NDJSON).
                   If None, auto-detect from file extension.
            **kwargs: Additional options for pandas.read_json.

        Returns:
            Sequence of DataRecord objects.

        Raises:
            FileNotFoundError: If the file doesn't exist.
            ValueError: If the JSON format is invalid.
        """
        if chunk_size:
            msg = "Chunked reading not supported for JSON files. Use NDJSON format instead."
            raise ValueError(msg)

        # Combine kwargs
        read_kwargs = {**self.kwargs, **kwargs}

        # Auto-detect lines format
        if lines is None:
            lines = self._is_ndjson_file()

        # Handle compression
        compression = detect_compression(self.path)
        if compression:
            read_kwargs["compression"] = compression

        # Read the data
        read_kwargs["lines"] = lines
        df = pd.read_json(self.path, **read_kwargs)

        # Validate against schema if provided
        if schema:
            self._validate_dataframe(df, schema)

        # Convert to DataRecord objects
        return self._dataframe_to_records(df, schema)

    def infer_schema(self, **kwargs: Any) -> Schema:
        """
        Infer the schema from the JSON file.

        Args:
            **kwargs: Additional options for pandas.read_json.

        Returns:
            Inferred Schema.
        """
        read_kwargs = {**self.kwargs, **kwargs}

        # Auto-detect lines format
        if "lines" not in read_kwargs:
            read_kwargs["lines"] = self._is_ndjson_file()

        # Read sample
        df = pd.read_json(self.path, **read_kwargs)

        return infer_schema_from_pandas(df, name=self.path.stem)

    def get_metadata(self) -> Mapping[str, Any]:
        """
        Get metadata about the JSON file.

        Returns:
            Metadata mapping.
        """
        stat = self.path.stat()

        # Try to read first record to get structure
        try:
            with open(self.path, encoding=self.encoding) as f:
                if self._is_ndjson_file():
                    first_line = f.readline()
                    first_record = json.loads(first_line)
                else:
                    data = json.load(f)
                    first_record = (
                        data[0] if isinstance(data, list) and len(data) > 0 else data
                    )

            if isinstance(first_record, dict):
                fields_count = len(first_record.keys())
                field_names = list(first_record.keys())
            else:
                fields_count = 0
                field_names = []

        except Exception:
            fields_count = 0
            field_names = []

        return {
            "size": stat.st_size,
            "modified": stat.st_mtime,
            "format": "ndjson" if self._is_ndjson_file() else "json",
            "compression": detect_compression(self.path),
            "encoding": self.encoding,
            "fields": fields_count,
            "field_names": field_names,
        }

    def _is_ndjson_file(self) -> bool:
        """Check if the file is newline-delimited JSON."""
        extension = self.path.suffix.lower()
        return extension in (".jsonl", ".ndjson")

    def _validate_dataframe(self, df: pd.DataFrame, schema: Schema) -> None:
        """Validate DataFrame against schema."""
        # Check that all required fields are present
        schema_field_names = {f.name for f in schema.fields}
        df_columns = set(df.columns)

        missing_fields = schema_field_names - df_columns
        if missing_fields:
            msg = f"Missing required fields in JSON: {missing_fields}"
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


# =============================================================================
# JSON Writer
# =============================================================================#


class JSONWriter(FileWriter):
    """
    Writer for JSON files.

    Supports both standard JSON and newline-delimited JSON (NDJSON).

    Example:
        >>> writer = JSONWriter("output.json")
        >>> count = writer.write(data)
        >>>
        >>> # Write as newline-delimited JSON
        >>> writer = JSONWriter("output.jsonl")
        >>> count = writer.write(data, lines=True)
        >>>
        >>> # With pretty printing
        >>> writer = JSONWriter("output.json")
        >>> count = writer.write(data, indent=2)
    """

    def __init__(
        self,
        path: str | Path,
        encoding: str = "utf-8",
        **kwargs: Any,
    ) -> None:
        """
        Initialize the JSON writer.

        Args:
            path: Path to the output JSON file.
            encoding: File encoding (default: utf-8).
            **kwargs: Additional options passed to pandas.DataFrame.to_json.
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
        Write data to a JSON file.

        Args:
            data: Sequence of DataRecord objects to write.
            schema: Optional schema (for validation).
            compression: Compression type ('gzip').
            mode: Write mode ('w' for overwrite, 'a' for append).
            **kwargs: Additional options for pandas.DataFrame.to_json.

        Returns:
            Number of records written.

        Raises:
            ValueError: If data is empty or invalid.
            IOError: If the file cannot be written.
        """
        if not data:
            msg = "Cannot write empty data to JSON"
            raise ValueError(msg)

        # Combine kwargs
        write_kwargs = {**self.kwargs, **kwargs}

        # Auto-detect lines format from file extension
        if "lines" not in write_kwargs:
            extension = self.path.suffix.lower()
            write_kwargs["lines"] = extension in (".jsonl", ".ndjson")

        # Convert records to DataFrame
        df = self._records_to_dataframe(data, schema)

        # Set orient to 'records' for array of objects format
        write_kwargs["orient"] = write_kwargs.get("orient", "records")

        # Set compression
        if compression:
            write_kwargs["compression"] = compression

        # Write mode
        if mode == "a" and write_kwargs.get("lines", False):
            # Append mode for NDJSON
            with open(self.path, mode="a", encoding=self.encoding) as f:
                for record in data:
                    json_line = json.dumps(record.data)
                    f.write(json_line + "\n")
            return len(data)
        elif mode == "a":
            msg = "Append mode only supported for NDJSON (lines=True)"
            raise ValueError(msg)

        # Write to JSON
        df.to_json(self.path, **write_kwargs)

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
        Write data to partitioned JSON files.

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
            extension = (
                ".jsonl"
                if self.path.suffix.lower() in (".jsonl", ".ndjson")
                else ".json"
            )
            partition_path = partition_dir / f"data{extension}"

            partition_writer = JSONWriter(partition_path, encoding=self.encoding)
            partition_writer.write(
                [
                    self._row_to_record(
                        row, schema or infer_schema_from_pandas(group_df)
                    )
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
        df = pd.DataFrame(
            data, columns=columns if set(columns) == set(data[0].keys()) else None
        )

        return df

    def _row_to_record(self, row: pd.Series, schema: Schema) -> DataRecord:
        """Convert a pandas Series row to a DataRecord."""
        data = {col: (None if pd.isna(val) else val) for col, val in row.items()}
        return DataRecord(data=data, schema=schema)
