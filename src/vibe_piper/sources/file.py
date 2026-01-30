"""
File Source Implementation

Provides declarative file source with:
- Support for multiple formats (CSV, JSON, JSONL, Parquet)
- Glob pattern support for multiple files
- Auto-schema inference from file headers
"""

import logging
from collections.abc import AsyncIterator, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Literal

import pandas as pd

from vibe_piper.sources.base import Source
from vibe_piper.types import DataRecord, DataType, PipelineContext, Schema, SchemaField

# =============================================================================
# Configuration Classes
# =============================================================================


@dataclass
class FileConfig:
    """Complete configuration for file source."""

    name: str
    """Source name"""

    path: str | Path
    """Path to file or glob pattern"""

    format: Literal["csv", "json", "jsonl", "parquet"] = "csv"
    """File format"""

    pattern: str | None = None
    """Glob pattern for multiple files (e.g., 'data_*.csv')"""

    encoding: str = "utf-8"
    """File encoding (CSV only)"""

    delimiter: str | None = None
    """CSV delimiter (default: auto-detect)"""

    schema: Schema | None = None
    """Optional explicit schema (inferred if None)"""


# =============================================================================
# File Source Implementation
# =============================================================================


class FileSource(Source[DataRecord]):
    """
    File source with auto-detection and schema inference.

    Provides a declarative interface for fetching data from files
    with automatic format detection, schema inference, and glob support.

    Example:
        Single file::

            source = FileSource(
                FileConfig(
                    name="users",
                    path="data/users.csv",
                    format="csv",
                )
            )

            data = await source.fetch(context)

        Multiple files (glob)::

            source = FileSource(
                FileConfig(
                    name="users",
                    path="data/",
                    pattern="users_*.csv",
                )
            )

            data = await source.fetch(context)

        JSON file::

            source = FileSource(
                FileConfig(
                    name="users",
                    path="data/users.json",
                    format="json",
                )
            )
    """

    def __init__(self, config: FileConfig) -> None:
        """
        Initialize file source.

        Args:
            config: File source configuration
        """
        self.config = config
        self._logger = logging.getLogger(self.__class__.__name__)

    def _get_files(self) -> list[Path]:
        """Get list of files to read."""
        path = Path(self.config.path)

        # If pattern specified, use glob
        if self.config.pattern:
            return sorted(path.glob(self.config.pattern))

        # Single file
        if path.is_file():
            return [path]

        # Directory - get all matching files
        pattern = f"*.{self.config.format}"
        return sorted(path.glob(pattern))

    async def fetch(self, context: PipelineContext) -> Sequence[DataRecord]:
        """
        Fetch all data from file source.

        Handles multiple files, schema inference, and format detection.

        Args:
            context: Pipeline execution context

        Returns:
            Sequence of DataRecord objects

        Raises:
            Exception: If fetch fails
        """
        files = self._get_files()

        if not files:
            self._logger.warning("No files found for source: %s", self.config.name)
            return []

        # Collect all records from all files
        all_records: list[DataRecord] = []
        for file_path in files:
            records = await self._read_file(file_path)
            all_records.extend(records)

        return all_records

    async def _read_file(self, file_path: Path) -> list[DataRecord]:
        """Read records from a single file."""
        format_type = self._detect_format(file_path)

        if format_type == "csv":
            records = await self._read_csv(file_path)
        elif format_type == "json":
            records = await self._read_json(file_path)
        elif format_type == "jsonl":
            records = await self._read_jsonl(file_path)
        elif format_type == "parquet":
            records = await self._read_parquet(file_path)
        else:
            msg = f"Unsupported format: {format_type}"
            raise ValueError(msg)

        return records

    async def _read_csv(self, file_path: Path) -> list[DataRecord]:
        """Read CSV file."""
        read_kwargs: dict[str, Any] = {
            "encoding": self.config.encoding,
        }

        # Auto-detect delimiter if not specified
        if self.config.delimiter:
            read_kwargs["sep"] = self.config.delimiter

        # Read data
        df = pd.read_csv(file_path, **read_kwargs)

        # Convert to DataRecords
        schema = self.config.schema or self._infer_schema_from_df(df, file_path.stem)
        return self._df_to_records(df, schema)

    async def _read_json(self, file_path: Path) -> list[DataRecord]:
        """Read JSON file."""
        df = pd.read_json(file_path)

        # Convert to DataRecords
        schema = self.config.schema or self._infer_schema_from_df(df, file_path.stem)
        return self._df_to_records(df, schema)

    async def _read_jsonl(self, file_path: Path) -> list[DataRecord]:
        """Read JSONL file."""
        df = pd.read_json(file_path, lines=True)

        # Convert to DataRecords
        schema = self.config.schema or self._infer_schema_from_df(df, file_path.stem)
        return self._df_to_records(df, schema)

    async def _read_parquet(self, file_path: Path) -> list[DataRecord]:
        """Read Parquet file."""
        df = pd.read_parquet(file_path)

        # Convert to DataRecords
        schema = self.config.schema or self._infer_schema_from_df(df, file_path.stem)
        return self._df_to_records(df, schema)

    def _detect_format(self, file_path: Path) -> str:
        """Detect file format from extension."""
        # Use config format if specified
        if self.config.format != "csv":
            return self.config.format

        # Auto-detect from extension
        suffix = file_path.suffix.lower()

        if suffix in (".csv", ".tsv"):
            return "csv"
        elif suffix == ".json":
            return "json"
        elif suffix == ".jsonl":
            return "jsonl"
        elif suffix == ".parquet":
            return "parquet"

        msg = f"Cannot detect format for file: {file_path}"
        raise ValueError(msg)

    async def stream(self, context: PipelineContext) -> AsyncIterator[DataRecord]:
        """
        Stream data from file source.

        Useful for large files where loading all data at once is impractical.

        Args:
            context: Pipeline execution context

        Yields:
            Individual DataRecord objects
        """
        files = self._get_files()

        if not files:
            self._logger.warning("No files found for source: %s", self.config.name)
            return

        for file_path in files:
            format_type = self._detect_format(file_path)

            if format_type == "csv":
                reader = pd.read_csv(file_path, chunksize=1000, encoding=self.config.encoding)
            elif format_type in ("json", "jsonl"):
                reader = pd.read_json(file_path, lines=True, chunksize=1000)
            elif format_type == "parquet":
                reader = pd.read_parquet(file_path, chunksize=1000)
            else:
                msg = f"Unsupported format: {format_type}"
                raise ValueError(msg)

            schema = self.config.schema or None

            for chunk_df in reader:
                # Infer schema if not provided
                current_schema = schema or self._infer_schema_from_df(chunk_df, file_path.stem)

                for _, row in chunk_df.iterrows():
                    data = {col: (None if pd.isna(val) else val) for col, val in row.items()}
                    yield DataRecord(data=data, schema=current_schema)

    def _infer_schema_from_df(self, df: pd.DataFrame, default_name: str) -> Schema:
        """Infer schema from DataFrame."""

        def infer_type(value: Any) -> DataType:
            """Infer DataType from value."""
            if isinstance(value, bool):
                return DataType.BOOLEAN
            elif isinstance(value, int):
                return DataType.INTEGER
            elif isinstance(value, float):
                return DataType.FLOAT
            elif isinstance(value, str):
                # Try to parse as datetime
                try:
                    from datetime import datetime

                    datetime.fromisoformat(value)
                    return DataType.DATETIME
                except Exception:
                    return DataType.STRING
            elif isinstance(value, list):
                return DataType.ARRAY
            elif isinstance(value, dict):
                return DataType.OBJECT
            else:
                return DataType.ANY

        # Create fields from first row
        if len(df) == 0:
            return Schema(name=default_name)

        sample_row = df.iloc[0]
        fields = [
            SchemaField(
                name=str(col),
                data_type=infer_type(val),
                required=True,
                nullable=pd.isna(val),
            )
            for col, val in sample_row.items()
        ]

        return Schema(name=default_name, fields=tuple(fields))

    def _df_to_records(self, df: pd.DataFrame, schema: Schema) -> list[DataRecord]:
        """Convert DataFrame to DataRecord objects."""
        records: list[DataRecord] = []

        for _, row in df.iterrows():
            data = {col: (None if pd.isna(val) else val) for col, val in row.items()}
            records.append(DataRecord(data=data, schema=schema))

        return records

    def infer_schema(self) -> Schema:
        """
        Infer schema from file(s).

        Returns:
            Inferred Schema
        """
        files = self._get_files()

        if not files:
            return Schema(name=self.config.name)

        # Read first file to infer schema
        file_path = files[0]
        try:
            if file_path.suffix == ".csv":
                df = pd.read_csv(file_path, nrows=1000)
            elif file_path.suffix == ".json":
                df = pd.read_json(file_path, nrows=1000)
            elif file_path.suffix == ".jsonl":
                df = pd.read_json(file_path, lines=True, nrows=1000)
            elif file_path.suffix == ".parquet":
                df = pd.read_parquet(file_path, nrows=1000)
            else:
                return Schema(name=self.config.name)
        except Exception:
            return Schema(name=self.config.name)

        if len(df) == 0:
            return Schema(name=self.config.name)

        return self._infer_schema_from_df(df, file_path.stem)

    def get_metadata(self) -> dict[str, Any]:
        """
        Get metadata about file source.

        Returns:
            Dictionary of metadata
        """
        files = self._get_files()
        return {
            "source_type": "file",
            "name": self.config.name,
            "format": self.config.format,
            "path": str(self.config.path),
            "pattern": self.config.pattern,
            "files": len(files),
        }
