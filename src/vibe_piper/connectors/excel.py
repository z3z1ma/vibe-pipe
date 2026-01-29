"""
Excel file reader and writer.

Provides Excel I/O with support for:
- Multiple sheets
- Schema inference and validation
- Formatting preservation (basic)
"""

from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any

import pandas as pd

from vibe_piper.connectors.base import FileReader, FileWriter
from vibe_piper.connectors.utils.inference import infer_schema_from_pandas
from vibe_piper.types import DataRecord, Schema

# =============================================================================
# Excel Reader
# =============================================================================#


class ExcelReader(FileReader):
    """
    Reader for Excel files.

    Supports both .xls and .xlsx formats with multi-sheet reading.

    Example:
        >>> reader = ExcelReader("data.xlsx")
        >>> data = reader.read()
        >>>
        >>> # Read specific sheet
        >>> reader = ExcelReader("data.xlsx")
        >>> data = reader.read(sheet_name="Sheet2")
        >>>
        >>> # Read multiple sheets
        >>> reader = ExcelReader("data.xlsx")
        >>> sheets = reader.read_all_sheets()
        >>> for sheet_name, data in sheets.items():
        ...     print(f"{sheet_name}: {len(data)} rows")
    """

    def __init__(
        self,
        path: str | Path,
        engine: str | None = None,
        **kwargs: Any,
    ) -> None:
        """
        Initialize the Excel reader.

        Args:
            path: Path to the Excel file.
            engine: Excel engine ('openpyxl', 'xlrd', or None for auto).
            **kwargs: Additional options passed to pandas.read_excel.
        """
        self.path = Path(path)
        self.engine = engine
        self.kwargs = kwargs

    def read(
        self,
        schema: Schema | None = None,
        chunk_size: int | None = None,
        sheet_name: str | int | None = None,
        **kwargs: Any,
    ) -> Sequence[DataRecord]:
        """
        Read data from an Excel sheet.

        Args:
            schema: Optional schema to validate against.
            chunk_size: Not supported for Excel (must read entire sheet).
            sheet_name: Name or index of the sheet to read.
                       If None, reads the first sheet.
            **kwargs: Additional options for pandas.read_excel.

        Returns:
            Sequence of DataRecord objects.

        Raises:
            FileNotFoundError: If the file doesn't exist.
            ValueError: If the sheet doesn't exist or format is invalid.
        """
        if chunk_size:
            msg = "Chunked reading not supported for Excel files."
            raise ValueError(msg)

        # Combine kwargs
        read_kwargs = {**self.kwargs, **kwargs}

        # Set engine
        if self.engine:
            read_kwargs["engine"] = self.engine

        # Set sheet name
        if sheet_name is not None:
            read_kwargs["sheet_name"] = sheet_name

        # Read the data
        df = pd.read_excel(self.path, **read_kwargs)

        # Validate against schema if provided
        if schema:
            self._validate_dataframe(df, schema)

        # Convert to DataRecord objects
        return self._dataframe_to_records(df, schema)

    def read_all_sheets(
        self,
        schema: Schema | None = None,
        **kwargs: Any,
    ) -> dict[str, Sequence[DataRecord]]:
        """
        Read all sheets from the Excel file.

        Args:
            schema: Optional schema to validate against (applied to all sheets).
            **kwargs: Additional options for pandas.read_excel.

        Returns:
            Dictionary mapping sheet names to DataRecord sequences.

        Example:
            >>> reader = ExcelReader("data.xlsx")
            >>> sheets = reader.read_all_sheets()
            >>> for sheet_name, data in sheets.items():
            ...     print(f"{sheet_name}: {len(data)} rows")
        """
        # Combine kwargs
        read_kwargs = {**self.kwargs, **kwargs}

        # Set engine
        if self.engine:
            read_kwargs["engine"] = self.engine

        # Read all sheets
        all_sheets = pd.read_excel(self.path, sheet_name=None, **read_kwargs)

        # Convert each sheet to records
        result: dict[str, Sequence[DataRecord]] = {}

        for sheet_name, df in all_sheets.items():
            # Validate against schema if provided
            if schema:
                self._validate_dataframe(df, schema)

            # Convert to DataRecord objects
            records = self._dataframe_to_records(df, schema)
            result[str(sheet_name)] = records

        return result

    def get_sheet_names(self) -> list[str]:
        """
        Get a list of all sheet names in the Excel file.

        Returns:
            List of sheet names.

        Example:
            >>> reader = ExcelReader("data.xlsx")
            >>> reader.get_sheet_names()
            ['Sheet1', 'Sheet2', 'Data']
        """
        try:
            xl_file = pd.ExcelFile(self.path, engine=self.engine)
            return xl_file.sheet_names
        except Exception as e:
            msg = f"Failed to read sheet names from {self.path}: {e}"
            raise ValueError(msg) from e

    def infer_schema(self, **kwargs: Any) -> Schema:
        """
        Infer the schema from the Excel file.

        Infers schema from the first sheet.

        Args:
            **kwargs: Additional options for pandas.read_excel.

        Returns:
            Inferred Schema.
        """
        read_kwargs = {**self.kwargs, **kwargs}

        # Set engine
        if self.engine:
            read_kwargs["engine"] = self.engine

        # Read sample
        df = pd.read_excel(self.path, nrows=1000, **read_kwargs)

        return infer_schema_from_pandas(df, name=self.path.stem)

    def infer_schema_all_sheets(self, **kwargs: Any) -> dict[str, Schema]:
        """
        Infer schemas for all sheets in the Excel file.

        Args:
            **kwargs: Additional options for pandas.read_excel.

        Returns:
            Dictionary mapping sheet names to inferred Schemas.

        Example:
            >>> reader = ExcelReader("data.xlsx")
            >>> schemas = reader.infer_schema_all_sheets()
            >>> for sheet_name, schema in schemas.items():
            ...     print(f"{sheet_name}: {len(schema.fields)} fields")
        """
        read_kwargs = {**self.kwargs, **kwargs}

        # Set engine
        if self.engine:
            read_kwargs["engine"] = self.engine

        # Read all sheets (sample only)
        all_sheets = pd.read_excel(self.path, sheet_name=None, nrows=1000, **read_kwargs)

        # Infer schema for each sheet
        result: dict[str, Schema] = {}

        for sheet_name, df in all_sheets.items():
            schema = infer_schema_from_pandas(df, name=str(sheet_name))
            result[str(sheet_name)] = schema

        return result

    def get_metadata(self, **kwargs: Any) -> Mapping[str, Any]:
        """
        Get metadata about the Excel file.

        Args:
            **kwargs: Additional options.

        Returns:
            Metadata mapping.
        """
        stat = self.path.stat()

        try:
            xl_file = pd.ExcelFile(self.path, engine=self.engine)
            sheet_names = xl_file.sheet_names

            # Read first row of each sheet to get column info
            sheet_info: dict[str, dict[str, Any]] = {}
            for sheet_name in sheet_names:
                df_sample = pd.read_excel(xl_file, sheet_name=sheet_name, nrows=1, **kwargs)
                sheet_info[sheet_name] = {
                    "columns": len(df_sample.columns),
                    "column_names": list(df_sample.columns),
                }

            return {
                "size": stat.st_size,
                "modified": stat.st_mtime,
                "format": "excel",
                "sheets": len(sheet_names),
                "sheet_names": sheet_names,
                "sheet_info": sheet_info,
            }

        except Exception:
            # Fallback to basic info
            return {
                "size": stat.st_size,
                "modified": stat.st_mtime,
                "format": "excel",
            }

    def _validate_dataframe(self, df: pd.DataFrame, schema: Schema) -> None:
        """Validate DataFrame against schema."""
        # Check that all required fields are present
        schema_field_names = {f.name for f in schema.fields}
        df_columns = set(df.columns)

        missing_fields = schema_field_names - df_columns
        if missing_fields:
            msg = f"Missing required fields in Excel: {missing_fields}"
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
# Excel Writer
# =============================================================================#


class ExcelWriter(FileWriter):
    """
    Writer for Excel files.

    Supports multi-sheet workbooks with flexible formatting options.

    Example:
        >>> writer = ExcelWriter("output.xlsx")
        >>> count = writer.write(data, sheet_name="Sheet1")
        >>>
        >>> # Write multiple sheets
        >>> writer = ExcelWriter("output.xlsx")
        >>> writer.write(data1, sheet_name="Sheet1", mode="w")
        >>> writer.write(data2, sheet_name="Sheet2", mode="a")
        >>>
        >>> # Write all sheets at once
        >>> writer = ExcelWriter("output.xlsx")
        >>> paths = writer.write_all_sheets({"Sheet1": data1, "Sheet2": data2})
    """

    def __init__(
        self,
        path: str | Path,
        engine: str = "openpyxl",
        **kwargs: Any,
    ) -> None:
        """
        Initialize the Excel writer.

        Args:
            path: Path to the output Excel file.
            engine: Excel engine ('openpyxl', 'xlsxwriter').
            **kwargs: Additional options passed to pandas.DataFrame.to_excel.
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
        sheet_name: str = "Sheet1",
        **kwargs: Any,
    ) -> int:
        """
        Write data to an Excel sheet.

        Args:
            data: Sequence of DataRecord objects to write.
            schema: Optional schema (for validation and column ordering).
            compression: Not supported for Excel format.
            mode: Write mode ('w' for overwrite, 'a' for append to existing workbook).
            sheet_name: Name of the sheet to write to.
            **kwargs: Additional options for pandas.DataFrame.to_excel.

        Returns:
            Number of records written.

        Raises:
            ValueError: If data is empty or compression is specified.
            IOError: If the file cannot be written.

        Note:
            When mode='a', the sheet is added to the existing workbook.
            If the sheet already exists, it will be overwritten.
        """
        if not data:
            msg = "Cannot write empty data to Excel"
            raise ValueError(msg)

        if compression:
            msg = "Compression not supported for Excel format"
            raise ValueError(msg)

        # Combine kwargs
        write_kwargs = {**self.kwargs, **kwargs}
        write_kwargs.pop("engine", None)

        # Convert records to DataFrame
        df = self._records_to_dataframe(data, schema)

        # Write to Excel
        if mode == "a" and self.path.exists():
            with pd.ExcelWriter(
                self.path,
                mode="a",
                engine=self.engine,
                if_sheet_exists="replace",
            ) as writer:
                df.to_excel(writer, sheet_name=sheet_name, index=False, **write_kwargs)
        else:
            with pd.ExcelWriter(self.path, mode="w", engine=self.engine) as writer:
                df.to_excel(writer, sheet_name=sheet_name, index=False, **write_kwargs)

        return len(data)

    def write_all_sheets(
        self,
        sheets: dict[str, Sequence[DataRecord]],
        schemas: dict[str, Schema] | None = None,
        **kwargs: Any,
    ) -> int:
        """
        Write multiple sheets to an Excel workbook.

        Args:
            sheets: Dictionary mapping sheet names to DataRecord sequences.
            schemas: Optional dictionary mapping sheet names to schemas.
            **kwargs: Additional options for pandas.DataFrame.to_excel.

        Returns:
            Total number of records written across all sheets.

        Example:
            >>> writer = ExcelWriter("output.xlsx")
            >>> data = {
            ...     "Customers": customer_records,
            ...     "Orders": order_records,
            ... }
            >>> total = writer.write_all_sheets(data)
        """
        if not sheets:
            return 0

        # Combine kwargs
        write_kwargs = {**self.kwargs, **kwargs}

        write_kwargs.pop("engine", None)

        # Write all sheets
        with pd.ExcelWriter(self.path, engine=self.engine) as writer:
            total_count = 0

            for sheet_name, records in sheets.items():
                # Get schema for this sheet
                schema = schemas.get(sheet_name) if schemas else None

                # Convert to DataFrame
                df = self._records_to_dataframe(records, schema)

                # Write sheet
                df.to_excel(writer, sheet_name=sheet_name, index=False, **write_kwargs)
                total_count += len(records)

        return total_count

    def write_partitioned(
        self,
        data: Sequence[DataRecord],
        partition_cols: Sequence[str],
        schema: Schema | None = None,
        compression: str | None = None,
        **kwargs: Any,
    ) -> Sequence[str]:
        """
        Write data to partitioned Excel files.

        Creates a directory structure with Excel files partitioned by the specified columns.

        Args:
            data: Sequence of DataRecord objects to write.
            partition_cols: Columns to partition by.
            schema: Optional schema.
            compression: Not supported for Excel.
            **kwargs: Additional options.

        Returns:
            Sequence of paths to the written files.

        Raises:
            ValueError: If partition columns are not found in the data or compression is specified.
        """
        if compression:
            msg = "Compression not supported for Excel format"
            raise ValueError(msg)

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
            partition_path = partition_dir / "data.xlsx"

            partition_writer = ExcelWriter(partition_path, engine=self.engine)
            partition_writer.write(
                [
                    self._row_to_record(row, schema or infer_schema_from_pandas(group_df))
                    for _, row in group_df.iterrows()
                ],
                schema,
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
