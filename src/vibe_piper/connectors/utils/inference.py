"""
Schema inference from files.

This module provides utilities to automatically infer VibePiper schemas
from file structures and data samples.
"""

from collections.abc import Sequence
from pathlib import Path
from typing import Any

import pandas as pd

from vibe_piper.connectors.utils.type_mapping import map_type_to_vibepiper
from vibe_piper.types import DataType, Schema, SchemaField

# =============================================================================
# Schema Inference
# =============================================================================#


def infer_schema_from_file(
    path: str | Path,
    format: str | None = None,
    sample_rows: int = 1000,
    **kwargs: Any,
) -> Schema:
    """
    Infer a VibePiper schema from a data file.

    Analyzes the file structure and data samples to create an appropriate
    schema with field names, types, and nullability information.

    Args:
        path: Path to the file.
        format: File format ('csv', 'json', 'parquet', 'excel').
               If None, auto-detect from file extension.
        sample_rows: Number of rows to sample for type inference.
        **kwargs: Additional format-specific options passed to pandas.

    Returns:
        Inferred Schema object.

    Raises:
        FileNotFoundError: If the file doesn't exist.
        ValueError: If the file format is not supported or cannot be parsed.

    Examples:
        >>> schema = infer_schema_from_file("data.csv")
        >>> schema.name
        'data'
        >>> schema.fields[0].name
        'id'
        >>> schema.fields[0].data_type
        <DataType.INTEGER: 2>
    """
    path = Path(path)

    if not path.exists():
        msg = f"File not found: {path}"
        raise FileNotFoundError(msg)

    # Auto-detect format if not specified
    if format is None:
        format = _detect_format_from_path(path)

    # Read sample data based on format
    df = _read_sample_data(path, format, sample_rows, **kwargs)

    # Infer schema from DataFrame
    schema = _infer_schema_from_dataframe(df, path.stem)

    return schema


def infer_schema_from_pandas(
    df: pd.DataFrame,
    name: str = "inferred_schema",
) -> Schema:
    """
    Infer a VibePiper schema from a pandas DataFrame.

    Args:
        df: The DataFrame to analyze.
        name: Name for the schema.

    Returns:
        Inferred Schema object.

    Examples:
        >>> import pandas as pd
        >>> df = pd.DataFrame({"id": [1, 2, 3], "name": ["Alice", "Bob", "Charlie"]})
        >>> schema = infer_schema_from_pandas(df)
    """
    return _infer_schema_from_dataframe(df, name)


def infer_schema_from_data(
    data: Sequence[dict[str, Any]],
    name: str = "inferred_schema",
) -> Schema:
    """
    Infer a VibePiper schema from a sequence of data records.

    Args:
        data: Sequence of dictionaries representing records.
        name: Name for the schema.

    Returns:
        Inferred Schema object.

    Examples:
        >>> data = [{"id": 1, "name": "Alice"}, {"id": 2, "name": "Bob"}]
        >>> schema = infer_schema_from_data(data)
    """
    if not data:
        # Empty data - return empty schema
        return Schema(name=name, fields=tuple())

    df = pd.DataFrame(data)
    return _infer_schema_from_dataframe(df, name)


# =============================================================================
# Internal Helpers
# =============================================================================#


def _detect_format_from_path(path: Path) -> str:
    """Detect file format from path extension."""
    extension = path.suffix.lower()

    mapping: dict[str, str] = {
        ".csv": "csv",
        ".tsv": "csv",
        ".txt": "csv",  # Assume CSV for txt
        ".json": "json",
        ".jsonl": "json",
        ".ndjson": "json",
        ".parquet": "parquet",
        ".xls": "excel",
        ".xlsx": "excel",
        ".xlsb": "excel",
    }

    if extension not in mapping:
        msg = f"Cannot detect format from extension: {extension}. Supported formats: {list(mapping.keys())}"
        raise ValueError(msg)

    return mapping[extension]


def _read_sample_data(
    path: Path,
    format: str,
    sample_rows: int,
    **kwargs: Any,
) -> pd.DataFrame:
    """Read a sample of data from the file."""
    try:
        if format == "csv":
            # Auto-detect delimiter
            delimiter = _detect_csv_delimiter(path, **kwargs)
            return pd.read_csv(
                path,
                nrows=sample_rows,
                delimiter=delimiter,
                **kwargs,
            )

        if format == "json":
            # Try reading as JSON first, then as NDJSON
            try:
                return pd.read_json(path, lines=False, **kwargs)
            except ValueError:
                # Try newline-delimited JSON
                return pd.read_json(path, lines=True, nrows=sample_rows, **kwargs)

        if format == "parquet":
            return pd.read_parquet(path, **kwargs)

        if format == "excel":
            return pd.read_excel(path, nrows=sample_rows, **kwargs)

        msg = f"Unsupported format: {format!r}"
        raise ValueError(msg)

    except Exception as e:
        msg = f"Failed to read file {path} with format {format!r}: {e}"
        raise ValueError(msg) from e


def _detect_csv_delimiter(path: Path, **kwargs: Any) -> str:
    """Auto-detect CSV delimiter."""
    if "delimiter" in kwargs or "sep" in kwargs:
        # User-specified delimiter
        return kwargs.get("delimiter", kwargs.get("sep", ","))

    # Try to detect delimiter
    try:
        with open(path, encoding="utf-8") as f:
            first_line = f.readline()

        # Count common delimiters
        delimiters = [",", ";", "\t", "|"]
        delimiter_counts = {d: first_line.count(d) for d in delimiters}

        # Choose the delimiter with the most occurrences
        detected = max(delimiter_counts, key=delimiter_counts.get)

        # If no delimiter found, default to comma
        if delimiter_counts[detected] == 0:
            return ","

        return detected

    except Exception:
        # Default to comma on error
        return ","


def _infer_schema_from_dataframe(df: pd.DataFrame, name: str) -> Schema:
    """Infer a VibePiper schema from a pandas DataFrame."""
    fields: list[SchemaField] = []

    for column in df.columns:
        dtype = df[column].dtype

        # Map pandas dtype to VibePiper DataType
        data_type = map_type_to_vibepiper(dtype, format="pandas")

        # Determine nullability (convert numpy bool to Python bool)
        nullable = bool(df[column].isna().any())
        required = not nullable

        # Infer constraints from data
        constraints = _infer_constraints(df[column], data_type)

        # Create schema field
        field = SchemaField(
            name=str(column),
            data_type=data_type,
            required=required,
            nullable=nullable,
            constraints=constraints,
        )
        fields.append(field)

    return Schema(name=name, fields=tuple(fields))


def _infer_constraints(series: pd.Series, data_type: DataType) -> dict[str, Any]:
    """Infer constraints from a pandas Series."""
    constraints: dict[str, Any] = {}

    if data_type == DataType.STRING:
        # Max length
        if series.hasnans:
            non_null_series = series.dropna()
        else:
            non_null_series = series

        if len(non_null_series) > 0:
            max_len = non_null_series.astype(str).str.len().max()
            if not pd.isna(max_len):
                constraints["max_length"] = int(max_len)

    elif data_type == DataType.INTEGER:
        # Min/max values
        min_val = series.min()
        max_val = series.max()
        if not pd.isna(min_val):
            constraints["min_value"] = int(min_val)
        if not pd.isna(max_val):
            constraints["max_value"] = int(max_val)

    elif data_type == DataType.FLOAT:
        # Min/max values
        min_val = series.min()
        max_val = series.max()
        if not pd.isna(min_val):
            constraints["min_value"] = float(min_val)
        if not pd.isna(max_val):
            constraints["max_value"] = float(max_val)

    return constraints


def refine_schema_with_sample(
    schema: Schema,
    data: Sequence[dict[str, Any]],
    threshold: float = 0.95,
) -> Schema:
    """
    Refine a schema based on actual data samples.

    Updates field types and nullability based on observed data patterns.
    Useful for adjusting initially inferred schemas with more data.

    Args:
        schema: Initial schema to refine.
        data: Sample data to analyze.
        threshold: Confidence threshold for type changes (0.0-1.0).

    Returns:
        Refined Schema.

    Examples:
        >>> schema = refine_schema_with_sample(initial_schema, more_data)
    """
    if not data:
        return schema

    df = pd.DataFrame(data)
    refined_fields: list[SchemaField] = []

    for field in schema.fields:
        if field.name not in df.columns:
            # Keep original field if not in sample
            refined_fields.append(field)
            continue

        series = df[field.name]

        # Refine nullability (convert numpy bool to Python bool)
        null_ratio = series.isna().sum() / len(series)
        nullable = bool(null_ratio > (1.0 - threshold))
        required = not nullable

        # Refine type if needed
        # (This is a simplified version - could be more sophisticated)
        inferred_type = map_type_to_vibepiper(series.dtype, format="pandas")

        refined_fields.append(
            SchemaField(
                name=field.name,
                data_type=inferred_type,
                required=required,
                nullable=nullable,
                description=field.description,
                constraints=field.constraints,
            )
        )

    return Schema(
        name=schema.name, fields=tuple(refined_fields), description=schema.description
    )
