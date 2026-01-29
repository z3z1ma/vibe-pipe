"""
Type mapping between file formats and VibePiper types.

This module provides bidirectional type mapping for converting between
file format types (CSV dtypes, JSON types, Parquet types, Excel types)
and VibePiper DataType enum values.
"""

from collections.abc import Mapping
from typing import Any

from vibe_piper.types import DataType

# =============================================================================
# Type Mapping to VibePiper
# =============================================================================


def map_type_to_vibepiper(file_type: Any, format: str = "auto") -> DataType:
    """
    Map a file format type to a VibePiper DataType.

    Args:
        file_type: The type from the file format (str, pandas dtype, etc.)
        format: The file format ('csv', 'json', 'parquet', 'excel', 'auto')

    Returns:
        The corresponding VibePiper DataType.

    Raises:
        ValueError: If the type cannot be mapped.

    Examples:
        >>> map_type_to_vibepiper("int64")
        <DataType.INTEGER: 2>
        >>> map_type_to_vibepiper("string")
        <DataType.STRING: 1>
        >>> map_type_to_vibepiper(True, format="json")
        <DataType.BOOLEAN: 4>
    """
    # Handle pandas dtypes
    if hasattr(file_type, "name"):
        file_type_str = str(file_type.name)
    elif isinstance(file_type, type):
        file_type_str = file_type.__name__
    else:
        file_type_str = str(file_type).lower()

    # Detect format if auto
    if format == "auto":
        # Try to infer format from type name patterns
        if "datetime64" in file_type_str or "timestamp" in file_type_str:
            return DataType.DATETIME
        format = "generic"

    # CSV/Pandas dtype mapping
    if format in ("csv", "excel", "pandas", "generic"):
        return _map_pandas_dtype(file_type_str)

    # JSON type mapping
    if format == "json":
        return _map_json_type(file_type_str)

    # Parquet type mapping
    if format == "parquet":
        return _map_parquet_type(file_type_str)

    msg = f"Cannot map type {file_type!r} with format {format!r} to VibePiper DataType"
    raise ValueError(msg)


def _map_pandas_dtype(dtype: str) -> DataType:
    """Map pandas dtype to VibePiper DataType."""
    dtype = dtype.lower()

    # Integer types
    if dtype in (
        "int",
        "int8",
        "int16",
        "int32",
        "int64",
        "uint8",
        "uint16",
        "uint32",
        "uint64",
    ):
        return DataType.INTEGER

    # Float types
    if dtype in ("float", "float16", "float32", "float64"):
        return DataType.FLOAT

    # Boolean type
    if dtype in ("bool", "boolean"):
        return DataType.BOOLEAN

    # DateTime types
    if dtype in ("datetime64", "datetime64[ns]", "datetime64[us]", "timestamp"):
        return DataType.DATETIME

    # Date type
    if dtype in ("date",):
        return DataType.DATE

    # String/Object types
    if dtype in ("str", "string", "object", "category"):
        return DataType.STRING

    # Array types
    if dtype in ("array",):
        return DataType.ARRAY

    # Default to ANY for unknown types
    return DataType.ANY


def _map_json_type(type_name: str) -> DataType:
    """Map JSON type to VibePiper DataType."""
    type_name = type_name.lower()

    if type_name == "str":
        return DataType.STRING
    if type_name in ("int", "integer"):
        return DataType.INTEGER
    if type_name in ("float", "number"):
        return DataType.FLOAT
    if type_name == "bool":
        return DataType.BOOLEAN
    if type_name in ("list", "array"):
        return DataType.ARRAY
    if type_name in ("dict", "object"):
        return DataType.OBJECT
    if type_name == "null":
        return DataType.ANY

    return DataType.ANY


def _map_parquet_type(type_name: str) -> DataType:
    """Map Parquet type to VibePiper DataType."""
    type_name = type_name.lower()

    # Primitive types
    if type_name == "int32":
        return DataType.INTEGER
    if type_name == "int64":
        return DataType.INTEGER
    if type_name == "float":
        return DataType.FLOAT
    if type_name == "double":
        return DataType.FLOAT
    if type_name == "boolean":
        return DataType.BOOLEAN
    if type_name == "string":
        return DataType.STRING
    if type_name == "binary":
        return DataType.STRING

    # Temporal types
    if type_name in ("timestamp", "timestamp_millis", "timestamp_micros"):
        return DataType.DATETIME
    if type_name == "date":
        return DataType.DATE

    # Complex types
    if type_name in ("list", "array"):
        return DataType.ARRAY
    if type_name in ("struct", "map"):
        return DataType.OBJECT

    return DataType.ANY


# =============================================================================
# Type Mapping from VibePiper
# =============================================================================


def map_type_from_vibepiper(data_type: DataType, format: str = "auto") -> str:
    """
    Map a VibePiper DataType to a file format type.

    Args:
        data_type: The VibePiper DataType to map.
        format: The target file format ('csv', 'json', 'parquet', 'excel', 'auto')

    Returns:
        The type string for the target format.

    Raises:
        ValueError: If the type cannot be mapped for the format.

    Examples:
        >>> map_type_from_vibepiper(DataType.INTEGER)
        'int64'
        >>> map_type_from_vibepiper(DataType.STRING, format="parquet")
        'string'
        >>> map_type_from_vibepiper(DataType.DATETIME, format="csv")
        'datetime64[ns]'
    """
    if format == "auto":
        format = "pandas"  # Default to pandas/CSV format

    if format in ("csv", "excel", "pandas"):
        return _map_from_vibepiper_pandas(data_type)

    if format == "json":
        return _map_from_vibepiper_json(data_type)

    if format == "parquet":
        return _map_from_vibepiper_parquet(data_type)

    msg = f"Cannot map DataType {data_type} to format {format!r}"
    raise ValueError(msg)


def _map_from_vibepiper_pandas(data_type: DataType) -> str:
    """Map VibePiper DataType to pandas dtype."""
    mapping: Mapping[DataType, str] = {
        DataType.STRING: "string",
        DataType.INTEGER: "int64",
        DataType.FLOAT: "float64",
        DataType.BOOLEAN: "boolean",
        DataType.DATETIME: "datetime64[ns]",
        DataType.DATE: "datetime64[ns]",
        DataType.ARRAY: "object",
        DataType.OBJECT: "object",
        DataType.ANY: "object",
    }

    if data_type not in mapping:
        msg = f"No pandas dtype mapping for DataType {data_type}"
        raise ValueError(msg)

    return mapping[data_type]


def _map_from_vibepiper_json(data_type: DataType) -> str:
    """Map VibePiper DataType to JSON type."""
    mapping: Mapping[DataType, str] = {
        DataType.STRING: "string",
        DataType.INTEGER: "integer",
        DataType.FLOAT: "number",
        DataType.BOOLEAN: "boolean",
        DataType.DATETIME: "string",  # ISO 8601 format
        DataType.DATE: "string",  # ISO 8601 format
        DataType.ARRAY: "array",
        DataType.OBJECT: "object",
        DataType.ANY: "any",
    }

    if data_type not in mapping:
        msg = f"No JSON type mapping for DataType {data_type}"
        raise ValueError(msg)

    return mapping[data_type]


def _map_from_vibepiper_parquet(data_type: DataType) -> str:
    """Map VibePiper DataType to Parquet type."""
    mapping: Mapping[DataType, str] = {
        DataType.STRING: "string",
        DataType.INTEGER: "int64",
        DataType.FLOAT: "double",
        DataType.BOOLEAN: "boolean",
        DataType.DATETIME: "timestamp",
        DataType.DATE: "date",
        DataType.ARRAY: "list",
        DataType.OBJECT: "struct",
        DataType.ANY: "string",
    }

    if data_type not in mapping:
        msg = f"No Parquet type mapping for DataType {data_type}"
        raise ValueError(msg)

    return mapping[data_type]


# =============================================================================
# Batch Type Mapping
# =============================================================================


def map_schema_to_vibepiper(
    schema_dict: Mapping[str, str], format: str = "auto"
) -> Mapping[str, DataType]:
    """
    Map a schema with file format types to VibePiper types.

    Args:
        schema_dict: Mapping of field names to file format types.
        format: The file format.

    Returns:
        Mapping of field names to VibePiper DataTypes.

    Examples:
        >>> map_schema_to_vibepiper({"id": "int64", "name": "string"})
        {"id": <DataType.INTEGER: 2>, "name": <DataType.STRING: 1>}
    """
    return {name: map_type_to_vibepiper(dtype, format) for name, dtype in schema_dict.items()}


def map_schema_from_vibepiper(
    schema_dict: Mapping[str, DataType], format: str = "auto"
) -> Mapping[str, str]:
    """
    Map a VibePiper schema to file format types.

    Args:
        schema_dict: Mapping of field names to VibePiper DataTypes.
        format: The target file format.

    Returns:
        Mapping of field names to file format type strings.

    Examples:
        >>> map_schema_from_vibepiper({\"id\": DataType.INTEGER, \"name\": DataType.STRING})
        {"id": "int64", "name": "string"}
    """
    return {name: map_type_from_vibepiper(dtype, format) for name, dtype in schema_dict.items()}
