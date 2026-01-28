"""
File I/O connectors for Vibe Piper.

This module provides a unified interface for reading and writing data files
in various formats including CSV, JSON, Parquet, and Excel.

All connectors support:
- Schema validation and inference
- Type mapping to/from VibePiper types
- Compression support
- Chunked reading for large files
- Error handling and recovery

Example:
    >>> from vibe_piper.connectors import CSVReader, ParquetWriter
    >>>
    >>> # Read a CSV file
    >>> reader = CSVReader("data/customers.csv")
    >>> data = reader.read()
    >>>
    >>> # Write to Parquet with compression
    >>> writer = ParquetWriter("output/customers.parquet")
    >>> writer.write(data, compression="snappy")
"""

from vibe_piper.connectors.base import FileReader, FileWriter
from vibe_piper.connectors.csv import CSVReader, CSVWriter
from vibe_piper.connectors.excel import ExcelReader, ExcelWriter
from vibe_piper.connectors.json import JSONReader, JSONWriter
from vibe_piper.connectors.parquet import ParquetReader, ParquetWriter
from vibe_piper.connectors.utils import infer_schema_from_file
from vibe_piper.connectors.utils.type_mapping import (
    map_type_from_vibepiper,
    map_type_to_vibepiper,
)

__all__ = [
    # Base protocols
    "FileReader",
    "FileWriter",
    # CSV
    "CSVReader",
    "CSVWriter",
    # JSON
    "JSONReader",
    "JSONWriter",
    # Parquet
    "ParquetReader",
    "ParquetWriter",
    # Excel
    "ExcelReader",
    "ExcelWriter",
    # Utilities
    "infer_schema_from_file",
    "map_type_to_vibepiper",
    "map_type_from_vibepiper",
]
