"""
Connectors for Vibe Piper.

This module provides both database and file I/O connectors for Vibe Piper,
enabling interaction with various data sources and formats.
"""

# Database connectors
from vibe_piper.connectors.base import DatabaseConnector, FileReader, FileWriter, QueryBuilder

# Optional imports for database connectors
try:
    from vibe_piper.connectors.postgres import PostgreSQLConfig, PostgreSQLConnector

    _postgres_available = True
except ImportError:
    _postgres_available = False
    PostgreSQLConnector = None  # type: ignore
    PostgreSQLConfig = None  # type: ignore

try:
    from vibe_piper.connectors.mysql import MySQLConfig, MySQLConnector

    _mysql_available = True
except ImportError:
    _mysql_available = False
    MySQLConnector = None  # type: ignore
    MySQLConfig = None  # type: ignore

try:
    from vibe_piper.connectors.snowflake import SnowflakeConfig, SnowflakeConnector

    _snowflake_available = True
except ImportError:
    _snowflake_available = False
    SnowflakeConnector = None  # type: ignore
    SnowflakeConfig = None  # type: ignore

try:
    from vibe_piper.connectors.bigquery import BigQueryConfig, BigQueryConnector

    _bigquery_available = True
except ImportError:
    _bigquery_available = False
    BigQueryConnector = None  # type: ignore
    BigQueryConfig = None  # type: ignore

# File I/O connectors
try:
    from vibe_piper.connectors.csv import CSVReader, CSVWriter
except ImportError:
    CSVReader = None  # type: ignore
    CSVWriter = None  # type: ignore

try:
    from vibe_piper.connectors.excel import ExcelReader, ExcelWriter
except ImportError:
    ExcelReader = None  # type: ignore
    ExcelWriter = None  # type: ignore

try:
    from vibe_piper.connectors.json import JSONReader, JSONWriter
except ImportError:
    JSONReader = None  # type: ignore
    JSONWriter = None  # type: ignore

try:
    from vibe_piper.connectors.parquet import ParquetReader, ParquetWriter
except ImportError:
    ParquetReader = None  # type: ignore
    ParquetWriter = None  # type: ignore

try:
    from vibe_piper.connectors.utils import infer_schema_from_file
    from vibe_piper.connectors.utils.type_mapping import (
        map_type_from_vibepiper,
        map_type_to_vibepiper,
    )
except ImportError:
    infer_schema_from_file = None  # type: ignore
    map_type_to_vibepiper = None  # type: ignore
    map_type_from_vibepiper = None  # type: ignore

__all__ = [
    # Base
    "DatabaseConnector",
    "QueryBuilder",
    "FileReader",
    "FileWriter",
    # Database connectors
    "PostgreSQLConnector",
    "PostgreSQLConfig",
    "MySQLConnector",
    "MySQLConfig",
    "SnowflakeConnector",
    "SnowflakeConfig",
    "BigQueryConnector",
    "BigQueryConfig",
    # File I/O
    "CSVReader",
    "CSVWriter",
    "JSONReader",
    "JSONWriter",
    "ParquetReader",
    "ParquetWriter",
    "ExcelReader",
    "ExcelWriter",
    # Utilities
    "infer_schema_from_file",
    "map_type_to_vibepiper",
    "map_type_from_vibepiper",
]
