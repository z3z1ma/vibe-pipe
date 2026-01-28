"""
Database Connectors for VibePiper

This module provides database connectors for common databases, enabling VibePiper
to interact with real data sources.
"""

from vibe_piper.connectors.base import DatabaseConnector, QueryBuilder

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

__all__ = [
    "DatabaseConnector",
    "QueryBuilder",
    "PostgreSQLConnector",
    "PostgreSQLConfig",
    "MySQLConnector",
    "MySQLConfig",
    "SnowflakeConnector",
    "SnowflakeConfig",
    "BigQueryConnector",
    "BigQueryConfig",
]
