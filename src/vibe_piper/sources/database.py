"""
Database Source Implementation

Provides declarative database source with:
- Incremental loading with watermark tracking
- Auto-schema inference from result sets
- Query builder integration
- Support for PostgreSQL, MySQL, Snowflake, BigQuery
"""

import logging
from collections.abc import AsyncIterator, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Literal

import pandas as pd

from vibe_piper.connectors.base import DatabaseConnector, QueryBuilder
from vibe_piper.connectors.bigquery import BigQueryConfig, BigQueryConnector
from vibe_piper.connectors.mysql import MySQLConfig, MySQLConnector
from vibe_piper.connectors.postgres import PostgreSQLConfig, PostgreSQLConnector
from vibe_piper.connectors.snowflake import SnowflakeConfig, SnowflakeConnector
from vibe_piper.connectors.utils.inference import infer_schema_from_pandas
from vibe_piper.sources.base import Source
from vibe_piper.types import DataRecord, PipelineContext, Schema

# =============================================================================
# Configuration Classes
# =============================================================================


@dataclass
class DatabaseConnectionConfig:
    """Database connection configuration."""

    type: Literal["postgres", "mysql", "snowflake", "bigquery"]
    """Database type"""

    host: str | None = None
    """Database host (not for BigQuery)"""

    port: int | None = None
    """Database port (not for BigQuery)"""

    database: str | None = None
    """Database name (dataset for BigQuery)"""

    user: str | None = None
    """Database user"""

    password: str | None = None
    """Database password"""

    project_id: str | None = None
    """BigQuery project ID"""

    dataset: str | None = None
    """BigQuery dataset"""

    credentials_path: str | None = None
    """Path to service account credentials (BigQuery, Snowflake)"""

    account: str | None = None
    """Snowflake account"""

    warehouse: str | None = None
    """Snowflake warehouse"""

    pool_size: int = 10
    """Connection pool size"""


@dataclass
class DatabaseConfig:
    """Complete configuration for database source."""

    name: str
    """Source name"""

    connection: DatabaseConnectionConfig
    """Database connection configuration"""

    query: str | None = None
    """SQL query to execute"""

    table: str | None = None
    """Table name to fetch from (alternative to query)"""

    incremental: bool = False
    """Whether to use incremental loading"""

    watermark_column: str | None = None
    """Column to track incremental loading (e.g., updated_at)"""

    watermark_path: str | None = None
    """Path to store watermark state file"""

    schema: Schema | None = None
    """Optional explicit schema (inferred if None)"""


# =============================================================================
# Database Source Implementation
# =============================================================================


class DatabaseSource(Source[DataRecord]):
    """
    Database source with incremental loading support.

    Provides a declarative interface for fetching data from databases
    with automatic schema inference and incremental loading support.

    Example:
        Basic database source::

            source = DatabaseSource(
                DatabaseConfig(
                    name="users",
                    connection=DatabaseConnectionConfig(
                        type="postgres",
                        host="localhost",
                        port=5432,
                        database="mydb",
                        user="user",
                        password="pass",
                    ),
                    table="users",
                )
            )

            data = await source.fetch(context)

        Incremental loading::

            source = DatabaseSource(
                DatabaseConfig(
                    name="users",
                    connection=DatabaseConnectionConfig(
                        type="postgres",
                        host="localhost",
                        database="mydb",
                        user="user",
                        password="pass",
                    ),
                    table="users",
                    incremental=True,
                    watermark_column="updated_at",
                    watermark_path="/tmp/watermarks/users.json",
                )
            )
    """

    def __init__(self, config: DatabaseConfig) -> None:
        """
        Initialize database source.

        Args:
            config: Database source configuration
        """
        self.config = config
        self._connector: DatabaseConnector | None = None
        self._watermark_value: Any | None = None
        self._logger = logging.getLogger(self.__class__.__name__)

    def _get_connector(self) -> DatabaseConnector:
        """Get or create database connector."""
        if self._connector is None:
            self._connector = self._create_connector()
            self._connector.connect()
        return self._connector

    def _create_connector(self) -> DatabaseConnector:
        """Create database connector from connection config."""
        conn_cfg = self.config.connection

        if conn_cfg.type == "postgres":
            if not conn_cfg.host or not conn_cfg.port or not conn_cfg.database:
                msg = "PostgreSQL requires host, port, and database"
                raise ValueError(msg)
            config = PostgreSQLConfig(
                host=conn_cfg.host,
                port=conn_cfg.port,
                database=conn_cfg.database,
                user=conn_cfg.user or "",
                password=conn_cfg.password or "",
                pool_size=conn_cfg.pool_size,
            )
            return PostgreSQLConnector(config)

        elif conn_cfg.type == "mysql":
            if not conn_cfg.host or not conn_cfg.port or not conn_cfg.database:
                msg = "MySQL requires host, port, and database"
                raise ValueError(msg)
            config = MySQLConfig(
                host=conn_cfg.host,
                port=conn_cfg.port,
                database=conn_cfg.database,
                user=conn_cfg.user or "",
                password=conn_cfg.password or "",
                pool_size=conn_cfg.pool_size,
            )
            return MySQLConnector(config)

        elif conn_cfg.type == "snowflake":
            if not conn_cfg.account or not conn_cfg.database:
                msg = "Snowflake requires account and database"
                raise ValueError(msg)
            config = SnowflakeConfig(
                account=conn_cfg.account,
                database=conn_cfg.database,
                user=conn_cfg.user or "",
                password=conn_cfg.password or "",
                warehouse=conn_cfg.warehouse,
                private_key_path=conn_cfg.credentials_path,
            )
            return SnowflakeConnector(config)

        elif conn_cfg.type == "bigquery":
            if not conn_cfg.project_id or not conn_cfg.dataset:
                msg = "BigQuery requires project_id and dataset"
                raise ValueError(msg)
            config = BigQueryConfig(
                project_id=conn_cfg.project_id,
                dataset=conn_cfg.dataset,
                credentials_path=conn_cfg.credentials_path,
            )
            return BigQueryConnector(config)

        msg = f"Unsupported database type: {conn_cfg.type}"
        raise ValueError(msg)

    def _load_watermark(self) -> Any:
        """Load watermark value from state file."""
        if not self.config.watermark_path:
            return None

        path = Path(self.config.watermark_path)
        if not path.exists():
            return None

        try:
            import json

            with open(path) as f:
                data = json.load(f)
                return data.get("value")
        except Exception as e:
            self._logger.warning("Failed to load watermark: %s", e)
            return None

    def _save_watermark(self, value: Any) -> None:
        """Save watermark value to state file."""
        if not self.config.watermark_path:
            return

        path = Path(self.config.watermark_path)
        path.parent.mkdir(parents=True, exist_ok=True)

        try:
            import json

            with open(path, "w") as f:
                json.dump({"value": value}, f)
        except Exception as e:
            self._logger.error("Failed to save watermark: %s", e)

    async def fetch(self, context: PipelineContext) -> Sequence[DataRecord]:
        """
        Fetch data from database source.

        Handles incremental loading and schema inference automatically.

        Args:
            context: Pipeline execution context

        Returns:
            Sequence of DataRecord objects

        Raises:
            Exception: If fetch fails
        """
        connector = self._get_connector()

        # Load watermark if incremental
        if self.config.incremental:
            self._watermark_value = self._load_watermark()

        # Build query
        query = self._build_query()

        # Execute query
        result = connector.execute_query(query)

        # Convert to DataRecords
        schema = self.config.schema or self.infer_schema()
        records = [DataRecord(data=row, schema=schema) for row in result.rows]

        # Save watermark if incremental and data was fetched
        if self.config.incremental and records:
            # Get max watermark value from fetched data
            if self.config.watermark_column:
                max_watermark = max(
                    (
                        row.get(self.config.watermark_column)
                        for row in result.rows
                        if self.config.watermark_column in row
                    ),
                    default=None,
                )
                if max_watermark is not None:
                    self._save_watermark(max_watermark)

        return records

    def _build_query(self) -> str:
        """Build SQL query from configuration."""
        if self.config.query:
            # Use explicit query
            return self.config.query

        if self.config.table:
            # Build query from table
            query_builder = QueryBuilder(self.config.table)

            # Add incremental filter if configured
            if (
                self.config.incremental
                and self.config.watermark_column
                and self._watermark_value is not None
            ):
                query_builder.where(
                    f"{self.config.watermark_column} > :watermark",
                    watermark=self._watermark_value,
                )

            query, params = query_builder.build_select()
            return query

        msg = "Either query or table must be specified"
        raise ValueError(msg)

    async def stream(self, context: PipelineContext) -> AsyncIterator[DataRecord]:
        """
        Stream data from database source.

        Useful for large database tables where loading all data at once is impractical.

        Args:
            context: Pipeline execution context

        Yields:
            Individual DataRecord objects
        """
        connector = self._get_connector()

        # Build query
        query = self._build_query()

        # Execute query
        result = connector.execute_query(query)

        schema = self.config.schema or self.infer_schema()

        for row in result.rows:
            yield DataRecord(data=row, schema=schema)

    def infer_schema(self) -> Schema:
        """
        Infer schema from database result set.

        Returns:
            Inferred Schema
        """
        connector = self._get_connector()

        # Build query
        query = self._build_query()

        # Execute query to get sample data
        result = connector.execute_query(query)

        if not result.rows:
            return Schema(name=self.config.name)

        # Convert to DataFrame for schema inference
        df = pd.DataFrame(result.rows, columns=result.columns)

        return infer_schema_from_pandas(df, name=self.config.name)

    def get_metadata(self) -> dict[str, Any]:
        """
        Get metadata about database source.

        Returns:
            Dictionary of metadata
        """
        return {
            "source_type": "database",
            "name": self.config.name,
            "database_type": self.config.connection.type,
            "table": self.config.table,
            "incremental": self.config.incremental,
            "watermark_column": self.config.watermark_column,
        }
