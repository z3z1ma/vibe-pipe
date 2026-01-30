"""
Database Sink Implementation

This module provides DatabaseSink for writing data to databases with
automatic DDL generation, UPSERT logic, batching, and error handling.
"""

import logging
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import datetime

from vibe_piper.connectors.base import DatabaseConnector
from vibe_piper.error_handling import RetryConfig, retry_with_backoff
from vibe_piper.sinks.base import SinkResult
from vibe_piper.sinks.ddl_generator import DDLGenerator, Dialect
from vibe_piper.types import DataRecord, MaterializationStrategy, PipelineContext, Schema

# =============================================================================
# Logger
# =============================================================================

logger = logging.getLogger(__name__)


# =============================================================================
# Configuration
# =============================================================================


@dataclass(frozen=True)
class DatabaseSinkConfig:
    """
    Configuration for DatabaseSink.

    Attributes:
        connector: Database connector instance
        table: Target table name
        schema: Schema for data validation and DDL generation
        schema_name: Optional database schema name
        materialization: Materialization strategy (TABLE, VIEW, FILE, INCREMENTAL)
        upsert_key: Column name(s) for UPSERT conflict resolution
        batch_size: Number of records to batch in one operation
        create_table_if_not_exists: Auto-create table from schema
        retry_config: Retry configuration for write operations
    """

    connector: DatabaseConnector
    table: str
    schema: Schema
    schema_name: str | None = None
    materialization: MaterializationStrategy = MaterializationStrategy.TABLE
    upsert_key: str | list[str] | None = None
    batch_size: int = 1000
    create_table_if_not_exists: bool = True
    retry_config: RetryConfig | None = None


# =============================================================================
# Database Sink Implementation
# =============================================================================


class DatabaseSink:
    """
    Database sink with auto-DDL and auto-UPSERT generation.

    This sink automatically:
    - Creates table from schema if it doesn't exist
    - Generates UPSERT statements based on upsert_key
    - Batches writes for performance
    - Handles errors with automatic retry

    Example:
        >>> connector = PostgreSQLConnector(config)
        >>> schema = Schema(
        ...     name="users",
        ...     fields=(
        ...         SchemaField(name="id", data_type=DataType.INTEGER, required=True),
        ...         SchemaField(name="name", data_type=DataType.STRING),
        ...     )
        ... )
        >>> sink = DatabaseSink(
        ...     config=DatabaseSinkConfig(
        ...         connector=connector,
        ...         table="users",
        ...         schema=schema,
        ...         upsert_key="id",
        ...     )
        ... )
        >>> sink.initialize(context)
        >>> result = sink.write(data_records, context)
        >>> print(f"Written {result.records_written} records")
    """

    def __init__(self, config: DatabaseSinkConfig) -> None:
        """
        Initialize DatabaseSink.

        Args:
            config: DatabaseSink configuration
        """
        self._config = config
        self._ddl_generator = DDLGenerator(self._get_dialect(config.connector))
        self._table_created = False
        self._total_records_written = 0
        self._total_batches = 0

    def _get_dialect(self, connector: DatabaseConnector) -> Dialect:
        """Detect dialect from connector type."""
        connector_type = type(connector).__name__.lower()

        if "postgres" in connector_type:
            return Dialect.POSTGRESQL
        elif "mysql" in connector_type:
            return Dialect.MYSQL
        elif "snowflake" in connector_type:
            return Dialect.SNOWFLAKE
        elif "bigquery" in connector_type:
            return Dialect.BIGQUERY
        else:
            # Default to PostgreSQL for unknown connectors
            logger.warning(
                f"Unknown connector type {connector_type}, defaulting to PostgreSQL dialect"
            )
            return Dialect.POSTGRESQL

    def initialize(self, context: PipelineContext) -> None:
        """
        Initialize sink by creating table if needed.

        Args:
            context: Pipeline execution context
        """
        if self._config.create_table_if_not_exists and not self._table_created:
            self._create_table_if_not_exists(context)

    def cleanup(self, context: PipelineContext) -> None:
        """
        Clean up resources after writing.

        Args:
            context: Pipeline execution context
        """
        # Disconnect from database
        # Note: Connector management is handled by the connector itself
        logger.info(
            f"DatabaseSink cleanup: {self._total_records_written} records written in {self._total_batches} batches"
        )

    def write(
        self,
        data: Sequence[DataRecord],
        context: PipelineContext,
    ) -> SinkResult:
        """
        Write data to database with batching and retry.

        Args:
            data: Sequence of DataRecord objects to write
            context: Pipeline execution context

        Returns:
            SinkResult with operation outcome
        """
        if not data:
            return SinkResult(
                success=True,
                records_written=0,
                timestamp=datetime.now(),
            )

        # Get retry decorator if config provided
        if self._config.retry_config:
            retry_decorator = self._create_retry_decorator()
            write_func = retry_decorator(self._write_with_retry)
        else:
            write_func = self._write_with_retry

        # Write with retry
        try:
            records_written = write_func(data, context)
            return SinkResult(
                success=True,
                records_written=records_written,
                timestamp=datetime.now(),
                metrics={
                    "total_records": self._total_records_written,
                    "total_batches": self._total_batches,
                },
            )
        except Exception as e:
            logger.error(f"DatabaseSink write failed: {e}")
            return SinkResult(
                success=False,
                records_written=0,
                error=str(e),
                timestamp=datetime.now(),
            )

    def _write_with_retry(self, data: Sequence[DataRecord], context: PipelineContext) -> int:
        """
        Write data to database (internal method for retry).

        Args:
            data: Sequence of DataRecord objects to write
            context: Pipeline execution context

        Returns:
            Number of records written

        Raises:
            Exception: If write operation fails
        """
        records_written = 0

        # Write in batches
        for i in range(0, len(data), self._config.batch_size):
            batch = data[i : i + self._config.batch_size]
            batch_count = self._write_batch(batch, context)
            records_written += batch_count
            self._total_batches += 1

        self._total_records_written += records_written
        return records_written

    def _write_batch(self, batch: Sequence[DataRecord], context: PipelineContext) -> int:
        """
        Write a single batch to database.

        Args:
            batch: Batch of DataRecord objects to write
            context: Pipeline execution context

        Returns:
            Number of records written in this batch

        Raises:
            Exception: If write operation fails
        """
        if not batch:
            return 0

        # Generate appropriate SQL statement
        if self._config.upsert_key:
            # Use UPSERT statement
            upsert_sql = self._ddl_generator.generate_upsert(
                table_name=self._config.table,
                schema=self._config.schema,
                upsert_key=self._config.upsert_key,
                schema_name=self._config.schema_name,
            )
        else:
            # Use simple INSERT
            upsert_sql = None

        # Execute SQL
        connector = self._config.connector

        if upsert_sql:
            # Execute UPSERT with parameterized query
            records_data = [record.data for record in batch]
            affected_rows = 0

            for record_data in records_data:
                if hasattr(connector, "execute"):
                    # Use execute() method
                    count = connector.execute(upsert_sql, record_data)
                    affected_rows += count
                else:
                    msg = f"Connector {type(connector).__name__} does not support execute()"
                    raise RuntimeError(msg)

            return affected_rows
        else:
            # Use batch insert if available
            if hasattr(connector, "execute_batch"):
                records_data = [record.data for record in batch]
                count = connector.execute_batch(
                    query=self._build_insert_query(),
                    params_list=records_data,
                )
                return count
            else:
                # Fall back to individual inserts
                records_data = [record.data for record in batch]
                affected_rows = 0

                for record_data in records_data:
                    count = connector.execute(
                        query=self._build_insert_query(),
                        params=record_data,
                    )
                    affected_rows += count

                return affected_rows

    def _build_insert_query(self) -> str:
        """Build INSERT query from schema."""
        fields = [f.name for f in self._config.schema.fields]
        columns = ", ".join(fields)
        placeholders = ", ".join([f":{f}" for f in fields])

        return f"INSERT INTO {self._config.table} ({columns}) VALUES ({placeholders})"

    def _create_table_if_not_exists(self, context: PipelineContext) -> None:
        """
        Create table from schema if it doesn't exist.

        Args:
            context: Pipeline execution context
        """
        connector = self._config.connector

        # Check if table exists
        if hasattr(connector, "table_exists"):
            table_exists = connector.table_exists(self._config.table)
            if table_exists:
                logger.info(f"Table {self._config.table} already exists, skipping creation")
                self._table_created = True
                return

        # Generate CREATE TABLE statement
        create_sql = self._ddl_generator.generate_create_table(
            table_name=self._config.table,
            schema=self._config.schema,
            schema_name=self._config.schema_name,
        )

        logger.info(f"Creating table {self._config.table} with DDL:\n{create_sql}")

        # Execute CREATE TABLE
        if hasattr(connector, "execute"):
            connector.execute(create_sql)
        else:
            msg = f"Connector {type(connector).__name__} does not support execute()"
            raise RuntimeError(msg)

        self._table_created = True

    def _create_retry_decorator(self):
        """Create retry decorator from config."""
        config = self._config.retry_config

        if config:
            return retry_with_backoff(
                max_retries=config.max_retries,
                backoff=config.backoff_strategy.name.lower(),
                jitter=config.jitter_strategy.name.lower(),
                base_delay=config.base_delay,
                max_delay=config.max_delay,
                jitter_amount=config.jitter_amount,
                retry_on_exceptions=config.retry_on_exceptions,
            )

        return None

    def get_metrics(self) -> dict[str, int | float]:
        """
        Get metrics about sink operations.

        Returns:
            Dictionary of metric names to values
        """
        return {
            "total_records_written": self._total_records_written,
            "total_batches": self._total_batches,
        }

    def table_exists(self) -> bool:
        """
        Check if target table exists.

        Returns:
            True if table exists, False otherwise
        """
        connector = self._config.connector
        if hasattr(connector, "table_exists"):
            return connector.table_exists(self._config.table)
        return False

    def drop_table(self, cascade: bool = False) -> None:
        """
        Drop the target table.

        Args:
            cascade: Whether to use CASCADE clause
        """
        drop_sql = self._ddl_generator.generate_drop_table(
            table_name=self._config.table,
            if_exists=True,
            cascade=cascade,
            schema_name=self._config.schema_name,
        )

        logger.info(f"Dropping table {self._config.table}")

        connector = self._config.connector
        if hasattr(connector, "execute"):
            connector.execute(drop_sql)
        else:
            msg = f"Connector {type(connector).__name__} does not support execute()"
            raise RuntimeError(msg)

        self._table_created = False
