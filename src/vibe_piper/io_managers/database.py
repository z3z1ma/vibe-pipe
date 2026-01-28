"""
Database-based IO Manager.

This module provides an IO manager that stores asset data in a SQL database.
Uses SQLAlchemy for database abstraction and supports multiple database backends.
"""

import json
from typing import Any

from vibe_piper.io_managers.base import IOManagerAdapter
from vibe_piper.types import PipelineContext


class DatabaseIOManager(IOManagerAdapter):
    """
    IO manager that stores data in a SQL database.

    This IO manager persists data to a database, making it available across
    pipeline runs. It uses SQLAlchemy for database abstraction and supports
    multiple database backends (PostgreSQL, MySQL, SQLite, etc.).

    Attributes:
        connection_string: Database connection string
        table_name: Name of the table to store asset data
        schema: Optional database schema

    Example:
        Use the database IO manager::

            @asset(
                io_manager="database",
                uri="postgresql://user:pass@localhost/db"
            )
            def my_asset():
                return {"data": "value"}
    """

    def __init__(
        self,
        connection_string: str,
        table_name: str = "asset_data",
        schema: str | None = None,
    ) -> None:
        """
        Initialize the database IO manager.

        Args:
            connection_string: SQLAlchemy database connection string
            table_name: Name of the table to store asset data
            schema: Optional database schema name

        Raises:
            ImportError: If sqlalchemy is not installed
        """
        try:
            from sqlalchemy import create_engine
            from sqlalchemy.orm import sessionmaker
        except ImportError as e:
            msg = (
                "sqlalchemy is required for DatabaseIOManager. "
                "Install it with: pip install sqlalchemy"
            )
            raise ImportError(msg) from e

        self.connection_string = connection_string
        self.table_name = table_name
        self.schema = schema

        # Create database engine
        self.engine = create_engine(connection_string)
        self.Session = sessionmaker(bind=self.engine)

        # Create table if it doesn't exist
        self._create_table_if_not_exists()

    def _get_full_table_name(self) -> str:
        """
        Get the full table name including schema if specified.

        Returns:
            Full table name
        """
        if self.schema:
            return f"{self.schema}.{self.table_name}"
        return self.table_name

    def _create_table_if_not_exists(self) -> None:
        """Create the asset data table if it doesn't exist."""
        from sqlalchemy import text

        # Build CREATE TABLE statement
        schema_prefix = f"{self.schema}." if self.schema else ""
        create_table_sql = f"""
            CREATE TABLE IF NOT EXISTS {schema_prefix}{self.table_name} (
                asset_key VARCHAR(255) PRIMARY KEY,
                data JSON NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """

        with self.engine.connect() as conn:
            conn.execute(text(create_table_sql))
            conn.commit()

    def _get_asset_key(self, context: PipelineContext) -> str:
        """
        Get the asset key for storage.

        Args:
            context: The pipeline execution context

        Returns:
            Asset key string
        """
        return f"{context.pipeline_id}_{context.run_id}"

    def handle_output(self, context: PipelineContext, data: Any) -> None:
        """
        Store data to the database.

        Args:
            context: The pipeline execution context
            data: The data to store

        Raises:
            IOError: If database operation fails
        """
        from sqlalchemy import text
        from sqlalchemy.exc import SQLAlchemyError

        asset_key = self._get_asset_key(context)
        table_name = self._get_full_table_name()

        # Serialize data to JSON
        try:
            json_data = json.dumps(data, default=str)
        except (TypeError, ValueError) as e:
            msg = f"Failed to serialize data: {e}"
            raise OSError(msg) from e

        # Upsert data into the database
        upsert_sql = f"""
            INSERT INTO {table_name} (asset_key, data, updated_at)
            VALUES (:asset_key, :data, CURRENT_TIMESTAMP)
            ON CONFLICT (asset_key) DO UPDATE SET
                data = EXCLUDED.data,
                updated_at = CURRENT_TIMESTAMP
        """

        # For SQLite, use different syntax
        if self.connection_string.startswith("sqlite"):
            upsert_sql = f"""
                INSERT OR REPLACE INTO {table_name} (asset_key, data, updated_at)
                VALUES (:asset_key, :data, CURRENT_TIMESTAMP)
            """

        try:
            with self.engine.connect() as conn:
                conn.execute(
                    text(upsert_sql),
                    {"asset_key": asset_key, "data": json_data},
                )
                conn.commit()
        except SQLAlchemyError as e:
            msg = f"Failed to store data in database: {e}"
            raise OSError(msg) from e

    def load_input(self, context: PipelineContext) -> Any:
        """
        Load data from the database.

        Args:
            context: The pipeline execution context

        Returns:
            The loaded data

        Raises:
            FileNotFoundError: If the asset doesn't exist
            IOError: If database operation fails
        """
        from sqlalchemy import text
        from sqlalchemy.exc import SQLAlchemyError

        asset_key = self._get_asset_key(context)
        table_name = self._get_full_table_name()

        select_sql = f"""
            SELECT data FROM {table_name}
            WHERE asset_key = :asset_key
        """

        try:
            with self.engine.connect() as conn:
                result = conn.execute(text(select_sql), {"asset_key": asset_key})
                row = result.fetchone()

                if row is None:
                    msg = f"Asset not found in database: {asset_key}"
                    raise FileNotFoundError(msg)

                json_data = row[0]
                return json.loads(json_data)
        except FileNotFoundError:
            raise
        except SQLAlchemyError as e:
            msg = f"Failed to load data from database: {e}"
            raise OSError(msg) from e

    def delete_asset(self, context: PipelineContext) -> None:
        """
        Delete an asset from the database.

        Args:
            context: The pipeline execution context
        """
        from sqlalchemy import text
        from sqlalchemy.exc import SQLAlchemyError

        asset_key = self._get_asset_key(context)
        table_name = self._get_full_table_name()

        delete_sql = f"""
            DELETE FROM {table_name}
            WHERE asset_key = :asset_key
        """

        try:
            with self.engine.connect() as conn:
                conn.execute(text(delete_sql), {"asset_key": asset_key})
                conn.commit()
        except SQLAlchemyError:
            # Ignore errors if asset doesn't exist
            pass

    def has_asset(self, context: PipelineContext) -> bool:
        """
        Check if an asset exists in the database.

        Args:
            context: The pipeline execution context

        Returns:
            True if the asset exists, False otherwise
        """
        from sqlalchemy import text
        from sqlalchemy.exc import SQLAlchemyError

        asset_key = self._get_asset_key(context)
        table_name = self._get_full_table_name()

        select_sql = f"""
            SELECT COUNT(*) FROM {table_name}
            WHERE asset_key = :asset_key
        """

        try:
            with self.engine.connect() as conn:
                result = conn.execute(text(select_sql), {"asset_key": asset_key})
                count = int(result.fetchone()[0])
                return count > 0
        except SQLAlchemyError:
            return False
